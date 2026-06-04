"""
Segmentation Service
=====================

Orchestrates segment CRUD, query compilation, PostgreSQL execution,
audience preview, Rank/Split, Set Operations, and scheduled computation.

Performance architecture:
  - Async connection pool (psycopg_pool.AsyncConnectionPool) — one pool shared
    across all requests; eliminates per-request TCP handshake + auth overhead.
  - Redis result cache — count and summary queries are cached for 10 minutes
    keyed on sha256(sql). Identical segments never hit the DB twice in a window.
  - COUNT(DISTINCT) compiled directly — no subquery materialisation wrapper.
  - Attribute distinct-value queries cached for 1 hour.
  - All DB I/O is properly async so FastAPI's event loop is never blocked.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Any

import redis.asyncio as aioredis
from psycopg.rows import dict_row

try:
    from psycopg_pool import AsyncConnectionPool
    _HAS_POOL = True
except ImportError:
    AsyncConnectionPool = None  # type: ignore[assignment,misc]
    _HAS_POOL = False

from app.schemas.segment_rules import SegmentDefinition
from app.services.query_engine.compiler import AthenaCompiler
from app.services.query_engine.pg_compiler import (
    PgCompiler,
    CorporatePgCompiler,
    compile_ranked,
    compile_set_operation,
    compile_set_operation_count,
    compile_split,
)

logger = logging.getLogger(__name__)


def _make_compiler(brand_code: str) -> PgCompiler:
    """
    Factory that returns the appropriate SQL compiler for a brand.

    ``"corporate"`` → CorporatePgCompiler  (flat cross-brand table, r1_id identity)
    everything else → PgCompiler           (per-brand DWH with unified_profiles)
    """
    if brand_code == "corporate":
        return CorporatePgCompiler()
    return PgCompiler(brand_code=brand_code)

# ---------------------------------------------------------------------------
# Module-level shared resources — initialised once at app startup
# ---------------------------------------------------------------------------

_pg_pool: AsyncConnectionPool | None = None
_redis: aioredis.Redis | None = None

_COUNT_TTL = 600       # 10 minutes
_SUMMARY_TTL = 600     # 10 minutes
_VALUES_TTL = 3600     # 1 hour — attribute distinct values change rarely


def _get_pg_conninfo() -> str:
    host = os.getenv("PG_HOST", "localhost")
    port = os.getenv("PG_PORT", "5432")
    dbname = os.getenv("PG_DB", "cdp_meta")
    user = os.getenv("PG_USER", "cdp")
    password = os.getenv("PG_PASSWORD", "cdp")
    return f"host={host} port={port} dbname={dbname} user={user} password={password}"


async def init_resources() -> None:
    """Open the connection pool and Redis client. Called once at app startup."""
    global _pg_pool, _redis

    conninfo = _get_pg_conninfo()
    if _HAS_POOL:
        try:
            _pg_pool = AsyncConnectionPool(
                conninfo,
                min_size=2,
                max_size=10,
                open=False,
                kwargs={"row_factory": dict_row},
            )
            await _pg_pool.open(wait=True)
            logger.info("PostgreSQL connection pool opened (min=2, max=10)")
        except Exception as exc:
            logger.warning(f"Connection pool failed to open ({exc}) — falling back to per-request connections")
            _pg_pool = None
    else:
        logger.warning("psycopg-pool not installed — run `pip install psycopg-pool` for connection pooling. Falling back to per-request connections.")

    redis_url = os.getenv("REDIS_URL", "redis://localhost:6379")
    try:
        _redis = aioredis.from_url(redis_url, decode_responses=True, socket_connect_timeout=2)
        await _redis.ping()
        logger.info(f"Redis connected at {redis_url}")
    except Exception as exc:
        logger.warning(f"Redis unavailable ({exc}) — caching disabled, queries will still work")
        _redis = None


async def close_resources() -> None:
    """Close pool and Redis on app shutdown."""
    global _pg_pool, _redis
    if _pg_pool:
        await _pg_pool.close()
        logger.info("PostgreSQL connection pool closed")
    if _redis:
        await _redis.aclose()


_PROPENSITY_TABLE_DDL = """
CREATE SCHEMA IF NOT EXISTS silver_reverse_etl;
CREATE SCHEMA IF NOT EXISTS nb_silver_reverse_etl;

CREATE TABLE IF NOT EXISTS silver_reverse_etl.customer_propensity_scores_spencers (
    customer_id                                          TEXT        NOT NULL PRIMARY KEY,
    spencers_segment_1_propensity                        NUMERIC(6,4),
    spencers_segment_2_propensity                        NUMERIC(6,4),
    spencers_segment_3_propensity                        NUMERIC(6,4),
    spencers_segment_4_propensity                        NUMERIC(6,4),
    spencers_segment_5_propensity                        NUMERIC(6,4),
    spencers_segment_6_propensity                        NUMERIC(6,4),
    spencers_segment_1_normalized_propensity             NUMERIC(6,4),
    spencers_segment_2_normalized_propensity             NUMERIC(6,4),
    spencers_segment_3_normalized_propensity             NUMERIC(6,4),
    spencers_segment_4_normalized_propensity             NUMERIC(6,4),
    spencers_segment_5_normalized_propensity             NUMERIC(6,4),
    spencers_segment_6_normalized_propensity             NUMERIC(6,4),
    model_run_date                                       DATE         DEFAULT CURRENT_DATE,
    updated_at                                           TIMESTAMPTZ  DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS nb_silver_reverse_etl.customer_propensity_scores_nbl (
    customer_id                                          TEXT        NOT NULL PRIMARY KEY,
    nbl_segment_1_propensity                             NUMERIC(6,4),
    nbl_segment_2_propensity                             NUMERIC(6,4),
    nbl_segment_3_propensity                             NUMERIC(6,4),
    nbl_segment_4_propensity                             NUMERIC(6,4),
    nbl_segment_1_normalized_propensity                  NUMERIC(6,4),
    nbl_segment_2_normalized_propensity                  NUMERIC(6,4),
    nbl_segment_3_normalized_propensity                  NUMERIC(6,4),
    nbl_segment_4_normalized_propensity                  NUMERIC(6,4),
    model_run_date                                       DATE         DEFAULT CURRENT_DATE,
    updated_at                                           TIMESTAMPTZ  DEFAULT NOW()
);
"""


async def ensure_propensity_tables() -> None:
    """
    Create propensity score tables if they don't exist.
    Called at startup so affinity attributes are always queryable — even before
    the first model run (all scores will be NULL until populated).
    """
    pool = _pg_pool
    if pool is None:
        logger.warning("Skipping propensity table init — pool not available")
        return
    try:
        async with pool.connection() as conn:
            await conn.execute(_PROPENSITY_TABLE_DDL)
            await conn.commit()
        logger.info("Propensity score tables verified/created")
    except Exception as exc:
        logger.warning("Could not create propensity tables: %s", exc)


_SEGMENTS_TABLE_DDL = """
CREATE TABLE IF NOT EXISTS public.saved_segments (
    id               UUID PRIMARY KEY,
    name             TEXT NOT NULL,
    description      TEXT,
    business_purpose TEXT,
    tags             TEXT[] NOT NULL DEFAULT '{}',
    segment_type     TEXT NOT NULL DEFAULT 'customer',   -- customer | corporate
    brand_code       TEXT,
    rules            JSONB NOT NULL,
    audience_count   BIGINT,
    status           TEXT NOT NULL DEFAULT 'active',      -- active | archived
    created_by       TEXT,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_computed_at TIMESTAMPTZ,
    parent_segment_id UUID,                                -- lineage: parent for rank/split children
    lineage          JSONB                                 -- rank/split logic provenance
);
CREATE INDEX IF NOT EXISTS idx_saved_segments_status  ON public.saved_segments (status);
CREATE INDEX IF NOT EXISTS idx_saved_segments_updated ON public.saved_segments (updated_at DESC);
-- Backfill columns for tables created before lineage existed.
ALTER TABLE public.saved_segments ADD COLUMN IF NOT EXISTS parent_segment_id UUID;
ALTER TABLE public.saved_segments ADD COLUMN IF NOT EXISTS lineage JSONB;
"""


async def ensure_segments_table() -> None:
    """Create the saved_segments repository table if it doesn't exist (startup)."""
    pool = _pg_pool
    if pool is None:
        logger.warning("Skipping saved_segments table init — pool not available")
        return
    try:
        async with pool.connection() as conn:
            await conn.execute(_SEGMENTS_TABLE_DDL)
            await conn.commit()
        logger.info("Saved-segments table verified/created")
    except Exception as exc:
        logger.warning("Could not create saved_segments table: %s", exc)


def _cache_key(prefix: str, sql: str) -> str:
    digest = hashlib.sha256(sql.encode()).hexdigest()
    return f"cdp:{prefix}:{digest}"


# ---------------------------------------------------------------------------
# SegmentationService
# ---------------------------------------------------------------------------


class SegmentationService:
    """
    High-level service for segment operations.
    All heavy DB calls are async and use the shared connection pool.
    """

    def __init__(
        self,
        db_session: Any = None,
        athena_client: Any = None,
        redis_client: Any = None,
    ):
        self.db = db_session
        self.athena = athena_client
        # redis_client arg kept for backwards compat; module-level _redis is used instead

    # =========================================================================
    # Internal async DB helpers
    # =========================================================================

    async def _execute_pg(self, sql: str) -> list[dict]:
        """Execute SQL via the shared async pool."""
        pool = _pg_pool
        if pool is None:
            # Pool not yet initialised (e.g. during tests) — fall back to direct connect
            import psycopg
            conninfo = _get_pg_conninfo()
            async with await psycopg.AsyncConnection.connect(conninfo, row_factory=dict_row) as conn:
                async with conn.cursor() as cur:
                    await cur.execute(sql)
                    return [dict(row) for row in await cur.fetchall()]

        try:
            async with pool.connection() as conn:
                async with conn.cursor() as cur:
                    await cur.execute(sql)
                    return [dict(row) for row in await cur.fetchall()]
        except Exception as e:
            logger.error(f"PostgreSQL execution error: {e}")
            raise

    async def _execute_pg_count(self, sql: str, cache_ttl: int = _COUNT_TTL) -> int | None:
        """Execute a COUNT query with Redis caching."""
        redis = _redis
        cache_key = _cache_key("count", sql)

        if redis:
            try:
                cached = await redis.get(cache_key)
                if cached is not None:
                    logger.debug(f"[CACHE HIT] count {cache_key[:24]}…")
                    return int(cached)
            except Exception as e:
                logger.warning(f"Redis GET failed: {e}")

        try:
            rows = await self._execute_pg(sql)
            count = int(rows[0]["audience_count"]) if rows and "audience_count" in rows[0] else 0
        except Exception:
            return None

        if redis and count is not None:
            try:
                await redis.setex(cache_key, cache_ttl, str(count))
            except Exception as e:
                logger.warning(f"Redis SET failed: {e}")

        return count

    async def _execute_pg_summary(self, sql: str) -> list[dict] | None:
        """Execute a summary/aggregation query with Redis caching."""
        redis = _redis
        cache_key = _cache_key("summary", sql)

        if redis:
            try:
                cached = await redis.get(cache_key)
                if cached is not None:
                    logger.debug(f"[CACHE HIT] summary {cache_key[:24]}…")
                    return json.loads(cached)
            except Exception as e:
                logger.warning(f"Redis GET failed: {e}")

        try:
            rows = await self._execute_pg(sql)
        except Exception:
            return None

        if redis:
            try:
                await redis.setex(cache_key, _SUMMARY_TTL, json.dumps(rows, default=str))
            except Exception as e:
                logger.warning(f"Redis SET failed: {e}")

        return rows

    # =========================================================================
    # SEGMENT CRUD
    # =========================================================================

    async def create_segment(
        self,
        brand_id: str | None,
        name: str,
        description: str,
        rules: dict,
        segment_type: str = "dynamic",
        schedule: str = "hourly",
        is_cross_brand: bool = False,
        created_by: str | None = None,
        tags: list[str] | None = None,
    ) -> dict:
        segment_id = str(uuid.uuid4())
        slug = name.lower().replace(" ", "-").replace("_", "-")
        definition = SegmentDefinition.model_validate(rules)

        compiler = _make_compiler(brand_id or "spencers")
        count_sql = compiler.compile_count(definition)
        audience_count = await self._execute_pg_count(count_sql)

        segment = {
            "id": segment_id,
            "brand_id": brand_id,
            "is_cross_brand": is_cross_brand,
            "name": name,
            "description": description,
            "slug": slug,
            "segment_type": segment_type,
            "rules": rules,
            "schedule": schedule,
            "is_active": True,
            "audience_count": audience_count,
            "computation_status": "ready" if audience_count is not None else "pending",
            "last_computed_at": datetime.now(timezone.utc).isoformat() if audience_count is not None else None,
            "tags": tags or [],
            "created_by": created_by,
            "created_at": datetime.now(timezone.utc).isoformat(),
        }

        logger.info(f"Created segment {segment_id}: {name} (audience: {audience_count})")
        return segment

    async def update_segment_rules(self, segment_id: str, rules: dict, updated_by: str | None = None) -> dict:
        SegmentDefinition.model_validate(rules)
        logger.info(f"Updated rules for segment {segment_id}")
        return {"segment_id": segment_id, "status": "rules_updated", "computation_status": "pending"}

    async def delete_segment(self, segment_id: str) -> dict:
        return {"segment_id": segment_id, "status": "deleted"}

    # =========================================================================
    # SAVED SEGMENTS REPOSITORY (persisted in public.saved_segments)
    # =========================================================================

    async def _exec_params(self, sql: str, params: tuple = (), fetch: bool = True) -> list[dict]:
        """Parameterized execute via the shared async pool (or direct fallback)."""
        from psycopg.rows import dict_row

        pool = _pg_pool
        if pool is None:
            import psycopg

            async with await psycopg.AsyncConnection.connect(
                _get_pg_conninfo(), row_factory=dict_row
            ) as conn:
                async with conn.cursor() as cur:
                    await cur.execute(sql, params)
                    rows = [dict(r) for r in await cur.fetchall()] if fetch else []
                    await conn.commit()
                    return rows

        async with pool.connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute(sql, params)
                rows = [dict(r) for r in await cur.fetchall()] if fetch else []
            await conn.commit()
            return rows

    @staticmethod
    def _segment_type(brand_code: str | None) -> str:
        return "corporate" if (brand_code or "").lower() == "corporate" else "customer"

    async def save_segment(self, payload: dict) -> dict:
        """Persist a new segment, computing its current audience count."""
        seg_id = str(uuid.uuid4())
        brand_code = payload.get("brand_code")
        rules = payload["rules"]
        SegmentDefinition.model_validate(rules)

        # Compute the live audience count for this segment.
        audience_count = None
        try:
            compiler = _make_compiler(brand_code or "spencers")
            count_sql = compiler.compile_count(SegmentDefinition.model_validate(rules))
            audience_count = await self._execute_pg_count(count_sql)
        except Exception as e:
            logger.warning(f"save_segment count failed: {e}")

        import json

        rows = await self._exec_params(
            """
            INSERT INTO public.saved_segments
                (id, name, description, business_purpose, tags, segment_type,
                 brand_code, rules, audience_count, status, created_by, last_computed_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, 'active', %s,
                    CASE WHEN %s IS NULL THEN NULL ELSE NOW() END)
            RETURNING *
            """,
            (
                seg_id, payload["name"], payload.get("description"),
                payload.get("business_purpose"), payload.get("tags") or [],
                self._segment_type(brand_code), brand_code, json.dumps(rules),
                audience_count, payload.get("created_by"), audience_count,
            ),
        )
        return self._row_to_segment(rows[0])

    async def list_saved_segments(
        self, search: str | None = None, segment_type: str | None = None,
        status: str | None = "active", created_by: str | None = None,
        sort: str = "updated", page: int = 1, page_size: int = 50,
    ) -> dict:
        clauses, params = ["1=1"], []
        if status and status != "all":
            clauses.append("status = %s"); params.append(status)
        if segment_type and segment_type != "all":
            clauses.append("segment_type = %s"); params.append(segment_type)
        if created_by:
            clauses.append("created_by ILIKE %s"); params.append(f"%{created_by}%")
        if search:
            clauses.append(
                "(name ILIKE %s OR description ILIKE %s OR business_purpose ILIKE %s "
                "OR array_to_string(tags, ',') ILIKE %s OR created_by ILIKE %s)"
            )
            params += [f"%{search}%"] * 5
        where = " AND ".join(clauses)
        order = {"name": "name ASC", "created": "created_at DESC",
                 "count": "audience_count DESC NULLS LAST"}.get(sort, "updated_at DESC")

        total = (await self._exec_params(
            f"SELECT COUNT(*) AS c FROM public.saved_segments WHERE {where}", tuple(params)
        ))[0]["c"]

        params2 = params + [page_size, (page - 1) * page_size]
        rows = await self._exec_params(
            f"SELECT * FROM public.saved_segments WHERE {where} "
            f"ORDER BY {order} LIMIT %s OFFSET %s", tuple(params2)
        )
        return {
            "segments": [self._row_to_segment(r) for r in rows],
            "total": total, "page": page, "page_size": page_size,
        }

    async def get_saved_segment(self, seg_id: str) -> dict | None:
        rows = await self._exec_params(
            "SELECT * FROM public.saved_segments WHERE id = %s", (seg_id,))
        return self._row_to_segment(rows[0]) if rows else None

    async def update_saved_segment(self, seg_id: str, payload: dict) -> dict | None:
        import json

        sets, params = [], []
        for field in ("name", "description", "business_purpose", "status"):
            if field in payload and payload[field] is not None:
                sets.append(f"{field} = %s"); params.append(payload[field])
        if "tags" in payload and payload["tags"] is not None:
            sets.append("tags = %s"); params.append(payload["tags"])
        recompute_count = None
        if "rules" in payload and payload["rules"] is not None:
            SegmentDefinition.model_validate(payload["rules"])
            sets.append("rules = %s"); params.append(json.dumps(payload["rules"]))
            # Recompute audience count when logic changes.
            existing = await self.get_saved_segment(seg_id)
            brand = payload.get("brand_code") or (existing or {}).get("brand_code")
            try:
                compiler = _make_compiler(brand or "spencers")
                csql = compiler.compile_count(SegmentDefinition.model_validate(payload["rules"]))
                recompute_count = await self._execute_pg_count(csql)
            except Exception as e:
                logger.warning(f"update recompute failed: {e}")
            sets.append("audience_count = %s"); params.append(recompute_count)
            sets.append("last_computed_at = NOW()")
        if not sets:
            return await self.get_saved_segment(seg_id)
        sets.append("updated_at = NOW()")
        params.append(seg_id)
        rows = await self._exec_params(
            f"UPDATE public.saved_segments SET {', '.join(sets)} WHERE id = %s RETURNING *",
            tuple(params),
        )
        return self._row_to_segment(rows[0]) if rows else None

    async def clone_saved_segment(self, seg_id: str, created_by: str | None = None) -> dict | None:
        src = await self.get_saved_segment(seg_id)
        if not src:
            return None
        return await self.save_segment({
            "name": f"{src['name']} (Copy)",
            "description": src.get("description"),
            "business_purpose": src.get("business_purpose"),
            "tags": src.get("tags") or [],
            "brand_code": src.get("brand_code"),
            "rules": src["rules"],
            "created_by": created_by or src.get("created_by"),
        })

    async def refresh_saved_segment_count(self, seg_id: str) -> dict | None:
        src = await self.get_saved_segment(seg_id)
        if not src:
            return None
        try:
            rules = src["rules"] or {}
            if isinstance(rules, dict) and rules.get("_rank_split"):
                # Reproducible rank/split child — recompute via the rank/split engine.
                count = await self._rank_split_child_count(
                    src.get("brand_code") or "spencers", rules)
            else:
                compiler = _make_compiler(src.get("brand_code") or "spencers")
                csql = compiler.compile_count(SegmentDefinition.model_validate(rules))
                count = await self._execute_pg_count(csql)
        except Exception as e:
            logger.warning(f"refresh count failed: {e}")
            count = None
        rows = await self._exec_params(
            "UPDATE public.saved_segments SET audience_count = %s, last_computed_at = NOW(), "
            "updated_at = NOW() WHERE id = %s RETURNING *", (count, seg_id))
        return self._row_to_segment(rows[0]) if rows else None

    async def delete_saved_segment(self, seg_id: str) -> bool:
        rows = await self._exec_params(
            "DELETE FROM public.saved_segments WHERE id = %s RETURNING id", (seg_id,))
        return bool(rows)

    # =========================================================================
    # RANK & SPLIT ENGINE
    # =========================================================================

    @staticmethod
    def _effective_limit(constraints: dict | None) -> int | None:
        """Derive a top-N cap from count/budget constraints (best-practice: budget wins)."""
        if not constraints:
            return None
        budget = constraints.get("budget")
        cpc = constraints.get("cost_per_contact")
        if budget and cpc and float(cpc) > 0:
            return max(0, int(float(budget) // float(cpc)))
        if constraints.get("max_count"):
            return int(constraints["max_count"])
        return None

    def _membership_sql(self, brand_code: str, rules: dict) -> str:
        from app.schemas.segment_rules import SegmentDefinition as _SD
        root = rules.get("root", rules)
        compiler = _make_compiler(brand_code)
        return compiler.compile(_SD.model_validate({"root": root}))

    async def rank_split_preview(
        self, brand_code: str, rules: dict, rank: list[dict] | None,
        splits: list[dict], constraints: dict | None = None,
    ) -> dict:
        """Compute per-group count, revenue, and avg spend for a rank/split config."""
        from app.services.query_engine.pg_compiler import BRAND_SCHEMA_CONFIG
        from app.services.segmentation.rank_split import build_rank_split_sql, cumulative_bounds

        if brand_code not in BRAND_SCHEMA_CONFIG:
            return {"groups": [], "error": "Rank & Split currently supports Spencer's and Nature's Basket."}

        ba_table = BRAND_SCHEMA_CONFIG[brand_code]["customer_behavioral_attributes"]
        membership = self._membership_sql(brand_code, rules)
        rank = rank or []
        eff_limit = self._effective_limit(constraints)
        bounds = cumulative_bounds(splits)

        groups = []
        for split, (lo, hi) in zip(splits, bounds):
            sql = build_rank_split_sql(membership, ba_table, rank, lo, hi, eff_limit, select="metrics")
            try:
                rows = await self._execute_pg(sql)
                r = rows[0] if rows else {}
            except Exception as e:
                logger.warning(f"rank_split preview group failed: {e}")
                r = {}
            cnt = int(r.get("cnt") or 0)
            rev = float(r.get("revenue") or 0)
            groups.append({
                "name": split.get("name") or f"Group {len(groups) + 1}",
                "percent": split.get("percent"),
                "pct_lower": lo, "pct_upper": hi,
                "count": cnt,
                "revenue": rev,
                "avg_spend": float(r.get("avg_spend") or 0),
            })

        return {
            "groups": groups,
            "total_count": sum(g["count"] for g in groups),
            "total_revenue": sum(g["revenue"] for g in groups),
            "effective_limit": eff_limit,
        }

    async def _rank_split_child_count(self, brand_code: str, child_rules: dict) -> int | None:
        """Recompute the audience count for a saved rank/split child segment."""
        from app.services.query_engine.pg_compiler import BRAND_SCHEMA_CONFIG
        from app.services.segmentation.rank_split import build_rank_split_sql

        rs = child_rules.get("_rank_split")
        if not rs or brand_code not in BRAND_SCHEMA_CONFIG:
            return None
        ba_table = BRAND_SCHEMA_CONFIG[brand_code]["customer_behavioral_attributes"]
        membership = self._membership_sql(brand_code, child_rules)
        sql = build_rank_split_sql(
            membership, ba_table, rs.get("rank") or [],
            rs["pct_lower"], rs["pct_upper"], rs.get("effective_limit"), select="count",
        )
        return await self._execute_pg_count(sql)

    async def save_rank_split_segments(
        self, brand_code: str, base_name: str, rules: dict,
        rank: list[dict] | None, splits: list[dict], constraints: dict | None,
        created_by: str | None, parent_segment_id: str | None = None,
    ) -> dict:
        """Persist each rank/split group as a reproducible child saved segment."""
        import json
        from app.services.segmentation.rank_split import cumulative_bounds

        rank = rank or []
        eff_limit = self._effective_limit(constraints)
        bounds = cumulative_bounds(splits)
        preview = await self.rank_split_preview(brand_code, rules, rank, splits, constraints)
        counts = {g["name"]: g["count"] for g in preview.get("groups", [])}

        root = rules.get("root", rules)
        seg_type = self._segment_type(brand_code)
        created = []
        for split, (lo, hi) in zip(splits, bounds):
            gname = split.get("name") or f"Group {len(created) + 1}"
            full_name = f"{base_name} — {gname}"
            child_rules = {
                "root": root,
                "_rank_split": {
                    "rank": rank, "pct_lower": lo, "pct_upper": hi,
                    "effective_limit": eff_limit,
                },
            }
            lineage = {
                "parent_name": base_name,
                "rank": rank,
                "split": {"type": "percent", "name": gname, "pct_lower": lo, "pct_upper": hi},
                "constraints": constraints or {},
            }
            seg_id = str(uuid.uuid4())
            rows = await self._exec_params(
                """
                INSERT INTO public.saved_segments
                    (id, name, description, tags, segment_type, brand_code, rules,
                     audience_count, status, created_by, last_computed_at,
                     parent_segment_id, lineage)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, 'active', %s, NOW(), %s, %s)
                RETURNING *
                """,
                (
                    seg_id, full_name,
                    f"Rank & Split group of “{base_name}” ({gname})",
                    split.get("tags") or [gname], seg_type, brand_code,
                    json.dumps(child_rules), counts.get(gname), created_by,
                    parent_segment_id, json.dumps(lineage),
                ),
            )
            created.append(self._row_to_segment(rows[0]))
        return {"created": created, "count": len(created)}

    @staticmethod
    def _row_to_segment(r: dict) -> dict:
        def iso(v):
            return v.isoformat() if hasattr(v, "isoformat") else v
        return {
            "id": str(r["id"]),
            "name": r["name"],
            "description": r.get("description"),
            "business_purpose": r.get("business_purpose"),
            "tags": list(r.get("tags") or []),
            "segment_type": r.get("segment_type"),
            "brand_code": r.get("brand_code"),
            "rules": r.get("rules"),
            "audience_count": r.get("audience_count"),
            "status": r.get("status"),
            "created_by": r.get("created_by"),
            "created_at": iso(r.get("created_at")),
            "updated_at": iso(r.get("updated_at")),
            "last_computed_at": iso(r.get("last_computed_at")),
            "parent_segment_id": str(r["parent_segment_id"]) if r.get("parent_segment_id") else None,
            "lineage": r.get("lineage"),
        }

    # =========================================================================
    # QUERY COMPILATION
    # =========================================================================

    def compile_segment_query(self, brand_code: str, rules: dict, datalake_config: dict | None = None) -> str:
        definition = SegmentDefinition.model_validate(rules)
        compiler = _make_compiler(brand_code)
        sql = compiler.compile(definition)

        if definition.rank and definition.rank.enabled and definition.rank.attribute:
            from app.services.query_engine.pg_compiler import SPENCERS_SCHEMA_MAP
            rank_col = SPENCERS_SCHEMA_MAP.get(
                definition.rank.attribute,
                f"ba.{definition.rank.attribute.split('.')[-1]}"
            )
            sql = compile_ranked(
                sql,
                rank_attribute=rank_col,
                rank_order=definition.rank.order,
                profile_limit=definition.rank.profile_limit,
            )

        return sql

    def compile_count_query(self, brand_code: str, rules: dict, datalake_config: dict | None = None) -> str:
        definition = SegmentDefinition.model_validate(rules)
        compiler = _make_compiler(brand_code)
        return compiler.compile_count(definition)

    def compile_preview_query(self, brand_code: str, rules: dict, limit: int = 100, datalake_config: dict | None = None) -> str:
        definition = SegmentDefinition.model_validate(rules)
        compiler = _make_compiler(brand_code)
        return compiler.compile_preview(definition, limit=limit)

    def compile_athena_query(self, brand_code: str, rules: dict, datalake_config: dict | None = None) -> str:
        config = datalake_config or {}
        definition = SegmentDefinition.model_validate(rules)
        compiler = AthenaCompiler(
            brand_code=brand_code,
            database=config.get("athena_database", f"{brand_code}_gold"),
            schema_mapping=config.get("schema_mapping", {}),
        )
        return compiler.compile(definition)

    # =========================================================================
    # AUDIENCE ESTIMATION
    # =========================================================================

    async def estimate_audience_size(self, brand_code: str, rules: dict, datalake_config: dict | None = None) -> dict:
        """Estimate audience size — count query cached in Redis for 10 min."""
        import json as _json
        logger.info(f"[ESTIMATE] brand={brand_code}")
        definition = SegmentDefinition.model_validate(rules)
        compiler = _make_compiler(brand_code)

        base_sql = compiler.compile(definition)
        count_sql = compiler.compile_count(definition)
        logger.debug(f"[ESTIMATE] sql={count_sql}")

        estimated_count = await self._execute_pg_count(count_sql)

        result = {
            "brand_code": brand_code,
            "estimated_count": estimated_count,
            "sql": count_sql,
            "status": "completed" if estimated_count is not None else "failed",
            "set_operation_counts": None,
            "split_counts": None,
        }

        # Handle set operations
        if definition.set_operation and definition.set_operation.enabled:
            so = definition.set_operation
            segment_sqls = [base_sql]

            for entry in so.segments:
                entry_rules = None
                if entry.rules:
                    entry_rules = entry.rules.model_dump()
                elif entry.segment_id:
                    # Resolve a saved segment by id → its stored rules.
                    saved = await self.get_saved_segment(entry.segment_id)
                    if saved:
                        entry_rules = saved.get("rules")
                if not entry_rules:
                    continue
                entry_compiler = _make_compiler(brand_code)
                # A rank/split child stores membership under _rank_split; use its base root.
                root_only = {"root": entry_rules.get("root", entry_rules)}
                entry_def = SegmentDefinition.model_validate(root_only)
                segment_sqls.append(entry_compiler.compile(entry_def))

            if len(segment_sqls) > 1:
                combined_count_sql = compile_set_operation_count(so.operation, segment_sqls)
                combined_count = await self._execute_pg_count(combined_count_sql)
                result["set_operation_counts"] = {
                    "operation": so.operation,
                    "combined_count": combined_count,
                    "segment_counts": [],
                }
                import asyncio
                ind_sqls = [
                    f"SELECT COUNT(DISTINCT customer_id) AS audience_count FROM (\n{sql}\n) seg_{i}"
                    for i, sql in enumerate(segment_sqls)
                ]
                ind_counts = await asyncio.gather(
                    *[self._execute_pg_count(s) for s in ind_sqls]
                )
                result["set_operation_counts"]["segment_counts"] = list(ind_counts)
                result["estimated_count"] = combined_count
                result["sql"] = combined_count_sql

        # Handle splits
        if definition.split and definition.split.enabled:
            sp = definition.split
            split_results = compile_split(
                base_sql,
                split_type=sp.split_type,
                split_config={
                    "attribute": sp.attribute,
                    "splits": [s.model_dump() for s in sp.splits],
                },
            )
            import asyncio
            split_sqls = [
                f"SELECT COUNT(DISTINCT customer_id) AS audience_count FROM (\n{sr['sql']}\n) split_sub"
                for sr in split_results
            ]
            split_count_results = await asyncio.gather(
                *[self._execute_pg_count(s) for s in split_sqls]
            )
            result["split_counts"] = [
                {
                    "name": sr["name"],
                    "count": sc,
                    "percent": sr.get("percent"),
                    "value": sr.get("value"),
                }
                for sr, sc in zip(split_results, split_count_results)
            ]

        return result

    # =========================================================================
    # SEGMENT SUMMARY
    # =========================================================================

    async def get_segment_summary(self, brand_code: str, rules: dict, metrics: list[str] | None = None) -> dict:
        """Calculate behavioral summary metrics — result cached in Redis for 10 min."""
        if metrics is None:
            metrics = ["total_spend", "avg_spend", "total_bills", "avg_visits", "spend_per_bill", "spend_per_visit"]

        definition = SegmentDefinition.model_validate(rules)
        compiler = _make_compiler(brand_code)
        summary_sql = compiler.compile_summary(definition, metrics)

        try:
            results = await self._execute_pg_summary(summary_sql)
            if not results:
                return {
                    "brand_code": brand_code,
                    "audience_size": 0,
                    "metrics": {m: 0 for m in metrics},
                    "sql": summary_sql,
                    "status": "completed",
                }

            row = dict(results[0])
            audience_size = row.pop("audience_size", 0)

            formatted_metrics = {}
            for k, v in row.items():
                if v is None:
                    formatted_metrics[k] = 0
                elif hasattr(v, "__float__"):
                    formatted_metrics[k] = float(v)
                else:
                    formatted_metrics[k] = v

            return {
                "brand_code": brand_code,
                "audience_size": audience_size,
                "metrics": formatted_metrics,
                "sql": summary_sql,
                "status": "completed",
            }
        except Exception as e:
            logger.error(f"Failed to calculate segment summary: {e}")
            return {
                "brand_code": brand_code,
                "audience_size": 0,
                "metrics": {m: None for m in metrics},
                "sql": summary_sql,
                "status": f"failed: {str(e)}",
            }

    # =========================================================================
    # AUDIENCE PREVIEW
    # =========================================================================

    async def preview_audience(self, brand_code: str, rules: dict, limit: int = 100, datalake_config: dict | None = None) -> dict:
        """Get a preview of matching profiles — not cached (limit is variable)."""
        preview_sql = self.compile_preview_query(brand_code, rules, limit, datalake_config)

        try:
            profiles = await self._execute_pg(preview_sql)
            for p in profiles:
                for k, v in p.items():
                    if isinstance(v, datetime):
                        p[k] = v.isoformat()
            return {
                "brand_code": brand_code,
                "profiles": profiles,
                "sql": preview_sql,
                "status": "completed",
            }
        except Exception as e:
            return {
                "brand_code": brand_code,
                "profiles": [],
                "sql": preview_sql,
                "status": f"failed: {str(e)}",
            }

    # =========================================================================
    # ATTRIBUTE DISTINCT VALUES
    # =========================================================================

    _ALIAS_TO_TABLE_BY_BRAND: dict[str, dict[str, str]] = {
        "spencers": {
            "p":    "silver_identity.unified_profiles",
            "ba":   "silver_reverse_etl.customer_behavioral_attributes",
            "gs":   "silver_identity.identity_graph_summary",
            "loc":  "bronze.raw_location_master",
            "bt":   "silver.s_fact_bill_transactions",
            "prop": "silver_reverse_etl.customer_propensity_scores_spencers",
        },
        "natures_basket": {
            "p":    "nb_silver_identity.unified_profiles",
            "ba":   "nb_silver_reverse_etl.customer_behavioral_attributes",
            "gs":   "nb_silver_identity.identity_graph_summary",
            "loc":  "nb_bronze.raw_location_master",
            "bt":   "nb_silver.s_fact_bill_transactions",
            "prop": "nb_silver_reverse_etl.customer_propensity_scores_nbl",
        },
        # Corporate cross-brand view — single flat table, alias "corp"
        "corporate": {
            "corp": "corporate_cih.silver_corp_customer_attributes",
        },
    }

    def get_attribute_distinct_values(
        self,
        attribute_key: str,
        limit: int = 500,
        brand_code: str = "spencers",
    ) -> list[str]:
        """Sync shim kept for callers that can't easily be made async."""
        import asyncio
        try:
            loop = asyncio.get_event_loop()
            if loop.is_running():
                import concurrent.futures
                with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
                    future = pool.submit(
                        asyncio.run,
                        self._get_attribute_distinct_values_async(attribute_key, limit, brand_code),
                    )
                    return future.result()
            else:
                return loop.run_until_complete(
                    self._get_attribute_distinct_values_async(attribute_key, limit, brand_code)
                )
        except Exception as e:
            logger.warning(f"get_attribute_distinct_values shim failed: {e}")
            return []

    async def _get_attribute_distinct_values_async(
        self,
        attribute_key: str,
        limit: int = 500,
        brand_code: str = "spencers",
    ) -> list[str]:
        """Fetch distinct non-null values for an attribute — cached for 1 hour."""
        from app.services.query_engine.pg_compiler import SPENCERS_SCHEMA_MAP, CORPORATE_SCHEMA_MAP
        import re

        # Resolve the column reference from the appropriate schema map
        if brand_code == "corporate":
            col_ref = CORPORATE_SCHEMA_MAP.get(attribute_key)
            # Auto-derive spn.*/nbl.* keys not in the map
            if not col_ref:
                parts = attribute_key.split(".", 1)
                if len(parts) == 2 and parts[0] in ("spn", "nbl"):
                    col_ref = f"corp.{parts[0]}_{parts[1]}"
        else:
            col_ref = SPENCERS_SCHEMA_MAP.get(attribute_key)

        if not col_ref:
            return []

        m = re.search(r'\b([a-z_]+)\.([a-z_]+)\b', col_ref, re.IGNORECASE)
        if not m:
            return []

        alias = m.group(1)
        col_name = m.group(2)
        alias_map = self._ALIAS_TO_TABLE_BY_BRAND.get(
            brand_code, self._ALIAS_TO_TABLE_BY_BRAND["spencers"]
        )
        table = alias_map.get(alias)
        if not table:
            return []

        sql = (
            f"SELECT DISTINCT {col_name}::TEXT AS val "
            f"FROM {table} "
            f"WHERE {col_name} IS NOT NULL "
            f"  AND TRIM({col_name}::TEXT) != '' "
            f"ORDER BY val "
            f"LIMIT {int(limit)}"
        )

        # Check cache first
        redis = _redis
        cache_key = _cache_key("vals", f"{brand_code}:{attribute_key}:{limit}")
        if redis:
            try:
                cached = await redis.get(cache_key)
                if cached is not None:
                    logger.debug(f"[CACHE HIT] attribute values {attribute_key}")
                    return json.loads(cached)
            except Exception as e:
                logger.warning(f"Redis GET failed: {e}")

        try:
            rows = await self._execute_pg(sql)
            values = [row["val"] for row in rows if row.get("val") is not None]
        except Exception as e:
            logger.warning(f"Could not fetch distinct values for '{attribute_key}': {e}")
            return []

        if redis:
            try:
                await redis.setex(cache_key, _VALUES_TTL, json.dumps(values))
            except Exception as e:
                logger.warning(f"Redis SET failed: {e}")

        return values

    # =========================================================================
    # SCHEDULED COMPUTATION
    # =========================================================================

    async def compute_segment(self, segment_id: str) -> dict:
        logger.info(f"Computing segment {segment_id}")
        return {"segment_id": segment_id, "status": "computed"}

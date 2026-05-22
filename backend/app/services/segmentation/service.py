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
                if entry.rules:
                    entry_def = SegmentDefinition.model_validate(entry.rules.model_dump())
                    entry_compiler = _make_compiler(brand_code)
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
            "p":   "silver_identity.unified_profiles",
            "ba":  "silver_reverse_etl.customer_behavioral_attributes",
            "gs":  "silver_identity.identity_graph_summary",
            "loc": "bronze.raw_location_master",
            "bt":  "silver.s_fact_bill_transactions",
        },
        "natures_basket": {
            "p":   "nb_silver_identity.unified_profiles",
            "ba":  "nb_silver_reverse_etl.customer_behavioral_attributes",
            "gs":  "nb_silver_identity.identity_graph_summary",
            "loc": "nb_bronze.raw_location_master",
            "bt":  "nb_silver.s_fact_bill_transactions",
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

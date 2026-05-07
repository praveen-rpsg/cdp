"""
Segmentation Service
=====================

Orchestrates segment CRUD, query compilation, PostgreSQL execution,
audience preview, Rank/Split, Set Operations, and scheduled computation.

Now uses PgCompiler for Spencer's DWH (PostgreSQL) as the primary engine,
with AthenaCompiler still available for cloud data lake queries.
"""

from __future__ import annotations

import logging
import os
import uuid
from datetime import datetime, timezone
from typing import Any

import psycopg
from psycopg.rows import dict_row

from app.schemas.segment_rules import SegmentDefinition
from app.services.query_engine.compiler import AthenaCompiler
from app.services.query_engine.pg_compiler import (
    PgCompiler,
    compile_ranked,
    compile_set_operation,
    compile_set_operation_count,
    compile_split,
)

logger = logging.getLogger(__name__)


def _get_pg_conninfo() -> str:
    host = os.getenv("PG_HOST", "localhost")
    port = os.getenv("PG_PORT", "5432")
    dbname = os.getenv("PG_DB", "cdp_meta")
    user = os.getenv("PG_USER", "postgres")
    password = os.getenv("PG_PASSWORD", "Raghav_1174")
    return f"host={host} port={port} dbname={dbname} user={user} password={password}"


class SegmentationService:
    """
    High-level service for segment operations.

    Uses PostgreSQL (Spencer's DWH) for real-time audience computation.
    """

    def __init__(
        self,
        db_session: Any = None,
        athena_client: Any = None,
        redis_client: Any = None,
    ):
        self.db = db_session
        self.athena = athena_client
        self.redis = redis_client
        self._pg_conninfo = _get_pg_conninfo()

    # =========================================================================
    # PostgreSQL Execution
    # =========================================================================

    def _execute_pg(self, sql: str) -> list[dict]:
        """Execute SQL against the Spencer's DWH PostgreSQL and return results."""
        try:
            with psycopg.connect(self._pg_conninfo) as conn:
                with conn.cursor(row_factory=dict_row) as cur:
                    cur.execute(sql)
                    return [dict(row) for row in cur.fetchall()]
        except Exception as e:
            logger.error(f"PostgreSQL execution error: {e}")
            raise

    def _execute_pg_count(self, sql: str) -> int | None:
        """Execute a COUNT query and return the count."""
        try:
            rows = self._execute_pg(sql)
            if rows and "audience_count" in rows[0]:
                return rows[0]["audience_count"]
            return 0
        except Exception:
            return None

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

        # Compile and get count from PostgreSQL
        compiler = PgCompiler(brand_code=brand_id or "spencers")
        count_sql = compiler.compile_count(definition)
        audience_count = self._execute_pg_count(count_sql)

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
        definition = SegmentDefinition.model_validate(rules)
        logger.info(f"Updated rules for segment {segment_id}")
        return {"segment_id": segment_id, "status": "rules_updated", "computation_status": "pending"}

    async def delete_segment(self, segment_id: str) -> dict:
        return {"segment_id": segment_id, "status": "deleted"}

    # =========================================================================
    # QUERY COMPILATION (PostgreSQL + Athena)
    # =========================================================================

    def compile_segment_query(self, brand_code: str, rules: dict, datalake_config: dict | None = None) -> str:
        """Compile segment rules into PostgreSQL SQL."""
        definition = SegmentDefinition.model_validate(rules)
        compiler = PgCompiler(brand_code=brand_code)
        sql = compiler.compile(definition)

        # Apply rank if enabled
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
        compiler = PgCompiler(brand_code=brand_code)
        return compiler.compile_count(definition)

    def compile_preview_query(self, brand_code: str, rules: dict, limit: int = 100, datalake_config: dict | None = None) -> str:
        definition = SegmentDefinition.model_validate(rules)
        compiler = PgCompiler(brand_code=brand_code)
        return compiler.compile_preview(definition, limit=limit)

    def compile_athena_query(self, brand_code: str, rules: dict, datalake_config: dict | None = None) -> str:
        """Legacy: Compile to Athena SQL (kept for reference/cloud mode)."""
        config = datalake_config or {}
        definition = SegmentDefinition.model_validate(rules)
        compiler = AthenaCompiler(
            brand_code=brand_code,
            database=config.get("athena_database", f"{brand_code}_gold"),
            schema_mapping=config.get("schema_mapping", {}),
        )
        return compiler.compile(definition)

    # =========================================================================
    # AUDIENCE ESTIMATION (Real PostgreSQL execution)
    # =========================================================================

    async def estimate_audience_size(self, brand_code: str, rules: dict, datalake_config: dict | None = None) -> dict:
        """Estimate audience size by actually executing against Spencer's DWH."""
        import json as _json
        logger.info(f"[ESTIMATE] rules_json={_json.dumps(rules, default=str)}")
        definition = SegmentDefinition.model_validate(rules)
        compiler = PgCompiler(brand_code=brand_code)

        # Base query
        base_sql = compiler.compile(definition)
        logger.info(f"[ESTIMATE] compiled_sql={base_sql}")
        count_sql = compiler.compile_count(definition)

        # Execute count
        estimated_count = self._execute_pg_count(count_sql)

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
                    entry_compiler = PgCompiler(brand_code=brand_code)
                    segment_sqls.append(entry_compiler.compile(entry_def))

            if len(segment_sqls) > 1:
                combined_count_sql = compile_set_operation_count(so.operation, segment_sqls)
                combined_count = self._execute_pg_count(combined_count_sql)
                result["set_operation_counts"] = {
                    "operation": so.operation,
                    "combined_count": combined_count,
                    "segment_counts": [],
                }
                # Get individual counts
                for i, sql in enumerate(segment_sqls):
                    ind_count_sql = f"SELECT COUNT(*) AS audience_count FROM (\n{sql}\n) seg_{i}"
                    ind_count = self._execute_pg_count(ind_count_sql)
                    result["set_operation_counts"]["segment_counts"].append(ind_count)

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
            split_counts = []
            for sr in split_results:
                sc_sql = f"SELECT COUNT(*) AS audience_count FROM (\n{sr['sql']}\n) split_sub"
                sc = self._execute_pg_count(sc_sql)
                split_counts.append({
                    "name": sr["name"],
                    "count": sc,
                    "percent": sr.get("percent"),
                    "value": sr.get("value"),
                })
            result["split_counts"] = split_counts

        return result

    async def get_segment_summary(self, brand_code: str, rules: dict, metrics: list[str] | None = None) -> dict:
        """Calculate behavioral summary metrics for a segment."""
        if metrics is None:
            metrics = ["total_spend", "avg_spend", "total_bills", "avg_visits", "spend_per_bill", "spend_per_visit"]
            
        definition = SegmentDefinition.model_validate(rules)
        compiler = PgCompiler(brand_code=brand_code)
        summary_sql = compiler.compile_summary(definition, metrics)
        
        try:
            results = self._execute_pg(summary_sql)
            if not results:
                return {
                    "brand_code": brand_code,
                    "audience_size": 0,
                    "metrics": {m: 0 for m in metrics},
                    "sql": summary_sql,
                    "status": "completed",
                }
            
            row = results[0]
            audience_size = row.pop("audience_size", 0)
            
            # Convert decimal results to floats for JSON serialization
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

    async def preview_audience(self, brand_code: str, rules: dict, limit: int = 100, datalake_config: dict | None = None) -> dict:
        """Get a preview of matching profiles from Spencer's DWH."""
        preview_sql = self.compile_preview_query(brand_code, rules, limit, datalake_config)

        try:
            profiles = self._execute_pg(preview_sql)
            # Convert any non-serializable types
            for p in profiles:
                for k, v in p.items():
                    if isinstance(v, (datetime,)):
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
    # SCHEDULED COMPUTATION
    # =========================================================================

    async def compute_segment(self, segment_id: str) -> dict:
        logger.info(f"Computing segment {segment_id}")
        return {"segment_id": segment_id, "status": "computed"}

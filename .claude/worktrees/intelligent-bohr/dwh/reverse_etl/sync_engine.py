"""
Spencer's CDP - Reverse ETL Sync Engine
========================================
Framework for syncing behavioral attributes and audiences to downstream
marketing tools (CleverTap, MoEngage, webhooks, CSV exports).

Inspired by Hightouch/Census-style reverse ETL architecture:
- Config-driven audience definitions
- Incremental sync with change detection
- Multiple destination adapters
- Audit logging of all syncs
"""

import os
import csv
import json
import logging
import hashlib
from datetime import datetime
from pathlib import Path
from abc import ABC, abstractmethod

import psycopg
from psycopg.rows import dict_row
import requests
import yaml

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


# ============================================================================
# Destination Adapters
# ============================================================================

class DestinationAdapter(ABC):
    """Base class for reverse ETL destination adapters."""

    @abstractmethod
    def sync_batch(self, records: list[dict], audience_id: str) -> dict:
        """Sync a batch of records. Returns {synced: int, failed: int, errors: list}."""
        pass

    @abstractmethod
    def name(self) -> str:
        pass


class WebhookAdapter(DestinationAdapter):
    """Sends audience records to a webhook endpoint (e.g., CleverTap, MoEngage API)."""

    def __init__(self, config: dict):
        self.url = os.path.expandvars(config.get("url", ""))
        self.batch_size = config.get("batch_size", 100)
        self.headers = config.get("headers", {"Content-Type": "application/json"})
        self.auth_token = os.getenv("REVERSE_ETL_AUTH_TOKEN", "")

    def name(self) -> str:
        return f"webhook:{self.url}"

    def sync_batch(self, records: list[dict], audience_id: str) -> dict:
        result = {"synced": 0, "failed": 0, "errors": []}
        headers = {**self.headers}
        if self.auth_token:
            headers["Authorization"] = f"Bearer {self.auth_token}"

        for i in range(0, len(records), self.batch_size):
            batch = records[i:i + self.batch_size]
            payload = {
                "audience_id": audience_id,
                "records": batch,
                "synced_at": datetime.utcnow().isoformat(),
                "batch_index": i // self.batch_size,
            }
            try:
                resp = requests.post(self.url, json=payload, headers=headers, timeout=30)
                if resp.status_code < 300:
                    result["synced"] += len(batch)
                else:
                    result["failed"] += len(batch)
                    result["errors"].append(f"HTTP {resp.status_code}: {resp.text[:200]}")
            except requests.RequestException as e:
                result["failed"] += len(batch)
                result["errors"].append(str(e))
                logger.warning(f"Webhook sync failed for batch {i}: {e}")

        return result


class CSVAdapter(DestinationAdapter):
    """Exports audience records to CSV files."""

    def __init__(self, config: dict):
        self.output_dir = Path(config.get("output_dir", "./exports"))
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def name(self) -> str:
        return f"csv:{self.output_dir}"

    def sync_batch(self, records: list[dict], audience_id: str) -> dict:
        if not records:
            return {"synced": 0, "failed": 0, "errors": []}

        timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        filepath = self.output_dir / f"{audience_id}_{timestamp}.csv"

        with open(filepath, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=records[0].keys())
            writer.writeheader()
            writer.writerows(records)

        logger.info(f"Exported {len(records)} records to {filepath}")
        return {"synced": len(records), "failed": 0, "errors": []}


# Adapter factory
ADAPTERS = {
    "webhook": WebhookAdapter,
    "csv": CSVAdapter,
}


# ============================================================================
# Sync Engine
# ============================================================================

class SyncEngine:
    """Orchestrates reverse ETL syncs from PostgreSQL to downstream destinations."""

    def __init__(self, config_path: str = None):
        if config_path is None:
            config_path = str(Path(__file__).parent / "config.yml")

        with open(config_path, "r") as f:
            raw = f.read()
            # Expand ${VAR:-default} patterns
            import re
            def _expand(match):
                var = match.group(1)
                default = match.group(3) if match.group(3) is not None else ""
                return os.environ.get(var, default)
            raw = re.sub(r'\$\{(\w+)(:-([^}]*))?\}', _expand, raw)
            self.config = yaml.safe_load(raw)

        db_conf = self.config["database"]
        host = str(db_conf.get("host", "localhost"))
        port = str(db_conf.get("port", 5432))
        dbname = str(db_conf.get("dbname", "cdp_meta"))
        user = str(db_conf.get("user", "cdp"))
        password = str(db_conf.get("password", "cdp"))
        self.db_conninfo = f"host={host} port={port} dbname={dbname} user={user} password={password}"

    def get_conn(self):
        return psycopg.connect(self.db_conninfo, autocommit=False)

    def _compute_row_hash(self, record: dict) -> str:
        """Compute a hash for change detection."""
        serialized = json.dumps(record, sort_keys=True, default=str)
        return hashlib.md5(serialized.encode()).hexdigest()

    def sync_audience(self, audience_config: dict):
        """Sync a single audience to all its destinations."""
        audience_id = audience_config["id"]
        audience_name = audience_config["name"]
        filter_sql = audience_config["filter"]

        source = self.config["source"]
        source_table = f"{source['schema']}.{source['table']}"
        identity_table = source["identity_join"]["table"]
        identity_key = source["identity_join"]["key"]
        mobile_field = source["identity_join"]["mobile_field"]
        email_field = source["identity_join"]["email_field"]

        # Build query
        query = f"""
            SELECT
                ba.*,
                ip.{mobile_field} AS mobile,
                ip.{email_field} AS email
            FROM {source_table} ba
            JOIN {identity_table} ip ON ip.{identity_key} = ba.{source['primary_key']}
            WHERE {filter_sql}
        """

        logger.info(f"Syncing audience: {audience_name} ({audience_id})")
        conn = self.get_conn()
        try:
            with conn.cursor(row_factory=dict_row) as cur:
                cur.execute(query)
                records = [dict(r) for r in cur.fetchall()]

            logger.info(f"  Found {len(records)} records matching audience filter")

            # Convert datetime objects to strings for serialization
            for record in records:
                for k, v in record.items():
                    if isinstance(v, datetime):
                        record[k] = v.isoformat()

            # Sync to each destination
            for dest_config in audience_config.get("destinations", []):
                dest_type = dest_config["type"]
                if dest_type not in ADAPTERS:
                    logger.error(f"  Unknown destination type: {dest_type}")
                    continue

                adapter = ADAPTERS[dest_type](dest_config)
                logger.info(f"  Syncing to {adapter.name()}")
                result = adapter.sync_batch(records, audience_id)

                # Log the sync
                self._log_sync(conn, audience_id, result)

                logger.info(
                    f"  Result: {result['synced']} synced, {result['failed']} failed"
                )

        finally:
            conn.close()

    def sync_all_audiences(self):
        """Sync all configured audiences."""
        audiences = self.config.get("audiences", [])
        logger.info(f"Starting reverse ETL sync for {len(audiences)} audiences")

        for audience in audiences:
            try:
                self.sync_audience(audience)
            except Exception as e:
                logger.error(f"Failed to sync audience {audience['id']}: {e}", exc_info=True)

        logger.info("Reverse ETL sync complete")

    def _log_sync(self, conn, audience_id: str, result: dict):
        """Record sync result in audit log."""
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO reverse_etl.audience_sync_log
                        (audience_id, sync_started_at, sync_completed_at,
                         records_synced, records_failed, status, error_message)
                    VALUES (%s, NOW(), NOW(), %s, %s, %s, %s)
                """, (
                    audience_id,
                    result["synced"],
                    result["failed"],
                    "SUCCESS" if result["failed"] == 0 else "PARTIAL",
                    "; ".join(result["errors"])[:1000] if result["errors"] else None,
                ))
            conn.commit()
        except Exception:
            # Don't fail the sync if logging fails
            conn.rollback()

    def register_audiences_in_db(self):
        """Register audience definitions in the database."""
        conn = self.get_conn()
        try:
            with conn.cursor() as cur:
                for audience in self.config.get("audiences", []):
                    cur.execute("""
                        INSERT INTO reverse_etl.audience_definitions
                            (audience_id, audience_name, description, filter_sql,
                             destination_type, destination_config, is_active)
                        VALUES (%s, %s, %s, %s, %s, %s, TRUE)
                        ON CONFLICT (audience_id) DO UPDATE SET
                            audience_name = EXCLUDED.audience_name,
                            description = EXCLUDED.description,
                            filter_sql = EXCLUDED.filter_sql,
                            destination_type = EXCLUDED.destination_type,
                            destination_config = EXCLUDED.destination_config,
                            updated_at = NOW()
                    """, (
                        audience["id"],
                        audience["name"],
                        audience.get("description", ""),
                        audience["filter"],
                        audience["destinations"][0]["type"],
                        json.dumps(audience["destinations"]),
                    ))
            conn.commit()
            logger.info(f"Registered {len(self.config.get('audiences', []))} audiences in DB")
        finally:
            conn.close()

    def register_attribute_mappings(self):
        """Register attribute sync mappings in the database."""
        conn = self.get_conn()
        try:
            with conn.cursor() as cur:
                source = self.config["source"]
                for mapping in self.config.get("attribute_mappings", []):
                    cur.execute("""
                        INSERT INTO reverse_etl.attribute_sync_config
                            (attribute_name, source_table, source_column,
                             destination_type, destination_field, transform_expr, is_active)
                        VALUES (%s, %s, %s, 'ALL', %s, %s, TRUE)
                        ON CONFLICT DO NOTHING
                    """, (
                        mapping["source_column"],
                        f"{source['schema']}.{source['table']}",
                        mapping["source_column"],
                        mapping["destination_field"],
                        mapping.get("transform"),
                    ))
            conn.commit()
            logger.info(f"Registered {len(self.config.get('attribute_mappings', []))} attribute mappings")
        finally:
            conn.close()


# ============================================================================
# CLI Entry Point
# ============================================================================

def main():
    import argparse

    parser = argparse.ArgumentParser(description="Spencer's CDP Reverse ETL Engine")
    parser.add_argument("command", choices=["sync", "register", "sync-one"],
                        help="Command to run")
    parser.add_argument("--audience", help="Audience ID for sync-one command")
    parser.add_argument("--config", help="Path to config.yml", default=None)
    args = parser.parse_args()

    engine = SyncEngine(config_path=args.config)

    if args.command == "register":
        engine.register_audiences_in_db()
        engine.register_attribute_mappings()
    elif args.command == "sync":
        engine.sync_all_audiences()
    elif args.command == "sync-one":
        if not args.audience:
            parser.error("--audience is required for sync-one")
        audiences = engine.config.get("audiences", [])
        target = next((a for a in audiences if a["id"] == args.audience), None)
        if not target:
            parser.error(f"Audience '{args.audience}' not found in config")
        engine.sync_audience(target)


if __name__ == "__main__":
    main()

"""
CESC UNIFY360 - Reverse ETL Phase 1 (POC / TEST) sync engine
============================================================
Flow:
    silver_gold.cesc_customer_360
      -> explicit 18-column SELECT + demo audience filter + ORDER BY + LIMIT
      -> CSV file
      -> audit row in cesc_reverse_etl.sync_log_test

Notes:
  - Read-only against Gold. No identity join, no identity resolution.
  - cesc_entity_id is the sync/upsert identity key.
  - NULLs are preserved (written as blank CSV cells). No defaults are invented.
  - Deterministic filtered full sync. No CDC / incremental logic.
  - Separate from Spencer's dwh/reverse_etl/. Nothing is imported from there.

Usage:
    python cesc_sync_engine_test.py sync
    python cesc_sync_engine_test.py sync --dry-run
    python cesc_sync_engine_test.py sync --config path/to/config_test.yml
"""

import argparse
import csv
import logging
import os
import re
import sys
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path

import psycopg
from psycopg import sql
import yaml

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("cesc_reverse_etl_test")

IDENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
DEFAULT_CONFIG = Path(__file__).parent / "config_test.yml"


# ----------------------------------------------------------------------------
# Config
# ----------------------------------------------------------------------------

def _check_ident(value, what):
    if not isinstance(value, str) or not IDENT_RE.match(value):
        raise ValueError(f"Invalid SQL identifier for {what}: {value!r}")
    return value


def load_config(path: Path) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)
    validate_config(cfg)
    return cfg


def validate_config(cfg: dict) -> None:
    src = cfg["source"]
    _check_ident(src["schema"], "source.schema")
    _check_ident(src["table"], "source.table")
    _check_ident(src["primary_key"], "source.primary_key")

    contract = cfg["contract"]
    cols = [c["source_column"] for c in contract]
    outs = [c["outbound_field"] for c in contract]
    for c in cols:
        _check_ident(c, "contract.source_column")
    if len(set(cols)) != len(cols):
        raise ValueError("Duplicate source_column in contract")
    if len(set(outs)) != len(outs):
        raise ValueError("Duplicate outbound_field in contract")
    if len(cols) != cfg["contract_size"]:
        raise ValueError(f"Contract has {len(cols)} fields; expected {cfg['contract_size']}")
    if src["primary_key"] not in cols:
        raise ValueError("primary_key must be part of the outbound contract")

    aud = cfg["audience"]
    _check_ident(aud["order_by"], "audience.order_by")
    if not isinstance(aud["limit"], int) or aud["limit"] <= 0:
        raise ValueError("audience.limit must be a positive integer")
    if not str(aud["filter"]).strip():
        raise ValueError("audience.filter must not be empty")

    if cfg["destination"]["type"] != "csv":
        raise ValueError("Phase 1 supports only the csv destination")

    _check_ident(cfg["audit"]["schema"], "audit.schema")
    _check_ident(cfg["audit"]["table"], "audit.table")


def build_conninfo(db: dict) -> str:
    host = os.environ.get("PG_HOST", str(db.get("host", "localhost")))
    port = os.environ.get("PG_PORT", str(db.get("port", 5432)))
    dbname = os.environ.get("PG_DB", str(db.get("dbname", "cdp_meta")))
    user = os.environ.get("PG_USER", str(db.get("user", "cdp")))
    parts = [f"host={host}", f"port={port}", f"dbname={dbname}", f"user={user}"]
    password = os.environ.get("PG_PASSWORD")
    if password:
        parts.append(f"password={password}")
    return " ".join(parts)


# ----------------------------------------------------------------------------
# Query / serialization
# ----------------------------------------------------------------------------

def build_query(cfg: dict) -> sql.Composed:
    """Explicit-column SELECT. The filter text comes from the trusted config file."""
    src = cfg["source"]
    aud = cfg["audience"]
    columns = sql.SQL(", ").join(sql.Identifier(c["source_column"]) for c in cfg["contract"])
    return sql.SQL("SELECT {cols} FROM {schema}.{table} WHERE {filter} ORDER BY {order_by} LIMIT {limit}").format(
        cols=columns,
        schema=sql.Identifier(src["schema"]),
        table=sql.Identifier(src["table"]),
        filter=sql.SQL(aud["filter"]),
        order_by=sql.Identifier(aud["order_by"]),
        limit=sql.Literal(aud["limit"]),
    )


def serialize_value(v):
    """Convert a PostgreSQL value to a CSV-safe string. NULL -> empty string."""
    if v is None:
        return ""
    if isinstance(v, Decimal):
        return format(v, "f")          # plain notation, no scientific format
    if isinstance(v, (datetime, date)):
        return v.isoformat()
    if isinstance(v, bool):
        return "true" if v else "false"
    return str(v)


def write_csv(rows, cfg: dict, config_dir: Path) -> Path:
    out_dir = Path(cfg["destination"]["output_dir"])
    if not out_dir.is_absolute():
        out_dir = config_dir / out_dir
    out_dir.mkdir(parents=True, exist_ok=True)

    # Deterministic filename: same audience -> same file, overwritten each run.
    final_path = out_dir / f"{cfg['audience']['id']}.csv"
    tmp_path = out_dir / f"{cfg['audience']['id']}.csv.tmp"

    header = [c["outbound_field"] for c in cfg["contract"]]
    with open(tmp_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(header)
        for row in rows:
            writer.writerow([serialize_value(v) for v in row])
    os.replace(tmp_path, final_path)   # avoid leaving a half-written CSV
    return final_path


# ----------------------------------------------------------------------------
# Audit
# ----------------------------------------------------------------------------

def _audit_table(cfg: dict) -> sql.Composed:
    return sql.SQL("{}.{}").format(sql.Identifier(cfg["audit"]["schema"]),
                                   sql.Identifier(cfg["audit"]["table"]))


def audit_start(conn, cfg: dict, started_at: datetime) -> int:
    """Insert a RUNNING row. If this fails the exception propagates and the sync aborts."""
    src = cfg["source"]
    aud = cfg["audience"]
    query = sql.SQL("""
        INSERT INTO {tbl}
            (audience_id, source_table, key_field, destination_type,
             filter_sql, row_limit, status, sync_started_at)
        VALUES (%s, %s, %s, %s, %s, %s, 'RUNNING', %s)
        RETURNING sync_id
    """).format(tbl=_audit_table(cfg))
    with conn.cursor() as cur:
        cur.execute(query, (
            aud["id"],
            f"{src['schema']}.{src['table']}",
            src["primary_key"],
            cfg["destination"]["type"],
            aud["filter"],
            aud["limit"],
            started_at,
        ))
        sync_id = cur.fetchone()[0]
    conn.commit()
    return sync_id


def audit_finish(conn, cfg: dict, sync_id: int, status: str, selected: int, written: int,
                 output_path, error_message=None) -> None:
    query = sql.SQL("""
        UPDATE {tbl}
           SET status = %s,
               records_selected = %s,
               records_written = %s,
               output_path = %s,
               error_message = %s,
               sync_completed_at = %s
         WHERE sync_id = %s
    """).format(tbl=_audit_table(cfg))
    with conn.cursor() as cur:
        cur.execute(query, (
            status, selected, written,
            str(output_path) if output_path else None,
            error_message[:2000] if error_message else None,
            datetime.now(timezone.utc),
            sync_id,
        ))
    conn.commit()


def _mark_failed_visibly(conn, cfg, sync_id, error_message):
    """Try to record FAILED. If that also fails, say so loudly (never silent)."""
    try:
        conn.rollback()
        audit_finish(conn, cfg, sync_id, "FAILED", 0, 0, None, error_message)
    except Exception as audit_exc:
        logger.error("AUDIT UPDATE FAILED for sync_id=%s: %s (original error: %s)",
                     sync_id, audit_exc, error_message)


# ----------------------------------------------------------------------------
# Sync
# ----------------------------------------------------------------------------

def run_sync(cfg: dict, config_dir: Path, dry_run: bool = False) -> int:
    aud = cfg["audience"]
    logger.info("Audience: %s (%s)", aud["name"], aud["id"])
    logger.info("Source: %s.%s | key: %s | contract fields: %d",
                cfg["source"]["schema"], cfg["source"]["table"],
                cfg["source"]["primary_key"], len(cfg["contract"]))

    conn = psycopg.connect(build_conninfo(cfg["database"]), autocommit=False)
    try:
        query = build_query(cfg)
        started_at = datetime.now(timezone.utc)
        sync_id = None

        if dry_run:
            logger.info("DRY RUN: no CSV will be written and no audit row will be inserted")
        else:
            sync_id = audit_start(conn, cfg, started_at)   # raises if audit is broken
            logger.info("Audit row opened: sync_id=%s", sync_id)

        try:
            with conn.cursor() as cur:
                cur.execute(query)
                rows = cur.fetchall()
            logger.info("Selected %d rows", len(rows))

            key_idx = [c["source_column"] for c in cfg["contract"]].index(cfg["source"]["primary_key"])
            keys = [r[key_idx] for r in rows]
            if len(set(keys)) != len(keys):
                raise ValueError("Duplicate cesc_entity_id in result. Gold grain violated")
            if any(k is None for k in keys):
                raise ValueError("NULL cesc_entity_id in result")

            if dry_run:
                logger.info("DRY RUN complete: %d rows would be written", len(rows))
                return 0

            out_path = write_csv(rows, cfg, config_dir)
            logger.info("Wrote %d rows to %s", len(rows), out_path)
        except Exception as exc:
            if sync_id is not None:
                _mark_failed_visibly(conn, cfg, sync_id, f"{type(exc).__name__}: {exc}")
            raise

        # If this final audit update fails, it raises. The failure is visible.
        audit_finish(conn, cfg, sync_id, "SUCCESS", len(rows), len(rows), out_path)
        logger.info("Audit row closed: sync_id=%s status=SUCCESS", sync_id)
        return 0
    finally:
        conn.close()


def main() -> int:
    parser = argparse.ArgumentParser(description="CESC Reverse ETL Phase 1 (POC / TEST)")
    parser.add_argument("command", choices=["sync"])
    parser.add_argument("--config", default=str(DEFAULT_CONFIG))
    parser.add_argument("--dry-run", action="store_true",
                        help="Run the query and checks only. No CSV, no audit row.")
    args = parser.parse_args()

    config_path = Path(args.config).resolve()
    cfg = load_config(config_path)
    try:
        return run_sync(cfg, config_path.parent, dry_run=args.dry_run)
    except Exception as exc:
        logger.error("Sync FAILED: %s: %s", type(exc).__name__, exc)
        return 1


if __name__ == "__main__":
    sys.exit(main())

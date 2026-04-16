#!/usr/bin/env bash
# ============================================================================
# Spencer's CDP - Full Pipeline Runner
# ============================================================================
# Usage: ./run_pipeline.sh [step]
#   Steps: all | ddl | ingest | dbt | reverse-etl
# ============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
export PG_HOST="${PG_HOST:-localhost}"
export PG_PORT="${PG_PORT:-5432}"
export PG_DB="${PG_DB:-cdp_meta}"
export PG_USER="${PG_USER:-cdp}"
export PG_PASSWORD="${PG_PASSWORD:-cdp}"

STEP="${1:-all}"

log() { echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*"; }

wait_for_pg() {
    log "Waiting for PostgreSQL at $PG_HOST:$PG_PORT..."
    for i in $(seq 1 30); do
        if PGPASSWORD=$PG_PASSWORD psql -h "$PG_HOST" -p "$PG_PORT" -U "$PG_USER" -d "$PG_DB" -c "SELECT 1" > /dev/null 2>&1; then
            log "PostgreSQL is ready"
            return 0
        fi
        sleep 2
    done
    log "ERROR: PostgreSQL not available after 60s"
    exit 1
}

run_ddl() {
    log "=== Step 1: Creating schemas and tables ==="
    for f in "$SCRIPT_DIR/sql/"*.sql; do
        log "  Executing: $(basename "$f")"
        PGPASSWORD=$PG_PASSWORD psql -h "$PG_HOST" -p "$PG_PORT" -U "$PG_USER" -d "$PG_DB" -f "$f" 2>&1 | grep -v "^$" || true
    done
    log "DDL complete"
}

run_ingest() {
    log "=== Step 2: Ingesting sample data into bronze layer ==="
    cd "$SCRIPT_DIR/ingestion"
    pip install -q -r requirements.txt 2>/dev/null || true
    python ingest_spencers.py
    log "Ingestion complete"
}

run_dbt() {
    log "=== Step 3: Running dbt transformations ==="
    cd "$SCRIPT_DIR/dbt_spencers"
    pip install -q dbt-postgres 2>/dev/null || true

    # Install dbt packages
    dbt deps --profiles-dir . 2>&1 || true

    # Run staging -> intermediate -> marts
    log "  Running dbt models..."
    dbt run --profiles-dir . --full-refresh 2>&1

    # Run tests
    log "  Running dbt tests..."
    dbt test --profiles-dir . 2>&1 || true

    log "dbt transformations complete"
}

run_reverse_etl() {
    log "=== Step 4: Registering reverse ETL audiences ==="
    cd "$SCRIPT_DIR/reverse_etl"
    pip install -q -r requirements.txt 2>/dev/null || true

    # Register audience definitions and attribute mappings
    python sync_engine.py register

    # Run sync (will export CSVs for file-based destinations)
    log "  Running audience sync..."
    python sync_engine.py sync 2>&1 || true

    log "Reverse ETL setup complete"
}

# Main execution
wait_for_pg

case "$STEP" in
    all)
        run_ddl
        run_ingest
        run_dbt
        run_reverse_etl
        ;;
    ddl) run_ddl ;;
    ingest) run_ingest ;;
    dbt) run_dbt ;;
    reverse-etl) run_reverse_etl ;;
    *)
        echo "Usage: $0 {all|ddl|ingest|dbt|reverse-etl}"
        exit 1
        ;;
esac

log "=== Pipeline complete ==="

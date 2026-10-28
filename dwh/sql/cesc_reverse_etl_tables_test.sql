-- ============================================================================
-- CESC UNIFY360 - Reverse ETL Phase 1 (POC / TEST) audit objects
-- ============================================================================
-- Separate from Spencer's schema `reverse_etl`. Touches nothing outside `cesc_reverse_etl`.
-- Idempotent: safe to re-run (no DROP, no data changes).

CREATE SCHEMA IF NOT EXISTS cesc_reverse_etl;

CREATE TABLE IF NOT EXISTS cesc_reverse_etl.sync_log_test (
    sync_id            BIGSERIAL PRIMARY KEY,
    audience_id        TEXT        NOT NULL,
    source_table       TEXT        NOT NULL,
    key_field          TEXT        NOT NULL,
    destination_type   TEXT        NOT NULL,
    output_path        TEXT,
    filter_sql         TEXT,
    row_limit          INT,
    records_selected   INT         NOT NULL DEFAULT 0,
    records_written    INT         NOT NULL DEFAULT 0,
    status             TEXT        NOT NULL
                       CHECK (status IN ('RUNNING', 'SUCCESS', 'FAILED')),
    error_message      TEXT,
    sync_started_at    TIMESTAMPTZ NOT NULL,
    sync_completed_at  TIMESTAMPTZ,
    created_at         TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

COMMENT ON TABLE cesc_reverse_etl.sync_log_test IS
    'CESC Reverse ETL Phase 1 POC audit log. One row per sync run.';

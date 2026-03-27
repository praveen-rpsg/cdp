-- ============================================================================
-- Reverse ETL Layer: Behavioral & derived attributes, audience syncs
-- ============================================================================

-- Customer behavioral attributes (computed/streaming)
CREATE TABLE IF NOT EXISTS reverse_etl.customer_behavioral_attributes (
    customer_id         TEXT PRIMARY KEY,
    -- Recency
    last_purchase_date  DATE,
    days_since_last_purchase INT,
    last_store_visited  TEXT,
    -- Frequency
    total_transactions  INT DEFAULT 0,
    transactions_last_30d INT DEFAULT 0,
    transactions_last_90d INT DEFAULT 0,
    avg_transactions_per_month NUMERIC DEFAULT 0,
    -- Monetary
    total_spend         NUMERIC DEFAULT 0,
    spend_last_30d      NUMERIC DEFAULT 0,
    spend_last_90d      NUMERIC DEFAULT 0,
    avg_order_value     NUMERIC DEFAULT 0,
    max_order_value     NUMERIC DEFAULT 0,
    -- Transaction detail
    total_line_items    INT DEFAULT 0,
    avg_basket_size     NUMERIC DEFAULT 0,
    total_discount      NUMERIC DEFAULT 0,
    distinct_articles   INT DEFAULT 0,
    distinct_categories INT DEFAULT 0,
    -- RFM Scores
    rfm_recency_score   INT,    -- 1-5
    rfm_frequency_score INT,    -- 1-5
    rfm_monetary_score  INT,    -- 1-5
    rfm_segment         TEXT,   -- 'Champions', 'Loyal', 'At Risk', 'Lost', etc.
    -- Category preferences
    top_category_1      TEXT,
    top_category_2      TEXT,
    top_category_3      TEXT,
    preferred_brand_1   TEXT,
    preferred_brand_2   TEXT,
    -- Channel preference
    primary_channel     TEXT,   -- 'ONLINE', 'OFFLINE'
    channel_mix_online_pct NUMERIC DEFAULT 0,
    -- Store affinity
    home_store_code     TEXT,
    store_count         INT DEFAULT 0,
    -- NPS
    latest_nps_score    INT,
    nps_category        TEXT,
    avg_nps_score       NUMERIC,
    -- Engagement
    has_given_feedback  BOOLEAN DEFAULT FALSE,
    feedback_count      INT DEFAULT 0,
    has_used_promo      BOOLEAN DEFAULT FALSE,
    promo_usage_count   INT DEFAULT 0,
    total_cashback      NUMERIC DEFAULT 0,
    -- Lifecycle
    customer_tenure_days INT,
    lifecycle_stage     TEXT,   -- 'New', 'Active', 'Lapsed', 'Churned'
    churn_risk_score    NUMERIC,
    -- Timestamps
    computed_at         TIMESTAMPTZ DEFAULT NOW(),
    updated_at          TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_cba_rfm ON reverse_etl.customer_behavioral_attributes(rfm_segment);
CREATE INDEX IF NOT EXISTS idx_cba_lifecycle ON reverse_etl.customer_behavioral_attributes(lifecycle_stage);
CREATE INDEX IF NOT EXISTS idx_cba_store ON reverse_etl.customer_behavioral_attributes(home_store_code);

-- Audience sync definitions (what to push to downstream systems)
CREATE TABLE IF NOT EXISTS reverse_etl.audience_definitions (
    audience_id         TEXT PRIMARY KEY,
    audience_name       TEXT NOT NULL,
    description         TEXT,
    filter_sql          TEXT NOT NULL,
    destination_type    TEXT NOT NULL,  -- 'CLEVERTAP', 'MOENGAGE', 'BRAZE', 'WEBHOOK', 'CSV'
    destination_config  JSONB,
    schedule_cron       TEXT,
    is_active           BOOLEAN DEFAULT TRUE,
    created_at          TIMESTAMPTZ DEFAULT NOW(),
    updated_at          TIMESTAMPTZ DEFAULT NOW()
);

-- Audience sync history
CREATE TABLE IF NOT EXISTS reverse_etl.audience_sync_log (
    sync_id             BIGSERIAL PRIMARY KEY,
    audience_id         TEXT REFERENCES reverse_etl.audience_definitions(audience_id),
    sync_started_at     TIMESTAMPTZ,
    sync_completed_at   TIMESTAMPTZ,
    records_synced      INT DEFAULT 0,
    records_failed      INT DEFAULT 0,
    status              TEXT DEFAULT 'PENDING', -- 'PENDING', 'RUNNING', 'SUCCESS', 'FAILED'
    error_message       TEXT,
    created_at          TIMESTAMPTZ DEFAULT NOW()
);

-- Attribute sync configuration (reverse ETL column-level sync)
CREATE TABLE IF NOT EXISTS reverse_etl.attribute_sync_config (
    config_id           BIGSERIAL PRIMARY KEY,
    attribute_name      TEXT NOT NULL,
    source_table        TEXT NOT NULL,
    source_column       TEXT NOT NULL,
    destination_type    TEXT NOT NULL,
    destination_field   TEXT NOT NULL,
    transform_expr      TEXT,   -- optional SQL transform
    sync_mode           TEXT DEFAULT 'UPSERT',  -- 'UPSERT', 'INSERT', 'MIRROR'
    is_active           BOOLEAN DEFAULT TRUE,
    created_at          TIMESTAMPTZ DEFAULT NOW()
);

-- Populate date dimension
INSERT INTO silver.s_dim_date (date_key, year, quarter, month, month_name, week_of_year, day_of_week, day_name, is_weekend, fiscal_year, fiscal_quarter)
SELECT
    d::DATE,
    EXTRACT(YEAR FROM d)::INT,
    EXTRACT(QUARTER FROM d)::INT,
    EXTRACT(MONTH FROM d)::INT,
    TO_CHAR(d, 'Month'),
    EXTRACT(WEEK FROM d)::INT,
    EXTRACT(DOW FROM d)::INT,
    TO_CHAR(d, 'Day'),
    EXTRACT(DOW FROM d) IN (0, 6),
    CASE WHEN EXTRACT(MONTH FROM d) >= 4 THEN EXTRACT(YEAR FROM d)::INT ELSE EXTRACT(YEAR FROM d)::INT - 1 END,
    CASE
        WHEN EXTRACT(MONTH FROM d) BETWEEN 4 AND 6 THEN 1
        WHEN EXTRACT(MONTH FROM d) BETWEEN 7 AND 9 THEN 2
        WHEN EXTRACT(MONTH FROM d) BETWEEN 10 AND 12 THEN 3
        ELSE 4
    END
FROM generate_series('2018-01-01'::DATE, '2026-12-31'::DATE, '1 day'::INTERVAL) AS d
ON CONFLICT (date_key) DO NOTHING;

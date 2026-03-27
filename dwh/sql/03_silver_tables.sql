-- ============================================================================
-- Silver Layer: Conformed Dimensions & Facts (from Data Model document)
-- ============================================================================

-- ---------------------------------------------------------------------------
-- DIMENSION TABLES
-- ---------------------------------------------------------------------------

-- S_LOCATION_MASTER_HISTORY: Historical location records with SCD Type 2
CREATE TABLE IF NOT EXISTS silver.s_location_master_history (
    location_sk         BIGSERIAL PRIMARY KEY,
    store_code          TEXT NOT NULL,
    store_name          TEXT,
    store_format        TEXT,
    store_zone          TEXT,
    store_business_region TEXT,
    store_region_code   TEXT,
    store_state         TEXT,
    store_city_code     TEXT,
    store_city_description TEXT,
    store_pincode       TEXT,
    store_address       TEXT,
    status              TEXT,
    store_opening_date  DATE,
    store_closing_date  DATE,
    is_latest           BOOLEAN DEFAULT TRUE,
    valid_from          DATE NOT NULL DEFAULT CURRENT_DATE,
    valid_to            DATE DEFAULT '9999-12-31',
    created_at          TIMESTAMPTZ DEFAULT NOW(),
    updated_at          TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_loc_hist_store ON silver.s_location_master_history(store_code);
CREATE INDEX IF NOT EXISTS idx_loc_hist_latest ON silver.s_location_master_history(is_latest) WHERE is_latest = TRUE;

-- S_DIM_LOCATION_MASTER: Current view of locations (is_latest = 1)
CREATE OR REPLACE VIEW silver.s_dim_location_master AS
SELECT
    location_sk,
    store_code,
    store_name,
    store_format,
    store_zone,
    store_business_region,
    store_region_code,
    store_state,
    store_city_code,
    store_city_description,
    store_pincode,
    store_address,
    status,
    store_opening_date,
    store_closing_date
FROM silver.s_location_master_history
WHERE is_latest = TRUE;

-- S_OFFLINE_ARTICLE_MASTER: Offline (ZABM) article master
CREATE TABLE IF NOT EXISTS silver.s_offline_article_master (
    article_sk          BIGSERIAL PRIMARY KEY,
    article_code        TEXT NOT NULL UNIQUE,
    article_description TEXT,
    segment_code        TEXT,
    segment_desc        TEXT,
    family_code         TEXT,
    family_desc         TEXT,
    class_code          TEXT,
    class_desc          TEXT,
    brick_code          TEXT,
    brick_desc          TEXT,
    manufacturer_code   TEXT,
    manufacturer_desc   TEXT,
    brand_code          TEXT,
    brand_desc          TEXT,
    category_code       TEXT,
    category_desc       TEXT,
    subcategory_code    TEXT,
    subcategory_desc    TEXT,
    base_uom            TEXT,
    ean_upc             TEXT,
    hsn_code            TEXT,
    net_weight          NUMERIC,
    weight_uom          TEXT,
    status              TEXT,
    article_type        TEXT,
    created_on          DATE,
    last_changed        DATE,
    created_at          TIMESTAMPTZ DEFAULT NOW(),
    updated_at          TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_offline_art_family ON silver.s_offline_article_master(family_code);
CREATE INDEX IF NOT EXISTS idx_offline_art_class ON silver.s_offline_article_master(class_code);
CREATE INDEX IF NOT EXISTS idx_offline_art_brick ON silver.s_offline_article_master(brick_code);
CREATE INDEX IF NOT EXISTS idx_offline_art_category ON silver.s_offline_article_master(category_code);

-- S_ONLINE_ARTICLE_MASTER: Online (ecom) product master
CREATE TABLE IF NOT EXISTS silver.s_online_article_master (
    product_sk          BIGSERIAL PRIMARY KEY,
    sku                 TEXT NOT NULL UNIQUE,
    name                TEXT,
    product_type        TEXT,
    visibility          TEXT,
    category_id         TEXT,
    sub_category_id     TEXT,
    sub_sub_category_id TEXT,
    store_codes         TEXT[],
    quantity            TEXT,
    unit                TEXT,
    brand               TEXT,
    food_type           TEXT,
    package_type        TEXT,
    config_type         TEXT,
    country_of_origin   TEXT,
    deep_url            TEXT,
    is_liquor           BOOLEAN DEFAULT FALSE,
    status              TEXT,
    is_active           BOOLEAN DEFAULT TRUE,
    description         TEXT,
    level               TEXT,
    created_at_source   DATE,
    updated_at_source   DATE,
    created_at          TIMESTAMPTZ DEFAULT NOW(),
    updated_at          TIMESTAMPTZ DEFAULT NOW()
);

-- S_DIM_ARTICLE_MASTER: Unified article dimension (combines offline + online)
CREATE OR REPLACE VIEW silver.s_dim_article_master AS
SELECT
    'OFFLINE' AS source_channel,
    article_code AS article_id,
    article_description AS article_name,
    segment_code,
    segment_desc,
    family_code,
    family_desc,
    class_code,
    class_desc,
    brick_code,
    brick_desc,
    category_code,
    category_desc,
    subcategory_code,
    subcategory_desc,
    brand_code,
    brand_desc,
    base_uom,
    status,
    updated_at
FROM silver.s_offline_article_master
UNION ALL
SELECT
    'ONLINE' AS source_channel,
    sku AS article_id,
    name AS article_name,
    NULL AS segment_code,
    NULL AS segment_desc,
    NULL AS family_code,
    NULL AS family_desc,
    NULL AS class_code,
    NULL AS class_desc,
    NULL AS brick_code,
    NULL AS brick_desc,
    category_id AS category_code,
    NULL AS category_desc,
    sub_category_id AS subcategory_code,
    NULL AS subcategory_desc,
    brand AS brand_code,
    brand AS brand_desc,
    unit AS base_uom,
    status,
    updated_at
FROM silver.s_online_article_master;

-- S_DIM_ECOM_CATEGORY: E-commerce category hierarchy
CREATE TABLE IF NOT EXISTS silver.s_dim_ecom_category (
    category_sk         BIGSERIAL PRIMARY KEY,
    category_id         TEXT NOT NULL UNIQUE,
    category_name       TEXT,
    created_at          TIMESTAMPTZ DEFAULT NOW()
);

-- S_DIM_CUSTOMER: Customer dimension (core for identity resolution)
CREATE TABLE IF NOT EXISTS silver.s_dim_customer (
    customer_sk         BIGSERIAL PRIMARY KEY,
    customer_id         TEXT NOT NULL UNIQUE,  -- canonical unified ID
    mobile              TEXT,
    name                TEXT,
    address             TEXT,
    pincode             TEXT,
    email               TEXT,
    primary_store_code  TEXT,
    first_seen_at       TIMESTAMPTZ,
    last_seen_at        TIMESTAMPTZ,
    created_at          TIMESTAMPTZ DEFAULT NOW(),
    updated_at          TIMESTAMPTZ DEFAULT NOW()
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_customer_mobile ON silver.s_dim_customer(mobile) WHERE mobile IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_customer_pincode ON silver.s_dim_customer(pincode);
CREATE INDEX IF NOT EXISTS idx_customer_store ON silver.s_dim_customer(primary_store_code);

-- S_DIM_PROMOTION: Promotion dimension
CREATE TABLE IF NOT EXISTS silver.s_dim_promotion (
    promotion_sk        BIGSERIAL PRIMARY KEY,
    promotion_id        TEXT NOT NULL,
    deal_code           TEXT,
    discount_type       TEXT,
    article_code        TEXT,
    article_description TEXT,
    uom                 TEXT,
    brand               TEXT,
    communication       TEXT,
    offer_text          TEXT,
    sp_price            NUMERIC,
    percent_off         NUMERIC,
    rs_off              NUMERIC,
    valid_from          DATE,
    valid_to            DATE,
    created_at          TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_promo_article ON silver.s_dim_promotion(article_code);
CREATE INDEX IF NOT EXISTS idx_promo_dates ON silver.s_dim_promotion(valid_from, valid_to);

-- S_DIM_FESTIVAL: Festival/campaign calendar dimension
CREATE TABLE IF NOT EXISTS silver.s_dim_festival (
    festival_sk         BIGSERIAL PRIMARY KEY,
    year                INT,
    campaign_name       TEXT NOT NULL,
    start_date          DATE,
    end_date            DATE,
    geography           TEXT,
    business_region     TEXT,
    zone                TEXT,
    brand               TEXT,
    created_at          TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_festival_dates ON silver.s_dim_festival(start_date, end_date);

-- S_DIM_DATE: Date dimension for time-based analysis
CREATE TABLE IF NOT EXISTS silver.s_dim_date (
    date_key            DATE PRIMARY KEY,
    year                INT,
    quarter             INT,
    month               INT,
    month_name          TEXT,
    week_of_year        INT,
    day_of_week         INT,
    day_name            TEXT,
    is_weekend          BOOLEAN,
    fiscal_year         INT,
    fiscal_quarter      INT
);

-- ---------------------------------------------------------------------------
-- FACT TABLES
-- ---------------------------------------------------------------------------

-- S_FACT_POS_DAILY_SALES: Daily POS sales fact (core sales fact)
CREATE TABLE IF NOT EXISTS silver.s_fact_pos_daily_sales (
    sale_sk             BIGSERIAL,
    bill_id             TEXT NOT NULL,
    line_item_id        TEXT,
    bill_date           DATE NOT NULL,
    store_code          TEXT NOT NULL,
    article_code        TEXT,
    customer_id         TEXT,
    mobile              TEXT,
    quantity            NUMERIC,
    gross_amount        NUMERIC,
    discount_amount     NUMERIC,
    net_amount          NUMERIC,
    tax_amount          NUMERIC,
    is_bulk_sale        BOOLEAN DEFAULT FALSE,
    is_sale_sign        BOOLEAN DEFAULT FALSE,
    sales_channel       TEXT DEFAULT 'OFFLINE',
    promotion_id        TEXT,
    partition_date      DATE,
    created_at          TIMESTAMPTZ DEFAULT NOW()
) PARTITION BY RANGE (bill_date);

-- Create monthly partitions for sample data period
CREATE TABLE IF NOT EXISTS silver.s_fact_pos_daily_sales_2024_09
    PARTITION OF silver.s_fact_pos_daily_sales
    FOR VALUES FROM ('2024-09-01') TO ('2024-10-01');

CREATE TABLE IF NOT EXISTS silver.s_fact_pos_daily_sales_2024_10
    PARTITION OF silver.s_fact_pos_daily_sales
    FOR VALUES FROM ('2024-10-01') TO ('2024-11-01');

CREATE TABLE IF NOT EXISTS silver.s_fact_pos_daily_sales_default
    PARTITION OF silver.s_fact_pos_daily_sales DEFAULT;

CREATE INDEX IF NOT EXISTS idx_pos_sales_store ON silver.s_fact_pos_daily_sales(store_code);
CREATE INDEX IF NOT EXISTS idx_pos_sales_article ON silver.s_fact_pos_daily_sales(article_code);
CREATE INDEX IF NOT EXISTS idx_pos_sales_customer ON silver.s_fact_pos_daily_sales(customer_id);
CREATE INDEX IF NOT EXISTS idx_pos_sales_date ON silver.s_fact_pos_daily_sales(bill_date);

-- S_FACT_TARGET_SALES: Sales targets by store/channel/category
CREATE TABLE IF NOT EXISTS silver.s_fact_target_sales (
    target_sk           BIGSERIAL PRIMARY KEY,
    store_code          TEXT NOT NULL,
    target_month        INT,
    target_year         INT,
    sales_channel       TEXT,
    category_code       TEXT,
    family_code         TEXT,
    target_amount       NUMERIC,
    created_at          TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_target_store ON silver.s_fact_target_sales(store_code);
CREATE INDEX IF NOT EXISTS idx_target_period ON silver.s_fact_target_sales(target_year, target_month);

-- S_FACT_SUMMARY_SALES: Summarized sales fact
CREATE TABLE IF NOT EXISTS silver.s_fact_summary_sales (
    summary_sk          BIGSERIAL,
    bill_date           DATE NOT NULL,
    store_code          TEXT NOT NULL,
    article_code        TEXT,
    sales_channel       TEXT,
    gross_sales         NUMERIC,
    line_item_count     INT,
    is_retail           BOOLEAN DEFAULT TRUE,
    is_institutional    BOOLEAN DEFAULT FALSE,
    partition_date      DATE,
    created_at          TIMESTAMPTZ DEFAULT NOW()
) PARTITION BY RANGE (bill_date);

CREATE TABLE IF NOT EXISTS silver.s_fact_summary_sales_2024_09
    PARTITION OF silver.s_fact_summary_sales
    FOR VALUES FROM ('2024-09-01') TO ('2024-10-01');

CREATE TABLE IF NOT EXISTS silver.s_fact_summary_sales_2024_10
    PARTITION OF silver.s_fact_summary_sales
    FOR VALUES FROM ('2024-10-01') TO ('2024-11-01');

CREATE TABLE IF NOT EXISTS silver.s_fact_summary_sales_default
    PARTITION OF silver.s_fact_summary_sales DEFAULT;

-- S_FACT_NPS_SURVEY: NPS survey responses fact
CREATE TABLE IF NOT EXISTS silver.s_fact_nps_survey (
    nps_sk              BIGSERIAL PRIMARY KEY,
    mobile              TEXT,
    customer_id         TEXT,
    store_code          TEXT NOT NULL,
    bill_date           DATE,
    cleanliness_hygiene INT,
    product_availability INT,
    quality_freshness   INT,
    value_for_money     INT,
    promotional_offers  INT,
    staff_assistance    INT,
    checkout_experience INT,
    overall_rating      INT,
    nps_score           INT GENERATED ALWAYS AS (overall_rating) STORED,
    nps_category        TEXT GENERATED ALWAYS AS (
        CASE
            WHEN overall_rating >= 9 THEN 'Promoter'
            WHEN overall_rating >= 7 THEN 'Passive'
            ELSE 'Detractor'
        END
    ) STORED,
    created_at          TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_nps_customer ON silver.s_fact_nps_survey(customer_id);
CREATE INDEX IF NOT EXISTS idx_nps_store ON silver.s_fact_nps_survey(store_code);

-- S_FACT_PROMO_CASHBACK: Promotional cashback transactions
CREATE TABLE IF NOT EXISTS silver.s_fact_promo_cashback (
    cashback_sk         BIGSERIAL PRIMARY KEY,
    mobile              TEXT,
    customer_id         TEXT,
    promo_id            TEXT NOT NULL,
    start_date          DATE,
    end_date            DATE,
    create_date         DATE,
    channel             TEXT,
    amount              NUMERIC,
    created_at          TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_cashback_customer ON silver.s_fact_promo_cashback(customer_id);
CREATE INDEX IF NOT EXISTS idx_cashback_promo ON silver.s_fact_promo_cashback(promo_id);

-- S_FACT_CUSTOMER_FEEDBACK: YVM customer feedback/complaints
CREATE TABLE IF NOT EXISTS silver.s_fact_customer_feedback (
    feedback_sk         BIGSERIAL PRIMARY KEY,
    reference_no        TEXT,
    mobile              TEXT,
    customer_id         TEXT,
    customer_name       TEXT,
    store_code          TEXT,
    store_name          TEXT,
    store_city          TEXT,
    region              TEXT,
    mode_of_feedback    TEXT,
    feedback_type       TEXT,
    feedback_class      TEXT,
    feedback_sub_class  TEXT,
    overall_satisfaction TEXT,
    feedback_text       TEXT,
    resolution          TEXT,
    resolution_notes    TEXT,
    platform_type       TEXT,
    registration_date   TIMESTAMPTZ,
    created_at          TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_feedback_customer ON silver.s_fact_customer_feedback(customer_id);
CREATE INDEX IF NOT EXISTS idx_feedback_store ON silver.s_fact_customer_feedback(store_code);

-- Monthly partitions for 2026 BILL_DELTA data
CREATE TABLE IF NOT EXISTS silver.s_fact_pos_daily_sales_2026_01
    PARTITION OF silver.s_fact_pos_daily_sales
    FOR VALUES FROM ('2026-01-01') TO ('2026-02-01');

CREATE TABLE IF NOT EXISTS silver.s_fact_pos_daily_sales_2026_02
    PARTITION OF silver.s_fact_pos_daily_sales
    FOR VALUES FROM ('2026-02-01') TO ('2026-03-01');

CREATE TABLE IF NOT EXISTS silver.s_fact_summary_sales_2026_01
    PARTITION OF silver.s_fact_summary_sales
    FOR VALUES FROM ('2026-01-01') TO ('2026-02-01');

CREATE TABLE IF NOT EXISTS silver.s_fact_summary_sales_2026_02
    PARTITION OF silver.s_fact_summary_sales
    FOR VALUES FROM ('2026-02-01') TO ('2026-03-01');

-- ---------------------------------------------------------------------------
-- BILL TRANSACTION FACT TABLES (from raw BILL_DELTA)
-- ---------------------------------------------------------------------------

-- S_FACT_BILL_TRANSACTIONS: Cleaned bill transaction fact (line-item level)
CREATE TABLE IF NOT EXISTS silver.s_fact_bill_transactions (
    bill_date               DATE NOT NULL,
    store_code              TEXT,
    store_desc              TEXT,
    till_no                 TEXT,
    bill_no                 TEXT,
    line_item_no            TEXT,
    bill_id                 TEXT,           -- composite key (store_code|till_no|bill_no|bill_date)
    condition_type          TEXT,
    promo_indicator         TEXT,
    city                    TEXT,
    city_desc               TEXT,
    store_format            TEXT,
    region                  TEXT,
    region_desc             TEXT,
    calendar_year           TEXT,
    calendar_year_week      TEXT,
    bill_start_time         TEXT,
    bill_end_time           TEXT,
    article                 TEXT,
    article_desc            TEXT,
    segment                 TEXT,
    segment_desc            TEXT,
    family                  TEXT,
    family_desc             TEXT,
    class                   TEXT,
    class_desc              TEXT,
    brick                   TEXT,
    brick_desc              TEXT,
    manufacturer            TEXT,
    manufacturer_name       TEXT,
    brand                   TEXT,
    brand_name              TEXT,
    base_unit               TEXT,
    gift_item_indicator     TEXT,
    indicator_for_bill      TEXT,
    sales_return            BOOLEAN,
    line_item_count         INT,
    liquidity_type          TEXT,
    mobile_number           TEXT,
    gross_sale_value        NUMERIC(15,2),
    total_discount          NUMERIC(15,2),
    total_mrp_value         NUMERIC(15,2),
    billed_qty              NUMERIC(12,3),
    billed_mrp              NUMERIC(15,2),
    vka0_discount           NUMERIC(12,2),
    ka04_discount           NUMERIC(12,2),
    ka02_discount           NUMERIC(12,2),
    z006_discount           NUMERIC(12,2),
    z007_discount           NUMERIC(12,2),
    zpro_discount           NUMERIC(12,2),
    zfre_discount           NUMERIC(12,2),
    zcat_discount           NUMERIC(12,2),
    zemp_discount           NUMERIC(12,2),
    zbil_discount           NUMERIC(12,2),
    weekend_flag            BOOLEAN,
    wednesday_flag          BOOLEAN,
    first_week_flag         BOOLEAN,
    last_week_flag          BOOLEAN,
    first_half_flag         BOOLEAN,
    day_of_week             TEXT,
    delivery_channel        TEXT,
    _source_file            TEXT,
    _loaded_at              TIMESTAMPTZ DEFAULT NOW()
) PARTITION BY RANGE (bill_date);

-- Monthly partitions for 2026 BILL_DELTA data
CREATE TABLE IF NOT EXISTS silver.s_fact_bill_transactions_2026_01
    PARTITION OF silver.s_fact_bill_transactions
    FOR VALUES FROM ('2026-01-01') TO ('2026-02-01');

CREATE TABLE IF NOT EXISTS silver.s_fact_bill_transactions_2026_02
    PARTITION OF silver.s_fact_bill_transactions
    FOR VALUES FROM ('2026-02-01') TO ('2026-03-01');

CREATE INDEX IF NOT EXISTS idx_bill_txn_mobile ON silver.s_fact_bill_transactions(mobile_number);
CREATE INDEX IF NOT EXISTS idx_bill_txn_store ON silver.s_fact_bill_transactions(store_code);
CREATE INDEX IF NOT EXISTS idx_bill_txn_article ON silver.s_fact_bill_transactions(article);
CREATE INDEX IF NOT EXISTS idx_bill_txn_date ON silver.s_fact_bill_transactions(bill_date);

-- S_FACT_BILL_SUMMARY: Bill-level summary (one row per bill)
CREATE TABLE IF NOT EXISTS silver.s_fact_bill_summary (
    bill_date               DATE,
    store_code              TEXT,
    store_format            TEXT,
    till_no                 TEXT,
    bill_no                 TEXT,
    bill_id                 TEXT PRIMARY KEY,
    mobile_number           TEXT,
    region                  TEXT,
    city                    TEXT,
    delivery_channel        TEXT,
    line_item_count         INT,
    gross_sale_value        NUMERIC(15,2),
    total_discount          NUMERIC(15,2),
    total_mrp_value         NUMERIC(15,2),
    bill_start_time         TEXT,
    bill_end_time           TEXT,
    billing_time_secs       INT,
    sales_return            BOOLEAN,
    promo_applied           BOOLEAN,
    gift_indicator          BOOLEAN,
    b2b_gst                 BOOLEAN,
    liquidity_indicator     BOOLEAN,
    weekend_flag            BOOLEAN,
    wednesday_flag          BOOLEAN,
    day_of_week             TEXT
);

CREATE INDEX IF NOT EXISTS idx_bill_sum_mobile ON silver.s_fact_bill_summary(mobile_number);
CREATE INDEX IF NOT EXISTS idx_bill_sum_date ON silver.s_fact_bill_summary(bill_date);
CREATE INDEX IF NOT EXISTS idx_bill_sum_store ON silver.s_fact_bill_summary(store_code);

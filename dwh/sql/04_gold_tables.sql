-- ============================================================================
-- Gold Layer: Aggregated business views (from Data Model document)
-- ============================================================================

-- G_SALES_TARGETS_ACTUALS: Targets vs actuals at family level
CREATE TABLE IF NOT EXISTS gold.g_sales_targets_actuals (
    id                  BIGSERIAL PRIMARY KEY,
    store_code          TEXT NOT NULL,
    family_code         TEXT,
    family_desc         TEXT,
    sales_channel       TEXT,
    target_month        INT,
    target_year         INT,
    target_amount       NUMERIC DEFAULT 0,
    actual_sales        NUMERIC DEFAULT 0,
    variance            NUMERIC GENERATED ALWAYS AS (actual_sales - target_amount) STORED,
    achievement_pct     NUMERIC,
    created_at          TIMESTAMPTZ DEFAULT NOW(),
    updated_at          TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_gsta_store ON gold.g_sales_targets_actuals(store_code);
CREATE INDEX IF NOT EXISTS idx_gsta_family ON gold.g_sales_targets_actuals(family_code);

-- G_TOTAL_SALES: Total sales at article level
CREATE TABLE IF NOT EXISTS gold.g_total_sales (
    id                  BIGSERIAL PRIMARY KEY,
    bill_date           DATE NOT NULL,
    store_code          TEXT NOT NULL,
    article_code        TEXT NOT NULL,
    article_description TEXT,
    family_code         TEXT,
    class_code          TEXT,
    category_code       TEXT,
    sales_channel       TEXT,
    quantity            NUMERIC DEFAULT 0,
    gross_sales         NUMERIC DEFAULT 0,
    discount_amount     NUMERIC DEFAULT 0,
    cogs                NUMERIC DEFAULT 0,
    net_sales           NUMERIC DEFAULT 0,
    created_at          TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_gts_date ON gold.g_total_sales(bill_date);
CREATE INDEX IF NOT EXISTS idx_gts_store ON gold.g_total_sales(store_code);
CREATE INDEX IF NOT EXISTS idx_gts_article ON gold.g_total_sales(article_code);

-- G_TOTAL_SALES_AGGREGATED: Aggregated sales with margins
CREATE TABLE IF NOT EXISTS gold.g_total_sales_aggregated (
    id                  BIGSERIAL PRIMARY KEY,
    bill_date           DATE NOT NULL,
    store_code          TEXT NOT NULL,
    article_code        TEXT NOT NULL,
    gross_sales         NUMERIC DEFAULT 0,
    gross_margin        NUMERIC DEFAULT 0,
    days_after_delivery INT DEFAULT 0,
    net_margin          NUMERIC DEFAULT 0,
    created_at          TIMESTAMPTZ DEFAULT NOW()
);

-- G_CLASS_LEVEL_NOB: Number of bills at class level
CREATE TABLE IF NOT EXISTS gold.g_class_level_nob (
    id                  BIGSERIAL PRIMARY KEY,
    bill_date           DATE NOT NULL,
    store_code          TEXT NOT NULL,
    class_code          TEXT NOT NULL,
    class_desc          TEXT,
    num_bills           INT DEFAULT 0,
    total_amount        NUMERIC DEFAULT 0,
    avg_bill_value      NUMERIC DEFAULT 0,
    created_at          TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_gcnob_date ON gold.g_class_level_nob(bill_date);
CREATE INDEX IF NOT EXISTS idx_gcnob_store ON gold.g_class_level_nob(store_code);

-- G_FAMILY_LEVEL_NOB: Number of bills at family level
CREATE TABLE IF NOT EXISTS gold.g_family_level_nob (
    id                  BIGSERIAL PRIMARY KEY,
    bill_date           DATE NOT NULL,
    store_code          TEXT NOT NULL,
    family_code         TEXT NOT NULL,
    family_desc         TEXT,
    num_bills           INT DEFAULT 0,
    total_amount        NUMERIC DEFAULT 0,
    avg_bill_value      NUMERIC DEFAULT 0,
    created_at          TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_gfnob_date ON gold.g_family_level_nob(bill_date);
CREATE INDEX IF NOT EXISTS idx_gfnob_store ON gold.g_family_level_nob(store_code);

-- G_CATEGORY_LEVEL_NOB: Number of bills at category level
CREATE TABLE IF NOT EXISTS gold.g_category_level_nob (
    id                  BIGSERIAL PRIMARY KEY,
    bill_date           DATE NOT NULL,
    store_code          TEXT NOT NULL,
    category_code       TEXT NOT NULL,
    category_desc       TEXT,
    num_bills           INT DEFAULT 0,
    total_amount        NUMERIC DEFAULT 0,
    avg_bill_value      NUMERIC DEFAULT 0,
    created_at          TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_gcatnob_date ON gold.g_category_level_nob(bill_date);
CREATE INDEX IF NOT EXISTS idx_gcatnob_store ON gold.g_category_level_nob(store_code);

-- G_CUSTOMER_TRANSACTION_SUMMARY: Complete customer-level aggregated metrics
CREATE TABLE IF NOT EXISTS gold.g_customer_transaction_summary (
    mobile_number               TEXT PRIMARY KEY,
    customer_surrogate_id       TEXT,
    first_bill_date             DATE,           -- FBD
    last_bill_date              DATE,           -- LBD
    recency_days                INT,
    tenure_days                 INT,
    total_bills                 INT,
    total_visits                INT,            -- distinct bill dates
    total_spend                 NUMERIC(15,2),
    spend_per_bill              NUMERIC(15,2),
    spend_per_visit             NUMERIC(15,2),
    avg_items_per_bill          NUMERIC(8,2),
    total_discount              NUMERIC(15,2),
    distinct_months             INT,
    distinct_store_count        INT,
    distinct_article_count      INT,
    dgbt_fs                     INT,            -- days gap between transactions from start
    avg_billing_time_secs       NUMERIC(8,2),
    return_bill_count           INT,
    promo_bill_count            INT,
    weekend_bill_count          INT,
    wednesday_bill_count        INT,
    first_week_bill_count       INT,
    last_week_bill_count        INT,
    fav_store_code              TEXT,
    fav_store_name              TEXT,
    fav_store_type              TEXT,
    fav_day                     TEXT,
    fav_article_by_spend        TEXT,
    fav_article_by_spend_desc   TEXT,
    fav_article_by_nob          TEXT,
    fav_article_by_nob_desc     TEXT,
    second_fav_article_by_spend TEXT,
    second_fav_article_by_nob   TEXT,
    channel_presence            TEXT,           -- Online/Offline/Omni
    spend_decile                INT,
    nob_decile                  INT,
    l1_segment                  TEXT,
    l2_segment                  TEXT,
    updated_at                  TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_gcts_surrogate ON gold.g_customer_transaction_summary(customer_surrogate_id);
CREATE INDEX IF NOT EXISTS idx_gcts_l1_segment ON gold.g_customer_transaction_summary(l1_segment);
CREATE INDEX IF NOT EXISTS idx_gcts_l2_segment ON gold.g_customer_transaction_summary(l2_segment);
CREATE INDEX IF NOT EXISTS idx_gcts_spend_decile ON gold.g_customer_transaction_summary(spend_decile);

-- G_CUSTOMER_CHANNEL_SUMMARY: Per customer per delivery channel
CREATE TABLE IF NOT EXISTS gold.g_customer_channel_summary (
    mobile_number               TEXT NOT NULL,
    delivery_channel            TEXT NOT NULL,
    bills                       INT,
    visits                      INT,
    spend                       NUMERIC(15,2),
    spend_per_bill              NUMERIC(15,2),
    spend_per_visit             NUMERIC(15,2),
    total_discount              NUMERIC(15,2),
    first_bill_date             DATE,
    last_bill_date              DATE,
    fav_store_code              TEXT,
    fav_article_by_spend        TEXT,
    fav_article_by_nob          TEXT,
    PRIMARY KEY (mobile_number, delivery_channel)
);

-- G_CUSTOMER_PRODUCT_SUMMARY: Per customer per brick/subcategory
CREATE TABLE IF NOT EXISTS gold.g_customer_product_summary (
    mobile_number               TEXT NOT NULL,
    brick                       TEXT,
    brick_desc                  TEXT,
    subcategory_code            TEXT,
    subcategory_desc            TEXT,
    bills                       INT,
    visits                      INT,
    spend                       NUMERIC(15,2),
    spend_per_bill              NUMERIC(15,2),
    total_discount              NUMERIC(15,2)
);

CREATE INDEX IF NOT EXISTS idx_gcps_mobile ON gold.g_customer_product_summary(mobile_number);
CREATE INDEX IF NOT EXISTS idx_gcps_brick ON gold.g_customer_product_summary(brick);

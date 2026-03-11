-- =============================================================================
-- COMPOSABLE CDP — GOLD LAYER TABLE SCHEMAS (Athena / Hive DDL)
-- =============================================================================
-- These are the expected table schemas in each brand's gold layer database.
-- Tables are partitioned on date columns for Athena query cost optimization.
-- Format: Parquet (columnar, snappy compression) — ideal for analytical queries.
--
-- Each brand has its own database (e.g., spencers_gold, fmcg_gold, etc.)
-- The CDP query compiler generates SQL that queries these tables.
-- =============================================================================

-- ─────────────────────────────────────────────────────────────────────────────
-- 1. CUSTOMERS — Unified customer profile (all brands)
-- ─────────────────────────────────────────────────────────────────────────────
CREATE EXTERNAL TABLE IF NOT EXISTS customers (
    customer_id             STRING          COMMENT 'Primary brand-level customer ID',
    corporate_id            STRING          COMMENT 'Cross-brand golden ID (nullable until linked)',

    -- Identity
    email                   STRING,
    email_hash              STRING          COMMENT 'SHA-256 of lowercase email',
    phone                   STRING          COMMENT 'Primary phone (E.164)',
    alternate_phones        ARRAY<STRING>,
    external_ids            MAP<STRING, STRING> COMMENT 'CRM/ERP/loyalty system IDs',

    -- Demographics
    first_name              STRING,
    last_name               STRING,
    full_name               STRING,
    date_of_birth           DATE,
    age                     INT,
    age_group               STRING,
    gender                  STRING,
    marital_status          STRING,
    household_size          INT,
    has_children            BOOLEAN,
    income_bracket          STRING,
    education_level         STRING,
    occupation              STRING,
    language_preference     STRING,

    -- Geography
    country                 STRING,
    state                   STRING,
    city                    STRING,
    pincode                 STRING,
    zone                    STRING          COMMENT 'north/south/east/west/central',
    tier                    STRING          COMMENT 'tier_1/tier_2/tier_3',
    dma                     STRING,
    home_store_id           STRING,
    home_store_name         STRING,
    home_store_city         STRING,
    service_area            STRING          COMMENT 'CESC distribution area',

    -- Transactional (pre-computed aggregates)
    total_orders            INT,
    total_revenue           DOUBLE,
    aov                     DOUBLE,
    max_order_value         DOUBLE,
    min_order_value         DOUBLE,
    first_purchase_date     DATE,
    last_purchase_date      DATE,
    days_since_last_purchase INT,
    purchase_frequency_30d  INT,
    purchase_frequency_90d  INT,
    purchase_frequency_365d INT,
    revenue_30d             DOUBLE,
    revenue_90d             DOUBLE,
    revenue_365d            DOUBLE,
    avg_basket_size         DOUBLE,
    avg_inter_purchase_days DOUBLE,
    preferred_payment_method STRING,
    preferred_channel       STRING,
    has_returned            BOOLEAN,
    return_rate             DOUBLE,
    total_discount_availed  DOUBLE,
    discount_sensitivity    STRING,
    coupon_usage_count      INT,

    -- Behavioral (pre-computed)
    total_sessions          INT,
    sessions_30d            INT,
    last_session_date       DATE,
    days_since_last_session INT,
    avg_session_duration_sec DOUBLE,
    pages_per_session       DOUBLE,
    total_page_views        INT,
    total_product_views     INT,
    total_add_to_cart       INT,
    cart_abandonment_count  INT,
    cart_abandonment_rate   DOUBLE,
    total_searches          INT,
    last_search_query       STRING,
    top_search_queries      ARRAY<STRING>,
    wishlist_count          INT,
    browse_to_buy_ratio     DOUBLE,
    preferred_browse_time   STRING,
    preferred_browse_day    STRING,

    -- Engagement (pre-computed)
    email_opens_30d         INT,
    email_clicks_30d        INT,
    email_open_rate         DOUBLE,
    email_click_rate        DOUBLE,
    sms_clicks_30d          INT,
    push_opens_30d          INT,
    whatsapp_responses_30d  INT,
    preferred_comm_channel  STRING,
    last_email_open_date    DATE,
    is_email_engaged        BOOLEAN,
    total_campaigns_received INT,
    campaign_fatigue_score  DOUBLE,
    nps_score               INT,
    nps_category            STRING,
    csat_score              DOUBLE,
    support_tickets_count   INT,
    open_support_tickets    INT,

    -- Lifecycle
    lifecycle_stage         STRING,
    customer_tenure_days    INT,
    acquisition_source      STRING,
    acquisition_campaign    STRING,
    rfm_recency_score       INT,
    rfm_frequency_score     INT,
    rfm_monetary_score      INT,
    rfm_segment             STRING,
    customer_value_tier     STRING,
    is_first_time_buyer     BOOLEAN,
    is_repeat_buyer         BOOLEAN,
    days_to_second_purchase INT,

    -- Product Affinity (pre-computed)
    top_categories          ARRAY<STRING>,
    top_brands_purchased    ARRAY<STRING>,
    top_categories_browsed  ARRAY<STRING>,
    category_breadth        INT,
    brand_loyalty_score     DOUBLE,
    price_sensitivity       STRING,
    preferred_pack_size     STRING,
    dietary_preference      ARRAY<STRING>,
    organic_buyer           BOOLEAN,
    private_label_buyer     BOOLEAN,

    -- Loyalty
    loyalty_is_member       BOOLEAN,
    loyalty_tier            STRING,
    loyalty_points_balance  INT,
    loyalty_points_earned_lifetime  INT,
    loyalty_points_redeemed_lifetime INT,
    loyalty_enrollment_date DATE,
    loyalty_last_earn_date  DATE,
    loyalty_last_burn_date  DATE,
    loyalty_redemption_rate DOUBLE,

    -- Device / Channel
    primary_device          STRING,
    primary_os              STRING,
    primary_browser         STRING,
    has_app_installed       BOOLEAN,
    app_version             STRING,
    push_enabled            BOOLEAN,
    is_omnichannel          BOOLEAN,
    channels_used           ARRAY<STRING>,
    last_touchpoint         STRING,

    -- Consent
    email_opt_in            BOOLEAN,
    sms_opt_in              BOOLEAN,
    push_opt_in             BOOLEAN,
    whatsapp_opt_in         BOOLEAN,
    cross_brand_opt_in      BOOLEAN,
    data_processing_consent BOOLEAN,
    last_consent_update     DATE,

    -- Predictive / ML (pre-computed by ML pipeline)
    churn_probability       DOUBLE,
    churn_risk_tier         STRING,
    next_purchase_probability DOUBLE,
    predicted_ltv           DOUBLE,
    predicted_next_category STRING,
    propensity_to_buy_score DOUBLE,
    upsell_propensity       DOUBLE,
    cross_sell_propensity   DOUBLE,
    engagement_score        DOUBLE,
    customer_health_score   DOUBLE,

    -- B2B (FMCG, CESC)
    company_name            STRING,
    industry                STRING,
    company_size            STRING,
    annual_contract_value   DOUBLE,
    account_type            STRING,
    credit_limit            DOUBLE,
    outstanding_amount      DOUBLE,
    payment_terms           STRING,

    -- CX Scores (pre-computed)
    overall_cx_score        DOUBLE,
    cx_tier                 STRING,
    cx_trend                STRING,
    service_reliability_score DOUBLE,
    billing_accuracy_score  DOUBLE,
    complaint_resolution_score DOUBLE,
    digital_experience_score DOUBLE,
    effort_score            DOUBLE,
    first_contact_resolution_rate DOUBLE,
    at_risk_cx              BOOLEAN,

    -- CESC-specific (on customer profile)
    connection_type         STRING,
    meter_type              STRING,
    has_smart_meter         BOOLEAN,
    sanctioned_load_kw      DOUBLE,
    has_solar               BOOLEAN,
    has_ev_charger          BOOLEAN,
    is_auto_pay_enrolled    BOOLEAN,
    has_paperless_billing   BOOLEAN,
    has_ebill_subscription  BOOLEAN,
    digital_adoption_tier   STRING,
    usage_alert_subscribed  BOOLEAN,
    outage_alert_subscribed BOOLEAN,
    has_portal_account      BOOLEAN,

    -- Offline store (pre-computed, Spencers/NBL)
    market_basket_cluster   STRING,
    basket_complexity_score DOUBLE,
    store_format_preference STRING,

    -- Metadata
    created_at              TIMESTAMP,
    updated_at              TIMESTAMP,
    _etl_loaded_at          TIMESTAMP
)
STORED AS PARQUET
LOCATION 's3://${BRAND}-datalake/gold/customers/'
TBLPROPERTIES ('parquet.compression'='SNAPPY');

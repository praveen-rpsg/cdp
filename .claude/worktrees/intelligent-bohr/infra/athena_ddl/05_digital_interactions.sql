-- =============================================================================
-- 6. DIGITAL_INTERACTIONS — Digital channel activity log (Power CESC)
-- =============================================================================
-- Tracks all digital touchpoints: portal logins, app usage, self-service
-- actions, chatbot interactions, etc. Used to compute digital adoption scores.
-- =============================================================================

CREATE EXTERNAL TABLE IF NOT EXISTS digital_interactions (
    interaction_id          STRING          COMMENT 'Unique interaction ID',
    customer_id             STRING          COMMENT 'FK to customers',
    account_number          STRING,

    -- Channel & Action
    channel                 STRING          COMMENT 'portal, app, whatsapp, ivr, chatbot, sms_link',
    action_type             STRING          COMMENT 'login, self_service, bill_view, bill_download, bill_pay, complaint_file, history_check, profile_update, alert_subscribe, feedback',
    action_detail           STRING          COMMENT 'More specific action description',

    -- Timing
    event_date              DATE,
    event_timestamp         TIMESTAMP,
    session_duration_sec    INT,

    -- Device context
    device_type             STRING          COMMENT 'mobile, desktop, tablet',
    os                      STRING,
    browser                 STRING,
    app_version             STRING,

    -- Payment (if action_type = bill_pay)
    payment_amount          DOUBLE,
    payment_method          STRING,
    payment_status          STRING,

    -- Self-service outcome
    task_completed          BOOLEAN         COMMENT 'Did the self-service action complete successfully?',
    fallback_to_agent       BOOLEAN         COMMENT 'Did the user fall back to a live agent?',

    -- Chatbot / WhatsApp specific
    bot_intent              STRING          COMMENT 'Detected intent: check_bill, report_outage, etc.',
    bot_resolution          BOOLEAN         COMMENT 'Was the query resolved by the bot?',
    messages_exchanged      INT,

    -- Metadata
    _etl_loaded_at          TIMESTAMP
)
PARTITIONED BY (event_month STRING COMMENT 'YYYY-MM partition key')
STORED AS PARQUET
LOCATION 's3://power-cesc-datalake/gold/digital_interactions/'
TBLPROPERTIES ('parquet.compression'='SNAPPY');


-- =============================================================================
-- 7. EVENTS — Generic event stream (all brands, website/app behavioral events)
-- =============================================================================
-- Used for event-based segmentation (has_performed, count-based conditions).
-- =============================================================================

CREATE EXTERNAL TABLE IF NOT EXISTS events (
    event_id                STRING          COMMENT 'Unique event ID',
    customer_id             STRING          COMMENT 'FK to customers (null for anonymous)',
    anonymous_id            STRING          COMMENT 'Anonymous visitor ID (pre-identification)',

    -- Event details
    event_name              STRING          COMMENT 'page_viewed, product_viewed, add_to_cart, purchase, search, signup, login, etc.',
    event_timestamp         TIMESTAMP,
    event_date              DATE,

    -- Properties (schemaless for flexibility)
    properties              MAP<STRING, STRING> COMMENT 'Event-specific key-value properties',

    -- Common event properties (denormalized for performance)
    page_url                STRING,
    page_title              STRING,
    referrer_url            STRING,
    product_id              STRING,
    product_name            STRING,
    category                STRING,
    search_query            STRING,
    revenue                 DOUBLE,

    -- Context
    channel                 STRING          COMMENT 'web, app, whatsapp, in_store_kiosk',
    device_type             STRING,
    os                      STRING,
    browser                 STRING,
    ip_city                 STRING,
    ip_country              STRING,

    -- UTM
    utm_source              STRING,
    utm_medium              STRING,
    utm_campaign            STRING,
    utm_content             STRING,
    utm_term                STRING,

    -- Session
    session_id              STRING,

    -- Metadata
    _etl_loaded_at          TIMESTAMP
)
PARTITIONED BY (event_month STRING COMMENT 'YYYY-MM partition key')
STORED AS PARQUET
LOCATION 's3://${BRAND}-datalake/gold/events/'
TBLPROPERTIES ('parquet.compression'='SNAPPY');

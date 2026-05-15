-- =============================================================================
-- NBL Silver Layer Population Script
-- =============================================================================
-- Populates all NBL silver tables from nb_bronze + nb_staging sources.
-- SAFE: touches ONLY nb_* schemas. Zero writes to any Spencers schema.
--
-- Execution phases (strict dependency order):
--   Phase 0 : Create nb_silver intermediate schema + 3 tables
--             (stg_bill_summary, int_identity_spine, int_identity_resolved)
--   Phase 1 : nb_silver_identity
--             (unified_profiles -> identity_edges -> identity_graph_summary)
--   Phase 2 : nb_silver_gold
--             (channel_summary, product_summary, daily_store_sales,
--              customer_transaction_summary)
--   Phase 3 : nb_silver_reverse_etl
--             (customer_behavioral_attributes)
-- =============================================================================

BEGIN;

-- ===========================================================================
-- PHASE 0 : nb_silver  — intermediate materialized tables
-- ===========================================================================

CREATE SCHEMA IF NOT EXISTS nb_silver AUTHORIZATION cdp;

-- ---------------------------------------------------------------------------
-- 0a. nb_silver.stg_bill_summary
--     One row per bill; aggregates line items, derives billing duration flags.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS nb_silver.stg_bill_summary (
    bill_date            DATE,
    store_code           TEXT,
    store_format         TEXT,
    store_desc           TEXT,
    till_no              TEXT,
    bill_no              TEXT,
    bill_id              TEXT,
    mobile_number        TEXT,
    region               TEXT,
    city                 TEXT,
    delivery_channel     TEXT,
    line_item_count      BIGINT,
    gross_sale_value     NUMERIC,
    total_discount       NUMERIC,
    total_mrp_value      NUMERIC,
    bill_start_time      TEXT,
    bill_end_time        TEXT,
    billing_time_secs    INTEGER,
    sales_return         BOOLEAN,
    promo_applied        BOOLEAN,
    gift_indicator       BOOLEAN,
    liquidity_indicator  BOOLEAN,
    weekend_flag         BOOLEAN,
    wednesday_flag       BOOLEAN,
    day_of_week          TEXT
);

TRUNCATE nb_silver.stg_bill_summary;

INSERT INTO nb_silver.stg_bill_summary
SELECT
    bill_date,
    store_code,
    store_format,
    MAX(store_desc)                                                             AS store_desc,
    till_no,
    bill_no,
    bill_id,
    mobile_number,
    region,
    city,
    delivery_channel,
    COUNT(*)                                                                    AS line_item_count,
    SUM(gross_sale_value)                                                       AS gross_sale_value,
    SUM(total_discount)                                                         AS total_discount,
    SUM(total_mrp_value)                                                        AS total_mrp_value,
    MIN(bill_start_time)                                                        AS bill_start_time,
    MAX(bill_end_time)                                                          AS bill_end_time,
    CASE
        WHEN MIN(bill_start_time) ~ '^\d{2}:\d{2}:\d{2}$'
         AND MAX(bill_end_time)   ~ '^\d{2}:\d{2}:\d{2}$'
        THEN EXTRACT(EPOCH FROM (
                MAX(bill_end_time)::TIME - MIN(bill_start_time)::TIME
             ))::INT
        ELSE NULL
    END                                                                         AS billing_time_secs,
    BOOL_OR(sales_return)                                                       AS sales_return,
    BOOL_OR(promo_indicator = 'P')                                              AS promo_applied,
    BOOL_OR(gift_item_indicator = 'Y')                                          AS gift_indicator,
    BOOL_OR(COALESCE(NULLIF(TRIM(liquidity_type), ''), '') = 'LQ')             AS liquidity_indicator,
    BOOL_OR(weekend_flag)                                                       AS weekend_flag,
    BOOL_OR(wednesday_flag)                                                     AS wednesday_flag,
    MAX(day_of_week)                                                            AS day_of_week
FROM nb_staging.stg_bill_transactions
GROUP BY
    bill_date, store_code, store_format, till_no, bill_no, bill_id,
    mobile_number, region, city, delivery_channel;

-- ---------------------------------------------------------------------------
-- 0b. nb_silver.int_identity_spine
--     All identifier observations from every NBL source system.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS nb_silver.int_identity_spine (
    mobile           TEXT,
    surrogate_id     TEXT,
    name             TEXT,
    email            TEXT,
    pincode          TEXT,
    store_code       TEXT,
    source_system    TEXT,
    observed_at      DATE,
    identifier_hash  TEXT
);

TRUNCATE nb_silver.int_identity_spine;

INSERT INTO nb_silver.int_identity_spine
WITH cih_identifiers AS (
    SELECT
        mobile, surrogate_id, name, email, pincode, store_code,
        source_system, NULL::DATE AS observed_at
    FROM nb_staging.stg_cih_profiles
    WHERE mobile IS NOT NULL
),
pos_identifiers AS (
    SELECT
        mobile, surrogate_id,
        NULL AS name, NULL AS email, NULL AS pincode,
        store_code, source_system, last_seen_at AS observed_at
    FROM nb_staging.stg_bill_identifiers
    WHERE mobile IS NOT NULL
),
nps_ids AS (
    SELECT
        CASE
            WHEN LENGTH(REGEXP_REPLACE(mobile, '[^0-9]', '', 'g')) = 10
            THEN REGEXP_REPLACE(mobile, '[^0-9]', '', 'g')
            ELSE mobile
        END                        AS mobile,
        NULL                       AS surrogate_id,
        NULL AS name, NULL AS email, NULL AS pincode,
        store_code,
        'NPS'                      AS source_system,
        bill_date::DATE            AS observed_at
    FROM nb_bronze.raw_nps_survey
    WHERE mobile IS NOT NULL AND mobile != ''
),
yvm_ids AS (
    SELECT
        CASE
            WHEN LENGTH(REGEXP_REPLACE(mobile_no, '[^0-9]', '', 'g')) = 10
            THEN REGEXP_REPLACE(mobile_no, '[^0-9]', '', 'g')
            ELSE mobile_no
        END                        AS mobile,
        NULL                       AS surrogate_id,
        customer_name              AS name,
        email_id                   AS email,
        pincode,
        store_code,
        'YVM'                      AS source_system,
        NULL::DATE                 AS observed_at
    FROM nb_bronze.raw_yvm_feedback
    WHERE mobile_no IS NOT NULL AND mobile_no != ''
),
cashback_ids AS (
    SELECT
        CASE
            WHEN LENGTH(REGEXP_REPLACE(mobile_number, '[^0-9]', '', 'g')) = 10
            THEN REGEXP_REPLACE(mobile_number, '[^0-9]', '', 'g')
            ELSE mobile_number
        END                        AS mobile,
        NULL                       AS surrogate_id,
        NULL AS name, NULL AS email, NULL AS pincode,
        NULL                       AS store_code,
        'PROMO'                    AS source_system,
        NULL::DATE                 AS observed_at
    FROM nb_bronze.raw_promo_cashback
    WHERE mobile_number IS NOT NULL AND mobile_number != ''
),
all_identifiers AS (
    SELECT * FROM cih_identifiers
    UNION ALL SELECT * FROM pos_identifiers
    UNION ALL SELECT * FROM nps_ids      WHERE mobile IS NOT NULL
    UNION ALL SELECT * FROM yvm_ids      WHERE mobile IS NOT NULL
    UNION ALL SELECT * FROM cashback_ids WHERE mobile IS NOT NULL
)
SELECT
    mobile, surrogate_id, name, email, pincode, store_code, source_system, observed_at,
    MD5(COALESCE(mobile,'') || '|' || COALESCE(email,'') || '|' || source_system) AS identifier_hash
FROM all_identifiers;

-- ---------------------------------------------------------------------------
-- 0c. nb_silver.int_identity_resolved
--     Deduplicated master: CIH profiles UNION POS-only customers.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS nb_silver.int_identity_resolved (
    unified_id                TEXT,
    canonical_mobile          TEXT,
    surrogate_id              TEXT,
    cih_original_surrogate_id TEXT,
    name                      TEXT,
    first_name                TEXT,
    last_name                 TEXT,
    email                     TEXT,
    city                      TEXT,
    pincode                   TEXT,
    street                    TEXT,
    region                    TEXT,
    store_code                TEXT,
    status                    TEXT,
    dob                       TEXT,
    age                       INTEGER,
    customer_group            TEXT,
    occupation                TEXT,
    whatsapp                  TEXT,
    dnd                       TEXT,
    gw_customer_flag          TEXT,
    accepts_email_marketing   TEXT,
    accepts_sms_marketing     TEXT,
    cih_total_orders          INTEGER,
    cih_total_spent           NUMERIC,
    primary_source            TEXT,
    has_transactions          BOOLEAN
);

TRUNCATE nb_silver.int_identity_resolved;

-- POS-first resolution: base is transaction customers (~28K unique mobiles),
-- LEFT JOIN to CIH for profile enrichment. This keeps unified_profiles scoped
-- to real shoppers, not the entire 2.7M CIH master.
INSERT INTO nb_silver.int_identity_resolved
WITH cih_deduped AS (
    SELECT DISTINCT ON (mobile) *
    FROM nb_staging.stg_cih_profiles
    WHERE mobile IS NOT NULL
    ORDER BY mobile, surrogate_id
),
bill_customers AS (
    SELECT DISTINCT ON (mobile)
        mobile, surrogate_id, store_code, last_seen_at
    FROM nb_staging.stg_bill_identifiers
    WHERE mobile IS NOT NULL
    ORDER BY mobile, last_seen_at DESC
)
SELECT
    MD5(b.mobile)                                                       AS unified_id,
    b.mobile                                                            AS canonical_mobile,
    COALESCE(c.surrogate_id, b.surrogate_id)                           AS surrogate_id,
    c.cih_original_surrogate_id,
    c.name, c.first_name, c.last_name,
    c.email, c.city, c.pincode, c.street, c.region,
    COALESCE(c.store_code, b.store_code)                               AS store_code,
    c.status, c.dob, c.age,
    c.customer_group, c.occupation, c.whatsapp, c.dnd,
    c.gw_customer_flag, c.accepts_email_marketing, c.accepts_sms_marketing,
    c.total_orders                                                      AS cih_total_orders,
    c.total_spent                                                       AS cih_total_spent,
    CASE WHEN c.mobile IS NOT NULL THEN 'CIH+POS' ELSE 'POS' END      AS primary_source,
    TRUE                                                                AS has_transactions
FROM bill_customers b
LEFT JOIN cih_deduped c ON b.mobile = c.mobile;

-- ===========================================================================
-- PHASE 1 : nb_silver_identity
-- ===========================================================================

-- ---------------------------------------------------------------------------
-- 1a. unified_profiles — golden customer record
-- ---------------------------------------------------------------------------
TRUNCATE nb_silver_identity.unified_profiles;

INSERT INTO nb_silver_identity.unified_profiles (
    unified_id, canonical_mobile, surrogate_id, cih_original_surrogate_id,
    display_name, first_name, last_name,
    email, city, pincode, street, region, registered_store,
    status, dob, age, customer_group, occupation,
    whatsapp, dnd, gw_customer_flag,
    accepts_email_marketing, accepts_sms_marketing,
    cih_total_orders, cih_total_spent,
    primary_source, has_transactions, profile_updated_at
)
SELECT
    unified_id,
    canonical_mobile,
    surrogate_id,
    cih_original_surrogate_id,
    COALESCE(name, NULLIF(TRIM(COALESCE(first_name,'') || ' ' || COALESCE(last_name,'')), '')) AS display_name,
    first_name,
    last_name,
    email, city, pincode, street, region,
    store_code  AS registered_store,
    status, dob, age, customer_group, occupation,
    whatsapp, dnd, gw_customer_flag,
    accepts_email_marketing, accepts_sms_marketing,
    cih_total_orders, cih_total_spent,
    primary_source, has_transactions,
    NOW()       AS profile_updated_at
FROM nb_silver.int_identity_resolved;

-- ---------------------------------------------------------------------------
-- 1b. identity_edges — identifier graph edges per customer
-- ---------------------------------------------------------------------------
TRUNCATE nb_silver_identity.identity_edges;

INSERT INTO nb_silver_identity.identity_edges (
    unified_id, identifier_type, identifier_value, source_system, confidence_score
)
WITH profiles AS (
    SELECT unified_id, canonical_mobile, surrogate_id, primary_source
    FROM nb_silver_identity.unified_profiles
),
spine AS (
    SELECT * FROM nb_silver.int_identity_spine
),
mobile_edges AS (
    SELECT DISTINCT
        p.unified_id,
        'mobile'            AS identifier_type,
        p.canonical_mobile  AS identifier_value,
        CASE WHEN p.primary_source LIKE '%CIH%' THEN 'CIH' ELSE 'POS' END AS source_system,
        1.0::NUMERIC        AS confidence_score
    FROM profiles p
    WHERE p.canonical_mobile IS NOT NULL
),
email_edges AS (
    SELECT DISTINCT
        p.unified_id,
        'email'             AS identifier_type,
        s.email             AS identifier_value,
        s.source_system,
        0.9::NUMERIC        AS confidence_score
    FROM spine s
    INNER JOIN profiles p ON s.mobile = p.canonical_mobile
    WHERE s.email IS NOT NULL AND s.email != ''
),
name_edges AS (
    SELECT DISTINCT
        p.unified_id,
        'name'              AS identifier_type,
        s.name              AS identifier_value,
        s.source_system,
        0.7::NUMERIC        AS confidence_score
    FROM spine s
    INNER JOIN profiles p ON s.mobile = p.canonical_mobile
    WHERE s.name IS NOT NULL AND s.name != '' AND LENGTH(s.name) > 1
),
store_edges AS (
    SELECT DISTINCT
        p.unified_id,
        'store_affinity'    AS identifier_type,
        s.store_code        AS identifier_value,
        s.source_system,
        0.8::NUMERIC        AS confidence_score
    FROM spine s
    INNER JOIN profiles p ON s.mobile = p.canonical_mobile
    WHERE s.store_code IS NOT NULL AND s.store_code != ''
)
SELECT * FROM mobile_edges
UNION ALL SELECT * FROM email_edges
UNION ALL SELECT * FROM name_edges
UNION ALL SELECT * FROM store_edges;

-- ---------------------------------------------------------------------------
-- 1c. identity_graph_summary — edge statistics + completeness score
-- ---------------------------------------------------------------------------
TRUNCATE nb_silver_identity.identity_graph_summary;

INSERT INTO nb_silver_identity.identity_graph_summary (
    unified_id, canonical_mobile, display_name, primary_source, has_transactions,
    total_edge_count, distinct_id_types, source_system_count, source_systems,
    mobile_count, email_count, store_count, avg_confidence, completeness_score
)
WITH edge_stats AS (
    SELECT
        unified_id,
        COUNT(*)                                                                    AS total_edge_count,
        COUNT(DISTINCT identifier_type)                                             AS distinct_id_types,
        COUNT(DISTINCT source_system)                                               AS source_system_count,
        STRING_AGG(DISTINCT source_system, ', ' ORDER BY source_system)            AS source_systems,
        COUNT(DISTINCT CASE WHEN identifier_type = 'mobile'         THEN identifier_value END) AS mobile_count,
        COUNT(DISTINCT CASE WHEN identifier_type = 'email'          THEN identifier_value END) AS email_count,
        COUNT(DISTINCT CASE WHEN identifier_type = 'store_affinity' THEN identifier_value END) AS store_count,
        AVG(confidence_score)                                                       AS avg_confidence
    FROM nb_silver_identity.identity_edges
    GROUP BY unified_id
)
SELECT
    p.unified_id,
    p.canonical_mobile,
    p.display_name,
    p.primary_source,
    p.has_transactions,
    COALESCE(es.total_edge_count,    0)  AS total_edge_count,
    COALESCE(es.distinct_id_types,   0)  AS distinct_id_types,
    COALESCE(es.source_system_count, 0)  AS source_system_count,
    es.source_systems,
    COALESCE(es.mobile_count, 0)         AS mobile_count,
    COALESCE(es.email_count,  0)         AS email_count,
    COALESCE(es.store_count,  0)         AS store_count,
    COALESCE(es.avg_confidence, 0)       AS avg_confidence,
    (
        CASE WHEN p.canonical_mobile IS NOT NULL                            THEN 1 ELSE 0 END +
        CASE WHEN p.email            IS NOT NULL                            THEN 1 ELSE 0 END +
        CASE WHEN p.display_name     IS NOT NULL AND p.display_name != ''   THEN 1 ELSE 0 END +
        CASE WHEN p.city             IS NOT NULL                            THEN 1 ELSE 0 END +
        CASE WHEN p.pincode          IS NOT NULL                            THEN 1 ELSE 0 END +
        CASE WHEN p.dob              IS NOT NULL                            THEN 1 ELSE 0 END +
        CASE WHEN p.has_transactions                                         THEN 1 ELSE 0 END
    )::NUMERIC / 7.0                     AS completeness_score
FROM nb_silver_identity.unified_profiles p
LEFT JOIN edge_stats es ON p.unified_id = es.unified_id;

-- ===========================================================================
-- PHASE 2 : nb_silver_gold
-- ===========================================================================

-- ---------------------------------------------------------------------------
-- 2a. customer_channel_summary — spend/bills per customer per channel
-- ---------------------------------------------------------------------------
TRUNCATE nb_silver_gold.customer_channel_summary;

INSERT INTO nb_silver_gold.customer_channel_summary (
    mobile_number, delivery_channel,
    bills, visits, spend, spend_per_bill, spend_per_visit,
    total_discount, first_bill_date, last_bill_date
)
SELECT
    mobile_number,
    delivery_channel,
    COUNT(DISTINCT bill_id)                                                             AS bills,
    COUNT(DISTINCT bill_date)                                                           AS visits,
    SUM(gross_sale_value)                                                               AS spend,
    CASE WHEN COUNT(DISTINCT bill_id) > 0
         THEN ROUND(SUM(gross_sale_value) / COUNT(DISTINCT bill_id),   2) ELSE 0 END   AS spend_per_bill,
    CASE WHEN COUNT(DISTINCT bill_date) > 0
         THEN ROUND(SUM(gross_sale_value) / COUNT(DISTINCT bill_date), 2) ELSE 0 END   AS spend_per_visit,
    SUM(total_discount)                                                                 AS total_discount,
    MIN(bill_date)                                                                      AS first_bill_date,
    MAX(bill_date)                                                                      AS last_bill_date
FROM nb_staging.stg_bill_transactions
WHERE sales_return = FALSE AND mobile_number IS NOT NULL
GROUP BY mobile_number, delivery_channel;

-- ---------------------------------------------------------------------------
-- 2b. customer_product_summary — spend/bills per customer per product category
-- ---------------------------------------------------------------------------
TRUNCATE nb_silver_gold.customer_product_summary;

INSERT INTO nb_silver_gold.customer_product_summary (
    mobile_number, brick, brick_desc,
    category_code, category_desc, family, family_desc,
    bills, visits, spend, spend_per_bill, total_discount
)
SELECT
    mobile_number,
    brick,              MAX(brick_desc)    AS brick_desc,
    segment             AS category_code,
    MAX(segment_desc)   AS category_desc,
    family,             MAX(family_desc)   AS family_desc,
    COUNT(DISTINCT bill_id)                                                             AS bills,
    COUNT(DISTINCT bill_date)                                                           AS visits,
    SUM(gross_sale_value)                                                               AS spend,
    CASE WHEN COUNT(DISTINCT bill_id) > 0
         THEN ROUND(SUM(gross_sale_value) / COUNT(DISTINCT bill_id), 2) ELSE 0 END     AS spend_per_bill,
    SUM(total_discount)                                                                 AS total_discount
FROM nb_staging.stg_bill_transactions
WHERE sales_return = FALSE AND mobile_number IS NOT NULL
GROUP BY mobile_number, brick, segment, family;

-- ---------------------------------------------------------------------------
-- 2c. daily_store_sales — daily article-level aggregation per store
-- ---------------------------------------------------------------------------
TRUNCATE nb_silver_gold.daily_store_sales;

INSERT INTO nb_silver_gold.daily_store_sales (
    bill_date, store_code,
    article_code, article_description,
    segment_code, segment_desc, family_code, family_desc,
    class_code, class_desc, brick_code, brick_desc,
    brand_code, brand_desc,
    store_format, region_code, region_desc, city, city_desc,
    sales_channel,
    quantity, gross_sales, discount_amount, net_sales,
    line_item_count, num_bills, created_at
)
SELECT
    bill_date,
    store_code,
    article            AS article_code,
    article_desc       AS article_description,
    segment            AS segment_code,
    segment_desc,
    family             AS family_code,
    family_desc,
    class              AS class_code,
    class_desc,
    brick              AS brick_code,
    brick_desc,
    brand              AS brand_code,
    brand_name         AS brand_desc,
    store_format,
    region             AS region_code,
    region_desc,
    city,
    city_desc,
    'OFFLINE'          AS sales_channel,
    SUM(billed_qty)                             AS quantity,
    SUM(gross_sale_value)                       AS gross_sales,
    SUM(total_discount)                         AS discount_amount,
    SUM(gross_sale_value - total_discount)      AS net_sales,
    COUNT(*)                                    AS line_item_count,
    COUNT(DISTINCT bill_id)                     AS num_bills,
    NOW()                                       AS created_at
FROM nb_staging.stg_bill_transactions
WHERE sales_return = FALSE AND article IS NOT NULL
GROUP BY
    bill_date, store_code, article, article_desc,
    segment, segment_desc, family, family_desc,
    class, class_desc, brick, brick_desc,
    brand, brand_name, store_format,
    region, region_desc, city, city_desc;

-- ---------------------------------------------------------------------------
-- 2d. customer_transaction_summary — full RFM + L1/L2 segmentation per customer
-- ---------------------------------------------------------------------------
TRUNCATE nb_silver_gold.customer_transaction_summary;

INSERT INTO nb_silver_gold.customer_transaction_summary (
    mobile_number, unified_id, surrogate_id,
    first_bill_date, last_bill_date,
    recency_days, tenure_days,
    total_bills, total_visits, total_spend,
    spend_per_bill, spend_per_visit, avg_items_per_bill, total_discount,
    distinct_months, distinct_store_count, distinct_article_count,
    dgbt_fs, avg_billing_time_secs,
    return_bill_count, promo_bill_count, weekend_bill_count, wednesday_bill_count,
    fav_store_code, fav_store_name, fav_store_type,
    fav_day,
    fav_article_by_spend, fav_article_by_spend_desc,
    fav_article_by_nob,   fav_article_by_nob_desc,
    second_fav_article_by_spend, second_fav_article_by_nob,
    channel_presence,
    spend_decile, nob_decile,
    updated_at,
    l1_segment, l2_segment
)
WITH txn AS (
    SELECT * FROM nb_staging.stg_bill_transactions WHERE sales_return = FALSE
),
bills AS (
    SELECT * FROM nb_silver.stg_bill_summary
),
profiles AS (
    SELECT unified_id, canonical_mobile, surrogate_id
    FROM nb_silver_identity.unified_profiles
),
customer_base AS (
    SELECT
        t.mobile_number,
        MIN(t.bill_date)                                        AS first_bill_date,
        MAX(t.bill_date)                                        AS last_bill_date,
        (CURRENT_DATE - MAX(t.bill_date))                       AS recency_days,
        (CURRENT_DATE - MIN(t.bill_date))                       AS tenure_days,
        COUNT(DISTINCT t.bill_id)                               AS total_bills,
        COUNT(DISTINCT t.bill_date)                             AS total_visits,
        COALESCE(SUM(t.gross_sale_value), 0)                    AS total_spend,
        COALESCE(SUM(t.total_discount),   0)                    AS total_discount_amount,
        COUNT(DISTINCT DATE_TRUNC('month', t.bill_date))        AS distinct_months,
        COUNT(DISTINCT t.store_code)                            AS distinct_store_count,
        COUNT(DISTINCT t.article)                               AS distinct_article_count,
        COUNT(*)                                                AS total_line_items
    FROM txn t
    WHERE t.mobile_number IS NOT NULL
    GROUP BY t.mobile_number
),
bill_flags AS (
    SELECT
        b.mobile_number,
        COUNT(CASE WHEN b.sales_return   THEN 1 END)            AS return_bill_count,
        COUNT(CASE WHEN b.promo_applied  THEN 1 END)            AS promo_bill_count,
        COUNT(CASE WHEN b.weekend_flag   THEN 1 END)            AS weekend_bill_count,
        COUNT(CASE WHEN b.wednesday_flag THEN 1 END)            AS wednesday_bill_count,
        AVG(b.billing_time_secs)
            FILTER (WHERE b.billing_time_secs > 0
                      AND b.billing_time_secs < 7200)           AS avg_billing_time_secs,
        (MAX(b.bill_date) - MIN(b.bill_date))                   AS dgbt_fs
    FROM bills b
    WHERE b.mobile_number IS NOT NULL
    GROUP BY b.mobile_number
),
fav_store AS (
    SELECT DISTINCT ON (mobile_number)
        mobile_number,
        store_code    AS fav_store_code,
        store_desc    AS fav_store_name,
        store_format  AS fav_store_type
    FROM (
        SELECT mobile_number, store_code,
               MAX(store_desc)            AS store_desc,
               MAX(store_format)          AS store_format,
               COUNT(DISTINCT bill_date)  AS visit_count,
               SUM(gross_sale_value)      AS spend
        FROM txn
        WHERE mobile_number IS NOT NULL
        GROUP BY mobile_number, store_code
    ) s
    ORDER BY mobile_number, visit_count DESC, spend DESC
),
fav_day AS (
    SELECT DISTINCT ON (mobile_number)
        mobile_number, TRIM(day_of_week) AS fav_day
    FROM (
        SELECT mobile_number, day_of_week,
               COUNT(DISTINCT bill_id) AS bill_count
        FROM txn WHERE mobile_number IS NOT NULL
        GROUP BY mobile_number, day_of_week
    ) d
    ORDER BY mobile_number, bill_count DESC
),
fav_article_spend AS (
    SELECT DISTINCT ON (mobile_number)
        mobile_number,
        article       AS fav_article_by_spend,
        article_desc  AS fav_article_by_spend_desc
    FROM (
        SELECT mobile_number, article,
               MAX(article_desc)      AS article_desc,
               SUM(gross_sale_value)  AS spend
        FROM txn WHERE mobile_number IS NOT NULL
        GROUP BY mobile_number, article
    ) a
    ORDER BY mobile_number, spend DESC
),
fav_article_nob AS (
    SELECT DISTINCT ON (mobile_number)
        mobile_number,
        article       AS fav_article_by_nob,
        article_desc  AS fav_article_by_nob_desc
    FROM (
        SELECT mobile_number, article,
               MAX(article_desc)          AS article_desc,
               COUNT(DISTINCT bill_id)    AS nob
        FROM txn WHERE mobile_number IS NOT NULL
        GROUP BY mobile_number, article
    ) a
    ORDER BY mobile_number, nob DESC
),
second_fav_spend AS (
    SELECT mobile_number, article AS second_fav_article_by_spend
    FROM (
        SELECT mobile_number, article,
               ROW_NUMBER() OVER (
                   PARTITION BY mobile_number
                   ORDER BY SUM(gross_sale_value) DESC
               ) AS rn
        FROM txn WHERE mobile_number IS NOT NULL
        GROUP BY mobile_number, article
    ) a WHERE rn = 2
),
second_fav_nob AS (
    SELECT mobile_number, article AS second_fav_article_by_nob
    FROM (
        SELECT mobile_number, article,
               ROW_NUMBER() OVER (
                   PARTITION BY mobile_number
                   ORDER BY COUNT(DISTINCT bill_id) DESC
               ) AS rn
        FROM txn WHERE mobile_number IS NOT NULL
        GROUP BY mobile_number, article
    ) a WHERE rn = 2
),
channel_pres AS (
    SELECT
        mobile_number,
        CASE
            WHEN COUNT(DISTINCT delivery_channel) > 1 THEN 'Omni'
            WHEN MAX(delivery_channel) = 'Online'    THEN 'Online'
            ELSE 'Offline'
        END AS channel_presence
    FROM txn WHERE mobile_number IS NOT NULL
    GROUP BY mobile_number
),
assembled AS (
    SELECT
        cb.mobile_number,
        p.unified_id,
        p.surrogate_id,
        cb.first_bill_date,
        cb.last_bill_date,
        cb.recency_days,
        cb.tenure_days,
        cb.total_bills,
        cb.total_visits,
        cb.total_spend,
        CASE WHEN cb.total_bills  > 0
             THEN ROUND(cb.total_spend / cb.total_bills,  2) ELSE 0 END AS spend_per_bill,
        CASE WHEN cb.total_visits > 0
             THEN ROUND(cb.total_spend / cb.total_visits, 2) ELSE 0 END AS spend_per_visit,
        CASE WHEN cb.total_bills  > 0
             THEN ROUND(cb.total_line_items::NUMERIC / cb.total_bills, 2) ELSE 0 END AS avg_items_per_bill,
        cb.total_discount_amount                            AS total_discount,
        cb.distinct_months,
        cb.distinct_store_count,
        cb.distinct_article_count,
        COALESCE(bf.dgbt_fs, 0)                            AS dgbt_fs,
        ROUND(COALESCE(bf.avg_billing_time_secs, 0)::NUMERIC, 2) AS avg_billing_time_secs,
        COALESCE(bf.return_bill_count,    0)               AS return_bill_count,
        COALESCE(bf.promo_bill_count,     0)               AS promo_bill_count,
        COALESCE(bf.weekend_bill_count,   0)               AS weekend_bill_count,
        COALESCE(bf.wednesday_bill_count, 0)               AS wednesday_bill_count,
        fs.fav_store_code,
        fs.fav_store_name,
        fs.fav_store_type,
        fd.fav_day,
        fas.fav_article_by_spend,
        fas.fav_article_by_spend_desc,
        fan.fav_article_by_nob,
        fan.fav_article_by_nob_desc,
        sfs.second_fav_article_by_spend,
        sfn.second_fav_article_by_nob,
        cp.channel_presence,
        NTILE(10) OVER (ORDER BY cb.total_spend) AS spend_decile,
        NTILE(10) OVER (ORDER BY cb.total_bills) AS nob_decile,
        NOW()                                    AS updated_at
    FROM customer_base cb
    LEFT JOIN profiles          p   ON cb.mobile_number = p.canonical_mobile
    LEFT JOIN bill_flags        bf  ON cb.mobile_number = bf.mobile_number
    LEFT JOIN fav_store         fs  ON cb.mobile_number = fs.mobile_number
    LEFT JOIN fav_day           fd  ON cb.mobile_number = fd.mobile_number
    LEFT JOIN fav_article_spend fas ON cb.mobile_number = fas.mobile_number
    LEFT JOIN fav_article_nob   fan ON cb.mobile_number = fan.mobile_number
    LEFT JOIN second_fav_spend  sfs ON cb.mobile_number = sfs.mobile_number
    LEFT JOIN second_fav_nob    sfn ON cb.mobile_number = sfn.mobile_number
    LEFT JOIN channel_pres      cp  ON cb.mobile_number = cp.mobile_number
),
with_l1 AS (
    SELECT *,
        CASE
            WHEN fav_store_type IN ('Large', 'LARGE', 'Hyper') THEN
                CASE
                    WHEN spend_per_bill >= 1400 AND total_visits >  4 THEN 'HVHF'
                    WHEN spend_per_bill <  1400 AND total_visits >  4 THEN 'LVHF'
                    WHEN spend_per_bill >= 1400 AND total_visits <= 4 THEN 'HVLF'
                    ELSE 'LVLF'
                END
            ELSE
                CASE
                    WHEN spend_per_bill >= 750 AND total_visits >  5 THEN 'HVHF'
                    WHEN spend_per_bill <  750 AND total_visits >  5 THEN 'LVHF'
                    WHEN spend_per_bill >= 750 AND total_visits <= 5 THEN 'HVLF'
                    ELSE 'LVLF'
                END
        END AS l1_segment
    FROM assembled
),
with_l2 AS (
    SELECT *,
        CASE
            WHEN fav_store_type IN ('Large', 'LARGE', 'Hyper') THEN
                CASE
                    WHEN recency_days <=  30 AND total_visits >= 4
                     AND spend_per_bill >= 1400 AND distinct_months >= 3 THEN 'STAR'
                    WHEN recency_days <=  30 AND total_visits >= 4
                     AND spend_per_bill <  1400 AND distinct_months >= 3 THEN 'LOYAL'
                    WHEN recency_days <=  30 AND total_bills = 1         THEN 'New'
                    WHEN recency_days <=  30                             THEN 'Win Back'
                    WHEN recency_days <=  60                             THEN 'ACTIVE'
                    WHEN recency_days <= 120                             THEN 'Inactive'
                    WHEN recency_days <= 180                             THEN 'LAPSER'
                    ELSE 'Deep Lapsed'
                END
            ELSE
                CASE
                    WHEN recency_days <=  30 AND total_visits >= 5
                     AND spend_per_bill >= 750 AND distinct_months >= 3  THEN 'STAR'
                    WHEN recency_days <=  30 AND total_visits >= 4
                     AND spend_per_bill <  750 AND distinct_months >= 3  THEN 'LOYAL'
                    WHEN recency_days <=  30 AND total_bills = 1         THEN 'New'
                    WHEN recency_days <=  30                             THEN 'Win Back'
                    WHEN recency_days <=  60                             THEN 'ACTIVE'
                    WHEN recency_days <= 120                             THEN 'Inactive'
                    WHEN recency_days <= 180                             THEN 'LAPSER'
                    ELSE 'Deep Lapsed'
                END
        END AS l2_segment
    FROM with_l1
)
SELECT
    mobile_number, unified_id, surrogate_id,
    first_bill_date, last_bill_date,
    recency_days, tenure_days,
    total_bills, total_visits, total_spend,
    spend_per_bill, spend_per_visit, avg_items_per_bill, total_discount,
    distinct_months, distinct_store_count, distinct_article_count,
    dgbt_fs, avg_billing_time_secs,
    return_bill_count, promo_bill_count, weekend_bill_count, wednesday_bill_count,
    fav_store_code, fav_store_name, fav_store_type,
    fav_day,
    fav_article_by_spend, fav_article_by_spend_desc,
    fav_article_by_nob,   fav_article_by_nob_desc,
    second_fav_article_by_spend, second_fav_article_by_nob,
    channel_presence,
    spend_decile::INT, nob_decile::INT,
    updated_at,
    l1_segment, l2_segment
FROM with_l2;

-- ===========================================================================
-- PHASE 3 : nb_silver_reverse_etl
-- ===========================================================================

-- ---------------------------------------------------------------------------
-- customer_behavioral_attributes — full 360 view for activation / reverse ETL
-- ---------------------------------------------------------------------------
TRUNCATE nb_silver_reverse_etl.customer_behavioral_attributes;

INSERT INTO nb_silver_reverse_etl.customer_behavioral_attributes (
    customer_id, mobile, display_name, email, city, pincode, region, registered_store,
    age, customer_group, occupation, whatsapp, dnd, gw_customer_flag,
    accepts_email_marketing, accepts_sms_marketing, surrogate_id,
    first_bill_date, last_bill_date, recency_days, tenure_days,
    total_bills, total_visits, total_spend, spend_per_bill, spend_per_visit,
    avg_items_per_bill, total_discount, distinct_months, distinct_store_count,
    distinct_article_count, dgbt_fs, avg_billing_time_secs,
    return_bill_count, promo_bill_count, weekend_bill_count, wednesday_bill_count,
    fav_store_code, fav_store_name, fav_store_type, fav_day,
    fav_article_by_spend, fav_article_by_spend_desc,
    fav_article_by_nob,   fav_article_by_nob_desc,
    second_fav_article_by_spend, second_fav_article_by_nob,
    channel_presence, spend_decile, nob_decile, l1_segment, l2_segment,
    store_spend, online_spend, store_bills, online_bills,
    lifecycle_stage,
    rfm_recency_score, rfm_frequency_score, rfm_monetary_score, computed_at
)
WITH profiles AS (
    SELECT * FROM nb_silver_identity.unified_profiles
),
txn_summary AS (
    SELECT * FROM nb_silver_gold.customer_transaction_summary
),
channel_summary AS (
    SELECT
        mobile_number,
        MAX(CASE WHEN delivery_channel = 'Store'  THEN spend END) AS store_spend,
        MAX(CASE WHEN delivery_channel = 'Online' THEN spend END) AS online_spend,
        MAX(CASE WHEN delivery_channel = 'Store'  THEN bills END) AS store_bills,
        MAX(CASE WHEN delivery_channel = 'Online' THEN bills END) AS online_bills
    FROM nb_silver_gold.customer_channel_summary
    GROUP BY mobile_number
)
SELECT
    p.unified_id            AS customer_id,
    p.canonical_mobile      AS mobile,
    p.display_name,
    p.email,
    p.city,
    p.pincode,
    p.region,
    p.registered_store,
    p.age,
    p.customer_group,
    p.occupation,
    p.whatsapp,
    p.dnd,
    p.gw_customer_flag,
    p.accepts_email_marketing,
    p.accepts_sms_marketing,
    p.surrogate_id,
    t.first_bill_date,
    t.last_bill_date,
    t.recency_days,
    t.tenure_days,
    t.total_bills,
    t.total_visits,
    t.total_spend,
    t.spend_per_bill,
    t.spend_per_visit,
    t.avg_items_per_bill,
    t.total_discount,
    t.distinct_months,
    t.distinct_store_count,
    t.distinct_article_count,
    t.dgbt_fs,
    t.avg_billing_time_secs,
    t.return_bill_count,
    t.promo_bill_count,
    t.weekend_bill_count,
    t.wednesday_bill_count,
    t.fav_store_code,
    t.fav_store_name,
    t.fav_store_type,
    t.fav_day,
    t.fav_article_by_spend,
    t.fav_article_by_spend_desc,
    t.fav_article_by_nob,
    t.fav_article_by_nob_desc,
    t.second_fav_article_by_spend,
    t.second_fav_article_by_nob,
    t.channel_presence,
    t.spend_decile,
    t.nob_decile,
    t.l1_segment,
    t.l2_segment,
    cs.store_spend,
    cs.online_spend,
    cs.store_bills,
    cs.online_bills,
    CASE
        WHEN t.mobile_number IS NULL THEN 'Registered'
        WHEN t.recency_days <=  30   THEN 'Active'
        WHEN t.recency_days <=  90   THEN 'At Risk'
        WHEN t.recency_days <= 180   THEN 'Lapsed'
        ELSE 'Churned'
    END                                                         AS lifecycle_stage,
    NTILE(5) OVER (ORDER BY COALESCE(t.recency_days, 9999) DESC)::INT AS rfm_recency_score,
    NTILE(5) OVER (ORDER BY COALESCE(t.total_bills,  0))::INT          AS rfm_frequency_score,
    NTILE(5) OVER (ORDER BY COALESCE(t.total_spend,  0))::INT          AS rfm_monetary_score,
    NOW()                                                       AS computed_at
FROM profiles p
LEFT JOIN txn_summary   t  ON p.canonical_mobile = t.mobile_number
LEFT JOIN channel_summary cs ON p.canonical_mobile = cs.mobile_number;

COMMIT;

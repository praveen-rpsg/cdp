-- ============================================================
-- NBL Schema Replication Script
-- Creates nb_silver_gold, nb_silver_reverse_etl,
-- nb_silver_identity, nb_staging with all objects
-- mirroring Spencers equivalents in cdp_meta.
--
-- SAFE: Only creates NBL schemas/objects.
-- Does NOT touch any Spencers schemas.
-- ============================================================

-- --------------------------------------------------------
-- 1. CREATE SCHEMAS
-- --------------------------------------------------------
CREATE SCHEMA IF NOT EXISTS nb_silver_gold    AUTHORIZATION cdp;
CREATE SCHEMA IF NOT EXISTS nb_silver_reverse_etl AUTHORIZATION cdp;
CREATE SCHEMA IF NOT EXISTS nb_silver_identity    AUTHORIZATION cdp;
CREATE SCHEMA IF NOT EXISTS nb_staging            AUTHORIZATION cdp;


-- --------------------------------------------------------
-- 2. nb_silver_gold TABLES
-- (mirrors silver_gold schema)
-- --------------------------------------------------------

CREATE TABLE IF NOT EXISTS nb_silver_gold.customer_channel_summary (
    mobile_number    text,
    delivery_channel text,
    bills            bigint,
    visits           bigint,
    spend            numeric,
    spend_per_bill   numeric,
    spend_per_visit  numeric,
    total_discount   numeric,
    first_bill_date  date,
    last_bill_date   date
);

CREATE TABLE IF NOT EXISTS nb_silver_gold.customer_product_summary (
    mobile_number  text,
    brick          text,
    brick_desc     text,
    category_code  text,
    category_desc  text,
    family         text,
    family_desc    text,
    bills          bigint,
    visits         bigint,
    spend          numeric,
    spend_per_bill numeric,
    total_discount numeric
);

CREATE TABLE IF NOT EXISTS nb_silver_gold.customer_transaction_summary (
    mobile_number                text,
    unified_id                   text,
    surrogate_id                 text,
    first_bill_date              date,
    last_bill_date               date,
    recency_days                 integer,
    tenure_days                  integer,
    total_bills                  bigint,
    total_visits                 bigint,
    total_spend                  numeric,
    spend_per_bill               numeric,
    spend_per_visit              numeric,
    avg_items_per_bill           numeric,
    total_discount               numeric,
    distinct_months              bigint,
    distinct_store_count         bigint,
    distinct_article_count       bigint,
    dgbt_fs                      integer,
    avg_billing_time_secs        numeric,
    return_bill_count            bigint,
    promo_bill_count             bigint,
    weekend_bill_count           bigint,
    wednesday_bill_count         bigint,
    fav_store_code               text,
    fav_store_name               text,
    fav_store_type               text,
    fav_day                      text,
    fav_article_by_spend         text,
    fav_article_by_spend_desc    text,
    fav_article_by_nob           text,
    fav_article_by_nob_desc      text,
    second_fav_article_by_spend  text,
    second_fav_article_by_nob    text,
    channel_presence             text,
    spend_decile                 integer,
    nob_decile                   integer,
    updated_at                   timestamp with time zone,
    l1_segment                   text,
    l2_segment                   text
);

CREATE TABLE IF NOT EXISTS nb_silver_gold.daily_store_sales (
    bill_date          date,
    store_code         text,
    article_code       text,
    article_description text,
    segment_code       text,
    segment_desc       text,
    family_code        text,
    family_desc        text,
    class_code         text,
    class_desc         text,
    brick_code         text,
    brick_desc         text,
    brand_code         text,
    brand_desc         text,
    store_format       text,
    region_code        text,
    region_desc        text,
    city               text,
    city_desc          text,
    sales_channel      text,
    quantity           numeric,
    gross_sales        numeric,
    discount_amount    numeric,
    net_sales          numeric,
    line_item_count    bigint,
    num_bills          bigint,
    created_at         timestamp with time zone
);


-- --------------------------------------------------------
-- 3. nb_silver_reverse_etl TABLES
-- (mirrors silver_reverse_etl schema)
-- --------------------------------------------------------

CREATE TABLE IF NOT EXISTS nb_silver_reverse_etl.customer_behavioral_attributes (
    customer_id                 text,
    mobile                      text,
    display_name                text,
    email                       text,
    city                        text,
    pincode                     text,
    region                      text,
    registered_store            text,
    age                         integer,
    customer_group              text,
    occupation                  text,
    whatsapp                    text,
    dnd                         text,
    gw_customer_flag            text,
    accepts_email_marketing     text,
    accepts_sms_marketing       text,
    surrogate_id                text,
    first_bill_date             date,
    last_bill_date              date,
    recency_days                integer,
    tenure_days                 integer,
    total_bills                 bigint,
    total_visits                bigint,
    total_spend                 numeric,
    spend_per_bill              numeric,
    spend_per_visit             numeric,
    avg_items_per_bill          numeric,
    total_discount              numeric,
    distinct_months             bigint,
    distinct_store_count        bigint,
    distinct_article_count      bigint,
    dgbt_fs                     integer,
    avg_billing_time_secs       numeric,
    return_bill_count           bigint,
    promo_bill_count            bigint,
    weekend_bill_count          bigint,
    wednesday_bill_count        bigint,
    fav_store_code              text,
    fav_store_name              text,
    fav_store_type              text,
    fav_day                     text,
    fav_article_by_spend        text,
    fav_article_by_spend_desc   text,
    fav_article_by_nob          text,
    fav_article_by_nob_desc     text,
    second_fav_article_by_spend text,
    second_fav_article_by_nob   text,
    channel_presence            text,
    spend_decile                integer,
    nob_decile                  integer,
    l1_segment                  text,
    l2_segment                  text,
    store_spend                 numeric,
    online_spend                numeric,
    store_bills                 bigint,
    online_bills                bigint,
    lifecycle_stage             text,
    rfm_recency_score           integer,
    rfm_frequency_score         integer,
    rfm_monetary_score          integer,
    computed_at                 timestamp with time zone
);


-- --------------------------------------------------------
-- 4. nb_silver_identity TABLES
-- (mirrors silver_identity schema)
-- --------------------------------------------------------

CREATE TABLE IF NOT EXISTS nb_silver_identity.identity_edges (
    unified_id       text,
    identifier_type  text,
    identifier_value text,
    source_system    text,
    confidence_score numeric
);

CREATE TABLE IF NOT EXISTS nb_silver_identity.identity_graph_summary (
    unified_id           text,
    canonical_mobile     text,
    display_name         text,
    primary_source       text,
    has_transactions     boolean,
    total_edge_count     bigint,
    distinct_id_types    bigint,
    source_system_count  bigint,
    source_systems       text,
    mobile_count         bigint,
    email_count          bigint,
    store_count          bigint,
    avg_confidence       numeric,
    completeness_score   numeric
);

CREATE TABLE IF NOT EXISTS nb_silver_identity.unified_profiles (
    unified_id               text,
    canonical_mobile         text,
    surrogate_id             text,
    cih_original_surrogate_id text,
    display_name             text,
    first_name               text,
    last_name                text,
    email                    text,
    city                     text,
    pincode                  text,
    street                   text,
    region                   text,
    registered_store         text,
    status                   text,
    dob                      text,
    age                      integer,
    customer_group           text,
    occupation               text,
    whatsapp                 text,
    dnd                      text,
    gw_customer_flag         text,
    accepts_email_marketing  text,
    accepts_sms_marketing    text,
    cih_total_orders         integer,
    cih_total_spent          numeric,
    primary_source           text,
    has_transactions         boolean,
    profile_updated_at       timestamp with time zone
);

-- Index mirroring silver_identity
CREATE INDEX IF NOT EXISTS idx_nb_p_registered_store
    ON nb_silver_identity.unified_profiles USING btree (registered_store);


-- --------------------------------------------------------
-- 5. nb_staging VIEWS
-- (mirrors staging schema; all bronze.* → nb_bronze.*)
-- --------------------------------------------------------

-- stg_articles
CREATE OR REPLACE VIEW nb_staging.stg_articles AS
WITH source AS (
    SELECT
        raw_article_master.article,
        raw_article_master.article_description,
        raw_article_master.segment_code,
        raw_article_master.segment_desc,
        raw_article_master.family_code,
        raw_article_master.family_desc,
        raw_article_master.class_code,
        raw_article_master.class_desc,
        raw_article_master.brick_code,
        raw_article_master.brick_desc,
        raw_article_master.manufacturer_code,
        raw_article_master.manufacturer_desc,
        raw_article_master.brand_code,
        raw_article_master.brand_desc,
        raw_article_master.category_code,
        raw_article_master.category_desc,
        raw_article_master.subcategory_code,
        raw_article_master.subcategory_desc,
        raw_article_master.created_on,
        raw_article_master.last_change,
        raw_article_master.created_by,
        raw_article_master.clt,
        raw_article_master.article_type,
        raw_article_master.old_article_no,
        raw_article_master.base_uom,
        raw_article_master.lab_off,
        raw_article_master.valid_from,
        raw_article_master.ean_upc,
        raw_article_master.ct,
        raw_article_master.document,
        raw_article_master.pgr,
        raw_article_master.temp,
        raw_article_master.net_weight,
        raw_article_master.weight_uom,
        raw_article_master.status,
        raw_article_master.group_code,
        raw_article_master.disposal_status,
        raw_article_master.listing,
        raw_article_master.article_category,
        raw_article_master.length,
        raw_article_master.length_uom,
        raw_article_master.width,
        raw_article_master.width_uom,
        raw_article_master.height,
        raw_article_master.height_uom,
        raw_article_master.shelf_life,
        raw_article_master.remaining_shelf_life,
        raw_article_master.tagging,
        raw_article_master.season,
        raw_article_master.season_year,
        raw_article_master.hsn_code,
        raw_article_master.nbl_sku_code,
        raw_article_master.tgs,
        raw_article_master._loaded_at
    FROM nb_bronze.raw_article_master
)
SELECT
    TRIM(article)             AS article_code,
    TRIM(article_description) AS article_description,
    TRIM(segment_code)        AS segment_code,
    TRIM(segment_desc)        AS segment_desc,
    TRIM(family_code)         AS family_code,
    TRIM(family_desc)         AS family_desc,
    TRIM(class_code)          AS class_code,
    TRIM(class_desc)          AS class_desc,
    TRIM(brick_code)          AS brick_code,
    TRIM(brick_desc)          AS brick_desc,
    TRIM(manufacturer_code)   AS manufacturer_code,
    TRIM(manufacturer_desc)   AS manufacturer_desc,
    TRIM(brand_code)          AS brand_code,
    TRIM(brand_desc)          AS brand_desc,
    TRIM(category_code)       AS category_code,
    TRIM(category_desc)       AS category_desc,
    TRIM(subcategory_code)    AS subcategory_code,
    TRIM(subcategory_desc)    AS subcategory_desc,
    TRIM(base_uom)            AS base_uom,
    TRIM(ean_upc)             AS ean_upc,
    TRIM(hsn_code)            AS hsn_code,
    CASE
        WHEN net_weight ~ '^\d' THEN net_weight::numeric
        ELSE NULL::numeric
    END AS net_weight,
    TRIM(weight_uom)  AS weight_uom,
    TRIM(status)      AS status,
    TRIM(article_type) AS article_type,
    CASE
        WHEN created_on ~ '^\d{8}$' THEN to_date(created_on, 'YYYYMMDD')
        ELSE NULL::date
    END AS created_on,
    CASE
        WHEN last_change ~ '^\d{8}$' THEN to_date(last_change, 'YYYYMMDD')
        ELSE NULL::date
    END AS last_changed
FROM source
WHERE article IS NOT NULL;


-- stg_bill_transactions
CREATE OR REPLACE VIEW nb_staging.stg_bill_transactions AS
WITH source AS (
    SELECT
        raw_bill_delta.bill_date,
        raw_bill_delta.plant,
        raw_bill_delta.plant_desc,
        raw_bill_delta.till_no,
        raw_bill_delta.bill_no,
        raw_bill_delta.line_item_no,
        raw_bill_delta.condition_type,
        raw_bill_delta.promo_indicator,
        raw_bill_delta.city,
        raw_bill_delta.city_desc,
        raw_bill_delta.store_format,
        raw_bill_delta.region,
        raw_bill_delta.region_desc,
        raw_bill_delta.manual_bill_date,
        raw_bill_delta.cal_year_month,
        raw_bill_delta.calendar_year,
        raw_bill_delta.calendar_year_week,
        raw_bill_delta.bill_start_time,
        raw_bill_delta.bill_end_time,
        raw_bill_delta.cashier_no,
        raw_bill_delta.cashier_name,
        raw_bill_delta.gstin,
        raw_bill_delta.deliv_slot,
        raw_bill_delta.deliv_challan_gen,
        raw_bill_delta.portal_order_no,
        raw_bill_delta.order_date_time,
        raw_bill_delta.manual_bill_no,
        raw_bill_delta.article,
        raw_bill_delta.article_desc,
        raw_bill_delta.segment,
        raw_bill_delta.segment_desc,
        raw_bill_delta.family,
        raw_bill_delta.family_desc,
        raw_bill_delta.class,
        raw_bill_delta.class_desc,
        raw_bill_delta.brick,
        raw_bill_delta.brick_desc,
        raw_bill_delta.manufacturer,
        raw_bill_delta.manufacturer_name,
        raw_bill_delta.brand,
        raw_bill_delta.brand_name,
        raw_bill_delta.base_unit,
        raw_bill_delta.base_unit_desc,
        raw_bill_delta.gift_item_indicator,
        raw_bill_delta.indicator_for_bill,
        raw_bill_delta.sales_return,
        raw_bill_delta.line_item_count,
        raw_bill_delta.liquidity_type,
        raw_bill_delta.mobile_number,
        raw_bill_delta.otp_feed_time,
        raw_bill_delta.otp_gen_time,
        raw_bill_delta.card_number_1,
        raw_bill_delta.card_number_2,
        raw_bill_delta.card_number_3,
        raw_bill_delta.card_number_4,
        raw_bill_delta.card_number_5,
        raw_bill_delta.card_number_6,
        raw_bill_delta.card_number_7,
        raw_bill_delta.card_number_8,
        raw_bill_delta.vka0_discount_value,
        raw_bill_delta.ka04_discount_value,
        raw_bill_delta.ka02_discount_value,
        raw_bill_delta.z006_discount_value,
        raw_bill_delta.z007_discount_value,
        raw_bill_delta.zpro_discount_value,
        raw_bill_delta.zfre_discount_value,
        raw_bill_delta.zcat_discount_value,
        raw_bill_delta.zemp_discount_value,
        raw_bill_delta.zbil_discount_value,
        raw_bill_delta.total_manual_bill_disc,
        raw_bill_delta.queue_length,
        raw_bill_delta.billed_mrp,
        raw_bill_delta.billed_qty,
        raw_bill_delta.total_mrp_value,
        raw_bill_delta.gross_sale_value,
        raw_bill_delta.total_discount,
        raw_bill_delta._loaded_at,
        raw_bill_delta._source_file
    FROM nb_bronze.raw_bill_delta
), cleaned AS (
    SELECT
        source.bill_date::date                                    AS bill_date,
        TRIM(source.plant)                                        AS store_code,
        TRIM(source.plant_desc)                                   AS store_desc,
        TRIM(source.store_format)                                 AS store_format,
        TRIM(source.region)                                       AS region,
        TRIM(source.region_desc)                                  AS region_desc,
        TRIM(source.city)                                         AS city,
        TRIM(source.city_desc)                                    AS city_desc,
        TRIM(source.plant) || '-' || TRIM(source.bill_no) || '-' || TRIM(source.bill_date) AS bill_id,
        TRIM(source.bill_no)                                      AS bill_no,
        TRIM(source.line_item_no)                                 AS line_item_no,
        NULLIF(TRIM(source.line_item_count), '')::integer         AS line_item_count,
        TRIM(source.till_no)                                      AS till_no,
        TRIM(source.article)                                      AS article,
        TRIM(source.article_desc)                                 AS article_desc,
        TRIM(source.segment)                                      AS segment,
        TRIM(source.segment_desc)                                 AS segment_desc,
        TRIM(source.family)                                       AS family,
        TRIM(source.family_desc)                                  AS family_desc,
        TRIM(source.class)                                        AS class,
        TRIM(source.class_desc)                                   AS class_desc,
        TRIM(source.brick)                                        AS brick,
        TRIM(source.brick_desc)                                   AS brick_desc,
        TRIM(source.brand)                                        AS brand,
        TRIM(source.brand_name)                                   AS brand_name,
        TRIM(source.manufacturer)                                 AS manufacturer,
        TRIM(source.manufacturer_name)                            AS manufacturer_name,
        NULLIF(TRIM(source.billed_mrp),         '')::numeric      AS billed_mrp,
        NULLIF(TRIM(source.billed_qty),         '')::numeric      AS billed_qty,
        NULLIF(TRIM(source.total_mrp_value),    '')::numeric      AS total_mrp_value,
        NULLIF(TRIM(source.gross_sale_value),   '')::numeric      AS gross_sale_value,
        NULLIF(TRIM(source.total_discount),     '')::numeric      AS total_discount,
        NULLIF(TRIM(source.vka0_discount_value),'')::numeric      AS vka0_discount_value,
        NULLIF(TRIM(source.ka04_discount_value),'')::numeric      AS ka04_discount_value,
        NULLIF(TRIM(source.ka02_discount_value),'')::numeric      AS ka02_discount_value,
        NULLIF(TRIM(source.z006_discount_value),'')::numeric      AS z006_discount_value,
        NULLIF(TRIM(source.z007_discount_value),'')::numeric      AS z007_discount_value,
        NULLIF(TRIM(source.zpro_discount_value),'')::numeric      AS zpro_discount_value,
        NULLIF(TRIM(source.zfre_discount_value),'')::numeric      AS zfre_discount_value,
        NULLIF(TRIM(source.zcat_discount_value),'')::numeric      AS zcat_discount_value,
        NULLIF(TRIM(source.zemp_discount_value),'')::numeric      AS zemp_discount_value,
        NULLIF(TRIM(source.zbil_discount_value),'')::numeric      AS zbil_discount_value,
        NULLIF(TRIM(source.total_manual_bill_disc),'')::numeric   AS total_manual_bill_disc,
        CASE
            WHEN TRIM(source.mobile_number) IS NULL                              THEN NULL
            WHEN TRIM(source.mobile_number) = ANY(ARRAY['','-','X','0'])         THEN NULL
            WHEN length(regexp_replace(regexp_replace(TRIM(source.mobile_number),
                    '^\+?91',''), '[^0-9]','','g')) = 10
                THEN regexp_replace(regexp_replace(TRIM(source.mobile_number),
                        '^\+?91',''), '[^0-9]','','g')
            ELSE NULL
        END AS mobile_number,
        TRIM(source.promo_indicator)       AS promo_indicator,
        TRIM(source.condition_type)        AS condition_type,
        TRIM(source.gift_item_indicator)   AS gift_item_indicator,
        TRIM(source.liquidity_type)        AS liquidity_type,
        CASE
            WHEN TRIM(source.sales_return) = '+' THEN true
            WHEN TRIM(source.sales_return) = '-' THEN false
            ELSE false
        END AS sales_return,
        TRIM(source.bill_start_time)       AS bill_start_time,
        TRIM(source.bill_end_time)         AS bill_end_time,
        CASE
            WHEN TRIM(source.till_no) = ANY(ARRAY['9998','9999']) THEN 'Online'
            ELSE 'Store'
        END AS delivery_channel,
        CASE
            WHEN EXTRACT(dow FROM source.bill_date::date) = ANY(ARRAY[0::numeric, 6::numeric]) THEN true
            ELSE false
        END AS weekend_flag,
        CASE
            WHEN EXTRACT(dow FROM source.bill_date::date) = 3 THEN true
            ELSE false
        END AS wednesday_flag,
        CASE
            WHEN EXTRACT(day FROM source.bill_date::date) <= 7 THEN true
            ELSE false
        END AS first_week_flag,
        CASE
            WHEN EXTRACT(day FROM source.bill_date::date) >=
                 EXTRACT(day FROM (date_trunc('month', source.bill_date::date::timestamptz) + '1 mon'::interval - '1 day'::interval) ) - 6
            THEN true
            ELSE false
        END AS last_week_flag,
        CASE
            WHEN EXTRACT(day FROM source.bill_date::date) <= 15 THEN true
            ELSE false
        END AS first_half_flag,
        to_char(source.bill_date::date::timestamptz, 'Day') AS day_of_week,
        source._source_file
    FROM source
    WHERE source.bill_date IS NOT NULL
      AND TRIM(source.bill_date) <> ''
      AND TRIM(source.bill_date) <> 'BILL_DATE'
      AND source.bill_date ~ '^\d{4}-\d{2}-\d{2}'
)
SELECT
    bill_date, store_code, store_desc, store_format, region, region_desc,
    city, city_desc, bill_id, bill_no, line_item_no, line_item_count,
    till_no, article, article_desc, segment, segment_desc, family,
    family_desc, class, class_desc, brick, brick_desc, brand, brand_name,
    manufacturer, manufacturer_name, billed_mrp, billed_qty,
    total_mrp_value, gross_sale_value, total_discount,
    vka0_discount_value, ka04_discount_value, ka02_discount_value,
    z006_discount_value, z007_discount_value, zpro_discount_value,
    zfre_discount_value, zcat_discount_value, zemp_discount_value,
    zbil_discount_value, total_manual_bill_disc, mobile_number,
    promo_indicator, condition_type, gift_item_indicator, liquidity_type,
    sales_return, bill_start_time, bill_end_time, delivery_channel,
    weekend_flag, wednesday_flag, first_week_flag, last_week_flag,
    first_half_flag, day_of_week, _source_file
FROM cleaned;


-- stg_bill_identifiers (references nb_staging.stg_bill_transactions)
CREATE OR REPLACE VIEW nb_staging.stg_bill_identifiers AS
SELECT DISTINCT
    mobile_number                          AS mobile,
    'POS_' || md5(mobile_number)           AS surrogate_id,
    store_code,
    max(bill_date)                         AS last_seen_at,
    'POS'::text                            AS source_system
FROM nb_staging.stg_bill_transactions
WHERE mobile_number IS NOT NULL
GROUP BY mobile_number, store_code;


-- stg_cashback_identifiers
CREATE OR REPLACE VIEW nb_staging.stg_cashback_identifiers AS
WITH source AS (
    SELECT
        raw_promo_cashback.mobile_number,
        raw_promo_cashback.promo_id,
        raw_promo_cashback.start_date,
        raw_promo_cashback.end_date,
        raw_promo_cashback.create_date,
        raw_promo_cashback.channel,
        raw_promo_cashback.amount,
        raw_promo_cashback._loaded_at
    FROM nb_bronze.raw_promo_cashback
), cleaned AS (
    SELECT
        regexp_replace(regexp_replace(TRIM(source.mobile_number), '^\+91',''), '[^0-9]','','g') AS mobile,
        source.promo_id,
        source.channel,
        source.amount::numeric AS amount,
        'PROMO_CASHBACK'::text AS source_system
    FROM source
    WHERE source.mobile_number IS NOT NULL
      AND length(regexp_replace(TRIM(source.mobile_number), '[^0-9]','','g')) = 10
)
SELECT mobile, promo_id, channel, amount, source_system
FROM cleaned;


-- stg_cih_profiles (references nb_bronze.raw_cih_profiles)
CREATE OR REPLACE VIEW nb_staging.stg_cih_profiles AS
WITH source AS (
    SELECT
        raw_cih_profiles.brand_id,
        raw_cih_profiles.r1,
        raw_cih_profiles.name,
        raw_cih_profiles.city,
        raw_cih_profiles.created_at,
        raw_cih_profiles.customer_group,
        raw_cih_profiles.device_history,
        raw_cih_profiles.dob,
        raw_cih_profiles.dob_day,
        raw_cih_profiles.dob_month,
        raw_cih_profiles.dob_year,
        raw_cih_profiles.email_domain,
        raw_cih_profiles.last_device,
        raw_cih_profiles.occupation,
        raw_cih_profiles.pincode,
        raw_cih_profiles.status,
        raw_cih_profiles.store_code,
        raw_cih_profiles.street,
        raw_cih_profiles.region,
        raw_cih_profiles.employee,
        raw_cih_profiles.pd_preferred_store,
        raw_cih_profiles.pd_subscription_end_date,
        raw_cih_profiles.whatsapp,
        raw_cih_profiles.age,
        raw_cih_profiles.dnd,
        raw_cih_profiles.pd_subscription_renewal_count,
        raw_cih_profiles.alternate_street_1,
        raw_cih_profiles.alternate_street_2,
        raw_cih_profiles.alternate_street_3,
        raw_cih_profiles.alternate_street_4,
        raw_cih_profiles.alternate_street_5,
        raw_cih_profiles.alternate_city_1,
        raw_cih_profiles.alternate_city_2,
        raw_cih_profiles.alternate_city_3,
        raw_cih_profiles.alternate_city_4,
        raw_cih_profiles.alternate_city_5,
        raw_cih_profiles.alternate_region_1,
        raw_cih_profiles.alternate_region_2,
        raw_cih_profiles.alternate_region_3,
        raw_cih_profiles.alternate_region_4,
        raw_cih_profiles.alternate_region_5,
        raw_cih_profiles.alternate_pincode_1,
        raw_cih_profiles.alternate_pincode_2,
        raw_cih_profiles.alternate_pincode_3,
        raw_cih_profiles.alternate_pincode_4,
        raw_cih_profiles.alternate_pincode_5,
        raw_cih_profiles.first_name,
        raw_cih_profiles.last_name,
        raw_cih_profiles.pd_store_contact_1,
        raw_cih_profiles.pd_store_contact_2,
        raw_cih_profiles.gw_customer_flag,
        raw_cih_profiles.accepts_email_marketing,
        raw_cih_profiles.accepts_sms_marketing,
        raw_cih_profiles.total_orders,
        raw_cih_profiles.total_spent,
        raw_cih_profiles.company,
        raw_cih_profiles.country,
        raw_cih_profiles.email_id,
        raw_cih_profiles.customer_surrogate_id,
        raw_cih_profiles._loaded_at,
        raw_cih_profiles._source_file
    FROM nb_bronze.raw_cih_profiles
), cleaned AS (
    SELECT
        TRIM(source.brand_id)                             AS mobile,
        'CIH_' || md5(TRIM(source.brand_id))             AS surrogate_id,
        NULLIF(NULLIF(source.name, '-1'), '')             AS name,
        NULLIF(NULLIF(source.first_name, '-1'), '')       AS first_name,
        NULLIF(NULLIF(source.last_name, '-1'), '')        AS last_name,
        NULLIF(NULLIF(source.email_id, '-1'), '')         AS email,
        NULLIF(NULLIF(source.city, '-1'), '')             AS city,
        NULLIF(NULLIF(source.pincode, '-1'), '')          AS pincode,
        NULLIF(NULLIF(source.street, '-1'), '')           AS street,
        NULLIF(NULLIF(source.region, '-1'), '')           AS region,
        NULLIF(NULLIF(source.store_code, '-1'), '')       AS store_code,
        NULLIF(NULLIF(source.status, '-1'), '')           AS status,
        NULLIF(NULLIF(source.dob, '-1'), '')              AS dob,
        CASE
            WHEN source.age IS NOT NULL AND source.age <> '-1' AND source.age ~ '^\d+$'
            THEN source.age::integer
            ELSE NULL::integer
        END AS age,
        NULLIF(NULLIF(source.customer_group, '-1'), '')   AS customer_group,
        NULLIF(NULLIF(source.email_domain, '-1'), '')     AS email_domain,
        NULLIF(NULLIF(source.occupation, '-1'), '')       AS occupation,
        NULLIF(NULLIF(source.whatsapp, '-1'), '')         AS whatsapp,
        NULLIF(NULLIF(source.dnd, '-1'), '')              AS dnd,
        NULLIF(NULLIF(source.gw_customer_flag, '-1'), '') AS gw_customer_flag,
        NULLIF(NULLIF(source.accepts_email_marketing, '-1'), '') AS accepts_email_marketing,
        NULLIF(NULLIF(source.accepts_sms_marketing, '-1'), '')  AS accepts_sms_marketing,
        CASE
            WHEN source.total_orders IS NOT NULL AND source.total_orders <> '-1'
                 AND source.total_orders ~ '^\d+$'
            THEN source.total_orders::integer
            ELSE NULL::integer
        END AS total_orders,
        CASE
            WHEN source.total_spent IS NOT NULL AND source.total_spent <> '-1'
                 AND source.total_spent ~ '^[\d.]+$'
            THEN source.total_spent::numeric
            ELSE NULL::numeric
        END AS total_spent,
        NULLIF(NULLIF(source.customer_surrogate_id, '-1'), '') AS cih_original_surrogate_id,
        'CIH'::text AS source_system
    FROM source
    WHERE source.brand_id IS NOT NULL
      AND TRIM(source.brand_id) <> ''
      AND TRIM(source.brand_id) <> '-1'
)
SELECT
    mobile, surrogate_id, name, first_name, last_name, email,
    city, pincode, street, region, store_code, status, dob, age,
    customer_group, email_domain, occupation, whatsapp, dnd,
    gw_customer_flag, accepts_email_marketing, accepts_sms_marketing,
    total_orders, total_spent, cih_original_surrogate_id, source_system
FROM cleaned;


-- stg_customers
CREATE OR REPLACE VIEW nb_staging.stg_customers AS
WITH source AS (
    SELECT
        raw_customer_data.address,
        raw_customer_data.created_at,
        raw_customer_data.last_updated_by,
        raw_customer_data.last_updated_in_store,
        raw_customer_data.last_updated_on,
        raw_customer_data.mobile,
        raw_customer_data.name,
        raw_customer_data.pincode,
        raw_customer_data._loaded_at
    FROM nb_bronze.raw_customer_data
), cleaned AS (
    SELECT
        regexp_replace(regexp_replace(TRIM(source.mobile), '^\+91',''), '[^0-9]','','g') AS mobile,
        TRIM(upper(source.name))     AS name,
        TRIM(source.address)         AS address,
        TRIM(source.pincode)         AS pincode,
        source.last_updated_in_store AS last_store_code,
        source.created_at            AS first_seen_at,
        source.last_updated_on       AS last_seen_at,
        'CRM'::text                  AS source_system,
        source._loaded_at
    FROM source
    WHERE source.mobile IS NOT NULL
      AND source.mobile <> ''
      AND length(regexp_replace(TRIM(source.mobile), '[^0-9]','','g')) = 10
)
SELECT mobile, name, address, pincode, last_store_code,
       first_seen_at, last_seen_at, source_system, _loaded_at
FROM cleaned;


-- stg_locations
CREATE OR REPLACE VIEW nb_staging.stg_locations AS
WITH source AS (
    SELECT
        raw_location_master.store_code,
        raw_location_master.store_name,
        raw_location_master.store_format,
        raw_location_master.store_zone,
        raw_location_master.store_business_region,
        raw_location_master.store_region_code,
        raw_location_master.store_state,
        raw_location_master.store_city_code,
        raw_location_master.store_city_description,
        raw_location_master.store_pincode,
        raw_location_master.store_address,
        raw_location_master.status,
        raw_location_master.store_opening_date,
        raw_location_master.store_closing_date,
        raw_location_master._loaded_at
    FROM nb_bronze.raw_location_master
)
SELECT
    TRIM(store_code)             AS store_code,
    TRIM(store_name)             AS store_name,
    TRIM(store_format)           AS store_format,
    TRIM(store_zone)             AS store_zone,
    TRIM(store_business_region)  AS store_business_region,
    TRIM(store_region_code)      AS store_region_code,
    TRIM(store_state)            AS store_state,
    TRIM(store_city_code)        AS store_city_code,
    TRIM(store_city_description) AS store_city_description,
    TRIM(store_pincode)          AS store_pincode,
    TRIM(store_address)          AS store_address,
    TRIM(status)                 AS status,
    CASE
        WHEN store_opening_date ~ '^\d{8}$' THEN to_date(store_opening_date, 'YYYYMMDD')
        ELSE NULL::date
    END AS store_opening_date,
    CASE
        WHEN store_closing_date ~ '^\d{8}$' THEN to_date(store_closing_date, 'YYYYMMDD')
        ELSE NULL::date
    END AS store_closing_date
FROM source
WHERE store_code IS NOT NULL;


-- stg_nps_identifiers
CREATE OR REPLACE VIEW nb_staging.stg_nps_identifiers AS
WITH source AS (
    SELECT
        raw_nps_survey.mobile,
        raw_nps_survey.store_code,
        raw_nps_survey.bill_date,
        raw_nps_survey.cleanliness_hygiene,
        raw_nps_survey.product_availability,
        raw_nps_survey.quality_freshness,
        raw_nps_survey.value_for_money,
        raw_nps_survey.promotional_offers,
        raw_nps_survey.staff_assistance,
        raw_nps_survey.checkout_experience,
        raw_nps_survey.overall_rating,
        raw_nps_survey._loaded_at
    FROM nb_bronze.raw_nps_survey
), cleaned AS (
    SELECT
        regexp_replace(regexp_replace(TRIM(source.mobile), '^\+91',''), '[^0-9]','','g') AS mobile,
        TRIM(source.store_code) AS store_code,
        source.bill_date,
        source.overall_rating,
        'NPS'::text AS source_system
    FROM source
    WHERE source.mobile IS NOT NULL
      AND length(regexp_replace(TRIM(source.mobile), '[^0-9]','','g')) = 10
)
SELECT mobile, store_code, bill_date, overall_rating, source_system
FROM cleaned;


-- stg_yvm_identifiers
CREATE OR REPLACE VIEW nb_staging.stg_yvm_identifiers AS
WITH source AS (
    SELECT
        raw_yvm_feedback.sl_no,
        raw_yvm_feedback.created_by,
        raw_yvm_feedback.store_city,
        raw_yvm_feedback.store_name,
        raw_yvm_feedback.store_code,
        raw_yvm_feedback.region,
        raw_yvm_feedback.mode_of_yvm,
        raw_yvm_feedback.reference_no,
        raw_yvm_feedback.cli_no,
        raw_yvm_feedback.pincode,
        raw_yvm_feedback.staff_behaviour,
        raw_yvm_feedback.store_cleanliness,
        raw_yvm_feedback.billing_efficiency,
        raw_yvm_feedback.product_quality,
        raw_yvm_feedback.products_availability,
        raw_yvm_feedback.overall_satisfaction,
        raw_yvm_feedback.feedback_text,
        raw_yvm_feedback.toll_free_notes,
        raw_yvm_feedback.salutation,
        raw_yvm_feedback.customer_name,
        raw_yvm_feedback.mobile_no,
        raw_yvm_feedback.landline_no,
        raw_yvm_feedback.email_id,
        raw_yvm_feedback.bill_date1,
        raw_yvm_feedback.bill_no1,
        raw_yvm_feedback.bill_amount1,
        raw_yvm_feedback.till_no1,
        raw_yvm_feedback.order_no1,
        raw_yvm_feedback.feedback_type,
        raw_yvm_feedback.feedback_class,
        raw_yvm_feedback.feedback_sub_class,
        raw_yvm_feedback.primary_tat,
        raw_yvm_feedback.categorized_by,
        raw_yvm_feedback.segment,
        raw_yvm_feedback.family,
        raw_yvm_feedback.class,
        raw_yvm_feedback.product_type,
        raw_yvm_feedback.brand_name,
        raw_yvm_feedback.item_description,
        raw_yvm_feedback.quantity,
        raw_yvm_feedback.resolution,
        raw_yvm_feedback.resolution_notes,
        raw_yvm_feedback.platform_type,
        raw_yvm_feedback.registration_datetime,
        raw_yvm_feedback._loaded_at
    FROM nb_bronze.raw_yvm_feedback
), cleaned AS (
    SELECT
        regexp_replace(regexp_replace(TRIM(source.mobile_no), '^\+91',''), '[^0-9]','','g') AS mobile,
        TRIM(upper(source.customer_name)) AS name,
        TRIM(source.email_id)             AS email,
        TRIM(source.pincode)              AS pincode,
        TRIM(source.store_code)           AS store_code,
        TRIM(source.store_city)           AS store_city,
        source.feedback_type,
        source.platform_type,
        'YVM'::text                       AS source_system
    FROM source
    WHERE source.mobile_no IS NOT NULL
      AND length(regexp_replace(TRIM(source.mobile_no), '[^0-9]','','g')) = 10
)
SELECT mobile, name, email, pincode, store_code, store_city,
       feedback_type, platform_type, source_system
FROM cleaned;


-- ============================================================
-- End of NBL Schema Setup Script
-- ============================================================

-- =============================================================================
-- NBL: Create and populate nb_silver.s_fact_bill_transactions
-- Source: nb_bronze.raw_bill_delta (all 76 raw columns)
-- Mirrors: silver.s_fact_bill_transactions (Spencers equivalent)
-- Idempotent: safe to re-run; TRUNCATE before INSERT
-- =============================================================================

BEGIN;

-- ---------------------------------------------------------------------------
-- DDL: partitioned fact table
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS nb_silver.s_fact_bill_transactions (
    bill_date               DATE NOT NULL,
    store_code              TEXT,
    store_desc              TEXT,
    till_no                 TEXT,
    bill_no                 TEXT,
    line_item_no            TEXT,
    bill_id                 TEXT,
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

CREATE TABLE IF NOT EXISTS nb_silver.s_fact_bill_transactions_2026_01
    PARTITION OF nb_silver.s_fact_bill_transactions
    FOR VALUES FROM ('2026-01-01') TO ('2026-02-01');

CREATE TABLE IF NOT EXISTS nb_silver.s_fact_bill_transactions_2026_02
    PARTITION OF nb_silver.s_fact_bill_transactions
    FOR VALUES FROM ('2026-02-01') TO ('2026-03-01');

CREATE INDEX IF NOT EXISTS idx_nb_bill_txn_mobile   ON nb_silver.s_fact_bill_transactions(mobile_number);
CREATE INDEX IF NOT EXISTS idx_nb_bill_txn_store    ON nb_silver.s_fact_bill_transactions(store_code);
CREATE INDEX IF NOT EXISTS idx_nb_bill_txn_article  ON nb_silver.s_fact_bill_transactions(article);
CREATE INDEX IF NOT EXISTS idx_nb_bill_txn_date     ON nb_silver.s_fact_bill_transactions(bill_date);

-- ---------------------------------------------------------------------------
-- Populate from nb_bronze.raw_bill_delta
-- Applies identical cleaning logic as nb_staging.stg_bill_transactions PLUS
-- the extra columns (calendar_year, calendar_year_week, base_unit,
-- indicator_for_bill) that exist in bronze but were omitted from the view.
-- ---------------------------------------------------------------------------
TRUNCATE nb_silver.s_fact_bill_transactions;

INSERT INTO nb_silver.s_fact_bill_transactions (
    bill_date, store_code, store_desc, till_no, bill_no, line_item_no,
    bill_id, condition_type, promo_indicator,
    city, city_desc, store_format, region, region_desc,
    calendar_year, calendar_year_week,
    bill_start_time, bill_end_time,
    article, article_desc, segment, segment_desc,
    family, family_desc, class, class_desc,
    brick, brick_desc, manufacturer, manufacturer_name,
    brand, brand_name, base_unit,
    gift_item_indicator, indicator_for_bill,
    sales_return, line_item_count, liquidity_type,
    mobile_number,
    gross_sale_value, total_discount, total_mrp_value,
    billed_qty, billed_mrp,
    vka0_discount, ka04_discount, ka02_discount,
    z006_discount, z007_discount, zpro_discount, zfre_discount,
    zcat_discount, zemp_discount, zbil_discount,
    weekend_flag, wednesday_flag, first_week_flag, last_week_flag, first_half_flag,
    day_of_week, delivery_channel,
    _source_file
)
SELECT
    bill_date::DATE                                                         AS bill_date,

    -- Store dimensions
    TRIM(plant)                                                             AS store_code,
    TRIM(plant_desc)                                                        AS store_desc,
    TRIM(till_no)                                                           AS till_no,
    TRIM(bill_no)                                                           AS bill_no,
    TRIM(line_item_no)                                                      AS line_item_no,

    -- Bill identity
    TRIM(plant) || '-' || TRIM(bill_no) || '-' || TRIM(bill_date)          AS bill_id,

    TRIM(condition_type)                                                    AS condition_type,
    TRIM(promo_indicator)                                                   AS promo_indicator,

    -- Geography
    TRIM(city)                                                              AS city,
    TRIM(city_desc)                                                         AS city_desc,
    TRIM(store_format)                                                      AS store_format,
    TRIM(region)                                                            AS region,
    TRIM(region_desc)                                                       AS region_desc,

    -- Calendar (present in bronze, absent from staging view)
    TRIM(calendar_year)                                                     AS calendar_year,
    TRIM(calendar_year_week)                                                AS calendar_year_week,

    -- Timestamps
    TRIM(bill_start_time)                                                   AS bill_start_time,
    TRIM(bill_end_time)                                                     AS bill_end_time,

    -- Article hierarchy
    TRIM(article)                                                           AS article,
    TRIM(article_desc)                                                      AS article_desc,
    TRIM(segment)                                                           AS segment,
    TRIM(segment_desc)                                                      AS segment_desc,
    TRIM(family)                                                            AS family,
    TRIM(family_desc)                                                       AS family_desc,
    TRIM(class)                                                             AS class,
    TRIM(class_desc)                                                        AS class_desc,
    TRIM(brick)                                                             AS brick,
    TRIM(brick_desc)                                                        AS brick_desc,
    TRIM(manufacturer)                                                      AS manufacturer,
    TRIM(manufacturer_name)                                                 AS manufacturer_name,
    TRIM(brand)                                                             AS brand,
    TRIM(brand_name)                                                        AS brand_name,

    -- Present in bronze, absent from staging view
    TRIM(base_unit)                                                         AS base_unit,
    TRIM(gift_item_indicator)                                               AS gift_item_indicator,
    TRIM(indicator_for_bill)                                                AS indicator_for_bill,

    -- Sales return flag
    CASE
        WHEN TRIM(sales_return) = '+' THEN TRUE
        WHEN TRIM(sales_return) = '-' THEN FALSE
        ELSE FALSE
    END                                                                     AS sales_return,

    NULLIF(TRIM(line_item_count), '')::INT                                  AS line_item_count,
    TRIM(liquidity_type)                                                    AS liquidity_type,

    -- Mobile: strip +91 prefix, keep only valid 10-digit numbers
    CASE
        WHEN TRIM(mobile_number) IS NULL                    THEN NULL
        WHEN TRIM(mobile_number) IN ('', '-', 'X', '0')    THEN NULL
        WHEN LENGTH(REGEXP_REPLACE(
                REGEXP_REPLACE(TRIM(mobile_number), '^\+?91', ''),
                '[^0-9]', '', 'g')) = 10
            THEN REGEXP_REPLACE(
                REGEXP_REPLACE(TRIM(mobile_number), '^\+?91', ''),
                '[^0-9]', '', 'g')
        ELSE NULL
    END                                                                     AS mobile_number,

    -- Monetary
    NULLIF(TRIM(gross_sale_value), '')::NUMERIC(15,2)                       AS gross_sale_value,
    NULLIF(TRIM(total_discount), '')::NUMERIC(15,2)                         AS total_discount,
    NULLIF(TRIM(total_mrp_value), '')::NUMERIC(15,2)                        AS total_mrp_value,
    NULLIF(TRIM(billed_qty), '')::NUMERIC(12,3)                             AS billed_qty,
    NULLIF(TRIM(billed_mrp), '')::NUMERIC(15,2)                             AS billed_mrp,

    -- Discounts (rename: drop _value suffix to match target schema)
    NULLIF(TRIM(vka0_discount_value), '')::NUMERIC(12,2)                    AS vka0_discount,
    NULLIF(TRIM(ka04_discount_value), '')::NUMERIC(12,2)                    AS ka04_discount,
    NULLIF(TRIM(ka02_discount_value), '')::NUMERIC(12,2)                    AS ka02_discount,
    NULLIF(TRIM(z006_discount_value), '')::NUMERIC(12,2)                    AS z006_discount,
    NULLIF(TRIM(z007_discount_value), '')::NUMERIC(12,2)                    AS z007_discount,
    NULLIF(TRIM(zpro_discount_value), '')::NUMERIC(12,2)                    AS zpro_discount,
    NULLIF(TRIM(zfre_discount_value), '')::NUMERIC(12,2)                    AS zfre_discount,
    NULLIF(TRIM(zcat_discount_value), '')::NUMERIC(12,2)                    AS zcat_discount,
    NULLIF(TRIM(zemp_discount_value), '')::NUMERIC(12,2)                    AS zemp_discount,
    NULLIF(TRIM(zbil_discount_value), '')::NUMERIC(12,2)                    AS zbil_discount,

    -- Derived flags
    CASE WHEN EXTRACT(DOW FROM bill_date::DATE) IN (0, 6) THEN TRUE ELSE FALSE END  AS weekend_flag,
    CASE WHEN EXTRACT(DOW FROM bill_date::DATE) = 3       THEN TRUE ELSE FALSE END  AS wednesday_flag,
    CASE WHEN EXTRACT(DAY FROM bill_date::DATE) <= 7      THEN TRUE ELSE FALSE END  AS first_week_flag,
    CASE
        WHEN EXTRACT(DAY FROM bill_date::DATE)
             >= EXTRACT(DAY FROM (DATE_TRUNC('month', bill_date::DATE) + INTERVAL '1 month' - INTERVAL '1 day')) - 6
        THEN TRUE ELSE FALSE
    END                                                                     AS last_week_flag,
    CASE WHEN EXTRACT(DAY FROM bill_date::DATE) <= 15     THEN TRUE ELSE FALSE END  AS first_half_flag,

    TO_CHAR(bill_date::DATE, 'Day')                                         AS day_of_week,

    CASE WHEN TRIM(till_no) IN ('9998', '9999') THEN 'Online' ELSE 'Store' END     AS delivery_channel,

    _source_file

FROM nb_bronze.raw_bill_delta
WHERE bill_date IS NOT NULL
  AND TRIM(bill_date) != ''
  AND TRIM(bill_date) != 'BILL_DATE'
  AND bill_date ~ '^\d{4}-\d{2}-\d{2}';

DO $$
DECLARE row_count BIGINT;
BEGIN
    SELECT COUNT(*) INTO row_count FROM nb_silver.s_fact_bill_transactions;
    RAISE NOTICE 'nb_silver.s_fact_bill_transactions: % rows loaded', row_count;
END $$;

COMMIT;

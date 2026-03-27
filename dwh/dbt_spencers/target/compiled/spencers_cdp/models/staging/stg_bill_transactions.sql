

-- Staging: Clean and type-cast POS bill transaction line items
-- Source: bronze.raw_bill_delta (RAW / unmasked bill delta files, 76 columns)
-- No customer_surrogate_id in this source; real MOBILE_NUMBER is available.

WITH source AS (
    SELECT * FROM "cdp_meta"."bronze"."raw_bill_delta"
),

cleaned AS (
    SELECT
        -- Date
        bill_date::DATE                                         AS bill_date,

        -- Store dimensions
        TRIM(plant)                                             AS store_code,
        TRIM(plant_desc)                                        AS store_desc,
        TRIM(store_format)                                      AS store_format,
        TRIM(region)                                            AS region,
        TRIM(region_desc)                                       AS region_desc,
        TRIM(city)                                              AS city,
        TRIM(city_desc)                                         AS city_desc,

        -- Bill identity
        TRIM(plant) || '-' || TRIM(bill_no) || '-' || TRIM(bill_date)
                                                                AS bill_id,
        TRIM(bill_no)                                           AS bill_no,
        TRIM(line_item_no)                                      AS line_item_no,
        NULLIF(TRIM(line_item_count), '')::INT                  AS line_item_count,
        TRIM(till_no)                                           AS till_no,

        -- Article / product hierarchy
        TRIM(article)                                           AS article,
        TRIM(article_desc)                                      AS article_desc,
        TRIM(segment)                                           AS segment,
        TRIM(segment_desc)                                      AS segment_desc,
        TRIM(family)                                            AS family,
        TRIM(family_desc)                                       AS family_desc,
        TRIM(class)                                             AS class,
        TRIM(class_desc)                                        AS class_desc,
        TRIM(brick)                                             AS brick,
        TRIM(brick_desc)                                        AS brick_desc,
        TRIM(brand)                                             AS brand,
        TRIM(brand_name)                                        AS brand_name,
        TRIM(manufacturer)                                      AS manufacturer,
        TRIM(manufacturer_name)                                 AS manufacturer_name,

        -- Monetary columns (cast from TEXT to NUMERIC, NULLIF for empty strings)
        NULLIF(TRIM(billed_mrp), '')::NUMERIC                   AS billed_mrp,
        NULLIF(TRIM(billed_qty), '')::NUMERIC                   AS billed_qty,
        NULLIF(TRIM(total_mrp_value), '')::NUMERIC              AS total_mrp_value,
        NULLIF(TRIM(gross_sale_value), '')::NUMERIC             AS gross_sale_value,
        NULLIF(TRIM(total_discount), '')::NUMERIC               AS total_discount,

        -- Individual discount columns
        NULLIF(TRIM(vka0_discount_value), '')::NUMERIC          AS vka0_discount_value,
        NULLIF(TRIM(ka04_discount_value), '')::NUMERIC          AS ka04_discount_value,
        NULLIF(TRIM(ka02_discount_value), '')::NUMERIC          AS ka02_discount_value,
        NULLIF(TRIM(z006_discount_value), '')::NUMERIC          AS z006_discount_value,
        NULLIF(TRIM(z007_discount_value), '')::NUMERIC          AS z007_discount_value,
        NULLIF(TRIM(zpro_discount_value), '')::NUMERIC          AS zpro_discount_value,
        NULLIF(TRIM(zfre_discount_value), '')::NUMERIC          AS zfre_discount_value,
        NULLIF(TRIM(zcat_discount_value), '')::NUMERIC          AS zcat_discount_value,
        NULLIF(TRIM(zemp_discount_value), '')::NUMERIC          AS zemp_discount_value,
        NULLIF(TRIM(zbil_discount_value), '')::NUMERIC          AS zbil_discount_value,
        NULLIF(TRIM(total_manual_bill_disc), '')::NUMERIC       AS total_manual_bill_disc,

        -- Customer identifier: clean mobile_number (real, unmasked)
        -- Strip non-digits, handle +91 prefix, keep only valid 10-digit numbers
        CASE
            WHEN TRIM(mobile_number) IS NULL
                THEN NULL
            WHEN TRIM(mobile_number) IN ('', '-', 'X', '0')
                THEN NULL
            WHEN LENGTH(REGEXP_REPLACE(
                    REGEXP_REPLACE(TRIM(mobile_number), '^\+?91', ''),
                    '[^0-9]', '', 'g'
                 )) = 10
                THEN REGEXP_REPLACE(
                    REGEXP_REPLACE(TRIM(mobile_number), '^\+?91', ''),
                    '[^0-9]', '', 'g'
                 )
            ELSE NULL
        END                                                     AS mobile_number,

        -- Promo / condition
        TRIM(promo_indicator)                                   AS promo_indicator,
        TRIM(condition_type)                                    AS condition_type,
        TRIM(gift_item_indicator)                               AS gift_item_indicator,
        TRIM(liquidity_type)                                    AS liquidity_type,

        -- Sales return flag: '-' → FALSE, '+' → TRUE, else FALSE
        CASE
            WHEN TRIM(sales_return) = '+' THEN TRUE
            WHEN TRIM(sales_return) = '-' THEN FALSE
            ELSE FALSE
        END                                                     AS sales_return,

        -- Timestamps
        TRIM(bill_start_time)                                   AS bill_start_time,
        TRIM(bill_end_time)                                     AS bill_end_time,

        -- Delivery channel derived from till_no
        CASE
            WHEN TRIM(till_no) IN ('9998', '9999') THEN 'Online'
            ELSE 'Store'
        END                                                     AS delivery_channel,

        -- Resulticks-design derived flags
        CASE
            WHEN EXTRACT(DOW FROM bill_date::DATE) IN (0, 6)
                THEN TRUE ELSE FALSE
        END                                                     AS weekend_flag,

        CASE
            WHEN EXTRACT(DOW FROM bill_date::DATE) = 3
                THEN TRUE ELSE FALSE
        END                                                     AS wednesday_flag,

        CASE
            WHEN EXTRACT(DAY FROM bill_date::DATE) <= 7
                THEN TRUE ELSE FALSE
        END                                                     AS first_week_flag,

        CASE
            WHEN EXTRACT(DAY FROM bill_date::DATE)
                 >= EXTRACT(DAY FROM (DATE_TRUNC('month', bill_date::DATE) + INTERVAL '1 month' - INTERVAL '1 day')) - 6
                THEN TRUE ELSE FALSE
        END                                                     AS last_week_flag,

        CASE
            WHEN EXTRACT(DAY FROM bill_date::DATE) <= 15
                THEN TRUE ELSE FALSE
        END                                                     AS first_half_flag,

        TO_CHAR(bill_date::DATE, 'Day')                         AS day_of_week,

        -- Lineage
        _source_file

    FROM source
    WHERE bill_date IS NOT NULL
      AND TRIM(bill_date) != ''
      AND TRIM(bill_date) != 'BILL_DATE'
      AND bill_date ~ '^\d{4}-\d{2}-\d{2}'
)

SELECT * FROM cleaned
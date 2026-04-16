

-- ETL: Load cleaned bill transactions from staging into silver fact table
-- Source: staging.stg_bill_transactions (view over bronze.raw_bill_delta)
-- Grain: One row per line item (bill_id + line_item_no)

WITH stg AS (
    SELECT * FROM "cdp_meta"."staging"."stg_bill_transactions"
)

SELECT
    -- Date
    bill_date,

    -- Store dimensions
    store_code,
    store_desc,
    store_format,
    till_no,
    region,
    region_desc,
    city,
    city_desc,

    -- Bill identity
    bill_no,
    line_item_no,
    line_item_count,
    bill_id,
    condition_type,

    -- Product hierarchy
    article,
    article_desc,
    segment,
    segment_desc,
    family,
    family_desc,
    class,
    class_desc,
    brick,
    brick_desc,
    brand,
    brand_name,
    manufacturer,
    manufacturer_name,

    -- Monetary
    gross_sale_value,
    total_discount,
    total_mrp_value,
    billed_qty,
    billed_mrp,

    -- Discount breakdown
    vka0_discount_value   AS vka0_discount,
    ka04_discount_value   AS ka04_discount,
    ka02_discount_value   AS ka02_discount,
    z006_discount_value   AS z006_discount,
    z007_discount_value   AS z007_discount,
    zpro_discount_value   AS zpro_discount,
    zfre_discount_value   AS zfre_discount,
    zcat_discount_value   AS zcat_discount,
    zemp_discount_value   AS zemp_discount,
    zbil_discount_value   AS zbil_discount,

    -- Customer identifier
    mobile_number,

    -- Flags
    sales_return,
    promo_indicator,
    gift_item_indicator,
    liquidity_type,
    delivery_channel,
    weekend_flag,
    wednesday_flag,
    first_week_flag,
    last_week_flag,
    first_half_flag,
    day_of_week,

    -- Lineage
    _source_file,
    NOW()                                           AS _loaded_at

FROM stg
WHERE mobile_number IS NOT NULL
  AND bill_date IS NOT NULL
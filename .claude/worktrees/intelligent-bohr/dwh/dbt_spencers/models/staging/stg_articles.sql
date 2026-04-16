-- Staging: Clean article master (ZABM)

WITH source AS (
    SELECT * FROM {{ source('bronze', 'raw_article_master') }}
)

SELECT
    TRIM(article) AS article_code,
    TRIM(article_description) AS article_description,
    TRIM(segment_code) AS segment_code,
    TRIM(segment_desc) AS segment_desc,
    TRIM(family_code) AS family_code,
    TRIM(family_desc) AS family_desc,
    TRIM(class_code) AS class_code,
    TRIM(class_desc) AS class_desc,
    TRIM(brick_code) AS brick_code,
    TRIM(brick_desc) AS brick_desc,
    TRIM(manufacturer_code) AS manufacturer_code,
    TRIM(manufacturer_desc) AS manufacturer_desc,
    TRIM(brand_code) AS brand_code,
    TRIM(brand_desc) AS brand_desc,
    TRIM(category_code) AS category_code,
    TRIM(category_desc) AS category_desc,
    TRIM(subcategory_code) AS subcategory_code,
    TRIM(subcategory_desc) AS subcategory_desc,
    TRIM(base_uom) AS base_uom,
    TRIM(ean_upc) AS ean_upc,
    TRIM(hsn_code) AS hsn_code,
    CASE WHEN net_weight ~ '^\d' THEN net_weight::NUMERIC END AS net_weight,
    TRIM(weight_uom) AS weight_uom,
    TRIM(status) AS status,
    TRIM(article_type) AS article_type,
    CASE WHEN created_on ~ '^\d{8}$' THEN TO_DATE(created_on, 'YYYYMMDD') END AS created_on,
    CASE WHEN last_change ~ '^\d{8}$' THEN TO_DATE(last_change, 'YYYYMMDD') END AS last_changed
FROM source
WHERE article IS NOT NULL

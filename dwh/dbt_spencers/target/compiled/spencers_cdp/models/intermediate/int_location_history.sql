-- Silver: Load location master into SCD Type 2 history table

WITH source AS (
    SELECT * FROM "cdp_meta"."staging"."stg_locations"
)

SELECT
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
    store_closing_date,
    TRUE AS is_latest,
    CURRENT_DATE AS valid_from,
    '9999-12-31'::DATE AS valid_to
FROM source
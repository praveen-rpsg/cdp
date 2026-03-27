
  create view "cdp_meta"."staging"."stg_locations__dbt_tmp"
    
    
  as (
    -- Staging: Clean location master

WITH source AS (
    SELECT * FROM "cdp_meta"."bronze"."raw_location_master"
)

SELECT
    TRIM(store_code) AS store_code,
    TRIM(store_name) AS store_name,
    TRIM(store_format) AS store_format,
    TRIM(store_zone) AS store_zone,
    TRIM(store_business_region) AS store_business_region,
    TRIM(store_region_code) AS store_region_code,
    TRIM(store_state) AS store_state,
    TRIM(store_city_code) AS store_city_code,
    TRIM(store_city_description) AS store_city_description,
    TRIM(store_pincode) AS store_pincode,
    TRIM(store_address) AS store_address,
    TRIM(status) AS status,
    CASE
        WHEN store_opening_date ~ '^\d{8}$'
        THEN TO_DATE(store_opening_date, 'YYYYMMDD')
    END AS store_opening_date,
    CASE
        WHEN store_closing_date ~ '^\d{8}$'
        THEN TO_DATE(store_closing_date, 'YYYYMMDD')
    END AS store_closing_date
FROM source
WHERE store_code IS NOT NULL
  );
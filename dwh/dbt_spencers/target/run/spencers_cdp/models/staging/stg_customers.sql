
  create view "cdp_meta"."staging"."stg_customers__dbt_tmp"
    
    
  as (
    -- Staging: Clean and normalize customer data
-- Source: bronze.raw_customer_data (CRM/Loyalty)

WITH source AS (
    SELECT * FROM "cdp_meta"."bronze"."raw_customer_data"
),

cleaned AS (
    SELECT
        -- Normalize mobile: strip spaces, leading zeros, +91 prefix
        REGEXP_REPLACE(
            REGEXP_REPLACE(TRIM(mobile), '^\+91', ''),
            '[^0-9]', '', 'g'
        ) AS mobile,
        TRIM(UPPER(name)) AS name,
        TRIM(address) AS address,
        TRIM(pincode) AS pincode,
        last_updated_in_store AS last_store_code,
        created_at::TIMESTAMPTZ AS first_seen_at,
        last_updated_on::TIMESTAMPTZ AS last_seen_at,
        'CRM' AS source_system,
        _loaded_at
    FROM source
    WHERE mobile IS NOT NULL
      AND mobile != ''
      AND LENGTH(REGEXP_REPLACE(TRIM(mobile), '[^0-9]', '', 'g')) = 10
)

SELECT * FROM cleaned
  );
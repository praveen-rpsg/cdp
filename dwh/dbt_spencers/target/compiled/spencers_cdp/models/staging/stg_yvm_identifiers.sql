-- Staging: Extract customer identifiers from YVM feedback data

WITH source AS (
    SELECT * FROM "cdp_meta"."bronze"."raw_yvm_feedback"
),

cleaned AS (
    SELECT
        REGEXP_REPLACE(
            REGEXP_REPLACE(TRIM(mobile_no::TEXT), '^\+91', ''),
            '[^0-9]', '', 'g'
        ) AS mobile,
        TRIM(UPPER(customer_name)) AS name,
        TRIM(email_id) AS email,
        TRIM(pincode::TEXT) AS pincode,
        TRIM(store_code) AS store_code,
        TRIM(store_city) AS store_city,
        feedback_type,
        platform_type,
        'YVM' AS source_system
    FROM source
    WHERE mobile_no IS NOT NULL
      AND LENGTH(REGEXP_REPLACE(TRIM(mobile_no::TEXT), '[^0-9]', '', 'g')) = 10
)

SELECT * FROM cleaned
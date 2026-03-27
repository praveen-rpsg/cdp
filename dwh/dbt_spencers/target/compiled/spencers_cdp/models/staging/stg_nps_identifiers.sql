-- Staging: Extract customer identifiers from NPS survey data

WITH source AS (
    SELECT * FROM "cdp_meta"."bronze"."raw_nps_survey"
),

cleaned AS (
    SELECT
        REGEXP_REPLACE(
            REGEXP_REPLACE(TRIM(mobile), '^\+91', ''),
            '[^0-9]', '', 'g'
        ) AS mobile,
        TRIM(store_code) AS store_code,
        bill_date,
        overall_rating::INT AS overall_rating,
        'NPS' AS source_system
    FROM source
    WHERE mobile IS NOT NULL
      AND LENGTH(REGEXP_REPLACE(TRIM(mobile), '[^0-9]', '', 'g')) = 10
)

SELECT * FROM cleaned
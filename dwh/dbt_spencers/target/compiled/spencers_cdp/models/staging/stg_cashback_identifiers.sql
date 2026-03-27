-- Staging: Extract customer identifiers from promo cashback data

WITH source AS (
    SELECT * FROM "cdp_meta"."bronze"."raw_promo_cashback"
),

cleaned AS (
    SELECT
        REGEXP_REPLACE(
            REGEXP_REPLACE(TRIM(mobile_number), '^\+91', ''),
            '[^0-9]', '', 'g'
        ) AS mobile,
        promo_id,
        channel,
        amount::NUMERIC AS amount,
        'PROMO_CASHBACK' AS source_system
    FROM source
    WHERE mobile_number IS NOT NULL
      AND LENGTH(REGEXP_REPLACE(TRIM(mobile_number), '[^0-9]', '', 'g')) = 10
)

SELECT * FROM cleaned
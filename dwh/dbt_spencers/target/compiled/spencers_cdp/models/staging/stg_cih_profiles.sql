

-- CIH = Customer Identity Hub golden records (~20M profiles)
-- brand_id IS the mobile number — used as the primary join key
-- surrogate_id = MD5 hash of mobile for a stable synthetic key

WITH source AS (
    SELECT * FROM "cdp_meta"."bronze"."raw_cih_profiles"
),

cleaned AS (
    SELECT
        -- brand_id IS the mobile number (primary key for joining)
        TRIM(brand_id)                                      AS mobile,
        -- Surrogate ID: hash of mobile
        'CIH_' || MD5(TRIM(brand_id))                      AS surrogate_id,

        NULLIF(NULLIF(name, '-1'), '')                      AS name,
        NULLIF(NULLIF(first_name, '-1'), '')                AS first_name,
        NULLIF(NULLIF(last_name, '-1'), '')                 AS last_name,
        NULLIF(NULLIF(email_id, '-1'), '')                  AS email,
        NULLIF(NULLIF(city, '-1'), '')                      AS city,
        NULLIF(NULLIF(pincode, '-1'), '')                   AS pincode,
        NULLIF(NULLIF(street, '-1'), '')                    AS street,
        NULLIF(NULLIF(region, '-1'), '')                    AS region,
        NULLIF(NULLIF(store_code, '-1'), '')                AS store_code,
        NULLIF(NULLIF(status, '-1'), '')                    AS status,
        NULLIF(NULLIF(dob, '-1'), '')                       AS dob,
        CASE
            WHEN age IS NOT NULL AND age != '-1' AND age ~ '^\d+$'
            THEN age::INT
            ELSE NULL
        END                                                 AS age,
        NULLIF(NULLIF(customer_group, '-1'), '')            AS customer_group,
        NULLIF(NULLIF(email_domain, '-1'), '')              AS email_domain,
        NULLIF(NULLIF(occupation, '-1'), '')                AS occupation,
        NULLIF(NULLIF(whatsapp, '-1'), '')                  AS whatsapp,
        NULLIF(NULLIF(dnd, '-1'), '')                       AS dnd,
        NULLIF(NULLIF(gw_customer_flag, '-1'), '')          AS gw_customer_flag,
        NULLIF(NULLIF(accepts_email_marketing, '-1'), '')   AS accepts_email_marketing,
        NULLIF(NULLIF(accepts_sms_marketing, '-1'), '')     AS accepts_sms_marketing,
        CASE
            WHEN total_orders IS NOT NULL AND total_orders != '-1' AND total_orders ~ '^\d+$'
            THEN total_orders::INT
            ELSE NULL
        END                                                 AS total_orders,
        CASE
            WHEN total_spent IS NOT NULL AND total_spent != '-1' AND total_spent ~ '^[\d.]+$'
            THEN total_spent::NUMERIC
            ELSE NULL
        END                                                 AS total_spent,
        NULLIF(NULLIF(customer_surrogate_id, '-1'), '')     AS cih_original_surrogate_id,
        'CIH'                                               AS source_system
    FROM source
    WHERE brand_id IS NOT NULL AND TRIM(brand_id) != '' AND TRIM(brand_id) != '-1'
)

SELECT * FROM cleaned
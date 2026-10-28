{{ config(
    materialized='table',
    indexes=[
        {'columns': ['cih_source_row_key'], 'unique': true}
    ]
) }}

WITH source AS (
    SELECT *
    FROM {{ source('bronze', 'cesc_cih_master') }}
),

normalized AS (
    SELECT
            MD5(
            CONCAT_WS(
                '|',
                COALESCE(r1_s, ''),
                COALESCE(name_s, ''),
                COALESCE(house_no_s, ''),
                COALESCE(floor_s, ''),
                COALESCE(street_s, ''),
                COALESCE(city_s, ''),
                COALESCE(pincode_s, ''),
                COALESCE(email_id_s, ''),
                COALESCE(brand_id_s, ''),
                COALESCE(is_mobile_b, ''),
                COALESCE(mobile_no_s, ''),
                COALESCE(email_domain_s, ''),
                COALESCE(dnd_s, ''),
                COALESCE(mode_of_payment_s, ''),
                COALESCE(payment_mode_description_s, ''),
                COALESCE(bill_ac_month_ss, ''),
                COALESCE(consumer_no_ss, ''),
                COALESCE(regional_office_ss, ''),
                COALESCE(consumer_type_ss, ''),
                COALESCE(road_code_ss, ''),
                COALESCE(premises_no_ss, ''),
                COALESCE(lamp_post_no_ss, ''),
                COALESCE(event_bill_ac_month_ss, ''),
                COALESCE(bill_type_ss, ''),
                COALESCE(consumer_account_status_ss, ''),
                COALESCE(ebill_status_ss, ''),
                COALESCE(supply_mode_ss, ''),
                COALESCE(supply_type_ss, ''),
                COALESCE(consumer_status_code_ss, ''),
                COALESCE(_source_file, ''),
                COALESCE(_ingestion_batch_id, '')
            )
        ) AS cih_source_row_key,
        -- Source lineage
        NULLIF(TRIM(r1_s), '') AS r1_source_ref,

        -- Name
        NULLIF(TRIM(name_s), '') AS name_raw,
        NULLIF(
            REGEXP_REPLACE(
                TRIM(name_s),
                '\s+',
                ' ',
                'g'
            ),
            ''
        ) AS name_normalized,

        -- Address attributes
        NULLIF(TRIM(house_no_s), '') AS house_no,
        NULLIF(TRIM(floor_s), '') AS floor,
        NULLIF(TRIM(street_s), '') AS street,
        NULLIF(TRIM(city_s), '') AS city,
        NULLIF(TRIM(pincode_s), '') AS pincode,

               -- Email
        NULLIF(TRIM(email_id_s), '') AS email_raw,
        CASE
            WHEN TRIM(email_id_s) ~ '^[^[:space:]@]+@[^[:space:]@]+\.[^[:space:]@]+$'
            THEN LOWER(TRIM(email_id_s))
            ELSE NULL
        END AS email_normalized,
        CASE
            WHEN email_id_s IS NULL OR TRIM(email_id_s) = '' THEN NULL
            WHEN TRIM(email_id_s) ~ '^[^[:space:]@]+@[^[:space:]@]+\.[^[:space:]@]+$' THEN TRUE
            ELSE FALSE
        END AS email_valid_flag,
        NULLIF(LOWER(TRIM(email_domain_s)), '') AS email_domain_normalized,
        -- CIH mobile representation
        -- brand_id_s is the reliable 10-digit source representation.
        NULLIF(TRIM(brand_id_s), '') AS mobile_raw,
        CASE
            WHEN TRIM(brand_id_s) ~ '^[0-9]{10}$'
            THEN TRIM(brand_id_s)
            ELSE NULL
        END AS mobile_normalized,

        -- Preserve the separate mobile source representation.
        -- Do not attempt to reconstruct it from scientific notation.
        NULLIF(TRIM(mobile_no_s), '') AS mobile_no_source_raw,

        -- Mobile/source flag
        NULLIF(TRIM(is_mobile_b), '') AS is_mobile_source_raw,
        CASE
            WHEN TRIM(is_mobile_b) = '1' THEN TRUE
            WHEN TRIM(is_mobile_b) = '0' THEN FALSE
            ELSE NULL
        END AS is_mobile_normalized,

        -- DND is intentionally kept source-valued.
        -- Its populated encoding has not been semantically mapped.
        NULLIF(TRIM(dnd_s), '') AS dnd_raw,

        -- Payment attributes
        NULLIF(TRIM(mode_of_payment_s), '') AS mode_of_payment_raw,
        NULLIF(TRIM(payment_mode_description_s), '') AS payment_mode_description_raw,

        -- Multi-valued source fields remain intact at this stage.
        bill_ac_month_ss,
        consumer_no_ss,
        regional_office_ss,
        consumer_type_ss,
        road_code_ss,
        premises_no_ss,
        lamp_post_no_ss,
        event_bill_ac_month_ss,
        bill_type_ss,
        consumer_account_status_ss,
        ebill_status_ss,
        supply_mode_ss,
        supply_type_ss,
        consumer_status_code_ss,

        -- Ingestion lineage
        _loaded_at,
        _source_file,
        _source_system,
        _ingestion_batch_id

    FROM source
)

SELECT *
FROM normalized

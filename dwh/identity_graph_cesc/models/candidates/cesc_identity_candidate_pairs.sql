{{ config(
    materialized='table',
    indexes=[
        {'columns': ['bill_source_key']},
        {'columns': ['cih_source_row_key']},
        {'columns': ['bill_source_key', 'cih_source_row_key'], 'unique': true}
    ]
) }}

WITH bill_spine AS (
    SELECT
        bill_source_key,
        consumer_no,
        mobile_normalized,
        mobile_valid_flag
    FROM {{ ref('cesc_bill_spine') }}
),

cih_phone AS NOT MATERIALIZED (
    SELECT
        cih_source_row_key,
        mobile_normalized,
        phone_block_1,
        phone_block_2,
        phone_block_3,
        phone_block_4,
        phone_block_5
    FROM {{ ref('cesc_cih_lookup_phone') }}
),

cih_consumer AS (
    SELECT cih_source_row_key, consumer_no
    FROM {{ ref('cesc_cih_lookup_consumer') }}
),

-- 1. Exact phone match (Branch A entry) -- UNCHANGED
pairs_phone_exact AS (
    SELECT
        b.bill_source_key,
        p.cih_source_row_key,
        TRUE AS has_exact_phone_path,
        FALSE AS has_near_phone_path,
        0 AS phone_digit_distance,
        NULL::INT AS phone_differing_position,
        FALSE AS has_consumer_path
    FROM bill_spine b
    JOIN cih_phone p ON b.mobile_normalized = p.mobile_normalized
    WHERE b.mobile_valid_flag = TRUE
),

-- 2. Exact consumer match (Branch B entry / account lookup) -- UNCHANGED
pairs_consumer AS (
    SELECT
        b.bill_source_key,
        c.cih_source_row_key,
        FALSE AS has_exact_phone_path,
        FALSE AS has_near_phone_path,
        NULL::INT AS phone_digit_distance,
        NULL::INT AS phone_differing_position,
        TRUE AS has_consumer_path
    FROM bill_spine b
    JOIN cih_consumer c ON b.consumer_no = c.consumer_no
),

-- 3. Combine exact-match candidate paths only.
-- Near-phone / Hamming-distance matching has been removed.
combined_candidates AS (
    SELECT
        bill_source_key,
        cih_source_row_key,
        has_exact_phone_path,
        has_near_phone_path,
        phone_digit_distance,
        phone_differing_position,
        has_consumer_path
    FROM pairs_phone_exact

    UNION ALL

    SELECT
        bill_source_key,
        cih_source_row_key,
        has_exact_phone_path,
        has_near_phone_path,
        phone_digit_distance,
        phone_differing_position,
        has_consumer_path
    FROM pairs_consumer
),

-- Collapse multiple exact-match paths for the same bill/CIH pair.
final_candidates AS (
    SELECT
        bill_source_key,
        cih_source_row_key,
        BOOL_OR(has_exact_phone_path) AS has_exact_phone_path,
        BOOL_OR(has_near_phone_path) AS has_near_phone_path,
        MIN(phone_digit_distance) AS phone_digit_distance,
        MIN(phone_differing_position) FILTER (
            WHERE phone_differing_position IS NOT NULL
        ) AS phone_differing_position,
        BOOL_OR(has_consumer_path) AS has_consumer_path
    FROM combined_candidates
    GROUP BY
        bill_source_key,
        cih_source_row_key
)

SELECT
    bill_source_key,
    cih_source_row_key,
    has_exact_phone_path,
    has_near_phone_path,
    phone_digit_distance,
    phone_differing_position,
    has_consumer_path
FROM final_candidates
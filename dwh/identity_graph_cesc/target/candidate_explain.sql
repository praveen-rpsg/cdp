EXPLAIN (COSTS TRUE, FORMAT TEXT)
WITH bill_spine AS (
    SELECT
        bill_source_key,
        consumer_no,
        mobile_normalized,
        mobile_valid_flag
    FROM "cdp_meta"."cesc_silver"."cesc_bill_spine"
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
    FROM "cdp_meta"."cesc_silver"."cesc_cih_lookup_phone"
),

cih_consumer AS (
    SELECT cih_source_row_key, consumer_no
    FROM "cdp_meta"."cesc_silver"."cesc_cih_lookup_consumer"
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

-- 3. Near-phone eligibility: valid 10-digit bills with NO exact phone match.
--    A consumer match does not exclude near-phone corroboration.
--    Computes the bill's own 10-element signature array using the EXACT
--    same 10 prefixed expressions as idx_cesc_cih_phone_hamming2_sig, just
--    evaluated against the bill's phone instead of the CIH phone.
bills_for_near_phone AS MATERIALIZED(
    SELECT
        b.bill_source_key,
        b.mobile_normalized AS bill_phone,
        ARRAY[
            '123:' || SUBSTRING(b.mobile_normalized FROM 1 FOR 2) || SUBSTRING(b.mobile_normalized FROM 3 FOR 2) || SUBSTRING(b.mobile_normalized FROM 5 FOR 2),
            '124:' || SUBSTRING(b.mobile_normalized FROM 1 FOR 2) || SUBSTRING(b.mobile_normalized FROM 3 FOR 2) || SUBSTRING(b.mobile_normalized FROM 7 FOR 2),
            '125:' || SUBSTRING(b.mobile_normalized FROM 1 FOR 2) || SUBSTRING(b.mobile_normalized FROM 3 FOR 2) || SUBSTRING(b.mobile_normalized FROM 9 FOR 2),
            '134:' || SUBSTRING(b.mobile_normalized FROM 1 FOR 2) || SUBSTRING(b.mobile_normalized FROM 5 FOR 2) || SUBSTRING(b.mobile_normalized FROM 7 FOR 2),
            '135:' || SUBSTRING(b.mobile_normalized FROM 1 FOR 2) || SUBSTRING(b.mobile_normalized FROM 5 FOR 2) || SUBSTRING(b.mobile_normalized FROM 9 FOR 2),
            '145:' || SUBSTRING(b.mobile_normalized FROM 1 FOR 2) || SUBSTRING(b.mobile_normalized FROM 7 FOR 2) || SUBSTRING(b.mobile_normalized FROM 9 FOR 2),
            '234:' || SUBSTRING(b.mobile_normalized FROM 3 FOR 2) || SUBSTRING(b.mobile_normalized FROM 5 FOR 2) || SUBSTRING(b.mobile_normalized FROM 7 FOR 2),
            '235:' || SUBSTRING(b.mobile_normalized FROM 3 FOR 2) || SUBSTRING(b.mobile_normalized FROM 5 FOR 2) || SUBSTRING(b.mobile_normalized FROM 9 FOR 2),
            '245:' || SUBSTRING(b.mobile_normalized FROM 3 FOR 2) || SUBSTRING(b.mobile_normalized FROM 7 FOR 2) || SUBSTRING(b.mobile_normalized FROM 9 FOR 2),
            '345:' || SUBSTRING(b.mobile_normalized FROM 5 FOR 2) || SUBSTRING(b.mobile_normalized FROM 7 FOR 2) || SUBSTRING(b.mobile_normalized FROM 9 FOR 2)
        ] AS bill_sig_array
    FROM bill_spine b
    LEFT JOIN pairs_phone_exact pe
      ON b.bill_source_key = pe.bill_source_key
    WHERE pe.bill_source_key IS NULL
      AND b.mobile_valid_flag = TRUE
      AND LENGTH(b.mobile_normalized) = 10
),

-- 4. Near-phone prefilter.
-- Hamming distance <= 2 across 10 digits means at least 3 of the
-- 5 two-digit blocks are identical.
--
-- Use the materialized/indexed CIH phone blocks so PostgreSQL can
-- use BitmapAnd across the block indexes.

near_phone_prefiltered AS (

    -- 123
    SELECT b.bill_source_key, b.bill_phone,
           cp.cih_source_row_key, cp.mobile_normalized AS cih_phone
    FROM bills_for_near_phone b
    JOIN cih_phone cp
      ON cp.phone_block_1 = SUBSTRING(b.bill_phone FROM 1 FOR 2)
     AND cp.phone_block_2 = SUBSTRING(b.bill_phone FROM 3 FOR 2)
     AND cp.phone_block_3 = SUBSTRING(b.bill_phone FROM 5 FOR 2)

    UNION ALL

    -- 124
    SELECT b.bill_source_key, b.bill_phone,
           cp.cih_source_row_key, cp.mobile_normalized AS cih_phone
    FROM bills_for_near_phone b
    JOIN cih_phone cp
      ON cp.phone_block_1 = SUBSTRING(b.bill_phone FROM 1 FOR 2)
     AND cp.phone_block_2 = SUBSTRING(b.bill_phone FROM 3 FOR 2)
     AND cp.phone_block_4 = SUBSTRING(b.bill_phone FROM 7 FOR 2)

    UNION ALL

    -- 125
    SELECT b.bill_source_key, b.bill_phone,
           cp.cih_source_row_key, cp.mobile_normalized AS cih_phone
    FROM bills_for_near_phone b
    JOIN cih_phone cp
      ON cp.phone_block_1 = SUBSTRING(b.bill_phone FROM 1 FOR 2)
     AND cp.phone_block_2 = SUBSTRING(b.bill_phone FROM 3 FOR 2)
     AND cp.phone_block_5 = SUBSTRING(b.bill_phone FROM 9 FOR 2)

    UNION ALL

    -- 134
    SELECT b.bill_source_key, b.bill_phone,
           cp.cih_source_row_key, cp.mobile_normalized AS cih_phone
    FROM bills_for_near_phone b
    JOIN cih_phone cp
      ON cp.phone_block_1 = SUBSTRING(b.bill_phone FROM 1 FOR 2)
     AND cp.phone_block_3 = SUBSTRING(b.bill_phone FROM 5 FOR 2)
     AND cp.phone_block_4 = SUBSTRING(b.bill_phone FROM 7 FOR 2)

    UNION ALL

    -- 135
    SELECT b.bill_source_key, b.bill_phone,
           cp.cih_source_row_key, cp.mobile_normalized AS cih_phone
    FROM bills_for_near_phone b
    JOIN cih_phone cp
      ON cp.phone_block_1 = SUBSTRING(b.bill_phone FROM 1 FOR 2)
     AND cp.phone_block_3 = SUBSTRING(b.bill_phone FROM 5 FOR 2)
     AND cp.phone_block_5 = SUBSTRING(b.bill_phone FROM 9 FOR 2)

    UNION ALL

    -- 145
    SELECT b.bill_source_key, b.bill_phone,
           cp.cih_source_row_key, cp.mobile_normalized AS cih_phone
    FROM bills_for_near_phone b
    JOIN cih_phone cp
      ON cp.phone_block_1 = SUBSTRING(b.bill_phone FROM 1 FOR 2)
     AND cp.phone_block_4 = SUBSTRING(b.bill_phone FROM 7 FOR 2)
     AND cp.phone_block_5 = SUBSTRING(b.bill_phone FROM 9 FOR 2)

    UNION ALL

    -- 234
    SELECT b.bill_source_key, b.bill_phone,
           cp.cih_source_row_key, cp.mobile_normalized AS cih_phone
    FROM bills_for_near_phone b
    JOIN cih_phone cp
      ON cp.phone_block_2 = SUBSTRING(b.bill_phone FROM 3 FOR 2)
     AND cp.phone_block_3 = SUBSTRING(b.bill_phone FROM 5 FOR 2)
     AND cp.phone_block_4 = SUBSTRING(b.bill_phone FROM 7 FOR 2)

    UNION ALL

    -- 235
    SELECT b.bill_source_key, b.bill_phone,
           cp.cih_source_row_key, cp.mobile_normalized AS cih_phone
    FROM bills_for_near_phone b
    JOIN cih_phone cp
      ON cp.phone_block_2 = SUBSTRING(b.bill_phone FROM 3 FOR 2)
     AND cp.phone_block_3 = SUBSTRING(b.bill_phone FROM 5 FOR 2)
     AND cp.phone_block_5 = SUBSTRING(b.bill_phone FROM 9 FOR 2)

    UNION ALL

    -- 245
    SELECT b.bill_source_key, b.bill_phone,
           cp.cih_source_row_key, cp.mobile_normalized AS cih_phone
    FROM bills_for_near_phone b
    JOIN cih_phone cp
      ON cp.phone_block_2 = SUBSTRING(b.bill_phone FROM 3 FOR 2)
     AND cp.phone_block_4 = SUBSTRING(b.bill_phone FROM 7 FOR 2)
     AND cp.phone_block_5 = SUBSTRING(b.bill_phone FROM 9 FOR 2)

    UNION ALL

    -- 345
    SELECT b.bill_source_key, b.bill_phone,
           cp.cih_source_row_key, cp.mobile_normalized AS cih_phone
    FROM bills_for_near_phone b
    JOIN cih_phone cp
      ON cp.phone_block_3 = SUBSTRING(b.bill_phone FROM 5 FOR 2)
     AND cp.phone_block_4 = SUBSTRING(b.bill_phone FROM 7 FOR 2)
     AND cp.phone_block_5 = SUBSTRING(b.bill_phone FROM 9 FOR 2)
),

-- 5. Remove duplicate candidate pairs generated by multiple
-- three-block combinations before calculating exact distance.
near_phone_prefiltered_distinct AS (
    SELECT DISTINCT
        bill_source_key,
        bill_phone,
        cih_source_row_key,
        cih_phone
    FROM near_phone_prefiltered
),

-- 6. Exact Hamming distance on the prefiltered candidate set only.
near_phone_scored AS (
    SELECT
        n.bill_source_key,
        n.cih_source_row_key,
        n.bill_phone,
        n.cih_phone,
        (
            SELECT COUNT(*)
            FROM generate_series(1, 10) AS pos
            WHERE SUBSTRING(n.bill_phone FROM pos FOR 1)
               <> SUBSTRING(n.cih_phone FROM pos FOR 1)
        )::INT AS digit_distance
    FROM near_phone_prefiltered_distinct n
),

-- 7. Keep only true distance 1 or 2.
near_phone_candidates AS (
    SELECT
        s.bill_source_key,
        s.cih_source_row_key,
        FALSE AS has_exact_phone_path,
        TRUE AS has_near_phone_path,
        s.digit_distance AS phone_digit_distance,
        CASE
            WHEN s.digit_distance = 1 THEN (
                SELECT pos
                FROM generate_series(1, 10) AS pos
                WHERE SUBSTRING(s.bill_phone FROM pos FOR 1)
                   <> SUBSTRING(s.cih_phone FROM pos FOR 1)
                LIMIT 1
            )
            ELSE NULL
        END::INT AS phone_differing_position,
        FALSE AS has_consumer_path
    FROM near_phone_scored s
    WHERE s.digit_distance IN (1, 2)
),

-- 8. Combine all candidate paths.
-- Preserve every bill/CIH relationship while collapsing multiple
-- paths for the same pair into one candidate record.
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

    UNION ALL

    SELECT
        bill_source_key,
        cih_source_row_key,
        has_exact_phone_path,
        has_near_phone_path,
        phone_digit_distance,
        phone_differing_position,
        has_consumer_path
    FROM near_phone_candidates
),

-- Collapse multiple paths for the same bill/CIH pair.
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
FROM final_candidates;
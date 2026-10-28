

WITH candidates AS (
    SELECT *
    FROM "cdp_meta"."cesc_silver"."cesc_identity_candidate_pairs"
),

bills AS (
    SELECT
        bill_source_key,
        consumer_no,
        mobile_normalized,
        mobile_valid_flag,
        email_normalized,
        email_valid_flag,
        bill_name_raw,
        supply_address,
        pin_cd,
        CONCAT_WS(
            ' ',
            NULLIF(TRIM(supply_address), ''),
            NULLIF(TRIM(pin_cd), '')
        ) AS bill_address_concat
    FROM "cdp_meta"."cesc_silver"."cesc_bill_spine"
),

cih AS (
    SELECT
        cih_source_row_key,
        name_normalized AS cih_name_normalized,
        email_normalized AS cih_email_normalized,
        mobile_normalized AS cih_mobile_normalized,
        CONCAT_WS(
            ' ',
            NULLIF(TRIM(house_no), ''),
            NULLIF(TRIM(floor), ''),
            NULLIF(TRIM(street), ''),
            NULLIF(TRIM(city), ''),
            NULLIF(TRIM(pincode), '')
        ) AS cih_address_concat
    FROM "cdp_meta"."staging"."stg_cesc_cih"
),

cih_email_counts AS (
    SELECT
        email_normalized,
        MAX(email_cih_count) AS email_cih_count
    FROM "cdp_meta"."cesc_silver"."cesc_cih_lookup_email"
    GROUP BY email_normalized
)

SELECT
    cand.bill_source_key,
    cand.cih_source_row_key,

    CASE
        WHEN cand.has_exact_phone_path
             AND cand.has_consumer_path
            THEN 'PHONE_CONSUMER'
        WHEN cand.has_exact_phone_path
            THEN 'PHONE'
        WHEN cand.has_consumer_path
            THEN 'CONSUMER'
        ELSE 'UNKNOWN'
    END AS candidate_generation_method,

    -- Phone evidence
    b.mobile_valid_flag AS phone_available,
    cand.has_exact_phone_path,
    cand.has_near_phone_path,
    cand.phone_digit_distance,
    cand.phone_differing_position,

    CASE
        WHEN cand.has_exact_phone_path THEN 'EXACT'
        WHEN cand.has_near_phone_path
             AND cand.phone_differing_position >= 6
            THEN 'NEAR_POS_6_10'
        WHEN cand.has_near_phone_path
             AND cand.phone_differing_position < 6
            THEN 'NEAR_POS_1_5'
        ELSE 'NONE'
    END AS phone_match_type,

    CASE
        WHEN cand.has_exact_phone_path THEN 45.00
        WHEN cand.has_near_phone_path
             AND cand.phone_differing_position >= 6
            THEN 35.00
        WHEN cand.has_near_phone_path
             AND cand.phone_differing_position < 6
            THEN 25.00
        ELSE 0.00
    END AS phone_evidence_points,

    -- Consumer evidence
    (b.consumer_no IS NOT NULL) AS consumer_available,

    cand.has_consumer_path AS consumer_match,

    CASE
        WHEN cand.has_consumer_path THEN 25.00
        ELSE 0.00
    END AS consumer_evidence_points,

    -- Email evidence
    b.email_valid_flag AS email_available,

    (
        b.email_valid_flag = TRUE
        AND b.email_normalized = c.cih_email_normalized
    ) AS email_match,

    ec.email_cih_count,

    CASE
        WHEN b.email_valid_flag = TRUE
         AND b.email_normalized = c.cih_email_normalized
            THEN 15.00
        ELSE 0.00
    END AS email_evidence_points,

    -- Name similarity
    (
        NOT cand.has_exact_phone_path
        AND b.bill_name_raw IS NOT NULL
        AND c.cih_name_normalized IS NOT NULL
    ) AS name_available,

    b.bill_name_raw,
    c.cih_name_normalized,

    CASE
        WHEN NOT cand.has_exact_phone_path
         AND b.bill_name_raw IS NOT NULL
         AND c.cih_name_normalized IS NOT NULL
            THEN ROUND(
                similarity(
                    b.bill_name_raw,
                    c.cih_name_normalized
                )::NUMERIC,
                4
            )
        ELSE NULL
    END AS name_similarity_score,

    CASE
        WHEN NOT cand.has_exact_phone_path
         AND b.bill_name_raw IS NOT NULL
         AND c.cih_name_normalized IS NOT NULL
            THEN ROUND(
                (
                    similarity(
                        b.bill_name_raw,
                        c.cih_name_normalized
                    ) * 10.0
                )::NUMERIC,
                2
            )
        ELSE 0.00
    END AS name_evidence_points,

    -- Address similarity
    (
        NOT cand.has_exact_phone_path
        AND NULLIF(b.bill_address_concat, '') IS NOT NULL
        AND NULLIF(c.cih_address_concat, '') IS NOT NULL
    ) AS address_available,

    b.bill_address_concat,
    c.cih_address_concat,

    CASE
        WHEN NOT cand.has_exact_phone_path
         AND NULLIF(b.bill_address_concat, '') IS NOT NULL
         AND NULLIF(c.cih_address_concat, '') IS NOT NULL
            THEN ROUND(
                similarity(
                    b.bill_address_concat,
                    c.cih_address_concat
                )::NUMERIC,
                4
            )
        ELSE NULL
    END AS address_similarity_score,

    CASE
        WHEN NOT cand.has_exact_phone_path
         AND NULLIF(b.bill_address_concat, '') IS NOT NULL
         AND NULLIF(c.cih_address_concat, '') IS NOT NULL
            THEN ROUND(
                (
                    similarity(
                        b.bill_address_concat,
                        c.cih_address_concat
                    ) * 5.0
                )::NUMERIC,
                2
            )
        ELSE 0.00
    END AS address_evidence_points,

    -- Deferred householding / surname tokens
    REVERSE(
        SPLIT_PART(
            REVERSE(TRIM(b.bill_name_raw)),
            ' ',
            1
        )
    ) AS bill_last_name_token,

    REVERSE(
        SPLIT_PART(
            REVERSE(TRIM(c.cih_name_normalized)),
            ' ',
            1
        )
    ) AS cih_last_name_token,

    CASE
        WHEN NOT cand.has_exact_phone_path
         AND b.bill_name_raw IS NOT NULL
         AND c.cih_name_normalized IS NOT NULL
         AND LOWER(
                REVERSE(
                    SPLIT_PART(
                        REVERSE(TRIM(b.bill_name_raw)),
                        ' ',
                        1
                    )
                )
             ) =
             LOWER(
                REVERSE(
                    SPLIT_PART(
                        REVERSE(TRIM(c.cih_name_normalized)),
                        ' ',
                        1
                    )
                )
             )
            THEN TRUE
        ELSE FALSE
    END AS last_name_exact_match,

    CLOCK_TIMESTAMP() AS evaluated_at

FROM candidates cand

JOIN bills b
    ON cand.bill_source_key = b.bill_source_key

JOIN cih c
    ON cand.cih_source_row_key = c.cih_source_row_key

LEFT JOIN cih_email_counts ec
    ON b.email_normalized = ec.email_normalized
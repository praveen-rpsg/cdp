

WITH evidence AS (
    SELECT *
    FROM "cdp_meta"."cesc_silver"."cesc_identity_evidence"
),

global_consumer_existence AS (
    SELECT DISTINCT consumer_no
    FROM "cdp_meta"."cesc_silver"."cesc_cih_lookup_consumer"
),

bills AS (
    SELECT bill_source_key, consumer_no
    FROM "cdp_meta"."cesc_silver"."cesc_bill_spine"
),

scored AS (
    SELECT
        e.bill_source_key,
        e.cih_source_row_key,
        e.candidate_generation_method,
        e.phone_match_type,
        e.phone_evidence_points,
        e.phone_digit_distance,
        e.phone_differing_position,
        e.consumer_match,
        e.consumer_evidence_points,
        e.email_match,
        e.email_evidence_points,
        e.email_cih_count,
        e.name_similarity_score,
        e.name_evidence_points,
        e.address_similarity_score,
        e.address_evidence_points,
        e.last_name_exact_match,

        -- Calculate applicable maximum points based on available evidence
        (
            CASE WHEN e.phone_available THEN 45.00 ELSE 0.00 END +
            CASE WHEN e.consumer_available THEN 25.00 ELSE 0.00 END +
            CASE WHEN e.email_available THEN 15.00 ELSE 0.00 END +
            CASE WHEN e.name_available THEN 10.00 ELSE 0.00 END +
            CASE WHEN e.address_available THEN 5.00 ELSE 0.00 END
        ) AS applicable_max_score,

        -- Raw sum of earned points
        (
            e.phone_evidence_points +
            e.consumer_evidence_points +
            e.email_evidence_points +
            e.name_evidence_points +
            e.address_evidence_points
        ) AS raw_score,

        -- Check if consumer number exists anywhere in CIH
        (gce.consumer_no IS NOT NULL) AS consumer_exists_globally_in_cih

    FROM evidence e
    JOIN bills b ON e.bill_source_key = b.bill_source_key
    LEFT JOIN global_consumer_existence gce ON b.consumer_no = gce.consumer_no
)

SELECT
    bill_source_key,
    cih_source_row_key,
    candidate_generation_method,
    phone_match_type,
    phone_evidence_points,
    phone_digit_distance,
    phone_differing_position,
    consumer_match,
    consumer_evidence_points,
    email_match,
    email_evidence_points,
    email_cih_count,
    name_similarity_score,
    name_evidence_points,
    address_similarity_score,
    address_evidence_points,
    last_name_exact_match,
    raw_score,
    applicable_max_score,

    -- Normalized score (0 - 100)
    CASE
        WHEN applicable_max_score > 0
        THEN ROUND((raw_score / applicable_max_score) * 100.0, 2)
        ELSE 0.00
    END AS normalized_score,

    -- Deterministic precedence & method assignment
    CASE
        -- Rule D1: Phone Exact + Consumer history match
        WHEN phone_match_type = 'EXACT' AND consumer_match = TRUE
        THEN 'PHONE_CONSUMER_MATCH'

        -- Rule D2: Phone Exact + Consumer brand new to system
        WHEN phone_match_type = 'EXACT' AND consumer_match = FALSE AND consumer_exists_globally_in_cih = FALSE
        THEN 'PHONE_MATCH_NEW_CONSUMER'

        -- Flowchart branch: Phone exact, consumer present on a different CIH
        WHEN phone_match_type = 'EXACT' AND consumer_match = FALSE AND consumer_exists_globally_in_cih = TRUE
        THEN 'PHONE_MATCH_CONSUMER_SHARED'

        -- Flowchart branch: Consumer match + Email match (phone missing/different)
        WHEN consumer_match = TRUE AND email_match = TRUE
        THEN 'CONSUMER_EMAIL_MATCH'

        -- Flowchart branch: Consumer match + Name similarity >= 0.70
        WHEN consumer_match = TRUE AND COALESCE(name_similarity_score, 0) >= 0.70
        THEN 'CONSUMER_NAME_PROBABLE'

        -- Near-phone match
        WHEN phone_match_type IN ('NEAR_POS_6_10', 'NEAR_POS_1_5')
        THEN 'NEAR_PHONE_CORROBORATED'

        ELSE 'EVIDENCE_EVALUATED'
    END AS resolution_method,

    CASE
        WHEN (phone_match_type = 'EXACT' AND consumer_match = TRUE)
          OR (phone_match_type = 'EXACT' AND consumer_match = FALSE AND consumer_exists_globally_in_cih = FALSE)
        THEN 'DETERMINISTIC'
        ELSE 'PROBABILISTIC'
    END AS score_type,

    'v1.0' AS score_version,

    -- Confidence score
    CASE
        WHEN (phone_match_type = 'EXACT' AND consumer_match = TRUE)
          OR (phone_match_type = 'EXACT' AND consumer_match = FALSE AND consumer_exists_globally_in_cih = FALSE)
        THEN 100.00
        ELSE (
            CASE
                WHEN applicable_max_score > 0
                THEN ROUND((raw_score / applicable_max_score) * 100.0, 2)
                ELSE 0.00
            END
        )
    END AS confidence_score,

    -- Decision bands: >= 70 CONNECT, < 70 MANUAL_REVIEW
    CASE
        WHEN (phone_match_type = 'EXACT' AND consumer_match = TRUE)
          OR (phone_match_type = 'EXACT' AND consumer_match = FALSE AND consumer_exists_globally_in_cih = FALSE)
        THEN 'AUTO_MERGE'
        WHEN (
            CASE
                WHEN applicable_max_score > 0
                THEN ROUND((raw_score / applicable_max_score) * 100.0, 2)
                ELSE 0.00
            END
        ) >= 70.0
        THEN 'CONNECT'
        ELSE 'MANUAL_REVIEW'
    END AS confidence_band,

    CLOCK_TIMESTAMP() AS scored_at
FROM scored
{{ config(
    materialized='table',
    indexes=[
        {'columns': ['bill_source_key']},
        {'columns': ['resolved_cih_source_row_key']},
        {'columns': ['resolution_status']},
        {'columns': ['confidence_band']}
    ]
) }}

WITH all_bills AS (
    SELECT
        bill_source_key
    FROM {{ ref('cesc_bill_spine') }}
),

scored_candidates AS (
    SELECT
        bill_source_key,
        cih_source_row_key,
        resolution_method,
        score_type,
        confidence_score,
        normalized_score,
        raw_score,
        applicable_max_score,
        confidence_band,
        score_version,
        phone_match_type,
        phone_evidence_points,
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
        scored_at
    FROM {{ ref('cesc_identity_scored_candidates') }}
),

ranked_candidates AS (
    SELECT
        s.*,

        COUNT(*) OVER (
            PARTITION BY s.bill_source_key
        ) AS total_candidate_count,

        ROW_NUMBER() OVER (
            PARTITION BY s.bill_source_key
            ORDER BY
                CASE
                    WHEN s.score_type = 'DETERMINISTIC' THEN 1
                    ELSE 2
                END,
                s.normalized_score DESC,
                s.raw_score DESC,
                s.cih_source_row_key ASC
        ) AS candidate_rank

    FROM scored_candidates s
),

confirmed_links AS (
    SELECT
        rc.bill_source_key,
        rc.cih_source_row_key AS resolved_cih_source_row_key,
        (rc.candidate_rank = 1) AS is_primary_resolution,
        rc.resolution_method,
        'RESOLVED' AS resolution_status,
        rc.score_type AS resolution_type,
        rc.confidence_score,
        rc.normalized_score,
        rc.raw_score,
        rc.applicable_max_score,
        rc.confidence_band,
        rc.score_type,
        rc.score_version,
        rc.candidate_rank,
        rc.total_candidate_count,
        rc.phone_match_type,
        rc.phone_evidence_points,
        rc.consumer_match,
        rc.consumer_evidence_points,
        rc.email_match,
        rc.email_evidence_points,
        rc.email_cih_count,
        rc.name_similarity_score,
        rc.name_evidence_points,
        rc.address_similarity_score,
        rc.address_evidence_points,
        rc.last_name_exact_match,
        FALSE AS manual_review_required,
        NULL::TEXT AS review_reason,
        rc.scored_at AS resolved_at
    FROM ranked_candidates rc
    WHERE rc.confidence_band IN ('AUTO_MERGE', 'CONNECT')
),

unconfirmed_candidates_bills AS (
    SELECT
        rc.bill_source_key,
        rc.cih_source_row_key AS resolved_cih_source_row_key,
        (rc.candidate_rank = 1) AS is_primary_resolution,
        rc.resolution_method,
        'CANDIDATE_UNDER_REVIEW' AS resolution_status,
        'MANUAL_REVIEW' AS resolution_type,
        rc.confidence_score,
        rc.normalized_score,
        rc.raw_score,
        rc.applicable_max_score,
        rc.confidence_band,
        rc.score_type,
        rc.score_version,
        rc.candidate_rank,
        rc.total_candidate_count,
        rc.phone_match_type,
        rc.phone_evidence_points,
        rc.consumer_match,
        rc.consumer_evidence_points,
        rc.email_match,
        rc.email_evidence_points,
        rc.email_cih_count,
        rc.name_similarity_score,
        rc.name_evidence_points,
        rc.address_similarity_score,
        rc.address_evidence_points,
        rc.last_name_exact_match,
        TRUE AS manual_review_required,
        'Confidence score below threshold (< 70)' AS review_reason,
        rc.scored_at AS resolved_at
    FROM ranked_candidates rc
    WHERE NOT EXISTS (
        SELECT 1
        FROM confirmed_links cl
        WHERE cl.bill_source_key = rc.bill_source_key
    )
),

bills_without_any_candidates AS (
    SELECT
        b.bill_source_key,
        NULL::TEXT AS resolved_cih_source_row_key,
        TRUE AS is_primary_resolution,
        'UNRESOLVED_NO_CIH_CANDIDATE' AS resolution_method,
        'UNRESOLVED' AS resolution_status,
        'UNRESOLVED' AS resolution_type,
        0.00 AS confidence_score,
        0.00 AS normalized_score,
        0.00 AS raw_score,
        0.00 AS applicable_max_score,
        'UNRESOLVED' AS confidence_band,
        'NONE' AS score_type,
        '{{ var("score_version") }}' AS score_version,
        1 AS candidate_rank,
        0 AS total_candidate_count,
        'NONE' AS phone_match_type,
        0.00 AS phone_evidence_points,
        FALSE AS consumer_match,
        0.00 AS consumer_evidence_points,
        FALSE AS email_match,
        0.00 AS email_evidence_points,
        NULL::INT AS email_cih_count,
        NULL::NUMERIC(5,4) AS name_similarity_score,
        0.00 AS name_evidence_points,
        NULL::NUMERIC(5,4) AS address_similarity_score,
        0.00 AS address_evidence_points,
        FALSE AS last_name_exact_match,
        FALSE AS manual_review_required,
        'No CIH candidate identified across phone, email, or consumer number' AS review_reason,
        CLOCK_TIMESTAMP() AS resolved_at
    FROM all_bills b
    WHERE NOT EXISTS (
        SELECT 1
        FROM scored_candidates sc
        WHERE sc.bill_source_key = b.bill_source_key
    )
)

SELECT *
FROM confirmed_links

UNION ALL

SELECT *
FROM unconfirmed_candidates_bills

UNION ALL

SELECT *
FROM bills_without_any_candidates
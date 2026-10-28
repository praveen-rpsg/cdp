{{ config(
    materialized='table',
    indexes=[
        {'columns': ['bill_source_key']},
        {'columns': ['review_status']}
    ]
) }}

WITH candidates_ranked AS (
    SELECT
        s.*,
        ROW_NUMBER() OVER (
            PARTITION BY s.bill_source_key
            ORDER BY s.normalized_score DESC, s.raw_score DESC, s.cih_source_row_key ASC
        ) AS rank_num
    FROM {{ ref('cesc_identity_scored_candidates') }} s
),

top_candidates AS (
    SELECT
        bill_source_key,
        cih_source_row_key AS top_candidate_key,
        normalized_score AS top_candidate_score,
        resolution_method AS top_resolution_method,
        jsonb_build_object(
            'phone_match_type', phone_match_type,
            'phone_points', phone_evidence_points,
            'consumer_match', consumer_match,
            'consumer_points', consumer_evidence_points,
            'email_match', email_match,
            'email_points', email_evidence_points,
            'email_cih_count', email_cih_count,
            'name_similarity', name_similarity_score,
            'name_points', name_evidence_points,
            'address_similarity', address_similarity_score,
            'address_points', address_evidence_points,
            'raw_score', raw_score,
            'applicable_max', applicable_max_score,
            'normalized_score', normalized_score
        ) AS top_evidence_summary
    FROM candidates_ranked
    WHERE rank_num = 1
),

second_candidates AS (
    SELECT
        bill_source_key,
        cih_source_row_key AS second_candidate_key,
        normalized_score AS second_candidate_score
    FROM candidates_ranked
    WHERE rank_num = 2
),

review_bills AS (
    SELECT DISTINCT
        bill_source_key,
        total_candidate_count,
        review_reason
    FROM {{ ref('cesc_identity_resolution') }}
    WHERE manual_review_required = TRUE
)

SELECT
    ROW_NUMBER() OVER () AS review_id,
    rb.bill_source_key,
    rb.total_candidate_count,
    tc.top_candidate_key,
    tc.top_candidate_score,
    sc.second_candidate_key,
    sc.second_candidate_score,
    ROUND((tc.top_candidate_score - COALESCE(sc.second_candidate_score, 0))::NUMERIC, 2) AS score_gap,
    tc.top_resolution_method AS resolution_method,
    rb.review_reason,
    tc.top_evidence_summary AS evidence_summary,
    'PENDING' AS review_status,
    NULL::TEXT AS reviewed_by,
    NULL::TIMESTAMPTZ AS reviewed_at,
    CLOCK_TIMESTAMP() AS created_at
FROM review_bills rb
JOIN top_candidates tc ON rb.bill_source_key = tc.bill_source_key
LEFT JOIN second_candidates sc ON rb.bill_source_key = sc.bill_source_key

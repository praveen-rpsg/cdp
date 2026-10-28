{{ config(
    materialized='table'
) }}

WITH resolution_summary AS (
    SELECT
        COUNT(DISTINCT bill_source_key) AS total_bills_evaluated,
        COUNT(DISTINCT CASE WHEN resolution_status = 'RESOLVED' THEN bill_source_key END) AS resolved_bill_count,
        COUNT(DISTINCT CASE WHEN resolution_type = 'DETERMINISTIC' AND resolution_status = 'RESOLVED' THEN bill_source_key END) AS deterministic_auto_merge_count,
        COUNT(DISTINCT CASE WHEN resolution_type = 'PROBABILISTIC' AND resolution_status = 'RESOLVED' THEN bill_source_key END) AS probabilistic_connect_count,
        COUNT(DISTINCT CASE WHEN resolution_status = 'CANDIDATE_UNDER_REVIEW' THEN bill_source_key END) AS manual_review_count,
        COUNT(DISTINCT CASE WHEN resolution_status = 'UNRESOLVED' THEN bill_source_key END) AS unresolved_no_candidate_count,

        -- Breakdown by resolution method
        COUNT(CASE WHEN resolution_method = 'PHONE_CONSUMER_MATCH' THEN 1 END) AS cnt_phone_consumer_match,
        COUNT(CASE WHEN resolution_method = 'PHONE_MATCH_NEW_CONSUMER' THEN 1 END) AS cnt_phone_new_consumer,
        COUNT(CASE WHEN resolution_method = 'PHONE_MATCH_CONSUMER_SHARED' THEN 1 END) AS cnt_phone_consumer_shared,
        COUNT(CASE WHEN resolution_method = 'CONSUMER_EMAIL_MATCH' THEN 1 END) AS cnt_consumer_email_match,
        COUNT(CASE WHEN resolution_method = 'CONSUMER_NAME_PROBABLE' THEN 1 END) AS cnt_consumer_name_probable,
        COUNT(CASE WHEN resolution_method = 'NEAR_PHONE_CORROBORATED' THEN 1 END) AS cnt_near_phone_corroborated,
        COUNT(CASE WHEN resolution_method = 'EVIDENCE_EVALUATED' THEN 1 END) AS cnt_evidence_evaluated,
        COUNT(CASE WHEN resolution_method = 'UNRESOLVED_NO_CIH_CANDIDATE' THEN 1 END) AS cnt_unresolved_no_candidate
    FROM {{ ref('cesc_identity_resolution') }}
),

candidate_summary AS (
    SELECT
        COUNT(*) AS total_candidate_pairs,
        COUNT(CASE WHEN has_exact_phone_path THEN 1 END) AS cnt_exact_phone_paths,
        COUNT(CASE WHEN has_near_phone_path THEN 1 END) AS cnt_near_phone_paths,
        COUNT(CASE WHEN has_consumer_path THEN 1 END) AS cnt_consumer_paths
    FROM {{ ref('cesc_identity_candidate_pairs') }}
),

entity_summary AS (
    SELECT COUNT(*) AS total_resolved_entities
    FROM {{ ref('cesc_entity_nodes') }}
)

SELECT
    '{{ var("run_id") }}' AS run_id,
    '{{ var("bill_period") }}' AS source_bill_period,
    '{{ var("score_version") }}' AS score_version,
    {{ var("threshold_connect") }} AS threshold_connect,
    {{ var("threshold_manual_review") }} AS threshold_manual_review,
    r.total_bills_evaluated,
    c.total_candidate_pairs,
    e.total_resolved_entities,
    r.resolved_bill_count,
    r.deterministic_auto_merge_count,
    r.probabilistic_connect_count,
    r.manual_review_count,
    r.unresolved_no_candidate_count,
    r.cnt_phone_consumer_match,
    r.cnt_phone_new_consumer,
    r.cnt_phone_consumer_shared,
    r.cnt_consumer_email_match,
    r.cnt_consumer_name_probable,
    r.cnt_near_phone_corroborated,
    r.cnt_evidence_evaluated,
    r.cnt_unresolved_no_candidate,
    c.cnt_exact_phone_paths,
    c.cnt_near_phone_paths,
    c.cnt_consumer_paths,
    CLOCK_TIMESTAMP() AS logged_at
FROM resolution_summary r
CROSS JOIN candidate_summary c
CROSS JOIN entity_summary e
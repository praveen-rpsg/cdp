{{ config(
    materialized='table',
    indexes=[
        {'columns': ['from_node_type', 'from_node_id']},
        {'columns': ['to_node_type', 'to_node_id']},
        {'columns': ['edge_type']}
    ]
) }}

WITH cih_entities AS (
    SELECT
        cesc_entity_id,
        primary_cih_source_row_key,
        canonical_name,
        canonical_mobile,
        canonical_email
    FROM {{ ref('cesc_entity_nodes') }}
),

bill_only_entities AS (
    SELECT
        m.cesc_entity_id,
        m.consumer_no,
        m.consumer_check_digit,
        m.identity_key_type,
        m.identity_key_value
    FROM cesc_silver.cesc_bill_only_consumer_map m
),

entities AS (
    SELECT
        cesc_entity_id,
        primary_cih_source_row_key,
        canonical_name,
        canonical_mobile,
        canonical_email
    FROM cih_entities
),

resolved_bills AS (
    SELECT
        r.bill_source_key,
        r.resolved_cih_source_row_key,
        r.resolution_method,
        r.confidence_score,
        r.score_type,
        e.cesc_entity_id,
        b.consumer_no,
        b.mobile_normalized AS bill_mobile,
        b.email_normalized AS bill_email,
        b._loaded_at AS observed_at
    FROM {{ ref('cesc_identity_resolution') }} r
    JOIN entities e ON r.resolved_cih_source_row_key = e.primary_cih_source_row_key
    JOIN {{ ref('cesc_bill_spine') }} b ON r.bill_source_key = b.bill_source_key
    WHERE r.resolution_status = 'RESOLVED'
),

unresolved_bills AS (
    SELECT
        b.bill_source_key,
        b.consumer_no,
        b.mobile_normalized AS bill_mobile,
        b.email_normalized AS bill_email,
        b._loaded_at AS observed_at
    FROM {{ ref('cesc_identity_resolution') }} r
    JOIN {{ ref('cesc_bill_spine') }} b ON r.bill_source_key = b.bill_source_key
    WHERE r.resolution_status IN ('CANDIDATE_UNDER_REVIEW', 'UNRESOLVED')
),

bill_only_bills AS (
    SELECT DISTINCT
        b.bill_source_key,
        b.consumer_no,
        b.consumer_check_digit,
        m.cesc_entity_id
    FROM {{ ref('cesc_identity_resolution') }} r
    JOIN {{ ref('cesc_bill_spine') }} b
        ON b.bill_source_key = r.bill_source_key
    JOIN cesc_silver.cesc_bill_only_consumer_map m
        ON m.consumer_no = b.consumer_no
       AND m.consumer_check_digit = b.consumer_check_digit
    WHERE r.resolution_status = 'UNRESOLVED'
      AND r.resolution_method = 'UNRESOLVED_NO_CIH_CANDIDATE'
),

cih_consumers AS (
    SELECT DISTINCT
        cih_source_row_key,
        consumer_no
    FROM {{ ref('cesc_cih_lookup_consumer') }}
),

-- Edge 1: CESC_ENTITY -> CIH_SOURCE_RECORD
e_entity_cih AS (
    SELECT
        'CESC_ENTITY' AS from_node_type,
        cesc_entity_id::TEXT AS from_node_id,
        'CIH_RECORD' AS to_node_type,
        primary_cih_source_row_key AS to_node_id,
        'HAS_CIH_RECORD' AS edge_type,
        'DETERMINISTIC' AS resolution_method,
        100.00 AS confidence_score,
        'DETERMINISTIC' AS score_type,
        '{{ var("run_id") }}' AS run_id,
        CLOCK_TIMESTAMP() AS created_at
    FROM entities
),

-- Edge 2: CESC_ENTITY -> PHONE (canonical)
e_entity_phone AS (
    SELECT
        'CESC_ENTITY' AS from_node_type,
        cesc_entity_id::TEXT AS from_node_id,
        'PHONE' AS to_node_type,
        canonical_mobile AS to_node_id,
        'HAS_PHONE' AS edge_type,
        'DETERMINISTIC' AS resolution_method,
        100.00 AS confidence_score,
        'DETERMINISTIC' AS score_type,
        '{{ var("run_id") }}' AS run_id,
        CLOCK_TIMESTAMP() AS created_at
    FROM entities
    WHERE canonical_mobile IS NOT NULL
),

-- Edge 3: CESC_ENTITY -> EMAIL (canonical)
e_entity_email AS (
    SELECT
        'CESC_ENTITY' AS from_node_type,
        cesc_entity_id::TEXT AS from_node_id,
        'EMAIL' AS to_node_type,
        canonical_email AS to_node_id,
        'HAS_EMAIL' AS edge_type,
        'DETERMINISTIC' AS resolution_method,
        100.00 AS confidence_score,
        'DETERMINISTIC' AS score_type,
        '{{ var("run_id") }}' AS run_id,
        CLOCK_TIMESTAMP() AS created_at
    FROM entities
    WHERE canonical_email IS NOT NULL
),

-- Edge 4: CESC_ENTITY -> CONSUMER_NUMBER (historical)
e_entity_consumer AS (
    SELECT DISTINCT
        'CESC_ENTITY' AS from_node_type,
        e.cesc_entity_id::TEXT AS from_node_id,
        'CONSUMER_NUMBER' AS to_node_type,
        cc.consumer_no AS to_node_id,
        'HAS_CONSUMER' AS edge_type,
        'DETERMINISTIC' AS resolution_method,
        100.00 AS confidence_score,
        'DETERMINISTIC' AS score_type,
        '{{ var("run_id") }}' AS run_id,
        CLOCK_TIMESTAMP() AS created_at
    FROM entities e
    JOIN cih_consumers cc ON e.primary_cih_source_row_key = cc.cih_source_row_key
),

-- Edge 5: CESC_ENTITY -> BILL_OBSERVATION (resolved bills)
e_entity_bill AS (
    SELECT
        'CESC_ENTITY' AS from_node_type,
        cesc_entity_id::TEXT AS from_node_id,
        'BILL_OBSERVATION' AS to_node_type,
        bill_source_key AS to_node_id,
        'OBSERVED_ON_BILL' AS edge_type,
        resolution_method,
        confidence_score,
        score_type,
        '{{ var("run_id") }}' AS run_id,
        CLOCK_TIMESTAMP() AS created_at
    FROM resolved_bills
),

-- Edge 6: BILL_ONLY_ENTITY -> BILL_OBSERVATION
e_bill_only_entity_bill AS (
    SELECT
        'CESC_ENTITY' AS from_node_type,
        cesc_entity_id::TEXT AS from_node_id,
        'BILL_OBSERVATION' AS to_node_type,
        bill_source_key AS to_node_id,
        'OBSERVED_ON_BILL' AS edge_type,
        'BILL_ONLY_IDENTITY' AS resolution_method,
        100.00 AS confidence_score,
        'DETERMINISTIC' AS score_type,
        '{{ var("run_id") }}' AS run_id,
        CLOCK_TIMESTAMP() AS created_at
    FROM bill_only_bills
),

-- Edge 7: UNRESOLVED_BILL -> CONSUMER_NUMBER (audit for unresolved observations)
e_unresolved_bill AS (
    SELECT
        'BILL_OBSERVATION' AS from_node_type,
        bill_source_key AS from_node_id,
        'CONSUMER_NUMBER' AS to_node_type,
        consumer_no AS to_node_id,
        'UNRESOLVED_BILL' AS edge_type,
        'UNRESOLVED' AS resolution_method,
        0.00 AS confidence_score,
        'NONE' AS score_type,
        '{{ var("run_id") }}' AS run_id,
        CLOCK_TIMESTAMP() AS created_at
    FROM unresolved_bills
)

SELECT * FROM e_entity_cih
UNION ALL
SELECT * FROM e_entity_phone
UNION ALL
SELECT * FROM e_entity_email
UNION ALL
SELECT * FROM e_entity_consumer
UNION ALL
SELECT * FROM e_entity_bill
UNION ALL
SELECT * FROM e_bill_only_entity_bill
UNION ALL
SELECT * FROM e_unresolved_bill

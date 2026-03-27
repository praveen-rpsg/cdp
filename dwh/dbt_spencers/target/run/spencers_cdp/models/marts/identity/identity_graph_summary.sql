
  
    

  create  table "cdp_meta"."silver_identity"."identity_graph_summary__dbt_tmp"
  
  
    as
  
  (
    

WITH edges AS (
    SELECT * FROM "cdp_meta"."silver_identity"."identity_edges"
),

profiles AS (
    SELECT * FROM "cdp_meta"."silver_identity"."unified_profiles"
),

edge_stats AS (
    SELECT
        unified_id,
        COUNT(*) AS total_edge_count,
        COUNT(DISTINCT identifier_type) AS distinct_id_types,
        COUNT(DISTINCT source_system) AS source_system_count,
        STRING_AGG(DISTINCT source_system, ', ' ORDER BY source_system) AS source_systems,
        COUNT(DISTINCT CASE WHEN identifier_type = 'mobile' THEN identifier_value END) AS mobile_count,
        COUNT(DISTINCT CASE WHEN identifier_type = 'email' THEN identifier_value END) AS email_count,
        COUNT(DISTINCT CASE WHEN identifier_type = 'store_affinity' THEN identifier_value END) AS store_count,
        AVG(confidence_score) AS avg_confidence
    FROM edges
    GROUP BY unified_id
)

SELECT
    p.unified_id,
    p.canonical_mobile,
    p.display_name,
    p.primary_source,
    p.has_transactions,
    COALESCE(es.total_edge_count, 0) AS total_edge_count,
    COALESCE(es.distinct_id_types, 0) AS distinct_id_types,
    COALESCE(es.source_system_count, 0) AS source_system_count,
    es.source_systems,
    COALESCE(es.mobile_count, 0) AS mobile_count,
    COALESCE(es.email_count, 0) AS email_count,
    COALESCE(es.store_count, 0) AS store_count,
    COALESCE(es.avg_confidence, 0) AS avg_confidence,
    -- Completeness score: how much profile data is filled
    (
        CASE WHEN p.canonical_mobile IS NOT NULL THEN 1 ELSE 0 END +
        CASE WHEN p.email IS NOT NULL THEN 1 ELSE 0 END +
        CASE WHEN p.display_name IS NOT NULL AND p.display_name != '' THEN 1 ELSE 0 END +
        CASE WHEN p.city IS NOT NULL THEN 1 ELSE 0 END +
        CASE WHEN p.pincode IS NOT NULL THEN 1 ELSE 0 END +
        CASE WHEN p.dob IS NOT NULL THEN 1 ELSE 0 END +
        CASE WHEN p.has_transactions THEN 1 ELSE 0 END
    )::NUMERIC / 7.0 AS completeness_score
FROM profiles p
LEFT JOIN edge_stats es ON p.unified_id = es.unified_id
  );
  
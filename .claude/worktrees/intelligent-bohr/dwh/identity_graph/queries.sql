-- ============================================================================
-- Identity Graph: Common Query Patterns
-- ============================================================================

-- 1. Lookup: Find a customer by mobile number
-- Returns the full profile with all connected identifiers
SELECT
    p.unified_id,
    p.canonical_mobile,
    p.canonical_name,
    p.canonical_email,
    p.primary_store_code,
    p.confidence_score,
    e.identifier_type,
    e.identifier_value,
    e.source_system,
    e.edge_type
FROM identity.unified_profiles p
JOIN identity.identity_edges e ON e.unified_id = p.unified_id
WHERE p.canonical_mobile = '9999999999'  -- replace with actual mobile
ORDER BY e.identifier_type, e.source_system;


-- 2. Graph traversal: Find all identifiers connected to a profile
SELECT
    e.unified_id,
    e.identifier_type,
    e.identifier_value,
    e.source_system,
    e.confidence,
    e.edge_type,
    e.first_seen_at,
    e.last_seen_at
FROM identity.identity_edges e
WHERE e.unified_id = 'SP_abc123'  -- replace with actual unified_id
  AND e.is_active = TRUE
ORDER BY e.confidence DESC;


-- 3. Reverse lookup: Given an email, find the unified profile
SELECT
    p.*
FROM identity.identity_edges e
JOIN identity.unified_profiles p ON p.unified_id = e.unified_id
WHERE e.identifier_type = 'email'
  AND e.identifier_value = 'user@example.com';


-- 4. Cross-system identity: Find customers present in multiple systems
SELECT
    p.unified_id,
    p.canonical_mobile,
    p.canonical_name,
    p.merge_count,
    ARRAY_AGG(DISTINCT e.source_system ORDER BY e.source_system) AS systems
FROM identity.unified_profiles p
JOIN identity.identity_edges e ON e.unified_id = p.unified_id
WHERE e.identifier_type = 'mobile'
GROUP BY p.unified_id, p.canonical_mobile, p.canonical_name, p.merge_count
HAVING COUNT(DISTINCT e.source_system) > 1
ORDER BY COUNT(DISTINCT e.source_system) DESC;


-- 5. Store affinity graph: Which stores does a customer visit?
SELECT
    p.unified_id,
    p.canonical_name,
    e.identifier_value AS store_code,
    l.store_name,
    l.store_city_description,
    e.first_seen_at,
    e.last_seen_at
FROM identity.unified_profiles p
JOIN identity.identity_edges e ON e.unified_id = p.unified_id
JOIN silver.s_location_master_history l ON l.store_code = e.identifier_value AND l.is_latest = TRUE
WHERE e.identifier_type = 'store_affinity'
  AND p.canonical_mobile = '9999999999';


-- 6. Graph quality: Profile completeness distribution
SELECT
    CASE
        WHEN gs.profile_completeness_score >= 0.8 THEN 'High (>=0.8)'
        WHEN gs.profile_completeness_score >= 0.5 THEN 'Medium (0.5-0.8)'
        ELSE 'Low (<0.5)'
    END AS quality_tier,
    COUNT(*) AS profile_count,
    ROUND(AVG(gs.total_edge_count), 1) AS avg_edges,
    ROUND(AVG(gs.source_system_count), 1) AS avg_sources
FROM identity.identity_graph_summary gs
GROUP BY 1
ORDER BY 1;


-- 7. Identity merge candidates: Profiles that might be duplicates
-- (Same name + same pincode but different mobile)
SELECT
    p1.unified_id AS profile_1,
    p1.canonical_mobile AS mobile_1,
    p2.unified_id AS profile_2,
    p2.canonical_mobile AS mobile_2,
    p1.canonical_name AS shared_name,
    p1.canonical_pincode AS shared_pincode
FROM identity.unified_profiles p1
JOIN identity.unified_profiles p2
    ON p1.canonical_name = p2.canonical_name
    AND p1.canonical_pincode = p2.canonical_pincode
    AND p1.unified_id < p2.unified_id  -- avoid duplicates
WHERE p1.canonical_name IS NOT NULL
  AND p1.canonical_pincode IS NOT NULL;


-- 8. Customer 360 view: Full profile with behavioral attributes
SELECT
    p.unified_id,
    p.canonical_mobile,
    p.canonical_name,
    p.canonical_email,
    p.primary_store_code,
    l.store_name AS primary_store_name,
    l.store_city_description AS primary_store_city,
    ba.lifecycle_stage,
    ba.churn_risk_score,
    ba.latest_nps_score,
    ba.nps_category,
    ba.has_given_feedback,
    ba.feedback_count,
    ba.has_used_promo,
    ba.promo_usage_count,
    ba.total_cashback,
    ba.customer_tenure_days,
    gs.profile_completeness_score,
    gs.source_systems,
    gs.stores_visited
FROM identity.unified_profiles p
LEFT JOIN silver.s_location_master_history l
    ON l.store_code = p.primary_store_code AND l.is_latest = TRUE
LEFT JOIN reverse_etl.customer_behavioral_attributes ba
    ON ba.customer_id = p.unified_id
LEFT JOIN identity.identity_graph_summary gs
    ON gs.unified_id = p.unified_id;



WITH profiles AS (
    SELECT unified_id, canonical_mobile, surrogate_id, primary_source
    FROM "cdp_meta"."silver_identity"."unified_profiles"
),

spine AS (
    SELECT * FROM "cdp_meta"."silver"."int_identity_spine"
),

-- Mobile edges: every profile has a mobile
mobile_edges AS (
    SELECT DISTINCT
        p.unified_id,
        'mobile' AS identifier_type,
        p.canonical_mobile AS identifier_value,
        CASE
            WHEN p.primary_source LIKE '%CIH%' THEN 'CIH'
            ELSE 'POS'
        END AS source_system,
        1.0 AS confidence_score
    FROM profiles p
    WHERE p.canonical_mobile IS NOT NULL
),

-- Email edges (from spine)
email_edges AS (
    SELECT DISTINCT
        p.unified_id,
        'email' AS identifier_type,
        s.email AS identifier_value,
        s.source_system,
        0.9 AS confidence_score
    FROM spine s
    INNER JOIN profiles p ON s.mobile = p.canonical_mobile
    WHERE s.email IS NOT NULL AND s.email != ''
),

-- Name edges (from spine)
name_edges AS (
    SELECT DISTINCT
        p.unified_id,
        'name' AS identifier_type,
        s.name AS identifier_value,
        s.source_system,
        0.7 AS confidence_score
    FROM spine s
    INNER JOIN profiles p ON s.mobile = p.canonical_mobile
    WHERE s.name IS NOT NULL AND s.name != '' AND LENGTH(s.name) > 1
),

-- Store affinity edges
store_edges AS (
    SELECT DISTINCT
        p.unified_id,
        'store_affinity' AS identifier_type,
        s.store_code AS identifier_value,
        s.source_system,
        0.8 AS confidence_score
    FROM spine s
    INNER JOIN profiles p ON s.mobile = p.canonical_mobile
    WHERE s.store_code IS NOT NULL AND s.store_code != ''
)

SELECT * FROM mobile_edges
UNION ALL
SELECT * FROM email_edges
UNION ALL
SELECT * FROM name_edges
UNION ALL
SELECT * FROM store_edges
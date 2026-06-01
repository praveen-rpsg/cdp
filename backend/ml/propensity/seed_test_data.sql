-- Quick smoke-test: seed a few rows into the propensity tables so the
-- segment builder returns non-zero audience counts before the model is run.
--
-- Replace the customer_id values with real unified_id values from your
-- silver_identity.unified_profiles table (run the query below to get some).
--
-- To find real customer IDs:
--   SELECT unified_id FROM silver_identity.unified_profiles LIMIT 10;
--   SELECT unified_id FROM nb_silver_identity.unified_profiles LIMIT 10;

-- ── Spencer's ──────────────────────────────────────────────────────────────
INSERT INTO silver_reverse_etl.customer_propensity_scores_spencers
    (customer_id,
     spencers_segment_1_propensity, spencers_segment_2_propensity,
     spencers_segment_3_propensity, spencers_segment_4_propensity,
     spencers_segment_5_propensity, spencers_segment_6_propensity,
     spencers_segment_1_normalized_propensity, spencers_segment_2_normalized_propensity,
     spencers_segment_3_normalized_propensity, spencers_segment_4_normalized_propensity,
     spencers_segment_5_normalized_propensity, spencers_segment_6_normalized_propensity)
VALUES
    -- strong food affinity
    ('0aa0cfa0f9dc92d204b332f44844be4e', 0.12, 0.82, 0.40, 0.05, 0.15, 0.55,  0.06, 0.39, 0.19, 0.02, 0.07, 0.26),
    -- strong fashion affinity
    ('dc6fb61a9b8cd3816a42dbe60ab2b6a9', 0.75, 0.20, 0.10, 0.08, 0.05, 0.30,  0.51, 0.14, 0.07, 0.05, 0.03, 0.20),
    -- hi-tech + GM mix
    ('c02395b6b74c0a597def2399ff72a344', 0.10, 0.30, 0.55, 0.70, 0.08, 0.25,  0.05, 0.15, 0.28, 0.35, 0.04, 0.13),
    -- non-food grocery dominant
    ('67e641926d5f6bd25c55458299dfa57d', 0.08, 0.45, 0.22, 0.03, 0.12, 0.78,  0.04, 0.22, 0.11, 0.01, 0.06, 0.38),
    -- balanced multi-segment
    ('232e83951b3954db0ebc568b0782168b', 0.50, 0.60, 0.55, 0.20, 0.35, 0.65,  0.17, 0.21, 0.19, 0.07, 0.12, 0.23)
ON CONFLICT (customer_id) DO UPDATE SET
    spencers_segment_1_propensity            = EXCLUDED.spencers_segment_1_propensity,
    spencers_segment_2_propensity            = EXCLUDED.spencers_segment_2_propensity,
    spencers_segment_3_propensity            = EXCLUDED.spencers_segment_3_propensity,
    spencers_segment_4_propensity            = EXCLUDED.spencers_segment_4_propensity,
    spencers_segment_5_propensity            = EXCLUDED.spencers_segment_5_propensity,
    spencers_segment_6_propensity            = EXCLUDED.spencers_segment_6_propensity,
    spencers_segment_1_normalized_propensity = EXCLUDED.spencers_segment_1_normalized_propensity,
    spencers_segment_2_normalized_propensity = EXCLUDED.spencers_segment_2_normalized_propensity,
    spencers_segment_3_normalized_propensity = EXCLUDED.spencers_segment_3_normalized_propensity,
    spencers_segment_4_normalized_propensity = EXCLUDED.spencers_segment_4_normalized_propensity,
    spencers_segment_5_normalized_propensity = EXCLUDED.spencers_segment_5_normalized_propensity,
    spencers_segment_6_normalized_propensity = EXCLUDED.spencers_segment_6_normalized_propensity,
    updated_at = NOW();

-- ── NBL ──────────────────────────────────────────────────────────────────
INSERT INTO nb_silver_reverse_etl.customer_propensity_scores_nbl
    (customer_id,
     nbl_segment_1_propensity, nbl_segment_2_propensity,
     nbl_segment_3_propensity, nbl_segment_4_propensity,
     nbl_segment_1_normalized_propensity, nbl_segment_2_normalized_propensity,
     nbl_segment_3_normalized_propensity, nbl_segment_4_normalized_propensity)
VALUES
    ('REPLACE_WITH_REAL_NBL_ID_1', 0.80, 0.25, 0.10, 0.40,  0.44, 0.14, 0.06, 0.22),
    ('REPLACE_WITH_REAL_NBL_ID_2', 0.30, 0.70, 0.20, 0.60,  0.17, 0.39, 0.11, 0.33)
ON CONFLICT (customer_id) DO UPDATE SET
    nbl_segment_1_propensity            = EXCLUDED.nbl_segment_1_propensity,
    nbl_segment_2_propensity            = EXCLUDED.nbl_segment_2_propensity,
    nbl_segment_3_propensity            = EXCLUDED.nbl_segment_3_propensity,
    nbl_segment_4_propensity            = EXCLUDED.nbl_segment_4_propensity,
    nbl_segment_1_normalized_propensity = EXCLUDED.nbl_segment_1_normalized_propensity,
    nbl_segment_2_normalized_propensity = EXCLUDED.nbl_segment_2_normalized_propensity,
    nbl_segment_3_normalized_propensity = EXCLUDED.nbl_segment_3_normalized_propensity,
    nbl_segment_4_normalized_propensity = EXCLUDED.nbl_segment_4_normalized_propensity,
    updated_at = NOW();

-- ── Verify ────────────────────────────────────────────────────────────────
SELECT 'spencers' AS brand, COUNT(*) AS rows FROM silver_reverse_etl.customer_propensity_scores_spencers
UNION ALL
SELECT 'nbl',              COUNT(*)          FROM nb_silver_reverse_etl.customer_propensity_scores_nbl;

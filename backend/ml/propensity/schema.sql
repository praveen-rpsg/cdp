-- Output table DDL for propensity scores.
-- Run ONCE before using affinity attributes in the segment builder.
-- Safe to re-run (all statements are idempotent).
--
-- Spencers — 6 segments × 2 (raw + normalized) = 12 score columns

CREATE SCHEMA IF NOT EXISTS silver_reverse_etl;
CREATE SCHEMA IF NOT EXISTS nb_silver_reverse_etl;

CREATE TABLE IF NOT EXISTS silver_reverse_etl.customer_propensity_scores_spencers (
    customer_id                              TEXT        NOT NULL PRIMARY KEY,

    -- Raw propensity scores (independent binary model outputs, 0–1 each)
    spencers_segment_1_propensity            NUMERIC(6,4),   -- FASHION CB
    spencers_segment_2_propensity            NUMERIC(6,4),   -- FOOD
    spencers_segment_3_propensity            NUMERIC(6,4),   -- GM
    spencers_segment_4_propensity            NUMERIC(6,4),   -- HI TECH
    spencers_segment_5_propensity            NUMERIC(6,4),   -- NON TRADE
    spencers_segment_6_propensity            NUMERIC(6,4),   -- NON FOOD GROCERY

    -- Normalized scores (sum to 1 per customer)
    spencers_segment_1_normalized_propensity NUMERIC(6,4),
    spencers_segment_2_normalized_propensity NUMERIC(6,4),
    spencers_segment_3_normalized_propensity NUMERIC(6,4),
    spencers_segment_4_normalized_propensity NUMERIC(6,4),
    spencers_segment_5_normalized_propensity NUMERIC(6,4),
    spencers_segment_6_normalized_propensity NUMERIC(6,4),

    model_run_date                           DATE         DEFAULT CURRENT_DATE,
    updated_at                               TIMESTAMPTZ  DEFAULT NOW()
);

COMMENT ON COLUMN silver_reverse_etl.customer_propensity_scores_spencers.spencers_segment_1_propensity IS 'FASHION CB — raw LightGBM binary propensity';
COMMENT ON COLUMN silver_reverse_etl.customer_propensity_scores_spencers.spencers_segment_2_propensity IS 'FOOD — raw LightGBM binary propensity';
COMMENT ON COLUMN silver_reverse_etl.customer_propensity_scores_spencers.spencers_segment_3_propensity IS 'GM — raw LightGBM binary propensity';
COMMENT ON COLUMN silver_reverse_etl.customer_propensity_scores_spencers.spencers_segment_4_propensity IS 'HI TECH — raw LightGBM binary propensity';
COMMENT ON COLUMN silver_reverse_etl.customer_propensity_scores_spencers.spencers_segment_5_propensity IS 'NON TRADE — raw LightGBM binary propensity';
COMMENT ON COLUMN silver_reverse_etl.customer_propensity_scores_spencers.spencers_segment_6_propensity IS 'NON FOOD GROCERY — raw LightGBM binary propensity';


-- NBL — 4 segments × 2 = 8 score columns

CREATE TABLE IF NOT EXISTS nb_silver_reverse_etl.customer_propensity_scores_nbl (
    customer_id                          TEXT        NOT NULL PRIMARY KEY,

    -- Raw propensity scores
    nbl_segment_1_propensity             NUMERIC(6,4),   -- FOOD
    nbl_segment_2_propensity             NUMERIC(6,4),   -- GM
    nbl_segment_3_propensity             NUMERIC(6,4),   -- NON TRADE
    nbl_segment_4_propensity             NUMERIC(6,4),   -- NON FOOD GROCERY

    -- Normalized scores (sum to 1 per customer)
    nbl_segment_1_normalized_propensity  NUMERIC(6,4),
    nbl_segment_2_normalized_propensity  NUMERIC(6,4),
    nbl_segment_3_normalized_propensity  NUMERIC(6,4),
    nbl_segment_4_normalized_propensity  NUMERIC(6,4),

    model_run_date                       DATE         DEFAULT CURRENT_DATE,
    updated_at                           TIMESTAMPTZ  DEFAULT NOW()
);

COMMENT ON COLUMN nb_silver_reverse_etl.customer_propensity_scores_nbl.nbl_segment_1_propensity IS 'FOOD — raw LightGBM binary propensity';
COMMENT ON COLUMN nb_silver_reverse_etl.customer_propensity_scores_nbl.nbl_segment_2_propensity IS 'GM — raw LightGBM binary propensity';
COMMENT ON COLUMN nb_silver_reverse_etl.customer_propensity_scores_nbl.nbl_segment_3_propensity IS 'NON TRADE — raw LightGBM binary propensity';
COMMENT ON COLUMN nb_silver_reverse_etl.customer_propensity_scores_nbl.nbl_segment_4_propensity IS 'NON FOOD GROCERY — raw LightGBM binary propensity';

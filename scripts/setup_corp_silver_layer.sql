-- =============================================================================
-- Corporate Silver Layer — Phase 1: DDL
-- Database : cdp_meta  (PostgreSQL 16)
-- Schema   : corporate_cih
-- =============================================================================
-- Creates 3 tables:
--
--   corporate_cih.silver_spn_attributes
--       One row per r1_id for every customer identified as a Spencers shopper
--       in raw_corp_cih. All 61 BA columns prefixed SPN_.
--
--   corporate_cih.silver_nbl_attributes
--       One row per r1_id for every customer identified as an NBL shopper.
--       All 61 BA columns prefixed NBL_.
--
--   corporate_cih.silver_corp_customer_attributes
--       Master cross-brand table. One row per r1_id. FULL OUTER JOIN of the
--       two silver tables plus RPSG-lifetime metrics from raw_corp_billanalytics.
--       NULLs where a brand doesn't apply; cross-brand customers (252K) have
--       all columns populated.
--
-- Safe to re-run: DROP TABLE IF EXISTS before every CREATE.
-- Indexes are created at the end (run AFTER population for speed).
-- =============================================================================

-- ── 1. silver_spn_attributes ──────────────────────────────────────────────────
DROP TABLE IF EXISTS corporate_cih.silver_spn_attributes;

CREATE TABLE corporate_cih.silver_spn_attributes (

    -- Identity / join keys
    r1_id                           TEXT    NOT NULL,   -- RPSG universal ID (from raw_corp_cih)
    mobile                          TEXT,               -- brand_id / mobile number (join key)

    -- Profile attributes (from silver_reverse_etl.customer_behavioral_attributes)
    spn_display_name                TEXT,
    spn_email                       TEXT,
    spn_city                        TEXT,
    spn_pincode                     TEXT,
    spn_region                      TEXT,
    spn_registered_store            TEXT,
    spn_age                         INTEGER,
    spn_customer_group              TEXT,
    spn_occupation                  TEXT,
    spn_whatsapp                    TEXT,
    spn_dnd                         TEXT,
    spn_gw_customer_flag            TEXT,
    spn_accepts_email_marketing     TEXT,
    spn_accepts_sms_marketing       TEXT,
    spn_surrogate_id                TEXT,

    -- Transactional / behavioral attributes
    spn_first_bill_date             DATE,
    spn_last_bill_date              DATE,
    spn_recency_days                INTEGER,
    spn_tenure_days                 INTEGER,
    spn_total_bills                 BIGINT,
    spn_total_visits                BIGINT,
    spn_total_spend                 NUMERIC,
    spn_spend_per_bill              NUMERIC,
    spn_spend_per_visit             NUMERIC,
    spn_avg_items_per_bill          NUMERIC,
    spn_total_discount              NUMERIC,
    spn_distinct_months             BIGINT,
    spn_distinct_store_count        BIGINT,
    spn_distinct_article_count      BIGINT,
    spn_dgbt_fs                     INTEGER,
    spn_avg_billing_time_secs       NUMERIC,
    spn_return_bill_count           BIGINT,
    spn_promo_bill_count            BIGINT,
    spn_weekend_bill_count          BIGINT,
    spn_wednesday_bill_count        BIGINT,

    -- Favourites
    spn_fav_store_code              TEXT,
    spn_fav_store_name              TEXT,
    spn_fav_store_type              TEXT,
    spn_fav_day                     TEXT,
    spn_fav_article_by_spend        TEXT,
    spn_fav_article_by_spend_desc   TEXT,
    spn_fav_article_by_nob          TEXT,
    spn_fav_article_by_nob_desc     TEXT,
    spn_second_fav_article_by_spend TEXT,
    spn_second_fav_article_by_nob   TEXT,

    -- Channel & segmentation
    spn_channel_presence            TEXT,
    spn_spend_decile                INTEGER,
    spn_nob_decile                  INTEGER,
    spn_l1_segment                  TEXT,
    spn_l2_segment                  TEXT,
    spn_store_spend                 NUMERIC,
    spn_online_spend                NUMERIC,
    spn_store_bills                 BIGINT,
    spn_online_bills                BIGINT,
    spn_lifecycle_stage             TEXT,
    spn_rfm_recency_score           INTEGER,
    spn_rfm_frequency_score         INTEGER,
    spn_rfm_monetary_score          INTEGER,
    spn_computed_at                 TIMESTAMP WITH TIME ZONE
);

COMMENT ON TABLE corporate_cih.silver_spn_attributes IS
  'Spencers behavioral attributes joined via CIH mobile key. One row per r1_id. All columns prefixed spn_.';

-- ── 2. silver_nbl_attributes ──────────────────────────────────────────────────
DROP TABLE IF EXISTS corporate_cih.silver_nbl_attributes;

CREATE TABLE corporate_cih.silver_nbl_attributes (

    -- Identity / join keys
    r1_id                           TEXT    NOT NULL,
    mobile                          TEXT,

    -- Profile attributes (from nb_silver_reverse_etl.customer_behavioral_attributes)
    nbl_display_name                TEXT,
    nbl_email                       TEXT,
    nbl_city                        TEXT,
    nbl_pincode                     TEXT,
    nbl_region                      TEXT,
    nbl_registered_store            TEXT,
    nbl_age                         INTEGER,
    nbl_customer_group              TEXT,
    nbl_occupation                  TEXT,
    nbl_whatsapp                    TEXT,
    nbl_dnd                         TEXT,
    nbl_gw_customer_flag            TEXT,
    nbl_accepts_email_marketing     TEXT,
    nbl_accepts_sms_marketing       TEXT,
    nbl_surrogate_id                TEXT,

    -- Transactional / behavioral attributes
    nbl_first_bill_date             DATE,
    nbl_last_bill_date              DATE,
    nbl_recency_days                INTEGER,
    nbl_tenure_days                 INTEGER,
    nbl_total_bills                 BIGINT,
    nbl_total_visits                BIGINT,
    nbl_total_spend                 NUMERIC,
    nbl_spend_per_bill              NUMERIC,
    nbl_spend_per_visit             NUMERIC,
    nbl_avg_items_per_bill          NUMERIC,
    nbl_total_discount              NUMERIC,
    nbl_distinct_months             BIGINT,
    nbl_distinct_store_count        BIGINT,
    nbl_distinct_article_count      BIGINT,
    nbl_dgbt_fs                     INTEGER,
    nbl_avg_billing_time_secs       NUMERIC,
    nbl_return_bill_count           BIGINT,
    nbl_promo_bill_count            BIGINT,
    nbl_weekend_bill_count          BIGINT,
    nbl_wednesday_bill_count        BIGINT,

    -- Favourites
    nbl_fav_store_code              TEXT,
    nbl_fav_store_name              TEXT,
    nbl_fav_store_type              TEXT,
    nbl_fav_day                     TEXT,
    nbl_fav_article_by_spend        TEXT,
    nbl_fav_article_by_spend_desc   TEXT,
    nbl_fav_article_by_nob          TEXT,
    nbl_fav_article_by_nob_desc     TEXT,
    nbl_second_fav_article_by_spend TEXT,
    nbl_second_fav_article_by_nob   TEXT,

    -- Channel & segmentation
    nbl_channel_presence            TEXT,
    nbl_spend_decile                INTEGER,
    nbl_nob_decile                  INTEGER,
    nbl_l1_segment                  TEXT,
    nbl_l2_segment                  TEXT,
    nbl_store_spend                 NUMERIC,
    nbl_online_spend                NUMERIC,
    nbl_store_bills                 BIGINT,
    nbl_online_bills                BIGINT,
    nbl_lifecycle_stage             TEXT,
    nbl_rfm_recency_score           INTEGER,
    nbl_rfm_frequency_score         INTEGER,
    nbl_rfm_monetary_score          INTEGER,
    nbl_computed_at                 TIMESTAMP WITH TIME ZONE
);

COMMENT ON TABLE corporate_cih.silver_nbl_attributes IS
  'NBL behavioral attributes joined via CIH mobile key. One row per r1_id. All columns prefixed nbl_.';

-- ── 3. silver_corp_customer_attributes (master cross-brand table) ─────────────
DROP TABLE IF EXISTS corporate_cih.silver_corp_customer_attributes;

CREATE TABLE corporate_cih.silver_corp_customer_attributes (

    -- ── Universal identity ────────────────────────────────────────────────────
    r1_id                           TEXT    NOT NULL,   -- RPSG universal customer ID
    mobile                          TEXT,               -- canonical mobile (from CIH)
    rpsg_brand_presence             TEXT,               -- e.g. "['Spencer' 'NBL']"

    -- ── RPSG-wide lifetime metrics (raw_corp_billanalytics) ───────────────────
    rpsg_ftd                        DATE,               -- first transaction date across all brands
    rpsg_ltd                        DATE,               -- last  transaction date across all brands
    rpsg_tenure_lifetime            INTEGER,            -- days since first txn (cross-brand)
    rpsg_recency_lifetime           INTEGER,            -- days since last  txn (cross-brand)

    -- ── Spencers attributes (spn_*) ──────────────────────────────────────────
    spn_display_name                TEXT,
    spn_email                       TEXT,
    spn_city                        TEXT,
    spn_pincode                     TEXT,
    spn_region                      TEXT,
    spn_registered_store            TEXT,
    spn_age                         INTEGER,
    spn_customer_group              TEXT,
    spn_occupation                  TEXT,
    spn_whatsapp                    TEXT,
    spn_dnd                         TEXT,
    spn_gw_customer_flag            TEXT,
    spn_accepts_email_marketing     TEXT,
    spn_accepts_sms_marketing       TEXT,
    spn_surrogate_id                TEXT,
    spn_first_bill_date             DATE,
    spn_last_bill_date              DATE,
    spn_recency_days                INTEGER,
    spn_tenure_days                 INTEGER,
    spn_total_bills                 BIGINT,
    spn_total_visits                BIGINT,
    spn_total_spend                 NUMERIC,
    spn_spend_per_bill              NUMERIC,
    spn_spend_per_visit             NUMERIC,
    spn_avg_items_per_bill          NUMERIC,
    spn_total_discount              NUMERIC,
    spn_distinct_months             BIGINT,
    spn_distinct_store_count        BIGINT,
    spn_distinct_article_count      BIGINT,
    spn_dgbt_fs                     INTEGER,
    spn_avg_billing_time_secs       NUMERIC,
    spn_return_bill_count           BIGINT,
    spn_promo_bill_count            BIGINT,
    spn_weekend_bill_count          BIGINT,
    spn_wednesday_bill_count        BIGINT,
    spn_fav_store_code              TEXT,
    spn_fav_store_name              TEXT,
    spn_fav_store_type              TEXT,
    spn_fav_day                     TEXT,
    spn_fav_article_by_spend        TEXT,
    spn_fav_article_by_spend_desc   TEXT,
    spn_fav_article_by_nob          TEXT,
    spn_fav_article_by_nob_desc     TEXT,
    spn_second_fav_article_by_spend TEXT,
    spn_second_fav_article_by_nob   TEXT,
    spn_channel_presence            TEXT,
    spn_spend_decile                INTEGER,
    spn_nob_decile                  INTEGER,
    spn_l1_segment                  TEXT,
    spn_l2_segment                  TEXT,
    spn_store_spend                 NUMERIC,
    spn_online_spend                NUMERIC,
    spn_store_bills                 BIGINT,
    spn_online_bills                BIGINT,
    spn_lifecycle_stage             TEXT,
    spn_rfm_recency_score           INTEGER,
    spn_rfm_frequency_score         INTEGER,
    spn_rfm_monetary_score          INTEGER,
    spn_computed_at                 TIMESTAMP WITH TIME ZONE,

    -- ── NBL attributes (nbl_*) ────────────────────────────────────────────────
    nbl_display_name                TEXT,
    nbl_email                       TEXT,
    nbl_city                        TEXT,
    nbl_pincode                     TEXT,
    nbl_region                      TEXT,
    nbl_registered_store            TEXT,
    nbl_age                         INTEGER,
    nbl_customer_group              TEXT,
    nbl_occupation                  TEXT,
    nbl_whatsapp                    TEXT,
    nbl_dnd                         TEXT,
    nbl_gw_customer_flag            TEXT,
    nbl_accepts_email_marketing     TEXT,
    nbl_accepts_sms_marketing       TEXT,
    nbl_surrogate_id                TEXT,
    nbl_first_bill_date             DATE,
    nbl_last_bill_date              DATE,
    nbl_recency_days                INTEGER,
    nbl_tenure_days                 INTEGER,
    nbl_total_bills                 BIGINT,
    nbl_total_visits                BIGINT,
    nbl_total_spend                 NUMERIC,
    nbl_spend_per_bill              NUMERIC,
    nbl_spend_per_visit             NUMERIC,
    nbl_avg_items_per_bill          NUMERIC,
    nbl_total_discount              NUMERIC,
    nbl_distinct_months             BIGINT,
    nbl_distinct_store_count        BIGINT,
    nbl_distinct_article_count      BIGINT,
    nbl_dgbt_fs                     INTEGER,
    nbl_avg_billing_time_secs       NUMERIC,
    nbl_return_bill_count           BIGINT,
    nbl_promo_bill_count            BIGINT,
    nbl_weekend_bill_count          BIGINT,
    nbl_wednesday_bill_count        BIGINT,
    nbl_fav_store_code              TEXT,
    nbl_fav_store_name              TEXT,
    nbl_fav_store_type              TEXT,
    nbl_fav_day                     TEXT,
    nbl_fav_article_by_spend        TEXT,
    nbl_fav_article_by_spend_desc   TEXT,
    nbl_fav_article_by_nob          TEXT,
    nbl_fav_article_by_nob_desc     TEXT,
    nbl_second_fav_article_by_spend TEXT,
    nbl_second_fav_article_by_nob   TEXT,
    nbl_channel_presence            TEXT,
    nbl_spend_decile                INTEGER,
    nbl_nob_decile                  INTEGER,
    nbl_l1_segment                  TEXT,
    nbl_l2_segment                  TEXT,
    nbl_store_spend                 NUMERIC,
    nbl_online_spend                NUMERIC,
    nbl_store_bills                 BIGINT,
    nbl_online_bills                BIGINT,
    nbl_lifecycle_stage             TEXT,
    nbl_rfm_recency_score           INTEGER,
    nbl_rfm_frequency_score         INTEGER,
    nbl_rfm_monetary_score          INTEGER,
    nbl_computed_at                 TIMESTAMP WITH TIME ZONE
);

COMMENT ON TABLE corporate_cih.silver_corp_customer_attributes IS
  'Master cross-brand customer attribute table. One row per RPSG R1 ID. '
  'SPN_* columns populated for Spencers customers, NBL_* for NBL customers. '
  'Cross-brand customers (252K) have both sets populated. '
  'RPSG lifetime metrics from raw_corp_billanalytics always present where available.';

COMMENT ON COLUMN corporate_cih.silver_corp_customer_attributes.r1_id IS
  'RPSG universal customer identifier — primary key for cross-brand joins.';
COMMENT ON COLUMN corporate_cih.silver_corp_customer_attributes.rpsg_brand_presence IS
  'All RPSG brands the customer is associated with, e.g. [''Spencer'' ''NBL''].';
COMMENT ON COLUMN corporate_cih.silver_corp_customer_attributes.rpsg_ftd IS
  'First transaction date across ALL RPSG brands (from raw_corp_billanalytics).';
COMMENT ON COLUMN corporate_cih.silver_corp_customer_attributes.rpsg_ltd IS
  'Last transaction date across ALL RPSG brands (from raw_corp_billanalytics).';

-- ── 4. Indexes (created post-population — listed here for reference) ──────────
-- Run these AFTER the loader populates all three tables:
--
-- CREATE UNIQUE INDEX idx_silver_spn_r1      ON corporate_cih.silver_spn_attributes (r1_id);
-- CREATE        INDEX idx_silver_spn_mobile  ON corporate_cih.silver_spn_attributes (mobile);
-- CREATE        INDEX idx_silver_spn_lc      ON corporate_cih.silver_spn_attributes (spn_lifecycle_stage);
--
-- CREATE UNIQUE INDEX idx_silver_nbl_r1      ON corporate_cih.silver_nbl_attributes (r1_id);
-- CREATE        INDEX idx_silver_nbl_mobile  ON corporate_cih.silver_nbl_attributes (mobile);
-- CREATE        INDEX idx_silver_nbl_lc      ON corporate_cih.silver_nbl_attributes (nbl_lifecycle_stage);
--
-- CREATE UNIQUE INDEX idx_silver_corp_r1     ON corporate_cih.silver_corp_customer_attributes (r1_id);
-- CREATE        INDEX idx_silver_corp_mobile ON corporate_cih.silver_corp_customer_attributes (mobile);
-- CREATE        INDEX idx_silver_corp_spn_lc ON corporate_cih.silver_corp_customer_attributes (spn_lifecycle_stage);
-- CREATE        INDEX idx_silver_corp_nbl_lc ON corporate_cih.silver_corp_customer_attributes (nbl_lifecycle_stage);

\echo ''
\echo 'Phase 1 complete: 3 silver tables created in corporate_cih schema.'
\echo '  corporate_cih.silver_spn_attributes          (60 spn_ columns)'
\echo '  corporate_cih.silver_nbl_attributes          (60 nbl_ columns)'
\echo '  corporate_cih.silver_corp_customer_attributes (120 cross-brand columns + RPSG lifetime)'
\echo ''
\echo 'Next: Phase 2 — run load_corp_silver_layer.py to populate all three tables.'

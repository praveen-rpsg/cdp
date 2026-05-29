-- =============================================================================
-- Corporate CIH Schema Setup
-- Database : cdp_meta  (PostgreSQL 16)
-- Schema   : corporate_cih
-- =============================================================================
-- Run as a superuser or a role that has CREATE privilege on cdp_meta.
-- Safe to re-run: all statements use IF NOT EXISTS / CREATE OR REPLACE.
-- =============================================================================

-- ── 1. Schema ─────────────────────────────────────────────────────────────────
CREATE SCHEMA IF NOT EXISTS corporate_cih;

COMMENT ON SCHEMA corporate_cih IS
  'Corporate Identity Hub (CIH) — cross-brand customer identity and lifetime bill analytics for RPSG Group.';

-- ── 2. raw_corp_cih ──────────────────────────────────────────────────────────
-- Source : C:\Corp_CIH\Corporate_CIHMaster_*.csv  (30 files, ~29.3 M rows)
-- CSV    : R1_s | BrandID_s | CustomerType_ss | RPSG_Brand_Presence_ss
-- Suffix legend: _s = scalar string, _ss = multi-valued string (Python list repr)

DROP TABLE IF EXISTS corporate_cih.raw_corp_cih;

CREATE TABLE corporate_cih.raw_corp_cih (
    r1_id               TEXT        NOT NULL,   -- R1_s  : RPSG universal customer ID
    brand_id            TEXT,                   -- BrandID_s  : brand-specific customer ID / phone
    customer_type       TEXT,                   -- CustomerType_ss : e.g. "['Spencer']" or "['CESC' 'NBL']"
    rpsg_brand_presence TEXT                    -- RPSG_Brand_Presence_ss : same format as customer_type
);

COMMENT ON TABLE  corporate_cih.raw_corp_cih IS
  'Raw Corporate Identity Hub master — one row per (R1 customer, brand) pairing across all RPSG brands.';
COMMENT ON COLUMN corporate_cih.raw_corp_cih.r1_id               IS 'RPSG universal customer identifier (R1 ID).';
COMMENT ON COLUMN corporate_cih.raw_corp_cih.brand_id            IS 'Brand-specific customer ID or mobile number.';
COMMENT ON COLUMN corporate_cih.raw_corp_cih.customer_type       IS 'Python-style list of brand names the customer belongs to.';
COMMENT ON COLUMN corporate_cih.raw_corp_cih.rpsg_brand_presence IS 'Python-style list of all RPSG brands the customer has a presence in.';

-- ── 3. raw_corp_billanalytics ─────────────────────────────────────────────────
-- Source : C:\Corp_BillAnalytics\Corporate_BillAnalytics_Customer_Lifetime_*.csv (25 files, ~24.8 M rows)
-- CSV    : R1_s | RPSG_FTD_dt | RPSG_LTD_dt | RPSG_TENURE_Lifetime_i | RPSG_RECENCY_Lifetime_i
-- Suffix legend: _dt = date, _i = integer (stored as float in CSVs → cast to INTEGER)

DROP TABLE IF EXISTS corporate_cih.raw_corp_billanalytics;

CREATE TABLE corporate_cih.raw_corp_billanalytics (
    r1_id                 TEXT    NOT NULL,   -- R1_s
    rpsg_ftd              DATE,               -- RPSG_FTD_dt  : first transaction date (lifetime)
    rpsg_ltd              DATE,               -- RPSG_LTD_dt  : last  transaction date (lifetime)
    rpsg_tenure_lifetime  INTEGER,            -- RPSG_TENURE_Lifetime_i  : days since first txn
    rpsg_recency_lifetime INTEGER             -- RPSG_RECENCY_Lifetime_i : days since last  txn
);

COMMENT ON TABLE  corporate_cih.raw_corp_billanalytics IS
  'Raw Corporate Bill Analytics — lifetime transaction summary per RPSG customer (R1 ID).';
COMMENT ON COLUMN corporate_cih.raw_corp_billanalytics.r1_id                 IS 'RPSG universal customer identifier (R1 ID).';
COMMENT ON COLUMN corporate_cih.raw_corp_billanalytics.rpsg_ftd              IS 'First transaction date across all RPSG brands (lifetime).';
COMMENT ON COLUMN corporate_cih.raw_corp_billanalytics.rpsg_ltd              IS 'Last transaction date across all RPSG brands (lifetime).';
COMMENT ON COLUMN corporate_cih.raw_corp_billanalytics.rpsg_tenure_lifetime  IS 'Days elapsed since first transaction (tenure in days).';
COMMENT ON COLUMN corporate_cih.raw_corp_billanalytics.rpsg_recency_lifetime IS 'Days elapsed since last transaction (recency in days).';

-- ── 4. Indexes (created AFTER bulk load for speed — see loader script) ────────
-- Uncomment and run once loading is complete.
--
-- CREATE INDEX IF NOT EXISTS idx_corp_cih_r1_id
--     ON corporate_cih.raw_corp_cih (r1_id);
--
-- CREATE INDEX IF NOT EXISTS idx_corp_ba_r1_id
--     ON corporate_cih.raw_corp_billanalytics (r1_id);

\echo 'Schema corporate_cih created with tables raw_corp_cih and raw_corp_billanalytics.'

"""
Phase 2 — Populate Corporate Silver Layer
==========================================

Populates the 3 silver tables created in Phase 1 (setup_corp_silver_layer.sql):

  Step 1 → corporate_cih.silver_spn_attributes
      raw_corp_cih (Spencer customers) × silver_reverse_etl.customer_behavioral_attributes
      JOIN key: raw_corp_cih.brand_id = customer_behavioral_attributes.mobile

  Step 2 → corporate_cih.silver_nbl_attributes
      raw_corp_cih (NBL customers) × nb_silver_reverse_etl.customer_behavioral_attributes
      JOIN key: raw_corp_cih.brand_id = customer_behavioral_attributes.mobile

  Step 3 → corporate_cih.silver_corp_customer_attributes
      FULL OUTER JOIN silver_spn_attributes + silver_nbl_attributes ON r1_id
      + raw_corp_cih (for rpsg_brand_presence)
      + raw_corp_billanalytics (for RPSG-wide lifetime metrics)

Usage
-----
  python load_corp_silver_layer.py [--step1-only | --step2-only | --step3-only]

Each step TRUNCATEs its target table before inserting.
Indexes are created after Step 3 completes (or use --no-index to skip).
"""

from __future__ import annotations

import argparse
import logging
import time

import psycopg2

# ── Config ────────────────────────────────────────────────────────────────────
PG_CONN = (
    "host=localhost port=5432 dbname=cdp_meta "
    "user=cdp password=cdp"
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

# ── SQL statements ─────────────────────────────────────────────────────────────

SQL_TRUNCATE_SPN = "TRUNCATE TABLE corporate_cih.silver_spn_attributes"
SQL_TRUNCATE_NBL = "TRUNCATE TABLE corporate_cih.silver_nbl_attributes"
SQL_TRUNCATE_CORP = "TRUNCATE TABLE corporate_cih.silver_corp_customer_attributes"

# ── Step 1: Spencer's silver attributes ───────────────────────────────────────
SQL_INSERT_SPN = """
INSERT INTO corporate_cih.silver_spn_attributes (
    r1_id, mobile,
    spn_display_name, spn_email, spn_city, spn_pincode, spn_region,
    spn_registered_store, spn_age, spn_customer_group, spn_occupation,
    spn_whatsapp, spn_dnd, spn_gw_customer_flag,
    spn_accepts_email_marketing, spn_accepts_sms_marketing, spn_surrogate_id,
    spn_first_bill_date, spn_last_bill_date,
    spn_recency_days, spn_tenure_days,
    spn_total_bills, spn_total_visits, spn_total_spend,
    spn_spend_per_bill, spn_spend_per_visit, spn_avg_items_per_bill,
    spn_total_discount, spn_distinct_months,
    spn_distinct_store_count, spn_distinct_article_count,
    spn_dgbt_fs, spn_avg_billing_time_secs,
    spn_return_bill_count, spn_promo_bill_count,
    spn_weekend_bill_count, spn_wednesday_bill_count,
    spn_fav_store_code, spn_fav_store_name, spn_fav_store_type, spn_fav_day,
    spn_fav_article_by_spend, spn_fav_article_by_spend_desc,
    spn_fav_article_by_nob, spn_fav_article_by_nob_desc,
    spn_second_fav_article_by_spend, spn_second_fav_article_by_nob,
    spn_channel_presence, spn_spend_decile, spn_nob_decile,
    spn_l1_segment, spn_l2_segment,
    spn_store_spend, spn_online_spend, spn_store_bills, spn_online_bills,
    spn_lifecycle_stage,
    spn_rfm_recency_score, spn_rfm_frequency_score, spn_rfm_monetary_score,
    spn_computed_at
)
SELECT DISTINCT ON (c.r1_id)
    c.r1_id,
    c.brand_id                      AS mobile,
    ba.display_name,
    ba.email,
    ba.city,
    ba.pincode,
    ba.region,
    ba.registered_store,
    ba.age,
    ba.customer_group,
    ba.occupation,
    ba.whatsapp,
    ba.dnd,
    ba.gw_customer_flag,
    ba.accepts_email_marketing,
    ba.accepts_sms_marketing,
    ba.surrogate_id,
    ba.first_bill_date,
    ba.last_bill_date,
    ba.recency_days,
    ba.tenure_days,
    ba.total_bills,
    ba.total_visits,
    ba.total_spend,
    ba.spend_per_bill,
    ba.spend_per_visit,
    ba.avg_items_per_bill,
    ba.total_discount,
    ba.distinct_months,
    ba.distinct_store_count,
    ba.distinct_article_count,
    ba.dgbt_fs,
    ba.avg_billing_time_secs,
    ba.return_bill_count,
    ba.promo_bill_count,
    ba.weekend_bill_count,
    ba.wednesday_bill_count,
    ba.fav_store_code,
    ba.fav_store_name,
    ba.fav_store_type,
    ba.fav_day,
    ba.fav_article_by_spend,
    ba.fav_article_by_spend_desc,
    ba.fav_article_by_nob,
    ba.fav_article_by_nob_desc,
    ba.second_fav_article_by_spend,
    ba.second_fav_article_by_nob,
    ba.channel_presence,
    ba.spend_decile,
    ba.nob_decile,
    ba.l1_segment,
    ba.l2_segment,
    ba.store_spend,
    ba.online_spend,
    ba.store_bills,
    ba.online_bills,
    ba.lifecycle_stage,
    ba.rfm_recency_score,
    ba.rfm_frequency_score,
    ba.rfm_monetary_score,
    ba.computed_at
FROM corporate_cih.raw_corp_cih c
JOIN silver_reverse_etl.customer_behavioral_attributes ba
    ON c.brand_id = ba.mobile
WHERE c.customer_type LIKE '%Spencer%'
ORDER BY c.r1_id, ba.computed_at DESC NULLS LAST
"""

# ── Step 2: NBL silver attributes ─────────────────────────────────────────────
SQL_INSERT_NBL = """
INSERT INTO corporate_cih.silver_nbl_attributes (
    r1_id, mobile,
    nbl_display_name, nbl_email, nbl_city, nbl_pincode, nbl_region,
    nbl_registered_store, nbl_age, nbl_customer_group, nbl_occupation,
    nbl_whatsapp, nbl_dnd, nbl_gw_customer_flag,
    nbl_accepts_email_marketing, nbl_accepts_sms_marketing, nbl_surrogate_id,
    nbl_first_bill_date, nbl_last_bill_date,
    nbl_recency_days, nbl_tenure_days,
    nbl_total_bills, nbl_total_visits, nbl_total_spend,
    nbl_spend_per_bill, nbl_spend_per_visit, nbl_avg_items_per_bill,
    nbl_total_discount, nbl_distinct_months,
    nbl_distinct_store_count, nbl_distinct_article_count,
    nbl_dgbt_fs, nbl_avg_billing_time_secs,
    nbl_return_bill_count, nbl_promo_bill_count,
    nbl_weekend_bill_count, nbl_wednesday_bill_count,
    nbl_fav_store_code, nbl_fav_store_name, nbl_fav_store_type, nbl_fav_day,
    nbl_fav_article_by_spend, nbl_fav_article_by_spend_desc,
    nbl_fav_article_by_nob, nbl_fav_article_by_nob_desc,
    nbl_second_fav_article_by_spend, nbl_second_fav_article_by_nob,
    nbl_channel_presence, nbl_spend_decile, nbl_nob_decile,
    nbl_l1_segment, nbl_l2_segment,
    nbl_store_spend, nbl_online_spend, nbl_store_bills, nbl_online_bills,
    nbl_lifecycle_stage,
    nbl_rfm_recency_score, nbl_rfm_frequency_score, nbl_rfm_monetary_score,
    nbl_computed_at
)
SELECT DISTINCT ON (c.r1_id)
    c.r1_id,
    c.brand_id                      AS mobile,
    ba.display_name,
    ba.email,
    ba.city,
    ba.pincode,
    ba.region,
    ba.registered_store,
    ba.age,
    ba.customer_group,
    ba.occupation,
    ba.whatsapp,
    ba.dnd,
    ba.gw_customer_flag,
    ba.accepts_email_marketing,
    ba.accepts_sms_marketing,
    ba.surrogate_id,
    ba.first_bill_date,
    ba.last_bill_date,
    ba.recency_days,
    ba.tenure_days,
    ba.total_bills,
    ba.total_visits,
    ba.total_spend,
    ba.spend_per_bill,
    ba.spend_per_visit,
    ba.avg_items_per_bill,
    ba.total_discount,
    ba.distinct_months,
    ba.distinct_store_count,
    ba.distinct_article_count,
    ba.dgbt_fs,
    ba.avg_billing_time_secs,
    ba.return_bill_count,
    ba.promo_bill_count,
    ba.weekend_bill_count,
    ba.wednesday_bill_count,
    ba.fav_store_code,
    ba.fav_store_name,
    ba.fav_store_type,
    ba.fav_day,
    ba.fav_article_by_spend,
    ba.fav_article_by_spend_desc,
    ba.fav_article_by_nob,
    ba.fav_article_by_nob_desc,
    ba.second_fav_article_by_spend,
    ba.second_fav_article_by_nob,
    ba.channel_presence,
    ba.spend_decile,
    ba.nob_decile,
    ba.l1_segment,
    ba.l2_segment,
    ba.store_spend,
    ba.online_spend,
    ba.store_bills,
    ba.online_bills,
    ba.lifecycle_stage,
    ba.rfm_recency_score,
    ba.rfm_frequency_score,
    ba.rfm_monetary_score,
    ba.computed_at
FROM corporate_cih.raw_corp_cih c
JOIN nb_silver_reverse_etl.customer_behavioral_attributes ba
    ON c.brand_id = ba.mobile
WHERE c.customer_type LIKE '%NBL%'
ORDER BY c.r1_id, ba.computed_at DESC NULLS LAST
"""

# ── Step 3: Combined cross-brand master table ──────────────────────────────────
SQL_INSERT_CORP = """
INSERT INTO corporate_cih.silver_corp_customer_attributes (
    -- Universal identity
    r1_id, mobile, rpsg_brand_presence,
    -- RPSG lifetime
    rpsg_ftd, rpsg_ltd, rpsg_tenure_lifetime, rpsg_recency_lifetime,
    -- Spencers
    spn_display_name, spn_email, spn_city, spn_pincode, spn_region,
    spn_registered_store, spn_age, spn_customer_group, spn_occupation,
    spn_whatsapp, spn_dnd, spn_gw_customer_flag,
    spn_accepts_email_marketing, spn_accepts_sms_marketing, spn_surrogate_id,
    spn_first_bill_date, spn_last_bill_date,
    spn_recency_days, spn_tenure_days,
    spn_total_bills, spn_total_visits, spn_total_spend,
    spn_spend_per_bill, spn_spend_per_visit, spn_avg_items_per_bill,
    spn_total_discount, spn_distinct_months,
    spn_distinct_store_count, spn_distinct_article_count,
    spn_dgbt_fs, spn_avg_billing_time_secs,
    spn_return_bill_count, spn_promo_bill_count,
    spn_weekend_bill_count, spn_wednesday_bill_count,
    spn_fav_store_code, spn_fav_store_name, spn_fav_store_type, spn_fav_day,
    spn_fav_article_by_spend, spn_fav_article_by_spend_desc,
    spn_fav_article_by_nob, spn_fav_article_by_nob_desc,
    spn_second_fav_article_by_spend, spn_second_fav_article_by_nob,
    spn_channel_presence, spn_spend_decile, spn_nob_decile,
    spn_l1_segment, spn_l2_segment,
    spn_store_spend, spn_online_spend, spn_store_bills, spn_online_bills,
    spn_lifecycle_stage,
    spn_rfm_recency_score, spn_rfm_frequency_score, spn_rfm_monetary_score,
    spn_computed_at,
    -- NBL
    nbl_display_name, nbl_email, nbl_city, nbl_pincode, nbl_region,
    nbl_registered_store, nbl_age, nbl_customer_group, nbl_occupation,
    nbl_whatsapp, nbl_dnd, nbl_gw_customer_flag,
    nbl_accepts_email_marketing, nbl_accepts_sms_marketing, nbl_surrogate_id,
    nbl_first_bill_date, nbl_last_bill_date,
    nbl_recency_days, nbl_tenure_days,
    nbl_total_bills, nbl_total_visits, nbl_total_spend,
    nbl_spend_per_bill, nbl_spend_per_visit, nbl_avg_items_per_bill,
    nbl_total_discount, nbl_distinct_months,
    nbl_distinct_store_count, nbl_distinct_article_count,
    nbl_dgbt_fs, nbl_avg_billing_time_secs,
    nbl_return_bill_count, nbl_promo_bill_count,
    nbl_weekend_bill_count, nbl_wednesday_bill_count,
    nbl_fav_store_code, nbl_fav_store_name, nbl_fav_store_type, nbl_fav_day,
    nbl_fav_article_by_spend, nbl_fav_article_by_spend_desc,
    nbl_fav_article_by_nob, nbl_fav_article_by_nob_desc,
    nbl_second_fav_article_by_spend, nbl_second_fav_article_by_nob,
    nbl_channel_presence, nbl_spend_decile, nbl_nob_decile,
    nbl_l1_segment, nbl_l2_segment,
    nbl_store_spend, nbl_online_spend, nbl_store_bills, nbl_online_bills,
    nbl_lifecycle_stage,
    nbl_rfm_recency_score, nbl_rfm_frequency_score, nbl_rfm_monetary_score,
    nbl_computed_at
)
SELECT
    COALESCE(spn.r1_id, nbl.r1_id)     AS r1_id,
    COALESCE(spn.mobile, nbl.mobile)    AS mobile,
    cih.rpsg_brand_presence,
    -- RPSG lifetime from billanalytics
    rpsg.rpsg_ftd,
    rpsg.rpsg_ltd,
    rpsg.rpsg_tenure_lifetime,
    rpsg.rpsg_recency_lifetime,
    -- Spencers columns
    spn.spn_display_name,
    spn.spn_email,
    spn.spn_city,
    spn.spn_pincode,
    spn.spn_region,
    spn.spn_registered_store,
    spn.spn_age,
    spn.spn_customer_group,
    spn.spn_occupation,
    spn.spn_whatsapp,
    spn.spn_dnd,
    spn.spn_gw_customer_flag,
    spn.spn_accepts_email_marketing,
    spn.spn_accepts_sms_marketing,
    spn.spn_surrogate_id,
    spn.spn_first_bill_date,
    spn.spn_last_bill_date,
    spn.spn_recency_days,
    spn.spn_tenure_days,
    spn.spn_total_bills,
    spn.spn_total_visits,
    spn.spn_total_spend,
    spn.spn_spend_per_bill,
    spn.spn_spend_per_visit,
    spn.spn_avg_items_per_bill,
    spn.spn_total_discount,
    spn.spn_distinct_months,
    spn.spn_distinct_store_count,
    spn.spn_distinct_article_count,
    spn.spn_dgbt_fs,
    spn.spn_avg_billing_time_secs,
    spn.spn_return_bill_count,
    spn.spn_promo_bill_count,
    spn.spn_weekend_bill_count,
    spn.spn_wednesday_bill_count,
    spn.spn_fav_store_code,
    spn.spn_fav_store_name,
    spn.spn_fav_store_type,
    spn.spn_fav_day,
    spn.spn_fav_article_by_spend,
    spn.spn_fav_article_by_spend_desc,
    spn.spn_fav_article_by_nob,
    spn.spn_fav_article_by_nob_desc,
    spn.spn_second_fav_article_by_spend,
    spn.spn_second_fav_article_by_nob,
    spn.spn_channel_presence,
    spn.spn_spend_decile,
    spn.spn_nob_decile,
    spn.spn_l1_segment,
    spn.spn_l2_segment,
    spn.spn_store_spend,
    spn.spn_online_spend,
    spn.spn_store_bills,
    spn.spn_online_bills,
    spn.spn_lifecycle_stage,
    spn.spn_rfm_recency_score,
    spn.spn_rfm_frequency_score,
    spn.spn_rfm_monetary_score,
    spn.spn_computed_at,
    -- NBL columns
    nbl.nbl_display_name,
    nbl.nbl_email,
    nbl.nbl_city,
    nbl.nbl_pincode,
    nbl.nbl_region,
    nbl.nbl_registered_store,
    nbl.nbl_age,
    nbl.nbl_customer_group,
    nbl.nbl_occupation,
    nbl.nbl_whatsapp,
    nbl.nbl_dnd,
    nbl.nbl_gw_customer_flag,
    nbl.nbl_accepts_email_marketing,
    nbl.nbl_accepts_sms_marketing,
    nbl.nbl_surrogate_id,
    nbl.nbl_first_bill_date,
    nbl.nbl_last_bill_date,
    nbl.nbl_recency_days,
    nbl.nbl_tenure_days,
    nbl.nbl_total_bills,
    nbl.nbl_total_visits,
    nbl.nbl_total_spend,
    nbl.nbl_spend_per_bill,
    nbl.nbl_spend_per_visit,
    nbl.nbl_avg_items_per_bill,
    nbl.nbl_total_discount,
    nbl.nbl_distinct_months,
    nbl.nbl_distinct_store_count,
    nbl.nbl_distinct_article_count,
    nbl.nbl_dgbt_fs,
    nbl.nbl_avg_billing_time_secs,
    nbl.nbl_return_bill_count,
    nbl.nbl_promo_bill_count,
    nbl.nbl_weekend_bill_count,
    nbl.nbl_wednesday_bill_count,
    nbl.nbl_fav_store_code,
    nbl.nbl_fav_store_name,
    nbl.nbl_fav_store_type,
    nbl.nbl_fav_day,
    nbl.nbl_fav_article_by_spend,
    nbl.nbl_fav_article_by_spend_desc,
    nbl.nbl_fav_article_by_nob,
    nbl.nbl_fav_article_by_nob_desc,
    nbl.nbl_second_fav_article_by_spend,
    nbl.nbl_second_fav_article_by_nob,
    nbl.nbl_channel_presence,
    nbl.nbl_spend_decile,
    nbl.nbl_nob_decile,
    nbl.nbl_l1_segment,
    nbl.nbl_l2_segment,
    nbl.nbl_store_spend,
    nbl.nbl_online_spend,
    nbl.nbl_store_bills,
    nbl.nbl_online_bills,
    nbl.nbl_lifecycle_stage,
    nbl.nbl_rfm_recency_score,
    nbl.nbl_rfm_frequency_score,
    nbl.nbl_rfm_monetary_score,
    nbl.nbl_computed_at
FROM corporate_cih.silver_spn_attributes spn
FULL OUTER JOIN corporate_cih.silver_nbl_attributes nbl
    ON spn.r1_id = nbl.r1_id
LEFT JOIN corporate_cih.raw_corp_cih cih
    ON COALESCE(spn.r1_id, nbl.r1_id) = cih.r1_id
LEFT JOIN corporate_cih.raw_corp_billanalytics rpsg
    ON COALESCE(spn.r1_id, nbl.r1_id) = rpsg.r1_id
"""

# ── Post-load indexes ──────────────────────────────────────────────────────────
INDEXES = [
    ("silver_spn_attributes",           "CREATE UNIQUE INDEX idx_silver_spn_r1     ON corporate_cih.silver_spn_attributes (r1_id)"),
    ("silver_spn_attributes",           "CREATE        INDEX idx_silver_spn_mobile ON corporate_cih.silver_spn_attributes (mobile)"),
    ("silver_spn_attributes",           "CREATE        INDEX idx_silver_spn_lc     ON corporate_cih.silver_spn_attributes (spn_lifecycle_stage)"),
    ("silver_nbl_attributes",           "CREATE UNIQUE INDEX idx_silver_nbl_r1     ON corporate_cih.silver_nbl_attributes (r1_id)"),
    ("silver_nbl_attributes",           "CREATE        INDEX idx_silver_nbl_mobile ON corporate_cih.silver_nbl_attributes (mobile)"),
    ("silver_nbl_attributes",           "CREATE        INDEX idx_silver_nbl_lc     ON corporate_cih.silver_nbl_attributes (nbl_lifecycle_stage)"),
    ("silver_corp_customer_attributes", "CREATE UNIQUE INDEX idx_corp_r1           ON corporate_cih.silver_corp_customer_attributes (r1_id)"),
    ("silver_corp_customer_attributes", "CREATE        INDEX idx_corp_mobile       ON corporate_cih.silver_corp_customer_attributes (mobile)"),
    ("silver_corp_customer_attributes", "CREATE        INDEX idx_corp_spn_lc       ON corporate_cih.silver_corp_customer_attributes (spn_lifecycle_stage)"),
    ("silver_corp_customer_attributes", "CREATE        INDEX idx_corp_nbl_lc       ON corporate_cih.silver_corp_customer_attributes (nbl_lifecycle_stage)"),
    ("silver_corp_customer_attributes", "CREATE        INDEX idx_corp_spn_recency  ON corporate_cih.silver_corp_customer_attributes (spn_recency_days)"),
    ("silver_corp_customer_attributes", "CREATE        INDEX idx_corp_nbl_recency  ON corporate_cih.silver_corp_customer_attributes (nbl_recency_days)"),
]


# ── Helpers ───────────────────────────────────────────────────────────────────

def _run(conn, label: str, sql: str) -> int:
    """Execute sql, commit, return rowcount."""
    log.info(f"  {label} ...")
    t0 = time.time()
    with conn.cursor() as cur:
        cur.execute(sql)
        rows = cur.rowcount
    conn.commit()
    elapsed = time.time() - t0
    log.info(f"  {label} done — {rows:,} rows in {elapsed:.1f}s")
    return rows


def _count(conn, table: str) -> int:
    with conn.cursor() as cur:
        cur.execute(f"SELECT COUNT(*) FROM {table}")
        return cur.fetchone()[0]


# ── Main ─────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Populate corporate silver layer.")
    grp = parser.add_mutually_exclusive_group()
    grp.add_argument("--step1-only", action="store_true", help="Only run Step 1 (silver_spn_attributes)")
    grp.add_argument("--step2-only", action="store_true", help="Only run Step 2 (silver_nbl_attributes)")
    grp.add_argument("--step3-only", action="store_true", help="Only run Step 3 (silver_corp_customer_attributes)")
    parser.add_argument("--no-index", action="store_true", help="Skip post-load index creation")
    args = parser.parse_args()

    run_all = not (args.step1_only or args.step2_only or args.step3_only)

    log.info("Connecting to PostgreSQL ...")
    conn = psycopg2.connect(PG_CONN)
    conn.autocommit = False

    # ── Step 1: silver_spn_attributes ─────────────────────────────────────────
    if run_all or args.step1_only:
        log.info("=" * 60)
        log.info("STEP 1 — silver_spn_attributes")
        log.info("=" * 60)
        _run(conn, "TRUNCATE silver_spn_attributes", SQL_TRUNCATE_SPN)
        _run(conn, "INSERT Spencer BA → silver_spn_attributes", SQL_INSERT_SPN)
        count = _count(conn, "corporate_cih.silver_spn_attributes")
        log.info(f"  ✓ silver_spn_attributes: {count:,} rows")

    # ── Step 2: silver_nbl_attributes ─────────────────────────────────────────
    if run_all or args.step2_only:
        log.info("=" * 60)
        log.info("STEP 2 — silver_nbl_attributes")
        log.info("=" * 60)
        _run(conn, "TRUNCATE silver_nbl_attributes", SQL_TRUNCATE_NBL)
        _run(conn, "INSERT NBL BA → silver_nbl_attributes", SQL_INSERT_NBL)
        count = _count(conn, "corporate_cih.silver_nbl_attributes")
        log.info(f"  ✓ silver_nbl_attributes: {count:,} rows")

    # ── Step 3: silver_corp_customer_attributes ────────────────────────────────
    if run_all or args.step3_only:
        log.info("=" * 60)
        log.info("STEP 3 — silver_corp_customer_attributes (cross-brand master)")
        log.info("=" * 60)
        _run(conn, "TRUNCATE silver_corp_customer_attributes", SQL_TRUNCATE_CORP)
        _run(conn, "FULL OUTER JOIN spn + nbl + rpsg lifetime → master", SQL_INSERT_CORP)
        count = _count(conn, "corporate_cih.silver_corp_customer_attributes")
        log.info(f"  ✓ silver_corp_customer_attributes: {count:,} rows")

        # Quick cross-brand sanity check
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                  COUNT(*) FILTER (WHERE spn_lifecycle_stage IS NOT NULL AND nbl_lifecycle_stage IS NOT NULL) AS cross_brand,
                  COUNT(*) FILTER (WHERE spn_lifecycle_stage IS NOT NULL AND nbl_lifecycle_stage IS     NULL) AS spn_only,
                  COUNT(*) FILTER (WHERE spn_lifecycle_stage IS     NULL AND nbl_lifecycle_stage IS NOT NULL) AS nbl_only
                FROM corporate_cih.silver_corp_customer_attributes
            """)
            cb, so, no = cur.fetchone()
        log.info(f"  Cross-brand (SPN+NBL): {cb:,}  |  SPN-only: {so:,}  |  NBL-only: {no:,}")

    # ── Indexes ────────────────────────────────────────────────────────────────
    if not args.no_index and (run_all or args.step3_only):
        log.info("=" * 60)
        log.info("Building indexes ...")
        log.info("=" * 60)
        with conn.cursor() as cur:
            for table, stmt in INDEXES:
                log.info(f"  {stmt[:72]} ...")
                cur.execute(stmt)
        conn.commit()
        log.info("  All indexes built.")

    conn.close()
    log.info("Phase 2 complete.")


if __name__ == "__main__":
    main()

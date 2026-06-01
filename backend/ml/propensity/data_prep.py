"""
Data preparation — extracts label + feature data from PostgreSQL for one brand.

All queries are parameterised; the date window and segment list are injected
at call time so the logic stays reusable across brands.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

import pandas as pd

from .config import BrandConfig, TRAIN_END, TRAIN_START

logger = logging.getLogger(__name__)


# ── Label extraction ──────────────────────────────────────────────────────────

def _label_sql(cfg: BrandConfig) -> str:
    """
    Build a query that returns one row per customer with a binary flag for
    each segment.  A customer is positive for segment X if they made at least
    one purchase where UPPER(bt.segment_desc) matches X within the date window.

    We use TRIM + UPPER for resilience against leading/trailing whitespace and
    mixed case in the source data.
    """
    segment_cols = ",\n    ".join(
        f"MAX(CASE WHEN UPPER(TRIM(bt.segment_desc)) = '{seg.upper()}' THEN 1 ELSE 0 END) "
        f"AS label_{i}"
        for i, seg in enumerate(cfg.segments, start=1)
    )
    return f"""
SELECT
    p.unified_id                    AS customer_id,
    {segment_cols}
FROM {cfg.tbl_profiles} p
JOIN {cfg.tbl_bt} bt
  ON RIGHT(bt.mobile_number, 10) = RIGHT(p.canonical_mobile, 10)
WHERE bt.bill_date BETWEEN %(start_date)s AND %(end_date)s
  AND bt.sales_return IS DISTINCT FROM TRUE
  AND p.canonical_mobile IS NOT NULL
GROUP BY p.unified_id
"""


# ── Feature extraction ────────────────────────────────────────────────────────

def _feature_sql(cfg: BrandConfig) -> str:
    """
    Returns a customer-level feature table joining:
      - unified_profiles (demographics, identity graph flags)
      - customer_behavioral_attributes (RFM + aggregate metrics)
      - s_fact_bill_transactions aggregated by segment (per-category spend/visits)

    All aggregations are scoped to the same date window as the labels to prevent
    leakage from future data.
    """
    # Per-segment spend, visits, average basket size, most-recent purchase gap
    seg_aggs = []
    for i, seg in enumerate(cfg.segments, start=1):
        tag = f"seg{i}"
        seg_aggs.append(f"""
    SUM(CASE WHEN UPPER(TRIM(bt.segment_desc)) = '{seg.upper()}' THEN bt.gross_sale_value ELSE 0 END)  AS {tag}_spend,
    COUNT(CASE WHEN UPPER(TRIM(bt.segment_desc)) = '{seg.upper()}' THEN bt.bill_no END)                AS {tag}_visits,
    AVG(CASE WHEN UPPER(TRIM(bt.segment_desc)) = '{seg.upper()}' THEN bt.billed_qty END)               AS {tag}_avg_qty,
    MAX(CASE WHEN UPPER(TRIM(bt.segment_desc)) = '{seg.upper()}' THEN bt.bill_date END)                AS {tag}_last_date""")

    seg_agg_sql = ",".join(seg_aggs)

    return f"""
WITH bt_agg AS (
    SELECT
        RIGHT(bt.mobile_number, 10)                         AS mobile_10,
        -- Overall RFM ─────────────────────────────────────────────────────
        COUNT(DISTINCT bt.bill_no)                          AS total_visits,
        SUM(bt.gross_sale_value)                            AS total_spend,
        AVG(bt.gross_sale_value)                            AS avg_basket_value,
        COUNT(DISTINCT bt.bill_date)                        AS active_days,
        MIN(bt.bill_date)                                   AS first_purchase_date,
        MAX(bt.bill_date)                                   AS last_purchase_date,
        COUNT(DISTINCT bt.store_code)                       AS distinct_stores,
        COUNT(DISTINCT UPPER(TRIM(bt.segment_desc)))        AS distinct_segments_purchased,
        SUM(bt.total_discount)                              AS total_discount,
        AVG(bt.total_discount)                              AS avg_discount,
        SUM(bt.billed_qty)                                  AS total_qty,
        -- Promo & channel engagement ──────────────────────────────────────
        SUM(CASE WHEN bt.promo_indicator = TRUE THEN 1 ELSE 0 END) AS promo_visits,
        SUM(CASE WHEN bt.weekend_flag    = TRUE THEN 1 ELSE 0 END) AS weekend_visits,
        SUM(CASE WHEN bt.delivery_channel ILIKE '%ecom%'  THEN 1 ELSE 0 END) AS ecom_visits,
        -- Per-segment aggregations ────────────────────────────────────────
        {seg_agg_sql}
    FROM {cfg.tbl_bt} bt
    WHERE bt.bill_date BETWEEN %(start_date)s AND %(end_date)s
      AND bt.sales_return IS DISTINCT FROM TRUE
      AND bt.mobile_number IS NOT NULL
    GROUP BY RIGHT(bt.mobile_number, 10)
)
SELECT
    -- ── Identity ──────────────────────────────────────────────────────────
    p.unified_id                                            AS customer_id,
    -- ── Demographics ──────────────────────────────────────────────────────
    p.city_tier,
    p.gender,
    p.age,
    p.has_email,
    p.has_transactions,
    p.primary_source,
    -- ── Behavioral attributes (pre-computed RFM from gold layer) ──────────
    ba.total_spend                                          AS ba_total_spend,
    ba.total_visits                                         AS ba_total_visits,
    ba.avg_basket_value                                     AS ba_avg_basket,
    ba.recency_days,
    ba.frequency_score,
    ba.monetary_score,
    ba.rfm_segment                                          AS ba_rfm_segment,
    ba.customer_lifetime_value                              AS ba_clv,
    ba.churn_risk_score,
    ba.visit_frequency_last_90d,
    ba.days_since_last_purchase,
    -- ── Transaction aggregates from date window ────────────────────────────
    a.total_visits,
    a.total_spend,
    a.avg_basket_value,
    a.active_days,
    a.distinct_stores,
    a.distinct_segments_purchased,
    a.total_discount,
    a.avg_discount,
    a.total_qty,
    a.promo_visits,
    a.weekend_visits,
    a.ecom_visits,
    -- date-range recency: days since last purchase within window
    %(end_date)s - a.last_purchase_date                    AS recency_in_window,
    %(end_date)s - a.first_purchase_date                   AS tenure_in_window,
    -- derived ratios
    CASE WHEN a.total_visits > 0
         THEN a.promo_visits::FLOAT / a.total_visits  ELSE NULL END  AS promo_rate,
    CASE WHEN a.total_visits > 0
         THEN a.weekend_visits::FLOAT / a.total_visits ELSE NULL END AS weekend_rate,
    CASE WHEN a.total_visits > 0
         THEN a.ecom_visits::FLOAT / a.total_visits   ELSE NULL END  AS ecom_rate,
    CASE WHEN a.total_spend  > 0
         THEN a.total_discount / a.total_spend        ELSE NULL END  AS discount_rate,
    -- ── Per-segment features ───────────────────────────────────────────────
    {", ".join(
        f"a.seg{i}_spend, a.seg{i}_visits, a.seg{i}_avg_qty, a.seg{i}_last_date"
        for i in range(1, len(cfg.segments) + 1)
    )}
FROM {cfg.tbl_profiles} p
LEFT JOIN {cfg.tbl_behaviors} ba
       ON ba.unified_id = p.unified_id
JOIN bt_agg a
  ON a.mobile_10 = RIGHT(p.canonical_mobile, 10)
WHERE p.canonical_mobile IS NOT NULL
"""


# ── Public interface ──────────────────────────────────────────────────────────

def fetch_data(
    conn,
    cfg: BrandConfig,
    start_date=TRAIN_START,
    end_date=TRAIN_END,
) -> pd.DataFrame:
    """
    Fetch merged label + feature data for one brand.

    Returns a DataFrame keyed by customer_id with:
      - label_1 … label_N  (binary, one per segment)
      - all feature columns

    conn: psycopg3 connection (sync) or psycopg2 connection
    """
    params = {"start_date": start_date, "end_date": end_date}

    logger.info("[%s] Fetching labels ...", cfg.brand_code)
    df_labels = pd.read_sql(_label_sql(cfg), conn, params=params)

    logger.info("[%s] Fetched %d label rows", cfg.brand_code, len(df_labels))

    logger.info("[%s] Fetching features ...", cfg.brand_code)
    df_features = pd.read_sql(_feature_sql(cfg), conn, params=params)

    logger.info("[%s] Fetched %d feature rows", cfg.brand_code, len(df_features))

    df = df_labels.merge(df_features, on="customer_id", how="inner")
    logger.info(
        "[%s] Merged dataset: %d rows × %d cols",
        cfg.brand_code, len(df), len(df.columns),
    )
    return df


def fetch_scoring_data(
    conn,
    cfg: BrandConfig,
    start_date=TRAIN_START,
    end_date=TRAIN_END,
) -> pd.DataFrame:
    """
    Fetch feature data for scoring (no labels required — all customers active
    in the date window are returned, including those who only bought from some
    segments).
    """
    params = {"start_date": start_date, "end_date": end_date}
    logger.info("[%s] Fetching scoring features ...", cfg.brand_code)
    df = pd.read_sql(_feature_sql(cfg), conn, params=params)
    logger.info("[%s] Scoring set: %d customers", cfg.brand_code, len(df))
    return df

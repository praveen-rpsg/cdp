"""
Propensity model pipeline — end-to-end orchestration.

Usage (run from repo root):

    python -m ml.propensity.pipeline \
        --brand spencers \
        --algo lgbm \
        --db-dsn "host=localhost dbname=cdp user=cdp password=secret" \
        --output-table silver_reverse_etl.customer_propensity_scores \
        --output-csv ./output/spencers_propensity.csv

Or call run_pipeline() programmatically from a notebook or scheduler.
"""

from __future__ import annotations

import argparse
import logging
import os
import pickle
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

import numpy as np
import pandas as pd

from .config import BRAND_CONFIGS, TRAIN_END, TRAIN_START, BrandConfig
from .data_prep import fetch_data, fetch_scoring_data
from .evaluator import summary_report
from .features import lgbm_preprocess, prepare_matrices, prepare_scoring_matrix
from .normalizer import build_output_table
from .trainer import SegmentModel, train_all_segments

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


# ── Core pipeline ─────────────────────────────────────────────────────────────

def run_pipeline(
    brand_code: str,
    conn,
    algo: str = "lgbm",
    output_table: Optional[str] = None,
    output_csv: Optional[str] = None,
    model_dir: Optional[str] = None,
    start_date=TRAIN_START,
    end_date=TRAIN_END,
) -> pd.DataFrame:
    """
    Full propensity pipeline for one brand.

    Steps:
      1. Fetch labelled + feature data
      2. Feature engineering
      3. Train N binary models (one per segment) with CV
      4. Generate OOF evaluation report
      5. Score all customers in the date window
      6. Normalize probabilities
      7. Optionally write to PostgreSQL table / CSV

    Returns the final customer-level output DataFrame.
    """
    cfg = BRAND_CONFIGS[brand_code]
    logger.info("=" * 60)
    logger.info("BRAND: %s | ALGO: %s | DATE: %s → %s",
                brand_code.upper(), algo.upper(), start_date, end_date)
    logger.info("=" * 60)

    # ── 1. Fetch training data ─────────────────────────────────────────────
    df_train = fetch_data(conn, cfg, start_date=start_date, end_date=end_date)
    if df_train.empty:
        raise RuntimeError(f"[{brand_code}] No training data returned — check date range and DB connection")

    # ── 2. Feature engineering ─────────────────────────────────────────────
    X_train, y_list = prepare_matrices(df_train, cfg)

    # ── 3. Train models ────────────────────────────────────────────────────
    models: List[SegmentModel] = train_all_segments(X_train, y_list, cfg, algo=algo)

    # ── 4. Evaluation report ───────────────────────────────────────────────
    eval_df = summary_report(models)
    logger.info("\n%s", eval_df.to_string(index=False))

    # ── 5. Score full customer base ────────────────────────────────────────
    df_score = fetch_scoring_data(conn, cfg, start_date=start_date, end_date=end_date)
    X_score, customer_ids = prepare_scoring_matrix(df_score, cfg)

    if algo == "lgbm":
        X_score = lgbm_preprocess(X_score)

    proba_matrix = np.column_stack([
        m.predict_proba(X_score) for m in models
    ])

    # ── 6. Normalize propensities ──────────────────────────────────────────
    output_df = build_output_table(customer_ids, proba_matrix, cfg)
    logger.info("[%s] Output table: %d rows × %d cols",
                brand_code, len(output_df), len(output_df.columns))

    _log_score_stats(output_df, cfg)

    # ── 7a. Write to PostgreSQL ────────────────────────────────────────────
    if output_table:
        _write_to_pg(output_df, output_table, conn, brand_code)

    # ── 7b. Write to CSV ───────────────────────────────────────────────────
    if output_csv:
        Path(output_csv).parent.mkdir(parents=True, exist_ok=True)
        output_df.to_csv(output_csv, index=False)
        logger.info("[%s] Scores written to %s", brand_code, output_csv)

    # ── 7c. Persist models ─────────────────────────────────────────────────
    if model_dir:
        _save_models(models, model_dir, brand_code, algo)

    return output_df


def _log_score_stats(output_df: pd.DataFrame, cfg: BrandConfig):
    """Log mean raw and normalized propensity per segment."""
    rows = []
    for seg in cfg.segments:
        raw_col  = cfg.output_cols[seg]["raw"]
        norm_col = cfg.output_cols[seg]["normalized"]
        rows.append({
            "segment":          seg,
            "mean_raw":         round(output_df[raw_col].mean(), 4),
            "mean_normalized":  round(output_df[norm_col].mean(), 4),
            "pct_above_0.5":    round((output_df[raw_col] >= 0.5).mean() * 100, 1),
        })
    logger.info("\nScore summary:\n%s", pd.DataFrame(rows).to_string(index=False))


def _write_to_pg(output_df: pd.DataFrame, table: str, conn, brand_code: str):
    """
    Upsert propensity scores into a PostgreSQL table.

    Expected table DDL (create once):
        CREATE TABLE IF NOT EXISTS <table> (
            customer_id TEXT PRIMARY KEY,
            <col1> NUMERIC, <col2> NUMERIC, ...
            updated_at TIMESTAMPTZ DEFAULT NOW()
        );
    """
    import io
    logger.info("[%s] Writing %d rows to %s ...", brand_code, len(output_df), table)
    cols = output_df.columns.tolist()
    buf = io.StringIO()
    output_df.to_csv(buf, index=False, header=False)
    buf.seek(0)

    with conn.cursor() as cur:
        # Truncate-insert strategy (replace scores for this brand each run)
        cur.execute(f"DELETE FROM {table} WHERE customer_id = ANY(%s)",
                    (output_df["customer_id"].tolist(),))
        cur.copy_expert(
            f"COPY {table} ({','.join(cols)}) FROM STDIN CSV",
            buf,
        )
    conn.commit()
    logger.info("[%s] Write complete.", brand_code)


def _save_models(models: List[SegmentModel], model_dir: str, brand_code: str, algo: str):
    out_dir = Path(model_dir) / brand_code / algo
    out_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    for m in models:
        fname = out_dir / f"seg{m.segment_index}_{m.segment_label.replace(' ', '_')}_{ts}.pkl"
        with open(fname, "wb") as f:
            pickle.dump(m, f)
        logger.info("Saved: %s", fname)


# ── Multi-brand runner ─────────────────────────────────────────────────────────

def run_all_brands(
    conn_spencers,
    conn_nbl,
    algo: str = "lgbm",
    output_dir: str = "./output",
    **kwargs,
) -> Dict[str, pd.DataFrame]:
    """
    Train and score both brands.  Pass separate connections if the brands
    live in different PostgreSQL instances; pass the same connection if they
    share a multi-schema database.
    """
    results = {}
    for brand_code, conn in [("spencers", conn_spencers), ("natures_basket", conn_nbl)]:
        csv_path = f"{output_dir}/{brand_code}_propensity.csv"
        results[brand_code] = run_pipeline(
            brand_code=brand_code,
            conn=conn,
            algo=algo,
            output_csv=csv_path,
            **kwargs,
        )
    return results


# ── CLI entry point ───────────────────────────────────────────────────────────

def _parse_args():
    p = argparse.ArgumentParser(description="Propensity model pipeline")
    p.add_argument("--brand", choices=list(BRAND_CONFIGS), required=True)
    p.add_argument("--algo", choices=["lgbm", "lr"], default="lgbm")
    p.add_argument("--db-dsn", required=True, help="psycopg3 DSN string")
    p.add_argument("--output-table", default=None)
    p.add_argument("--output-csv", default=None)
    p.add_argument("--model-dir", default="./models")
    return p.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    try:
        import psycopg
        conn = psycopg.connect(args.db_dsn)
    except ImportError:
        import psycopg2 as psycopg
        conn = psycopg.connect(args.db_dsn)

    try:
        run_pipeline(
            brand_code   = args.brand,
            conn         = conn,
            algo         = args.algo,
            output_table = args.output_table,
            output_csv   = args.output_csv,
            model_dir    = args.model_dir,
        )
    finally:
        conn.close()

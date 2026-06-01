"""
Probability normalization — applies per-customer normalization so that the
N propensity scores for each customer sum to 1.

Formula:
    Normalized_i = Raw_i / Sum(Raw_1 ... Raw_N)

Edge case: if all Raw scores are 0 for a customer (no transactions at all),
the normalized scores are set to 1/N (uniform distribution) to avoid division
by zero and ensure the output is always a valid probability vector.
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from .config import BrandConfig


def normalize_propensities(
    raw_df: pd.DataFrame,
    cfg: BrandConfig,
) -> pd.DataFrame:
    """
    Given a DataFrame with raw propensity columns (named by cfg.output_cols[seg]['raw']),
    adds normalized propensity columns.

    Returns the same DataFrame augmented with normalized columns in place.
    """
    raw_cols = [cfg.output_cols[seg]["raw"] for seg in cfg.segments]
    norm_cols = [cfg.output_cols[seg]["normalized"] for seg in cfg.segments]

    raw_matrix = raw_df[raw_cols].values.astype(float)

    row_sums = raw_matrix.sum(axis=1, keepdims=True)
    # For customers where all scores are 0, assign uniform distribution
    uniform = np.ones_like(raw_matrix) / cfg.n_segments
    normalized = np.where(row_sums > 0, raw_matrix / row_sums, uniform)

    for j, col in enumerate(norm_cols):
        raw_df[col] = normalized[:, j]

    return raw_df


def build_output_table(
    customer_ids: pd.Series,
    proba_matrix: np.ndarray,
    cfg: BrandConfig,
) -> pd.DataFrame:
    """
    Construct the final customer-level output table with raw + normalized
    propensity columns.

    Args:
        customer_ids  — pd.Series of unified_id values (length N)
        proba_matrix  — np.ndarray shape (N, n_segments), raw probabilities
        cfg           — BrandConfig for this brand

    Returns DataFrame with columns:
        customer_id,
        {BRAND}_SEGMENT_1_PROPENSITY, ..., {BRAND}_SEGMENT_N_PROPENSITY,
        {BRAND}_SEGMENT_1_NORMALIZED_PROPENSITY, ..., {BRAND}_SEGMENT_N_NORMALIZED_PROPENSITY
    """
    out = pd.DataFrame({"customer_id": customer_ids.values})

    for i, seg in enumerate(cfg.segments):
        raw_col = cfg.output_cols[seg]["raw"]
        out[raw_col] = proba_matrix[:, i]

    out = normalize_propensities(out, cfg)
    return out

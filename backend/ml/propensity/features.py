"""
Feature engineering — cleans raw DB output into a model-ready matrix.

Steps applied in order:
  1. Drop identifier columns
  2. Derive additional ratio/flag features
  3. Encode categoricals (ordinal for tree models, OHE for LR)
  4. Impute missing values (median for numeric, mode for categorical)
  5. Return X (features) and y_list (list of binary label Series)
"""

from __future__ import annotations

import logging
from typing import List, Tuple

import numpy as np
import pandas as pd
from sklearn.compose import ColumnTransformer
from sklearn.impute import SimpleImputer
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder, StandardScaler

from .config import BrandConfig

logger = logging.getLogger(__name__)

_ID_COLS   = {"customer_id"}
_DATE_COLS = {
    "first_purchase_date", "last_purchase_date",
    # per-segment last-date columns handled dynamically below
}


def _add_derived_features(df: pd.DataFrame, cfg: BrandConfig) -> pd.DataFrame:
    """Add engineered columns on top of the raw DB output."""
    df = df.copy()

    # Spend share per segment (fraction of total spend in each segment)
    for i in range(1, cfg.n_segments + 1):
        col_spend = f"seg{i}_spend"
        if col_spend in df.columns and "total_spend" in df.columns:
            df[f"seg{i}_spend_share"] = np.where(
                df["total_spend"] > 0,
                df[col_spend] / df["total_spend"].replace(0, np.nan),
                0.0,
            )

    # Visit share per segment
    for i in range(1, cfg.n_segments + 1):
        col_vis = f"seg{i}_visits"
        if col_vis in df.columns and "total_visits" in df.columns:
            df[f"seg{i}_visit_share"] = np.where(
                df["total_visits"] > 0,
                df[col_vis] / df["total_visits"].replace(0, np.nan),
                0.0,
            )

    # Dominant segment index (0 = no purchases, 1..N = segment with most visits)
    seg_visit_cols = [f"seg{i}_visits" for i in range(1, cfg.n_segments + 1)
                      if f"seg{i}_visits" in df.columns]
    if seg_visit_cols:
        df["dominant_segment"] = df[seg_visit_cols].fillna(0).values.argmax(axis=1) + 1

    # Clamp recency to non-negative (can be negative if data has future dates)
    for col in ["recency_in_window", "tenure_in_window", "days_since_last_purchase", "recency_days"]:
        if col in df.columns:
            df[col] = df[col].clip(lower=0)

    # Drop raw date columns — we've already computed recency above
    date_cols = set(_DATE_COLS)
    for i in range(1, cfg.n_segments + 1):
        date_cols.add(f"seg{i}_last_date")
    df.drop(columns=[c for c in date_cols if c in df.columns], inplace=True)

    return df


def _get_label_cols(df: pd.DataFrame) -> List[str]:
    return [c for c in df.columns if c.startswith("label_")]


def prepare_matrices(
    df: pd.DataFrame,
    cfg: BrandConfig,
) -> Tuple[pd.DataFrame, List[pd.Series]]:
    """
    Split df into (X, [y_1, y_2, ...]) where each y_i is a binary label Series.

    Returns:
        X         — DataFrame of features (no IDs, no labels)
        y_list    — list of length cfg.n_segments, each a binary pd.Series
    """
    df = _add_derived_features(df, cfg)

    label_cols = _get_label_cols(df)
    if len(label_cols) != cfg.n_segments:
        raise ValueError(
            f"Expected {cfg.n_segments} label columns, found {len(label_cols)}: {label_cols}"
        )

    y_list: List[pd.Series] = [df[lc].astype(int) for lc in label_cols]

    drop_cols = set(_ID_COLS) | set(label_cols)
    X = df.drop(columns=[c for c in drop_cols if c in df.columns])

    logger.info(
        "[%s] Feature matrix: %d rows × %d cols | Positive rates: %s",
        cfg.brand_code,
        len(X),
        len(X.columns),
        {lc: round(y.mean(), 3) for lc, y in zip(label_cols, y_list)},
    )
    return X, y_list


def prepare_scoring_matrix(df: pd.DataFrame, cfg: BrandConfig) -> Tuple[pd.DataFrame, pd.Series]:
    """
    Prepare feature matrix for scoring (inference).  Returns (X, customer_ids).
    """
    customer_ids = df["customer_id"].copy()
    df = _add_derived_features(df, cfg)

    label_cols = _get_label_cols(df)
    drop_cols = set(_ID_COLS) | set(label_cols)
    X = df.drop(columns=[c for c in drop_cols if c in df.columns])
    return X, customer_ids


def build_preprocessor(X: pd.DataFrame) -> ColumnTransformer:
    """
    Builds a sklearn ColumnTransformer that:
      - imputes + scales numeric columns
      - imputes + one-hot-encodes categorical columns

    This preprocessor is used ONLY for the Logistic Regression benchmark.
    LightGBM handles missing values and categoricals natively.
    """
    num_cols  = X.select_dtypes(include=["number"]).columns.tolist()
    cat_cols  = X.select_dtypes(include=["object", "category"]).columns.tolist()

    num_pipe = Pipeline([
        ("imputer", SimpleImputer(strategy="median")),
        ("scaler",  StandardScaler()),
    ])
    cat_pipe = Pipeline([
        ("imputer", SimpleImputer(strategy="most_frequent")),
        ("encoder", OneHotEncoder(handle_unknown="ignore", sparse_output=False)),
    ])

    transformers = []
    if num_cols:
        transformers.append(("num", num_pipe, num_cols))
    if cat_cols:
        transformers.append(("cat", cat_pipe, cat_cols))

    return ColumnTransformer(transformers, remainder="drop")


def lgbm_preprocess(X: pd.DataFrame) -> pd.DataFrame:
    """
    Minimal preprocessing for LightGBM:
      - Convert object/category dtypes to pandas Categorical (LGBM reads these natively)
      - Leave NaN as-is (LGBM handles internally via surrogate splits)
    """
    X = X.copy()
    for col in X.select_dtypes(include=["object"]).columns:
        X[col] = X[col].astype("category")
    return X

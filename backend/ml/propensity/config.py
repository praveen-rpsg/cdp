"""
Propensity model configuration — segment definitions, date scope, schema constants.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from typing import Dict, List

# ── Date scope ────────────────────────────────────────────────────────────────
TRAIN_START: date = date(2026, 1, 27)
TRAIN_END:   date = date(2026, 2, 6)

# ── Brand-level segment definitions ──────────────────────────────────────────
# Values must match bt.segment_desc exactly (case-insensitive match applied in SQL).
SPENCERS_SEGMENTS: List[str] = [
    "FASHION CB",
    "FOOD",
    "GM",
    "HI TECH",
    "NON TRADE",
    "NON FOOD GROCERY",
]

NBL_SEGMENTS: List[str] = [
    "FOOD",
    "GM",
    "NON TRADE",
    "NON FOOD GROCERY",
]

# ── Output column naming ──────────────────────────────────────────────────────
def _output_cols(brand_prefix: str, segments: List[str]) -> Dict[str, Dict[str, str]]:
    """Map segment label → {raw, normalized} output column names."""
    out: Dict[str, Dict[str, str]] = {}
    for i, seg in enumerate(segments, start=1):
        out[seg] = {
            "raw":        f"{brand_prefix}_SEGMENT_{i}_PROPENSITY",
            "normalized": f"{brand_prefix}_SEGMENT_{i}_NORMALIZED_PROPENSITY",
        }
    return out

SPENCERS_OUTPUT_COLS = _output_cols("SPENCERS", SPENCERS_SEGMENTS)
NBL_OUTPUT_COLS      = _output_cols("NBL",      NBL_SEGMENTS)


@dataclass
class BrandConfig:
    brand_code:    str
    segments:      List[str]
    output_cols:   Dict[str, Dict[str, str]]
    # Fully-qualified table names (must match BRAND_SCHEMA_CONFIG in pg_compiler)
    tbl_profiles:  str
    tbl_behaviors: str
    tbl_bt:        str

    @property
    def n_segments(self) -> int:
        return len(self.segments)


BRAND_CONFIGS: Dict[str, BrandConfig] = {
    "spencers": BrandConfig(
        brand_code   = "spencers",
        segments     = SPENCERS_SEGMENTS,
        output_cols  = SPENCERS_OUTPUT_COLS,
        tbl_profiles = "silver_identity.unified_profiles",
        tbl_behaviors= "silver_reverse_etl.customer_behavioral_attributes",
        tbl_bt       = "silver.s_fact_bill_transactions",
    ),
    "natures_basket": BrandConfig(
        brand_code   = "natures_basket",
        segments     = NBL_SEGMENTS,
        output_cols  = NBL_OUTPUT_COLS,
        tbl_profiles = "nb_silver_identity.unified_profiles",
        tbl_behaviors= "nb_silver_reverse_etl.customer_behavioral_attributes",
        tbl_bt       = "nb_silver.s_fact_bill_transactions",
    ),
}

# ── Modelling hyper-parameters ────────────────────────────────────────────────
LGBM_PARAMS = {
    "objective":       "binary",
    "metric":          "auc",
    "learning_rate":   0.05,
    "num_leaves":      63,
    "min_child_samples": 20,
    "feature_fraction":  0.8,
    "bagging_fraction":  0.8,
    "bagging_freq":      5,
    "reg_alpha":         0.1,
    "reg_lambda":        0.1,
    "n_estimators":    500,
    "early_stopping_rounds": 50,
    "verbose":        -1,
    "n_jobs":         -1,
    "random_state":   42,
}

LR_PARAMS = {
    "max_iter":     1000,
    "solver":       "lbfgs",
    "C":            0.1,
    "class_weight": "balanced",
    "random_state": 42,
    "n_jobs":      -1,
}

CV_FOLDS = 5
RANDOM_STATE = 42

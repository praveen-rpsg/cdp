"""
Model evaluation — computes AUC, PR-AUC, Precision, Recall, F1, Log Loss.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

import numpy as np
import pandas as pd
from sklearn.metrics import (
    average_precision_score,
    f1_score,
    log_loss,
    precision_score,
    recall_score,
    roc_auc_score,
)


@dataclass
class EvalResult:
    fold:       int
    roc_auc:    float
    pr_auc:     float
    log_loss:   float
    precision:  float
    recall:     float
    f1:         float
    threshold:  float   # decision threshold used for P/R/F1 (0.5 default)
    n_pos:      int
    n_neg:      int

    def to_dict(self):
        return {
            "fold":      self.fold,
            "roc_auc":   round(self.roc_auc,   4),
            "pr_auc":    round(self.pr_auc,    4),
            "log_loss":  round(self.log_loss,  4),
            "precision": round(self.precision, 4),
            "recall":    round(self.recall,    4),
            "f1":        round(self.f1,        4),
            "threshold": self.threshold,
            "n_pos":     self.n_pos,
            "n_neg":     self.n_neg,
        }


def evaluate_binary(
    y_true: pd.Series,
    y_pred: np.ndarray,
    fold: int = 0,
    threshold: float = 0.5,
) -> EvalResult:
    y_true_np = np.asarray(y_true)
    y_bin     = (y_pred >= threshold).astype(int)

    n_pos = int(y_true_np.sum())
    n_neg = int(len(y_true_np) - n_pos)

    # Guard against degenerate folds (all-one-class)
    if n_pos == 0 or n_neg == 0:
        return EvalResult(
            fold=fold, roc_auc=float("nan"), pr_auc=float("nan"),
            log_loss=float("nan"), precision=float("nan"),
            recall=float("nan"), f1=float("nan"),
            threshold=threshold, n_pos=n_pos, n_neg=n_neg,
        )

    return EvalResult(
        fold      = fold,
        roc_auc   = roc_auc_score(y_true_np, y_pred),
        pr_auc    = average_precision_score(y_true_np, y_pred),
        log_loss  = log_loss(y_true_np, y_pred),
        precision = precision_score(y_true_np, y_bin, zero_division=0),
        recall    = recall_score(y_true_np, y_bin, zero_division=0),
        f1        = f1_score(y_true_np, y_bin, zero_division=0),
        threshold = threshold,
        n_pos     = n_pos,
        n_neg     = n_neg,
    )


def evaluation_report(models) -> pd.DataFrame:
    """
    Aggregate per-fold metrics across all trained segment models into a
    human-readable DataFrame.

    models: List[SegmentModel]
    """
    rows = []
    for m in models:
        for r in m.cv_results:
            row = {"brand": m.brand_code, "segment": m.segment_label, "algo": m.algo}
            row.update(r.to_dict())
            rows.append(row)
    return pd.DataFrame(rows)


def summary_report(models) -> pd.DataFrame:
    """Mean metrics per segment across CV folds."""
    df = evaluation_report(models)
    agg = (
        df.groupby(["brand", "segment", "algo"])
        [["roc_auc", "pr_auc", "log_loss", "precision", "recall", "f1"]]
        .mean()
        .round(4)
        .reset_index()
    )
    return agg

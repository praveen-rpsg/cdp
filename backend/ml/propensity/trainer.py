"""
Model training — fits one binary propensity model per segment.

Supported algorithms:
  - LightGBM  (primary, handles missing values + categoricals natively)
  - LogisticRegression  (benchmark, requires full preprocessing)

Both are trained with Stratified K-Fold CV for OOF evaluation, then retrained
on the full dataset to produce the final scoring model.

Class imbalance strategy:
  - LightGBM: scale_pos_weight = negative / positive
  - LogisticRegression: class_weight='balanced' (set in LR_PARAMS)
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
from sklearn.linear_model import LogisticRegression
from sklearn.model_selection import StratifiedKFold

from .config import BrandConfig, CV_FOLDS, LGBM_PARAMS, LR_PARAMS, RANDOM_STATE
from .evaluator import EvalResult, evaluate_binary

try:
    import lightgbm as lgb
    _LGBM_AVAILABLE = True
except ImportError:
    _LGBM_AVAILABLE = False

logger = logging.getLogger(__name__)


@dataclass
class SegmentModel:
    """Container for a trained model for one segment."""
    brand_code:      str
    segment_label:   str
    segment_index:   int          # 1-based
    algo:            str          # "lgbm" | "lr"
    model:           Any          # fitted estimator
    feature_names:   List[str]
    cv_results:      List[EvalResult] = field(default_factory=list)
    oof_predictions: Optional[np.ndarray] = None

    @property
    def mean_auc(self) -> float:
        if not self.cv_results:
            return float("nan")
        return float(np.mean([r.roc_auc for r in self.cv_results]))

    def predict_proba(self, X: pd.DataFrame) -> np.ndarray:
        """Return probability of positive class (shape: [n_samples])."""
        if self.algo == "lgbm":
            return self.model.predict(X[self.feature_names], num_iteration=self.model.best_iteration_)
        return self.model.predict_proba(X)[:, 1]

    def summary(self) -> Dict[str, Any]:
        return {
            "brand":          self.brand_code,
            "segment":        self.segment_label,
            "segment_index":  self.segment_index,
            "algo":           self.algo,
            "cv_folds":       len(self.cv_results),
            "mean_auc":       round(self.mean_auc, 4),
            "per_fold_auc":   [round(r.roc_auc, 4) for r in self.cv_results],
        }


# ── LightGBM helpers ──────────────────────────────────────────────────────────

def _scale_pos_weight(y: pd.Series) -> float:
    neg = (y == 0).sum()
    pos = (y == 1).sum()
    return float(neg / pos) if pos > 0 else 1.0


def _train_lgbm_fold(
    X_tr: pd.DataFrame,
    y_tr: pd.Series,
    X_val: pd.DataFrame,
    y_val: pd.Series,
    params: Dict[str, Any],
    spw: float,
) -> Tuple[Any, np.ndarray]:
    """Train one LGBM fold and return (booster, val_preds)."""
    p = {**params, "scale_pos_weight": spw}
    n_est = p.pop("n_estimators", 500)
    early = p.pop("early_stopping_rounds", 50)

    dtrain = lgb.Dataset(X_tr, label=y_tr, free_raw_data=False)
    dval   = lgb.Dataset(X_val, label=y_val, reference=dtrain, free_raw_data=False)

    callbacks = [lgb.early_stopping(early, verbose=False), lgb.log_evaluation(-1)]
    booster = lgb.train(
        p,
        dtrain,
        num_boost_round=n_est,
        valid_sets=[dval],
        callbacks=callbacks,
    )
    val_preds = booster.predict(X_val, num_iteration=booster.best_iteration)
    return booster, val_preds


def _train_lgbm_final(
    X: pd.DataFrame,
    y: pd.Series,
    params: Dict[str, Any],
    spw: float,
    best_n_iter: int,
) -> Any:
    """Retrain LightGBM on full data for a fixed number of rounds."""
    p = {**params, "scale_pos_weight": spw}
    p.pop("n_estimators", None)
    p.pop("early_stopping_rounds", None)

    dtrain = lgb.Dataset(X, label=y, free_raw_data=False)
    booster = lgb.train(p, dtrain, num_boost_round=best_n_iter,
                        callbacks=[lgb.log_evaluation(-1)])
    booster.best_iteration_ = best_n_iter
    return booster


# ── LR helpers ────────────────────────────────────────────────────────────────

def _train_lr_fold(
    X_tr: np.ndarray,
    y_tr: pd.Series,
    X_val: np.ndarray,
) -> Tuple[LogisticRegression, np.ndarray]:
    lr = LogisticRegression(**LR_PARAMS)
    lr.fit(X_tr, y_tr)
    val_preds = lr.predict_proba(X_val)[:, 1]
    return lr, val_preds


# ── Main training function ─────────────────────────────────────────────────────

def train_segment_model(
    X: pd.DataFrame,
    y: pd.Series,
    cfg: BrandConfig,
    segment_index: int,           # 1-based
    algo: str = "lgbm",
    preprocessor=None,            # required when algo == "lr"
) -> SegmentModel:
    """
    Train a binary propensity model for one segment using CV, then refit on
    the full dataset.

    Returns a SegmentModel with OOF predictions and per-fold eval metrics.
    """
    seg_label = cfg.segments[segment_index - 1]
    pos_rate  = y.mean()
    logger.info(
        "[%s] Training %s model for segment %d/%d '%s' | positive rate=%.3f",
        cfg.brand_code, algo.upper(), segment_index, cfg.n_segments, seg_label, pos_rate,
    )

    skf = StratifiedKFold(n_splits=CV_FOLDS, shuffle=True, random_state=RANDOM_STATE)
    oof_preds  = np.zeros(len(y))
    cv_results: List[EvalResult] = []
    best_n_iters: List[int] = []

    for fold, (tr_idx, val_idx) in enumerate(skf.split(X, y), start=1):
        X_tr, X_val = X.iloc[tr_idx], X.iloc[val_idx]
        y_tr, y_val = y.iloc[tr_idx], y.iloc[val_idx]

        if algo == "lgbm":
            if not _LGBM_AVAILABLE:
                raise RuntimeError("lightgbm is not installed — pip install lightgbm")
            spw = _scale_pos_weight(y_tr)
            model_fold, val_preds = _train_lgbm_fold(X_tr, y_tr, X_val, y_val, LGBM_PARAMS, spw)
            best_n_iters.append(model_fold.best_iteration)

        elif algo == "lr":
            if preprocessor is None:
                raise ValueError("preprocessor required for LR training")
            preprocessor.fit(X_tr)
            X_tr_t  = preprocessor.transform(X_tr)
            X_val_t = preprocessor.transform(X_val)
            model_fold, val_preds = _train_lr_fold(X_tr_t, y_tr, X_val_t)
        else:
            raise ValueError(f"Unknown algo: {algo!r}")

        oof_preds[val_idx] = val_preds
        result = evaluate_binary(y_val, val_preds, fold=fold)
        cv_results.append(result)
        logger.info("  Fold %d | AUC=%.4f  PR-AUC=%.4f  LogLoss=%.4f",
                    fold, result.roc_auc, result.pr_auc, result.log_loss)

    # ── Refit on full data ──────────────────────────────────────────────────
    if algo == "lgbm":
        spw = _scale_pos_weight(y)
        best_n = int(np.mean(best_n_iters))
        final_model = _train_lgbm_final(X, y, LGBM_PARAMS, spw, best_n)
        feature_names = X.columns.tolist()

    elif algo == "lr":
        preprocessor.fit(X)
        X_t = preprocessor.transform(X)
        final_model = LogisticRegression(**LR_PARAMS)
        final_model.fit(X_t, y)
        # For LR we store the preprocessor alongside the model
        final_model._preprocessor = preprocessor
        feature_names = X.columns.tolist()

    mean_auc = float(np.mean([r.roc_auc for r in cv_results]))
    logger.info(
        "[%s] Segment %d '%s' — mean CV AUC=%.4f",
        cfg.brand_code, segment_index, seg_label, mean_auc,
    )

    return SegmentModel(
        brand_code     = cfg.brand_code,
        segment_label  = seg_label,
        segment_index  = segment_index,
        algo           = algo,
        model          = final_model,
        feature_names  = feature_names,
        cv_results     = cv_results,
        oof_predictions= oof_preds,
    )


def train_all_segments(
    X: pd.DataFrame,
    y_list: List[pd.Series],
    cfg: BrandConfig,
    algo: str = "lgbm",
) -> List[SegmentModel]:
    """
    Train one model per segment. Returns a list of SegmentModel in segment order.
    """
    from .features import build_preprocessor, lgbm_preprocess

    if algo == "lgbm":
        X_model = lgbm_preprocess(X)
        preprocessor = None
    else:
        preprocessor = build_preprocessor(X)
        X_model = X

    models: List[SegmentModel] = []
    for i, y in enumerate(y_list, start=1):
        seg_model = train_segment_model(
            X_model, y, cfg, segment_index=i,
            algo=algo, preprocessor=preprocessor,
        )
        models.append(seg_model)

    logger.info(
        "[%s] All %d models trained. Mean AUCs: %s",
        cfg.brand_code,
        len(models),
        {m.segment_label: round(m.mean_auc, 4) for m in models},
    )
    return models

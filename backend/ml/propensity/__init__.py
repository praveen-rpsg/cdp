"""
ml.propensity — multi-label customer propensity scoring for Spencers and NBL.

Public API:
    from ml.propensity.pipeline import run_pipeline, run_all_brands
    from ml.propensity.config import BRAND_CONFIGS
"""

from .pipeline import run_all_brands, run_pipeline

__all__ = ["run_pipeline", "run_all_brands"]

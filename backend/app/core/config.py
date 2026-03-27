"""
Core configuration for the Composable CDP platform.
Supports multi-brand tenancy with per-brand data lake connections.
"""

from __future__ import annotations

from enum import Enum
from typing import Any

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings


class BrandCode(str, Enum):
    SPENCERS = "spencers"
    FMCG = "fmcg"
    POWER_CESC = "power_cesc"
    NATURES_BASKET = "natures_basket"


class ChannelType(str, Enum):
    B2C = "b2c"
    B2B = "b2b"
    D2C = "d2c"
    ECOM = "ecom"


BRAND_CHANNEL_MAP: dict[BrandCode, list[ChannelType]] = {
    BrandCode.SPENCERS: [ChannelType.B2C, ChannelType.D2C, ChannelType.ECOM],
    BrandCode.FMCG: [ChannelType.D2C, ChannelType.B2B],
    BrandCode.POWER_CESC: [ChannelType.B2C, ChannelType.B2B],
    BrandCode.NATURES_BASKET: [ChannelType.B2C, ChannelType.ECOM],
}


class BrandDataLakeConfig(BaseSettings):
    """Per-brand Athena/data lake connection settings."""

    brand_code: BrandCode
    aws_region: str = "ap-south-1"
    athena_database: str
    athena_workgroup: str = "primary"
    athena_output_bucket: str
    gold_layer_prefix: str = "gold"
    # Table names in the gold layer
    customers_table: str = "customers"
    transactions_table: str = "transactions"
    events_table: str = "events"
    products_table: str = "products"
    interactions_table: str = "interactions"


class Settings(BaseSettings):
    model_config = {"env_prefix": "CDP_", "env_nested_delimiter": "__"}

    # --- App ---
    app_name: str = "UNIFY360"
    app_version: str = "0.1.0"
    debug: bool = False
    api_prefix: str = "/api/v1"

    # --- Database (metadata store) ---
    database_url: str = "postgresql+asyncpg://cdp:cdp@localhost:5432/cdp_meta"

    # --- Redis ---
    redis_url: str = "redis://localhost:6379/0"

    # --- Auth ---
    secret_key: str = "CHANGE-ME-IN-PRODUCTION"
    access_token_expire_minutes: int = 60

    # --- AWS defaults ---
    aws_region: str = "ap-south-1"
    aws_access_key_id: str | None = None
    aws_secret_access_key: str | None = None

    # --- Brand data lake configs (populated at startup from DB or env) ---
    brand_datalakes: dict[str, BrandDataLakeConfig] = Field(default_factory=dict)

    # --- Segmentation ---
    max_segment_preview_rows: int = 1000
    segment_computation_schedule_minutes: int = 60
    max_concurrent_athena_queries: int = 5

    # --- Corporate cross-brand ---
    corporate_athena_database: str = "corporate_cdp"
    corporate_athena_workgroup: str = "corporate"
    corporate_output_bucket: str = "s3://corporate-cdp-athena-results/"


settings = Settings()

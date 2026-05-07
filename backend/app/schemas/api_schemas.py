"""
Pydantic schemas for API request/response models.
"""

from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field


# =============================================================================
# SEGMENT SCHEMAS
# =============================================================================

class SegmentCreate(BaseModel):
    brand_id: str | None = None
    name: str = Field(..., min_length=1, max_length=255)
    description: str | None = None
    segment_type: str = "dynamic"
    rules: dict = Field(..., description="Segment rule tree")
    schedule: str = "hourly"
    is_cross_brand: bool = False
    tags: list[str] = []


class SegmentUpdate(BaseModel):
    name: str | None = None
    description: str | None = None
    rules: dict | None = None
    schedule: str | None = None
    is_active: bool | None = None
    tags: list[str] | None = None


class SegmentResponse(BaseModel):
    id: str
    brand_id: str | None
    name: str
    description: str | None
    slug: str
    segment_type: str
    rules: dict
    schedule: str
    is_active: bool
    is_cross_brand: bool
    audience_count: int | None
    computation_status: str
    last_computed_at: datetime | None
    tags: list[str]
    created_by: str | None
    created_at: datetime | None


class SegmentListResponse(BaseModel):
    segments: list[SegmentResponse]
    total: int
    page: int
    page_size: int


# =============================================================================
# AUDIENCE SCHEMAS
# =============================================================================

class AudienceEstimateRequest(BaseModel):
    brand_code: str
    rules: dict
    datalake_config: dict | None = None


class AudienceEstimateResponse(BaseModel):
    brand_code: str
    estimated_count: int | None
    sql: str
    status: str
    # Set operation results (optional)
    set_operation_counts: dict | None = None
    # Split results (optional)
    split_counts: list[dict] | None = None


class AudienceSummaryRequest(BaseModel):
    brand_code: str
    rules: dict
    metrics: list[str] = ["total_spend", "avg_spend", "total_bills", "avg_visits", "spend_per_bill", "spend_per_visit"]
    datalake_config: dict | None = None


class AudienceSummaryResponse(BaseModel):
    brand_code: str
    audience_size: int
    metrics: dict[str, float | int | None]
    sql: str
    status: str


class AudiencePreviewRequest(BaseModel):
    brand_code: str
    rules: dict
    limit: int = 100
    datalake_config: dict | None = None


class AudiencePreviewResponse(BaseModel):
    brand_code: str
    profiles: list[dict]
    sql: str
    status: str


# =============================================================================
# QUERY COMPILATION SCHEMAS
# =============================================================================

class CompileRequest(BaseModel):
    brand_code: str
    rules: dict
    datalake_config: dict | None = None


class CompileResponse(BaseModel):
    sql: str
    brand_code: str


# =============================================================================
# ATTRIBUTE CATALOG SCHEMAS
# =============================================================================

class AttributeResponse(BaseModel):
    key: str
    label: str
    category: str
    data_type: str
    description: str
    operators: list[str]
    example_values: list[Any] = []
    is_computed: bool
    is_array: bool
    is_b2b_only: bool
    applicable_brands: list[str] | None
    unit: str | None
    source_table: str | None = None


class AttributeCatalogResponse(BaseModel):
    attributes: list[AttributeResponse]
    total: int
    categories: list[str]


# =============================================================================
# BRAND SCHEMAS
# =============================================================================

class BrandResponse(BaseModel):
    id: str
    code: str
    name: str
    channels: list[str]
    business_model: str | None
    is_active: bool

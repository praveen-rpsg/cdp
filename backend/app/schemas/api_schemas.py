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
# SAVED SEGMENTS REPOSITORY SCHEMAS
# =============================================================================

class SavedSegmentCreate(BaseModel):
    name: str = Field(..., min_length=1, max_length=255)
    description: str | None = None
    business_purpose: str | None = None
    tags: list[str] = []
    brand_code: str | None = None
    rules: dict = Field(..., description="Segment rule tree")
    created_by: str | None = None


class SavedSegmentUpdate(BaseModel):
    name: str | None = None
    description: str | None = None
    business_purpose: str | None = None
    tags: list[str] | None = None
    rules: dict | None = None
    brand_code: str | None = None
    status: str | None = None  # active | archived


class SavedSegment(BaseModel):
    id: str
    name: str
    description: str | None = None
    business_purpose: str | None = None
    tags: list[str] = []
    segment_type: str
    brand_code: str | None = None
    rules: dict
    audience_count: int | None = None
    status: str
    created_by: str | None = None
    created_at: str | None = None
    updated_at: str | None = None
    last_computed_at: str | None = None
    parent_segment_id: str | None = None
    lineage: dict | None = None


class SavedSegmentListResponse(BaseModel):
    segments: list[SavedSegment]
    total: int
    page: int
    page_size: int


# =============================================================================
# RANK & SPLIT SCHEMAS
# =============================================================================

class RankAttribute(BaseModel):
    attribute: str
    weight: float = 1.0
    order: str = "desc"  # desc = higher value ranks first


class SplitGroupInput(BaseModel):
    name: str = ""
    percent: float | None = None
    tags: list[str] = []


class RankSplitConstraints(BaseModel):
    max_count: int | None = None
    budget: float | None = None
    cost_per_contact: float | None = None


class RankSplitRequest(BaseModel):
    brand_code: str
    rules: dict
    rank: list[RankAttribute] = []
    splits: list[SplitGroupInput] = Field(..., min_length=1)
    constraints: RankSplitConstraints | None = None


class RankSplitSaveRequest(RankSplitRequest):
    base_name: str = Field(..., min_length=1)
    created_by: str | None = None
    parent_segment_id: str | None = None


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

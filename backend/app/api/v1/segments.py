"""
Segment API Routes
===================

CRUD operations for segments, audience estimation, preview,
query compilation, and the attribute catalog.
"""

from __future__ import annotations

from fastapi import APIRouter, HTTPException, Query

from app.schemas.api_schemas import (
    AttributeCatalogResponse,
    AttributeResponse,
    AudienceEstimateRequest,
    AudienceEstimateResponse,
    AudiencePreviewRequest,
    AudiencePreviewResponse,
    CompileRequest,
    CompileResponse,
    SegmentCreate,
    SegmentListResponse,
    SegmentResponse,
    SegmentUpdate,
)
from app.schemas.profile_attributes import (
    ATTRIBUTE_CATALOG,
    ATTRIBUTES_BY_CATEGORY,
    AttributeCategory,
    get_attributes_for_brand,
)
from app.services.segmentation.service import SegmentationService

router = APIRouter(prefix="/segments", tags=["Segments"])

# Service instance — in production, injected via FastAPI Depends
service = SegmentationService()


# =============================================================================
# SEGMENT CRUD
# =============================================================================


@router.post("/", response_model=SegmentResponse)
async def create_segment(payload: SegmentCreate):
    """Create a new segment definition."""
    result = await service.create_segment(
        brand_id=payload.brand_id,
        name=payload.name,
        description=payload.description,
        rules=payload.rules,
        segment_type=payload.segment_type,
        schedule=payload.schedule,
        is_cross_brand=payload.is_cross_brand,
        tags=payload.tags,
    )
    return result


@router.get("/", response_model=SegmentListResponse)
async def list_segments(
    brand_id: str | None = Query(None, description="Filter by brand ID"),
    segment_type: str | None = Query(None, description="Filter by type"),
    is_active: bool | None = Query(None),
    search: str | None = Query(None, description="Search by name"),
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
):
    """List segments with filters and pagination."""
    # In production: query DB with filters
    return SegmentListResponse(segments=[], total=0, page=page, page_size=page_size)


@router.get("/{segment_id}", response_model=SegmentResponse)
async def get_segment(segment_id: str):
    """Get a specific segment by ID."""
    # In production: fetch from DB
    raise HTTPException(status_code=404, detail="Segment not found")


@router.put("/{segment_id}", response_model=SegmentResponse)
async def update_segment(segment_id: str, payload: SegmentUpdate):
    """Update a segment."""
    if payload.rules:
        result = await service.update_segment_rules(segment_id, payload.rules)
    # In production: update other fields in DB
    raise HTTPException(status_code=404, detail="Segment not found")


@router.delete("/{segment_id}")
async def delete_segment(segment_id: str):
    """Delete (soft) a segment."""
    result = await service.delete_segment(segment_id)
    return result


# =============================================================================
# AUDIENCE ESTIMATION & PREVIEW
# =============================================================================


@router.post("/estimate", response_model=AudienceEstimateResponse)
async def estimate_audience(payload: AudienceEstimateRequest):
    """
    Estimate audience size for a segment definition without saving it.
    Used by the segment builder UI for real-time count feedback.
    """
    result = await service.estimate_audience_size(
        brand_code=payload.brand_code,
        rules=payload.rules,
        datalake_config=payload.datalake_config,
    )
    return result


@router.post("/preview", response_model=AudiencePreviewResponse)
async def preview_audience(payload: AudiencePreviewRequest):
    """
    Preview matching profiles for a segment definition.
    Returns sample rows to help users validate their segment logic.
    """
    result = await service.preview_audience(
        brand_code=payload.brand_code,
        rules=payload.rules,
        limit=payload.limit,
        datalake_config=payload.datalake_config,
    )
    return result


# =============================================================================
# QUERY COMPILATION (for power users)
# =============================================================================


@router.post("/compile", response_model=CompileResponse)
async def compile_segment_query(payload: CompileRequest):
    """
    Compile segment rules into PostgreSQL SQL without executing.
    Useful for power users who want to review/modify the generated SQL.
    """
    sql = service.compile_segment_query(
        brand_code=payload.brand_code,
        rules=payload.rules,
        datalake_config=payload.datalake_config,
    )
    return CompileResponse(sql=sql, brand_code=payload.brand_code)


# =============================================================================
# SEGMENT COMPUTATION
# =============================================================================


@router.post("/{segment_id}/compute")
async def trigger_compute(segment_id: str):
    """Trigger manual computation of a segment's audience."""
    result = await service.compute_segment(segment_id)
    return result


# =============================================================================
# ATTRIBUTE CATALOG
# =============================================================================


@router.get("/attributes/catalog", response_model=AttributeCatalogResponse)
async def get_attribute_catalog(
    brand_code: str | None = Query(None, description="Filter attributes by brand"),
    category: str | None = Query(None, description="Filter by category"),
):
    """
    Get the full attribute catalog available for segmentation.
    This powers the segment builder UI's attribute picker.
    """
    attrs = ATTRIBUTE_CATALOG

    if brand_code:
        attrs = get_attributes_for_brand(brand_code)

    if category:
        try:
            cat = AttributeCategory(category)
            attrs = [a for a in attrs if a.category == cat]
        except ValueError:
            raise HTTPException(status_code=400, detail=f"Invalid category: {category}")

    categories = sorted(set(a.category.value for a in attrs))

    return AttributeCatalogResponse(
        attributes=[
            AttributeResponse(
                key=a.key,
                label=a.label,
                category=a.category.value,
                data_type=a.data_type.value,
                description=a.description,
                operators=a.operators,
                example_values=a.example_values,
                is_computed=a.is_computed,
                is_array=a.is_array,
                is_b2b_only=a.is_b2b_only,
                applicable_brands=a.applicable_brands,
                unit=a.unit,
            )
            for a in attrs
        ],
        total=len(attrs),
        categories=categories,
    )


# =============================================================================
# SEGMENT TEMPLATES
# =============================================================================

from app.services.segmentation.templates import (
    TEMPLATES,
    get_template_by_id,
    get_templates_by_category,
    get_templates_by_function,
    get_templates_for_brand,
)


@router.get("/templates/list")
async def list_templates(
    brand_code: str | None = Query(None, description="Filter templates by brand"),
    category: str | None = Query(None, description="Filter by category"),
    business_function: str | None = Query(None, description="Filter by business function (marketing, product, merch, cx, finance)"),
):
    """
    List available segment templates.
    Templates are pre-built segment definitions for common business use cases.
    """
    templates = TEMPLATES

    if brand_code:
        templates = get_templates_for_brand(brand_code)
    if category:
        templates = [t for t in templates if t.category == category]
    if business_function:
        templates = [t for t in templates if t.business_function == business_function]

    return {
        "templates": [t.to_dict() for t in templates],
        "total": len(templates),
    }


@router.get("/templates/{template_id}")
async def get_template(template_id: str):
    """Get a specific template by ID, ready to load into the segment builder."""
    template = get_template_by_id(template_id)
    if not template:
        raise HTTPException(status_code=404, detail="Template not found")
    return template.to_dict()

"""
Customer Single View API
========================

Composes a single customer's 360° profile for the Customer Single View page.
Lookup by mobile number or unified_id, brand-aware.
"""

from __future__ import annotations

from fastapi import APIRouter, HTTPException, Query

from app.schemas.customer_view import (
    CorporateSingleViewResponse,
    CustomerSingleViewResponse,
)
from app.services.customer_view.service import CustomerViewService

router = APIRouter(prefix="/customers", tags=["Customer Single View"])

service = CustomerViewService()


@router.get("/single-view", response_model=CustomerSingleViewResponse)
async def get_single_view(
    brand_code: str = Query(..., description="Brand code (spencers, natures_basket)"),
    customer_id: str | None = Query(None, description="unified_id (exact)"),
    mobile: str | None = Query(None, description="Mobile number (last-10-digit match)"),
):
    if not customer_id and not mobile:
        raise HTTPException(status_code=400, detail="Provide customer_id or mobile")
    try:
        return await service.get_single_view(
            customer_id=customer_id, mobile=mobile, brand_code=brand_code
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/search")
async def search_customers(
    brand_code: str = Query(..., description="spencers | natures_basket | corporate"),
    q: str = Query(..., description="Partial mobile, id, or name (min 3 chars)"),
    limit: int = Query(10, ge=1, le=25),
):
    """Type-ahead customer search for the single-view search boxes."""
    return {"results": await service.search_customers(brand_code=brand_code, q=q, limit=limit)}


@router.get("/corporate-view", response_model=CorporateSingleViewResponse)
async def get_corporate_view(
    r1_id: str | None = Query(None, description="RPSG universal ID (exact)"),
    mobile: str | None = Query(None, description="Mobile number (last-10-digit match)"),
):
    if not r1_id and not mobile:
        raise HTTPException(status_code=400, detail="Provide r1_id or mobile")
    return await service.get_corporate_view(r1_id=r1_id, mobile=mobile)


@router.delete("/single-view/cache", status_code=204)
async def invalidate_single_view_cache(
    brand_code: str = Query(...),
    customer_id: str = Query(..., description="unified_id whose cache entry to evict"),
):
    """Evict a cached single-view profile (e.g. after a manual data correction)."""
    await service._cache_del(f"sv:{brand_code}:{customer_id}")


@router.delete("/corporate-view/cache", status_code=204)
async def invalidate_corporate_view_cache(
    r1_id: str = Query(..., description="r1_id whose cache entry to evict"),
):
    """Evict a cached corporate-view profile."""
    await service._cache_del(f"corp:{r1_id}")

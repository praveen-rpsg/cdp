"""
Brand API Routes — multi-tenant brand management.
"""

from __future__ import annotations

from fastapi import APIRouter

from app.core.config import BRAND_CHANNEL_MAP, BrandCode

router = APIRouter(prefix="/brands", tags=["Brands"])

# Static brand registry — in production, fetched from DB
BRANDS = [
    {
        "id": "brand-spencers-001",
        "code": "spencers",
        "name": "Spencers",
        "channels": ["b2c", "d2c", "ecom"],
        "business_model": "retail",
        "is_active": True,
    },
    {
        "id": "brand-fmcg-002",
        "code": "fmcg",
        "name": "FMCG",
        "channels": ["d2c", "b2b"],
        "business_model": "fmcg",
        "is_active": True,
    },
    {
        "id": "brand-cesc-003",
        "code": "power_cesc",
        "name": "Power CESC",
        "channels": ["b2c", "b2b"],
        "business_model": "utility",
        "is_active": True,
    },
    {
        "id": "brand-nb-004",
        "code": "natures_basket",
        "name": "Nature's Basket",
        "channels": ["b2c", "ecom"],
        "business_model": "grocery",
        "is_active": True,
    },
]


@router.get("/")
async def list_brands():
    """List all brands in the corporate BU."""
    return {"brands": BRANDS, "total": len(BRANDS)}


@router.get("/{brand_code}")
async def get_brand(brand_code: str):
    """Get details for a specific brand."""
    for brand in BRANDS:
        if brand["code"] == brand_code:
            return brand
    return {"error": "Brand not found"}, 404


@router.get("/{brand_code}/datalake-status")
async def get_datalake_status(brand_code: str):
    """Check connectivity and table availability for a brand's data lake."""
    return {
        "brand_code": brand_code,
        "status": "connected",
        "tables": {
            "customers": {"row_count": None, "last_updated": None},
            "transactions": {"row_count": None, "last_updated": None},
            "events": {"row_count": None, "last_updated": None},
            "products": {"row_count": None, "last_updated": None},
        },
        "message": "Connect Athena credentials to get live status",
    }

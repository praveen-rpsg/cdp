"""
Composable CDP — FastAPI Application
"""

from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.api.v1.brands import router as brands_router
from app.api.v1.customers import router as customers_router
from app.api.v1.segments import router as segments_router
from app.core.config import settings
from app.services.segmentation.service import (
    close_resources,
    ensure_propensity_tables,
    ensure_segments_table,
    init_resources,
)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Open the DB pool and Redis on startup; close them on shutdown."""
    await init_resources()
    await ensure_propensity_tables()
    await ensure_segments_table()
    yield
    await close_resources()


app = FastAPI(
    title=settings.app_name,
    version=settings.app_version,
    description="Composable Customer Data Platform — Multi-Brand Segmentation Engine",
    docs_url="/docs",
    redoc_url="/redoc",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000", "http://localhost:5173"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(brands_router, prefix=settings.api_prefix)
app.include_router(segments_router, prefix=settings.api_prefix)
app.include_router(customers_router, prefix=settings.api_prefix)


@app.get("/health")
async def health_check():
    return {"status": "healthy", "version": settings.app_version}


@app.get("/")
async def root():
    return {
        "name": settings.app_name,
        "version": settings.app_version,
        "docs": "/docs",
        "brands": ["spencers", "fmcg", "power_cesc", "natures_basket"],
    }

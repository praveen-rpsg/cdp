"""
Multi-tenant models: Corporate BU, Brands, Users, and Roles.
"""

from __future__ import annotations

import uuid
from datetime import datetime

from sqlalchemy import (
    Boolean,
    DateTime,
    Enum,
    ForeignKey,
    String,
    Text,
    func,
)
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


class Base(DeclarativeBase):
    pass


class CorporateBU(Base):
    """Top-level corporate business unit (e.g., RPSG Group)."""

    __tablename__ = "corporate_bus"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    slug: Mapped[str] = mapped_column(String(100), unique=True, nullable=False)
    config: Mapped[dict] = mapped_column(JSONB, default=dict)
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())

    brands: Mapped[list[Brand]] = relationship(back_populates="corporate_bu")


class Brand(Base):
    """A brand within a corporate BU. Each brand has its own data lake."""

    __tablename__ = "brands"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    corporate_bu_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("corporate_bus.id"), nullable=False)
    code: Mapped[str] = mapped_column(String(50), unique=True, nullable=False)  # e.g. 'spencers'
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    channels: Mapped[list] = mapped_column(JSONB, default=list)  # ['b2c','d2c','ecom']
    business_model: Mapped[str] = mapped_column(String(50))  # 'retail', 'fmcg', 'utility', 'grocery'

    # Data lake connection config
    datalake_config: Mapped[dict] = mapped_column(JSONB, default=dict)

    # Schema mapping: maps CDP canonical fields to brand-specific column names
    schema_mapping: Mapped[dict] = mapped_column(JSONB, default=dict)

    is_active: Mapped[bool] = mapped_column(Boolean, default=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())

    corporate_bu: Mapped[CorporateBU] = relationship(back_populates="brands")
    segments: Mapped[list["Segment"]] = relationship(back_populates="brand")


class User(Base):
    """Platform user with brand/corporate-level access."""

    __tablename__ = "users"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    email: Mapped[str] = mapped_column(String(255), unique=True, nullable=False)
    hashed_password: Mapped[str] = mapped_column(String(255), nullable=False)
    full_name: Mapped[str] = mapped_column(String(255))
    role: Mapped[str] = mapped_column(
        Enum("admin", "power_user", "business_user", "viewer", name="user_role"),
        default="business_user",
    )
    # Corporate-level user can see all brands; brand-level sees only assigned brands
    corporate_bu_id: Mapped[uuid.UUID | None] = mapped_column(ForeignKey("corporate_bus.id"))
    allowed_brand_ids: Mapped[list] = mapped_column(JSONB, default=list)
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())


# Import here to avoid circular dependency — Segment model defined in segment.py
from app.models.segment import Segment  # noqa: E402, F401

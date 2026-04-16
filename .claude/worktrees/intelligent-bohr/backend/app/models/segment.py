"""
Segment model — stores segment definitions, rules, and computation metadata.
"""

from __future__ import annotations

import uuid
from datetime import datetime

from sqlalchemy import (
    Boolean,
    DateTime,
    Enum,
    ForeignKey,
    Integer,
    String,
    Text,
    func,
)
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.models.tenant import Base


class Segment(Base):
    """
    A segment definition. The `rules` JSONB column stores the full
    segment builder tree that gets compiled into Athena SQL.
    """

    __tablename__ = "segments"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    brand_id: Mapped[uuid.UUID | None] = mapped_column(ForeignKey("brands.id"))
    is_cross_brand: Mapped[bool] = mapped_column(Boolean, default=False)

    name: Mapped[str] = mapped_column(String(255), nullable=False)
    description: Mapped[str | None] = mapped_column(Text)
    slug: Mapped[str] = mapped_column(String(255), nullable=False)

    # Segment type
    segment_type: Mapped[str] = mapped_column(
        Enum("static", "dynamic", "predictive", "lookalike", "lifecycle", name="segment_type"),
        default="dynamic",
    )

    # The rule tree — JSON structure that the UI builds and the query engine compiles
    rules: Mapped[dict] = mapped_column(JSONB, nullable=False, default=dict)

    # Scheduling
    schedule: Mapped[str] = mapped_column(String(50), default="hourly")  # cron or preset
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)

    # Computation results
    last_computed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    audience_count: Mapped[int | None] = mapped_column(Integer)
    computation_status: Mapped[str] = mapped_column(
        Enum("pending", "computing", "ready", "failed", name="computation_status"),
        default="pending",
    )
    last_query_sql: Mapped[str | None] = mapped_column(Text)  # generated Athena SQL for debugging

    # Tags and metadata
    tags: Mapped[list] = mapped_column(JSONB, default=list)
    created_by: Mapped[uuid.UUID | None] = mapped_column(ForeignKey("users.id"))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())

    brand: Mapped["Brand"] = relationship(back_populates="segments")
    activations: Mapped[list["SegmentActivation"]] = relationship(back_populates="segment")

    # Import Brand here for type reference
    from app.models.tenant import Brand


class SegmentActivation(Base):
    """Track where segments are activated (email, ads, push, etc.)."""

    __tablename__ = "segment_activations"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    segment_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("segments.id"), nullable=False)
    destination_type: Mapped[str] = mapped_column(String(50))  # 'email', 'facebook_ads', 'google_ads', etc.
    destination_config: Mapped[dict] = mapped_column(JSONB, default=dict)
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)
    last_synced_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())

    segment: Mapped[Segment] = relationship(back_populates="activations")


class SegmentVersion(Base):
    """Version history for segment rule changes."""

    __tablename__ = "segment_versions"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    segment_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("segments.id"), nullable=False)
    version: Mapped[int] = mapped_column(Integer, nullable=False)
    rules: Mapped[dict] = mapped_column(JSONB, nullable=False)
    created_by: Mapped[uuid.UUID | None] = mapped_column(ForeignKey("users.id"))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())

"""
Segment Rule Schema — The JSON structure that the UI builds
and the query engine compiles into SQL.

Supports:
- Nested AND/OR groups (unlimited depth)
- Attribute conditions with typed operators
- Event-based conditions (has_performed, count-based)
- Time windowing (absolute and relative)
- Cross-brand conditions (for corporate-level segments)
- Segment membership conditions (segment-of-segments)
"""

from __future__ import annotations

from datetime import date, datetime
from enum import Enum
from typing import Any, Literal, Union

from pydantic import BaseModel, Field


class LogicalOperator(str, Enum):
    AND = "and"
    OR = "or"


class TimeWindowType(str, Enum):
    LAST_N_DAYS = "last_n_days"
    BETWEEN_DATES = "between_dates"
    BEFORE_DATE = "before_date"
    AFTER_DATE = "after_date"
    ALL_TIME = "all_time"


class TimeWindow(BaseModel):
    """Time constraint for event or computed attribute conditions."""
    type: TimeWindowType = TimeWindowType.ALL_TIME
    days: int | None = None
    start_date: date | None = None
    end_date: date | None = None


class AttributeCondition(BaseModel):
    """A single condition on a profile attribute."""
    type: Literal["attribute"] = "attribute"
    attribute_key: str  # e.g. "txn.total_revenue"
    operator: str  # e.g. "greater_than"
    value: Any  # The comparison value
    second_value: Any | None = None  # For "between" operators
    time_window: TimeWindow | None = None
    negate: bool = False


class EventCondition(BaseModel):
    """A condition based on behavioral events."""
    type: Literal["event"] = "event"
    event_name: str  # e.g. "product_viewed", "add_to_cart", "purchase"
    operator: str  # "has_performed", "has_not_performed", "performed_count_greater_than", etc.
    count_value: int | None = None  # For count-based operators
    time_window: TimeWindow = Field(default_factory=lambda: TimeWindow(type=TimeWindowType.ALL_TIME))
    # Optional filters on the event properties
    event_property_filters: list[EventPropertyFilter] | None = None
    negate: bool = False


class EventPropertyFilter(BaseModel):
    """Filter on a property of an event (e.g., product_viewed where category = 'electronics')."""
    property_name: str
    operator: str
    value: Any


class SegmentMembershipCondition(BaseModel):
    """Condition based on membership in another segment."""
    type: Literal["segment_membership"] = "segment_membership"
    segment_id: str
    operator: Literal["is_member", "is_not_member"] = "is_member"


class CrossBrandCondition(BaseModel):
    """Condition that references data from another brand (corporate-level only)."""
    type: Literal["cross_brand"] = "cross_brand"
    brand_code: str
    condition: AttributeCondition | EventCondition


# Union of all condition types
ConditionType = Union[
    AttributeCondition,
    EventCondition,
    SegmentMembershipCondition,
    CrossBrandCondition,
    "ConditionGroup",
]


class ConditionGroup(BaseModel):
    """A group of conditions joined by AND/OR. Can be nested."""
    type: Literal["group"] = "group"
    logical_operator: LogicalOperator = LogicalOperator.AND
    conditions: list[ConditionType] = []


# Rebuild model to handle forward references
ConditionGroup.model_rebuild()


class RankConfig(BaseModel):
    """Rank profiles by a numeric attribute."""
    enabled: bool = False
    attribute: str | None = None          # e.g. "txn.total_revenue"
    order: Literal["asc", "desc"] = "desc"
    profile_limit: int | None = None      # Top/bottom N


class SplitEntry(BaseModel):
    """A single split bucket."""
    name: str = ""
    percent: int | None = None            # For percent-based splits
    value: str | None = None              # For attribute-based splits


class SplitConfig(BaseModel):
    """Split an audience into sub-segments."""
    enabled: bool = False
    split_type: Literal["percent", "attribute"] = "percent"
    attribute: str | None = None          # For attribute splits
    num_splits: int = 2
    splits: list[SplitEntry] = []


class SetOperationEntry(BaseModel):
    """A reference to another segment for set operations."""
    segment_id: str | None = None
    rules: "SegmentDefinition | None" = None   # Inline rules
    name: str | None = None


class SetOperation(BaseModel):
    """Combine multiple segments via Union/Overlap/Exclude."""
    enabled: bool = False
    operation: Literal["union", "overlap", "exclude_overlap", "exclude"] = "union"
    segments: list[SetOperationEntry] = []


class SegmentDefinition(BaseModel):
    """
    The top-level segment rule definition.
    This is what gets stored in the segment.rules JSONB column
    and compiled into PostgreSQL (or Athena) SQL.
    """
    root: ConditionGroup
    # Optional: limit results
    limit: int | None = None
    # Optional: order/sort
    order_by: str | None = None
    order_direction: Literal["asc", "desc"] = "desc"
    # Rank & Split
    rank: RankConfig | None = None
    split: SplitConfig | None = None
    # Set operations (Union, Overlap, Exclude)
    set_operation: SetOperation | None = None


# Rebuild for forward references
SetOperationEntry.model_rebuild()


# =============================================================================
# EXAMPLE SEGMENT DEFINITIONS
# =============================================================================

EXAMPLE_HIGH_VALUE_AT_RISK = SegmentDefinition(
    root=ConditionGroup(
        logical_operator=LogicalOperator.AND,
        conditions=[
            AttributeCondition(
                attribute_key="txn.total_spend",
                operator="greater_than",
                value=50000,
            ),
            AttributeCondition(
                attribute_key="temporal.recency_days",
                operator="greater_than",
                value=60,
            ),
            AttributeCondition(
                attribute_key="lifecycle.lifecycle_stage",
                operator="equals",
                value="At Risk",
            ),
        ],
    )
)

EXAMPLE_STAR_CUSTOMERS = SegmentDefinition(
    root=ConditionGroup(
        logical_operator=LogicalOperator.AND,
        conditions=[
            AttributeCondition(
                attribute_key="lifecycle.l2_segment",
                operator="equals",
                value="STAR",
            ),
            AttributeCondition(
                attribute_key="txn.total_bills",
                operator="greater_than_or_equal",
                value=4,
            ),
        ],
    )
)

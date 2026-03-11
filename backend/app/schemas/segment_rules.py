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


class SegmentDefinition(BaseModel):
    """
    The top-level segment rule definition.
    This is what gets stored in the segment.rules JSONB column
    and compiled into Athena SQL.
    """
    root: ConditionGroup
    # Optional: limit results
    limit: int | None = None
    # Optional: order/sort
    order_by: str | None = None
    order_direction: Literal["asc", "desc"] = "desc"


# =============================================================================
# EXAMPLE SEGMENT DEFINITIONS
# =============================================================================

EXAMPLE_HIGH_VALUE_AT_RISK = SegmentDefinition(
    root=ConditionGroup(
        logical_operator=LogicalOperator.AND,
        conditions=[
            AttributeCondition(
                attribute_key="txn.total_revenue",
                operator="greater_than",
                value=50000,
            ),
            AttributeCondition(
                attribute_key="txn.days_since_last_purchase",
                operator="greater_than",
                value=60,
            ),
            AttributeCondition(
                attribute_key="predict.churn_risk_tier",
                operator="in_list",
                value=["high", "critical"],
            ),
        ],
    )
)

EXAMPLE_CROSS_BRAND_GROCERY_BUYERS = SegmentDefinition(
    root=ConditionGroup(
        logical_operator=LogicalOperator.AND,
        conditions=[
            AttributeCondition(
                attribute_key="identity.is_cross_brand_customer",
                operator="is_true",
                value=True,
            ),
            ConditionGroup(
                logical_operator=LogicalOperator.OR,
                conditions=[
                    AttributeCondition(
                        attribute_key="identity.known_brands",
                        operator="contains",
                        value="spencers",
                    ),
                    AttributeCondition(
                        attribute_key="identity.known_brands",
                        operator="contains",
                        value="natures_basket",
                    ),
                ],
            ),
            AttributeCondition(
                attribute_key="txn.purchase_frequency_90d",
                operator="greater_than_or_equal",
                value=3,
            ),
        ],
    )
)

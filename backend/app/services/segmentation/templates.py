"""
Pre-Built Segment Templates
=============================

Business-ready segment definitions for Spencer's retail.
Users can load these as starting points and customize.

All templates use only valid attribute keys from the SPENCERS_SCHEMA_MAP.
"""

from __future__ import annotations

from app.schemas.segment_rules import (
    AttributeCondition,
    ConditionGroup,
    EventCondition,
    LogicalOperator,
    SegmentDefinition,
    TimeWindow,
    TimeWindowType,
)


def _grp(op: str, *conds) -> ConditionGroup:
    """Shorthand to build a condition group."""
    return ConditionGroup(
        logical_operator=LogicalOperator(op),
        conditions=list(conds),
    )


def _attr(key: str, operator: str, value, second_value=None, negate=False) -> AttributeCondition:
    """Shorthand to build an attribute condition."""
    return AttributeCondition(
        attribute_key=key,
        operator=operator,
        value=value,
        second_value=second_value,
        negate=negate,
    )


def _evt(name: str, operator: str, days: int | None = None, count: int | None = None) -> EventCondition:
    """Shorthand to build an event condition."""
    tw = TimeWindow(type=TimeWindowType.LAST_N_DAYS, days=days) if days else TimeWindow(type=TimeWindowType.ALL_TIME)
    return EventCondition(
        event_name=name,
        operator=operator,
        time_window=tw,
        count_value=count,
    )


# =============================================================================
# TEMPLATE DATACLASS
# =============================================================================

class SegmentTemplate:
    def __init__(
        self,
        id: str,
        name: str,
        description: str,
        category: str,
        applicable_brands: list[str] | None,
        business_function: str,
        rules: SegmentDefinition,
    ):
        self.id = id
        self.name = name
        self.description = description
        self.category = category
        self.applicable_brands = applicable_brands  # None = all brands
        self.business_function = business_function  # marketing, product, merch, cx, finance
        self.rules = rules

    def to_dict(self) -> dict:
        return {
            "id": self.id,
            "name": self.name,
            "description": self.description,
            "category": self.category,
            "applicable_brands": self.applicable_brands,
            "business_function": self.business_function,
            "rules": self.rules.model_dump(),
        }


# =============================================================================
# SPENCER'S-SPECIFIC TEMPLATES
# =============================================================================

TEMPLATES: list[SegmentTemplate] = [

    # 1. STAR Customers
    SegmentTemplate(
        id="tmpl-spencers-star-customers",
        name="STAR Customers",
        description="Customers classified as STAR in the L2 lifecycle segment — top-tier loyalty and value",
        category="lifecycle",
        applicable_brands=["spencers"],
        business_function="marketing",
        rules=SegmentDefinition(root=_grp("and",
            _attr("lifecycle.l2_segment", "equals", "STAR"),
        )),
    ),

    # 2. High Value High Frequency
    SegmentTemplate(
        id="tmpl-spencers-hvhf",
        name="High Value High Frequency",
        description="Customers in the HVHF L1 segment — high spend and high visit frequency",
        category="lifecycle",
        applicable_brands=["spencers"],
        business_function="marketing",
        rules=SegmentDefinition(root=_grp("and",
            _attr("lifecycle.l1_segment", "equals", "HVHF"),
        )),
    ),

    # 3. At Risk - High Spenders
    SegmentTemplate(
        id="tmpl-spencers-at-risk-high-spenders",
        name="At Risk — High Spenders",
        description="Customers in the At Risk lifecycle stage who have spent more than 10,000 — priority win-back targets",
        category="lifecycle",
        applicable_brands=["spencers"],
        business_function="marketing",
        rules=SegmentDefinition(root=_grp("and",
            _attr("lifecycle.lifecycle_stage", "equals", "At Risk"),
            _attr("txn.total_spend", "greater_than", 10000),
        )),
    ),

    # 4. Churned Customers
    SegmentTemplate(
        id="tmpl-spencers-churned",
        name="Churned Customers",
        description="Customers whose lifecycle stage is Churned — re-engagement and win-back campaigns",
        category="lifecycle",
        applicable_brands=["spencers"],
        business_function="marketing",
        rules=SegmentDefinition(root=_grp("and",
            _attr("lifecycle.lifecycle_stage", "equals", "Churned"),
        )),
    ),

    # 5. New First-Time Buyers
    SegmentTemplate(
        id="tmpl-spencers-new-first-time-buyers",
        name="New First-Time Buyers",
        description="First-time buyers with a purchase in the last 30 days — onboarding and nurture targets",
        category="lifecycle",
        applicable_brands=["spencers"],
        business_function="marketing",
        rules=SegmentDefinition(root=_grp("and",
            _attr("lifecycle.is_first_time_buyer", "is_true", True),
            _attr("temporal.recency_days", "less_than", 30),
        )),
    ),

    # 6. Weekend Shoppers
    SegmentTemplate(
        id="tmpl-spencers-weekend-shoppers",
        name="Weekend Shoppers",
        description="Customers with more than 3 weekend bills — target for weekend promotions and stock-up offers",
        category="transactional",
        applicable_brands=["spencers"],
        business_function="marketing",
        rules=SegmentDefinition(root=_grp("and",
            _attr("txn.weekend_bill_count", "greater_than", 3),
        )),
    ),

    # 7. Promo Lovers
    SegmentTemplate(
        id="tmpl-spencers-promo-lovers",
        name="Promo Lovers",
        description="Customers with more than 5 promotional bills — high affinity for deals and discounts",
        category="transactional",
        applicable_brands=["spencers"],
        business_function="merch",
        rules=SegmentDefinition(root=_grp("and",
            _attr("txn.promo_bill_count", "greater_than", 5),
        )),
    ),

    # 8. Omni-Channel Customers
    SegmentTemplate(
        id="tmpl-spencers-omni-channel",
        name="Omni-Channel Customers",
        description="Customers present across both online and offline channels — unified experience targets",
        category="channel",
        applicable_brands=["spencers"],
        business_function="marketing",
        rules=SegmentDefinition(root=_grp("and",
            _attr("channel.channel_presence", "equals", "Omni"),
        )),
    ),

    # 9. Online Only
    SegmentTemplate(
        id="tmpl-spencers-online-only",
        name="Online Only",
        description="Customers who shop exclusively online — drive-to-store or digital-exclusive campaigns",
        category="channel",
        applicable_brands=["spencers"],
        business_function="marketing",
        rules=SegmentDefinition(root=_grp("and",
            _attr("channel.channel_presence", "equals", "Online"),
        )),
    ),

    # 10. Top Spend Decile
    SegmentTemplate(
        id="tmpl-spencers-top-spend-decile",
        name="Top Spend Decile",
        description="Customers in spend decile 9 or 10 — highest spending tier for VIP treatment",
        category="transactional",
        applicable_brands=["spencers"],
        business_function="marketing",
        rules=SegmentDefinition(root=_grp("and",
            _attr("decile.spend_decile", "greater_than_or_equal", 9),
        )),
    ),

    # 11. Deep Lapsed Win-Back
    SegmentTemplate(
        id="tmpl-spencers-deep-lapsed-winback",
        name="Deep Lapsed Win-Back",
        description="Customers classified as Deep Lapsed or LAPSER in L2 segment — aggressive win-back targeting",
        category="lifecycle",
        applicable_brands=["spencers"],
        business_function="marketing",
        rules=SegmentDefinition(root=_grp("and",
            _attr("lifecycle.l2_segment", "in_list", ["Deep Lapsed", "LAPSER"]),
        )),
    ),

    # 12. Multi-Store Shoppers
    SegmentTemplate(
        id="tmpl-spencers-multi-store-shoppers",
        name="Multi-Store Shoppers",
        description="Customers who have shopped at 3 or more distinct stores — high mobility and brand loyalty",
        category="transactional",
        applicable_brands=["spencers"],
        business_function="marketing",
        rules=SegmentDefinition(root=_grp("and",
            _attr("txn.distinct_store_count", "greater_than_or_equal", 3),
        )),
    ),

    # 13. SMS Marketable
    SegmentTemplate(
        id="tmpl-spencers-sms-marketable",
        name="SMS Marketable",
        description="Customers who accept SMS marketing and are not on DND — reachable via SMS campaigns",
        category="consent",
        applicable_brands=["spencers"],
        business_function="marketing",
        rules=SegmentDefinition(root=_grp("and",
            _attr("consent.accepts_sms_marketing", "not_equals", "No"),
            _attr("consent.dnd", "not_equals", "Y"),
        )),
    ),

    # 14. Repeat Buyers Trending Up
    SegmentTemplate(
        id="tmpl-spencers-repeat-buyers-trending-up",
        name="Repeat Buyers Trending Up",
        description="Repeat buyers in spend decile 7 or above — growing value customers to nurture",
        category="lifecycle",
        applicable_brands=["spencers"],
        business_function="marketing",
        rules=SegmentDefinition(root=_grp("and",
            _attr("lifecycle.is_repeat_buyer", "is_true", True),
            _attr("decile.spend_decile", "greater_than_or_equal", 7),
        )),
    ),

    # 15. CIH Registered No Purchase
    SegmentTemplate(
        id="tmpl-spencers-cih-registered-no-purchase",
        name="CIH Registered No Purchase",
        description="Customers registered via CIH with no transaction history — activation targets",
        category="lifecycle",
        applicable_brands=["spencers"],
        business_function="marketing",
        rules=SegmentDefinition(root=_grp("and",
            _attr("identity.has_transactions", "is_false", False),
            _attr("identity.primary_source", "equals", "CIH"),
        )),
    ),

]


# =============================================================================
# HELPERS
# =============================================================================

def get_templates_for_brand(brand_code: str) -> list[SegmentTemplate]:
    """Return templates applicable to a specific brand."""
    return [
        t for t in TEMPLATES
        if t.applicable_brands is None or brand_code in t.applicable_brands
    ]


def get_templates_by_category(category: str) -> list[SegmentTemplate]:
    """Return templates in a specific category."""
    return [t for t in TEMPLATES if t.category == category]


def get_templates_by_function(function: str) -> list[SegmentTemplate]:
    """Return templates for a specific business function."""
    return [t for t in TEMPLATES if t.business_function == function]


def get_template_by_id(template_id: str) -> SegmentTemplate | None:
    """Fetch a single template by ID."""
    for t in TEMPLATES:
        if t.id == template_id:
            return t
    return None

"""
Pre-Built Segment Templates
=============================

Business-ready segment definitions for each brand.
Users can load these as starting points and customize.

Organized by:
- Cross-brand templates (applicable to all)
- Spencers-specific (retail hypermarket + offline store)
- Nature's Basket-specific (premium grocery + offline)
- FMCG-specific (D2C + B2B)
- Power CESC-specific (utility billing + CX)
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
# CROSS-BRAND TEMPLATES (all brands)
# =============================================================================

TEMPLATES: list[SegmentTemplate] = [

    # ─── Lifecycle ───────────────────────────────────────────────────────
    SegmentTemplate(
        id="tmpl-high-value-at-risk",
        name="High-Value At-Risk Customers",
        description="Top-tier customers (LTV > 50K) showing churn signals: no purchase in 60+ days and high churn risk",
        category="lifecycle",
        applicable_brands=None,
        business_function="marketing",
        rules=SegmentDefinition(root=_grp("and",
            _attr("txn.total_revenue", "greater_than", 50000),
            _attr("txn.days_since_last_purchase", "greater_than", 60),
            _attr("predict.churn_risk_tier", "in_list", ["high", "critical"]),
        )),
    ),

    SegmentTemplate(
        id="tmpl-new-first-time-buyers",
        name="First-Time Buyers (Last 30 Days)",
        description="Customers who made their first-ever purchase in the last 30 days",
        category="lifecycle",
        applicable_brands=None,
        business_function="marketing",
        rules=SegmentDefinition(root=_grp("and",
            _attr("lifecycle.is_first_time_buyer", "is_true", True),
            _attr("txn.first_purchase_date", "in_last_n_days", 30),
        )),
    ),

    SegmentTemplate(
        id="tmpl-dormant-reactivation",
        name="Dormant — Re-engagement Target",
        description="Customers with no purchase or session in 90+ days, previously active",
        category="lifecycle",
        applicable_brands=None,
        business_function="marketing",
        rules=SegmentDefinition(root=_grp("and",
            _attr("txn.days_since_last_purchase", "greater_than", 90),
            _attr("behavior.days_since_last_session", "greater_than", 90),
            _attr("txn.total_orders", "greater_than", 2),
        )),
    ),

    SegmentTemplate(
        id="tmpl-champions",
        name="Champions (RFM)",
        description="Top RFM segment: high recency, frequency, and monetary scores",
        category="lifecycle",
        applicable_brands=None,
        business_function="marketing",
        rules=SegmentDefinition(root=_grp("and",
            _attr("lifecycle.rfm_segment", "equals", "champions"),
        )),
    ),

    SegmentTemplate(
        id="tmpl-repeat-buyers-trending-up",
        name="Repeat Buyers — Spend Trending Up",
        description="Repeat purchasers whose 90d revenue exceeds their 365d average",
        category="lifecycle",
        applicable_brands=None,
        business_function="marketing",
        rules=SegmentDefinition(root=_grp("and",
            _attr("lifecycle.is_repeat_buyer", "is_true", True),
            _attr("txn.revenue_90d", "greater_than", 0),
            _attr("txn.purchase_frequency_90d", "greater_than_or_equal", 3),
        )),
    ),

    # ─── Engagement ──────────────────────────────────────────────────────
    SegmentTemplate(
        id="tmpl-email-unengaged",
        name="Email Unengaged (90 Days)",
        description="Opted-in to email but no opens/clicks in 90 days — suppress or re-engage",
        category="engagement",
        applicable_brands=None,
        business_function="marketing",
        rules=SegmentDefinition(root=_grp("and",
            _attr("consent.email_opt_in", "is_true", True),
            _attr("engage.is_email_engaged", "is_false", True),
            _attr("engage.total_campaigns_received", "greater_than", 5),
        )),
    ),

    SegmentTemplate(
        id="tmpl-high-engagement-low-purchase",
        name="High Engagement, Low Purchase",
        description="Highly engaged (browsing, email clicks) but low conversion — nurture target",
        category="engagement",
        applicable_brands=None,
        business_function="marketing",
        rules=SegmentDefinition(root=_grp("and",
            _attr("behavior.sessions_30d", "greater_than", 5),
            _attr("engage.engagement_score", "greater_than", 60) if False else _attr("behavior.total_product_views", "greater_than", 20),
            _attr("txn.purchase_frequency_30d", "less_than_or_equal", 0),
        )),
    ),

    SegmentTemplate(
        id="tmpl-cart-abandoners",
        name="Cart Abandoners (7 Days)",
        description="Added items to cart in last 7 days but did not purchase",
        category="engagement",
        applicable_brands=None,
        business_function="marketing",
        rules=SegmentDefinition(root=_grp("and",
            _evt("add_to_cart", "has_performed", days=7),
            _evt("purchase", "has_not_performed", days=7),
        )),
    ),

    # ─── Cross-brand ─────────────────────────────────────────────────────
    SegmentTemplate(
        id="tmpl-cross-brand-shoppers",
        name="Cross-Brand Customers",
        description="Customers identified across 2+ brands — cross-sell opportunity",
        category="cross_brand",
        applicable_brands=None,
        business_function="marketing",
        rules=SegmentDefinition(root=_grp("and",
            _attr("identity.is_cross_brand_customer", "is_true", True),
            _attr("consent.cross_brand_opt_in", "is_true", True),
        )),
    ),

    SegmentTemplate(
        id="tmpl-loyalty-platinum-at-risk",
        name="Loyalty Platinum — At Risk",
        description="Platinum/Gold tier loyalty members with declining engagement",
        category="loyalty",
        applicable_brands=None,
        business_function="marketing",
        rules=SegmentDefinition(root=_grp("and",
            _attr("loyalty.tier", "in_list", ["platinum", "gold"]),
            _attr("txn.days_since_last_purchase", "greater_than", 45),
            _attr("predict.churn_probability", "greater_than", 0.5),
        )),
    ),

    SegmentTemplate(
        id="tmpl-discount-seekers",
        name="Discount-Sensitive Customers",
        description="High discount sensitivity — primarily purchases on promotions/coupons",
        category="transactional",
        applicable_brands=None,
        business_function="merch",
        rules=SegmentDefinition(root=_grp("and",
            _attr("txn.discount_sensitivity", "equals", "high"),
            _attr("txn.coupon_usage_count", "greater_than", 3),
        )),
    ),

    # ═════════════════════════════════════════════════════════════════════
    # SPENCERS-SPECIFIC TEMPLATES
    # ═════════════════════════════════════════════════════════════════════

    SegmentTemplate(
        id="tmpl-spencers-weekend-stock-up",
        name="Weekend Stock-Up Shoppers",
        description="Customers who primarily visit on weekends with large baskets (>15 items, >₹2000)",
        category="offline_store",
        applicable_brands=["spencers"],
        business_function="merch",
        rules=SegmentDefinition(root=_grp("and",
            _attr("store.is_weekend_shopper", "is_true", True),
            _attr("basket.avg_items_per_bill", "greater_than", 15),
            _attr("basket.avg_basket_value", "greater_than", 2000),
        )),
    ),

    SegmentTemplate(
        id="tmpl-spencers-store-only-digital-opportunity",
        name="Store-Only — Digital Migration Opportunity",
        description="Customers who only shop in-store, have no app, and are email opted-in — target for app downloads",
        category="offline_store",
        applicable_brands=["spencers"],
        business_function="product",
        rules=SegmentDefinition(root=_grp("and",
            _attr("store.is_store_only_customer", "is_true", True),
            _attr("device.has_app_installed", "is_false", True),
            _attr("consent.email_opt_in", "is_true", True),
            _attr("store.store_visits_90d", "greater_than_or_equal", 3),
        )),
    ),

    SegmentTemplate(
        id="tmpl-spencers-private-label-promoters",
        name="Private Label Champions",
        description="Customers with >30% basket share in private label — advocate for store brands",
        category="basket",
        applicable_brands=["spencers"],
        business_function="merch",
        rules=SegmentDefinition(root=_grp("and",
            _attr("basket.private_label_pct", "greater_than", 30),
            _attr("txn.total_orders", "greater_than", 5),
        )),
    ),

    SegmentTemplate(
        id="tmpl-spencers-fresh-focused",
        name="Fresh-Focused Shoppers",
        description="Customers where >50% of basket value is fresh produce — target for quality/freshness messaging",
        category="basket",
        applicable_brands=["spencers"],
        business_function="merch",
        rules=SegmentDefinition(root=_grp("and",
            _attr("basket.fresh_produce_pct", "greater_than", 50),
            _attr("store.store_visits_30d", "greater_than_or_equal", 2),
        )),
    ),

    SegmentTemplate(
        id="tmpl-spencers-multi-store-high-value",
        name="Multi-Store High-Value Shoppers",
        description="Shops at 3+ stores with high lifetime value — potential for store-specific offers",
        category="offline_store",
        applicable_brands=["spencers"],
        business_function="marketing",
        rules=SegmentDefinition(root=_grp("and",
            _attr("store.distinct_stores_visited", "greater_than_or_equal", 3),
            _attr("txn.total_revenue", "greater_than", 100000),
            _attr("store.store_visits_90d", "greater_than_or_equal", 4),
        )),
    ),

    SegmentTemplate(
        id="tmpl-spencers-baby-parents",
        name="Parents with Babies",
        description="Customers purchasing baby care products — lifecycle marketing for young families",
        category="basket",
        applicable_brands=["spencers"],
        business_function="marketing",
        rules=SegmentDefinition(root=_grp("and",
            _attr("basket.has_baby_products", "is_true", True),
            _attr("txn.purchase_frequency_90d", "greater_than_or_equal", 2),
        )),
    ),

    SegmentTemplate(
        id="tmpl-spencers-self-checkout-adopters",
        name="Self-Checkout Adopters",
        description="Customers who use self-checkout >60% of the time — tech-savvy segment",
        category="offline_store",
        applicable_brands=["spencers"],
        business_function="product",
        rules=SegmentDefinition(root=_grp("and",
            _attr("store.self_checkout_pct", "greater_than", 60),
            _attr("store.total_store_visits", "greater_than", 5),
        )),
    ),

    # ═════════════════════════════════════════════════════════════════════
    # NATURE'S BASKET-SPECIFIC TEMPLATES
    # ═════════════════════════════════════════════════════════════════════

    SegmentTemplate(
        id="tmpl-nbl-gourmet-explorers",
        name="Gourmet Explorers",
        description="High category breadth, premium product affinity, frequent new SKU trials",
        category="basket",
        applicable_brands=["natures_basket"],
        business_function="merch",
        rules=SegmentDefinition(root=_grp("and",
            _attr("basket.premium_products_pct", "greater_than", 40),
            _attr("basket.new_sku_trial_rate_90d", "greater_than", 25),
            _attr("product.category_breadth", "greater_than", 8),
        )),
    ),

    SegmentTemplate(
        id="tmpl-nbl-organic-health-conscious",
        name="Organic & Health-Conscious",
        description="Organic buyers who also purchase health/wellness products",
        category="basket",
        applicable_brands=["natures_basket"],
        business_function="merch",
        rules=SegmentDefinition(root=_grp("and",
            _attr("product.organic_buyer", "is_true", True),
            _attr("basket.has_health_wellness", "is_true", True),
        )),
    ),

    SegmentTemplate(
        id="tmpl-nbl-imported-product-buyers",
        name="Imported Product Buyers",
        description="Customers with affinity for imported/international products — premium segment",
        category="basket",
        applicable_brands=["natures_basket"],
        business_function="merch",
        rules=SegmentDefinition(root=_grp("and",
            _attr("product.price_sensitivity", "equals", "premium"),
            _attr("basket.avg_basket_value", "greater_than", 3000),
        )),
    ),

    SegmentTemplate(
        id="tmpl-nbl-pet-parents",
        name="Pet Parents",
        description="Regular pet food/care purchasers — subscription opportunity",
        category="basket",
        applicable_brands=["natures_basket"],
        business_function="marketing",
        rules=SegmentDefinition(root=_grp("and",
            _attr("basket.has_pet_products", "is_true", True),
            _attr("txn.purchase_frequency_90d", "greater_than_or_equal", 3),
        )),
    ),

    # ═════════════════════════════════════════════════════════════════════
    # FMCG-SPECIFIC TEMPLATES
    # ═════════════════════════════════════════════════════════════════════

    SegmentTemplate(
        id="tmpl-fmcg-high-value-distributors",
        name="High-Value Distributors",
        description="Top-tier B2B distributors by annual contract value",
        category="b2b",
        applicable_brands=["fmcg"],
        business_function="marketing",
        rules=SegmentDefinition(root=_grp("and",
            _attr("b2b.account_type", "equals", "distributor"),
            _attr("b2b.annual_contract_value", "greater_than", 5000000),
        )),
    ),

    SegmentTemplate(
        id="tmpl-fmcg-d2c-repeat-subscribers",
        name="D2C Repeat Subscribers",
        description="Direct-to-consumer repeat buyers with 3+ orders in 90 days",
        category="lifecycle",
        applicable_brands=["fmcg"],
        business_function="marketing",
        rules=SegmentDefinition(root=_grp("and",
            _attr("lifecycle.is_repeat_buyer", "is_true", True),
            _attr("txn.purchase_frequency_90d", "greater_than_or_equal", 3),
            _attr("txn.preferred_channel", "equals", "online"),
        )),
    ),

    SegmentTemplate(
        id="tmpl-fmcg-overdue-b2b-accounts",
        name="B2B Accounts with Overdue Payments",
        description="B2B accounts with outstanding amounts beyond payment terms",
        category="b2b",
        applicable_brands=["fmcg"],
        business_function="finance",
        rules=SegmentDefinition(root=_grp("and",
            _attr("b2b.outstanding_amount", "greater_than", 100000),
            _attr("b2b.payment_terms", "in_list", ["net_30", "net_60"]),
        )),
    ),

    # ═════════════════════════════════════════════════════════════════════
    # POWER CESC-SPECIFIC TEMPLATES
    # ═════════════════════════════════════════════════════════════════════

    SegmentTemplate(
        id="tmpl-cesc-chronic-late-payers",
        name="Chronic Late Payers",
        description="Customers with >3 late payments in 12 months and poor payment reliability",
        category="utility_billing",
        applicable_brands=["power_cesc"],
        business_function="finance",
        rules=SegmentDefinition(root=_grp("and",
            _attr("billing.late_payment_count_12m", "greater_than", 3),
            _attr("billing.payment_reliability_tier", "in_list", ["poor", "defaulter"]),
        )),
    ),

    SegmentTemplate(
        id="tmpl-cesc-high-consumption-residential",
        name="High-Consumption Residential",
        description="Residential customers in top consumption slab — energy efficiency program target",
        category="utility_billing",
        applicable_brands=["power_cesc"],
        business_function="marketing",
        rules=SegmentDefinition(root=_grp("and",
            _attr("utility.connection_type", "equals", "residential"),
            _attr("billing.consumption_slab", "equals", "500+_units"),
            _attr("billing.consumption_trend_6m", "in_list", ["increasing", "stable"]),
        )),
    ),

    SegmentTemplate(
        id="tmpl-cesc-solar-ev-prospects",
        name="Solar & EV Prospects",
        description="High-consumption residential customers without solar/EV — upsell green energy",
        category="utility_billing",
        applicable_brands=["power_cesc"],
        business_function="marketing",
        rules=SegmentDefinition(root=_grp("and",
            _attr("utility.connection_type", "equals", "residential"),
            _attr("utility.avg_monthly_consumption_kwh", "greater_than", 400),
            _attr("utility.has_solar", "is_false", True),
            _attr("utility.has_ev_charger", "is_false", True),
            _attr("billing.payment_reliability_tier", "in_list", ["excellent", "good"]),
        )),
    ),

    SegmentTemplate(
        id="tmpl-cesc-repeat-complainers",
        name="Repeat Complainers — CX At-Risk",
        description="Customers with 5+ complaints, high escalation rate, and declining CX score",
        category="utility_complaint",
        applicable_brands=["power_cesc"],
        business_function="cx",
        rules=SegmentDefinition(root=_grp("and",
            _attr("complaint.complaints_12m", "greater_than", 5),
            _attr("complaint.escalation_rate", "greater_than", 25),
            _attr("cx.at_risk_cx", "is_true", True),
        )),
    ),

    SegmentTemplate(
        id="tmpl-cesc-digital-non-adopters",
        name="Digital Non-Adopters",
        description="Customers with no portal/app usage, paying via cash counter — migration target",
        category="digital_adoption",
        applicable_brands=["power_cesc"],
        business_function="product",
        rules=SegmentDefinition(root=_grp("and",
            _attr("digital.digital_adoption_tier", "in_list", ["offline_only", "offline_preferred"]),
            _attr("digital.has_portal_account", "is_false", True),
            _attr("digital.has_app_installed", "is_false", True),
            _attr("billing.preferred_payment_mode", "in_list", ["cash_counter", "cheque"]),
        )),
    ),

    SegmentTemplate(
        id="tmpl-cesc-smart-meter-candidates",
        name="Smart Meter Upgrade Candidates",
        description="Customers on conventional meters with billing disputes — smart meter would improve accuracy",
        category="utility_billing",
        applicable_brands=["power_cesc"],
        business_function="cx",
        rules=SegmentDefinition(root=_grp("and",
            _attr("billing.has_smart_meter", "is_false", True),
            _attr("billing.meter_type", "equals", "conventional"),
            _attr("complaint.billing_dispute_count", "greater_than", 1),
        )),
    ),

    SegmentTemplate(
        id="tmpl-cesc-autopay-conversion",
        name="AutoPay Conversion Targets",
        description="Regular digital payers not yet on autopay — easy conversion for payment stability",
        category="digital_adoption",
        applicable_brands=["power_cesc"],
        business_function="product",
        rules=SegmentDefinition(root=_grp("and",
            _attr("billing.is_auto_pay_enrolled", "is_false", True),
            _attr("digital.online_payment_pct", "greater_than", 70),
            _attr("billing.on_time_payment_rate", "greater_than", 80),
        )),
    ),

    SegmentTemplate(
        id="tmpl-cesc-outage-impacted-high-value",
        name="Outage-Impacted High-Value Customers",
        description="High-consumption customers who experienced significant outages — proactive CX recovery",
        category="customer_experience",
        applicable_brands=["power_cesc"],
        business_function="cx",
        rules=SegmentDefinition(root=_grp("and",
            _attr("cx.outage_count_12m", "greater_than", 5),
            _attr("cx.outage_impact_hours_12m", "greater_than", 24),
            _attr("utility.avg_monthly_bill", "greater_than", 5000),
            _attr("cx.cx_trend", "equals", "declining"),
        )),
    ),

    SegmentTemplate(
        id="tmpl-cesc-billing-accuracy-issues",
        name="Billing Accuracy Concerns",
        description="Customers with low billing accuracy score and multiple disputes — process improvement",
        category="customer_experience",
        applicable_brands=["power_cesc"],
        business_function="cx",
        rules=SegmentDefinition(root=_grp("and",
            _attr("cx.billing_accuracy_score", "less_than", 50),
            _attr("complaint.billing_dispute_count", "greater_than", 2),
        )),
    ),

    SegmentTemplate(
        id="tmpl-cesc-whatsapp-engaged",
        name="WhatsApp-Engaged Customers",
        description="Customers actively using WhatsApp bot — channel optimization candidates",
        category="digital_adoption",
        applicable_brands=["power_cesc"],
        business_function="product",
        rules=SegmentDefinition(root=_grp("and",
            _attr("digital.whatsapp_bot_interactions", "greater_than", 3),
            _attr("consent.whatsapp_opt_in", "is_true", True),
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

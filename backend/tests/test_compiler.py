"""
Tests for the Athena SQL compiler.
Verifies that segment rule trees compile into correct SQL.
"""

import sys
import os

# Allow imports from the app package
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from app.schemas.segment_rules import (
    AttributeCondition,
    ConditionGroup,
    CrossBrandCondition,
    EventCondition,
    EventPropertyFilter,
    LogicalOperator,
    SegmentDefinition,
    TimeWindow,
    TimeWindowType,
)
from app.services.query_engine.compiler import AthenaCompiler


def get_compiler(brand: str = "spencers") -> AthenaCompiler:
    return AthenaCompiler(
        brand_code=brand,
        database=f"{brand}_gold",
    )


def test_simple_attribute_condition():
    """Single attribute condition: revenue > 50000."""
    definition = SegmentDefinition(
        root=ConditionGroup(
            logical_operator=LogicalOperator.AND,
            conditions=[
                AttributeCondition(
                    attribute_key="txn.total_revenue",
                    operator="greater_than",
                    value=50000,
                ),
            ],
        )
    )
    sql = get_compiler().compile(definition)
    assert "c.total_revenue > 50000" in sql
    assert "spencers_gold.customers" in sql
    print("PASS: simple_attribute_condition")
    print(sql)
    print()


def test_and_group():
    """AND group: revenue > 50000 AND days_since_last_purchase > 60."""
    definition = SegmentDefinition(
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
            ],
        )
    )
    sql = get_compiler().compile(definition)
    assert "AND" in sql
    assert "c.total_revenue > 50000" in sql
    assert "c.days_since_last_purchase > 60" in sql
    print("PASS: and_group")
    print(sql)
    print()


def test_or_group():
    """OR group."""
    definition = SegmentDefinition(
        root=ConditionGroup(
            logical_operator=LogicalOperator.OR,
            conditions=[
                AttributeCondition(
                    attribute_key="geo.city",
                    operator="equals",
                    value="Kolkata",
                ),
                AttributeCondition(
                    attribute_key="geo.city",
                    operator="equals",
                    value="Mumbai",
                ),
            ],
        )
    )
    sql = get_compiler().compile(definition)
    assert "OR" in sql
    assert "'Kolkata'" in sql
    assert "'Mumbai'" in sql
    print("PASS: or_group")
    print(sql)
    print()


def test_nested_groups():
    """Nested groups: (A AND B) AND (C OR D)."""
    definition = SegmentDefinition(
        root=ConditionGroup(
            logical_operator=LogicalOperator.AND,
            conditions=[
                AttributeCondition(
                    attribute_key="txn.total_revenue",
                    operator="greater_than",
                    value=10000,
                ),
                ConditionGroup(
                    logical_operator=LogicalOperator.OR,
                    conditions=[
                        AttributeCondition(
                            attribute_key="lifecycle.stage",
                            operator="equals",
                            value="at_risk",
                        ),
                        AttributeCondition(
                            attribute_key="lifecycle.stage",
                            operator="equals",
                            value="lapsing",
                        ),
                    ],
                ),
            ],
        )
    )
    sql = get_compiler().compile(definition)
    assert "AND" in sql
    assert "OR" in sql
    assert "'at_risk'" in sql
    assert "'lapsing'" in sql
    print("PASS: nested_groups")
    print(sql)
    print()


def test_in_list_operator():
    """IN list operator: churn_risk_tier IN ('high', 'critical')."""
    definition = SegmentDefinition(
        root=ConditionGroup(
            logical_operator=LogicalOperator.AND,
            conditions=[
                AttributeCondition(
                    attribute_key="predict.churn_risk_tier",
                    operator="in_list",
                    value=["high", "critical"],
                ),
            ],
        )
    )
    sql = get_compiler().compile(definition)
    assert "IN ('high', 'critical')" in sql
    print("PASS: in_list_operator")
    print(sql)
    print()


def test_date_operator_in_last_n_days():
    """Date operator: last_purchase_date in last 30 days."""
    definition = SegmentDefinition(
        root=ConditionGroup(
            logical_operator=LogicalOperator.AND,
            conditions=[
                AttributeCondition(
                    attribute_key="txn.last_purchase_date",
                    operator="in_last_n_days",
                    value=30,
                ),
            ],
        )
    )
    sql = get_compiler().compile(definition)
    assert "DATE_ADD('day', -30, CURRENT_DATE)" in sql
    print("PASS: date_in_last_n_days")
    print(sql)
    print()


def test_event_condition_has_performed():
    """Event condition: has performed 'purchase' in last 30 days."""
    definition = SegmentDefinition(
        root=ConditionGroup(
            logical_operator=LogicalOperator.AND,
            conditions=[
                EventCondition(
                    event_name="purchase",
                    operator="has_performed",
                    time_window=TimeWindow(type=TimeWindowType.LAST_N_DAYS, days=30),
                ),
            ],
        )
    )
    sql = get_compiler().compile(definition)
    assert "WITH" in sql
    assert "'purchase'" in sql
    assert "INNER JOIN" in sql
    print("PASS: event_has_performed")
    print(sql)
    print()


def test_event_condition_with_property_filter():
    """Event with property filter: product_viewed where category = 'electronics'."""
    definition = SegmentDefinition(
        root=ConditionGroup(
            logical_operator=LogicalOperator.AND,
            conditions=[
                EventCondition(
                    event_name="product_viewed",
                    operator="performed_count_greater_than",
                    count_value=5,
                    time_window=TimeWindow(type=TimeWindowType.LAST_N_DAYS, days=7),
                    event_property_filters=[
                        EventPropertyFilter(
                            property_name="category",
                            operator="equals",
                            value="electronics",
                        )
                    ],
                ),
            ],
        )
    )
    sql = get_compiler().compile(definition)
    assert "'product_viewed'" in sql
    assert "'electronics'" in sql
    print("PASS: event_with_property_filter")
    print(sql)
    print()


def test_count_query():
    """Count query wraps the segment query."""
    definition = SegmentDefinition(
        root=ConditionGroup(
            logical_operator=LogicalOperator.AND,
            conditions=[
                AttributeCondition(
                    attribute_key="loyalty.is_member",
                    operator="is_true",
                    value=True,
                ),
            ],
        )
    )
    sql = get_compiler().compile_count(definition)
    assert "COUNT(*)" in sql
    assert "audience_count" in sql
    print("PASS: count_query")
    print(sql)
    print()


def test_preview_query():
    """Preview query returns profile fields with LIMIT."""
    definition = SegmentDefinition(
        root=ConditionGroup(
            logical_operator=LogicalOperator.AND,
            conditions=[
                AttributeCondition(
                    attribute_key="demographic.age",
                    operator="between",
                    value=25,
                    second_value=34,
                ),
            ],
        )
    )
    sql = get_compiler().compile_preview(definition, limit=50)
    assert "email" in sql
    assert "first_name" in sql
    assert "LIMIT 50" in sql
    print("PASS: preview_query")
    print(sql)
    print()


def test_boolean_operator():
    """Boolean: is_cross_brand_customer = TRUE."""
    definition = SegmentDefinition(
        root=ConditionGroup(
            logical_operator=LogicalOperator.AND,
            conditions=[
                AttributeCondition(
                    attribute_key="identity.is_cross_brand_customer",
                    operator="is_true",
                    value=True,
                ),
            ],
        )
    )
    sql = get_compiler().compile(definition)
    assert "= TRUE" in sql
    print("PASS: boolean_operator")
    print(sql)
    print()


def test_different_brand():
    """Compiles for a different brand (fmcg)."""
    definition = SegmentDefinition(
        root=ConditionGroup(
            logical_operator=LogicalOperator.AND,
            conditions=[
                AttributeCondition(
                    attribute_key="b2b.company_size",
                    operator="equals",
                    value="enterprise",
                ),
            ],
        )
    )
    sql = get_compiler("fmcg").compile(definition)
    assert "fmcg_gold.customers" in sql
    assert "'enterprise'" in sql
    print("PASS: different_brand")
    print(sql)
    print()


def test_basket_avg_items_per_bill():
    """Basket attribute: avg items per bill > 10 for Spencers."""
    definition = SegmentDefinition(
        root=ConditionGroup(
            logical_operator=LogicalOperator.AND,
            conditions=[
                AttributeCondition(
                    attribute_key="basket.avg_items_per_bill",
                    operator="greater_than",
                    value=10,
                ),
            ],
        )
    )
    sql = get_compiler("spencers").compile(definition)
    assert "WITH" in sql
    assert "basket_" in sql
    assert "AVG(t.line_item_count)" in sql
    assert "LEFT JOIN" in sql
    assert "spencers_gold.transactions" in sql
    print("PASS: basket_avg_items_per_bill")
    print(sql)
    print()


def test_basket_fresh_produce_with_line_items():
    """Basket attribute that requires JOIN to line_items table."""
    definition = SegmentDefinition(
        root=ConditionGroup(
            logical_operator=LogicalOperator.AND,
            conditions=[
                AttributeCondition(
                    attribute_key="basket.fresh_produce_pct",
                    operator="greater_than",
                    value=40,
                ),
            ],
        )
    )
    sql = get_compiler("natures_basket").compile(definition)
    assert "line_items" in sql
    assert "fresh_produce" in sql
    assert "natures_basket_gold" in sql
    print("PASS: basket_fresh_produce_with_line_items")
    print(sql)
    print()


def test_store_visit_attributes():
    """Store visit attributes: weekend shopper who visits > 3 stores."""
    definition = SegmentDefinition(
        root=ConditionGroup(
            logical_operator=LogicalOperator.AND,
            conditions=[
                AttributeCondition(
                    attribute_key="store.is_weekend_shopper",
                    operator="is_true",
                    value=True,
                ),
                AttributeCondition(
                    attribute_key="store.distinct_stores_visited",
                    operator="greater_than",
                    value=3,
                ),
            ],
        )
    )
    sql = get_compiler("spencers").compile(definition)
    assert "DAY_OF_WEEK" in sql
    assert "COUNT(DISTINCT t.store_id)" in sql
    assert "AND" in sql
    print("PASS: store_visit_attributes")
    print(sql)
    print()


def test_billing_late_payments():
    """CESC billing: late payments > 3 in last 12 months."""
    definition = SegmentDefinition(
        root=ConditionGroup(
            logical_operator=LogicalOperator.AND,
            conditions=[
                AttributeCondition(
                    attribute_key="billing.late_payment_count_12m",
                    operator="greater_than",
                    value=3,
                ),
            ],
        )
    )
    sql = get_compiler("power_cesc").compile(definition)
    assert "bill_" in sql
    assert "utility_billing" in sql
    assert "due_date" in sql
    print("PASS: billing_late_payments")
    print(sql)
    print()


def test_complaint_escalation_rate():
    """CESC complaints: escalation rate > 30%."""
    definition = SegmentDefinition(
        root=ConditionGroup(
            logical_operator=LogicalOperator.AND,
            conditions=[
                AttributeCondition(
                    attribute_key="complaint.escalation_rate",
                    operator="greater_than",
                    value=30,
                ),
            ],
        )
    )
    sql = get_compiler("power_cesc").compile(definition)
    assert "cmp_" in sql
    assert "complaints" in sql
    assert "is_escalated" in sql
    print("PASS: complaint_escalation_rate")
    print(sql)
    print()


def test_digital_adoption_portal_logins():
    """CESC digital adoption: portal logins > 5 in 90d."""
    definition = SegmentDefinition(
        root=ConditionGroup(
            logical_operator=LogicalOperator.AND,
            conditions=[
                AttributeCondition(
                    attribute_key="digital.portal_login_count_90d",
                    operator="greater_than",
                    value=5,
                ),
            ],
        )
    )
    sql = get_compiler("power_cesc").compile(definition)
    assert "digi_" in sql
    assert "digital_interactions" in sql
    assert "portal" in sql
    print("PASS: digital_adoption_portal_logins")
    print(sql)
    print()


def test_cx_score_attribute():
    """CX score: customers with CX score < 40 (at risk)."""
    definition = SegmentDefinition(
        root=ConditionGroup(
            logical_operator=LogicalOperator.AND,
            conditions=[
                AttributeCondition(
                    attribute_key="cx.overall_cx_score",
                    operator="less_than",
                    value=40,
                ),
                AttributeCondition(
                    attribute_key="cx.at_risk_cx",
                    operator="is_true",
                    value=True,
                ),
            ],
        )
    )
    sql = get_compiler("power_cesc").compile(definition)
    assert "overall_cx_score" in sql
    assert "at_risk_cx" in sql
    print("PASS: cx_score_attribute")
    print(sql)
    print()


def test_combined_offline_billing_segment():
    """Complex: Spencers high-value weekend shoppers with large baskets."""
    definition = SegmentDefinition(
        root=ConditionGroup(
            logical_operator=LogicalOperator.AND,
            conditions=[
                AttributeCondition(
                    attribute_key="store.is_weekend_shopper",
                    operator="is_true",
                    value=True,
                ),
                AttributeCondition(
                    attribute_key="basket.avg_basket_value",
                    operator="greater_than",
                    value=2000,
                ),
                AttributeCondition(
                    attribute_key="store.store_visits_30d",
                    operator="greater_than_or_equal",
                    value=4,
                ),
                AttributeCondition(
                    attribute_key="loyalty.is_member",
                    operator="is_true",
                    value=True,
                ),
            ],
        )
    )
    sql = get_compiler("spencers").compile(definition)
    # Should have multiple CTEs and JOINs for basket + store attributes
    assert "WITH" in sql
    assert sql.count("LEFT JOIN") >= 3  # 3 basket/store CTEs
    assert "c.is_member = TRUE" in sql  # loyalty is on customer table directly
    print("PASS: combined_offline_billing_segment")
    print(sql)
    print()


if __name__ == "__main__":
    test_simple_attribute_condition()
    test_and_group()
    test_or_group()
    test_nested_groups()
    test_in_list_operator()
    test_date_operator_in_last_n_days()
    test_event_condition_has_performed()
    test_event_condition_with_property_filter()
    test_count_query()
    test_preview_query()
    test_boolean_operator()
    test_different_brand()
    test_basket_avg_items_per_bill()
    test_basket_fresh_produce_with_line_items()
    test_store_visit_attributes()
    test_billing_late_payments()
    test_complaint_escalation_rate()
    test_digital_adoption_portal_logins()
    test_cx_score_attribute()
    test_combined_offline_billing_segment()
    print("=" * 60)
    print("ALL TESTS PASSED")

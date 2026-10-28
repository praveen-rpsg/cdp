"""CESC-only tests for the historical bill accounting-month attribute."""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import pytest

from app.schemas.profile_attributes import ATTRIBUTE_BY_KEY, AttributeDataType
from app.schemas.segment_rules import SegmentDefinition
from app.services.query_engine.pg_compiler import CescGroupViewPgCompiler, PgCompiler


def _rules(condition: dict) -> SegmentDefinition:
    return SegmentDefinition.model_validate({
        "root": {"type": "group", "logical_operator": "and", "conditions": [condition]}
    })


def _month(operator: str, value, second_value=None) -> dict:
    result = {
        "type": "attribute",
        "attribute_key": "cesc.bill_ac_month",
        "operator": operator,
        "value": value,
    }
    if second_value is not None:
        result["second_value"] = second_value
    return result


def test_catalog_declares_cesc_bill_ac_month():
    attr = ATTRIBUTE_BY_KEY["cesc.bill_ac_month"]
    assert attr.label == "Bill AC Month"
    assert attr.data_type == AttributeDataType.STRING
    assert attr.applicable_brands == ["power_cesc"]
    assert attr.source_table == "silver_gold.cesc_customer_monthly"
    assert attr.example_values == []


@pytest.mark.parametrize("value", ["202602", 202602])
def test_bill_ac_month_forces_integer_and_string_values_to_text(value):
    sql = PgCompiler("power_cesc").compile_count(_rules(_month("equals", value)))
    assert "EXISTS (SELECT 1 FROM silver_gold.cesc_customer_monthly cm" in sql
    assert "cm.cesc_entity_id = p.cesc_entity_id" in sql
    assert "cm.bill_ac_month = '202602'" in sql
    assert "cm.bill_ac_month = 202602" not in sql


def test_bill_ac_month_negative_operators_use_not_exists():
    compiler = PgCompiler("power_cesc")
    for operator, value in (("not_equals", "202602"), ("not_in_list", ["202602", "202601"]), ("not_between", "202601")):
        second = "202603" if operator == "not_between" else None
        sql = compiler.compile_count(_rules(_month(operator, value, second)))
        assert "NOT (EXISTS (SELECT 1 FROM silver_gold.cesc_customer_monthly cm" in sql


def test_bill_ac_month_supports_ordered_text_operators():
    sql = PgCompiler("power_cesc").compile_count(
        _rules(_month("between", "202601", "202603"))
    )
    assert "cm.bill_ac_month BETWEEN '202601' AND '202603'" in sql


def test_bill_ac_month_is_rejected_by_group_view():
    with pytest.raises(ValueError, match="unsupported in CESC Group View"):
        CescGroupViewPgCompiler().compile_count(_rules(_month("equals", "202602")))

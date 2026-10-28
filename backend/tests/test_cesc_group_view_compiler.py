"""Focused tests for the opt-in CESC Group View compiler."""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from app.schemas.segment_rules import SegmentDefinition
from app.services.query_engine.pg_compiler import CescGroupViewPgCompiler


def _rules(*conditions: dict) -> SegmentDefinition:
    return SegmentDefinition.model_validate(
        {
            "root": {
                "type": "group",
                "logical_operator": "and",
                "conditions": list(conditions),
            }
        }
    )


def _attribute(key: str, operator: str, value=None, second_value=None) -> dict:
    result = {
        "type": "attribute",
        "attribute_key": key,
        "operator": operator,
        "value": value,
    }
    if second_value is not None:
        result["second_value"] = second_value
    return result


def test_group_view_uses_namespaced_logical_keys_and_single_view_scan():
    compiler = CescGroupViewPgCompiler()
    sql = compiler.compile_count(
        _rules(_attribute("cesc.net_amount", "greater_than", 3000))
    )

    assert "'G:' || m.person_communication_group_id" in sql
    assert "'E:' || p.cesc_entity_id" in sql
    assert sql.count("FROM cesc_serving.customer_360_seg_v p") == 1
    assert "LEFT JOIN member_map m USING (consumer_no)" in sql
    assert "NOT EXISTS" not in sql
    assert "SUM(p.net_amount) > 3000" in sql
    assert "audience_count" in sql


def test_group_view_uses_member_predicates_for_text_and_true_negation():
    compiler = CescGroupViewPgCompiler()
    sql = compiler.compile_count(
        _rules(
            _attribute("cesc.consumer_account_status", "not_equals", "L"),
            _attribute("cesc.pincode", "not_contains", "700"),
        )
    )

    assert "NOT COALESCE(BOOL_OR(p.consumer_account_status ILIKE 'L'), FALSE)" in sql
    assert "NOT COALESCE(BOOL_OR(p.pincode ILIKE '%700%'), FALSE)" in sql


def test_group_view_numeric_negatives_apply_to_the_group_aggregate():
    compiler = CescGroupViewPgCompiler()
    sql = compiler.compile_count(
        _rules(
            _attribute("cesc.net_amount", "not_equals", 3000),
            _attribute("cesc.net_amount", "not_between", 1000, 3000),
        )
    )

    assert "SUM(p.net_amount) != 3000" in sql
    assert "SUM(p.net_amount) BETWEEN 1000 AND 3000" in sql
    assert "SUM(p.net_amount) IS NULL" in sql


def test_group_view_preview_aliases_are_stable():
    compiler = CescGroupViewPgCompiler()
    sql = compiler.compile_preview(
        _rules(_attribute("cesc.net_amount", "greater_than", 3000)),
        limit=10,
    )

    for alias in ("customer_id", "mobile", "name", "email", "pincode", "registered_store", "city"):
        assert f" AS {alias}" in sql


def test_group_view_summary_is_group_level():
    compiler = CescGroupViewPgCompiler()
    sql = compiler.compile_summary(
        _rules(_attribute("cesc.net_amount", "greater_than", 3000)),
        ["total_spend", "avg_spend", "total_bills"],
    )

    assert "COUNT(*) AS audience_size" in sql
    assert "SUM(total_spend_group_value) AS total_spend" in sql
    assert "AVG(avg_spend_group_value) AS avg_spend" in sql
    assert "SUM(total_bills_group_value) AS total_bills" in sql


def test_group_view_member_map_integrity_check_is_present():
    sql = CescGroupViewPgCompiler().validate_member_map_sql()

    assert "GROUP BY consumer_no" in sql
    assert "COUNT(DISTINCT person_communication_group_id) > 1" in sql
    assert "LIMIT 1" in sql


def test_group_view_allows_throwaway_schema_names():
    compiler = CescGroupViewPgCompiler(
        view_name="test_cesc_gv.customer_360_seg_v",
        decision_table="test_cesc_gv.grouping_decision",
    )
    sql = compiler.compile_count(
        _rules(_attribute("cesc.net_amount", "greater_than", 3000))
    )

    assert "FROM test_cesc_gv.customer_360_seg_v p" in sql
    assert "FROM test_cesc_gv.grouping_decision" in sql

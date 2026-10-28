"""Regression tests for the active PostgreSQL CESC segment compiler."""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import psycopg

from app.schemas.segment_rules import SegmentDefinition
from app.services.segmentation.service import _make_compiler


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


def _attribute(key: str, operator: str, value) -> dict:
    return {
        "type": "attribute",
        "attribute_key": key,
        "operator": operator,
        "value": value,
    }


def test_cesc_compile_uses_entity_id_and_no_join():
    compiler = _make_compiler("power_cesc")
    sql = compiler.compile(
        _rules(
            _attribute("cesc.consumer_account_status", "equals", "L"),
            _attribute("cesc.net_amount", "greater_than", 3000),
        )
    )

    assert "p.cesc_entity_id AS customer_id" in sql
    assert "p.unified_id" not in sql
    assert "ba.customer_id = p.unified_id" not in sql
    assert "p.consumer_account_status ILIKE 'L'" in sql
    assert "p.net_amount > 3000" in sql


def test_cesc_count_alias_and_identity_are_stable():
    compiler = _make_compiler("power_cesc")
    sql = compiler.compile_count(
        _rules(_attribute("cesc.net_amount", "greater_than", 3000))
    )

    assert "COUNT(DISTINCT p.cesc_entity_id) AS audience_count" in sql
    assert "p.unified_id" not in sql


def test_cesc_preview_aliases_are_stable():
    compiler = _make_compiler("power_cesc")
    sql = compiler.compile_preview(
        _rules(_attribute("cesc.net_amount", "greater_than", 3000)),
        limit=100,
    )

    assert "p.cesc_entity_id AS customer_id" in sql
    for alias in ("mobile", "name", "email", "pincode", "registered_store", "city"):
        assert f" AS {alias}" in sql or f"  p.{alias}" in sql
    assert "p.unified_id" not in sql


def test_cesc_summary_uses_entity_rows_directly():
    compiler = _make_compiler("power_cesc")
    sql = compiler.compile_summary(
        _rules(_attribute("cesc.net_amount", "greater_than", 3000)),
        ["total_spend", "avg_spend", "total_bills"],
    )

    assert "COUNT(DISTINCT p.cesc_entity_id) AS audience_size" in sql
    assert "SUM(COALESCE(p.lifetime_spend, 0)) AS total_spend" in sql
    assert "AVG(COALESCE(p.lifetime_spend, 0)) AS avg_spend" in sql
    assert "LEFT JOIN" not in sql
    assert "p.unified_id" not in sql


def test_cesc_net_amount_count_matches_live_behavior():
    """The old compiler returns zero; this must match the direct CESC query."""
    compiler = _make_compiler("power_cesc")
    rules = _rules(_attribute("cesc.net_amount", "greater_than", 3000))
    generated_sql = compiler.compile_count(rules)

    conninfo = (
        f"host={os.getenv('PG_HOST', 'host.docker.internal')} "
        f"port={os.getenv('PG_PORT', '5432')} "
        f"dbname={os.getenv('PG_DB', 'cdp_meta')} "
        f"user={os.getenv('PG_USER', 'cdp')} "
        f"password={os.getenv('PG_PASSWORD', 'cdp')}"
    )
    with psycopg.connect(conninfo) as conn:
        with conn.cursor() as cur:
            cur.execute(generated_sql)
            generated_count = cur.fetchone()[0]
            cur.execute(
                "SELECT COUNT(DISTINCT cesc_entity_id) "
                "FROM cesc_serving.customer_360_seg_v "
                "WHERE net_amount > 3000"
            )
            direct_count = cur.fetchone()[0]

    assert generated_count == direct_count
    assert generated_count == 180568


if __name__ == "__main__":
    for test in (
        test_cesc_compile_uses_entity_id_and_no_join,
        test_cesc_count_alias_and_identity_are_stable,
        test_cesc_preview_aliases_are_stable,
        test_cesc_summary_uses_entity_rows_directly,
        test_cesc_net_amount_count_matches_live_behavior,
    ):
        test()
        print(f"PASS: {test.__name__}")

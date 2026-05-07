"""
Reproduce the compile_group logic from both branches and show the
SQL each would generate for the At Risk High Spend segment.
"""
import sys
sys.path.insert(0, r"c:\cdp_new\backend")

from app.schemas.segment_rules import (
    SegmentDefinition, ConditionGroup, AttributeCondition, LogicalOperator
)

# ── The segment definition ────────────────────────────────────────────────────
definition = SegmentDefinition(
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

# ── FEATURE branch SQL (current code) ────────────────────────────────────────
from app.services.query_engine.pg_compiler import PgCompiler

compiler_feature = PgCompiler(brand_code="spencers")
sql_feature = compiler_feature.compile_count(definition)

print("=" * 60)
print("FEATURE BRANCH SQL:")
print("=" * 60)
print(sql_feature)

# ── MAIN branch logic (manually replicate old _compile_group) ─────────────────
def main_compile_group(group):
    parts = []
    for condition in group.conditions:
        col_map = {
            "txn.total_spend": "ba.total_spend",
            "temporal.recency_days": "ba.recency_days",
            "lifecycle.lifecycle_stage": "ba.lifecycle_stage",
        }
        col = col_map[condition.attribute_key]
        if condition.operator == "greater_than":
            compiled = f"{col} > {condition.value}"
        elif condition.operator == "equals":
            compiled = f"{col} = '{condition.value}'"
        if compiled:
            parts.append(compiled)
    joiner = f" {group.logical_operator.value.upper()} "
    return joiner.join(f"({p})" for p in parts)

where_main = main_compile_group(definition.root)
sql_main = f"""SELECT COUNT(*) AS audience_count FROM (
SELECT DISTINCT p.unified_id AS customer_id
FROM silver_identity.unified_profiles p
LEFT JOIN silver_reverse_etl.customer_behavioral_attributes ba ON ba.customer_id = p.unified_id
WHERE {where_main}
) segment_results"""

print()
print("=" * 60)
print("MAIN BRANCH SQL:")
print("=" * 60)
print(sql_main)

# ── Run both and compare counts ───────────────────────────────────────────────
import psycopg
conn = psycopg.connect('host=127.0.0.1 port=5432 user=cdp password=cdp dbname=cdp_meta')
with conn.cursor() as cur:
    cur.execute(sql_feature)
    count_feature = cur.fetchone()[0]
    cur.execute(sql_main)
    count_main = cur.fetchone()[0]

print()
print(f"FEATURE count : {count_feature}")
print(f"MAIN count    : {count_main}")
print(f"Difference    : {count_main - count_feature}")

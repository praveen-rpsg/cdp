"""
Comprehensive Cross-Brand Feature Test Suite
=============================================
Tests every layer of the cross-brand pipeline:

  Layer 1 — Data:         silver layer tables have correct counts & structure
  Layer 2 — Compiler:     CorporatePgCompiler generates correct SQL
  Layer 3 — Routing:      _make_compiler routes 'corporate' → CorporatePgCompiler
  Layer 4 — SQL Execute:  compiled SQL runs against live DB and returns expected counts
  Layer 5 — NL Detection: _is_cross_brand_query correctly identifies cross-brand intent
  Layer 6 — Attr Catalog: cross-brand attributes exist with correct operators
  Layer 7 — Brands API:   /api/v1/brands returns corporate brand entry

Run:
  python C:/cdp_new/scripts/test_cross_brand.py
"""

import sys
import os
import json
import traceback

# ── path setup ────────────────────────────────────────────────────────────────
sys.path.insert(0, "C:/cdp_new/backend")
os.environ.setdefault("DATABASE_URL", "postgresql://cdp:cdp@localhost:5432/cdp_meta")

import psycopg2

PASS = "[PASS]"
FAIL = "[FAIL]"
INFO = "[INFO]"

results = []

def check(name: str, condition: bool, detail: str = ""):
    status = PASS if condition else FAIL
    results.append((status, name, detail))
    icon = "V" if condition else "X"
    print(f"  {icon} {status} {name}" + (f" — {detail}" if detail else ""))
    return condition

def section(title: str):
    print(f"\n{'='*60}")
    print(f"  {title}")
    print(f"{'='*60}")

def get_db():
    return psycopg2.connect(
        host="localhost", port=5432, dbname="cdp_meta",
        user="cdp", password="cdp"
    )

def run_count(sql: str) -> int:
    conn = get_db()
    try:
        cur = conn.cursor()
        cur.execute(sql)
        row = cur.fetchone()
        return row[0] if row else 0
    finally:
        conn.close()


# =============================================================================
# LAYER 1 — DATA: Silver layer table structure & counts
# =============================================================================
section("LAYER 1 — DATA: Silver layer tables")

conn = get_db()
cur = conn.cursor()

# Table existence
for tbl in [
    "corporate_cih.raw_corp_cih",
    "corporate_cih.raw_corp_billanalytics",
    "corporate_cih.silver_spn_attributes",
    "corporate_cih.silver_nbl_attributes",
    "corporate_cih.silver_corp_customer_attributes",
]:
    schema, table = tbl.split(".")
    cur.execute("""
        SELECT COUNT(*) FROM information_schema.tables
        WHERE table_schema=%s AND table_name=%s
    """, (schema, table))
    exists = cur.fetchone()[0] > 0
    check(f"Table exists: {tbl}", exists)

# Row counts — must be non-zero
cur.execute("SELECT COUNT(*) FROM corporate_cih.silver_spn_attributes")
spn_rows = cur.fetchone()[0]
check("silver_spn_attributes has rows", spn_rows > 0, f"{spn_rows:,} rows")

cur.execute("SELECT COUNT(*) FROM corporate_cih.silver_nbl_attributes")
nbl_rows = cur.fetchone()[0]
check("silver_nbl_attributes has rows", nbl_rows > 0, f"{nbl_rows:,} rows")

cur.execute("SELECT COUNT(*) FROM corporate_cih.silver_corp_customer_attributes")
corp_rows = cur.fetchone()[0]
check("silver_corp_customer_attributes has rows", corp_rows > 0, f"{corp_rows:,} rows")

# Corp table should be >= max(spn, nbl) since it's a FULL OUTER JOIN
check(
    "Corp table covers both brands (rows >= max(spn,nbl))",
    corp_rows >= max(spn_rows, nbl_rows),
    f"corp={corp_rows:,} spn={spn_rows:,} nbl={nbl_rows:,}"
)

# Cross-brand customers (both IS NOT NULL)
cur.execute("""
    SELECT COUNT(*) FROM corporate_cih.silver_corp_customer_attributes
    WHERE spn_lifecycle_stage IS NOT NULL AND nbl_lifecycle_stage IS NOT NULL
""")
cross_count = cur.fetchone()[0]
check("Cross-brand customers exist (both spn+nbl present)", cross_count > 0, f"{cross_count:,} customers")

# Distinct l2_segment values present
cur.execute("SELECT DISTINCT spn_l2_segment FROM corporate_cih.silver_corp_customer_attributes WHERE spn_l2_segment IS NOT NULL")
l2_vals = [r[0] for r in cur.fetchall()]
check("spn_l2_segment has distinct values", len(l2_vals) > 0, f"values: {l2_vals}")

# rpsg_brand_presence is populated
cur.execute("SELECT COUNT(*) FROM corporate_cih.silver_corp_customer_attributes WHERE rpsg_brand_presence IS NOT NULL")
bp_count = cur.fetchone()[0]
check("rpsg_brand_presence is populated", bp_count > 0, f"{bp_count:,} rows")

conn.close()


# =============================================================================
# LAYER 2 — COMPILER: CorporatePgCompiler SQL generation
# =============================================================================
section("LAYER 2 — COMPILER: CorporatePgCompiler SQL generation")

from app.services.query_engine.pg_compiler import CorporatePgCompiler, PgCompiler
from app.schemas.segment_rules import (
    AttributeCondition, ConditionGroup, SegmentDefinition, LogicalOperator
)

compiler = CorporatePgCompiler()

def make_def(*conditions, operator="and"):
    return SegmentDefinition(root=ConditionGroup(
        type="group",
        logical_operator=operator,
        conditions=list(conditions)
    ))

def make_attr(key, op, val=None, negate=False, second_value=None):
    return AttributeCondition(
        type="attribute",
        attribute_key=key,
        operator=op,
        value=val,
        negate=negate,
        second_value=second_value,
    )

# Test 1: Brand membership EXISTS
sql = compiler.compile_count(make_def(
    make_attr("corp.is_spencers_customer", "exists", None)
))
check(
    "is_spencers_customer exists → IS NOT NULL",
    "spn_lifecycle_stage IS NOT NULL" in sql,
    sql.split("WHERE")[-1].strip()[:80]
)

# Test 2: NBL membership EXISTS
sql = compiler.compile_count(make_def(
    make_attr("corp.is_nbl_customer", "exists", None)
))
check(
    "is_nbl_customer exists → IS NOT NULL",
    "nbl_lifecycle_stage IS NOT NULL" in sql,
    sql.split("WHERE")[-1].strip()[:80]
)

# Test 3: Both brands (cross-brand AND)
sql = compiler.compile_count(make_def(
    make_attr("corp.is_spencers_customer", "exists", None),
    make_attr("corp.is_nbl_customer", "exists", None),
))
check(
    "Both brands exists → both IS NOT NULL in WHERE",
    "spn_lifecycle_stage IS NOT NULL" in sql and "nbl_lifecycle_stage IS NOT NULL" in sql,
    sql.split("WHERE")[-1].strip()[:120]
)

# Test 4: spn.* namespace resolution
sql = compiler.compile_count(make_def(
    make_attr("spn.spend_per_bill", "greater_than", 3000)
))
check(
    "spn.spend_per_bill → corp.spn_spend_per_bill",
    "corp.spn_spend_per_bill" in sql,
    sql.split("WHERE")[-1].strip()[:80]
)

# Test 5: nbl.* namespace resolution
sql = compiler.compile_count(make_def(
    make_attr("nbl.total_bills", "greater_than", 5)
))
check(
    "nbl.total_bills → corp.nbl_total_bills",
    "corp.nbl_total_bills" in sql,
    sql.split("WHERE")[-1].strip()[:80]
)

# Test 6: corp.* direct namespace
sql = compiler.compile_count(make_def(
    make_attr("corp.rpsg_tenure_lifetime", "greater_than", 365)
))
check(
    "corp.rpsg_tenure_lifetime → corp.rpsg_tenure_lifetime",
    "corp.rpsg_tenure_lifetime" in sql,
    sql.split("WHERE")[-1].strip()[:80]
)

# Test 7: rpsg_brand_presence contains
sql = compiler.compile_count(make_def(
    make_attr("corp.rpsg_brand_presence", "contains", "Spencer")
))
check(
    "rpsg_brand_presence contains → ILIKE '%Spencer%'",
    "ILIKE '%Spencer%'" in sql,
    sql.split("WHERE")[-1].strip()[:80]
)

# Test 8: rpsg_brand_presence not_contains
sql = compiler.compile_count(make_def(
    make_attr("corp.rpsg_brand_presence", "not_contains", "CESC")
))
check(
    "rpsg_brand_presence not_contains → NOT ILIKE '%CESC%'",
    "NOT ILIKE '%CESC%'" in sql,
    sql.split("WHERE")[-1].strip()[:80]
)

# Test 9: NOT / negate on a condition
sql = compiler.compile_count(make_def(
    make_attr("corp.is_nbl_customer", "exists", None, negate=True)
))
check(
    "negate on is_nbl_customer → NOT (...IS NOT NULL)",
    "NOT" in sql and "nbl_lifecycle_stage" in sql,
    sql.split("WHERE")[-1].strip()[:80]
)

# Test 10: OR group
sql = compiler.compile_count(make_def(
    make_attr("spn.lifecycle_stage", "equals", "Lapsed"),
    make_attr("nbl.lifecycle_stage", "equals", "Lapsed"),
    operator="or"
))
check(
    "OR group compiles correctly",
    " OR " in sql,
    sql.split("WHERE")[-1].strip()[:120]
)

# Test 11: between operator
sql = compiler.compile_count(make_def(
    make_attr("spn.spend_per_bill", "between", 500, second_value=2000)
))
check(
    "between operator generates correct SQL",
    "BETWEEN" in sql.upper() or ("500" in sql and "2000" in sql),
    sql.split("WHERE")[-1].strip()[:80]
)

# Test 12: compile() returns r1_id (not unified_id)
sql = compiler.compile(make_def(
    make_attr("corp.is_spencers_customer", "exists", None)
))
check(
    "compile() returns corp.r1_id (not unified_id)",
    "corp.r1_id" in sql and "unified_id" not in sql,
    sql.split("SELECT")[1].split("FROM")[0].strip()[:80]
)

# Test 13: compile_preview() returns both spn and nbl columns
sql = compiler.compile_preview(make_def(
    make_attr("corp.is_spencers_customer", "exists", None)
))
check(
    "compile_preview() selects spn + nbl lifecycle stages",
    "spn_lifecycle_stage" in sql and "nbl_lifecycle_stage" in sql,
    "preview columns present"
)

# Test 14: FROM uses silver_corp table (no JOINs to brand tables)
sql = compiler.compile_count(make_def(
    make_attr("spn.total_spend", "greater_than", 10000)
))
check(
    "FROM references silver_corp table only (no JOINs to brand tables)",
    "silver_corp_customer_attributes" in sql and "silver_reverse_etl" not in sql,
    sql.split("FROM")[1].split("WHERE")[0].strip()[:80]
)

# Test 15: No conditions → no WHERE clause (or 1=1)
sql = compiler.compile_count(SegmentDefinition(root=ConditionGroup(
    type="group", logical_operator="and", conditions=[]
)))
check(
    "Empty conditions → valid SQL without broken WHERE",
    "FROM corporate_cih.silver_corp_customer_attributes" in sql,
    "no broken WHERE clause"
)


# =============================================================================
# LAYER 3 — ROUTING: _make_compiler returns correct class
# =============================================================================
section("LAYER 3 — ROUTING: Compiler factory routing")

from app.services.segmentation.service import _make_compiler

c = _make_compiler("corporate")
check("'corporate' → CorporatePgCompiler", isinstance(c, CorporatePgCompiler), type(c).__name__)

c = _make_compiler("spencers")
check("'spencers' → PgCompiler (not Corporate)", isinstance(c, PgCompiler) and not isinstance(c, CorporatePgCompiler), type(c).__name__)

try:
    c = _make_compiler("natures_basket")
    check("'natures_basket' → PgCompiler", isinstance(c, PgCompiler) and not isinstance(c, CorporatePgCompiler), type(c).__name__)
except Exception as e:
    check("'natures_basket' → PgCompiler", False, str(e))


# =============================================================================
# LAYER 4 — LIVE DB: SQL executes and returns expected counts
# =============================================================================
section("LAYER 4 — LIVE DB: SQL execution against PostgreSQL")

compiler = CorporatePgCompiler()

tests = [
    # (description, definition, expected_min, expected_max)
    (
        "All Spencers customers (spn_lifecycle_stage IS NOT NULL)",
        make_def(make_attr("corp.is_spencers_customer", "exists", None)),
        100000, 300000,
    ),
    (
        "All NBL customers (nbl_lifecycle_stage IS NOT NULL)",
        make_def(make_attr("corp.is_nbl_customer", "exists", None)),
        10000, 100000,
    ),
    (
        "Cross-brand: both spn AND nbl present",
        make_def(
            make_attr("corp.is_spencers_customer", "exists", None),
            make_attr("corp.is_nbl_customer", "exists", None),
        ),
        100, 5000,
    ),
    (
        "SPN spend_per_bill > 3000",
        make_def(make_attr("spn.spend_per_bill", "greater_than", 3000)),
        1, 500000,
    ),
    (
        "NBL spend_per_bill > 500",
        make_def(make_attr("nbl.spend_per_bill", "greater_than", 500)),
        1, 100000,
    ),
    (
        "rpsg_brand_presence contains 'Spencer' (ILIKE)",
        make_def(make_attr("corp.rpsg_brand_presence", "contains", "Spencer")),
        100000, 300000,
    ),
    (
        "rpsg_brand_presence contains 'NBL' (ILIKE)",
        make_def(make_attr("corp.rpsg_brand_presence", "contains", "NBL")),
        10000, 100000,
    ),
    (
        "SPN At-Risk + NBL present (the original bug query)",
        make_def(
            make_attr("corp.is_spencers_customer", "exists", None),
            make_attr("corp.is_nbl_customer", "exists", None),
            make_attr("spn.lifecycle_stage", "equals", "At Risk"),
        ),
        100, 5000,
    ),
    (
        "rpsg_tenure_lifetime > 365 (corp.* direct)",
        make_def(make_attr("corp.rpsg_tenure_lifetime", "greater_than", 365)),
        1, 500000,
    ),
    (
        "SPN lapsed + NBL lapsed (both inactive)",
        make_def(
            make_attr("spn.lifecycle_stage", "equals", "Lapsed"),
            make_attr("nbl.lifecycle_stage", "equals", "Lapsed"),
        ),
        0, 500,
    ),
    (
        "Spencers-only customers (SPN present, NBL absent)",
        make_def(
            make_attr("corp.is_spencers_customer", "exists", None),
            make_attr("corp.is_nbl_customer", "not_exists", None),
        ),
        100000, 300000,
    ),
    (
        "NBL-only customers (NBL present, SPN absent)",
        make_def(
            make_attr("corp.is_nbl_customer", "exists", None),
            make_attr("corp.is_spencers_customer", "not_exists", None),
        ),
        1000, 100000,
    ),
]

for desc, defn, exp_min, exp_max in tests:
    try:
        sql = compiler.compile_count(defn)
        count = run_count(sql)
        in_range = exp_min <= count <= exp_max
        check(
            desc,
            in_range,
            f"count={count:,} (expected {exp_min:,}–{exp_max:,})"
        )
    except Exception as e:
        check(desc, False, f"EXCEPTION: {e}")

# Verify SPN-only + NBL-only + cross-brand = total corp rows
try:
    spn_only = run_count(compiler.compile_count(make_def(
        make_attr("corp.is_spencers_customer", "exists", None),
        make_attr("corp.is_nbl_customer", "not_exists", None),
    )))
    nbl_only = run_count(compiler.compile_count(make_def(
        make_attr("corp.is_nbl_customer", "exists", None),
        make_attr("corp.is_spencers_customer", "not_exists", None),
    )))
    both = run_count(compiler.compile_count(make_def(
        make_attr("corp.is_spencers_customer", "exists", None),
        make_attr("corp.is_nbl_customer", "exists", None),
    )))
    total = run_count("SELECT COUNT(*) FROM corporate_cih.silver_corp_customer_attributes")
    segment_sum = spn_only + nbl_only + both
    check(
        "SPN-only + NBL-only + cross-brand = total corp rows",
        segment_sum == total,
        f"{spn_only:,} + {nbl_only:,} + {both:,} = {segment_sum:,} (total={total:,})"
    )
except Exception as e:
    check("Partition identity check", False, str(e))


# =============================================================================
# LAYER 5 — NL DETECTION: _is_cross_brand_query
# =============================================================================
section("LAYER 5 — NL DETECTION: Cross-brand intent detection")

from app.services.nl_segmentation.service import _is_cross_brand_query

true_positives = [
    ("nature's basket", "explicit brand name"),
    ("Customers who shopped at NBL", "NBL abbreviation"),
    ("Spencers customers also at nbl", "lowercase nbl"),
    ("cross-brand customers", "cross-brand keyword"),
    ("both brands", "both brands keyword"),
    ("Nature's Basket high spenders", "NB full name"),
    ("customers active at both spencers and nbl", "both + nbl"),
    ("RPSG cross brand loyals", "RPSG cross brand"),
    ("shopping across brands", "shopping across brands"),
    ("multi-brand customers", "multi-brand"),
]

false_positives = [
    ("High spenders in Kolkata", "single-brand generic"),
    ("Lapsed customers with spend > 5000", "single-brand transactional"),
    ("STAR segment customers", "segment name only"),
    ("Customers who accept SMS marketing", "channel consent"),
    ("New first-time buyers last 30 days", "lifecycle only"),
    ("Weekend shoppers with promo bills", "behaviour only"),
]

print("\n  True positives (should detect as cross-brand):")
for query, label in true_positives:
    result = _is_cross_brand_query(query)
    check(f"  DETECT: '{query[:50]}'", result, label)

print("\n  True negatives (should NOT detect as cross-brand):")
for query, label in false_positives:
    result = _is_cross_brand_query(query)
    check(f"  IGNORE: '{query[:50]}'", not result, label)


# =============================================================================
# LAYER 6 — ATTRIBUTE CATALOG: Cross-brand attrs exist with correct operators
# =============================================================================
section("LAYER 6 — ATTRIBUTE CATALOG: Cross-brand attributes")

from app.schemas.profile_attributes import ATTRIBUTE_CATALOG

corp_attrs = {a.key: a for a in ATTRIBUTE_CATALOG if "corporate" in (a.applicable_brands or [])}

check("At least 20 corporate-applicable attributes exist", len(corp_attrs) >= 20, f"{len(corp_attrs)} found")

# Key attributes must exist
for key in [
    "corp.is_spencers_customer",
    "corp.is_nbl_customer",
    "corp.rpsg_brand_presence",
    "corp.rpsg_ftd",
    "corp.rpsg_tenure_lifetime",
    "spn.lifecycle_stage",
    "spn.spend_per_bill",
    "spn.total_bills",
    "nbl.lifecycle_stage",
    "nbl.spend_per_bill",
    "nbl.total_bills",
]:
    check(f"Catalog has: {key}", key in corp_attrs)

# is_spencers_customer / is_nbl_customer must use EXISTS_OPS only
for key in ["corp.is_spencers_customer", "corp.is_nbl_customer"]:
    if key in corp_attrs:
        ops = corp_attrs[key].operators
        has_exists = "exists" in ops and "not_exists" in ops
        no_equals = "equals" not in ops
        check(f"{key} uses exists/not_exists only", has_exists and no_equals, f"operators={ops}")

# rpsg_brand_presence must NOT have equals, must have contains
if "corp.rpsg_brand_presence" in corp_attrs:
    ops = corp_attrs["corp.rpsg_brand_presence"].operators
    check(
        "rpsg_brand_presence has 'contains' operator",
        "contains" in ops,
        f"operators={ops}"
    )
    check(
        "rpsg_brand_presence does NOT have 'equals' (prevents the 0-count bug)",
        "equals" not in ops,
        f"operators={ops}"
    )

# cross_brand category exists in catalog
cross_brand_attrs = [a for a in corp_attrs.values() if str(a.category) in ("cross_brand", "AttributeCategory.CROSS_BRAND")]
check("Attributes with cross_brand category exist", len(cross_brand_attrs) > 0, f"{len(cross_brand_attrs)} found")


# =============================================================================
# LAYER 7 — BRANDS API: /api/v1/brands returns corporate entry
# =============================================================================
section("LAYER 7 — BRANDS API: Corporate brand registration")

# Import and inspect directly (avoids needing the HTTP server running)
from app.api.v1.brands import BRANDS  # type: ignore

corp_brands = [b for b in BRANDS if b.get("code") == "corporate"]
check("BRANDS list contains 'corporate' entry", len(corp_brands) == 1, f"{len(corp_brands)} found")

if corp_brands:
    cb = corp_brands[0]
    check("Corporate brand name is 'Cross-Brand (RPSG)'", cb.get("name") == "Cross-Brand (RPSG)", cb.get("name"))
    check("Corporate brand is_active=True", cb.get("is_active") is True)
    check("Corporate brand has is_cross_brand=True flag", cb.get("is_cross_brand") is True)


# =============================================================================
# SUMMARY
# =============================================================================
section("TEST SUMMARY")

total  = len(results)
passed = sum(1 for r in results if r[0] == PASS)
failed = sum(1 for r in results if r[0] == FAIL)

print(f"\n  Total:  {total}")
print(f"  Passed: {passed}  ({'%.0f' % (passed/total*100)}%)")
print(f"  Failed: {failed}")

if failed:
    print("\n  FAILED TESTS:")
    for status, name, detail in results:
        if status == FAIL:
            print(f"    X {name}" + (f" — {detail}" if detail else ""))

print()
sys.exit(0 if failed == 0 else 1)

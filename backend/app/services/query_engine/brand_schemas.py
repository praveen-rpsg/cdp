"""
Brand Schema Registry
======================

Central registry that maps each brand_code to its physical PostgreSQL
table names and alias-to-table lookups.

Since NBL and Spencer's share an identical dbt model structure, both brands
use the same canonical SPENCERS_SCHEMA_MAP (attribute key → alias.column).
The only difference between brands is which *schemas* those aliases resolve to.

Adding a new brand:
    1. Add an entry to BRAND_TABLE_CONFIG with the brand's schema prefixes.
    2. If the brand has a structurally different column layout, define a new
       schema_map and reference it in the config's "schema_map" key.
    3. Add the brand to profile_attributes.py applicable_brands lists.
"""

from __future__ import annotations

from app.services.query_engine.pg_compiler import SPENCERS_SCHEMA_MAP


# ---------------------------------------------------------------------------
# Per-brand table configuration
# ---------------------------------------------------------------------------
# Each entry defines:
#   schema_map   – attribute key → alias.column mapping (reused across brands
#                  that share the same dbt model structure)
#   alias_tables – alias prefix → fully-qualified PostgreSQL table name
#   base_table   – the primary FROM table (unified_profiles)
#   ba_table     – customer_behavioral_attributes
#   gs_table     – identity_graph_summary
#   loc_table    – location master (for geo.state / geo.zone joins)
#   bt_table     – bill transaction line-item table (for EXISTS subqueries)
# ---------------------------------------------------------------------------

BRAND_TABLE_CONFIG: dict[str, dict] = {
    "spencers": {
        "schema_map": SPENCERS_SCHEMA_MAP,
        "alias_tables": {
            "p":   "silver_identity.unified_profiles",
            "ba":  "silver_reverse_etl.customer_behavioral_attributes",
            "gs":  "silver_identity.identity_graph_summary",
            "loc": "bronze.raw_location_master",
            "bt":  "silver.s_fact_bill_transactions",
        },
        "base_table": "silver_identity.unified_profiles",
        "ba_table":   "silver_reverse_etl.customer_behavioral_attributes",
        "gs_table":   "silver_identity.identity_graph_summary",
        "loc_table":  "bronze.raw_location_master",
        "bt_table":   "silver.s_fact_bill_transactions",
    },
    "nbl": {
        # NBL uses the same attribute key → column mapping as Spencer's
        # (identical dbt model structure, just under nb_* schema prefixes).
        "schema_map": SPENCERS_SCHEMA_MAP,
        "alias_tables": {
            "p":   "nb_silver_identity.unified_profiles",
            "ba":  "nb_silver_reverse_etl.customer_behavioral_attributes",
            "gs":  "nb_silver_identity.identity_graph_summary",
            "loc": "nb_bronze.raw_location_master",
            "bt":  "nb_silver.s_fact_bill_transactions",
        },
        "base_table": "nb_silver_identity.unified_profiles",
        "ba_table":   "nb_silver_reverse_etl.customer_behavioral_attributes",
        "gs_table":   "nb_silver_identity.identity_graph_summary",
        "loc_table":  "nb_bronze.raw_location_master",
        "bt_table":   "nb_silver.s_fact_bill_transactions",
    },
}

# Fallback to Spencer's if an unknown brand_code is passed.
_DEFAULT_BRAND = "spencers"


def get_brand_config(brand_code: str) -> dict:
    """Return the table config for a brand, defaulting to Spencer's."""
    return BRAND_TABLE_CONFIG.get(brand_code, BRAND_TABLE_CONFIG[_DEFAULT_BRAND])


def get_schema_map(brand_code: str) -> dict[str, str]:
    """Return the attribute → column schema map for a brand."""
    return get_brand_config(brand_code)["schema_map"]


def get_alias_tables(brand_code: str) -> dict[str, str]:
    """Return the alias → fully-qualified table name mapping for a brand."""
    return get_brand_config(brand_code)["alias_tables"]

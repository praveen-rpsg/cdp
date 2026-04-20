"""
PostgreSQL Compiler for Spencer's CDP
======================================

Compiles segment rule trees into PostgreSQL-compatible SQL
against the Spencer's DWH (identity + silver + gold + reverse_etl schemas).

Replaces the Athena compiler for local/POC usage with real Spencer's data.
"""

from __future__ import annotations

from datetime import date
from typing import Any

from app.schemas.segment_rules import (
    AttributeCondition,
    ConditionGroup,
    ConditionType,
    CrossBrandCondition,
    EventCondition,
    EventPropertyFilter,
    LogicalOperator,
    SegmentDefinition,
    SegmentMembershipCondition,
    TimeWindow,
    TimeWindowType,
)


# Spencer's DWH schema mapping: canonical attribute keys -> actual PG columns
# Maps to: p = silver_identity.unified_profiles
#           ba = silver_reverse_etl.customer_behavioral_attributes
#           txn = silver_gold.customer_transaction_summary
#           loc = bronze.raw_location_master (joined via store)
#           gs = silver_identity.identity_graph_summary
SPENCERS_SCHEMA_MAP = {
    # ── Identity ──
    "identity.customer_id": "p.unified_id",
    "identity.email": "p.email",
    "identity.phone": "p.canonical_mobile",
    "identity.mobile": "p.canonical_mobile",
    "identity.surrogate_id": "p.surrogate_id",
    "identity.has_transactions": "p.has_transactions",
    "identity.primary_source": "p.primary_source",

    # ── Demographics (from CIH golden record) ──
    "demographic.full_name": "p.display_name",
    "demographic.first_name": "p.first_name",
    "demographic.last_name": "p.last_name",
    "demographic.name": "p.display_name",
    "demographic.age": "ba.age",
    "demographic.dob": "p.dob",
    "demographic.customer_group": "ba.customer_group",
    "demographic.occupation": "ba.occupation",
    "demographic.whatsapp": "ba.whatsapp",

    # ── Geographic ──
    "geo.pincode": "p.pincode",
    "geo.city": "p.city",
    "geo.street": "p.street",
    "geo.region": "p.region",
    "geo.state": "loc.store_state",
    "geo.zone": "loc.store_zone",
    "geo.store_format": "loc.store_format",
    "geo.home_store_id": "p.registered_store",

    # ── Transactional / Bill Summary (Precalculated) ──
    "txn.total_bills": "ba.total_bills",
    "txn.total_visits": "ba.total_visits",
    "txn.total_spend": "ba.total_spend",
    "txn.spend_per_bill": "ba.spend_per_bill",
    "txn.spend_per_visit": "ba.spend_per_visit",
    "txn.avg_items_per_bill": "ba.avg_items_per_bill",
    "txn.total_discount": "ba.total_discount",
    "txn.distinct_months": "ba.distinct_months",
    "txn.distinct_store_count": "ba.distinct_store_count",
    "txn.distinct_article_count": "ba.distinct_article_count",
    "txn.avg_billing_time_secs": "ba.avg_billing_time_secs",
    "txn.return_bill_count": "ba.return_bill_count",
    "txn.promo_bill_count": "ba.promo_bill_count",
    "txn.weekend_bill_count": "ba.weekend_bill_count",
    "txn.wednesday_bill_count": "ba.wednesday_bill_count",

    # ── Temporal / Recency ──
    "temporal.first_bill_date": "ba.first_bill_date",
    "temporal.last_bill_date": "ba.last_bill_date",
    "temporal.recency_days": "ba.recency_days",
    "temporal.tenure_days": "ba.tenure_days",
    "temporal.dgbt_fs": "ba.dgbt_fs",

    # ── Lifecycle / Segmentation ──
    "lifecycle.l1_segment": "ba.l1_segment",
    "lifecycle.l2_segment": "ba.l2_segment",
    "lifecycle.lifecycle_stage": "ba.lifecycle_stage",
    "lifecycle.rfm_recency_score": "ba.rfm_recency_score",
    "lifecycle.rfm_frequency_score": "ba.rfm_frequency_score",
    "lifecycle.rfm_monetary_score": "ba.rfm_monetary_score",
    "lifecycle.is_first_time_buyer": "CASE WHEN ba.total_bills = 1 THEN TRUE ELSE FALSE END",
    "lifecycle.is_repeat_buyer": "CASE WHEN ba.total_bills > 1 THEN TRUE ELSE FALSE END",
    "lifecycle.is_active": "CASE WHEN ba.lifecycle_stage = 'Active' THEN TRUE ELSE FALSE END",
    "lifecycle.is_churned": "CASE WHEN ba.lifecycle_stage = 'Churned' THEN TRUE ELSE FALSE END",

    # ── Decile / Ranking ──
    "decile.spend_decile": "ba.spend_decile",
    "decile.nob_decile": "ba.nob_decile",

    # ── Store / Favourites (Precalculated) ──
    "store.fav_store_code": "ba.fav_store_code",
    "store.fav_store_name": "ba.fav_store_name",
    "store.fav_store_type": "ba.fav_store_type",
    "store.fav_day": "ba.fav_day",

    # ── Product / Article Favourites (Precalculated) ──
    "product.fav_article_by_spend": "ba.fav_article_by_spend",
    "product.fav_article_by_spend_desc": "ba.fav_article_by_spend_desc",
    "product.fav_article_by_nob": "ba.fav_article_by_nob",
    "product.fav_article_by_nob_desc": "ba.fav_article_by_nob_desc",
    "product.second_fav_article_by_spend": "ba.second_fav_article_by_spend",
    "product.second_fav_article_by_nob": "ba.second_fav_article_by_nob",

    # ── Channel ──
    "channel.channel_presence": "ba.channel_presence",
    "channel.store_spend": "ba.store_spend",
    "channel.online_spend": "ba.online_spend",
    "channel.store_bills": "ba.store_bills",
    "channel.online_bills": "ba.online_bills",

    # ── Consent / Preferences ──
    "consent.dnd": "ba.dnd",
    "consent.accepts_email_marketing": "ba.accepts_email_marketing",
    "consent.accepts_sms_marketing": "ba.accepts_sms_marketing",
    "consent.gw_customer_flag": "ba.gw_customer_flag",

    # ── Bill Transaction (line-item level, compiled as EXISTS subqueries) ──
    "bt.bill_date": "bt.bill_date",
    "bt.store_code": "bt.store_code",
    "bt.store_desc": "bt.store_desc",
    "bt.store_format": "bt.store_format",
    "bt.region": "bt.region",
    "bt.region_desc": "bt.region_desc",
    "bt.city": "bt.city",
    "bt.city_desc": "bt.city_desc",
    "bt.article": "bt.article",
    "bt.article_desc": "bt.article_desc",
    "bt.segment_desc": "bt.segment_desc",
    "bt.family_desc": "bt.family_desc",
    "bt.class_desc": "bt.class_desc",
    "bt.brick_desc": "bt.brick_desc",
    "bt.brand_name": "bt.brand_name",
    "bt.manufacturer_name": "bt.manufacturer_name",
    "bt.gross_sale_value": "bt.gross_sale_value",
    "bt.total_discount": "bt.total_discount",
    "bt.total_mrp_value": "bt.total_mrp_value",
    "bt.billed_qty": "bt.billed_qty",
    "bt.billed_mrp": "bt.billed_mrp",
    "bt.delivery_channel": "bt.delivery_channel",
    "bt.sales_return": "bt.sales_return",
    "bt.weekend_flag": "bt.weekend_flag",
    "bt.wednesday_flag": "bt.wednesday_flag",
    "bt.promo_indicator": "bt.promo_indicator",
    "bt.day_of_week": "bt.day_of_week",
    "bt.mobile_number": "bt.mobile_number",
}


class PgCompiler:
    """
    Compiles SegmentDefinition rule trees into PostgreSQL SQL
    against the Spencer's DWH schemas (dbt-generated).

    Main tables:
    - silver_identity.unified_profiles p   (CIH-seeded customer profiles)
    - silver_reverse_etl.customer_behavioral_attributes ba  (computed attrs)
    - silver_identity.identity_graph_summary gs  (graph metrics)
    - bronze.raw_location_master loc  (store info)
    """

    def __init__(
        self,
        brand_code: str = "spencers",
        schema_mapping: dict[str, str] | None = None,
    ):
        self.brand_code = brand_code
        self.schema_mapping = {**SPENCERS_SCHEMA_MAP, **(schema_mapping or {})}
        self._cte_counter = 0
        self._ctes: list[str] = []
        self._extra_joins: list[str] = []
        self._needs_ba = False
        self._needs_gs = False
        self._needs_loc = False
        self._needs_nps = False

    def _reset(self):
        self._cte_counter = 0
        self._ctes = []
        self._extra_joins = []
        self._needs_ba = False
        self._needs_gs = False
        self._needs_loc = False
        self._needs_nps = False

    def compile(self, definition: SegmentDefinition) -> str:
        """Compile a full segment definition into PostgreSQL SQL."""
        self._reset()
        where_clause = self._compile_group(definition.root)
        return self._build_query(where_clause, definition, select="DISTINCT p.unified_id AS customer_id")

    def compile_count(self, definition: SegmentDefinition) -> str:
        """Compile a COUNT query for audience size estimation."""
        inner = self.compile(definition)
        return f"SELECT COUNT(*) AS audience_count FROM (\n{inner}\n) segment_results"

    def compile_preview(self, definition: SegmentDefinition, limit: int = 100) -> str:
        """Compile a preview query returning sample profiles."""
        self._reset()
        where_clause = self._compile_group(definition.root)
        select = (
            "p.unified_id AS customer_id,\n"
            "  p.canonical_mobile AS mobile,\n"
            "  p.display_name AS name,\n"
            "  p.email,\n"
            "  p.pincode,\n"
            "  p.registered_store,\n"
            "  p.city"
        )
        preview_def = SegmentDefinition(
            root=definition.root,
            limit=limit,
            order_by=definition.order_by,
            order_direction=definition.order_direction,
        )
        return self._build_query(where_clause, preview_def, select=select)

    def _build_query(self, where_clause: str, definition: SegmentDefinition, select: str) -> str:
        """Assemble the final SQL query with proper JOINs."""
        # Detect which tables are needed from the where clause and select
        combined = where_clause + " " + select
        if "ba." in combined:
            self._needs_ba = True
        if "gs." in combined:
            self._needs_gs = True
        if "loc." in combined:
            self._needs_loc = True

        parts = []

        # CTEs
        if self._ctes:
            parts.append("WITH " + ",\n".join(self._ctes))

        parts.append(f"SELECT {select}")
        parts.append("FROM silver_identity.unified_profiles p")

        # Standard JOINs based on what's needed
        if self._needs_ba:
            parts.append(
                "LEFT JOIN silver_reverse_etl.customer_behavioral_attributes ba "
                "ON ba.customer_id = p.unified_id"
            )
        if self._needs_gs:
            parts.append(
                "LEFT JOIN silver_identity.identity_graph_summary gs "
                "ON gs.unified_id = p.unified_id"
            )
        if self._needs_loc:
            parts.append(
                "LEFT JOIN bronze.raw_location_master loc "
                "ON loc.store_code = p.registered_store"
            )

        # Extra CTE-based JOINs
        for join in self._extra_joins:
            parts.append(join)

        if where_clause and where_clause != "1=1":
            parts.append(f"WHERE {where_clause}")

        # ORDER BY
        if definition.order_by:
            col = self._resolve_column(definition.order_by)
            direction = (definition.order_direction or "desc").upper()
            parts.append(f"ORDER BY {col} {direction} NULLS LAST")

        # LIMIT
        if definition.limit:
            parts.append(f"LIMIT {int(definition.limit)}")

        return "\n".join(parts)

    # =========================================================================
    # Condition compilers
    # =========================================================================

    def _compile_group(self, group: ConditionGroup) -> str:
        parts = []
        sql = ""
        for i, condition in enumerate(group.conditions):
            compiled = self._compile_condition(condition)
            if not compiled:
                continue

            if i == 0:
                sql = f"({compiled})"
            else:
                op = getattr(condition, "logical_operator", None)
                if isinstance(condition, ConditionGroup):
                    op = getattr(condition, "logical_operator_prefix", None)
                if op is None:
                    op = group.logical_operator
                
                # Default is string or Enum, so check
                op_val = op.value.upper() if hasattr(op, "value") else str(op).upper()
                joiner = f" {op_val} "
                # Wrap sequentially to enforce strict left-to-right evaluation
                sql = f"({sql}{joiner}({compiled}))"

        if not sql:
            return "1=1"
        return sql

    def _compile_condition(self, condition: ConditionType) -> str:
        if isinstance(condition, ConditionGroup):
            return self._compile_group(condition)
        elif isinstance(condition, AttributeCondition):
            return self._compile_attribute(condition)
        elif isinstance(condition, EventCondition):
            return self._compile_event(condition)
        elif isinstance(condition, SegmentMembershipCondition):
            return self._compile_segment_membership(condition)
        elif isinstance(condition, CrossBrandCondition):
            return self._compile_cross_brand(condition)
        else:
            raise ValueError(f"Unknown condition type: {type(condition)}")

    def _compile_attribute(self, cond: AttributeCondition) -> str:
        # BT (bill transaction) attributes use EXISTS subqueries against line-item data
        if cond.attribute_key.startswith("bt."):
            return self._compile_bt_attribute(cond)
        col = self._resolve_column(cond.attribute_key)
        sql = self._operator_to_sql(col, cond.operator, cond.value, cond.second_value)
        if cond.negate:
            sql = f"NOT ({sql})"
        return sql

    def _compile_bt_attribute(self, cond: AttributeCondition) -> str:
        """
        Compile a Bill Transaction attribute condition as an EXISTS subquery.

        BT attributes are line-item level (one row per article per bill).
        We compile them as EXISTS subqueries against silver.s_fact_bill_transactions
        joined back to the main query via mobile_number = canonical_mobile.

        This avoids fan-out (the main query still returns 1 row per customer)
        while enabling filtering on any line-item attribute.
        """
        col_name = cond.attribute_key.split(".", 1)[1]  # e.g., "brand_name"
        col = f"bt.{col_name}"
        inner_condition = self._operator_to_sql(col, cond.operator, cond.value, cond.second_value)

        if cond.negate:
            return (
                f"NOT EXISTS (\n"
                f"  SELECT 1 FROM silver.s_fact_bill_transactions bt\n"
                f"  WHERE bt.mobile_number = p.canonical_mobile\n"
                f"    AND {inner_condition}\n"
                f")"
            )
        return (
            f"EXISTS (\n"
            f"  SELECT 1 FROM silver.s_fact_bill_transactions bt\n"
            f"  WHERE bt.mobile_number = p.canonical_mobile\n"
            f"    AND {inner_condition}\n"
            f")"
        )

    def _compile_event(self, cond: EventCondition) -> str:
        """Events from purchase/feedback treated as attribute conditions on ba table."""
        self._needs_ba = True
        if cond.event_name in ("purchase", "transaction"):
            time_filter = ""
            if cond.time_window and cond.time_window.type == TimeWindowType.LAST_N_DAYS and cond.time_window.days:
                time_filter = (
                    f" AND ba.last_bill_date >= CURRENT_DATE - INTERVAL '{int(cond.time_window.days)} days'"
                )
            elif cond.time_window and cond.time_window.type == TimeWindowType.AFTER_DATE and cond.time_window.start_date:
                time_filter = f" AND ba.last_bill_date > '{cond.time_window.start_date}'::DATE"
            elif cond.time_window and cond.time_window.type == TimeWindowType.BEFORE_DATE and cond.time_window.end_date:
                time_filter = f" AND ba.last_bill_date < '{cond.time_window.end_date}'::DATE"
            elif cond.time_window and cond.time_window.type == TimeWindowType.BETWEEN_DATES:
                start = cond.time_window.start_date
                end = cond.time_window.end_date
                if start and end:
                    time_filter = (
                        f" AND ba.last_bill_date BETWEEN '{start}'::DATE AND '{end}'::DATE"
                    )

            if cond.operator == "has_performed":
                return f"ba.total_bills > 0{time_filter}"
            elif cond.operator == "has_not_performed":
                return f"(ba.total_bills IS NULL OR ba.total_bills = 0{time_filter})"
            elif cond.operator.startswith("performed_count_"):
                count_op = cond.operator.replace("performed_count_", "")
                base = self._operator_to_sql(
                    "COALESCE(ba.total_bills, 0)",
                    count_op,
                    cond.count_value,
                    None,
                )
                return f"{base}{time_filter}" if time_filter else base
        elif cond.event_name in ("feedback", "yvm"):
            # No dedicated feedback column in new schema — use promo_bill_count as proxy
            return "1=1"
        elif cond.event_name in ("promo_usage", "cashback"):
            if cond.operator == "has_performed":
                return "ba.promo_bill_count > 0"
            return "COALESCE(ba.promo_bill_count, 0) = 0"

        return "1=1"

    def _compile_segment_membership(self, cond: SegmentMembershipCondition) -> str:
        table = f"segment_results_{cond.segment_id.replace('-', '_')}"
        if cond.operator == "is_member":
            return f"p.unified_id IN (SELECT customer_id FROM {table})"
        return f"p.unified_id NOT IN (SELECT customer_id FROM {table})"

    def _compile_cross_brand(self, cond: CrossBrandCondition) -> str:
        # For Spencer's POC, cross-brand is not applicable
        return "1=1"

    # =========================================================================
    # SQL helpers
    # =========================================================================

    def _resolve_column(self, attribute_key: str) -> str:
        if attribute_key in self.schema_mapping:
            col = self.schema_mapping[attribute_key]
            # Track table dependencies
            if col.startswith("ba."):
                self._needs_ba = True
            elif col.startswith("gs."):
                self._needs_gs = True
            elif col.startswith("loc."):
                self._needs_loc = True
            return col

        # Default: derive from key
        parts = attribute_key.split(".", 1)
        col_name = parts[1] if len(parts) > 1 else parts[0]
        # Try behavioral attributes first
        self._needs_ba = True
        return f"ba.{col_name}"

    def _quote(self, value: Any) -> str:
        if value is None:
            return "NULL"
        if isinstance(value, bool):
            return "TRUE" if value else "FALSE"
        if isinstance(value, (int, float)):
            return str(value)
        if isinstance(value, date):
            return f"'{value.isoformat()}'::DATE"
        escaped = str(value).replace("'", "''")
        return f"'{escaped}'"

    def _quote_list(self, values: list) -> str:
        return "(" + ", ".join(self._quote(v) for v in values) + ")"

    def _operator_to_sql(self, column: str, operator: str, value: Any, second_value: Any) -> str:
        match operator:
            # String (equals/not_equals are case-insensitive via ILIKE for robustness)
            case "equals":
                return f"{column} ILIKE {self._quote(value)}"
            case "not_equals":
                return f"{column} NOT ILIKE {self._quote(value)}"
            case "contains":
                return f"{column} ILIKE {self._quote(f'%{value}%')}"
            case "not_contains":
                return f"{column} NOT ILIKE {self._quote(f'%{value}%')}"
            case "starts_with":
                return f"{column} ILIKE {self._quote(f'{value}%')}"
            case "ends_with":
                return f"{column} ILIKE {self._quote(f'%{value}')}"
            case "in_list":
                if isinstance(value, list):
                    return f"{column} IN {self._quote_list(value)}"
                return f"{column} IN ({self._quote(value)})"
            case "not_in_list":
                if isinstance(value, list):
                    return f"{column} NOT IN {self._quote_list(value)}"
                return f"{column} NOT IN ({self._quote(value)})"
            case "regex_match":
                return f"{column} ~ {self._quote(value)}"
            case "is_empty":
                return f"({column} IS NULL OR {column} = '')"
            case "is_not_empty":
                return f"({column} IS NOT NULL AND {column} != '')"

            # Numeric
            case "greater_than":
                return f"{column} > {self._quote(value)}"
            case "less_than":
                return f"{column} < {self._quote(value)}"
            case "greater_than_or_equal":
                return f"{column} >= {self._quote(value)}"
            case "less_than_or_equal":
                return f"{column} <= {self._quote(value)}"
            case "between":
                return f"{column} BETWEEN {self._quote(value)} AND {self._quote(second_value)}"
            case "not_between":
                return f"{column} NOT BETWEEN {self._quote(value)} AND {self._quote(second_value)}"

            # Date (PostgreSQL syntax)
            case "before":
                return f"{column} < {self._quote(value)}"
            case "after":
                return f"{column} > {self._quote(value)}"
            case "in_last_n_days":
                return f"{column} >= CURRENT_DATE - INTERVAL '{int(value)} days'"
            case "not_in_last_n_days":
                return f"{column} < CURRENT_DATE - INTERVAL '{int(value)} days'"
            case "in_next_n_days":
                return f"{column} BETWEEN CURRENT_DATE AND CURRENT_DATE + INTERVAL '{int(value)} days'"
            case "is_today":
                return f"{column}::DATE = CURRENT_DATE"
            case "is_this_week":
                return f"DATE_TRUNC('week', {column}) = DATE_TRUNC('week', CURRENT_DATE)"
            case "is_this_month":
                return f"DATE_TRUNC('month', {column}) = DATE_TRUNC('month', CURRENT_DATE)"
            case "is_this_quarter":
                return f"DATE_TRUNC('quarter', {column}) = DATE_TRUNC('quarter', CURRENT_DATE)"
            case "is_this_year":
                return f"EXTRACT(YEAR FROM {column}) = EXTRACT(YEAR FROM CURRENT_DATE)"
            case "is_anniversary":
                return (
                    f"(EXTRACT(MONTH FROM {column}) = EXTRACT(MONTH FROM CURRENT_DATE) "
                    f"AND EXTRACT(DAY FROM {column}) = EXTRACT(DAY FROM CURRENT_DATE))"
                )
            case "day_of_week_is":
                return f"EXTRACT(DOW FROM {column}) = {self._quote(value)}"

            # Boolean
            case "is_true":
                return f"{column} = TRUE"
            case "is_false":
                return f"{column} = FALSE"

            # Array (PostgreSQL array ops)
            case "contains_any":
                if isinstance(value, list):
                    arr = "ARRAY[" + ",".join(self._quote(v) for v in value) + "]"
                    return f"{column} && {arr}"
                return f"{self._quote(value)} = ANY({column})"
            case "contains_all":
                if isinstance(value, list):
                    arr = "ARRAY[" + ",".join(self._quote(v) for v in value) + "]"
                    return f"{column} @> {arr}"
                return f"{self._quote(value)} = ANY({column})"
            case "array_length_equals":
                return f"array_length({column}, 1) = {int(value)}"
            case "array_length_greater_than":
                return f"array_length({column}, 1) > {int(value)}"
            case "array_length_less_than":
                return f"array_length({column}, 1) < {int(value)}"

            # Existence
            case "exists":
                return f"{column} IS NOT NULL"
            case "not_exists":
                return f"{column} IS NULL"

            case _:
                raise ValueError(f"Unsupported operator: {operator}")


# =============================================================================
# Segment Set Operations: Union, Overlap (Intersection), Exclude
# =============================================================================

def compile_set_operation(
    operation: str,  # "union", "overlap", "exclude_overlap", "exclude"
    segment_sqls: list[str],
) -> str:
    """
    Combine multiple compiled segment queries using set operations.

    - union: OR - all profiles from any segment
    - overlap: AND - only profiles in ALL segments (intersection)
    - exclude_overlap: profiles in first segment but NOT in any overlapping set
    - exclude: profiles in first segment but NOT in subsequent segments
    """
    if not segment_sqls:
        return "SELECT NULL AS customer_id WHERE FALSE"

    if len(segment_sqls) == 1:
        return segment_sqls[0]

    match operation:
        case "union":
            return "\nUNION\n".join(f"({sql})" for sql in segment_sqls)

        case "overlap":
            # Intersection via INTERSECT
            return "\nINTERSECT\n".join(f"({sql})" for sql in segment_sqls)

        case "exclude_overlap":
            # First segment EXCEPT those in any other segment
            base = segment_sqls[0]
            others = "\nUNION\n".join(f"({sql})" for sql in segment_sqls[1:])
            return f"({base})\nEXCEPT\n({others})"

        case "exclude":
            # First segment EXCEPT all others
            base = segment_sqls[0]
            others = "\nUNION\n".join(f"({sql})" for sql in segment_sqls[1:])
            return f"({base})\nEXCEPT\n({others})"

        case _:
            raise ValueError(f"Unknown set operation: {operation}")


def compile_set_operation_count(operation: str, segment_sqls: list[str]) -> str:
    """Wrap set operation result in a COUNT query."""
    inner = compile_set_operation(operation, segment_sqls)
    return f"SELECT COUNT(*) AS audience_count FROM (\n{inner}\n) combined_results"


# =============================================================================
# Rank & Split
# =============================================================================

def compile_ranked(
    base_sql: str,
    rank_attribute: str,
    rank_order: str = "desc",
    profile_limit: int | None = None,
) -> str:
    """
    Rank a segment's profiles by a numeric attribute.
    Optionally limit to top/bottom N profiles.
    """
    direction = "DESC" if rank_order == "desc" else "ASC"
    sql = (
        f"SELECT *, ROW_NUMBER() OVER (ORDER BY {rank_attribute} {direction} NULLS LAST) AS rank_position\n"
        f"FROM (\n{base_sql}\n) ranked_base"
    )
    if profile_limit:
        sql += f"\nORDER BY rank_position\nLIMIT {int(profile_limit)}"
    return sql


def compile_split(
    base_sql: str,
    split_type: str,  # "percent" or "attribute"
    split_config: dict,
) -> list[dict]:
    """
    Split a segment into sub-audiences.

    For percent splits: {"splits": [{"name": "Split 1", "percent": 25}, ...]}
    For attribute splits: {"attribute": "geo.state", "splits": [{"name": "WB", "value": "West Bengal"}, ...]}

    Returns list of {"name": str, "sql": str} for each split.
    """
    results = []

    if split_type == "percent":
        splits = split_config.get("splits", [])
        cumulative = 0
        total_splits = len(splits)
        for i, split in enumerate(splits):
            pct = split.get("percent", 100 // total_splits)
            lower = cumulative
            upper = cumulative + pct
            # Use NTILE for even distribution
            split_sql = (
                f"SELECT * FROM (\n"
                f"  SELECT *, NTILE(100) OVER (ORDER BY customer_id) AS pct_bucket\n"
                f"  FROM (\n{base_sql}\n  ) base\n"
                f") split_base\n"
                f"WHERE pct_bucket > {lower} AND pct_bucket <= {upper}"
            )
            results.append({
                "name": split.get("name", f"Split {i+1}"),
                "sql": split_sql,
                "percent": pct,
            })
            cumulative = upper

    elif split_type == "attribute":
        attribute = split_config.get("attribute", "")
        splits = split_config.get("splits", [])
        used_values = []
        for i, split in enumerate(splits):
            val = split.get("value", "")
            used_values.append(val)
            # Resolve column from schema mapping
            col = SPENCERS_SCHEMA_MAP.get(attribute, f"ba.{attribute.split('.')[-1]}")
            split_sql = (
                f"SELECT * FROM (\n{base_sql}\n) base\n"
                f"/* JOIN for split attribute */\n"
                f"WHERE {col} = '{val}'"
            )
            results.append({
                "name": split.get("name", f"Split {i+1}"),
                "sql": split_sql,
                "value": val,
            })

        # Remainder split (everything NOT in named splits)
        if used_values:
            col = SPENCERS_SCHEMA_MAP.get(attribute, f"ba.{attribute.split('.')[-1]}")
            vals = ", ".join(f"'{v}'" for v in used_values)
            remainder_sql = (
                f"SELECT * FROM (\n{base_sql}\n) base\n"
                f"WHERE {col} NOT IN ({vals}) OR {col} IS NULL"
            )
            results.append({
                "name": "Remainder",
                "sql": remainder_sql,
                "value": "__remainder__",
            })

    return results

"""
Athena SQL Compiler
====================

Compiles the segment rule tree (ConditionGroup) into optimized Athena/Presto SQL.

Design principles:
- Generates parameterized-safe SQL (values escaped, no injection)
- Uses CTEs for readability and Athena query plan optimization
- Supports federated queries across brand data lakes
- Generates lightweight queries — minimal full-table scans
- Leverages partition pruning on date columns where possible
"""

from __future__ import annotations

from datetime import date, timedelta
from typing import Any

from app.schemas.profile_attributes import ATTRIBUTE_BY_KEY, AttributeDataType
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


class AthenaCompiler:
    """
    Compiles a SegmentDefinition into Athena SQL.

    Usage:
        compiler = AthenaCompiler(
            brand_code="spencers",
            database="spencers_gold",
            schema_mapping={...}  # brand-specific column overrides
        )
        sql = compiler.compile(segment_definition)
    """

    # Attribute prefixes that require JOINs to specific tables
    BASKET_PREFIXES = ("basket.", "store.")
    BILLING_PREFIXES = ("billing.",)
    COMPLAINT_PREFIXES = ("complaint.",)
    DIGITAL_PREFIXES = ("digital.",)
    CX_PREFIXES = ("cx.",)

    def __init__(
        self,
        brand_code: str,
        database: str,
        schema_mapping: dict[str, str] | None = None,
        customers_table: str = "customers",
        transactions_table: str = "transactions",
        events_table: str = "events",
        products_table: str = "products",
        line_items_table: str = "line_items",
        bills_table: str = "bills",
        store_visits_table: str = "store_visits",
        billing_table: str = "utility_billing",
        complaints_table: str = "complaints",
        digital_interactions_table: str = "digital_interactions",
    ):
        self.brand_code = brand_code
        self.database = database
        self.schema_mapping = schema_mapping or {}
        self.customers_table = customers_table
        self.transactions_table = transactions_table
        self.events_table = events_table
        self.products_table = products_table
        self.line_items_table = line_items_table
        self.bills_table = bills_table
        self.store_visits_table = store_visits_table
        self.billing_table = billing_table
        self.complaints_table = complaints_table
        self.digital_interactions_table = digital_interactions_table
        self._cte_counter = 0
        self._ctes: list[str] = []
        self._joins: list[str] = []
        self._event_subqueries: list[str] = []

    def compile(self, definition: SegmentDefinition) -> str:
        """Compile a full segment definition into Athena SQL."""
        self._cte_counter = 0
        self._ctes = []
        self._joins = []
        self._event_subqueries = []

        where_clause = self._compile_group(definition.root)

        # Build the final query
        sql_parts = []

        # CTEs
        if self._ctes:
            sql_parts.append("WITH " + ",\n".join(self._ctes))

        # Main SELECT
        sql_parts.append(
            f"SELECT DISTINCT c.customer_id\n"
            f"FROM {self._fqn(self.customers_table)} c"
        )

        # JOINs for event-based conditions
        for join in self._joins:
            sql_parts.append(join)

        # WHERE
        if where_clause:
            sql_parts.append(f"WHERE {where_clause}")

        # ORDER BY
        if definition.order_by:
            col = self._resolve_column(definition.order_by)
            sql_parts.append(f"ORDER BY {col} {definition.order_direction.upper()}")

        # LIMIT
        if definition.limit:
            sql_parts.append(f"LIMIT {int(definition.limit)}")

        return "\n".join(sql_parts)

    def compile_count(self, definition: SegmentDefinition) -> str:
        """Compile a COUNT query for audience size estimation."""
        inner = self.compile(definition)
        return f"SELECT COUNT(*) AS audience_count FROM (\n{inner}\n) segment_results"

    def compile_preview(self, definition: SegmentDefinition, limit: int = 100) -> str:
        """Compile a preview query returning sample profiles with key attributes."""
        self._cte_counter = 0
        self._ctes = []
        self._joins = []

        where_clause = self._compile_group(definition.root)

        sql_parts = []
        if self._ctes:
            sql_parts.append("WITH " + ",\n".join(self._ctes))

        sql_parts.append(
            f"SELECT\n"
            f"  c.customer_id,\n"
            f"  c.{self._map('identity.email', 'email')} AS email,\n"
            f"  c.{self._map('demographic.first_name', 'first_name')} AS first_name,\n"
            f"  c.{self._map('demographic.last_name', 'last_name')} AS last_name,\n"
            f"  c.{self._map('geo.city', 'city')} AS city,\n"
            f"  c.{self._map('geo.state', 'state')} AS state\n"
            f"FROM {self._fqn(self.customers_table)} c"
        )

        for join in self._joins:
            sql_parts.append(join)

        if where_clause:
            sql_parts.append(f"WHERE {where_clause}")

        sql_parts.append(f"LIMIT {int(limit)}")

        return "\n".join(sql_parts)

    # =========================================================================
    # INTERNAL: Condition compilers
    # =========================================================================

    def _compile_group(self, group: ConditionGroup) -> str:
        """Compile a condition group into a WHERE clause fragment."""
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
                
                op_val = op.value.upper() if hasattr(op, "value") else str(op).upper()
                joiner = f" {op_val} "
                sql = f"({sql}{joiner}({compiled}))"

        if not sql:
            return "1=1"
        return sql

    def _compile_condition(self, condition: ConditionType) -> str:
        """Dispatch to the right compiler based on condition type."""
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
        """Compile an attribute condition into SQL.

        For domain-specific attributes (basket/store/billing/complaint/digital/cx),
        generates CTEs that aggregate from the appropriate source tables and JOINs
        back to the customers table.
        """
        key = cond.attribute_key

        # Check if this attribute needs a domain-specific CTE
        if key.startswith(self.BASKET_PREFIXES):
            return self._compile_basket_attribute(cond)
        elif key.startswith(self.BILLING_PREFIXES):
            return self._compile_billing_attribute(cond)
        elif key.startswith(self.COMPLAINT_PREFIXES):
            return self._compile_complaint_attribute(cond)
        elif key.startswith(self.DIGITAL_PREFIXES):
            return self._compile_digital_attribute(cond)
        elif key.startswith(self.CX_PREFIXES):
            return self._compile_cx_attribute(cond)

        col = self._resolve_column(cond.attribute_key)
        op = cond.operator
        val = cond.value
        val2 = cond.second_value

        sql = self._operator_to_sql(col, op, val, val2, cond.attribute_key)

        if cond.negate:
            sql = f"NOT ({sql})"

        return sql

    # =========================================================================
    # DOMAIN-SPECIFIC: Basket / Offline Store (Spencers, NBL)
    # =========================================================================

    def _compile_basket_attribute(self, cond: AttributeCondition) -> str:
        """Compile basket/store attributes via CTE on transactions + line_items."""
        self._cte_counter += 1
        cte_name = f"basket_{self._cte_counter}"
        col_name = cond.attribute_key.split(".", 1)[1]

        # Map attribute keys to their aggregation SQL over the bills/line_items tables
        BASKET_AGG_MAP = {
            # Store visit attributes (aggregate from transactions/bills)
            "total_store_visits": "COUNT(DISTINCT t.bill_id)",
            "store_visits_30d": "COUNT(DISTINCT CASE WHEN t.bill_date >= DATE_ADD('day', -30, CURRENT_DATE) THEN t.bill_id END)",
            "store_visits_90d": "COUNT(DISTINCT CASE WHEN t.bill_date >= DATE_ADD('day', -90, CURRENT_DATE) THEN t.bill_id END)",
            "last_store_visit_date": "MAX(t.bill_date)",
            "days_since_last_store_visit": "DATE_DIFF('day', MAX(t.bill_date), CURRENT_DATE)",
            "home_store_id": "APPROX_MOST_FREQUENT(t.store_id, 1)",
            "distinct_stores_visited": "COUNT(DISTINCT t.store_id)",
            "is_multi_store_shopper": "CASE WHEN COUNT(DISTINCT t.store_id) > 1 THEN TRUE ELSE FALSE END",
            "in_store_revenue_total": "SUM(CASE WHEN t.channel = 'in_store' THEN t.bill_total ELSE 0 END)",
            "in_store_aov": "AVG(CASE WHEN t.channel = 'in_store' THEN t.bill_total END)",
            "is_store_only_customer": "CASE WHEN COUNT(DISTINCT t.channel) = 1 AND MIN(t.channel) = 'in_store' THEN TRUE ELSE FALSE END",
            "is_weekend_shopper": "CASE WHEN SUM(CASE WHEN DAY_OF_WEEK(t.bill_date) IN (6,7) THEN 1 ELSE 0 END) * 1.0 / NULLIF(COUNT(*), 0) > 0.5 THEN TRUE ELSE FALSE END",
            "weekend_visit_pct": "SUM(CASE WHEN DAY_OF_WEEK(t.bill_date) IN (6,7) THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0)",
            # Basket line-item attributes (aggregate from line_items)
            "avg_items_per_bill": "AVG(t.line_item_count)",
            "avg_basket_value": "AVG(t.bill_total)",
            "max_basket_value": "MAX(t.bill_total)",
            "avg_unit_price": "SUM(t.bill_total) / NULLIF(SUM(t.line_item_count), 0)",
            "distinct_skus_purchased": "COUNT(DISTINCT li.sku_id)",
            "total_quantity_purchased": "SUM(li.quantity)",
            "fresh_produce_pct": "SUM(CASE WHEN li.department = 'fresh_produce' THEN li.line_total ELSE 0 END) * 100.0 / NULLIF(SUM(li.line_total), 0)",
            "packaged_food_pct": "SUM(CASE WHEN li.department = 'packaged_food' THEN li.line_total ELSE 0 END) * 100.0 / NULLIF(SUM(li.line_total), 0)",
            "non_food_pct": "SUM(CASE WHEN li.department NOT IN ('fresh_produce','packaged_food','beverages') THEN li.line_total ELSE 0 END) * 100.0 / NULLIF(SUM(li.line_total), 0)",
            "private_label_pct": "SUM(CASE WHEN li.is_private_label = TRUE THEN li.line_total ELSE 0 END) * 100.0 / NULLIF(SUM(li.line_total), 0)",
            "promo_items_pct": "SUM(CASE WHEN li.is_promo = TRUE THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(li.sku_id), 0)",
            "has_alcohol": "MAX(CASE WHEN li.category = 'alcohol' THEN TRUE ELSE FALSE END)",
            "has_baby_products": "MAX(CASE WHEN li.category = 'baby_care' THEN TRUE ELSE FALSE END)",
            "has_pet_products": "MAX(CASE WHEN li.category = 'pet_care' THEN TRUE ELSE FALSE END)",
            "has_health_wellness": "MAX(CASE WHEN li.category IN ('health','wellness','supplements') THEN TRUE ELSE FALSE END)",
            "avg_bill_discount_pct": "AVG(t.discount_amount * 100.0 / NULLIF(t.bill_total + t.discount_amount, 0))",
        }

        agg_expr = BASKET_AGG_MAP.get(col_name)
        needs_line_items = col_name in (
            "distinct_skus_purchased", "total_quantity_purchased", "fresh_produce_pct",
            "packaged_food_pct", "non_food_pct", "private_label_pct", "promo_items_pct",
            "has_alcohol", "has_baby_products", "has_pet_products", "has_health_wellness",
            "avg_unit_price",
        )

        if agg_expr:
            li_join = ""
            if needs_line_items:
                li_join = f"\n    LEFT JOIN {self._fqn(self.line_items_table)} li ON t.bill_id = li.bill_id"

            cte_sql = (
                f"{cte_name} AS (\n"
                f"  SELECT t.customer_id, {agg_expr} AS {col_name}\n"
                f"  FROM {self._fqn(self.transactions_table)} t{li_join}\n"
                f"  WHERE t.channel = 'in_store'\n"
                f"  GROUP BY t.customer_id\n"
                f")"
            )
            self._ctes.append(cte_sql)
            self._joins.append(
                f"LEFT JOIN {cte_name} ON {cte_name}.customer_id = c.customer_id"
            )
            col_ref = f"{cte_name}.{col_name}"
        else:
            # Fall back to direct column on customers table (pre-computed)
            col_ref = self._resolve_column(cond.attribute_key)

        sql = self._operator_to_sql(col_ref, cond.operator, cond.value, cond.second_value, cond.attribute_key)
        if cond.negate:
            sql = f"NOT ({sql})"
        return sql

    # =========================================================================
    # DOMAIN-SPECIFIC: Utility Billing (Power CESC)
    # =========================================================================

    def _compile_billing_attribute(self, cond: AttributeCondition) -> str:
        """Compile billing attributes via CTE on utility_billing table."""
        self._cte_counter += 1
        cte_name = f"bill_{self._cte_counter}"
        col_name = cond.attribute_key.split(".", 1)[1]

        BILLING_AGG_MAP = {
            "total_bills_generated": "COUNT(*)",
            "current_outstanding": "SUM(CASE WHEN b.is_current = TRUE THEN b.outstanding_amount ELSE 0 END)",
            "total_outstanding": "SUM(b.outstanding_amount)",
            "last_bill_amount": "MAX_BY(b.bill_amount, b.bill_date)",
            "last_bill_date": "MAX(b.bill_date)",
            "last_payment_date": "MAX(b.payment_date)",
            "last_payment_amount": "MAX_BY(b.payment_amount, b.payment_date)",
            "avg_bill_amount_12m": "AVG(CASE WHEN b.bill_date >= DATE_ADD('month', -12, CURRENT_DATE) THEN b.bill_amount END)",
            "consumption_kwh_last_month": "MAX_BY(b.consumption_kwh, b.bill_date)",
            "on_time_payment_rate": (
                "SUM(CASE WHEN b.payment_date <= b.due_date THEN 1 ELSE 0 END) * 100.0 / "
                "NULLIF(COUNT(CASE WHEN b.payment_date IS NOT NULL THEN 1 END), 0)"
            ),
            "late_payment_count_12m": (
                "SUM(CASE WHEN b.bill_date >= DATE_ADD('month', -12, CURRENT_DATE) "
                "AND b.payment_date > b.due_date THEN 1 ELSE 0 END)"
            ),
            "avg_days_to_pay": "AVG(DATE_DIFF('day', b.bill_date, b.payment_date))",
            "has_partial_payments": "MAX(CASE WHEN b.payment_amount < b.bill_amount AND b.payment_amount > 0 THEN TRUE ELSE FALSE END)",
            "disconnection_notice_count": "SUM(CASE WHEN b.disconnection_notice = TRUE THEN 1 ELSE 0 END)",
        }

        agg_expr = BILLING_AGG_MAP.get(col_name)
        if agg_expr:
            cte_sql = (
                f"{cte_name} AS (\n"
                f"  SELECT b.customer_id, {agg_expr} AS {col_name}\n"
                f"  FROM {self._fqn(self.billing_table)} b\n"
                f"  GROUP BY b.customer_id\n"
                f")"
            )
            self._ctes.append(cte_sql)
            self._joins.append(f"LEFT JOIN {cte_name} ON {cte_name}.customer_id = c.customer_id")
            col_ref = f"{cte_name}.{col_name}"
        else:
            col_ref = self._resolve_column(cond.attribute_key)

        sql = self._operator_to_sql(col_ref, cond.operator, cond.value, cond.second_value, cond.attribute_key)
        if cond.negate:
            sql = f"NOT ({sql})"
        return sql

    # =========================================================================
    # DOMAIN-SPECIFIC: Complaints (Power CESC)
    # =========================================================================

    def _compile_complaint_attribute(self, cond: AttributeCondition) -> str:
        """Compile complaint attributes via CTE on complaints table."""
        self._cte_counter += 1
        cte_name = f"cmp_{self._cte_counter}"
        col_name = cond.attribute_key.split(".", 1)[1]

        COMPLAINT_AGG_MAP = {
            "total_complaints": "COUNT(*)",
            "complaints_12m": "SUM(CASE WHEN cr.created_at >= DATE_ADD('month', -12, CURRENT_DATE) THEN 1 ELSE 0 END)",
            "open_complaints": "SUM(CASE WHEN cr.status IN ('open','in_progress','assigned','escalated') THEN 1 ELSE 0 END)",
            "last_complaint_date": "MAX(cr.created_at)",
            "last_complaint_type": "MAX_BY(cr.complaint_type, cr.created_at)",
            "last_complaint_status": "MAX_BY(cr.status, cr.created_at)",
            "avg_resolution_time_hours": (
                "AVG(CASE WHEN cr.resolved_at IS NOT NULL THEN "
                "DATE_DIFF('hour', cr.created_at, cr.resolved_at) END)"
            ),
            "escalation_count": "SUM(CASE WHEN cr.is_escalated = TRUE THEN 1 ELSE 0 END)",
            "escalation_rate": (
                "SUM(CASE WHEN cr.is_escalated = TRUE THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0)"
            ),
            "outage_complaints_12m": (
                "SUM(CASE WHEN cr.complaint_type = 'power_outage' "
                "AND cr.created_at >= DATE_ADD('month', -12, CURRENT_DATE) THEN 1 ELSE 0 END)"
            ),
            "billing_dispute_count": "SUM(CASE WHEN cr.complaint_type = 'billing_dispute' THEN 1 ELSE 0 END)",
            "has_regulatory_complaint": "MAX(CASE WHEN cr.complaint_type = 'regulatory' THEN TRUE ELSE FALSE END)",
        }

        agg_expr = COMPLAINT_AGG_MAP.get(col_name)
        if agg_expr:
            cte_sql = (
                f"{cte_name} AS (\n"
                f"  SELECT cr.customer_id, {agg_expr} AS {col_name}\n"
                f"  FROM {self._fqn(self.complaints_table)} cr\n"
                f"  GROUP BY cr.customer_id\n"
                f")"
            )
            self._ctes.append(cte_sql)
            self._joins.append(f"LEFT JOIN {cte_name} ON {cte_name}.customer_id = c.customer_id")
            col_ref = f"{cte_name}.{col_name}"
        else:
            col_ref = self._resolve_column(cond.attribute_key)

        sql = self._operator_to_sql(col_ref, cond.operator, cond.value, cond.second_value, cond.attribute_key)
        if cond.negate:
            sql = f"NOT ({sql})"
        return sql

    # =========================================================================
    # DOMAIN-SPECIFIC: Digital Adoption (Power CESC)
    # =========================================================================

    def _compile_digital_attribute(self, cond: AttributeCondition) -> str:
        """Compile digital adoption attributes — most are on customer profile, some need CTE."""
        self._cte_counter += 1
        cte_name = f"digi_{self._cte_counter}"
        col_name = cond.attribute_key.split(".", 1)[1]

        DIGITAL_AGG_MAP = {
            "portal_login_count_90d": (
                "SUM(CASE WHEN di.channel = 'portal' AND di.event_date >= DATE_ADD('day', -90, CURRENT_DATE) THEN 1 ELSE 0 END)"
            ),
            "app_login_count_90d": (
                "SUM(CASE WHEN di.channel = 'app' AND di.event_date >= DATE_ADD('day', -90, CURRENT_DATE) THEN 1 ELSE 0 END)"
            ),
            "last_digital_login_date": "MAX(di.event_date)",
            "self_service_usage_count_90d": (
                "SUM(CASE WHEN di.action_type = 'self_service' AND di.event_date >= DATE_ADD('day', -90, CURRENT_DATE) THEN 1 ELSE 0 END)"
            ),
            "whatsapp_bot_interactions": (
                "SUM(CASE WHEN di.channel = 'whatsapp' THEN 1 ELSE 0 END)"
            ),
        }

        agg_expr = DIGITAL_AGG_MAP.get(col_name)
        if agg_expr:
            cte_sql = (
                f"{cte_name} AS (\n"
                f"  SELECT di.customer_id, {agg_expr} AS {col_name}\n"
                f"  FROM {self._fqn(self.digital_interactions_table)} di\n"
                f"  GROUP BY di.customer_id\n"
                f")"
            )
            self._ctes.append(cte_sql)
            self._joins.append(f"LEFT JOIN {cte_name} ON {cte_name}.customer_id = c.customer_id")
            col_ref = f"{cte_name}.{col_name}"
        else:
            # Boolean flags like has_portal_account, has_paperless_billing are on customer profile
            col_ref = self._resolve_column(cond.attribute_key)

        sql = self._operator_to_sql(col_ref, cond.operator, cond.value, cond.second_value, cond.attribute_key)
        if cond.negate:
            sql = f"NOT ({sql})"
        return sql

    # =========================================================================
    # DOMAIN-SPECIFIC: CX Scoring
    # =========================================================================

    def _compile_cx_attribute(self, cond: AttributeCondition) -> str:
        """CX score attributes are pre-computed on the customer profile table."""
        col = self._resolve_column(cond.attribute_key)
        sql = self._operator_to_sql(col, cond.operator, cond.value, cond.second_value, cond.attribute_key)
        if cond.negate:
            sql = f"NOT ({sql})"
        return sql

    def _compile_event(self, cond: EventCondition) -> str:
        """Compile an event-based condition using a subquery or CTE."""
        self._cte_counter += 1
        cte_name = f"evt_{self._cte_counter}"

        # Build the event subquery
        time_filter = self._time_window_sql("e.event_timestamp", cond.time_window)

        property_filters = ""
        if cond.event_property_filters:
            pf_parts = []
            for pf in cond.event_property_filters:
                pf_sql = self._operator_to_sql(
                    f"e.properties['{pf.property_name}']",
                    pf.operator,
                    pf.value,
                    None,
                    None,
                )
                pf_parts.append(pf_sql)
            property_filters = " AND " + " AND ".join(pf_parts)

        cte_sql = (
            f"{cte_name} AS (\n"
            f"  SELECT customer_id, COUNT(*) AS event_count\n"
            f"  FROM {self._fqn(self.events_table)} e\n"
            f"  WHERE e.event_name = {self._quote(cond.event_name)}\n"
            f"  {f'AND {time_filter}' if time_filter else ''}"
            f"  {property_filters}\n"
            f"  GROUP BY customer_id\n"
            f")"
        )
        self._ctes.append(cte_sql)

        # Build the join and condition
        op = cond.operator
        if op == "has_performed":
            self._joins.append(f"INNER JOIN {cte_name} ON {cte_name}.customer_id = c.customer_id")
            return "1=1"
        elif op == "has_not_performed":
            self._joins.append(f"LEFT JOIN {cte_name} ON {cte_name}.customer_id = c.customer_id")
            return f"{cte_name}.customer_id IS NULL"
        elif op.startswith("performed_count_"):
            self._joins.append(f"LEFT JOIN {cte_name} ON {cte_name}.customer_id = c.customer_id")
            count_op = op.replace("performed_count_", "")
            return self._operator_to_sql(
                f"COALESCE({cte_name}.event_count, 0)",
                count_op,
                cond.count_value,
                None,
                None,
            )
        else:
            self._joins.append(f"LEFT JOIN {cte_name} ON {cte_name}.customer_id = c.customer_id")
            return f"{cte_name}.event_count > 0"

    def _compile_segment_membership(self, cond: SegmentMembershipCondition) -> str:
        """Compile a segment-of-segments condition."""
        # References a pre-computed segment results table
        table = f"segment_results_{cond.segment_id.replace('-', '_')}"
        if cond.operator == "is_member":
            return f"c.customer_id IN (SELECT customer_id FROM {self._fqn(table)})"
        else:
            return f"c.customer_id NOT IN (SELECT customer_id FROM {self._fqn(table)})"

    def _compile_cross_brand(self, cond: CrossBrandCondition) -> str:
        """Compile a cross-brand condition using federated query."""
        # Cross-brand queries reference the corporate database
        self._cte_counter += 1
        cte_name = f"xbrand_{self._cte_counter}"

        inner_compiler = AthenaCompiler(
            brand_code=cond.brand_code,
            database=f"{cond.brand_code}_gold",
            schema_mapping={},
        )

        if isinstance(cond.condition, AttributeCondition):
            inner_where = inner_compiler._compile_attribute(cond.condition)
        elif isinstance(cond.condition, EventCondition):
            inner_where = inner_compiler._compile_event(cond.condition)
        else:
            raise ValueError("Cross-brand conditions only support attribute and event conditions")

        cte_sql = (
            f"{cte_name} AS (\n"
            f"  SELECT corporate_id\n"
            f"  FROM {cond.brand_code}_gold.customers\n"
            f"  WHERE {inner_where}\n"
            f")"
        )
        self._ctes.append(cte_sql)
        self._joins.append(f"INNER JOIN {cte_name} ON c.corporate_id = {cte_name}.corporate_id")
        return "1=1"

    # =========================================================================
    # INTERNAL: SQL helpers
    # =========================================================================

    def _fqn(self, table: str) -> str:
        """Fully qualified table name."""
        return f"{self.database}.{table}"

    def _resolve_column(self, attribute_key: str) -> str:
        """Resolve a canonical attribute key to its actual column reference."""
        # Check schema mapping first
        if attribute_key in self.schema_mapping:
            return f"c.{self.schema_mapping[attribute_key]}"

        # Otherwise derive from the key: "txn.total_revenue" -> "c.total_revenue"
        parts = attribute_key.split(".", 1)
        col_name = parts[1] if len(parts) > 1 else parts[0]
        return f"c.{col_name}"

    def _map(self, attribute_key: str, default: str) -> str:
        """Map an attribute key to column name, with a fallback default."""
        if attribute_key in self.schema_mapping:
            return self.schema_mapping[attribute_key]
        return default

    def _quote(self, value: Any) -> str:
        """Safely quote a value for SQL."""
        if value is None:
            return "NULL"
        if isinstance(value, bool):
            return "TRUE" if value else "FALSE"
        if isinstance(value, (int, float)):
            return str(value)
        if isinstance(value, date):
            return f"DATE '{value.isoformat()}'"
        # String — escape single quotes
        escaped = str(value).replace("'", "''")
        return f"'{escaped}'"

    def _quote_list(self, values: list) -> str:
        """Quote a list of values for IN clauses."""
        return "(" + ", ".join(self._quote(v) for v in values) + ")"

    def _operator_to_sql(
        self,
        column: str,
        operator: str,
        value: Any,
        second_value: Any,
        attribute_key: str | None,
    ) -> str:
        """Convert an operator + value into a SQL expression."""
        match operator:
            # --- String operators ---
            case "equals":
                return f"{column} = {self._quote(value)}"
            case "not_equals":
                return f"{column} != {self._quote(value)}"
            case "contains":
                return f"{column} LIKE {self._quote(f'%{value}%')}"
            case "not_contains":
                return f"{column} NOT LIKE {self._quote(f'%{value}%')}"
            case "starts_with":
                return f"{column} LIKE {self._quote(f'{value}%')}"
            case "ends_with":
                return f"{column} LIKE {self._quote(f'%{value}')}"
            case "in_list":
                if isinstance(value, list):
                    return f"{column} IN {self._quote_list(value)}"
                return f"{column} IN ({self._quote(value)})"
            case "not_in_list":
                if isinstance(value, list):
                    return f"{column} NOT IN {self._quote_list(value)}"
                return f"{column} NOT IN ({self._quote(value)})"
            case "regex_match":
                return f"REGEXP_LIKE({column}, {self._quote(value)})"
            case "is_empty":
                return f"({column} IS NULL OR {column} = '')"
            case "is_not_empty":
                return f"({column} IS NOT NULL AND {column} != '')"

            # --- Numeric operators ---
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

            # --- Date operators ---
            case "before":
                return f"{column} < {self._quote(value)}"
            case "after":
                return f"{column} > {self._quote(value)}"
            case "in_last_n_days":
                return f"{column} >= DATE_ADD('day', -{int(value)}, CURRENT_DATE)"
            case "not_in_last_n_days":
                return f"{column} < DATE_ADD('day', -{int(value)}, CURRENT_DATE)"
            case "in_next_n_days":
                return f"{column} BETWEEN CURRENT_DATE AND DATE_ADD('day', {int(value)}, CURRENT_DATE)"
            case "is_today":
                return f"CAST({column} AS DATE) = CURRENT_DATE"
            case "is_this_week":
                return f"DATE_TRUNC('week', {column}) = DATE_TRUNC('week', CURRENT_DATE)"
            case "is_this_month":
                return f"DATE_TRUNC('month', {column}) = DATE_TRUNC('month', CURRENT_DATE)"
            case "is_this_quarter":
                return f"DATE_TRUNC('quarter', {column}) = DATE_TRUNC('quarter', CURRENT_DATE)"
            case "is_this_year":
                return f"YEAR({column}) = YEAR(CURRENT_DATE)"
            case "is_anniversary":
                return (
                    f"(MONTH({column}) = MONTH(CURRENT_DATE) "
                    f"AND DAY({column}) = DAY(CURRENT_DATE))"
                )
            case "day_of_week_is":
                return f"DAY_OF_WEEK({column}) = {self._quote(value)}"

            # --- Boolean operators ---
            case "is_true":
                return f"{column} = TRUE"
            case "is_false":
                return f"{column} = FALSE"

            # --- Array operators (Athena array functions) ---
            case "contains_any":
                if isinstance(value, list):
                    conditions = " OR ".join(
                        f"CONTAINS({column}, {self._quote(v)})" for v in value
                    )
                    return f"({conditions})"
                return f"CONTAINS({column}, {self._quote(value)})"
            case "contains_all":
                if isinstance(value, list):
                    conditions = " AND ".join(
                        f"CONTAINS({column}, {self._quote(v)})" for v in value
                    )
                    return f"({conditions})"
                return f"CONTAINS({column}, {self._quote(value)})"
            case "array_length_equals":
                return f"CARDINALITY({column}) = {int(value)}"
            case "array_length_greater_than":
                return f"CARDINALITY({column}) > {int(value)}"
            case "array_length_less_than":
                return f"CARDINALITY({column}) < {int(value)}"

            # --- Existence ---
            case "exists":
                return f"{column} IS NOT NULL"
            case "not_exists":
                return f"{column} IS NULL"

            case _:
                raise ValueError(f"Unsupported operator: {operator}")

    def _time_window_sql(self, column: str, window: TimeWindow) -> str:
        """Generate SQL for a time window filter."""
        match window.type:
            case TimeWindowType.ALL_TIME:
                return ""
            case TimeWindowType.LAST_N_DAYS:
                return f"{column} >= DATE_ADD('day', -{int(window.days)}, CURRENT_TIMESTAMP)"
            case TimeWindowType.BETWEEN_DATES:
                return (
                    f"{column} BETWEEN {self._quote(window.start_date)} "
                    f"AND {self._quote(window.end_date)}"
                )
            case TimeWindowType.BEFORE_DATE:
                return f"{column} < {self._quote(window.end_date)}"
            case TimeWindowType.AFTER_DATE:
                return f"{column} > {self._quote(window.start_date)}"
            case _:
                return ""

"""
Canonical CDP Profile Attribute Catalog
========================================

This is the unified attribute schema that maps across all brands.
Each brand's gold layer columns are mapped to these canonical attributes
via the brand schema_mapping config.

Attributes are organized into categories inspired by:
- Tealium AudienceStream CDP
- Adobe Real-Time CDP / AEP XDM
- mParticle
- Salesforce Data Cloud (formerly Salesforce CDP)
- Segment Unify
- Treasure Data

The schema supports both B2C and B2B use cases across all 4 brands.
"""

from __future__ import annotations

from datetime import date, datetime
from enum import Enum
from typing import Any, Literal

from pydantic import BaseModel, Field


# =============================================================================
# ATTRIBUTE DATA TYPES
# =============================================================================

class AttributeDataType(str, Enum):
    STRING = "string"
    INTEGER = "integer"
    FLOAT = "float"
    BOOLEAN = "boolean"
    DATE = "date"
    DATETIME = "datetime"
    ARRAY_STRING = "array_string"
    ARRAY_INTEGER = "array_integer"
    JSON = "json"


class AttributeCategory(str, Enum):
    IDENTITY = "identity"
    DEMOGRAPHIC = "demographic"
    GEOGRAPHIC = "geographic"
    PSYCHOGRAPHIC = "psychographic"
    BEHAVIORAL = "behavioral"
    TRANSACTIONAL = "transactional"
    ENGAGEMENT = "engagement"
    LIFECYCLE = "lifecycle"
    CHANNEL = "channel"
    CONSENT = "consent"
    PREDICTIVE = "predictive"
    CUSTOM = "custom"
    B2B = "b2b"
    PRODUCT_AFFINITY = "product_affinity"
    LOYALTY = "loyalty"
    SESSION = "session"
    DEVICE = "device"
    OFFLINE_STORE = "offline_store"
    BASKET = "basket"
    UTILITY_BILLING = "utility_billing"
    UTILITY_COMPLAINT = "utility_complaint"
    DIGITAL_ADOPTION = "digital_adoption"
    CUSTOMER_EXPERIENCE = "customer_experience"


# =============================================================================
# ATTRIBUTE DEFINITION
# =============================================================================

class AttributeDefinition(BaseModel):
    """Defines a single attribute available for segmentation."""

    key: str  # Canonical key, e.g. "demographic.age"
    label: str  # Human-readable label
    category: AttributeCategory
    data_type: AttributeDataType
    description: str
    operators: list[str]  # Allowed operators for this attribute
    example_values: list[Any] = []
    is_computed: bool = False  # True if derived/calculated
    is_array: bool = False
    is_b2b_only: bool = False
    applicable_brands: list[str] | None = None  # None = all brands
    unit: str | None = None  # 'days', 'INR', 'count', etc.
    source_table: str | None = None  # Which gold-layer table this comes from


# =============================================================================
# OPERATOR SETS
# =============================================================================

STRING_OPS = ["equals", "not_equals", "contains", "not_contains", "starts_with",
              "ends_with", "is_empty", "is_not_empty", "in_list", "not_in_list",
              "regex_match"]

NUMERIC_OPS = ["equals", "not_equals", "greater_than", "less_than",
               "greater_than_or_equal", "less_than_or_equal", "between",
               "not_between", "is_empty", "is_not_empty"]

DATE_OPS = ["equals", "before", "after", "between", "not_between",
            "in_last_n_days", "not_in_last_n_days", "in_next_n_days",
            "is_today", "is_this_week", "is_this_month", "is_this_quarter",
            "is_this_year", "is_anniversary", "day_of_week_is",
            "is_empty", "is_not_empty"]

BOOLEAN_OPS = ["is_true", "is_false", "is_empty"]

ARRAY_OPS = ["contains", "not_contains", "contains_any", "contains_all",
             "is_empty", "is_not_empty", "array_length_equals",
             "array_length_greater_than", "array_length_less_than"]

EXISTS_OPS = ["exists", "not_exists"]

EVENT_OPS = ["has_performed", "has_not_performed", "performed_count_equals",
             "performed_count_greater_than", "performed_count_less_than",
             "first_performed_before", "first_performed_after",
             "last_performed_before", "last_performed_after",
             "performed_in_last_n_days", "not_performed_in_last_n_days"]


# =============================================================================
# MASTER ATTRIBUTE CATALOG
# =============================================================================

ATTRIBUTE_CATALOG: list[AttributeDefinition] = [

    # =========================================================================
    # 1. IDENTITY ATTRIBUTES  (table: unified_profiles)
    # =========================================================================
    AttributeDefinition(
        key="identity.customer_id",
        label="Customer ID",
        category=AttributeCategory.IDENTITY,
        data_type=AttributeDataType.STRING,
        description="Unified customer identifier (unified_id) from identity resolution",
        operators=STRING_OPS,
        example_values=["UID-000123", "UID-004567"],
        applicable_brands=["spencers"],
        source_table="unified_profiles",
    ),
    AttributeDefinition(
        key="identity.email",
        label="Email Address",
        category=AttributeCategory.IDENTITY,
        data_type=AttributeDataType.STRING,
        description="Customer email address from unified profile",
        operators=STRING_OPS,
        example_values=["customer@example.com"],
        applicable_brands=["spencers"],
        source_table="unified_profiles",
    ),
    AttributeDefinition(
        key="identity.phone",
        label="Phone Number",
        category=AttributeCategory.IDENTITY,
        data_type=AttributeDataType.STRING,
        description="Canonical mobile number used as the primary contact identifier",
        operators=STRING_OPS,
        example_values=["9876543210", "8012345678"],
        applicable_brands=["spencers"],
        source_table="unified_profiles",
    ),
    AttributeDefinition(
        key="identity.mobile",
        label="Mobile Number",
        category=AttributeCategory.IDENTITY,
        data_type=AttributeDataType.STRING,
        description="Canonical mobile number (alias for phone)",
        operators=STRING_OPS,
        example_values=["9876543210"],
        applicable_brands=["spencers"],
        source_table="unified_profiles",
    ),
    AttributeDefinition(
        key="identity.surrogate_id",
        label="Surrogate ID",
        category=AttributeCategory.IDENTITY,
        data_type=AttributeDataType.STRING,
        description="Hash-based surrogate identifier for the customer",
        operators=STRING_OPS,
        example_values=["SRG-abc123def"],
        applicable_brands=["spencers"],
        source_table="unified_profiles",
    ),
    AttributeDefinition(
        key="identity.has_transactions",
        label="Has Transactions",
        category=AttributeCategory.IDENTITY,
        data_type=AttributeDataType.BOOLEAN,
        description="Whether the customer has any transaction history on record",
        operators=BOOLEAN_OPS,
        example_values=[True, False],
        applicable_brands=["spencers"],
        source_table="unified_profiles",
    ),
    AttributeDefinition(
        key="identity.primary_source",
        label="Primary Source",
        category=AttributeCategory.IDENTITY,
        data_type=AttributeDataType.STRING,
        description="Source system that originated this customer record (CIH, POS, or CIH+POS)",
        operators=STRING_OPS,
        example_values=["CIH", "POS", "CIH+POS"],
        applicable_brands=["spencers"],
        source_table="unified_profiles",
    ),

    # =========================================================================
    # 2. DEMOGRAPHIC ATTRIBUTES  (tables: unified_profiles + behavioral_attributes)
    # =========================================================================
    AttributeDefinition(
        key="demographic.full_name",
        label="Full Name",
        category=AttributeCategory.DEMOGRAPHIC,
        data_type=AttributeDataType.STRING,
        description="Customer display name from unified profile",
        operators=STRING_OPS,
        example_values=["Rajesh Kumar", "Priya Sharma"],
        applicable_brands=["spencers"],
        source_table="unified_profiles",
    ),
    AttributeDefinition(
        key="demographic.first_name",
        label="First Name",
        category=AttributeCategory.DEMOGRAPHIC,
        data_type=AttributeDataType.STRING,
        description="Customer first name",
        operators=STRING_OPS,
        example_values=["Rajesh", "Priya"],
        applicable_brands=["spencers"],
        source_table="unified_profiles",
    ),
    AttributeDefinition(
        key="demographic.last_name",
        label="Last Name",
        category=AttributeCategory.DEMOGRAPHIC,
        data_type=AttributeDataType.STRING,
        description="Customer last name",
        operators=STRING_OPS,
        example_values=["Kumar", "Sharma"],
        applicable_brands=["spencers"],
        source_table="unified_profiles",
    ),
    AttributeDefinition(
        key="demographic.name",
        label="Name",
        category=AttributeCategory.DEMOGRAPHIC,
        data_type=AttributeDataType.STRING,
        description="Customer display name (alias for full_name)",
        operators=STRING_OPS,
        example_values=["Rajesh Kumar"],
        applicable_brands=["spencers"],
        source_table="unified_profiles",
    ),
    AttributeDefinition(
        key="demographic.age",
        label="Age",
        category=AttributeCategory.DEMOGRAPHIC,
        data_type=AttributeDataType.INTEGER,
        description="Customer age in years",
        operators=NUMERIC_OPS,
        example_values=[25, 35, 50, 65],
        applicable_brands=["spencers"],
        unit="years",
        source_table="customer_behavioral_attributes",
    ),
    AttributeDefinition(
        key="demographic.dob",
        label="Date of Birth",
        category=AttributeCategory.DEMOGRAPHIC,
        data_type=AttributeDataType.STRING,
        description="Customer date of birth stored as text",
        operators=STRING_OPS,
        example_values=["1990-05-15", "1985-11-20"],
        applicable_brands=["spencers"],
        source_table="unified_profiles",
    ),
    AttributeDefinition(
        key="demographic.customer_group",
        label="Customer Group",
        category=AttributeCategory.DEMOGRAPHIC,
        data_type=AttributeDataType.STRING,
        description="Customer classification group from the loyalty or CRM system",
        operators=STRING_OPS,
        example_values=["General", "Gold", "Platinum"],
        applicable_brands=["spencers"],
        source_table="customer_behavioral_attributes",
    ),
    AttributeDefinition(
        key="demographic.occupation",
        label="Occupation",
        category=AttributeCategory.DEMOGRAPHIC,
        data_type=AttributeDataType.STRING,
        description="Self-declared occupation of the customer",
        operators=STRING_OPS,
        example_values=["Salaried", "Business", "Student", "Homemaker"],
        applicable_brands=["spencers"],
        source_table="customer_behavioral_attributes",
    ),
    AttributeDefinition(
        key="demographic.whatsapp",
        label="WhatsApp Number",
        category=AttributeCategory.DEMOGRAPHIC,
        data_type=AttributeDataType.STRING,
        description="WhatsApp contact number for the customer",
        operators=STRING_OPS,
        example_values=["9876543210"],
        applicable_brands=["spencers"],
        source_table="customer_behavioral_attributes",
    ),

    # =========================================================================
    # 3. GEOGRAPHIC ATTRIBUTES  (tables: unified_profiles + location_master)
    # =========================================================================
    AttributeDefinition(
        key="geo.pincode",
        label="Pincode",
        category=AttributeCategory.GEOGRAPHIC,
        data_type=AttributeDataType.STRING,
        description="Customer postal pincode",
        operators=STRING_OPS,
        example_values=["560001", "400001", "110001"],
        applicable_brands=["spencers"],
        source_table="unified_profiles",
    ),
    AttributeDefinition(
        key="geo.city",
        label="City",
        category=AttributeCategory.GEOGRAPHIC,
        data_type=AttributeDataType.STRING,
        description="Customer city from the unified profile",
        operators=STRING_OPS,
        example_values=["Bengaluru", "Mumbai", "Kolkata", "Hyderabad"],
        applicable_brands=["spencers"],
        source_table="unified_profiles",
    ),
    AttributeDefinition(
        key="geo.street",
        label="Street Address",
        category=AttributeCategory.GEOGRAPHIC,
        data_type=AttributeDataType.STRING,
        description="Customer street address",
        operators=STRING_OPS,
        example_values=["MG Road", "Park Street"],
        applicable_brands=["spencers"],
        source_table="unified_profiles",
    ),
    AttributeDefinition(
        key="geo.region",
        label="Region",
        category=AttributeCategory.GEOGRAPHIC,
        data_type=AttributeDataType.STRING,
        description="Customer region from the unified profile",
        operators=STRING_OPS,
        example_values=["South", "East", "West", "North"],
        applicable_brands=["spencers"],
        source_table="unified_profiles",
    ),
    AttributeDefinition(
        key="geo.state",
        label="State",
        category=AttributeCategory.GEOGRAPHIC,
        data_type=AttributeDataType.STRING,
        description="State of the customer's registered store (from location master)",
        operators=STRING_OPS,
        example_values=["Karnataka", "West Bengal", "Tamil Nadu", "Maharashtra"],
        applicable_brands=["spencers"],
        source_table="raw_location_master",
    ),
    AttributeDefinition(
        key="geo.zone",
        label="Store Zone",
        category=AttributeCategory.GEOGRAPHIC,
        data_type=AttributeDataType.STRING,
        description="Zone of the customer's registered store (from location master)",
        operators=STRING_OPS,
        example_values=["South", "East", "West", "North"],
        applicable_brands=["spencers"],
        source_table="raw_location_master",
    ),
    AttributeDefinition(
        key="geo.store_format",
        label="Store Format",
        category=AttributeCategory.GEOGRAPHIC,
        data_type=AttributeDataType.STRING,
        description="Format of the customer's registered store (Hyper, Daily, etc.)",
        operators=STRING_OPS,
        example_values=["Hyper", "Daily", "Express"],
        applicable_brands=["spencers"],
        source_table="raw_location_master",
    ),
    AttributeDefinition(
        key="geo.home_store_id",
        label="Home Store ID",
        category=AttributeCategory.GEOGRAPHIC,
        data_type=AttributeDataType.STRING,
        description="Store code of the customer's registered/home store",
        operators=STRING_OPS,
        example_values=["SP001", "SP045", "SP120"],
        applicable_brands=["spencers"],
        source_table="unified_profiles",
    ),

    # =========================================================================
    # 4. TRANSACTIONAL ATTRIBUTES  (table: customer_behavioral_attributes)
    # =========================================================================
    AttributeDefinition(
        key="txn.total_bills",
        label="Total Bills",
        category=AttributeCategory.TRANSACTIONAL,
        data_type=AttributeDataType.INTEGER,
        description="Total number of bills/invoices for the customer across all channels",
        operators=NUMERIC_OPS,
        example_values=[1, 5, 20, 100],
        applicable_brands=["spencers"],
        unit="count",
        source_table="customer_behavioral_attributes",
    ),
    AttributeDefinition(
        key="txn.total_visits",
        label="Total Visits",
        category=AttributeCategory.TRANSACTIONAL,
        data_type=AttributeDataType.INTEGER,
        description="Total number of distinct visits (a visit may include multiple bills)",
        operators=NUMERIC_OPS,
        example_values=[1, 10, 50],
        applicable_brands=["spencers"],
        unit="count",
        source_table="customer_behavioral_attributes",
    ),
    AttributeDefinition(
        key="txn.total_spend",
        label="Total Spend",
        category=AttributeCategory.TRANSACTIONAL,
        data_type=AttributeDataType.FLOAT,
        description="Cumulative spend across all transactions",
        operators=NUMERIC_OPS,
        example_values=[500.0, 5000.0, 50000.0],
        applicable_brands=["spencers"],
        unit="INR",
        source_table="customer_behavioral_attributes",
    ),
    AttributeDefinition(
        key="txn.spend_per_bill",
        label="Spend per Bill",
        category=AttributeCategory.TRANSACTIONAL,
        data_type=AttributeDataType.FLOAT,
        description="Average amount spent per bill (total_spend / total_bills)",
        operators=NUMERIC_OPS,
        example_values=[250.0, 800.0, 2000.0],
        applicable_brands=["spencers"],
        unit="INR",
        is_computed=True,
        source_table="customer_behavioral_attributes",
    ),
    AttributeDefinition(
        key="txn.spend_per_visit",
        label="Spend per Visit",
        category=AttributeCategory.TRANSACTIONAL,
        data_type=AttributeDataType.FLOAT,
        description="Average amount spent per visit (total_spend / total_visits)",
        operators=NUMERIC_OPS,
        example_values=[300.0, 1000.0, 3000.0],
        applicable_brands=["spencers"],
        unit="INR",
        is_computed=True,
        source_table="customer_behavioral_attributes",
    ),
    AttributeDefinition(
        key="txn.avg_items_per_bill",
        label="Avg Items per Bill",
        category=AttributeCategory.TRANSACTIONAL,
        data_type=AttributeDataType.FLOAT,
        description="Average number of line items per bill",
        operators=NUMERIC_OPS,
        example_values=[3.5, 8.0, 15.0],
        applicable_brands=["spencers"],
        unit="count",
        is_computed=True,
        source_table="customer_behavioral_attributes",
    ),
    AttributeDefinition(
        key="txn.total_discount",
        label="Total Discount",
        category=AttributeCategory.TRANSACTIONAL,
        data_type=AttributeDataType.FLOAT,
        description="Total discount amount availed across all transactions",
        operators=NUMERIC_OPS,
        example_values=[100.0, 500.0, 5000.0],
        applicable_brands=["spencers"],
        unit="INR",
        source_table="customer_behavioral_attributes",
    ),
    AttributeDefinition(
        key="txn.distinct_months",
        label="Distinct Active Months",
        category=AttributeCategory.TRANSACTIONAL,
        data_type=AttributeDataType.INTEGER,
        description="Number of distinct calendar months with at least one transaction",
        operators=NUMERIC_OPS,
        example_values=[1, 6, 12, 24],
        applicable_brands=["spencers"],
        unit="months",
        source_table="customer_behavioral_attributes",
    ),
    AttributeDefinition(
        key="txn.distinct_store_count",
        label="Distinct Stores Shopped",
        category=AttributeCategory.TRANSACTIONAL,
        data_type=AttributeDataType.INTEGER,
        description="Number of distinct stores the customer has transacted at",
        operators=NUMERIC_OPS,
        example_values=[1, 3, 5],
        applicable_brands=["spencers"],
        unit="count",
        source_table="customer_behavioral_attributes",
    ),
    AttributeDefinition(
        key="txn.distinct_article_count",
        label="Distinct Articles Purchased",
        category=AttributeCategory.TRANSACTIONAL,
        data_type=AttributeDataType.INTEGER,
        description="Number of distinct article/product codes purchased",
        operators=NUMERIC_OPS,
        example_values=[10, 50, 200],
        applicable_brands=["spencers"],
        unit="count",
        source_table="customer_behavioral_attributes",
    ),
    AttributeDefinition(
        key="txn.avg_billing_time_secs",
        label="Avg Billing Time",
        category=AttributeCategory.TRANSACTIONAL,
        data_type=AttributeDataType.FLOAT,
        description="Average time in seconds to complete a billing transaction",
        operators=NUMERIC_OPS,
        example_values=[120.0, 300.0, 600.0],
        applicable_brands=["spencers"],
        unit="seconds",
        is_computed=True,
        source_table="customer_behavioral_attributes",
    ),
    AttributeDefinition(
        key="txn.return_bill_count",
        label="Return Bill Count",
        category=AttributeCategory.TRANSACTIONAL,
        data_type=AttributeDataType.INTEGER,
        description="Number of return/refund bills",
        operators=NUMERIC_OPS,
        example_values=[0, 1, 5],
        applicable_brands=["spencers"],
        unit="count",
        source_table="customer_behavioral_attributes",
    ),
    AttributeDefinition(
        key="txn.promo_bill_count",
        label="Promo Bill Count",
        category=AttributeCategory.TRANSACTIONAL,
        data_type=AttributeDataType.INTEGER,
        description="Number of bills that included promotional items or offers",
        operators=NUMERIC_OPS,
        example_values=[0, 3, 10],
        applicable_brands=["spencers"],
        unit="count",
        source_table="customer_behavioral_attributes",
    ),
    AttributeDefinition(
        key="txn.weekend_bill_count",
        label="Weekend Bill Count",
        category=AttributeCategory.TRANSACTIONAL,
        data_type=AttributeDataType.INTEGER,
        description="Number of bills placed on weekends (Saturday/Sunday)",
        operators=NUMERIC_OPS,
        example_values=[0, 5, 20],
        applicable_brands=["spencers"],
        unit="count",
        source_table="customer_behavioral_attributes",
    ),
    AttributeDefinition(
        key="txn.wednesday_bill_count",
        label="Wednesday Bill Count",
        category=AttributeCategory.TRANSACTIONAL,
        data_type=AttributeDataType.INTEGER,
        description="Number of bills placed on Wednesdays (Spencer's promo day)",
        operators=NUMERIC_OPS,
        example_values=[0, 2, 10],
        applicable_brands=["spencers"],
        unit="count",
        source_table="customer_behavioral_attributes",
    ),

    # =========================================================================
    # 5. TEMPORAL ATTRIBUTES  (table: customer_behavioral_attributes)
    # =========================================================================
    AttributeDefinition(
        key="temporal.first_bill_date",
        label="First Bill Date",
        category=AttributeCategory.BEHAVIORAL,
        data_type=AttributeDataType.DATE,
        description="Date of the customer's first ever transaction",
        operators=DATE_OPS,
        example_values=["2022-01-15", "2023-06-01"],
        applicable_brands=["spencers"],
        source_table="customer_behavioral_attributes",
    ),
    AttributeDefinition(
        key="temporal.last_bill_date",
        label="Last Bill Date",
        category=AttributeCategory.BEHAVIORAL,
        data_type=AttributeDataType.DATE,
        description="Date of the customer's most recent transaction",
        operators=DATE_OPS,
        example_values=["2025-12-20", "2026-01-05"],
        applicable_brands=["spencers"],
        source_table="customer_behavioral_attributes",
    ),
    AttributeDefinition(
        key="temporal.recency_days",
        label="Recency (Days)",
        category=AttributeCategory.BEHAVIORAL,
        data_type=AttributeDataType.INTEGER,
        description="Number of days since the last transaction",
        operators=NUMERIC_OPS,
        example_values=[5, 30, 90, 365],
        applicable_brands=["spencers"],
        unit="days",
        is_computed=True,
        source_table="customer_behavioral_attributes",
    ),
    AttributeDefinition(
        key="temporal.tenure_days",
        label="Tenure (Days)",
        category=AttributeCategory.BEHAVIORAL,
        data_type=AttributeDataType.INTEGER,
        description="Number of days since the first transaction (customer lifetime)",
        operators=NUMERIC_OPS,
        example_values=[30, 180, 365, 730],
        applicable_brands=["spencers"],
        unit="days",
        is_computed=True,
        source_table="customer_behavioral_attributes",
    ),
    AttributeDefinition(
        key="temporal.dgbt_fs",
        label="Days Gap Between Transactions",
        category=AttributeCategory.BEHAVIORAL,
        data_type=AttributeDataType.INTEGER,
        description="Average number of days between consecutive transactions",
        operators=NUMERIC_OPS,
        example_values=[7, 15, 30, 60],
        applicable_brands=["spencers"],
        unit="days",
        is_computed=True,
        source_table="customer_behavioral_attributes",
    ),

    # =========================================================================
    # 6. LIFECYCLE & SEGMENTATION ATTRIBUTES  (table: customer_behavioral_attributes)
    # =========================================================================
    AttributeDefinition(
        key="lifecycle.l1_segment",
        label="L1 Segment",
        category=AttributeCategory.LIFECYCLE,
        data_type=AttributeDataType.STRING,
        description="Level-1 value segment based on spend and frequency (HVHF/LVHF/HVLF/LVLF)",
        operators=STRING_OPS,
        example_values=["HVHF", "LVHF", "HVLF", "LVLF"],
        applicable_brands=["spencers"],
        is_computed=True,
        source_table="customer_behavioral_attributes",
    ),
    AttributeDefinition(
        key="lifecycle.l2_segment",
        label="L2 Segment",
        category=AttributeCategory.LIFECYCLE,
        data_type=AttributeDataType.STRING,
        description="Level-2 behavioral segment (STAR, LOYAL, Win Back, New, ACTIVE, Inactive, LAPSER, Deep Lapsed)",
        operators=STRING_OPS,
        example_values=["STAR", "LOYAL", "Win Back", "New", "ACTIVE", "Inactive", "LAPSER", "Deep Lapsed"],
        applicable_brands=["spencers"],
        is_computed=True,
        source_table="customer_behavioral_attributes",
    ),
    AttributeDefinition(
        key="lifecycle.lifecycle_stage",
        label="Lifecycle Stage",
        category=AttributeCategory.LIFECYCLE,
        data_type=AttributeDataType.STRING,
        description="Customer lifecycle stage (Active, At Risk, Lapsed, Churned, Registered)",
        operators=STRING_OPS,
        example_values=["Active", "At Risk", "Lapsed", "Churned", "Registered"],
        applicable_brands=["spencers"],
        is_computed=True,
        source_table="customer_behavioral_attributes",
    ),
    AttributeDefinition(
        key="lifecycle.rfm_recency_score",
        label="RFM Recency Score",
        category=AttributeCategory.LIFECYCLE,
        data_type=AttributeDataType.INTEGER,
        description="Recency score from RFM analysis (1=worst to 5=best)",
        operators=NUMERIC_OPS,
        example_values=[1, 2, 3, 4, 5],
        applicable_brands=["spencers"],
        is_computed=True,
        source_table="customer_behavioral_attributes",
    ),
    AttributeDefinition(
        key="lifecycle.rfm_frequency_score",
        label="RFM Frequency Score",
        category=AttributeCategory.LIFECYCLE,
        data_type=AttributeDataType.INTEGER,
        description="Frequency score from RFM analysis (1=worst to 5=best)",
        operators=NUMERIC_OPS,
        example_values=[1, 2, 3, 4, 5],
        applicable_brands=["spencers"],
        is_computed=True,
        source_table="customer_behavioral_attributes",
    ),
    AttributeDefinition(
        key="lifecycle.rfm_monetary_score",
        label="RFM Monetary Score",
        category=AttributeCategory.LIFECYCLE,
        data_type=AttributeDataType.INTEGER,
        description="Monetary score from RFM analysis (1=worst to 5=best)",
        operators=NUMERIC_OPS,
        example_values=[1, 2, 3, 4, 5],
        applicable_brands=["spencers"],
        is_computed=True,
        source_table="customer_behavioral_attributes",
    ),
    AttributeDefinition(
        key="lifecycle.is_first_time_buyer",
        label="Is First-Time Buyer",
        category=AttributeCategory.LIFECYCLE,
        data_type=AttributeDataType.BOOLEAN,
        description="True if the customer has exactly one bill (computed: total_bills = 1)",
        operators=BOOLEAN_OPS,
        example_values=[True, False],
        applicable_brands=["spencers"],
        is_computed=True,
        source_table="customer_behavioral_attributes",
    ),
    AttributeDefinition(
        key="lifecycle.is_repeat_buyer",
        label="Is Repeat Buyer",
        category=AttributeCategory.LIFECYCLE,
        data_type=AttributeDataType.BOOLEAN,
        description="True if the customer has more than one bill (computed: total_bills > 1)",
        operators=BOOLEAN_OPS,
        example_values=[True, False],
        applicable_brands=["spencers"],
        is_computed=True,
        source_table="customer_behavioral_attributes",
    ),
    AttributeDefinition(
        key="lifecycle.is_active",
        label="Is Active",
        category=AttributeCategory.LIFECYCLE,
        data_type=AttributeDataType.BOOLEAN,
        description="True if the customer's lifecycle stage is 'Active' (computed)",
        operators=BOOLEAN_OPS,
        example_values=[True, False],
        applicable_brands=["spencers"],
        is_computed=True,
        source_table="customer_behavioral_attributes",
    ),
    AttributeDefinition(
        key="lifecycle.is_churned",
        label="Is Churned",
        category=AttributeCategory.LIFECYCLE,
        data_type=AttributeDataType.BOOLEAN,
        description="True if the customer's lifecycle stage is 'Churned' (computed)",
        operators=BOOLEAN_OPS,
        example_values=[True, False],
        applicable_brands=["spencers"],
        is_computed=True,
        source_table="customer_behavioral_attributes",
    ),

    # =========================================================================
    # 7. DECILE ATTRIBUTES  (table: customer_behavioral_attributes)
    # =========================================================================
    AttributeDefinition(
        key="decile.spend_decile",
        label="Spend Decile",
        category=AttributeCategory.TRANSACTIONAL,
        data_type=AttributeDataType.INTEGER,
        description="Spend-based decile ranking (1=lowest 10% to 10=top 10%)",
        operators=NUMERIC_OPS,
        example_values=[1, 3, 5, 7, 10],
        applicable_brands=["spencers"],
        is_computed=True,
        source_table="customer_behavioral_attributes",
    ),
    AttributeDefinition(
        key="decile.nob_decile",
        label="NOB Decile",
        category=AttributeCategory.TRANSACTIONAL,
        data_type=AttributeDataType.INTEGER,
        description="Number-of-bills decile ranking (1=lowest 10% to 10=top 10%)",
        operators=NUMERIC_OPS,
        example_values=[1, 3, 5, 7, 10],
        applicable_brands=["spencers"],
        is_computed=True,
        source_table="customer_behavioral_attributes",
    ),

    # =========================================================================
    # 8. STORE PREFERENCE ATTRIBUTES  (table: customer_behavioral_attributes)
    # =========================================================================
    AttributeDefinition(
        key="store.fav_store_code",
        label="Favourite Store Code",
        category=AttributeCategory.OFFLINE_STORE,
        data_type=AttributeDataType.STRING,
        description="Store code where the customer shops most frequently",
        operators=STRING_OPS,
        example_values=["SP001", "SP045"],
        applicable_brands=["spencers"],
        is_computed=True,
        source_table="customer_behavioral_attributes",
    ),
    AttributeDefinition(
        key="store.fav_store_name",
        label="Favourite Store Name",
        category=AttributeCategory.OFFLINE_STORE,
        data_type=AttributeDataType.STRING,
        description="Name of the store where the customer shops most frequently",
        operators=STRING_OPS,
        example_values=["Spencer's Indiranagar", "Spencer's Salt Lake"],
        applicable_brands=["spencers"],
        is_computed=True,
        source_table="customer_behavioral_attributes",
    ),
    AttributeDefinition(
        key="store.fav_store_type",
        label="Favourite Store Type",
        category=AttributeCategory.OFFLINE_STORE,
        data_type=AttributeDataType.STRING,
        description="Format/type of the customer's most-frequented store",
        operators=STRING_OPS,
        example_values=["Hyper", "Daily", "Express"],
        applicable_brands=["spencers"],
        is_computed=True,
        source_table="customer_behavioral_attributes",
    ),
    AttributeDefinition(
        key="store.fav_day",
        label="Favourite Shopping Day",
        category=AttributeCategory.OFFLINE_STORE,
        data_type=AttributeDataType.STRING,
        description="Day of the week the customer shops most often",
        operators=STRING_OPS,
        example_values=["Monday", "Wednesday", "Saturday", "Sunday"],
        applicable_brands=["spencers"],
        is_computed=True,
        source_table="customer_behavioral_attributes",
    ),

    # =========================================================================
    # 9. PRODUCT AFFINITY ATTRIBUTES  (table: customer_behavioral_attributes)
    # =========================================================================
    AttributeDefinition(
        key="product.fav_article_by_spend",
        label="Top Article by Spend",
        category=AttributeCategory.PRODUCT_AFFINITY,
        data_type=AttributeDataType.STRING,
        description="Article code with the highest total spend for this customer",
        operators=STRING_OPS,
        example_values=["ART-1001", "ART-2050"],
        applicable_brands=["spencers"],
        is_computed=True,
        source_table="customer_behavioral_attributes",
    ),
    AttributeDefinition(
        key="product.fav_article_by_spend_desc",
        label="Top Article by Spend (Description)",
        category=AttributeCategory.PRODUCT_AFFINITY,
        data_type=AttributeDataType.STRING,
        description="Description of the article with the highest total spend",
        operators=STRING_OPS,
        example_values=["Amul Butter 500g", "Tata Salt 1kg"],
        applicable_brands=["spencers"],
        is_computed=True,
        source_table="customer_behavioral_attributes",
    ),
    AttributeDefinition(
        key="product.fav_article_by_nob",
        label="Top Article by Frequency",
        category=AttributeCategory.PRODUCT_AFFINITY,
        data_type=AttributeDataType.STRING,
        description="Article code purchased in the most number of bills",
        operators=STRING_OPS,
        example_values=["ART-1001", "ART-3020"],
        applicable_brands=["spencers"],
        is_computed=True,
        source_table="customer_behavioral_attributes",
    ),
    AttributeDefinition(
        key="product.fav_article_by_nob_desc",
        label="Top Article by Frequency (Description)",
        category=AttributeCategory.PRODUCT_AFFINITY,
        data_type=AttributeDataType.STRING,
        description="Description of the article purchased in the most bills",
        operators=STRING_OPS,
        example_values=["Amul Milk 1L", "Britannia Bread"],
        applicable_brands=["spencers"],
        is_computed=True,
        source_table="customer_behavioral_attributes",
    ),
    AttributeDefinition(
        key="product.second_fav_article_by_spend",
        label="2nd Top Article by Spend",
        category=AttributeCategory.PRODUCT_AFFINITY,
        data_type=AttributeDataType.STRING,
        description="Article code with the second-highest total spend",
        operators=STRING_OPS,
        example_values=["ART-2050"],
        applicable_brands=["spencers"],
        is_computed=True,
        source_table="customer_behavioral_attributes",
    ),
    AttributeDefinition(
        key="product.second_fav_article_by_nob",
        label="2nd Top Article by Frequency",
        category=AttributeCategory.PRODUCT_AFFINITY,
        data_type=AttributeDataType.STRING,
        description="Article code with the second-highest purchase frequency",
        operators=STRING_OPS,
        example_values=["ART-3020"],
        applicable_brands=["spencers"],
        is_computed=True,
        source_table="customer_behavioral_attributes",
    ),

    # =========================================================================
    # 10. CHANNEL ATTRIBUTES  (table: customer_behavioral_attributes)
    # =========================================================================
    AttributeDefinition(
        key="channel.channel_presence",
        label="Channel Presence",
        category=AttributeCategory.CHANNEL,
        data_type=AttributeDataType.STRING,
        description="Customer's channel footprint: Online, Offline, or Omni",
        operators=STRING_OPS,
        example_values=["Online", "Offline", "Omni"],
        applicable_brands=["spencers"],
        is_computed=True,
        source_table="customer_behavioral_attributes",
    ),
    AttributeDefinition(
        key="channel.store_spend",
        label="In-Store Spend",
        category=AttributeCategory.CHANNEL,
        data_type=AttributeDataType.FLOAT,
        description="Total spend through offline/in-store channel",
        operators=NUMERIC_OPS,
        example_values=[500.0, 5000.0, 25000.0],
        applicable_brands=["spencers"],
        unit="INR",
        source_table="customer_behavioral_attributes",
    ),
    AttributeDefinition(
        key="channel.online_spend",
        label="Online Spend",
        category=AttributeCategory.CHANNEL,
        data_type=AttributeDataType.FLOAT,
        description="Total spend through online channel",
        operators=NUMERIC_OPS,
        example_values=[200.0, 2000.0, 10000.0],
        applicable_brands=["spencers"],
        unit="INR",
        source_table="customer_behavioral_attributes",
    ),
    AttributeDefinition(
        key="channel.store_bills",
        label="In-Store Bills",
        category=AttributeCategory.CHANNEL,
        data_type=AttributeDataType.INTEGER,
        description="Number of bills from offline/in-store channel",
        operators=NUMERIC_OPS,
        example_values=[1, 10, 50],
        applicable_brands=["spencers"],
        unit="count",
        source_table="customer_behavioral_attributes",
    ),
    AttributeDefinition(
        key="channel.online_bills",
        label="Online Bills",
        category=AttributeCategory.CHANNEL,
        data_type=AttributeDataType.INTEGER,
        description="Number of bills from online channel",
        operators=NUMERIC_OPS,
        example_values=[0, 5, 20],
        applicable_brands=["spencers"],
        unit="count",
        source_table="customer_behavioral_attributes",
    ),

    # =========================================================================
    # 11. CONSENT & COMMUNICATION ATTRIBUTES  (table: customer_behavioral_attributes)
    # =========================================================================
    AttributeDefinition(
        key="consent.dnd",
        label="DND Status",
        category=AttributeCategory.CONSENT,
        data_type=AttributeDataType.STRING,
        description="Do-Not-Disturb flag for the customer",
        operators=STRING_OPS,
        example_values=["Y", "N"],
        applicable_brands=["spencers"],
        source_table="customer_behavioral_attributes",
    ),
    AttributeDefinition(
        key="consent.accepts_email_marketing",
        label="Accepts Email Marketing",
        category=AttributeCategory.CONSENT,
        data_type=AttributeDataType.STRING,
        description="Whether the customer has opted in for email marketing",
        operators=STRING_OPS,
        example_values=["Y", "N"],
        applicable_brands=["spencers"],
        source_table="customer_behavioral_attributes",
    ),
    AttributeDefinition(
        key="consent.accepts_sms_marketing",
        label="Accepts SMS Marketing",
        category=AttributeCategory.CONSENT,
        data_type=AttributeDataType.STRING,
        description="Whether the customer has opted in for SMS marketing",
        operators=STRING_OPS,
        example_values=["Y", "N"],
        applicable_brands=["spencers"],
        source_table="customer_behavioral_attributes",
    ),
    AttributeDefinition(
        key="consent.gw_customer_flag",
        label="Gourmet West Customer Flag",
        category=AttributeCategory.CONSENT,
        data_type=AttributeDataType.STRING,
        description="Flag indicating if the customer is a Gourmet West loyalty member",
        operators=STRING_OPS,
        example_values=["Y", "N"],
        applicable_brands=["spencers"],
        source_table="customer_behavioral_attributes",
    ),

    # =========================================================================
    # 12. IDENTITY GRAPH METRICS  (table: identity_graph_summary)
    # =========================================================================
    AttributeDefinition(
        key="identity_graph.total_edge_count",
        label="Identity Graph Edge Count",
        category=AttributeCategory.IDENTITY,
        data_type=AttributeDataType.INTEGER,
        description="Total number of edges (links) in this customer's identity graph",
        operators=NUMERIC_OPS,
        example_values=[1, 3, 8],
        applicable_brands=["spencers"],
        unit="count",
        is_computed=True,
        source_table="identity_graph_summary",
    ),
    AttributeDefinition(
        key="identity_graph.distinct_id_types",
        label="Distinct ID Types",
        category=AttributeCategory.IDENTITY,
        data_type=AttributeDataType.INTEGER,
        description="Number of distinct identifier types linked to this customer (mobile, email, card, etc.)",
        operators=NUMERIC_OPS,
        example_values=[1, 2, 4],
        applicable_brands=["spencers"],
        unit="count",
        is_computed=True,
        source_table="identity_graph_summary",
    ),
    AttributeDefinition(
        key="identity_graph.source_system_count",
        label="Source System Count",
        category=AttributeCategory.IDENTITY,
        data_type=AttributeDataType.INTEGER,
        description="Number of distinct source systems contributing to this customer profile",
        operators=NUMERIC_OPS,
        example_values=[1, 2, 3],
        applicable_brands=["spencers"],
        unit="count",
        is_computed=True,
        source_table="identity_graph_summary",
    ),
    AttributeDefinition(
        key="identity_graph.source_systems",
        label="Source Systems",
        category=AttributeCategory.IDENTITY,
        data_type=AttributeDataType.STRING,
        description="Comma-separated list of source systems (e.g. CIH, POS)",
        operators=STRING_OPS,
        example_values=["CIH", "POS", "CIH,POS"],
        applicable_brands=["spencers"],
        is_computed=True,
        source_table="identity_graph_summary",
    ),
    AttributeDefinition(
        key="identity_graph.mobile_count",
        label="Mobile Numbers Linked",
        category=AttributeCategory.IDENTITY,
        data_type=AttributeDataType.INTEGER,
        description="Number of distinct mobile numbers linked to this unified profile",
        operators=NUMERIC_OPS,
        example_values=[1, 2],
        applicable_brands=["spencers"],
        unit="count",
        is_computed=True,
        source_table="identity_graph_summary",
    ),
    AttributeDefinition(
        key="identity_graph.email_count",
        label="Email Addresses Linked",
        category=AttributeCategory.IDENTITY,
        data_type=AttributeDataType.INTEGER,
        description="Number of distinct email addresses linked to this unified profile",
        operators=NUMERIC_OPS,
        example_values=[0, 1, 2],
        applicable_brands=["spencers"],
        unit="count",
        is_computed=True,
        source_table="identity_graph_summary",
    ),
    AttributeDefinition(
        key="identity_graph.store_count",
        label="Stores in Identity Graph",
        category=AttributeCategory.IDENTITY,
        data_type=AttributeDataType.INTEGER,
        description="Number of distinct stores linked in this customer's identity graph",
        operators=NUMERIC_OPS,
        example_values=[1, 3, 5],
        applicable_brands=["spencers"],
        unit="count",
        is_computed=True,
        source_table="identity_graph_summary",
    ),
    AttributeDefinition(
        key="identity_graph.avg_confidence",
        label="Avg Identity Confidence",
        category=AttributeCategory.IDENTITY,
        data_type=AttributeDataType.FLOAT,
        description="Average confidence score of identity links in the graph (0-1)",
        operators=NUMERIC_OPS,
        example_values=[0.75, 0.90, 1.0],
        applicable_brands=["spencers"],
        is_computed=True,
        source_table="identity_graph_summary",
    ),
    AttributeDefinition(
        key="identity_graph.completeness_score",
        label="Profile Completeness Score",
        category=AttributeCategory.IDENTITY,
        data_type=AttributeDataType.FLOAT,
        description="Score indicating how complete the customer profile is (0-1)",
        operators=NUMERIC_OPS,
        example_values=[0.4, 0.7, 1.0],
        applicable_brands=["spencers"],
        is_computed=True,
        source_table="identity_graph_summary",
    ),
]


# =============================================================================
# HELPER: Build lookup by category and key
# =============================================================================

ATTRIBUTE_BY_KEY: dict[str, AttributeDefinition] = {attr.key: attr for attr in ATTRIBUTE_CATALOG}

ATTRIBUTES_BY_CATEGORY: dict[AttributeCategory, list[AttributeDefinition]] = {}
for attr in ATTRIBUTE_CATALOG:
    ATTRIBUTES_BY_CATEGORY.setdefault(attr.category, []).append(attr)


def get_attributes_for_brand(brand_code: str) -> list[AttributeDefinition]:
    """Return attributes applicable to a specific brand."""
    return [
        attr for attr in ATTRIBUTE_CATALOG
        if attr.applicable_brands is None or brand_code in attr.applicable_brands
    ]


def get_attributes_by_category(
    category: AttributeCategory,
    brand_code: str | None = None,
) -> list[AttributeDefinition]:
    """Return attributes in a category, optionally filtered by brand."""
    attrs = ATTRIBUTES_BY_CATEGORY.get(category, [])
    if brand_code:
        attrs = [a for a in attrs if a.applicable_brands is None or brand_code in a.applicable_brands]
    return attrs

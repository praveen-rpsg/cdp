/**
 * Segment Builder Types
 *
 * These mirror the backend Pydantic schemas for segment rules
 * and provide the type system for the visual segment builder.
 */

// =============================================================================
// ATTRIBUTE CATALOG TYPES
// =============================================================================

export type AttributeDataType =
  | "string"
  | "integer"
  | "float"
  | "boolean"
  | "date"
  | "datetime"
  | "array_string"
  | "array_integer"
  | "json";

export type AttributeCategory =
  | "identity"
  | "demographic"
  | "geographic"
  | "psychographic"
  | "behavioral"
  | "transactional"
  | "engagement"
  | "lifecycle"
  | "channel"
  | "consent"
  | "predictive"
  | "custom"
  | "b2b"
  | "product_affinity"
  | "loyalty"
  | "session"
  | "device"
  | "offline_store"
  | "basket"
  | "utility_billing"
  | "utility_complaint"
  | "digital_adoption"
  | "customer_experience"
  | "bill_transaction";

export interface AttributeDefinition {
  key: string;
  label: string;
  category: AttributeCategory;
  data_type: AttributeDataType;
  description: string;
  operators: string[];
  example_values: any[];
  is_computed: boolean;
  is_array: boolean;
  is_b2b_only: boolean;
  applicable_brands: string[] | null;
  unit: string | null;
  source_table: string | null;
}

// =============================================================================
// SEGMENT RULE TYPES
// =============================================================================

export type LogicalOperator = "and" | "or";

export type TimeWindowType =
  | "last_n_days"
  | "between_dates"
  | "before_date"
  | "after_date"
  | "all_time";

export interface TimeWindow {
  type: TimeWindowType;
  days?: number;
  start_date?: string;
  end_date?: string;
}

export interface AttributeCondition {
  type: "attribute";
  id: string;
  attribute_key: string;
  operator: string;
  value: any;
  second_value?: any;
  time_window?: TimeWindow;
  negate: boolean;
  _initialCategory?: string;
  logical_operator?: LogicalOperator;
}

export interface EventCondition {
  type: "event";
  id: string;
  event_name: string;
  operator: string;
  count_value?: number;
  time_window: TimeWindow;
  event_property_filters?: EventPropertyFilter[];
  negate: boolean;
  logical_operator?: LogicalOperator;
}

export interface EventPropertyFilter {
  property_name: string;
  operator: string;
  value: any;
}

export interface SegmentMembershipCondition {
  type: "segment_membership";
  id: string;
  segment_id: string;
  operator: "is_member" | "is_not_member";
  logical_operator?: LogicalOperator;
}

export interface CrossBrandCondition {
  type: "cross_brand";
  id: string;
  brand_code: string;
  condition: AttributeCondition | EventCondition;
  logical_operator?: LogicalOperator;
}

export type Condition =
  | AttributeCondition
  | EventCondition
  | SegmentMembershipCondition
  | CrossBrandCondition
  | ConditionGroup;

export interface ConditionGroup {
  type: "group";
  id: string;
  logical_operator: LogicalOperator;
  logical_operator_prefix?: LogicalOperator;
  conditions: Condition[];
}

// =============================================================================
// RANK & SPLIT
// =============================================================================

export interface RankConfig {
  enabled: boolean;
  attribute: string | null;
  order: "asc" | "desc";
  profile_limit: number | null;
}

export interface SplitEntry {
  name: string;
  percent?: number;
  value?: string;
}

export interface SplitConfig {
  enabled: boolean;
  split_type: "percent" | "attribute";
  attribute: string | null;
  num_splits: number;
  splits: SplitEntry[];
}

// =============================================================================
// SET OPERATIONS (Union, Overlap, Exclude)
// =============================================================================

export type SetOperationType = "union" | "overlap" | "exclude_overlap" | "exclude";

export interface SetOperationEntry {
  segment_id?: string;
  rules?: SegmentDefinition;
  name?: string;
}

export interface SetOperation {
  enabled: boolean;
  operation: SetOperationType;
  segments: SetOperationEntry[];
}

// =============================================================================
// SEGMENT DEFINITION
// =============================================================================

export interface SegmentDefinition {
  root: ConditionGroup;
  limit?: number;
  order_by?: string;
  order_direction?: "asc" | "desc";
  rank?: RankConfig;
  split?: SplitConfig;
  set_operation?: SetOperation;
}

// =============================================================================
// SEGMENT ENTITY
// =============================================================================

export type SegmentType =
  | "static"
  | "dynamic"
  | "predictive"
  | "lookalike"
  | "lifecycle";

export interface Segment {
  id: string;
  brand_id: string | null;
  name: string;
  description: string | null;
  slug: string;
  segment_type: SegmentType;
  rules: SegmentDefinition;
  schedule: string;
  is_active: boolean;
  is_cross_brand: boolean;
  audience_count: number | null;
  computation_status: "pending" | "computing" | "ready" | "failed";
  last_computed_at: string | null;
  tags: string[];
  created_by: string | null;
  created_at: string;
}

// =============================================================================
// BRAND
// =============================================================================

export interface Brand {
  id: string;
  code: string;
  name: string;
  channels: string[];
  business_model: string;
  is_active: boolean;
}

// =============================================================================
// SPLIT COUNT RESULT
// =============================================================================

export interface SplitCountResult {
  name: string;
  count: number | null;
  percent?: number;
  value?: string;
}

// =============================================================================
// OPERATOR DISPLAY LABELS
// =============================================================================

export const OPERATOR_LABELS: Record<string, string> = {
  equals: "equals",
  not_equals: "does not equal",
  contains: "contains",
  not_contains: "does not contain",
  starts_with: "starts with",
  ends_with: "ends with",
  is_empty: "is empty",
  is_not_empty: "is not empty",
  in_list: "is one of",
  not_in_list: "is not one of",
  regex_match: "matches regex",
  greater_than: "is greater than",
  less_than: "is less than",
  greater_than_or_equal: "is at least",
  less_than_or_equal: "is at most",
  between: "is between",
  not_between: "is not between",
  before: "is before",
  after: "is after",
  in_last_n_days: "in the last N days",
  not_in_last_n_days: "not in the last N days",
  in_next_n_days: "in the next N days",
  is_today: "is today",
  is_this_week: "is this week",
  is_this_month: "is this month",
  is_this_quarter: "is this quarter",
  is_this_year: "is this year",
  is_anniversary: "is anniversary",
  day_of_week_is: "day of week is",
  is_true: "is true",
  is_false: "is false",
  has_performed: "has performed",
  has_not_performed: "has not performed",
  performed_count_equals: "performed exactly",
  performed_count_greater_than: "performed more than",
  performed_count_less_than: "performed fewer than",
  contains_any: "contains any of",
  contains_all: "contains all of",
  array_length_equals: "has exactly N items",
  array_length_greater_than: "has more than N items",
  array_length_less_than: "has fewer than N items",
  is_member: "is a member of",
  is_not_member: "is not a member of",
  exists: "exists",
  not_exists: "does not exist",
};

// =============================================================================
// SET OPERATION LABELS
// =============================================================================

export const SET_OPERATION_LABELS: Record<SetOperationType, { label: string; description: string; color: string }> = {
  union: { label: "Union", description: "Combined (OR) — all profiles from any segment", color: "#22c55e" },
  overlap: { label: "Overlap", description: "Intersection (AND) — only profiles in ALL segments", color: "#3b82f6" },
  exclude_overlap: { label: "Exclude Overlap", description: "First segment minus overlapping profiles", color: "#f59e0b" },
  exclude: { label: "Exclude", description: "First segment minus all other segments", color: "#ef4444" },
};

// =============================================================================
// CATEGORY DISPLAY CONFIG
// =============================================================================

export const CATEGORY_CONFIG: Record<
  AttributeCategory,
  { label: string; icon: string; color: string }
> = {
  bill_transaction: { label: "Bill Transactions", icon: "shopping-bag", color: "#f43f5e" },
  identity: { label: "Identity", icon: "fingerprint", color: "#6366f1" },
  demographic: { label: "Demographics", icon: "users", color: "#8b5cf6" },
  geographic: { label: "Geography", icon: "map-pin", color: "#06b6d4" },
  psychographic: { label: "Psychographic", icon: "brain", color: "#d946ef" },
  behavioral: { label: "Behavior", icon: "mouse-pointer", color: "#f59e0b" },
  transactional: { label: "Transactions", icon: "credit-card", color: "#10b981" },
  engagement: { label: "Engagement", icon: "mail", color: "#3b82f6" },
  lifecycle: { label: "Lifecycle", icon: "refresh-cw", color: "#ec4899" },
  channel: { label: "Channel", icon: "smartphone", color: "#14b8a6" },
  consent: { label: "Consent", icon: "shield", color: "#64748b" },
  predictive: { label: "Predictive / ML", icon: "trending-up", color: "#ef4444" },
  custom: { label: "Custom", icon: "settings", color: "#78716c" },
  b2b: { label: "B2B / Account", icon: "building", color: "#0ea5e9" },
  product_affinity: { label: "Product Affinity", icon: "shopping-bag", color: "#84cc16" },
  loyalty: { label: "Loyalty", icon: "award", color: "#eab308" },
  session: { label: "Session", icon: "globe", color: "#a855f7" },
  device: { label: "Device", icon: "monitor", color: "#f97316" },
  offline_store: { label: "Offline Store", icon: "store", color: "#059669" },
  basket: { label: "Basket / Line Items", icon: "shopping-cart", color: "#16a34a" },
  utility_billing: { label: "Utility Billing", icon: "zap", color: "#dc2626" },
  utility_complaint: { label: "Complaints / Service", icon: "alert-triangle", color: "#ea580c" },
  digital_adoption: { label: "Digital Adoption", icon: "wifi", color: "#7c3aed" },
  customer_experience: { label: "Customer Experience", icon: "heart", color: "#e11d48" },
};

// Rankable numeric attributes for Spencer's (per Resulticks attribute design)
export const RANKABLE_ATTRIBUTES = [
  // ── Precalculated / Bill Summary ──
  { key: "txn.total_spend", label: "Total Spend (LTV)" },
  { key: "txn.total_bills", label: "Total Bills (NOB)" },
  { key: "txn.total_visits", label: "Total Visits" },
  { key: "txn.spend_per_bill", label: "Spend Per Bill (ABV)" },
  { key: "txn.spend_per_visit", label: "Spend Per Visit" },
  { key: "txn.avg_items_per_bill", label: "Avg Items Per Bill" },
  { key: "txn.total_discount", label: "Total Discount" },
  { key: "txn.distinct_store_count", label: "Unique Stores Visited" },
  { key: "txn.distinct_article_count", label: "Distinct Articles Bought" },
  { key: "txn.distinct_months", label: "Distinct Months Active" },
  { key: "txn.avg_billing_time_secs", label: "Avg Billing Time (sec)" },
  { key: "txn.return_bill_count", label: "Return Bills" },
  { key: "txn.promo_bill_count", label: "Promo Bills" },
  { key: "txn.weekend_bill_count", label: "Weekend Bills" },
  { key: "txn.wednesday_bill_count", label: "Wednesday Bills" },
  // ── Temporal / Recency ──
  { key: "temporal.recency_days", label: "Recency (Days)" },
  { key: "temporal.tenure_days", label: "Tenure (Days)" },
  { key: "temporal.dgbt_fs", label: "DGBT (Days Gap Between Txns)" },
  // ── Decile / Ranking ──
  { key: "decile.spend_decile", label: "Spend Decile" },
  { key: "decile.nob_decile", label: "NOB Decile" },
  // ── Channel ──
  { key: "channel.store_spend", label: "Store Spend" },
  { key: "channel.online_spend", label: "Online Spend" },
  { key: "channel.store_bills", label: "Store Bills" },
  { key: "channel.online_bills", label: "Online Bills" },
];

// Splittable categorical attributes for Spencer's
export const SPLITTABLE_ATTRIBUTES = [
  // ── Lifecycle / Segmentation ──
  { key: "lifecycle.l1_segment", label: "L1 Segment (HVHF/LVHF/HVLF/LVLF)" },
  { key: "lifecycle.l2_segment", label: "L2 Segment (STAR/LOYAL/Active/Lapsed/...)" },
  { key: "lifecycle.lifecycle_stage", label: "Lifecycle Stage" },
  // ── Geographic ──
  { key: "geo.city", label: "City" },
  { key: "geo.zone", label: "Zone" },
  { key: "geo.state", label: "State" },
  { key: "geo.store_format", label: "Store Format" },
  // ── Channel ──
  { key: "channel.channel_presence", label: "Channel (Online/Offline/Omni)" },
  // ── Store / Favourites ──
  { key: "store.fav_store_type", label: "Favourite Store Type" },
  { key: "store.fav_day", label: "Favourite Day" },
  // ── Product ──
  { key: "product.fav_article_by_spend_desc", label: "Fav Article (by Spend)" },
  { key: "product.fav_article_by_nob_desc", label: "Fav Article (by NOB)" },
  // ── Consent ──
  { key: "consent.dnd", label: "DND Status" },
  { key: "consent.accepts_email_marketing", label: "Accepts Email Marketing" },
  { key: "consent.accepts_sms_marketing", label: "Accepts SMS Marketing" },
  // ── Decile ──
  { key: "decile.spend_decile", label: "Spend Decile (1-10)" },
  { key: "decile.nob_decile", label: "NOB Decile (1-10)" },
];

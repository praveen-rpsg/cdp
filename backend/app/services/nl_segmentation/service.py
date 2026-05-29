"""
Natural Language Segmentation Service
======================================

Converts plain-English audience queries into structured segment rules
using an LLM as the reasoning engine, grounded in the semantic data dictionary.

Flow:
  1. User types: "High spenders who haven't bought in 60 days in Kolkata"
  2. This service sends the query + full attribute catalog to the LLM
  3. The LLM returns a structured JSON rule tree
  4. We validate, compile to SQL via PgCompiler, execute COUNT only, and return results
     NO PII or row-level data is ever returned or processed by the LLM.

LLM backend: AWS Bedrock (boto3 bedrock-runtime converse API).
"""

from __future__ import annotations

import json
import logging
import os
import time
from typing import Any

from app.schemas.profile_attributes import ATTRIBUTE_CATALOG
from app.schemas.segment_rules import (
    AttributeCondition,
    ConditionGroup,
    LogicalOperator,
    SegmentDefinition,
)
from app.services.query_engine.pg_compiler import (
    PgCompiler,
    CorporatePgCompiler,
    SPENCERS_SCHEMA_MAP,
    CORPORATE_SCHEMA_MAP,
)


def _make_nl_compiler(brand_code: str) -> PgCompiler:
    """Return the right compiler for NL-compiled segment rules."""
    if brand_code == "corporate":
        return CorporatePgCompiler()
    return PgCompiler(brand_code=brand_code)

logger = logging.getLogger(__name__)

# AWS Bedrock pricing — USD per 1 million tokens (input / output).
# Update these when AWS publishes new rates.
_LLM_PRICING: dict[str, dict[str, float]] = {
    # ── On-demand (standard regions) ─────────────────────────────────
    "anthropic.claude-sonnet-4-20250514-v1:0":   {"input":  3.00, "output": 15.00},
    "anthropic.claude-3-5-sonnet-20241022-v2:0": {"input":  3.00, "output": 15.00},
    "anthropic.claude-3-5-haiku-20241022-v1:0":  {"input":  0.80, "output":  4.00},
    "anthropic.claude-3-haiku-20240307-v1:0":    {"input":  0.25, "output":  1.25},
    # ── Cross-region inference — APAC (~10 % surcharge) ──────────────
    "apac.anthropic.claude-sonnet-4-20250514-v1:0":   {"input":  3.30, "output": 16.50},
    "apac.anthropic.claude-3-5-sonnet-20241022-v2:0": {"input":  3.30, "output": 16.50},
    "apac.anthropic.claude-3-5-haiku-20241022-v1:0":  {"input":  0.88, "output":  4.40},
    "apac.anthropic.claude-3-haiku-20240307-v1:0":    {"input":  0.275,"output":  1.375},
    # ── Cross-region inference — US ───────────────────────────────────
    "us.anthropic.claude-sonnet-4-20250514-v1:0":   {"input":  3.00, "output": 15.00},
    "us.anthropic.claude-3-5-sonnet-20241022-v2:0": {"input":  3.00, "output": 15.00},
    "us.anthropic.claude-3-haiku-20240307-v1:0":    {"input":  0.25, "output":  1.25},
}


def _compute_cost(model: str, input_tokens: int | None, output_tokens: int | None) -> float | None:
    """Return estimated USD cost for a call, or None if the model isn't in the pricing table."""
    pricing = _LLM_PRICING.get(model)
    if not pricing or input_tokens is None or output_tokens is None:
        return None
    cost = (input_tokens / 1_000_000) * pricing["input"] + (output_tokens / 1_000_000) * pricing["output"]
    return round(cost, 6)


# Build the semantic dictionary once at module load
_SEMANTIC_DICT: str | None = None


def _build_semantic_dictionary() -> str:
    """Build a compact text representation of the attribute catalog for the LLM prompt."""
    global _SEMANTIC_DICT
    if _SEMANTIC_DICT is not None:
        return _SEMANTIC_DICT

    lines = ["# Spencer's CDP Attribute Dictionary", ""]
    lines.append("Each attribute has: key, label, type, operators, description, examples")
    lines.append("")

    current_cat = None
    for attr in ATTRIBUTE_CATALOG:
        cat = attr.category.value
        if cat != current_cat:
            lines.append(f"\n## {cat.upper()}")
            current_cat = cat
        ops_str = ", ".join(attr.operators[:6])
        if len(attr.operators) > 6:
            ops_str += f" (+{len(attr.operators) - 6} more)"
        examples = ""
        if attr.example_values:
            examples = f" | examples: {attr.example_values[:4]}"
        unit = f" ({attr.unit})" if attr.unit else ""
        lines.append(
            f"- **{attr.key}** [{attr.data_type.value}{unit}]: {attr.label} — {attr.description}"
            f"  operators: [{ops_str}]{examples}"
        )

    _SEMANTIC_DICT = "\n".join(lines)
    return _SEMANTIC_DICT


# ---------------------------------------------------------------------------
# Cross-brand intent detection
# ---------------------------------------------------------------------------

# Phrases that indicate the user wants to query across Spencers AND NBL.
# Any match → route to brand_code="corporate" with CorporatePgCompiler.
_CROSS_BRAND_SIGNALS: list[str] = [
    "nature's basket",
    "natures basket",
    "naturesbasket",
    "nature basket",
    " nbl",          # space-prefixed to avoid matching "sNBL" etc.
    "nbl ",          # suffix variant
    "(nbl)",
    "both brands",
    "both stores",
    "cross brand",
    "cross-brand",
    "cross brand",
    "spencers and nbl",
    "spencers and nature",
    "nbl and spencers",
    "spencers & nbl",
    "nbl & spencers",
    "shop at both",
    "shopped at both",
    "shopping at both",
]


def _is_cross_brand_query(nl_query: str) -> bool:
    """Return True if the query references both Spencers and NBL / Nature's Basket."""
    q = nl_query.lower()
    return any(sig in q for sig in _CROSS_BRAND_SIGNALS)


# Cache for the cross-brand semantic dictionary
_CROSS_BRAND_SEMANTIC_DICT: str | None = None


def _build_cross_brand_semantic_dictionary() -> str:
    """
    Build the attribute dictionary for the corporate / cross-brand view.
    Includes only attributes with applicable_brands=["corporate"] plus the
    RPSG corp.* identity attributes.
    """
    global _CROSS_BRAND_SEMANTIC_DICT
    if _CROSS_BRAND_SEMANTIC_DICT is not None:
        return _CROSS_BRAND_SEMANTIC_DICT

    lines = [
        "# RPSG Corporate Cross-Brand Attribute Dictionary",
        "",
        "This dictionary covers the CROSS-BRAND view where each customer row",
        "has BOTH Spencers (spn.*) AND Nature's Basket (nbl.*) attributes.",
        "",
        "NAMESPACES:",
        "  spn.*   -> Spencers behavioral data (NULL if customer never shopped at Spencers)",
        "  nbl.*   -> Nature's Basket behavioral data (NULL if customer never shopped at NBL)",
        "  corp.*  -> Cross-brand RPSG identity / lifetime metrics",
        "",
        "BRAND MEMBERSHIP ASSERTIONS (CRITICAL):",
        "  To assert a customer IS a Spencers customer:",
        '    {"attribute_key": "corp.is_spencers_customer", "operator": "exists", "value": null}',
        "  To assert a customer IS an NBL customer:",
        '    {"attribute_key": "corp.is_nbl_customer", "operator": "exists", "value": null}',
        "  Alternatively, any spn.* or nbl.* key with operator 'exists' achieves the same effect.",
        "",
        "Each attribute has: key, label, type, operators, description, examples",
        "",
    ]

    current_ns = None
    for attr in ATTRIBUTE_CATALOG:
        if not attr.applicable_brands or "corporate" not in attr.applicable_brands:
            continue
        ns = attr.key.split(".")[0]
        if ns != current_ns:
            ns_label = {
                "corp": "## CORPORATE / RPSG CROSS-BRAND",
                "spn":  "## SPENCERS (spn.*) — NULL when customer has no Spencers history",
                "nbl":  "## NATURE'S BASKET (nbl.*) — NULL when customer has no NBL history",
            }.get(ns, f"## {ns.upper()}")
            lines.append(f"\n{ns_label}")
            current_ns = ns

        ops_str = ", ".join(attr.operators[:6])
        if len(attr.operators) > 6:
            ops_str += f" (+{len(attr.operators) - 6} more)"
        examples = f" | examples: {attr.example_values[:3]}" if attr.example_values else ""
        unit = f" ({attr.unit})" if attr.unit else ""
        lines.append(
            f"- **{attr.key}** [{attr.data_type.value}{unit}]: {attr.label} "
            f"— {attr.description}  operators: [{ops_str}]{examples}"
        )

    _CROSS_BRAND_SEMANTIC_DICT = "\n".join(lines)
    return _CROSS_BRAND_SEMANTIC_DICT


SYSTEM_PROMPT = """You are a segment rule compiler for Spencer's Retail CDP (Customer Data Platform).

Your job: convert natural language audience queries into a structured JSON rule tree.

IMPORTANT RULES:
1. ONLY use attribute keys from the provided data dictionary. Never invent keys.
2. ONLY use operators listed for each attribute's data type.
3. Return ONLY valid JSON — no markdown, no explanation, no ```json``` fences.
4. The JSON must follow this exact schema:

{
  "root": {
    "type": "group",
    "logical_operator": "and" | "or",
    "conditions": [
      {
        "type": "attribute",
        "attribute_key": "<key from dictionary>",
        "operator": "<valid operator>",
        "value": <value>,
        "negate": false
      },
      ... more conditions or nested groups
    ]
  },
  "explanation": "Brief human-readable explanation of what this segment captures"
}

OPERATOR REFERENCE:
- String: equals, not_equals, contains, not_contains, starts_with, ends_with, is_empty, is_not_empty, in_list, not_in_list
- Numeric: equals, not_equals, greater_than, less_than, greater_than_or_equal, less_than_or_equal, between, not_between
- Date: equals, before, after, between, in_last_n_days, not_in_last_n_days
- Boolean: is_true, is_false

CITY RULE — ALWAYS FOLLOW:
  When the user mentions a city (e.g. "in Kolkata", "Mumbai customers", "Hyderabad shoppers"):
  ALWAYS use: {"attribute_key": "bt.city_desc", "operator": "equals", "value": "<City>"}
  NEVER use:  geo.city  (that is the customer's registered home city, not transaction location)
  bt.city_desc = the city where the bill transaction physically occurred — this is what users mean.

VALUE GUIDELINES:
- For "between" operator: set "value" to the lower bound, "second_value" to the upper bound
- For "in_list" operator: set "value" to an array of strings
- For numeric operators: use numbers (not strings)
- For boolean operators: set "value" to true
- For L1 segments: use HVHF, LVHF, HVLF, LVLF
- For L2 segments: use STAR, LOYAL, Win Back, New, ACTIVE, Inactive, LAPSER, Deep Lapsed
- For lifecycle_stage: use Active, At Risk, Lapsed, Churned, Registered
- For channel_presence: use Online, Offline, Omni

SPENCER'S PRODUCT HIERARCHY — CRITICAL RULES:
Spencer's uses a 5-level product taxonomy stored in bt.* bill-transaction attributes.
Hierarchy (broadest → most granular):
  bt.segment_desc → bt.family_desc → bt.class_desc → bt.brick_desc → bt.article_desc

ACTUAL TAXONOMY VALUES — ALWAYS USE THESE EXACT STRINGS (case-insensitive with "contains"):

  bt.segment_desc (6 values — top level division):
    FOOD | NON FOOD GROCERY | GM | FASHION CB | HI TECH | NON TRADE

  bt.family_desc (42 values — department/aisle level):
    FRESH FRUIT | FRESH VEGETABLE | FISH & MEAT | EGG | CHILLED & FROZEN | FROZEN |
    BAKERY & FOOD SERVIC | ALLIANCE FOOD SERVIC | BEVERAGES | PROCESSED FOOD |
    CEREALS & MINOR CERE | FLOUR & READY MIX | EDIBLE OIL & FATS | PULSES & MINOR PULSE |
    MASALA SPICES & HERB | SALT & FORTIFIED SAL | SWEETNER & SUGAR | DRY FRUIT & NUTS |
    ORGANIC STAPLES | ICE CREAM | HEALTH & BEAUTY | HOUSE HOLD NEED | HOME |
    ELECTRICALS | ELECTRONICS | LINEN | FURNITURE | PLAY | CONSUMABLES |
    CB BASIC WEAR | CB LADIES WEAR | CB MENS WEAR | CB KIDS WEAR | CB FOOT WEAR |
    CB ACCESSORIES | TOBACCO & ACCESSORIE | LIQUOR | BEAUTY AIDS | SERVICE |
    MARKETING | OTHER INCOME | WORK

SYNONYM MAP — when user says X, use this taxonomy value:
  "dairy"          → bt.family_desc contains "CHILLED & FROZEN"  (milk, curd, cheese, butter, paneer)
  "dairy products" → bt.family_desc contains "CHILLED & FROZEN"
  "milk"           → bt.brick_desc contains "MILK"  (or family CHILLED & FROZEN)
  "bread"          → bt.family_desc contains "BAKERY"
  "bakery"         → bt.family_desc contains "BAKERY"
  "snacks"         → bt.class_desc contains "SNACK"  OR  bt.brick_desc contains "NAMKEEN"
  "biscuits"       → bt.class_desc contains "BISCUIT"
  "cookies"        → bt.class_desc contains "BISCUIT"
  "chips"          → bt.brick_desc contains "CHIPS"
  "chocolate"      → bt.brick_desc contains "CHOCOLATE"
  "noodles"        → bt.brick_desc contains "NOODLE"  OR  bt.article_desc contains "MAGGI"
  "rice"           → bt.brick_desc contains "RICE"
  "atta / flour"   → bt.family_desc contains "FLOUR"
  "oil / cooking oil" → bt.family_desc contains "EDIBLE OIL"
  "spices / masala"   → bt.family_desc contains "MASALA"
  "pulses / dal"      → bt.family_desc contains "PULSES"
  "sugar"          → bt.family_desc contains "SWEETNER"
  "tea"            → bt.brick_desc contains "TEA"
  "coffee"         → bt.class_desc contains "COFFEE"
  "soft drinks / cola" → bt.family_desc contains "BEVERAGES"  +  bt.class_desc contains "CARBONATED"
  "juice"          → bt.brick_desc contains "JUICE"
  "water"          → bt.brick_desc contains "WATER"
  "alcohol / liquor / beer / wine / whisky" → bt.family_desc contains "LIQUOR"
  "personal care"  → bt.family_desc contains "HEALTH & BEAUTY"  OR  bt.family_desc contains "BEAUTY AIDS"
  "shampoo"        → bt.brick_desc contains "SHAMPOO"
  "soap"           → bt.brick_desc contains "SOAP"
  "detergent"      → bt.brick_desc contains "DETERGENT"
  "cleaning / household" → bt.family_desc contains "HOUSE HOLD"
  "fruits"         → bt.family_desc contains "FRESH FRUIT"
  "vegetables"     → bt.family_desc contains "FRESH VEGETABLE"
  "organic"        → bt.family_desc contains "ORGANIC"
  "frozen food"    → bt.family_desc contains "FROZEN"  OR  bt.family_desc contains "CHILLED & FROZEN"
  "ice cream"      → bt.family_desc contains "ICE CREAM"
  "electronics"    → bt.family_desc contains "ELECTRONICS"
  "fashion / clothing / apparel" → bt.segment_desc contains "FASHION"
  "dry fruits / nuts" → bt.family_desc contains "DRY FRUIT"
  "meat / chicken / mutton" → bt.family_desc contains "FISH & MEAT"
  "eggs"           → bt.family_desc contains "EGG"

STEP 1 — CLASSIFY the product term using these signals:

  SIGNAL A: Does the term contain a recognisable BRAND NAME?
    (e.g. Amul, Tata, Surf Excel, Fortune, Maggi, Britannia, Dabur, HUL, P&G, ITC)
    → YES → use bt.article_desc with "contains"
    → NO  → it is a category/type, proceed to SIGNAL B

  SIGNAL B: Does the term include a SIZE or VARIANT?
    (e.g. "500ml", "1kg", "family pack", "economy size")
    → YES → use bt.article_desc with "contains"
    → NO  → check the SYNONYM MAP above first, then proceed to SIGNAL C

  SIGNAL C: If not in the SYNONYM MAP, map the category term to the right level:
    - Top division ("food", "non-food", "general merchandise", "fashion")
        → bt.segment_desc with "contains"
    - Department / aisle → bt.family_desc with "contains"
    - Sub-category / class → bt.class_desc with "contains"
    - Specific product type → bt.brick_desc with "contains"

STEP 2 — WHEN UNCERTAIN about the hierarchy level (brick vs class vs article):
  Do NOT guess. Instead, emit an OR group that checks all plausible levels:
  {
    "type": "group",
    "logical_operator": "or",
    "conditions": [
      {"type": "attribute", "attribute_key": "bt.brick_desc",   "operator": "contains", "value": "<term>"},
      {"type": "attribute", "attribute_key": "bt.class_desc",   "operator": "contains", "value": "<term>"},
      {"type": "attribute", "attribute_key": "bt.article_desc", "operator": "contains", "value": "<term>"}
    ]
  }
  This ensures customers are found regardless of which level the term is stored at.

STEP 3 — OPERATOR rules for all product fields:
  - Always use "contains" (not "equals") — stored values often have extra words
    e.g. "BATH MUGS & ACCESSORIES" matches contains "BATH MUGS"
  - Use "in_list" only when the user explicitly lists multiple distinct values
  - Search values should be UPPERCASE to match stored data conventions

If the user's query is ambiguous, make reasonable assumptions and explain them in the "explanation" field.
If the query cannot be mapped to available attributes, return:
{"error": "Cannot map query to available attributes", "suggestion": "Try asking about: spend, visits, recency, location, segments, or channel"}
"""


# ---------------------------------------------------------------------------
# Cross-brand system prompt
# ---------------------------------------------------------------------------

CROSS_BRAND_SYSTEM_PROMPT = """You are a segment rule compiler for the RPSG Corporate CDP.

Your job: convert natural language cross-brand audience queries into a structured JSON rule tree
that operates on the CORPORATE cross-brand view — a single flat table with BOTH
Spencers (spn.*) and Nature's Basket (nbl.*) attributes per customer.

IMPORTANT RULES:
1. ONLY use attribute keys from the provided cross-brand data dictionary. Never invent keys.
2. ONLY use operators listed for each attribute.
3. Return ONLY valid JSON — no markdown, no explanation, no ```json``` fences.
4. The JSON must follow this exact schema:

{
  "root": {
    "type": "group",
    "logical_operator": "and" | "or",
    "conditions": [
      {
        "type": "attribute",
        "attribute_key": "<key from dictionary>",
        "operator": "<valid operator>",
        "value": <value or null>,
        "negate": false
      }
    ]
  },
  "explanation": "Brief human-readable explanation of what this segment captures"
}

CROSS-BRAND ATTRIBUTE NAMESPACES:
  spn.*   -> Spencers behavioral attributes (spn_lifecycle_stage, spn_spend_per_bill, etc.)
  nbl.*   -> Nature's Basket behavioral attributes (nbl_lifecycle_stage, nbl_spend_per_bill, etc.)
  corp.*  -> RPSG cross-brand identity and lifetime metrics

BRAND MEMBERSHIP ASSERTIONS — ALWAYS USE THESE (CRITICAL RULE):
  "Spencers customers"  → {"attribute_key": "corp.is_spencers_customer", "operator": "exists", "value": null}
  "NBL customers"       → {"attribute_key": "corp.is_nbl_customer",       "operator": "exists", "value": null}
  "has shopped at NBL"  → {"attribute_key": "nbl.total_bills",            "operator": "exists", "value": null}
  "has shopped at SPN"  → {"attribute_key": "spn.total_bills",            "operator": "exists", "value": null}

  !! NEVER USE corp.rpsg_brand_presence WITH "equals" FOR BRAND MEMBERSHIP !!
  corp.rpsg_brand_presence is a raw text column storing Python list repr like ['Spencer' 'NBL'].
  It stores LIFETIME brand presence across ALL RPSG brands — not transaction-window membership.
  Only use it with "contains" if you need to check for a specific brand string in the list,
  e.g. {"attribute_key": "corp.rpsg_brand_presence", "operator": "contains", "value": "NBL"}.
  For any "is a Spencers/NBL customer" assertion, ALWAYS use corp.is_spencers_customer / corp.is_nbl_customer.

OPERATOR REFERENCE:
  String : equals, not_equals, contains, not_contains, starts_with, ends_with, is_empty, is_not_empty, in_list
  Numeric: equals, not_equals, greater_than, less_than, greater_than_or_equal, less_than_or_equal, between, not_between
  Date   : equals, before, after, between, in_last_n_days, not_in_last_n_days
  Boolean: is_true, is_false
  Exists : exists, not_exists  (use value: null — checks column IS NOT NULL)

VALUE GUIDELINES:
  - "exists" / "not_exists": always set value to null
  - "between": set "value" to lower bound, "second_value" to upper bound
  - "in_list": set "value" to an array
  - Numeric operators: use numbers, not strings
  - spn.lifecycle_stage actual values in data: "At Risk", "Lapsed", "Registered"
  - nbl.lifecycle_stage actual values in data: "Lapsed", "Registered"
  - spn.l2_segment actual values in data:      "Inactive", "LAPSER"
  - nbl.l2_segment actual values in data:      "Inactive", "LAPSER"

  CITY RULE — ALWAYS FOLLOW:
  When the user mentions a city (e.g. "in Kolkata", "Mumbai customers"):
  ALWAYS use: {"attribute_key": "spn.city", "operator": "equals", "value": "<City>"} for Spencers city
          OR: {"attribute_key": "nbl.city", "operator": "equals", "value": "<City>"} for NBL city
  These map to the customer's registered city in the respective brand profile.
  NEVER use corp.rpsg_brand_presence or any generic geo.city key in cross-brand queries.

  CRITICAL SEGMENT MAPPING — read before choosing a column:
    "inactive"   → spn.l2_segment = "Inactive"  OR  nbl.l2_segment = "Inactive"
    "lapser"     → spn.l2_segment = "LAPSER"     OR  nbl.l2_segment = "LAPSER"
    "lapsed"     → spn.lifecycle_stage = "Lapsed" OR nbl.lifecycle_stage = "Lapsed"
    "at risk"    → spn.lifecycle_stage = "At Risk"  (no NBL equivalent in current data)
    "registered" → spn.lifecycle_stage = "Registered" OR nbl.lifecycle_stage = "Registered"
    !! "Inactive" is ONLY in l2_segment — NEVER in lifecycle_stage !!
    !! "At Risk" is ONLY in spn.lifecycle_stage — NOT in nbl.lifecycle_stage in current data !!

CROSS-BRAND QUERY PATTERNS:

Pattern 1 — "Spencers customers who have ALSO shopped at NBL":
  AND [
    corp.is_spencers_customer EXISTS,
    corp.is_nbl_customer EXISTS
  ]

Pattern 2 — "Spencers customers who shopped at NBL with NBL spend > X":
  AND [
    corp.is_spencers_customer EXISTS,
    nbl.spend_per_bill > X
  ]
  (nbl.spend_per_bill IS NOT NULL already implies NBL membership)

Pattern 3 — "NBL customers with high Spencers spend":
  AND [
    corp.is_nbl_customer EXISTS,
    spn.spend_per_bill > X
  ]

Pattern 4 — "Cross-brand customers with high combined spend":
  AND [
    corp.is_spencers_customer EXISTS,
    corp.is_nbl_customer EXISTS,
    spn.total_spend > X   (or nbl.total_spend > Y)
  ]

Pattern 5 — "Spencers Lapsed customers who are Active at NBL":
  AND [
    spn.lifecycle_stage EQUALS "Lapsed",
    nbl.lifecycle_stage EQUALS "Active"
  ]

WORKED EXAMPLE:
Query: "Spencers customers who have shopped at Nature's Basket with spend per bill more than 3000 in last 12 months"

Correct JSON:
{
  "root": {
    "type": "group",
    "logical_operator": "and",
    "conditions": [
      {"type": "attribute", "attribute_key": "corp.is_spencers_customer", "operator": "exists", "value": null},
      {"type": "attribute", "attribute_key": "nbl.spend_per_bill", "operator": "greater_than", "value": 3000},
      {"type": "attribute", "attribute_key": "nbl.last_bill_date", "operator": "in_last_n_days", "value": 365}
    ]
  },
  "explanation": "Cross-brand customers who have Spencers membership AND shopped at NBL with average spend per bill above Rs 3,000 within the past 12 months."
}

NOTE: nbl.spend_per_bill IS NOT NULL already implies the customer has shopped at NBL,
so a separate nbl membership assertion is not needed when an nbl.* numeric condition is used.

If the query cannot be mapped, return:
{"error": "Cannot map query to available attributes", "suggestion": "Try asking about: cross-brand spend, lifecycle stage differences, shared customers"}
"""


class NLSegmentationService:
    """Translates natural language queries into segment definitions via AWS Bedrock."""

    def __init__(self):
        import boto3

        region = os.getenv("BEDROCK_REGION", os.getenv("CDP_AWS_REGION", "ap-south-1"))
        self._bedrock = boto3.client("bedrock-runtime", region_name=region)
        self.model = os.getenv(
            "BEDROCK_MODEL_ID", "apac.anthropic.claude-3-haiku-20240307-v1:0"
        )
        logger.info("NL segmentation using AWS Bedrock — model: %s", self.model)

    # ------------------------------------------------------------------
    # Internal LLM abstraction
    # ------------------------------------------------------------------

    def _call_llm(
        self, system: str, user_message: str, max_tokens: int = 2000
    ) -> tuple[str, dict]:
        """Call Bedrock and return (text, trace).

        trace contains model, region, token counts, latency, and estimated cost.
        """
        t0 = time.perf_counter()
        response = self._bedrock.converse(
            modelId=self.model,
            system=[{"text": system}] if system else [],
            messages=[{"role": "user", "content": [{"text": user_message}]}],
            inferenceConfig={"maxTokens": max_tokens},
        )
        latency_ms = round((time.perf_counter() - t0) * 1000)
        usage = response.get("usage", {})
        input_tokens = usage.get("inputTokens")
        output_tokens = usage.get("outputTokens")
        text = response["output"]["message"]["content"][0]["text"].strip()
        trace = {
            "provider": "bedrock",
            "model": self.model,
            "region": os.getenv("BEDROCK_REGION", os.getenv("CDP_AWS_REGION", "ap-south-1")),
            "input_tokens": input_tokens,
            "output_tokens": output_tokens,
            "latency_ms": latency_ms,
            "cost_usd": _compute_cost(self.model, input_tokens, output_tokens),
        }
        logger.info("llm_call %s", json.dumps(trace))
        return text, trace

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def query(
        self,
        nl_query: str,
        brand_code: str = "spencers",
        execute: bool = True,
    ) -> dict[str, Any]:
        """
        Convert a natural language query to a segment, compile SQL, and optionally execute.

        Cross-brand detection: if the query references Nature's Basket / NBL alongside
        Spencers, brand_code is automatically overridden to "corporate" and the
        CorporatePgCompiler is used against silver_corp_customer_attributes.

        Returns:
            {
                "nl_query": str,
                "brand_code": str,         # effective brand (may be "corporate" after auto-detect)
                "is_cross_brand": bool,
                "rules": dict,             # structured rule tree
                "explanation": str,        # LLM's explanation
                "sql": str,                # compiled PostgreSQL
                "estimated_count": int,    # audience size (if execute=True)
            }

        NOTE: No PII or row-level profile data is ever returned.
        """
        # ── Cross-brand intent detection ──────────────────────────────────────
        is_cross_brand = _is_cross_brand_query(nl_query)
        if is_cross_brand:
            brand_code = "corporate"
            logger.info("Cross-brand intent detected — routing to corporate compiler")

        # ── Choose system prompt and semantic dictionary ───────────────────────
        if is_cross_brand:
            system_prompt = CROSS_BRAND_SYSTEM_PROMPT
            semantic_dict = _build_cross_brand_semantic_dictionary()
        else:
            system_prompt = SYSTEM_PROMPT
            semantic_dict = _build_semantic_dictionary()

        user_message = (
            f"DATA DICTIONARY:\n{semantic_dict}\n\n"
            f"USER QUERY: {nl_query}\n\n"
            "Convert this to a segment rule JSON."
        )

        try:
            raw_text, llm_trace = self._call_llm(system_prompt, user_message, max_tokens=2000)
        except Exception as e:
            logger.error("Bedrock API error: %s", e)
            return {
                "nl_query": nl_query,
                "brand_code": brand_code,
                "is_cross_brand": is_cross_brand,
                "error": f"LLM API error: {str(e)}",
                "rules": None,
                "sql": None,
                "estimated_count": None,
            }

        # Strip markdown code fences if the model wraps the response
        json_str = raw_text
        if json_str.startswith("```"):
            lines = json_str.split("\n")
            lines = [l for l in lines if not l.strip().startswith("```")]
            json_str = "\n".join(lines)

        try:
            parsed = json.loads(json_str)
        except json.JSONDecodeError as e:
            logger.error("Failed to parse LLM response as JSON: %s\nRaw: %s", e, raw_text[:500])
            return {
                "nl_query": nl_query,
                "brand_code": brand_code,
                "is_cross_brand": is_cross_brand,
                "error": f"Failed to parse AI response: {str(e)}",
                "raw_response": raw_text[:1000],
                "rules": None,
                "sql": None,
                "estimated_count": None,
                "_llm_trace": llm_trace,
            }

        if "error" in parsed and "root" not in parsed:
            return {
                "nl_query": nl_query,
                "brand_code": brand_code,
                "is_cross_brand": is_cross_brand,
                "error": parsed["error"],
                "suggestion": parsed.get("suggestion", ""),
                "rules": None,
                "sql": None,
                "estimated_count": None,
                "_llm_trace": llm_trace,
            }

        explanation = parsed.pop("explanation", "")
        rules = parsed

        try:
            definition = self._parse_definition(rules)
            compiler = _make_nl_compiler(brand_code)
            sql = compiler.compile_count(definition)
        except Exception as e:
            logger.error("Failed to compile NL rules to SQL: %s", e)
            return {
                "nl_query": nl_query,
                "brand_code": brand_code,
                "is_cross_brand": is_cross_brand,
                "rules": rules,
                "explanation": explanation,
                "error": f"Rule compilation failed: {str(e)}",
                "sql": None,
                "estimated_count": None,
                "_llm_trace": llm_trace,
            }

        result = {
            "nl_query": nl_query,
            "brand_code": brand_code,
            "is_cross_brand": is_cross_brand,
            "rules": rules,
            "explanation": explanation,
            "sql": sql,
            "estimated_count": None,
            "_llm_trace": llm_trace,
        }

        # Execute COUNT only — never fetch row-level / PII data
        if execute:
            try:
                import psycopg

                db_url = os.getenv(
                    "DATABASE_URL",
                    f"postgresql://{os.getenv('PG_USER', 'cdp')}:{os.getenv('PG_PASSWORD', 'cdp')}"
                    f"@{os.getenv('PG_HOST', 'localhost')}:{os.getenv('PG_PORT', '5432')}"
                    f"/{os.getenv('PG_DB', 'cdp_meta')}",
                )
                with psycopg.connect(db_url) as conn:
                    row = conn.execute(sql).fetchone()
                    result["estimated_count"] = row[0] if row else 0
            except Exception as e:
                logger.error("SQL execution failed: %s", e)
                result["error"] = f"SQL execution failed: {str(e)}"

        return result

    async def suggest(self, partial_query: str) -> list[str]:
        """
        Given a partial query, suggest completions using the attribute catalog.
        Lightweight — doesn't call the LLM, just pattern-matches against the catalog.
        """
        partial_lower = partial_query.lower()
        suggestions = []

        for attr in ATTRIBUTE_CATALOG:
            if (
                partial_lower in attr.label.lower()
                or partial_lower in attr.description.lower()
                or partial_lower in attr.key.lower()
            ):
                suggestions.append(attr.label)

        common_patterns = [
            "High spenders in {city}",
            "Customers who haven't bought in {N} days",
            "STAR segment customers",
            "Weekend shoppers with spend > {amount}",
            "Omni-channel customers in {region}",
            "New customers (first-time buyers)",
            "Churned customers with high lifetime value",
            "Promo lovers (promo bills > {N})",
            "Top decile customers by spend",
            "Customers who accept SMS marketing",
            # Cross-brand patterns
            "Spencers customers who also shop at Nature's Basket",
            "Spencers customers who shopped at NBL with spend per bill > 3000",
            "Cross-brand customers active at both Spencers and NBL",
            "Spencers lapsed customers who are still active at NBL",
            "High-value NBL customers who haven't tried Spencers yet",
        ]
        for pattern in common_patterns:
            if partial_lower in pattern.lower():
                suggestions.append(pattern)

        return suggestions[:10]

    async def explain(self, rules: dict) -> str:
        """
        Given a structured rule tree, generate a human-readable explanation.
        """
        user_message = (
            "Given this CDP segment rule tree, write a concise 1-2 sentence "
            "plain English description of what audience it captures.\n\n"
            f"Rules: {json.dumps(rules, indent=2)}\n\n"
            "Reply with ONLY the description, nothing else."
        )
        try:
            text, _ = self._call_llm("", user_message, max_tokens=500)
            return text
        except Exception as e:
            logger.error("Explain failed: %s", e)
            return "Unable to generate explanation."

    # ------------------------------------------------------------------
    # Rule parsing helpers
    # ------------------------------------------------------------------

    def _parse_definition(self, rules: dict) -> SegmentDefinition:
        """Parse the LLM-generated rule tree into a SegmentDefinition."""
        return SegmentDefinition(
            root=self._parse_group(rules.get("root", rules)),
            limit=rules.get("limit"),
            order_by=rules.get("order_by"),
            order_direction=rules.get("order_direction", "desc"),
        )

    def _parse_group(self, node: dict) -> ConditionGroup:
        """Recursively parse a condition group."""
        conditions = []
        for cond in node.get("conditions", []):
            cond_type = cond.get("type", "attribute")
            if cond_type == "group" or "conditions" in cond:
                conditions.append(self._parse_group(cond))
            else:
                conditions.append(
                    AttributeCondition(
                        attribute_key=cond["attribute_key"],
                        operator=cond["operator"],
                        value=cond.get("value"),
                        second_value=cond.get("second_value"),
                        negate=cond.get("negate", False),
                    )
                )
        return ConditionGroup(
            logical_operator=LogicalOperator(node.get("logical_operator", "and")),
            conditions=conditions,
        )

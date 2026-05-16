"""
Natural Language Segmentation Service
======================================

Converts plain-English audience queries into structured segment rules
using Claude as the reasoning engine, grounded in the semantic data dictionary.

Flow:
  1. User types: "High spenders who haven't bought in 60 days in Kolkata"
  2. This service sends the query + full attribute catalog to Claude
  3. Claude returns a structured JSON rule tree
  4. We validate, compile to SQL via PgCompiler, execute COUNT only, and return results
     NO PII or row-level data is ever returned or processed by the LLM.
"""

from __future__ import annotations

import json
import logging
import os
from typing import Any

import anthropic

from app.schemas.profile_attributes import ATTRIBUTE_CATALOG
from app.schemas.segment_rules import (
    AttributeCondition,
    ConditionGroup,
    LogicalOperator,
    SegmentDefinition,
)
from app.services.query_engine.pg_compiler import PgCompiler, SPENCERS_SCHEMA_MAP

logger = logging.getLogger(__name__)

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

VALUE GUIDELINES:
- For "between" operator: set "value" to the lower bound, "second_value" to the upper bound
- For "in_list" operator: set "value" to an array of strings
- For numeric operators: use numbers (not strings)
- For boolean operators: set "value" to true
- For L1 segments: use HVHF, LVHF, HVLF, LVLF
- For L2 segments: use STAR, LOYAL, Win Back, New, ACTIVE, Inactive, LAPSER, Deep Lapsed
- For lifecycle_stage: use Active, At Risk, Lapsed, Churned, Registered
- For channel_presence: use Online, Offline, Omni

If the user's query is ambiguous, make reasonable assumptions and explain them in the "explanation" field.
If the query cannot be mapped to available attributes, return:
{"error": "Cannot map query to available attributes", "suggestion": "Try asking about: spend, visits, recency, location, segments, or channel"}
"""


class NLSegmentationService:
    """Translates natural language queries into segment definitions using Claude."""

    def __init__(self):
        api_key = os.getenv("ANTHROPIC_API_KEY", "")
        if not api_key:
            logger.warning("ANTHROPIC_API_KEY not set — NL segmentation will fail")
        self.client = anthropic.Anthropic(api_key=api_key)
        self.model = os.getenv("ANTHROPIC_MODEL", "claude-sonnet-4-20250514")

    async def query(
        self,
        nl_query: str,
        brand_code: str = "spencers",
        execute: bool = True,
    ) -> dict[str, Any]:
        """
        Convert a natural language query to a segment, compile SQL, and optionally execute.

        Returns:
            {
                "nl_query": str,
                "rules": dict,           # structured rule tree
                "explanation": str,       # Claude's explanation
                "sql": str,              # compiled PostgreSQL
                "estimated_count": int,  # audience size (if execute=True)
            }

        NOTE: No PII or row-level profile data is ever returned.
        """
        semantic_dict = _build_semantic_dictionary()

        # Call Claude to convert NL → structured rules
        try:
            response = self.client.messages.create(
                model=self.model,
                max_tokens=2000,
                system=SYSTEM_PROMPT,
                messages=[
                    {
                        "role": "user",
                        "content": (
                            f"DATA DICTIONARY:\n{semantic_dict}\n\n"
                            f"USER QUERY: {nl_query}\n\n"
                            "Convert this to a segment rule JSON."
                        ),
                    }
                ],
            )
        except anthropic.APIError as e:
            logger.error(f"Claude API error: {e}")
            return {
                "nl_query": nl_query,
                "error": f"Claude API error: {str(e)}",
                "rules": None,
                "sql": None,
                "estimated_count": None,
            }

        raw_text = response.content[0].text.strip()

        # Strip markdown code fences if Claude wraps response
        json_str = raw_text
        if json_str.startswith("```"):
            lines = json_str.split("\n")
            lines = [l for l in lines if not l.strip().startswith("```")]
            json_str = "\n".join(lines)

        try:
            parsed = json.loads(json_str)
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse Claude response as JSON: {e}\nRaw: {raw_text[:500]}")
            return {
                "nl_query": nl_query,
                "error": f"Failed to parse AI response: {str(e)}",
                "raw_response": raw_text[:1000],
                "rules": None,
                "sql": None,
                "estimated_count": None,
            }

        # Check for error response from Claude
        if "error" in parsed and "root" not in parsed:
            return {
                "nl_query": nl_query,
                "error": parsed["error"],
                "suggestion": parsed.get("suggestion", ""),
                "rules": None,
                "sql": None,
                "estimated_count": None,
            }

        explanation = parsed.pop("explanation", "")
        rules = parsed

        # Validate and compile to SQL
        try:
            definition = self._parse_definition(rules)
            sql = PgCompiler(brand_code).compile_count(definition)
        except Exception as e:
            logger.error(f"Failed to compile NL rules to SQL: {e}")
            return {
                "nl_query": nl_query,
                "rules": rules,
                "explanation": explanation,
                "error": f"Rule compilation failed: {str(e)}",
                "sql": None,
                "estimated_count": None,
            }

        result = {
            "nl_query": nl_query,
            "rules": rules,
            "explanation": explanation,
            "sql": sql,
            "estimated_count": None,
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
                logger.error(f"SQL execution failed: {e}")
                result["error"] = f"SQL execution failed: {str(e)}"

        return result

    async def suggest(self, partial_query: str) -> list[str]:
        """
        Given a partial query, suggest completions using the attribute catalog.
        Lightweight — doesn't call Claude, just pattern-matches against catalog.
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
        ]
        for pattern in common_patterns:
            if partial_lower in pattern.lower():
                suggestions.append(pattern)

        return suggestions[:10]

    async def explain(self, rules: dict) -> str:
        """
        Given a structured rule tree, generate a human-readable explanation.
        Uses Claude to produce natural language from the rules.
        """
        try:
            response = self.client.messages.create(
                model=self.model,
                max_tokens=500,
                messages=[
                    {
                        "role": "user",
                        "content": (
                            "Given this CDP segment rule tree, write a concise 1-2 sentence "
                            "plain English description of what audience it captures.\n\n"
                            f"Rules: {json.dumps(rules, indent=2)}\n\n"
                            "Reply with ONLY the description, nothing else."
                        ),
                    }
                ],
            )
            return response.content[0].text.strip()
        except Exception as e:
            logger.error(f"Explain failed: {e}")
            return "Unable to generate explanation."

    def _parse_definition(self, rules: dict) -> SegmentDefinition:
        """Parse the Claude-generated rule tree into a SegmentDefinition."""
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

"""
Rank & Split SQL builder
========================

Builds reproducible SQL for ranking a segment's members (single or weighted
multi-attribute) and slicing them into percentage groups via NTILE.

A "rank_list" item: {"attribute": "txn.total_spend", "weight": 1.0, "order": "desc"}
  - order "desc" → higher value = higher priority (top of the ranking)
  - order "asc"  → lower value  = higher priority

A group is a percentile slice (pct_lower, pct_upper] over a 1..100 NTILE bucket,
where bucket 1 is the highest-priority cohort.

An optional `effective_limit` restricts the ranked base to the top-N members
before slicing (used by count / budget constraints).
"""

from __future__ import annotations

from app.services.query_engine.pg_compiler import SPENCERS_SCHEMA_MAP


def _metric_col(attribute: str) -> str:
    """Resolve a rankable attribute key to its `m.`-aliased ba column."""
    col = SPENCERS_SCHEMA_MAP.get(attribute, f"ba.{attribute.split('.')[-1]}")
    # All rankable attributes live on the ba table (alias remapped to m here).
    bare = col.split(".")[-1]
    return f"m.{bare}"


def _rank_score_expr(rank_list: list[dict]) -> str:
    """Weighted sum of per-attribute PERCENT_RANK windows (0..1, higher = better)."""
    if not rank_list:
        return "PERCENT_RANK() OVER (ORDER BY customer_id)"
    total_w = sum(abs(r.get("weight", 1.0)) for r in rank_list) or 1.0
    terms = []
    for r in rank_list:
        col = _metric_col(r["attribute"])
        w = abs(r.get("weight", 1.0)) / total_w
        # desc priority → largest value should map to percent_rank 1 → ORDER BY ASC
        direction = "ASC" if r.get("order", "desc") == "desc" else "DESC"
        terms.append(f"{w:.6f} * PERCENT_RANK() OVER (ORDER BY {col} {direction})")
    return " + ".join(terms)


def build_rank_split_sql(
    membership_sql: str,
    ba_table: str,
    rank_list: list[dict],
    pct_lower: float,
    pct_upper: float,
    effective_limit: int | None = None,
    select: str = "metrics",
) -> str:
    """
    Compose the full SQL for one percentage group.

    select="metrics" → COUNT, SUM(total_spend), AVG(total_spend)
    select="count"   → COUNT only (audience_count)
    select="members" → customer_id list (for export / saving)
    """
    score_expr = _rank_score_expr(rank_list)

    # Distinct ba columns we must carry through for the score + metrics.
    needed = {"total_spend"}
    for r in rank_list:
        needed.add(_metric_col(r["attribute"]).split(".")[-1])
    join_cols = ", ".join(f"m.{c} AS {c}" for c in sorted(needed))

    cte = [
        f"base AS (\n{membership_sql}\n)",
        (
            "joined AS (\n"
            f"  SELECT b.customer_id, {join_cols}\n"
            f"  FROM base b\n"
            f"  LEFT JOIN {ba_table} m ON m.customer_id = b.customer_id\n"
            ")"
        ),
        (
            "scored AS (\n"
            # joined projects bare column names, so strip the m. alias from the score expr
            f"  SELECT customer_id, total_spend, ({score_expr.replace('m.', '')}) AS rank_score\n"
            "  FROM joined\n"
            ")"
        ),
    ]

    source = "scored"
    if effective_limit:
        cte.append(
            "limited AS (\n"
            "  SELECT customer_id, total_spend, rank_score,\n"
            "         ROW_NUMBER() OVER (ORDER BY rank_score DESC NULLS LAST) AS rn\n"
            "  FROM scored\n"
            ")"
        )
        cte.append(
            "qualified AS (\n"
            f"  SELECT customer_id, total_spend, rank_score FROM limited WHERE rn <= {int(effective_limit)}\n"
            ")"
        )
        source = "qualified"

    cte.append(
        "bucketed AS (\n"
        "  SELECT customer_id, total_spend,\n"
        "         NTILE(100) OVER (ORDER BY rank_score DESC NULLS LAST) AS bucket\n"
        f"  FROM {source}\n"
        ")"
    )

    where = f"bucket > {pct_lower:g} AND bucket <= {pct_upper:g}"
    if select == "count":
        final = f"SELECT COUNT(*) AS audience_count FROM bucketed WHERE {where}"
    elif select == "members":
        final = f"SELECT customer_id FROM bucketed WHERE {where}"
    else:
        final = (
            "SELECT COUNT(*) AS cnt, "
            "COALESCE(SUM(total_spend), 0) AS revenue, "
            "COALESCE(AVG(total_spend), 0) AS avg_spend "
            f"FROM bucketed WHERE {where}"
        )

    return "WITH " + ",\n".join(cte) + "\n" + final


def cumulative_bounds(splits: list[dict]) -> list[tuple[float, float]]:
    """Convert a list of percentage groups into cumulative (lower, upper] bucket bounds."""
    bounds, cum = [], 0.0
    n = len(splits)
    for i, s in enumerate(splits):
        pct = s.get("percent")
        if pct is None:
            pct = 100.0 / n if n else 100.0
        lower = cum
        upper = 100.0 if i == n - 1 else min(100.0, cum + pct)  # last group absorbs rounding
        bounds.append((lower, upper))
        cum = upper
    return bounds

"""
Customer Single View service
============================

Composes a single customer's 360° profile across identity, behavioral,
propensity, and transaction sources. Brand-aware via BRAND_SCHEMA_CONFIG.

Lookup is by mobile number (last-10-digit match) or by unified_id.
"""

from __future__ import annotations

import logging

from app.schemas.customer_view import (
    BehavioralBlock,
    BrandPanel,
    CategorySpend,
    CorporateIdentityBlock,
    CorporateInsights,
    CorporateMetrics,
    CorporateSingleViewResponse,
    CustomerSingleViewResponse,
    IdentityBlock,
    PropensityBlock,
    ReachabilityBlock,
    SegmentScore,
    SpendTrendPoint,
    TimelineEvent,
    TopArticle,
)
from app.services.query_engine.pg_compiler import BRAND_SCHEMA_CONFIG, CORPORATE_TABLE
from app.services.query_engine.pg_compiler import BRAND_SCHEMA_CONFIG
from app.services import segmentation as _seg_pkg  # noqa: F401  (ensures module import)
import app.services.segmentation.service as _seg_mod

logger = logging.getLogger(__name__)


# Per-brand propensity segment labels, ordered to match segment_N columns.
# Mirrors backend/ml/propensity/config.py.
_BRAND_SEGMENTS: dict[str, list[str]] = {
    "spencers": ["FASHION CB", "FOOD", "GM", "HI TECH", "NON TRADE", "NON FOOD GROCERY"],
    "natures_basket": ["FOOD", "GM", "NON TRADE", "NON FOOD GROCERY"],
}

# Column prefix inside the propensity table for each brand.
_BRAND_PROP_PREFIX: dict[str, str] = {
    "spencers": "spencers",
    "natures_basket": "nbl",
}


def _f(v) -> float | None:
    if v is None:
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _s(v) -> str | None:
    return None if v is None else str(v)


class CustomerViewService:
    async def _query(self, sql: str, params: tuple = ()) -> list[dict]:
        """Parameterized query via the segmentation module's shared async pool."""
        from psycopg.rows import dict_row

        pool = _seg_mod._pg_pool
        if pool is None:
            import psycopg

            conninfo = _seg_mod._get_pg_conninfo()
            async with await psycopg.AsyncConnection.connect(
                conninfo, row_factory=dict_row
            ) as conn:
                async with conn.cursor() as cur:
                    await cur.execute(sql, params)
                    return [dict(r) for r in await cur.fetchall()]

        async with pool.connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute(sql, params)
                return [dict(r) for r in await cur.fetchall()]

    async def get_single_view(
        self, customer_id: str | None, mobile: str | None, brand_code: str
    ) -> CustomerSingleViewResponse:
        if brand_code not in BRAND_SCHEMA_CONFIG:
            raise ValueError(f"Unknown brand_code: {brand_code}")

        cfg = BRAND_SCHEMA_CONFIG[brand_code]
        tbl_p = cfg["unified_profiles"]
        tbl_ba = cfg["customer_behavioral_attributes"]
        tbl_prop = cfg["customer_propensity_scores"]
        tbl_bt = cfg["s_fact_bill_transactions"]

        # ── Resolve the customer (identity + reachability) ──────────────────────
        if customer_id:
            where = "p.unified_id = %s"
            params: tuple = (customer_id,)
        elif mobile:
            where = "RIGHT(p.canonical_mobile, 10) = RIGHT(%s, 10)"
            params = (mobile,)
        else:
            return CustomerSingleViewResponse(found=False, brand_code=brand_code)

        prof_sql = f"""
            SELECT p.unified_id, p.surrogate_id, p.display_name, p.first_name, p.last_name,
                   p.dob, p.age, p.email, p.canonical_mobile, p.city, p.pincode, p.street,
                   p.region, p.registered_store, p.customer_group, p.occupation,
                   p.primary_source, p.status,
                   p.dnd, p.accepts_email_marketing, p.accepts_sms_marketing,
                   p.whatsapp, p.gw_customer_flag
              FROM {tbl_p} p
             WHERE {where}
             LIMIT 1
        """
        prof_rows = await self._query(prof_sql, params)
        if not prof_rows:
            return CustomerSingleViewResponse(found=False, brand_code=brand_code)

        p = prof_rows[0]
        uid = p["unified_id"]
        mobile_val = p.get("canonical_mobile")

        identity = IdentityBlock(
            unified_id=uid,
            surrogate_id=_s(p.get("surrogate_id")),
            name=_s(p.get("display_name")),
            first_name=_s(p.get("first_name")),
            last_name=_s(p.get("last_name")),
            dob=_s(p.get("dob")),
            age=p.get("age"),
            email=_s(p.get("email")),
            mobile=_s(mobile_val),
            city=_s(p.get("city")),
            pincode=_s(p.get("pincode")),
            street=_s(p.get("street")),
            region=_s(p.get("region")),
            registered_store=_s(p.get("registered_store")),
            customer_group=_s(p.get("customer_group")),
            occupation=_s(p.get("occupation")),
            primary_source=_s(p.get("primary_source")),
            status=_s(p.get("status")),
        )
        reachability = ReachabilityBlock(
            dnd=_s(p.get("dnd")),
            accepts_email_marketing=_s(p.get("accepts_email_marketing")),
            accepts_sms_marketing=_s(p.get("accepts_sms_marketing")),
            whatsapp=_s(p.get("whatsapp")),
            gw_customer_flag=_s(p.get("gw_customer_flag")),
        )

        # ── Behavioral ──────────────────────────────────────────────────────────
        ba_sql = f"""
            SELECT first_bill_date, last_bill_date, recency_days, tenure_days,
                   total_bills, total_visits, total_spend, spend_per_bill,
                   spend_per_visit, total_discount, spend_decile, nob_decile,
                   l1_segment, l2_segment, lifecycle_stage,
                   fav_store_name, fav_store_type, fav_day,
                   fav_article_by_spend_desc, fav_article_by_nob_desc,
                   channel_presence, store_spend, online_spend, store_bills, online_bills,
                   promo_bill_count, return_bill_count, weekend_bill_count,
                   wednesday_bill_count, distinct_months, distinct_store_count,
                   distinct_article_count, avg_items_per_bill,
                   rfm_recency_score, rfm_frequency_score, rfm_monetary_score
              FROM {tbl_ba}
             WHERE customer_id = %s
             LIMIT 1
        """
        ba_rows = await self._query(ba_sql, (uid,))
        behavioral = None
        if ba_rows:
            b = ba_rows[0]
            behavioral = BehavioralBlock(
                first_bill_date=_s(b.get("first_bill_date")),
                last_bill_date=_s(b.get("last_bill_date")),
                recency_days=_f(b.get("recency_days")),
                tenure_days=_f(b.get("tenure_days")),
                total_bills=_f(b.get("total_bills")),
                total_visits=_f(b.get("total_visits")),
                total_spend=_f(b.get("total_spend")),
                spend_per_bill=_f(b.get("spend_per_bill")),
                spend_per_visit=_f(b.get("spend_per_visit")),
                total_discount=_f(b.get("total_discount")),
                spend_decile=b.get("spend_decile"),
                nob_decile=b.get("nob_decile"),
                l1_segment=_s(b.get("l1_segment")),
                l2_segment=_s(b.get("l2_segment")),
                lifecycle_stage=_s(b.get("lifecycle_stage")),
                fav_store_name=_s(b.get("fav_store_name")),
                fav_store_type=_s(b.get("fav_store_type")),
                fav_day=_s(b.get("fav_day")),
                fav_article_by_spend_desc=_s(b.get("fav_article_by_spend_desc")),
                fav_article_by_nob_desc=_s(b.get("fav_article_by_nob_desc")),
                channel_presence=_s(b.get("channel_presence")),
                store_spend=_f(b.get("store_spend")),
                online_spend=_f(b.get("online_spend")),
                store_bills=_f(b.get("store_bills")),
                online_bills=_f(b.get("online_bills")),
                promo_bill_count=_f(b.get("promo_bill_count")),
                return_bill_count=_f(b.get("return_bill_count")),
                weekend_bill_count=_f(b.get("weekend_bill_count")),
                wednesday_bill_count=_f(b.get("wednesday_bill_count")),
                distinct_months=_f(b.get("distinct_months")),
                distinct_store_count=_f(b.get("distinct_store_count")),
                distinct_article_count=_f(b.get("distinct_article_count")),
                avg_items_per_bill=_f(b.get("avg_items_per_bill")),
                rfm_recency_score=b.get("rfm_recency_score"),
                rfm_frequency_score=b.get("rfm_frequency_score"),
                rfm_monetary_score=b.get("rfm_monetary_score"),
            )

        # ── Propensity ──────────────────────────────────────────────────────────
        propensity = await self._fetch_propensity(uid, brand_code, tbl_prop)

        # ── Trend / articles / category spend (from bt, mobile-keyed) ────────────
        spend_trend: list[SpendTrendPoint] = []
        top_articles: list[TopArticle] = []
        category_spend: list[CategorySpend] = []
        if mobile_val:
            spend_trend = await self._fetch_spend_trend(mobile_val, tbl_bt)
            top_articles = await self._fetch_top_articles(mobile_val, tbl_bt)
            category_spend = await self._fetch_category_spend(mobile_val, tbl_bt)

        return CustomerSingleViewResponse(
            found=True,
            brand_code=brand_code,
            identity=identity,
            behavioral=behavioral,
            reachability=reachability,
            propensity=propensity,
            spend_trend=spend_trend,
            top_articles=top_articles,
            location_propensity=category_spend,
        )

    async def _fetch_propensity(
        self, uid: str, brand_code: str, tbl_prop: str
    ) -> PropensityBlock | None:
        labels = _BRAND_SEGMENTS.get(brand_code, [])
        prefix = _BRAND_PROP_PREFIX.get(brand_code, "")
        if not labels or not prefix:
            return None

        cols: list[str] = []
        for i in range(1, len(labels) + 1):
            cols.append(f"{prefix}_segment_{i}_propensity")
            cols.append(f"{prefix}_segment_{i}_normalized_propensity")

        sql = f"SELECT {', '.join(cols)} FROM {tbl_prop} WHERE customer_id = %s LIMIT 1"
        try:
            rows = await self._query(sql, (uid,))
        except Exception as e:
            logger.warning(f"propensity fetch failed: {e}")
            return None
        if not rows:
            return None

        r = rows[0]
        segments: list[SegmentScore] = []
        dominant_label: str | None = None
        dominant_score: float | None = None
        for i, label in enumerate(labels, start=1):
            raw = _f(r.get(f"{prefix}_segment_{i}_propensity"))
            norm = _f(r.get(f"{prefix}_segment_{i}_normalized_propensity"))
            segments.append(SegmentScore(label=label, raw_score=raw, normalized_score=norm))
            if norm is not None and (dominant_score is None or norm > dominant_score):
                dominant_score = norm
                dominant_label = label

        return PropensityBlock(
            segments=segments,
            dominant_segment=dominant_label,
            dominant_segment_score=dominant_score,
        )

    async def _fetch_spend_trend(self, mobile: str, tbl_bt: str) -> list[SpendTrendPoint]:
        sql = f"""
            SELECT TO_CHAR(DATE_TRUNC('month', bt.bill_date), 'YYYY-MM') AS month,
                   SUM(COALESCE(bt.gross_sale_value, 0)) AS spend
              FROM {tbl_bt} bt
             WHERE RIGHT(bt.mobile_number, 10) = RIGHT(%s, 10)
               AND bt.bill_date IS NOT NULL
             GROUP BY 1
             ORDER BY 1 DESC
             LIMIT 9
        """
        try:
            rows = await self._query(sql, (mobile,))
        except Exception as e:
            logger.warning(f"spend trend fetch failed: {e}")
            return []
        pts = [SpendTrendPoint(month=r["month"], spend=_f(r["spend"]) or 0.0) for r in rows]
        return list(reversed(pts))

    async def _fetch_top_articles(self, mobile: str, tbl_bt: str) -> list[TopArticle]:
        """Top 10 articles by spend, each tagged with its product segment."""
        sql = f"""
            SELECT COALESCE(bt.article_desc, bt.article) AS article,
                   UPPER(TRIM(bt.segment_desc)) AS segment,
                   SUM(COALESCE(bt.gross_sale_value, 0)) AS spend
              FROM {tbl_bt} bt
             WHERE RIGHT(bt.mobile_number, 10) = RIGHT(%s, 10)
               AND COALESCE(bt.article_desc, bt.article) IS NOT NULL
             GROUP BY 1, 2
             HAVING SUM(COALESCE(bt.gross_sale_value, 0)) > 0
             ORDER BY spend DESC
             LIMIT 10
        """
        try:
            rows = await self._query(sql, (mobile,))
        except Exception as e:
            logger.warning(f"top articles fetch failed: {e}")
            return []
        return [
            TopArticle(
                article=_s(r["article"]) or "—",
                segment=_s(r["segment"]),
                spend=_f(r["spend"]) or 0.0,
            )
            for r in rows
        ]

    async def _fetch_category_spend(self, mobile: str, tbl_bt: str) -> list[CategorySpend]:
        """Actual spend rolled up per product segment, with % share and a
        normalized (relative-intensity) score."""
        sql = f"""
            SELECT UPPER(TRIM(bt.segment_desc)) AS segment,
                   SUM(COALESCE(bt.gross_sale_value, 0)) AS spend
              FROM {tbl_bt} bt
             WHERE RIGHT(bt.mobile_number, 10) = RIGHT(%s, 10)
               AND bt.segment_desc IS NOT NULL
             GROUP BY 1
             HAVING SUM(COALESCE(bt.gross_sale_value, 0)) > 0
             ORDER BY spend DESC
        """
        try:
            rows = await self._query(sql, (mobile,))
        except Exception as e:
            logger.warning(f"category spend fetch failed: {e}")
            return []
        spends = [(_s(r["segment"]) or "—", _f(r["spend"]) or 0.0) for r in rows]
        total = sum(s for _, s in spends)
        max_spend = max((s for _, s in spends), default=0.0)
        return [
            CategorySpend(
                segment=seg,
                spend=sp,
                share_pct=(sp / total * 100.0) if total > 0 else 0.0,
                normalized_score=(sp / max_spend) if max_spend > 0 else 0.0,
            )
            for seg, sp in spends
        ]

    # =========================================================================
    # Corporate cross-brand single view
    # =========================================================================

    async def get_corporate_view(
        self, r1_id: str | None, mobile: str | None
    ) -> CorporateSingleViewResponse:
        if r1_id:
            where = "r1_id = %s"
            params: tuple = (r1_id,)
        elif mobile:
            where = "RIGHT(mobile, 10) = RIGHT(%s, 10)"
            params = (mobile,)
        else:
            return CorporateSingleViewResponse(found=False)

        # Columns shared by both brand prefixes.
        per_brand = [
            "display_name", "city", "lifecycle_stage", "l2_segment",
            "total_spend", "total_bills", "total_visits", "spend_per_bill",
            "spend_per_visit", "recency_days", "first_bill_date", "last_bill_date",
            "fav_store_name", "fav_day", "store_spend", "online_spend",
            "dnd", "accepts_email_marketing", "accepts_sms_marketing",
            "spend_decile", "return_bill_count", "channel_presence",
            "rfm_recency_score", "rfm_frequency_score", "rfm_monetary_score",
        ]
        select_cols = ["r1_id", "mobile", "rpsg_brand_presence", "rpsg_ftd", "rpsg_ltd",
                       "rpsg_tenure_lifetime", "rpsg_recency_lifetime"]
        for pfx in ("spn", "nbl"):
            select_cols += [f"{pfx}_{c}" for c in per_brand]

        sql = f"SELECT {', '.join(select_cols)} FROM {CORPORATE_TABLE} WHERE {where} LIMIT 1"
        rows = await self._query(sql, params)
        if not rows:
            return CorporateSingleViewResponse(found=False)

        c = rows[0]
        presence = _s(c.get("rpsg_brand_presence")) or ""
        is_spn = "spencer" in presence.lower()
        is_nbl = "nbl" in presence.lower()

        # A brand panel is "present" if it has any spend signal for that brand.
        def _panel(pfx: str, present_flag: bool) -> BrandPanel:
            spend = _f(c.get(f"{pfx}_total_spend"))
            bills = _f(c.get(f"{pfx}_total_bills"))
            present = present_flag or bool(spend) or bool(bills)
            return BrandPanel(
                present=present,
                display_name=_s(c.get(f"{pfx}_display_name")),
                city=_s(c.get(f"{pfx}_city")),
                lifecycle_stage=_s(c.get(f"{pfx}_lifecycle_stage")),
                l2_segment=_s(c.get(f"{pfx}_l2_segment")),
                total_spend=spend,
                total_bills=bills,
                total_visits=_f(c.get(f"{pfx}_total_visits")),
                spend_per_bill=_f(c.get(f"{pfx}_spend_per_bill")),
                spend_per_visit=_f(c.get(f"{pfx}_spend_per_visit")),
                recency_days=_f(c.get(f"{pfx}_recency_days")),
                first_bill_date=_s(c.get(f"{pfx}_first_bill_date")),
                last_bill_date=_s(c.get(f"{pfx}_last_bill_date")),
                fav_store_name=_s(c.get(f"{pfx}_fav_store_name")),
                fav_day=_s(c.get(f"{pfx}_fav_day")),
                store_spend=_f(c.get(f"{pfx}_store_spend")),
                online_spend=_f(c.get(f"{pfx}_online_spend")),
                dnd=_s(c.get(f"{pfx}_dnd")),
                accepts_email_marketing=_s(c.get(f"{pfx}_accepts_email_marketing")),
                accepts_sms_marketing=_s(c.get(f"{pfx}_accepts_sms_marketing")),
            )

        identity = CorporateIdentityBlock(
            r1_id=c["r1_id"],
            mobile=_s(c.get("mobile")),
            name=_s(c.get("spn_display_name")) or _s(c.get("nbl_display_name")),
            brand_presence=presence or None,
            rpsg_ftd=_s(c.get("rpsg_ftd")),
            rpsg_ltd=_s(c.get("rpsg_ltd")),
            rpsg_tenure_lifetime=c.get("rpsg_tenure_lifetime"),
            rpsg_recency_lifetime=c.get("rpsg_recency_lifetime"),
            is_spencers_customer=is_spn,
            is_nbl_customer=is_nbl,
        )

        # ── Insights + metrics (derived from corp columns) ───────────────────────
        spn_spend = _f(c.get("spn_total_spend")) or 0.0
        nbl_spend = _f(c.get("nbl_total_spend")) or 0.0
        spn_bills = _f(c.get("spn_total_bills")) or 0.0
        nbl_bills = _f(c.get("nbl_total_bills")) or 0.0
        total_spend = spn_spend + nbl_spend
        total_bills = spn_bills + nbl_bills

        # Preferred brand = higher lifetime spend.
        pfx_pref = "spn" if spn_spend >= nbl_spend else "nbl"
        pref_brand_name = "Spencers" if pfx_pref == "spn" else "Nature's Basket"

        # Preferred channel by combined store vs online spend.
        store_sp = (_f(c.get("spn_store_spend")) or 0.0) + (_f(c.get("nbl_store_spend")) or 0.0)
        online_sp = (_f(c.get("spn_online_spend")) or 0.0) + (_f(c.get("nbl_online_spend")) or 0.0)
        pref_channel = None
        if store_sp or online_sp:
            pref_channel = "In-Store" if store_sp >= online_sp else "Online"

        # Preferred category from actual transaction spend (both brands).
        pref_category = await self._fetch_preferred_category(_s(c.get("mobile")))

        insights = CorporateInsights(
            lifetime_value=total_spend or None,
            preferred_brand=pref_brand_name if total_spend > 0 else None,
            preferred_category=pref_category,
            preferred_channel=pref_channel,
        )

        ret_bills = (_f(c.get("spn_return_bill_count")) or 0.0) + (_f(c.get("nbl_return_bill_count")) or 0.0)
        num_brands = len([t for t in presence.replace("[", "").replace("]", "").replace("'", "").split() if t]) or None

        metrics = CorporateMetrics(
            num_rpsg_brands=num_brands,
            avg_basket_value=(total_spend / total_bills) if total_bills > 0 else None,
            return_pct=(ret_bills / total_bills * 100.0) if total_bills > 0 else None,
            rfm_brand=pref_brand_name,
            rfm_recency=c.get(f"{pfx_pref}_rfm_recency_score"),
            rfm_frequency=c.get(f"{pfx_pref}_rfm_frequency_score"),
            rfm_monetary=c.get(f"{pfx_pref}_rfm_monetary_score"),
            spend_decile=c.get(f"{pfx_pref}_spend_decile"),
            segment=_s(c.get(f"{pfx_pref}_l2_segment")),
            lifecycle_stage=_s(c.get(f"{pfx_pref}_lifecycle_stage")),
        )

        # ── Activity timeline (transactions across both brands) ──────────────────
        timeline = await self._fetch_corporate_timeline(_s(c.get("mobile")))

        return CorporateSingleViewResponse(
            found=True,
            identity=identity,
            insights=insights,
            metrics=metrics,
            activity_timeline=timeline,
            spencers=_panel("spn", is_spn),
            nbl=_panel("nbl", is_nbl),
        )

    async def _fetch_preferred_category(self, mobile: str | None) -> str | None:
        if not mobile:
            return None
        unions = []
        for br in ("spencers", "natures_basket"):
            tbl = BRAND_SCHEMA_CONFIG[br]["s_fact_bill_transactions"]
            unions.append(
                f"SELECT UPPER(TRIM(segment_desc)) AS segment, "
                f"COALESCE(gross_sale_value,0) AS spend FROM {tbl} "
                f"WHERE RIGHT(mobile_number,10)=RIGHT(%s,10) AND segment_desc IS NOT NULL"
            )
        sql = (
            "SELECT segment, SUM(spend) AS total FROM (\n"
            + "\nUNION ALL\n".join(unions)
            + "\n) u GROUP BY segment ORDER BY total DESC LIMIT 1"
        )
        try:
            rows = await self._query(sql, (mobile, mobile))
        except Exception as e:
            logger.warning(f"preferred category fetch failed: {e}")
            return None
        return _s(rows[0]["segment"]) if rows else None

    async def _fetch_corporate_timeline(self, mobile: str | None) -> list[TimelineEvent]:
        if not mobile:
            return []
        parts = []
        for br, label in (("spencers", "Spencers"), ("natures_basket", "Nature's Basket")):
            tbl = BRAND_SCHEMA_CONFIG[br]["s_fact_bill_transactions"]
            parts.append(
                f"SELECT bill_date, '{label}' AS brand, "
                f"COALESCE(store_desc, store_code) AS store, "
                f"UPPER(TRIM(segment_desc)) AS segment, "
                f"COALESCE(gross_sale_value,0) AS spend "
                f"FROM {tbl} WHERE RIGHT(mobile_number,10)=RIGHT(%s,10) AND bill_date IS NOT NULL"
            )
        # One row per bill_date+brand+store (a "visit"), summing line items.
        sql = (
            "SELECT bill_date, brand, store, "
            "  (ARRAY_AGG(segment ORDER BY spend DESC))[1] AS segment, "
            "  SUM(spend) AS spend FROM (\n"
            + "\nUNION ALL\n".join(parts)
            + "\n) u GROUP BY bill_date, brand, store "
            "ORDER BY bill_date DESC LIMIT 20"
        )
        try:
            rows = await self._query(sql, (mobile, mobile))
        except Exception as e:
            logger.warning(f"corporate timeline fetch failed: {e}")
            return []
        return [
            TimelineEvent(
                date=_s(r["bill_date"]) or "",
                brand=_s(r["brand"]) or "",
                type="Purchase",
                store=_s(r["store"]),
                segment=_s(r["segment"]),
                spend=_f(r["spend"]),
            )
            for r in rows
        ]

"""
Customer Single View — response schemas
=======================================

Composes a single customer's 360° profile from:
  - unified_profiles            (identity, reachability)
  - customer_behavioral_attributes (behavioral panel)
  - customer_propensity_scores_*   (the 6 / 4 normalized segment scores)
  - s_fact_bill_transactions       (spend trend, location-category propensity)

Only data-backed panels are returned. Panels with no current source
(segment membership, engagement history, lookalike) are intentionally omitted.
"""

from __future__ import annotations

from pydantic import BaseModel


class IdentityBlock(BaseModel):
    unified_id: str
    surrogate_id: str | None = None
    name: str | None = None
    first_name: str | None = None
    last_name: str | None = None
    dob: str | None = None
    age: int | None = None
    email: str | None = None
    mobile: str | None = None
    city: str | None = None
    pincode: str | None = None
    street: str | None = None
    region: str | None = None
    registered_store: str | None = None
    customer_group: str | None = None
    occupation: str | None = None
    primary_source: str | None = None
    status: str | None = None


class BehavioralBlock(BaseModel):
    first_bill_date: str | None = None
    last_bill_date: str | None = None
    recency_days: float | None = None
    tenure_days: float | None = None
    total_bills: float | None = None
    total_visits: float | None = None
    total_spend: float | None = None
    spend_per_bill: float | None = None
    spend_per_visit: float | None = None
    total_discount: float | None = None
    spend_decile: int | None = None
    nob_decile: int | None = None
    l1_segment: str | None = None
    l2_segment: str | None = None
    lifecycle_stage: str | None = None
    fav_store_name: str | None = None
    fav_store_type: str | None = None
    fav_day: str | None = None
    fav_article_by_spend_desc: str | None = None
    fav_article_by_nob_desc: str | None = None
    channel_presence: str | None = None
    store_spend: float | None = None
    online_spend: float | None = None
    store_bills: float | None = None
    online_bills: float | None = None


class ReachabilityBlock(BaseModel):
    # Stored as text 'Yes'/'No' in unified_profiles, not booleans.
    dnd: str | None = None
    accepts_email_marketing: str | None = None
    accepts_sms_marketing: str | None = None
    whatsapp: str | None = None
    gw_customer_flag: str | None = None


class SegmentScore(BaseModel):
    label: str
    raw_score: float | None = None
    normalized_score: float | None = None


class PropensityBlock(BaseModel):
    segments: list[SegmentScore] = []
    dominant_segment: str | None = None
    dominant_segment_score: float | None = None


class SpendTrendPoint(BaseModel):
    month: str          # 'YYYY-MM'
    spend: float


class LocationPropensityCell(BaseModel):
    store: str
    segment: str
    spend: float


class CustomerSingleViewResponse(BaseModel):
    found: bool
    brand_code: str
    identity: IdentityBlock | None = None
    behavioral: BehavioralBlock | None = None
    reachability: ReachabilityBlock | None = None
    propensity: PropensityBlock | None = None
    spend_trend: list[SpendTrendPoint] = []
    location_propensity: list[LocationPropensityCell] = []


# =============================================================================
# Corporate cross-brand single view
# =============================================================================

class CorporateIdentityBlock(BaseModel):
    r1_id: str
    mobile: str | None = None
    name: str | None = None
    brand_presence: str | None = None
    rpsg_ftd: str | None = None          # first transaction across RPSG
    rpsg_ltd: str | None = None          # last transaction across RPSG
    rpsg_tenure_lifetime: int | None = None
    rpsg_recency_lifetime: int | None = None
    is_spencers_customer: bool = False
    is_nbl_customer: bool = False


class BrandPanel(BaseModel):
    """One brand's slice of a corporate customer's profile."""
    present: bool = False
    display_name: str | None = None
    city: str | None = None
    lifecycle_stage: str | None = None
    l2_segment: str | None = None
    total_spend: float | None = None
    total_bills: float | None = None
    total_visits: float | None = None
    spend_per_bill: float | None = None
    spend_per_visit: float | None = None
    recency_days: float | None = None
    first_bill_date: str | None = None
    last_bill_date: str | None = None
    fav_store_name: str | None = None
    fav_day: str | None = None
    store_spend: float | None = None
    online_spend: float | None = None
    dnd: str | None = None
    accepts_email_marketing: str | None = None
    accepts_sms_marketing: str | None = None


class CorporateSingleViewResponse(BaseModel):
    found: bool
    brand_code: str = "corporate"
    identity: CorporateIdentityBlock | None = None
    spencers: BrandPanel | None = None
    nbl: BrandPanel | None = None

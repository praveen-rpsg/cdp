/**
 * Corporate Customer Single View — cross-brand comparison.
 *
 * Corporate has no propensity or line-item data, so this view shows
 * identity + RPSG lifetime metrics + side-by-side Spencers vs NBL panels.
 * Lookup by RPSG ID (r1_id) or mobile number.
 */

import React, { useEffect, useState } from "react";
import { useSegmentStore } from "../../store/segmentStore";

interface BrandPanel {
  present: boolean;
  display_name: string | null;
  city: string | null;
  lifecycle_stage: string | null;
  l2_segment: string | null;
  total_spend: number | null;
  total_bills: number | null;
  total_visits: number | null;
  spend_per_bill: number | null;
  spend_per_visit: number | null;
  recency_days: number | null;
  first_bill_date: string | null;
  last_bill_date: string | null;
  fav_store_name: string | null;
  fav_day: string | null;
  store_spend: number | null;
  online_spend: number | null;
  dnd: string | null;
  accepts_email_marketing: string | null;
  accepts_sms_marketing: string | null;
}
interface CorporateView {
  found: boolean;
  identity?: {
    r1_id: string;
    mobile: string | null;
    name: string | null;
    brand_presence: string | null;
    rpsg_ftd: string | null;
    rpsg_ltd: string | null;
    rpsg_tenure_lifetime: number | null;
    rpsg_recency_lifetime: number | null;
    is_spencers_customer: boolean;
    is_nbl_customer: boolean;
  } | null;
  insights?: {
    lifetime_value: number | null;
    preferred_brand: string | null;
    preferred_category: string | null;
    preferred_channel: string | null;
  } | null;
  metrics?: {
    num_rpsg_brands: number | null;
    avg_basket_value: number | null;
    return_pct: number | null;
    rfm_brand: string | null;
    rfm_recency: number | null;
    rfm_frequency: number | null;
    rfm_monetary: number | null;
    spend_decile: number | null;
    segment: string | null;
    lifecycle_stage: string | null;
  } | null;
  activity_timeline: {
    date: string;
    brand: string;
    type: string;
    store: string | null;
    segment: string | null;
    spend: number | null;
  }[];
  spencers?: BrandPanel | null;
  nbl?: BrandPanel | null;
}

const SEGMENT_COLORS: Record<string, string> = {
  "FOOD": "#16a34a",
  "NON FOOD GROCERY": "#0891b2",
  "GM": "#7c3aed",
  "HI TECH": "#2563eb",
  "FASHION CB": "#db2777",
  "NON TRADE": "#ea580c",
};
const BRAND_COLORS: Record<string, string> = {
  "Spencers": "#E0402E",
  "Nature's Basket": "#16a34a",
};

const fmtINR = (v: number | null | undefined) =>
  v == null ? "—" : "₹" + Math.round(v).toLocaleString("en-IN");
const fmtNum = (v: number | null | undefined) =>
  v == null ? "—" : Math.round(v).toLocaleString("en-IN");

const Field: React.FC<{ label: string; value: React.ReactNode }> = ({ label, value }) => (
  <div className="flex justify-between gap-3 py-1 text-sm">
    <span className="text-gray-500">{label}</span>
    <span className="font-medium text-gray-800 text-right truncate">{value ?? "—"}</span>
  </div>
);

const HeroCard: React.FC<{ label: string; value: React.ReactNode; accent: string }> = ({
  label, value, accent,
}) => (
  <div className="bg-white rounded-xl border border-gray-200 shadow-sm p-4 border-l-4" style={{ borderLeftColor: accent }}>
    <div className="text-xs text-gray-400 uppercase tracking-wider">{label}</div>
    <div className="text-lg font-bold text-gray-900 mt-1 truncate" title={typeof value === "string" ? value : undefined}>{value}</div>
  </div>
);

const MetricTile: React.FC<{ label: string; value: React.ReactNode }> = ({ label, value }) => (
  <div className="bg-gray-50 rounded-lg p-3 border border-gray-100">
    <div className="text-[10px] text-gray-400 uppercase tracking-wider truncate">{label}</div>
    <div className="text-sm font-semibold text-gray-800 mt-1 truncate" title={typeof value === "string" ? value : undefined}>{value}</div>
  </div>
);

const BrandColumn: React.FC<{ title: string; color: string; panel?: BrandPanel | null }> = ({
  title, color, panel,
}) => {
  if (!panel?.present) {
    return (
      <div className="bg-white rounded-xl border border-gray-200 shadow-sm p-4">
        <h3 className="text-sm font-bold mb-2" style={{ color }}>{title}</h3>
        <p className="text-xs text-gray-400 py-6 text-center">Not a {title} customer.</p>
      </div>
    );
  }
  return (
    <div className="bg-white rounded-xl border border-gray-200 shadow-sm p-4">
      <h3 className="text-sm font-bold mb-3 flex items-center gap-2" style={{ color }}>
        <span className="w-2.5 h-2.5 rounded-full" style={{ backgroundColor: color }} />
        {title}
      </h3>
      <Field label="Name" value={panel.display_name} />
      <Field label="City" value={panel.city} />
      <Field label="Lifecycle" value={panel.lifecycle_stage} />
      <Field label="Segment (L2)" value={panel.l2_segment} />
      <div className="my-2 border-t border-gray-100" />
      <Field label="Total Spend" value={fmtINR(panel.total_spend)} />
      <Field label="Total Bills" value={fmtNum(panel.total_bills)} />
      <Field label="Total Visits" value={fmtNum(panel.total_visits)} />
      <Field label="Spend / Bill" value={fmtINR(panel.spend_per_bill)} />
      <Field label="Recency (days)" value={fmtNum(panel.recency_days)} />
      <div className="my-2 border-t border-gray-100" />
      <Field label="First Bill" value={panel.first_bill_date} />
      <Field label="Last Bill" value={panel.last_bill_date} />
      <Field label="Fav Store" value={panel.fav_store_name} />
      <Field label="Store Spend" value={fmtINR(panel.store_spend)} />
      <Field label="Online Spend" value={fmtINR(panel.online_spend)} />
      <div className="my-2 border-t border-gray-100" />
      <Field label="DND" value={panel.dnd} />
      <Field label="Email Opt-In" value={panel.accepts_email_marketing} />
      <Field label="SMS Opt-In" value={panel.accepts_sms_marketing} />
    </div>
  );
};

export const CorporateSingleView: React.FC = () => {
  const { pendingCorporateView, setPendingCorporateView } = useSegmentStore();
  const [searchBy, setSearchBy] = useState<"mobile" | "id">("mobile");
  const [query, setQuery] = useState("");
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [data, setData] = useState<CorporateView | null>(null);

  // Deep-link handoff from a (corporate) segment preview row.
  useEffect(() => {
    if (pendingCorporateView) {
      const { mobile, r1_id } = pendingCorporateView;
      // Prefer mobile if present, else fall back to r1_id.
      const by: "mobile" | "id" = mobile ? "mobile" : "id";
      const q = mobile || r1_id || "";
      setSearchBy(by);
      setQuery(q);
      setPendingCorporateView(null);
      if (q) doSearch(q, by);
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [pendingCorporateView]);

  const doSearch = async (q: string = query, by: "mobile" | "id" = searchBy) => {
    if (!q.trim()) return;
    setLoading(true);
    setError(null);
    setData(null);
    const param = by === "mobile" ? "mobile" : "r1_id";
    try {
      const res = await fetch(
        `/api/v1/customers/corporate-view?${param}=${encodeURIComponent(q.trim())}`
      );
      const json = await res.json();
      if (!json.found) {
        setError(`No corporate customer found for this ${by === "mobile" ? "mobile number" : "RPSG ID"}`);
      } else {
        setData(json);
      }
    } catch {
      setError("Lookup failed. Is the backend running?");
    } finally {
      setLoading(false);
    }
  };

  const id = data?.identity;

  return (
    <div className="max-w-7xl w-full mx-auto px-4 sm:px-6 py-4 sm:py-6 space-y-5">
      {/* Search bar */}
      <div className="bg-white rounded-xl border border-gray-200 shadow-sm p-4 flex flex-col sm:flex-row gap-3 items-stretch sm:items-end">
        <div className="w-full sm:w-40">
          <label className="block text-xs font-semibold text-gray-600 uppercase tracking-wider mb-1.5">Search By</label>
          <select
            value={searchBy}
            onChange={(e) => { setSearchBy(e.target.value as "mobile" | "id"); setQuery(""); }}
            className="w-full px-3 py-2 border border-gray-200 rounded-lg text-sm bg-white focus:ring-2 focus:ring-indigo-500 outline-none cursor-pointer"
          >
            <option value="mobile">Mobile Number</option>
            <option value="id">RPSG ID (r1_id)</option>
          </select>
        </div>
        <div className="flex-1">
          <label className="block text-xs font-semibold text-gray-600 uppercase tracking-wider mb-1.5">
            {searchBy === "mobile" ? "Mobile Number" : "RPSG ID"}
          </label>
          <input
            type="text"
            value={query}
            onChange={(e) => setQuery(e.target.value)}
            onKeyDown={(e) => e.key === "Enter" && doSearch()}
            placeholder={searchBy === "mobile" ? "e.g., 9830159393" : "e.g., 5JGS"}
            className="w-full px-3 py-2 border border-gray-200 rounded-lg text-sm focus:ring-2 focus:ring-indigo-500 outline-none font-mono"
          />
        </div>
        <button
          onClick={() => doSearch()}
          disabled={loading || !query.trim()}
          className="px-5 py-2 bg-purple-700 text-white rounded-lg text-sm font-medium hover:bg-purple-800 disabled:opacity-40 active:scale-95 transition"
        >
          {loading ? "Searching…" : "Search"}
        </button>
      </div>

      {error && (
        <div className="bg-amber-50 border border-amber-200 text-amber-800 rounded-xl px-4 py-3 text-sm">{error}</div>
      )}

      {!data && !error && !loading && (
        <div className="text-center text-gray-400 py-20 text-sm">
          Search a corporate customer by RPSG ID or mobile to see their cross-brand profile.
        </div>
      )}

      {data && id && (
        <>
          {/* Identity / RPSG lifetime */}
          <div className="bg-white rounded-xl border border-gray-200 shadow-sm p-4">
            <h3 className="text-xs font-semibold text-gray-500 uppercase tracking-wider mb-3 flex items-center gap-2">
              🔗 RPSG Cross-Brand Identity
            </h3>
            <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-x-8">
              <Field label="Name" value={id.name} />
              <Field label="RPSG ID" value={<span className="font-mono text-xs">{id.r1_id}</span>} />
              <Field label="Mobile" value={id.mobile} />
              <Field label="Brand Presence" value={id.brand_presence} />
              <Field label="First Txn (RPSG)" value={id.rpsg_ftd} />
              <Field label="Last Txn (RPSG)" value={id.rpsg_ltd} />
              <Field label="Tenure (days)" value={fmtNum(id.rpsg_tenure_lifetime)} />
              <Field label="Recency (days)" value={fmtNum(id.rpsg_recency_lifetime)} />
            </div>
            <div className="mt-3 pt-3 border-t border-gray-100 flex gap-2">
              {id.is_spencers_customer && (
                <span className="px-2 py-0.5 rounded-full text-xs font-semibold text-white" style={{ backgroundColor: "#E0402E" }}>Spencers</span>
              )}
              {id.is_nbl_customer && (
                <span className="px-2 py-0.5 rounded-full text-xs font-semibold text-white" style={{ backgroundColor: "#16a34a" }}>Nature's Basket</span>
              )}
            </div>
          </div>

          {/* Insights hero strip */}
          {data.insights && (
            <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-4">
              <HeroCard label="Lifetime Value" value={fmtINR(data.insights.lifetime_value)} accent="#7B2D8B" />
              <HeroCard label="Preferred Brand" value={data.insights.preferred_brand || "—"} accent="#E0402E" />
              <HeroCard label="Preferred Category" value={data.insights.preferred_category || "—"} accent="#16a34a" />
              <HeroCard label="Preferred Channel" value={data.insights.preferred_channel || "—"} accent="#2563eb" />
            </div>
          )}

          {/* Wallet share + cross-sell */}
          {(() => {
            const spn = data.spencers?.total_spend ?? 0;
            const nbl = data.nbl?.total_spend ?? 0;
            const tot = spn + nbl;
            const spnActive = !!data.spencers?.present;
            const nblActive = !!data.nbl?.present;
            const crossSell =
              spnActive && !nblActive
                ? "Active in Spencers only — cross-sell Nature's Basket"
                : nblActive && !spnActive
                ? "Active in Nature's Basket only — cross-sell Spencers"
                : null;
            if (tot <= 0 && !crossSell) return null;
            return (
              <div className="bg-white rounded-xl border border-gray-200 shadow-sm p-4">
                <h3 className="text-xs font-semibold text-gray-500 uppercase tracking-wider mb-3 flex items-center gap-2">
                  💰 Wallet Share
                </h3>
                {tot > 0 && (
                  <>
                    <div className="flex h-6 rounded-lg overflow-hidden">
                      {spn > 0 && (
                        <div className="flex items-center justify-center text-[11px] font-semibold text-white"
                          style={{ width: `${(spn / tot) * 100}%`, backgroundColor: "#E0402E" }}>
                          {((spn / tot) * 100).toFixed(0)}%
                        </div>
                      )}
                      {nbl > 0 && (
                        <div className="flex items-center justify-center text-[11px] font-semibold text-white"
                          style={{ width: `${(nbl / tot) * 100}%`, backgroundColor: "#16a34a" }}>
                          {((nbl / tot) * 100).toFixed(0)}%
                        </div>
                      )}
                    </div>
                    <div className="flex justify-between text-xs text-gray-500 mt-1.5">
                      <span>Spencers · {fmtINR(spn)}</span>
                      <span>Nature's Basket · {fmtINR(nbl)}</span>
                    </div>
                  </>
                )}
                {crossSell && (
                  <div className="mt-3 flex items-center gap-2 px-3 py-2 bg-amber-50 border border-amber-200 rounded-lg text-sm text-amber-800">
                    <span>🎯</span>
                    <span className="font-medium">{crossSell}</span>
                  </div>
                )}
              </div>
            );
          })()}

          {/* Metrics tile grid */}
          {data.metrics && (
            <div className="bg-white rounded-xl border border-gray-200 shadow-sm p-4">
              <h3 className="text-xs font-semibold text-gray-500 uppercase tracking-wider mb-3 flex items-center gap-2">
                📊 Key Metrics
                {data.metrics.rfm_brand && (
                  <span className="text-[10px] normal-case font-normal text-gray-400">
                    (RFM / decile / segment from {data.metrics.rfm_brand})
                  </span>
                )}
              </h3>
              <div className="grid grid-cols-2 sm:grid-cols-3 lg:grid-cols-4 gap-3">
                <MetricTile label="RPSG Brands" value={fmtNum(data.metrics.num_rpsg_brands)} />
                <MetricTile label="Avg Basket Value" value={fmtINR(data.metrics.avg_basket_value)} />
                <MetricTile label="Return %" value={data.metrics.return_pct == null ? "—" : data.metrics.return_pct.toFixed(1) + "%"} />
                <MetricTile label="Spend Decile" value={fmtNum(data.metrics.spend_decile)} />
                <MetricTile label="RFM (R/F/M)" value={[data.metrics.rfm_recency, data.metrics.rfm_frequency, data.metrics.rfm_monetary].every((x) => x == null) ? "—" : `${data.metrics.rfm_recency ?? "–"}/${data.metrics.rfm_frequency ?? "–"}/${data.metrics.rfm_monetary ?? "–"}`} />
                <MetricTile label="Segment (L2)" value={data.metrics.segment || "—"} />
                <MetricTile label="Lifecycle" value={data.metrics.lifecycle_stage || "—"} />
              </div>
            </div>
          )}

          {/* Activity Timeline */}
          {data.activity_timeline.length > 0 && (
            <div className="bg-white rounded-xl border border-gray-200 shadow-sm p-4">
              <h3 className="text-xs font-semibold text-gray-500 uppercase tracking-wider mb-4 flex items-center gap-2">
                🕑 Activity Timeline
                <span className="text-[10px] normal-case font-normal text-gray-400">(transaction events, both brands)</span>
              </h3>
              <div className="overflow-x-auto pb-2">
                <div className="flex items-start gap-0 min-w-min relative">
                  {/* connecting line */}
                  <div className="absolute top-3 left-0 right-0 h-0.5 bg-gray-200" style={{ zIndex: 0 }} />
                  {data.activity_timeline.map((e, i) => {
                    const dot = BRAND_COLORS[e.brand] || "#6366f1";
                    return (
                      <div key={i} className="flex flex-col items-center px-4 relative" style={{ zIndex: 1, minWidth: 130 }}>
                        <div className="w-6 h-6 rounded-full border-2 border-white shadow flex items-center justify-center" style={{ backgroundColor: dot }}>
                          <span className="w-2 h-2 rounded-full bg-white" />
                        </div>
                        <div className="text-[10px] text-gray-400 mt-1.5">{e.date}</div>
                        <div className="text-xs font-semibold text-gray-800 mt-0.5 text-center leading-tight">{e.brand}</div>
                        <div className="text-[11px] text-gray-600 text-center leading-tight truncate max-w-[120px]" title={e.store || ""}>{e.store || "—"}</div>
                        {e.segment && (
                          <span className="mt-1 px-1.5 py-0.5 rounded text-[9px] font-medium text-white" style={{ backgroundColor: SEGMENT_COLORS[e.segment] || "#94a3b8" }}>
                            {e.segment}
                          </span>
                        )}
                        <div className="text-[11px] font-semibold text-gray-700 mt-1 tabular-nums">{fmtINR(e.spend)}</div>
                      </div>
                    );
                  })}
                </div>
              </div>
            </div>
          )}

          {/* Side-by-side brand comparison */}
          <div className="grid grid-cols-1 lg:grid-cols-2 gap-5">
            <BrandColumn title="Spencers" color="#E0402E" panel={data.spencers} />
            <BrandColumn title="Nature's Basket" color="#16a34a" panel={data.nbl} />
          </div>
        </>
      )}
    </div>
  );
};

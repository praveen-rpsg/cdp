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
  spencers?: BrandPanel | null;
  nbl?: BrandPanel | null;
}

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
                <span className="px-2 py-0.5 rounded-full text-xs font-semibold text-white" style={{ backgroundColor: "#E0402E" }}>Spencer's</span>
              )}
              {id.is_nbl_customer && (
                <span className="px-2 py-0.5 rounded-full text-xs font-semibold text-white" style={{ backgroundColor: "#16a34a" }}>Nature's Basket</span>
              )}
            </div>
          </div>

          {/* Side-by-side brand comparison */}
          <div className="grid grid-cols-1 lg:grid-cols-2 gap-5">
            <BrandColumn title="Spencer's" color="#E0402E" panel={data.spencers} />
            <BrandColumn title="Nature's Basket" color="#16a34a" panel={data.nbl} />
          </div>
        </>
      )}
    </div>
  );
};

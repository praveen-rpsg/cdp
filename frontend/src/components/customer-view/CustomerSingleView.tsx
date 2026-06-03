/**
 * Customer Single View — 360° profile for a single customer.
 *
 * Lookup by mobile number. Renders only data-backed panels:
 * Key Attributes, Behavioral, Reachability, Brand/Category Propensity,
 * Location Propensity, and Last-9-Months Spend Trend.
 */

import React, { useEffect, useState } from "react";
import { useSegmentStore } from "../../store/segmentStore";

/* ── Types ─────────────────────────────────────────────────── */
interface SegmentScore {
  label: string;
  raw_score: number | null;
  normalized_score: number | null;
}
interface SingleView {
  found: boolean;
  brand_code: string;
  identity?: Record<string, any> | null;
  behavioral?: Record<string, any> | null;
  reachability?: Record<string, any> | null;
  propensity?: {
    segments: SegmentScore[];
    dominant_segment: string | null;
    dominant_segment_score: number | null;
  } | null;
  spend_trend: { month: string; spend: number }[];
  top_articles: { article: string; segment: string | null; spend: number }[];
  location_propensity: { segment: string; spend: number; share_pct: number; normalized_score: number }[];
}

const SEGMENT_COLORS: Record<string, string> = {
  "FOOD": "#16a34a",
  "NON FOOD GROCERY": "#0891b2",
  "GM": "#7c3aed",
  "HI TECH": "#2563eb",
  "FASHION CB": "#db2777",
  "NON TRADE": "#ea580c",
};

const fmtINR = (v: number | null | undefined) =>
  v == null ? "—" : "₹" + Math.round(v).toLocaleString("en-IN");
const fmtNum = (v: number | null | undefined) =>
  v == null ? "—" : Math.round(v).toLocaleString("en-IN");

/* ── Small UI helpers ──────────────────────────────────────── */
const Card: React.FC<{ title: string; icon?: string; children: React.ReactNode; className?: string }> = ({
  title, icon, children, className,
}) => (
  <div className={`bg-white rounded-xl border border-gray-200 shadow-sm p-4 ${className || ""}`}>
    <h3 className="text-xs font-semibold text-gray-500 uppercase tracking-wider mb-3 flex items-center gap-2">
      {icon && <span>{icon}</span>}{title}
    </h3>
    {children}
  </div>
);

const Field: React.FC<{ label: string; value: React.ReactNode }> = ({ label, value }) => (
  <div className="flex justify-between gap-3 py-1 text-sm">
    <span className="text-gray-500">{label}</span>
    <span className="font-medium text-gray-800 text-right truncate">{value ?? "—"}</span>
  </div>
);

/* ── Channel Split / Omnichannel table ─────────────────────── */
const ChannelSplitTable: React.FC<{ b: Record<string, any> }> = ({ b }) => {
  const online = b.online_spend ?? 0;
  const store = b.store_spend ?? 0;
  const total = online + store;
  const onlineBills = b.online_bills ?? null;
  const storeBills = b.store_bills ?? null;
  const totalBills = b.total_bills ?? null;
  const share = (v: number) => (total > 0 ? ((v / total) * 100).toFixed(1) + "%" : "—");

  const rows = [
    { label: "Online", bills: onlineBills, spend: online, share: share(online), accent: "#2563eb" },
    { label: "Offline (Store)", bills: storeBills, spend: store, share: share(store), accent: "#ea580c" },
  ];

  return (
    <div className="bg-white rounded-xl border border-gray-200 shadow-sm overflow-hidden">
      <h3 className="text-xs font-semibold text-gray-500 uppercase tracking-wider px-4 pt-4 pb-2 flex items-center gap-2">
        🛒 Omnichannel Profile
      </h3>
      <table className="w-full text-sm">
        <thead>
          <tr className="bg-slate-800 text-white text-xs uppercase tracking-wider">
            <th className="py-2.5 px-4 text-left font-semibold">Channel</th>
            <th className="py-2.5 px-4 text-right font-semibold">Bills</th>
            <th className="py-2.5 px-4 text-right font-semibold">Spend</th>
            <th className="py-2.5 px-4 text-right font-semibold">Share</th>
          </tr>
        </thead>
        <tbody>
          {rows.map((r) => (
            <tr key={r.label} className="border-b border-gray-100">
              <td className="py-2.5 px-4 font-medium text-gray-800">
                <span className="inline-flex items-center gap-2">
                  <span className="w-2 h-2 rounded-full" style={{ backgroundColor: r.accent }} />
                  {r.label}
                </span>
              </td>
              <td className="py-2.5 px-4 text-right tabular-nums text-gray-600">{fmtNum(r.bills)}</td>
              <td className="py-2.5 px-4 text-right tabular-nums text-gray-700">{fmtINR(r.spend)}</td>
              <td className="py-2.5 px-4 text-right tabular-nums font-semibold text-gray-800">{r.share}</td>
            </tr>
          ))}
          <tr className="bg-gray-50">
            <td className="py-2.5 px-4 font-bold text-gray-900">Combined</td>
            <td className="py-2.5 px-4 text-right tabular-nums font-bold text-gray-900">{fmtNum(totalBills)}</td>
            <td className="py-2.5 px-4 text-right tabular-nums font-bold text-gray-900">{fmtINR(total)}</td>
            <td className="py-2.5 px-4 text-right tabular-nums font-bold text-gray-900">{total > 0 ? "100%" : "—"}</td>
          </tr>
        </tbody>
      </table>
    </div>
  );
};

/* ══════════════════════════════════════════════════════════════ */
export const CustomerSingleView: React.FC = () => {
  const { brands, selectedBrandCode, setSelectedBrand, pendingSingleView, setPendingSingleView } =
    useSegmentStore();
  const [searchBy, setSearchBy] = useState<"mobile" | "id">("mobile");
  const [query, setQuery] = useState("");
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [data, setData] = useState<SingleView | null>(null);

  const brand = selectedBrandCode || "spencers";

  // Deep-link handoff from a segment preview row (via the store).
  useEffect(() => {
    if (pendingSingleView?.mobile) {
      const { mobile: m, brand: br } = pendingSingleView;
      const targetBrand = br || brand;
      setSearchBy("mobile");
      setQuery(m);
      if (br) setSelectedBrand(br);
      setPendingSingleView(null);
      doSearch(m, "mobile", targetBrand);
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [pendingSingleView]);

  const doSearch = async (q: string, by: "mobile" | "id", br: string) => {
    if (!q.trim()) return;
    setLoading(true);
    setError(null);
    setData(null);
    const param = by === "mobile" ? "mobile" : "customer_id";
    try {
      const res = await fetch(
        `/api/v1/customers/single-view?brand_code=${encodeURIComponent(br)}&${param}=${encodeURIComponent(q.trim())}`
      );
      const json = await res.json();
      if (!json.found) {
        setError(
          `No customer found for this ${by === "mobile" ? "mobile number" : "unified ID"} in ${br}`
        );
      } else {
        setData(json);
      }
    } catch {
      setError("Lookup failed. Is the backend running?");
    } finally {
      setLoading(false);
    }
  };

  const id = data?.identity || {};
  const b = data?.behavioral || {};
  const r = data?.reachability || {};
  const prop = data?.propensity;

  const maxNorm = Math.max(0.0001, ...(prop?.segments.map((s) => s.normalized_score || 0) || [0]));

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
            <option value="id">Unified ID</option>
          </select>
        </div>
        <div className="flex-1">
          <label className="block text-xs font-semibold text-gray-600 uppercase tracking-wider mb-1.5">
            {searchBy === "mobile" ? "Mobile Number" : "Unified ID"}
          </label>
          <input
            type="text"
            value={query}
            onChange={(e) => setQuery(e.target.value)}
            onKeyDown={(e) => e.key === "Enter" && doSearch(query, searchBy, brand)}
            placeholder={searchBy === "mobile" ? "e.g., 9876543210" : "e.g., 0aa0cfa0f9dc92d204b332f44844be4e"}
            className="w-full px-3 py-2 border border-gray-200 rounded-lg text-sm focus:ring-2 focus:ring-indigo-500 outline-none font-mono"
          />
        </div>
        <div className="w-full sm:w-48">
          <label className="block text-xs font-semibold text-gray-600 uppercase tracking-wider mb-1.5">Brand</label>
          <select
            value={brand}
            onChange={(e) => setSelectedBrand(e.target.value)}
            className="w-full px-3 py-2 border border-gray-200 rounded-lg text-sm bg-white focus:ring-2 focus:ring-indigo-500 outline-none cursor-pointer"
          >
            {brands.filter((x) => x.code === "spencers" || x.code === "natures_basket").map((x) => (
              <option key={x.code} value={x.code}>{x.name}</option>
            ))}
          </select>
        </div>
        <button
          onClick={() => doSearch(query, searchBy, brand)}
          disabled={loading || !query.trim()}
          className="px-5 py-2 bg-indigo-600 text-white rounded-lg text-sm font-medium hover:bg-indigo-700 disabled:opacity-40 active:scale-95 transition"
        >
          {loading ? "Searching…" : "Search"}
        </button>
      </div>

      {error && (
        <div className="bg-amber-50 border border-amber-200 text-amber-800 rounded-xl px-4 py-3 text-sm">{error}</div>
      )}

      {!data && !error && !loading && (
        <div className="text-center text-gray-400 py-20 text-sm">
          Search by mobile number or unified ID to view a customer's 360° profile.
        </div>
      )}

      {data && (
        <>
          {/* Row 1: Identity / Behavioral / Reachability */}
          <div className="grid grid-cols-1 lg:grid-cols-3 gap-5">
            <Card title="Key Attributes" icon="👤">
              <Field label="Name" value={id.name} />
              <Field label="Unified ID" value={<span className="font-mono text-xs">{id.unified_id}</span>} />
              <Field label="Mobile" value={id.mobile} />
              <Field label="Email" value={id.email} />
              <Field label="DOB" value={id.dob} />
              <Field label="Age" value={id.age} />
              <Field label="City" value={id.city} />
              <Field label="Pincode" value={id.pincode} />
              <Field label="Customer Group" value={id.customer_group} />
              <Field label="Source" value={id.primary_source} />
            </Card>

            <Card title="Behavioral Attributes" icon="🎯">
              <Field label="First Bill" value={b.first_bill_date} />
              <Field label="Last Bill" value={b.last_bill_date} />
              <Field label="Recency (days)" value={fmtNum(b.recency_days)} />
              <Field label="Total Bills" value={fmtNum(b.total_bills)} />
              <Field label="Total Visits" value={fmtNum(b.total_visits)} />
              <Field label="Total Spend" value={fmtINR(b.total_spend)} />
              <Field label="Spend / Bill" value={fmtINR(b.spend_per_bill)} />
              <Field label="Spend Decile" value={b.spend_decile} />
              <Field label="Lifecycle" value={b.lifecycle_stage} />
              <Field
                label="Fav Store"
                value={
                  b.fav_store_name
                    ? `${b.fav_store_name}${id.registered_store ? ` (${id.registered_store})` : ""}`
                    : id.registered_store || "—"
                }
              />
              <Field label="Fav Day" value={b.fav_day} />
            </Card>

            <Card title="Reachability" icon="📡">
              <Field label="DND" value={r.dnd} />
              <Field label="Email Opt-In" value={r.accepts_email_marketing} />
              <Field label="SMS Opt-In" value={r.accepts_sms_marketing} />
              <Field label="WhatsApp" value={r.whatsapp} />
              <Field label="Channel Presence" value={b.channel_presence} />
            </Card>
          </div>

          {/* Omnichannel / Channel Split */}
          <ChannelSplitTable b={b} />

          {/* Row 2: Brand Propensity + Category Propensity */}
          {prop && prop.segments.length > 0 && (
            <div className="grid grid-cols-1 lg:grid-cols-2 gap-5">
              <Card title="Brand Propensity (Normalized Affinity)" icon="📊">
                <div className="space-y-2.5 mt-1">
                  {prop.segments.map((s) => {
                    const pct = ((s.normalized_score || 0) / maxNorm) * 100;
                    const color = SEGMENT_COLORS[s.label] || "#6366f1";
                    return (
                      <div key={s.label} className="flex items-center gap-3">
                        <span className="w-36 text-xs text-gray-600 truncate" title={s.label}>{s.label}</span>
                        <div className="flex-1 h-5 bg-gray-100 rounded overflow-hidden">
                          <div className="h-full rounded transition-all duration-500" style={{ width: `${pct}%`, backgroundColor: color }} />
                        </div>
                        <span className="w-12 text-right text-xs font-semibold tabular-nums text-gray-700">
                          {((s.normalized_score || 0) * 100).toFixed(0)}%
                        </span>
                      </div>
                    );
                  })}
                </div>
                {prop.dominant_segment && (
                  <div className="mt-4 pt-3 border-t border-gray-100 flex items-center gap-2 text-sm">
                    <span className="text-gray-500">Dominant Affinity:</span>
                    <span
                      className="px-2 py-0.5 rounded-full text-xs font-semibold text-white"
                      style={{ backgroundColor: SEGMENT_COLORS[prop.dominant_segment] || "#6366f1" }}
                    >
                      {prop.dominant_segment} · {((prop.dominant_segment_score || 0) * 100).toFixed(0)}%
                    </span>
                  </div>
                )}
              </Card>

              <Card title="Category Propensity" icon="🎯">
                <table className="w-full text-sm">
                  <thead>
                    <tr className="text-left text-xs text-gray-400 uppercase tracking-wider border-b border-gray-100">
                      <th className="py-1.5">Segment</th>
                      <th className="py-1.5 text-right">Raw</th>
                      <th className="py-1.5 text-right">Normalized</th>
                    </tr>
                  </thead>
                  <tbody>
                    {prop.segments.map((s) => (
                      <tr key={s.label} className="border-b border-gray-50">
                        <td className="py-1.5 flex items-center gap-2">
                          <span className="w-2.5 h-2.5 rounded-full" style={{ backgroundColor: SEGMENT_COLORS[s.label] || "#6366f1" }} />
                          {s.label}
                        </td>
                        <td className="py-1.5 text-right tabular-nums text-gray-600">
                          {s.raw_score == null ? "—" : s.raw_score.toFixed(3)}
                        </td>
                        <td className="py-1.5 text-right tabular-nums font-semibold text-gray-800">
                          {s.normalized_score == null ? "—" : (s.normalized_score * 100).toFixed(1) + "%"}
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </Card>
            </div>
          )}

          {/* Row 3: Top Articles + Category spend */}
          <div className="grid grid-cols-1 lg:grid-cols-2 gap-5">
            {data.top_articles.length > 0 && (
              <Card title="Top 10 Articles (by Spend)" icon="🏆">
                <div className="max-h-72 overflow-y-auto">
                  <table className="w-full text-xs">
                    <thead className="text-gray-400 uppercase tracking-wider sticky top-0 bg-white">
                      <tr className="border-b border-gray-100">
                        <th className="py-1.5 text-left w-6">#</th>
                        <th className="py-1.5 text-left">Article (Segment)</th>
                        <th className="py-1.5 text-right">Spend</th>
                      </tr>
                    </thead>
                    <tbody>
                      {data.top_articles.map((a, i) => (
                        <tr key={i} className="border-b border-gray-50">
                          <td className="py-1.5 text-gray-400">{i + 1}</td>
                          <td className="py-1.5">
                            <span className="text-gray-800">{a.article}</span>
                            {a.segment && (
                              <span
                                className="ml-1.5 px-1.5 py-0.5 rounded text-[10px] font-medium text-white align-middle"
                                style={{ backgroundColor: SEGMENT_COLORS[a.segment] || "#94a3b8" }}
                              >
                                {a.segment}
                              </span>
                            )}
                          </td>
                          <td className="py-1.5 text-right tabular-nums whitespace-nowrap">{fmtINR(a.spend)}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              </Card>
            )}

            {data.location_propensity.length > 0 && (
              <Card title="Location Category Propensity" icon="📍">
                <table className="w-full text-xs">
                  <thead className="text-gray-400 uppercase tracking-wider">
                    <tr className="border-b border-gray-100">
                      <th className="py-1.5 text-left">Segment</th>
                      <th className="py-1.5 text-right">Spend</th>
                      <th className="py-1.5 text-right">% Share</th>
                      <th className="py-1.5 text-left pl-3 w-1/3">Normalized</th>
                    </tr>
                  </thead>
                  <tbody>
                    {data.location_propensity.map((c, i) => {
                      const color = SEGMENT_COLORS[c.segment] || "#94a3b8";
                      return (
                        <tr key={i} className="border-b border-gray-50">
                          <td className="py-1.5">
                            <span className="px-1.5 py-0.5 rounded text-[10px] font-medium text-white"
                              style={{ backgroundColor: color }}>
                              {c.segment}
                            </span>
                          </td>
                          <td className="py-1.5 text-right tabular-nums whitespace-nowrap">{fmtINR(c.spend)}</td>
                          <td className="py-1.5 text-right tabular-nums">{c.share_pct.toFixed(1)}%</td>
                          <td className="py-1.5 pl-3">
                            <div className="flex items-center gap-2">
                              <div className="flex-1 h-2 bg-gray-100 rounded overflow-hidden">
                                <div className="h-full rounded" style={{ width: `${c.normalized_score * 100}%`, backgroundColor: color }} />
                              </div>
                              <span className="w-8 text-right tabular-nums text-gray-600">{c.normalized_score.toFixed(2)}</span>
                            </div>
                          </td>
                        </tr>
                      );
                    })}
                  </tbody>
                </table>
              </Card>
            )}
          </div>
        </>
      )}
    </div>
  );
};

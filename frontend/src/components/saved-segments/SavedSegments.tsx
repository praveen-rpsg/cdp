/**
 * Saved Segments Repository — dashboard for managing reusable audience segments.
 * List, search, filter, view details, edit (load into builder), clone,
 * refresh count, archive/restore, and delete.
 */

import React, { useEffect, useState, useCallback } from "react";
import { useSegmentStore } from "../../store/segmentStore";

interface SavedSegment {
  id: string;
  name: string;
  description: string | null;
  business_purpose: string | null;
  tags: string[];
  segment_type: string;
  brand_code: string | null;
  rules: any;
  audience_count: number | null;
  status: string;
  created_by: string | null;
  created_at: string | null;
  updated_at: string | null;
  last_computed_at: string | null;
}

const fmtNum = (v: number | null) => (v == null ? "—" : v.toLocaleString("en-IN"));
const fmtDate = (v: string | null) => (v ? new Date(v).toLocaleDateString("en-IN", { day: "2-digit", month: "short", year: "numeric" }) : "—");

export const SavedSegments: React.FC = () => {
  const { loadSavedSegment } = useSegmentStore();
  const [segments, setSegments] = useState<SavedSegment[]>([]);
  const [loading, setLoading] = useState(false);
  const [search, setSearch] = useState("");
  const [typeFilter, setTypeFilter] = useState("all");
  const [statusFilter, setStatusFilter] = useState("active");
  const [sort, setSort] = useState("updated");
  const [detail, setDetail] = useState<SavedSegment | null>(null);
  const [toast, setToast] = useState<string | null>(null);

  const showToast = (m: string) => { setToast(m); setTimeout(() => setToast(null), 2500); };

  const fetchSegments = useCallback(async () => {
    setLoading(true);
    try {
      const qs = new URLSearchParams({
        status: statusFilter, segment_type: typeFilter, sort, page_size: "200",
      });
      if (search.trim()) qs.set("search", search.trim());
      const r = await fetch(`/api/v1/segments/saved/list?${qs}`);
      const d = await r.json();
      setSegments(d.segments || []);
    } catch {
      setSegments([]);
    } finally {
      setLoading(false);
    }
  }, [search, typeFilter, statusFilter, sort]);

  useEffect(() => {
    const t = setTimeout(fetchSegments, 250); // debounce search
    return () => clearTimeout(t);
  }, [fetchSegments]);

  const doEdit = (s: SavedSegment) => {
    loadSavedSegment(s);
    window.dispatchEvent(new CustomEvent("nav:segmentation"));
  };
  const doClone = async (s: SavedSegment) => {
    await fetch(`/api/v1/segments/saved/${s.id}/clone`, { method: "POST" });
    showToast("Segment cloned");
    fetchSegments();
  };
  const doRefresh = async (s: SavedSegment) => {
    await fetch(`/api/v1/segments/saved/${s.id}/refresh`, { method: "POST" });
    showToast("Audience count refreshed");
    fetchSegments();
  };
  const doArchive = async (s: SavedSegment) => {
    await fetch(`/api/v1/segments/saved/${s.id}`, {
      method: "PUT", headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ status: s.status === "active" ? "archived" : "active" }),
    });
    showToast(s.status === "active" ? "Segment archived" : "Segment restored");
    fetchSegments();
  };
  const doDelete = async (s: SavedSegment) => {
    if (!confirm(`Delete "${s.name}" permanently?`)) return;
    await fetch(`/api/v1/segments/saved/${s.id}`, { method: "DELETE" });
    showToast("Segment deleted");
    fetchSegments();
  };

  const selCls = "px-3 py-2 border border-gray-200 rounded-lg text-sm bg-white focus:ring-2 focus:ring-indigo-500 outline-none cursor-pointer";

  return (
    <div className="max-w-7xl w-full mx-auto px-4 sm:px-6 py-4 sm:py-6 space-y-4">
      <div className="flex items-center justify-between">
        <h2 className="text-lg font-bold text-gray-900">Saved Segments</h2>
        <span className="text-sm text-gray-400">{segments.length} segment{segments.length !== 1 ? "s" : ""}</span>
      </div>

      {/* Controls */}
      <div className="bg-white rounded-xl border border-gray-200 shadow-sm p-3 flex flex-wrap gap-2 items-center">
        <input
          value={search}
          onChange={(e) => setSearch(e.target.value)}
          placeholder="Search name, description, tags, creator…"
          className="flex-1 min-w-[200px] px-3 py-2 border border-gray-200 rounded-lg text-sm focus:ring-2 focus:ring-indigo-500 outline-none"
        />
        <select value={typeFilter} onChange={(e) => setTypeFilter(e.target.value)} className={selCls}>
          <option value="all">All Types</option>
          <option value="customer">Customer</option>
          <option value="corporate">Corporate</option>
        </select>
        <select value={statusFilter} onChange={(e) => setStatusFilter(e.target.value)} className={selCls}>
          <option value="active">Active</option>
          <option value="archived">Archived</option>
          <option value="all">All Status</option>
        </select>
        <select value={sort} onChange={(e) => setSort(e.target.value)} className={selCls}>
          <option value="updated">Recently Modified</option>
          <option value="created">Newest</option>
          <option value="name">Name (A–Z)</option>
          <option value="count">Audience Size</option>
        </select>
      </div>

      {/* Table */}
      <div className="bg-white rounded-xl border border-gray-200 shadow-sm overflow-hidden">
        <div className="overflow-x-auto">
          <table className="w-full text-sm">
            <thead>
              <tr className="bg-slate-800 text-white text-xs uppercase tracking-wider">
                <th className="py-2.5 px-3 text-left">Segment</th>
                <th className="py-2.5 px-3 text-left">Type</th>
                <th className="py-2.5 px-3 text-right">Audience</th>
                <th className="py-2.5 px-3 text-left">Tags</th>
                <th className="py-2.5 px-3 text-left">Created By</th>
                <th className="py-2.5 px-3 text-left">Modified</th>
                <th className="py-2.5 px-3 text-left">Status</th>
                <th className="py-2.5 px-3 text-right">Actions</th>
              </tr>
            </thead>
            <tbody>
              {loading && (
                <tr><td colSpan={8} className="py-10 text-center text-gray-400">Loading…</td></tr>
              )}
              {!loading && segments.length === 0 && (
                <tr><td colSpan={8} className="py-10 text-center text-gray-400">No saved segments. Build one in Segmentation and click “Save Segment”.</td></tr>
              )}
              {!loading && segments.map((s) => (
                <tr key={s.id} className="border-b border-gray-100 hover:bg-gray-50">
                  <td className="py-2.5 px-3">
                    <button onClick={() => setDetail(s)} className="text-left">
                      <div className="font-semibold text-gray-800 hover:text-indigo-600">{s.name}</div>
                      {s.description && <div className="text-xs text-gray-400 truncate max-w-[260px]">{s.description}</div>}
                    </button>
                  </td>
                  <td className="py-2.5 px-3">
                    <span className={`px-2 py-0.5 rounded-full text-[10px] font-semibold ${s.segment_type === "corporate" ? "bg-purple-100 text-purple-700" : "bg-blue-100 text-blue-700"}`}>
                      {s.segment_type === "corporate" ? "Corporate" : "Customer"}
                    </span>
                  </td>
                  <td className="py-2.5 px-3 text-right tabular-nums font-medium">{fmtNum(s.audience_count)}</td>
                  <td className="py-2.5 px-3">
                    <div className="flex flex-wrap gap-1 max-w-[180px]">
                      {s.tags.slice(0, 3).map((t) => <span key={t} className="px-1.5 py-0.5 bg-gray-100 text-gray-600 rounded text-[10px]">{t}</span>)}
                      {s.tags.length > 3 && <span className="text-[10px] text-gray-400">+{s.tags.length - 3}</span>}
                    </div>
                  </td>
                  <td className="py-2.5 px-3 text-gray-600 text-xs">{s.created_by || "—"}</td>
                  <td className="py-2.5 px-3 text-gray-500 text-xs whitespace-nowrap">{fmtDate(s.updated_at)}</td>
                  <td className="py-2.5 px-3">
                    <span className={`px-2 py-0.5 rounded-full text-[10px] font-semibold ${s.status === "active" ? "bg-emerald-100 text-emerald-700" : "bg-gray-200 text-gray-500"}`}>
                      {s.status === "active" ? "Active" : "Archived"}
                    </span>
                  </td>
                  <td className="py-2.5 px-3">
                    <div className="flex items-center justify-end gap-1 text-xs">
                      <button onClick={() => setDetail(s)} className="px-2 py-1 text-gray-600 hover:bg-gray-100 rounded" title="View">View</button>
                      <button onClick={() => doEdit(s)} className="px-2 py-1 text-indigo-600 hover:bg-indigo-50 rounded" title="Edit in builder">Edit</button>
                      <button onClick={() => doClone(s)} className="px-2 py-1 text-gray-600 hover:bg-gray-100 rounded" title="Clone">Clone</button>
                      <button onClick={() => doRefresh(s)} className="px-2 py-1 text-gray-600 hover:bg-gray-100 rounded" title="Refresh count">↻</button>
                      <button onClick={() => doArchive(s)} className="px-2 py-1 text-amber-600 hover:bg-amber-50 rounded" title={s.status === "active" ? "Archive" : "Restore"}>{s.status === "active" ? "Archive" : "Restore"}</button>
                      <button onClick={() => doDelete(s)} className="px-2 py-1 text-red-500 hover:bg-red-50 rounded" title="Delete">✕</button>
                    </div>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>

      {/* Detail modal — centered, occupies most of the screen */}
      {detail && (
        <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/40 p-4 sm:p-8" onClick={() => setDetail(null)}>
          <div className="bg-white w-full max-w-5xl rounded-2xl shadow-2xl flex flex-col overflow-hidden" onClick={(e) => e.stopPropagation()}>
            {/* Header */}
            <div className="flex items-start justify-between gap-4 px-6 py-4 border-b border-gray-100">
              <div className="min-w-0">
                <div className="flex items-center gap-2 flex-wrap">
                  <h3 className="text-xl font-bold text-gray-900">{detail.name}</h3>
                  <span className={`px-2 py-0.5 rounded-full text-[10px] font-semibold ${detail.segment_type === "corporate" ? "bg-purple-100 text-purple-700" : "bg-blue-100 text-blue-700"}`}>
                    {detail.segment_type === "corporate" ? "Corporate" : "Customer"}
                  </span>
                  <span className={`px-2 py-0.5 rounded-full text-[10px] font-semibold ${detail.status === "active" ? "bg-emerald-100 text-emerald-700" : "bg-gray-200 text-gray-500"}`}>
                    {detail.status === "active" ? "Active" : "Archived"}
                  </span>
                </div>
                {detail.description && <p className="text-sm text-gray-500 mt-1">{detail.description}</p>}
              </div>
              <button onClick={() => setDetail(null)} className="text-gray-400 hover:text-gray-700 text-xl leading-none shrink-0">✕</button>
            </div>

            {/* Body — two columns (no body scroll; only the JSON panel scrolls) */}
            <div className="px-6 py-5 grid grid-cols-1 lg:grid-cols-2 gap-6 items-start">
              {/* Left: metadata */}
              <div className="space-y-5">
                <div className="bg-indigo-50 rounded-xl p-4 flex items-center justify-between">
                  <span className="text-sm font-medium text-indigo-700">Audience Size</span>
                  <span className="text-2xl font-black text-indigo-700 tabular-nums">{fmtNum(detail.audience_count)}</span>
                </div>
                <div className="grid grid-cols-2 gap-4 text-sm">
                  <Info label="Type" value={detail.segment_type} />
                  <Info label="Brand" value={detail.brand_code || "—"} />
                  <Info label="Created By" value={detail.created_by || "—"} />
                  <Info label="Status" value={detail.status} />
                  <Info label="Created" value={fmtDate(detail.created_at)} />
                  <Info label="Modified" value={fmtDate(detail.updated_at)} />
                  <Info label="Last Count Refresh" value={fmtDate(detail.last_computed_at)} />
                </div>
                {detail.business_purpose && (
                  <div>
                    <div className="text-[10px] text-gray-400 uppercase tracking-wider mb-1">Business Purpose</div>
                    <div className="text-sm text-gray-700">{detail.business_purpose}</div>
                  </div>
                )}
                {detail.tags.length > 0 && (
                  <div>
                    <div className="text-[10px] text-gray-400 uppercase tracking-wider mb-1">Tags</div>
                    <div className="flex flex-wrap gap-1.5">{detail.tags.map((t) => <span key={t} className="px-2 py-0.5 bg-gray-100 text-gray-600 rounded text-xs">{t}</span>)}</div>
                  </div>
                )}
              </div>

              {/* Right: segment logic */}
              <div>
                <div className="text-[10px] text-gray-400 uppercase tracking-wider mb-1">Segment Logic (JSON)</div>
                <pre className="p-4 bg-gray-900 text-green-400 rounded-xl text-[11px] overflow-auto leading-relaxed max-h-[50vh]">{JSON.stringify(detail.rules, null, 2)}</pre>
              </div>
            </div>

            {/* Footer actions */}
            <div className="flex justify-end gap-2 px-6 py-4 border-t border-gray-100 bg-gray-50">
              <button onClick={() => doClone(detail)} className="px-4 py-2 bg-white border border-gray-200 text-gray-700 rounded-lg text-sm hover:bg-gray-100">Clone</button>
              <button onClick={() => { doRefresh(detail); }} className="px-4 py-2 bg-white border border-gray-200 text-gray-700 rounded-lg text-sm hover:bg-gray-100">Refresh Count</button>
              <button onClick={() => { doEdit(detail); setDetail(null); }} className="px-5 py-2 bg-indigo-600 text-white rounded-lg text-sm font-medium hover:bg-indigo-700">Edit in Builder</button>
            </div>
          </div>
        </div>
      )}

      {toast && (
        <div className="fixed bottom-6 right-6 z-50 px-4 py-3 bg-emerald-600 text-white text-sm rounded-xl shadow-lg">{toast}</div>
      )}
    </div>
  );
};

const Info: React.FC<{ label: string; value: React.ReactNode }> = ({ label, value }) => (
  <div>
    <div className="text-[10px] text-gray-400 uppercase tracking-wider">{label}</div>
    <div className="font-medium text-gray-800 capitalize">{value}</div>
  </div>
);

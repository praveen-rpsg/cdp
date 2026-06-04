/**
 * Rank & Split Studio — prioritise a segment by weighted ranking, divide it
 * into percentage groups (optionally constrained by count/budget), preview
 * per-group metrics, and save each group as a child segment.
 */

import React, { useState } from "react";
import { useSegmentStore } from "../../store/segmentStore";
import { RANKABLE_ATTRIBUTES } from "../../types/segment";

interface RankRow { attribute: string; weight: number; order: "desc" | "asc" }
interface SplitRow { name: string; percent: number }
interface GroupResult { name: string; count: number; revenue: number; avg_spend: number; percent: number | null }

const fmtINR = (v: number) => "₹" + Math.round(v).toLocaleString("en-IN");
const fmtNum = (v: number) => Math.round(v).toLocaleString("en-IN");

export const RankSplitStudio: React.FC<{ onClose: () => void; onSaved: (msg: string) => void }> = ({ onClose, onSaved }) => {
  const { selectedBrandCode, segmentName, getSegmentDefinition } = useSegmentStore();

  const [ranks, setRanks] = useState<RankRow[]>([{ attribute: "txn.total_spend", weight: 1, order: "desc" }]);
  const [splits, setSplits] = useState<SplitRow[]>([
    { name: "Top 50%", percent: 50 },
    { name: "Next 30%", percent: 30 },
    { name: "Bottom 20%", percent: 20 },
  ]);
  const [useBudget, setUseBudget] = useState(false);
  const [maxCount, setMaxCount] = useState<string>("");
  const [budget, setBudget] = useState<string>("");
  const [cpc, setCpc] = useState<string>("5");
  const [preview, setPreview] = useState<GroupResult[] | null>(null);
  const [baseName, setBaseName] = useState(segmentName || "");
  const [creator, setCreator] = useState(localStorage.getItem("u360_creator") || "");
  const [busy, setBusy] = useState<"" | "preview" | "save">("");

  const corporate = selectedBrandCode === "corporate";
  const totalPct = splits.reduce((s, x) => s + (x.percent || 0), 0);

  const constraints = () => {
    const c: any = {};
    if (useBudget) {
      if (budget) c.budget = parseFloat(budget);
      if (cpc) c.cost_per_contact = parseFloat(cpc);
    } else if (maxCount) {
      c.max_count = parseInt(maxCount);
    }
    return Object.keys(c).length ? c : null;
  };

  const payload = () => ({
    brand_code: selectedBrandCode,
    rules: getSegmentDefinition(),
    rank: ranks.filter((r) => r.attribute),
    splits: splits.map((s) => ({ name: s.name, percent: s.percent })),
    constraints: constraints(),
  });

  const doPreview = async () => {
    setBusy("preview");
    try {
      const r = await fetch("/api/v1/segments/rank-split/preview", {
        method: "POST", headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload()),
      });
      const d = await r.json();
      setPreview(d.groups || []);
    } catch { setPreview([]); }
    finally { setBusy(""); }
  };

  const doSave = async () => {
    if (!baseName.trim()) return;
    setBusy("save");
    try {
      const r = await fetch("/api/v1/segments/rank-split/save", {
        method: "POST", headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ ...payload(), base_name: baseName.trim(), created_by: creator.trim() || null }),
      });
      const d = await r.json();
      if (creator) localStorage.setItem("u360_creator", creator);
      onSaved(`Saved ${d.count} group${d.count !== 1 ? "s" : ""} to Saved Segments`);
      onClose();
    } catch { onSaved("Failed to save split groups"); }
    finally { setBusy(""); }
  };

  const fld = "px-2 py-1.5 border border-gray-200 rounded-lg text-sm focus:ring-2 focus:ring-indigo-500 outline-none";
  const maxCount2 = Math.max(1, ...(preview?.map((g) => g.count) || [1]));

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/40 p-4" onClick={onClose}>
      <div className="bg-white w-full max-w-4xl rounded-2xl shadow-2xl flex flex-col max-h-[92vh]" onClick={(e) => e.stopPropagation()}>
        <div className="flex items-center justify-between px-6 py-4 border-b border-gray-100">
          <h3 className="text-lg font-bold text-gray-900">Rank &amp; Split Studio</h3>
          <button onClick={onClose} className="text-gray-400 hover:text-gray-700 text-xl">✕</button>
        </div>

        {corporate ? (
          <div className="p-8 text-center text-amber-700 bg-amber-50 m-6 rounded-xl text-sm">
            Rank &amp; Split currently supports Spencer&apos;s and Nature&apos;s Basket segments.
            Corporate cross-brand splitting is planned for a later phase.
          </div>
        ) : (
          <div className="overflow-y-auto px-6 py-5 space-y-6">
            {/* Ranking */}
            <section>
              <div className="flex items-center justify-between mb-2">
                <h4 className="text-sm font-semibold text-gray-700">1 · Ranking (weighted)</h4>
                <button onClick={() => setRanks([...ranks, { attribute: "", weight: 1, order: "desc" }])}
                  className="text-xs text-indigo-600 hover:underline">+ Add attribute</button>
              </div>
              <div className="space-y-2">
                {ranks.map((r, i) => (
                  <div key={i} className="flex items-center gap-2">
                    <select value={r.attribute} onChange={(e) => { const n = [...ranks]; n[i].attribute = e.target.value; setRanks(n); }} className={`${fld} flex-1`}>
                      <option value="">Select attribute…</option>
                      {RANKABLE_ATTRIBUTES.map((a) => <option key={a.key} value={a.key}>{a.label}</option>)}
                    </select>
                    <input type="number" value={r.weight} min={0} step={0.1} onChange={(e) => { const n = [...ranks]; n[i].weight = parseFloat(e.target.value) || 0; setRanks(n); }} className={`${fld} w-20`} title="Weight" />
                    <select value={r.order} onChange={(e) => { const n = [...ranks]; n[i].order = e.target.value as any; setRanks(n); }} className={`${fld} w-28`}>
                      <option value="desc">High → Low</option>
                      <option value="asc">Low → High</option>
                    </select>
                    {ranks.length > 1 && <button onClick={() => setRanks(ranks.filter((_, j) => j !== i))} className="text-gray-400 hover:text-red-500 px-1">✕</button>}
                  </div>
                ))}
              </div>
              <p className="text-[11px] text-gray-400 mt-1">Multiple attributes are combined into a weighted priority score (weights are normalised).</p>
            </section>

            {/* Splits */}
            <section>
              <div className="flex items-center justify-between mb-2">
                <h4 className="text-sm font-semibold text-gray-700">2 · Percentage Split</h4>
                <button onClick={() => setSplits([...splits, { name: `Group ${splits.length + 1}`, percent: 0 }])}
                  className="text-xs text-indigo-600 hover:underline">+ Add group</button>
              </div>
              <div className="space-y-2">
                {splits.map((s, i) => (
                  <div key={i} className="flex items-center gap-2">
                    <input value={s.name} onChange={(e) => { const n = [...splits]; n[i].name = e.target.value; setSplits(n); }} placeholder="Group name" className={`${fld} flex-1`} />
                    <input type="number" value={s.percent} min={0} max={100} onChange={(e) => { const n = [...splits]; n[i].percent = parseInt(e.target.value) || 0; setSplits(n); }} className={`${fld} w-20 text-right`} />
                    <span className="text-sm text-gray-500 w-4">%</span>
                    {splits.length > 1 && <button onClick={() => setSplits(splits.filter((_, j) => j !== i))} className="text-gray-400 hover:text-red-500 px-1">✕</button>}
                  </div>
                ))}
              </div>
              <p className={`text-[11px] mt-1 ${totalPct === 100 ? "text-gray-400" : "text-amber-600"}`}>Total: {totalPct}% {totalPct !== 100 && "(last group absorbs any remainder)"}</p>
            </section>

            {/* Constraints */}
            <section>
              <h4 className="text-sm font-semibold text-gray-700 mb-2">3 · Constraints (optional)</h4>
              <div className="flex gap-4 text-sm mb-2">
                <label className="flex items-center gap-1.5"><input type="radio" checked={!useBudget} onChange={() => setUseBudget(false)} /> Max audience</label>
                <label className="flex items-center gap-1.5"><input type="radio" checked={useBudget} onChange={() => setUseBudget(true)} /> Budget</label>
              </div>
              {!useBudget ? (
                <input type="number" value={maxCount} onChange={(e) => setMaxCount(e.target.value)} placeholder="Max customers (e.g. 50000)" className={`${fld} w-64`} />
              ) : (
                <div className="flex items-center gap-2 flex-wrap">
                  <input type="number" value={budget} onChange={(e) => setBudget(e.target.value)} placeholder="Budget ₹" className={`${fld} w-40`} />
                  <span className="text-sm text-gray-400">÷</span>
                  <input type="number" value={cpc} onChange={(e) => setCpc(e.target.value)} placeholder="Cost/contact" className={`${fld} w-32`} />
                  <span className="text-xs text-gray-500">= {budget && cpc && parseFloat(cpc) > 0 ? fmtNum(Math.floor(parseFloat(budget) / parseFloat(cpc))) : "—"} customers (top by rank)</span>
                </div>
              )}
            </section>

            {/* Preview */}
            {preview && (
              <section>
                <h4 className="text-sm font-semibold text-gray-700 mb-2">Preview</h4>
                <table className="w-full text-sm">
                  <thead><tr className="text-left text-xs text-gray-400 uppercase border-b border-gray-100">
                    <th className="py-1.5">Group</th><th className="py-1.5 text-right">Customers</th>
                    <th className="py-1.5 text-right">Revenue</th><th className="py-1.5 text-right">Avg Spend</th>
                    <th className="py-1.5 pl-3 w-1/3">Size</th>
                  </tr></thead>
                  <tbody>
                    {preview.map((g, i) => (
                      <tr key={i} className="border-b border-gray-50">
                        <td className="py-1.5 font-medium">{g.name}</td>
                        <td className="py-1.5 text-right tabular-nums">{fmtNum(g.count)}</td>
                        <td className="py-1.5 text-right tabular-nums">{fmtINR(g.revenue)}</td>
                        <td className="py-1.5 text-right tabular-nums">{fmtINR(g.avg_spend)}</td>
                        <td className="py-1.5 pl-3"><div className="h-3 bg-indigo-500 rounded" style={{ width: `${(g.count / maxCount2) * 100}%` }} /></td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </section>
            )}

            {/* Save meta */}
            <section className="grid grid-cols-2 gap-3">
              <div>
                <label className="block text-xs font-semibold text-gray-600 uppercase tracking-wider mb-1">Base Name</label>
                <input value={baseName} onChange={(e) => setBaseName(e.target.value)} placeholder="e.g. Active Customers" className={`${fld} w-full`} />
              </div>
              <div>
                <label className="block text-xs font-semibold text-gray-600 uppercase tracking-wider mb-1">Your Name</label>
                <input value={creator} onChange={(e) => setCreator(e.target.value)} placeholder="Creator" className={`${fld} w-full`} />
              </div>
            </section>
          </div>
        )}

        {!corporate && (
          <div className="flex justify-end gap-2 px-6 py-4 border-t border-gray-100 bg-gray-50">
            <button onClick={doPreview} disabled={busy !== "" || !selectedBrandCode} className="px-4 py-2 bg-white border border-gray-200 rounded-lg text-sm hover:bg-gray-100 disabled:opacity-40">
              {busy === "preview" ? "Previewing…" : "Preview"}
            </button>
            <button onClick={doSave} disabled={busy !== "" || !baseName.trim() || !preview} className="px-5 py-2 bg-indigo-600 text-white rounded-lg text-sm font-medium hover:bg-indigo-700 disabled:opacity-40">
              {busy === "save" ? "Saving…" : "Save Groups as Segments"}
            </button>
          </div>
        )}
      </div>
    </div>
  );
};

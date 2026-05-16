import React, { useState, useCallback, useEffect, useRef } from "react";
import { useSegmentStore } from "../../store/segmentStore";
import {
  CATEGORY_CONFIG,
  RANKABLE_ATTRIBUTES,
  SPLITTABLE_ATTRIBUTES,
  SET_OPERATION_LABELS,
} from "../../types/segment";
import type { SetOperationType } from "../../types/segment";
import { ConditionGroupUI } from "./ConditionGroupUI";
import AudienceSummaryPanel from "./AudienceSummaryPanel";
import { NLSegmentPanel } from "./NLSegmentPanel";

/* ── Toast ─────────────────────────────────────────────────── */
interface ToastState { message: string; type: "success" | "error" }

const Toast: React.FC<{ toast: ToastState | null }> = ({ toast }) => {
  if (!toast) return null;
  return (
    <div
      className={`fixed bottom-6 right-6 z-50 flex items-center gap-3 px-4 py-3 rounded-xl shadow-xl text-white text-sm font-medium max-w-sm toast-enter pointer-events-none ${
        toast.type === "success" ? "bg-emerald-600" : "bg-red-600"
      }`}
    >
      {toast.type === "success" ? (
        <svg className="w-4 h-4 flex-shrink-0" fill="none" viewBox="0 0 24 24" stroke="currentColor">
          <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2.5} d="M5 13l4 4L19 7" />
        </svg>
      ) : (
        <svg className="w-4 h-4 flex-shrink-0" fill="none" viewBox="0 0 24 24" stroke="currentColor">
          <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2.5} d="M6 18L18 6M6 6l12 12" />
        </svg>
      )}
      {toast.message}
    </div>
  );
};

/* ── ChevronIcon ─────────────────────────────────────────────── */
const ChevronIcon: React.FC<{ open: boolean }> = ({ open }) => (
  <svg
    className={`w-4 h-4 transition-transform duration-200 ${open ? "rotate-180" : ""}`}
    fill="none" viewBox="0 0 24 24" stroke="currentColor"
  >
    <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M19 9l-7 7-7-7" />
  </svg>
);

/* ── Collapsible panel wrapper ──────────────────────────────── */
const CollapsibleContent: React.FC<{ open: boolean; children: React.ReactNode }> = ({ open, children }) => (
  <div
    className="overflow-hidden transition-all duration-300 ease-in-out"
    style={{ maxHeight: open ? "1200px" : "0", opacity: open ? 1 : 0 }}
  >
    {children}
  </div>
);

/* ══════════════════════════════════════════════════════════════
   SegmentBuilder
══════════════════════════════════════════════════════════════ */
export const SegmentBuilder: React.FC = () => {
  const {
    segmentName,
    segmentDescription,
    segmentType,
    rules,
    selectedBrandCode,
    brands,
    audienceCount,
    isEstimating,
    compiledSQL,
    isDirty,
    rankConfig,
    splitConfig,
    splitCounts,
    setOperation,
    setOperationCounts,
    setSegmentName,
    setSegmentDescription,
    setSegmentType,
    setSelectedBrand,
    setIsEstimating,
    setAudienceCount,
    setCompiledSQL,
    setRankConfig,
    setSplitConfig,
    setSplitCounts,
    addSplitEntry,
    removeSplitEntry,
    updateSplitEntry,
    setSetOperation,
    setSetOperationCounts,
    addCondition,
    quickAddCondition,
    updateCondition,
    resetRules,
    loadRules,
    getSegmentDefinition,
    estimateAudience,
    fetchSummary,
  } = useSegmentStore();

  const [builderMode, setBuilderMode] = useState<"visual" | "nl">("visual");
  const [showSQL, setShowSQL] = useState(false);
  const [copiedSQL, setCopiedSQL] = useState(false);
  const [activeTab, setActiveTab] = useState<"builder" | "json">("builder");
  const [templates, setTemplates] = useState<any[]>([]);
  const [templateFilter, setTemplateFilter] = useState<string>("all");
  const [showRankSplit, setShowRankSplit] = useState(false);
  const [showSetOps, setShowSetOps] = useState(false);
  const [toast, setToast] = useState<ToastState | null>(null);
  const toastTimer = useRef<ReturnType<typeof setTimeout> | null>(null);

  const showToast = useCallback((message: string, type: "success" | "error" = "success") => {
    if (toastTimer.current) clearTimeout(toastTimer.current);
    setToast({ message, type });
    toastTimer.current = setTimeout(() => setToast(null), 3500);
  }, []);

  useEffect(() => () => { if (toastTimer.current) clearTimeout(toastTimer.current); }, []);

  useEffect(() => {
    const url = selectedBrandCode
      ? `/api/v1/segments/templates/list?brand_code=${selectedBrandCode}`
      : "/api/v1/segments/templates/list";
    fetch(url)
      .then((r) => r.json())
      .then((data) => setTemplates(data.templates || []))
      .catch(() => setTemplates([]));
  }, [selectedBrandCode]);

  const handleLoadTemplate = (template: any) => {
    if (template.rules?.root) {
      const addIds = (node: any): any => {
        const id = `cond_${Date.now()}_${Math.random().toString(36).slice(2, 9)}`;
        if (node.type === "group" || node.conditions) {
          return { ...node, type: "group", id, conditions: (node.conditions || []).map(addIds) };
        }
        return { ...node, id };
      };
      loadRules(addIds(template.rules.root));
      setSegmentName(template.name);
      setSegmentDescription(template.description);
    }
  };

  const handleEstimate = async () => { await estimateAudience(); };

  const handleSave = async () => {
    try {
      const response = await fetch("/api/v1/segments/", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          brand_id: selectedBrandCode,
          name: segmentName,
          description: segmentDescription,
          segment_type: segmentType,
          rules: getSegmentDefinition(),
          is_cross_brand: false,
        }),
      });
      const data = await response.json();
      showToast(`Segment saved — ID: ${data.id}`);
    } catch {
      showToast("Failed to save segment. Please try again.", "error");
    }
  };

  const inputCls = "w-full px-3 py-2 border border-gray-200 rounded-lg text-sm focus:ring-2 focus:ring-indigo-500 focus:border-indigo-500 outline-none transition-colors duration-150 bg-white hover:border-gray-300";
  const selectCls = `${inputCls} cursor-pointer`;

  return (
    <div className="min-h-screen bg-gray-50 flex flex-col">

      {/* ── Top Bar ── */}
      <div className="bg-white border-b border-gray-200 sticky top-0 z-40 shadow-sm">
        <div className="max-w-7xl mx-auto px-4 sm:px-6">

          {/* Primary row */}
          <div className="flex items-center gap-3 sm:gap-5 h-14">

            {/* RPSG Branding */}
            <div className="flex items-center gap-2 sm:gap-3 shrink-0">
              <img
                src="/rpsg-logo.png"
                alt="RP-Sanjiv Goenka Group"
                className="h-8 sm:h-10 w-auto object-contain"
              />
              <span
                style={{
                  background: "linear-gradient(90deg, #7B2D8B 0%, #C73280 35%, #E0402E 65%, #F5A010 100%)",
                  WebkitBackgroundClip: "text",
                  WebkitTextFillColor: "transparent",
                  backgroundClip: "text",
                  fontWeight: 900,
                  fontSize: "clamp(0.95rem, 2.5vw, 1.2rem)",
                  letterSpacing: "0.1em",
                  lineHeight: 1,
                }}
              >
                UNIFY 360
              </span>
            </div>

            {/* Divider + label — hidden on mobile */}
            <div className="hidden sm:flex items-center gap-3 shrink-0">
              <div className="w-px h-6 bg-gray-200" />
              <h1 className="text-xs font-semibold text-gray-400 uppercase tracking-wider hidden md:block">
                Segment Builder
              </h1>
            </div>

            {/* Mode toggle — desktop */}
            <div className="hidden sm:flex bg-gray-100 rounded-lg p-0.5 shrink-0">
              {(["visual", "nl"] as const).map((mode) => (
                <button
                  key={mode}
                  onClick={() => setBuilderMode(mode)}
                  className={`px-3 py-1.5 text-xs font-medium rounded-md transition-all duration-200 ${
                    builderMode === mode
                      ? "bg-white text-indigo-700 shadow-sm"
                      : "text-gray-500 hover:text-gray-700"
                  }`}
                >
                  {mode === "visual" ? "Visual Builder" : "Natural Language"}
                </button>
              ))}
            </div>

            {isDirty && (
              <span className="hidden sm:inline-flex px-2 py-0.5 text-xs bg-amber-100 text-amber-700 rounded-full shrink-0">
                Unsaved
              </span>
            )}

            <div className="flex-1" />

            {/* Action buttons */}
            <div className="flex items-center gap-1.5 sm:gap-2">
              <button
                onClick={resetRules}
                className="hidden sm:block px-3 py-1.5 text-sm text-gray-500 hover:text-gray-800 hover:bg-gray-100 rounded-lg transition-colors duration-150 active:scale-95"
              >
                Reset
              </button>
              <button
                onClick={handleEstimate}
                disabled={isEstimating || !selectedBrandCode}
                className="hidden md:flex items-center gap-1.5 px-3 py-1.5 text-sm bg-gray-100 hover:bg-gray-200 rounded-lg disabled:opacity-40 transition-colors duration-150 active:scale-95"
              >
                {isEstimating ? (
                  <>
                    <div className="w-3 h-3 border-2 border-gray-500 border-t-transparent rounded-full animate-spin" />
                    Estimating…
                  </>
                ) : "Estimate Audience"}
              </button>
              <button
                onClick={handleSave}
                disabled={!segmentName || !selectedBrandCode}
                className="px-3 sm:px-4 py-1.5 sm:py-2 text-sm bg-indigo-600 text-white hover:bg-indigo-700 rounded-lg disabled:opacity-40 transition-colors duration-150 active:scale-95 font-medium shadow-sm"
              >
                <span className="hidden sm:inline">Save Segment</span>
                <span className="sm:hidden">Save</span>
              </button>
            </div>
          </div>

          {/* Mobile secondary row */}
          <div className="flex sm:hidden items-center gap-2 py-2 border-t border-gray-100">
            <div className="flex bg-gray-100 rounded-lg p-0.5 flex-1">
              {(["visual", "nl"] as const).map((mode) => (
                <button
                  key={mode}
                  onClick={() => setBuilderMode(mode)}
                  className={`flex-1 py-1.5 text-xs font-medium rounded-md transition-all duration-200 ${
                    builderMode === mode ? "bg-white text-indigo-700 shadow-sm" : "text-gray-500"
                  }`}
                >
                  {mode === "visual" ? "Visual" : "AI"}
                </button>
              ))}
            </div>
            {isDirty && <span className="px-2 py-0.5 text-xs bg-amber-100 text-amber-700 rounded-full">Unsaved</span>}
            <button onClick={handleEstimate} disabled={isEstimating || !selectedBrandCode} className="px-2.5 py-1.5 text-xs bg-gray-100 hover:bg-gray-200 rounded-lg disabled:opacity-40 transition-colors duration-150">
              {isEstimating ? "…" : "Estimate"}
            </button>
            <button onClick={resetRules} className="px-2.5 py-1.5 text-xs text-gray-500 hover:bg-gray-100 rounded-lg transition-colors duration-150">
              Reset
            </button>
          </div>
        </div>
      </div>

      {/* ── NL Mode ── */}
      {builderMode === "nl" && (
        <div className="flex-1 flex flex-col min-h-0 max-w-5xl w-full mx-auto px-4 sm:px-6 py-4 sm:py-6">
          <div className="bg-white rounded-xl border border-gray-200 flex-1 flex flex-col overflow-hidden shadow-sm">
            <NLSegmentPanel />
          </div>
        </div>
      )}

      {/* ── Visual Builder ── */}
      {builderMode === "visual" && (
        <div className="max-w-7xl w-full mx-auto px-4 sm:px-6 py-4 sm:py-6 flex flex-col lg:flex-row gap-4 lg:gap-6 items-start">

          {/* Main builder */}
          <div className="flex-1 min-w-0 space-y-4 sm:space-y-5">

            {/* Segment metadata */}
            <div className="bg-white rounded-xl border border-gray-200 p-4 sm:p-6 space-y-4 shadow-sm">
              <div className="grid grid-cols-1 sm:grid-cols-2 gap-4">
                <div>
                  <label className="block text-xs font-semibold text-gray-600 uppercase tracking-wider mb-1.5">Segment Name</label>
                  <input type="text" value={segmentName} onChange={(e) => setSegmentName(e.target.value)} placeholder="e.g., High-Value At-Risk Customers" className={inputCls} />
                </div>
                <div className="flex gap-3">
                  <div className="flex-1">
                    <label className="block text-xs font-semibold text-gray-600 uppercase tracking-wider mb-1.5">Brand</label>
                    <select value={selectedBrandCode || ""} onChange={(e) => setSelectedBrand(e.target.value)} className={selectCls}>
                      <option value="">Select brand…</option>
                      {brands.map((b) => <option key={b.code} value={b.code}>{b.name}</option>)}
                    </select>
                  </div>
                  <div className="flex-1">
                    <label className="block text-xs font-semibold text-gray-600 uppercase tracking-wider mb-1.5">Type</label>
                    <select value={segmentType} onChange={(e) => setSegmentType(e.target.value)} className={selectCls}>
                      <option value="dynamic">Dynamic</option>
                      <option value="static">Static</option>
                      <option value="predictive">Predictive</option>
                      <option value="lookalike">Lookalike</option>
                      <option value="lifecycle">Lifecycle</option>
                    </select>
                  </div>
                </div>
              </div>
              <div>
                <label className="block text-xs font-semibold text-gray-600 uppercase tracking-wider mb-1.5">Description</label>
                <textarea value={segmentDescription} onChange={(e) => setSegmentDescription(e.target.value)} placeholder="Describe this segment's purpose…" rows={2} className={`${inputCls} resize-none`} />
              </div>
            </div>

            {/* Builder / JSON tabs */}
            <div className="bg-white rounded-xl border border-gray-200 shadow-sm overflow-hidden">
              <div className="flex border-b border-gray-100">
                {(["builder", "json"] as const).map((tab) => (
                  <button key={tab} onClick={() => setActiveTab(tab)}
                    className={`px-4 sm:px-5 py-3 text-sm font-medium border-b-2 transition-colors duration-150 ${
                      activeTab === tab ? "border-indigo-600 text-indigo-600 bg-indigo-50/50" : "border-transparent text-gray-500 hover:text-gray-700 hover:bg-gray-50"
                    }`}
                  >
                    {tab === "builder" ? "Visual Builder" : "JSON / Power User"}
                  </button>
                ))}
              </div>
              <div className="p-4">
                {activeTab === "builder" ? (
                  <ConditionGroupUI group={rules} isRoot />
                ) : (
                  <pre className="p-4 bg-gray-900 text-green-400 rounded-lg overflow-x-auto text-xs font-mono max-h-96 leading-relaxed">
                    {JSON.stringify(getSegmentDefinition(), null, 2)}
                  </pre>
                )}
              </div>
            </div>

            {/* Rank & Split */}
            <div className="bg-white rounded-xl border border-gray-200 shadow-sm overflow-hidden">
              <button onClick={() => setShowRankSplit(!showRankSplit)} className="w-full px-4 py-3.5 flex items-center justify-between text-sm font-medium text-gray-700 hover:bg-gray-50/80 transition-colors duration-150">
                <div className="flex items-center gap-2">
                  <svg className="w-4 h-4 text-indigo-500 flex-shrink-0" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                    <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M3 4h13M3 8h9m-9 4h6m4 0l4-4m0 0l4 4m-4-4v12" />
                  </svg>
                  <span>Rank &amp; Split</span>
                  {(rankConfig.enabled || splitConfig.enabled) && (
                    <span className="px-2 py-0.5 text-[10px] bg-indigo-100 text-indigo-700 rounded-full font-semibold">Active</span>
                  )}
                </div>
                <ChevronIcon open={showRankSplit} />
              </button>
              <CollapsibleContent open={showRankSplit}>
                <div className="px-4 pb-5 space-y-5 border-t border-gray-100">
                  <div className="pt-4">
                    <label className="flex items-center gap-2.5 cursor-pointer mb-3">
                      <input type="checkbox" checked={rankConfig.enabled} onChange={(e) => setRankConfig({ enabled: e.target.checked })} className="w-4 h-4 text-indigo-600 rounded border-gray-300 focus:ring-indigo-500" />
                      <span className="text-sm font-medium text-gray-700">Rank by Attribute</span>
                    </label>
                    {rankConfig.enabled && (
                      <div className="ml-6 space-y-3 p-3 bg-indigo-50/70 rounded-xl border border-indigo-100">
                        <div className="grid grid-cols-1 sm:grid-cols-3 gap-3">
                          <div>
                            <label className="block text-xs font-semibold text-gray-500 uppercase tracking-wider mb-1.5">Rank Attribute</label>
                            <select value={rankConfig.attribute || ""} onChange={(e) => setRankConfig({ attribute: e.target.value || null })} className="w-full px-2 py-2 border border-gray-200 rounded-lg text-xs focus:ring-2 focus:ring-indigo-500 outline-none bg-white">
                              <option value="">Select…</option>
                              {RANKABLE_ATTRIBUTES.map((attr) => <option key={attr.key} value={attr.key}>{attr.label}</option>)}
                            </select>
                          </div>
                          <div>
                            <label className="block text-xs font-semibold text-gray-500 uppercase tracking-wider mb-1.5">Order</label>
                            <select value={rankConfig.order} onChange={(e) => setRankConfig({ order: e.target.value as "asc" | "desc" })} className="w-full px-2 py-2 border border-gray-200 rounded-lg text-xs focus:ring-2 focus:ring-indigo-500 outline-none bg-white">
                              <option value="desc">Highest First</option>
                              <option value="asc">Lowest First</option>
                            </select>
                          </div>
                          <div>
                            <label className="block text-xs font-semibold text-gray-500 uppercase tracking-wider mb-1.5">Limit (Top N)</label>
                            <input type="number" value={rankConfig.profile_limit ?? ""} onChange={(e) => setRankConfig({ profile_limit: e.target.value ? parseInt(e.target.value) : null })} placeholder="No limit" className="w-full px-2 py-2 border border-gray-200 rounded-lg text-xs focus:ring-2 focus:ring-indigo-500 outline-none bg-white" />
                          </div>
                        </div>
                        {rankConfig.attribute && (
                          <p className="text-xs text-indigo-600">
                            Ranking by <strong>{RANKABLE_ATTRIBUTES.find(a => a.key === rankConfig.attribute)?.label || rankConfig.attribute}</strong>
                            {" "}({rankConfig.order === "desc" ? "highest first" : "lowest first"})
                            {rankConfig.profile_limit ? `, top ${rankConfig.profile_limit.toLocaleString()}` : ""}
                          </p>
                        )}
                      </div>
                    )}
                  </div>
                  <div className="border-t border-gray-100 pt-4">
                    <label className="flex items-center gap-2.5 cursor-pointer mb-3">
                      <input type="checkbox" checked={splitConfig.enabled} onChange={(e) => setSplitConfig({ enabled: e.target.checked })} className="w-4 h-4 text-green-600 rounded border-gray-300 focus:ring-green-500" />
                      <span className="text-sm font-medium text-gray-700">Split Segment</span>
                    </label>
                    {splitConfig.enabled && (
                      <div className="ml-6 space-y-3 p-3 bg-green-50/70 rounded-xl border border-green-100">
                        <div className="grid grid-cols-1 sm:grid-cols-2 gap-3">
                          <div>
                            <label className="block text-xs font-semibold text-gray-500 uppercase tracking-wider mb-1.5">Split Type</label>
                            <select value={splitConfig.split_type} onChange={(e) => setSplitConfig({ split_type: e.target.value as "percent" | "attribute" })} className="w-full px-2 py-2 border border-gray-200 rounded-lg text-xs focus:ring-2 focus:ring-green-500 outline-none bg-white">
                              <option value="percent">Percentage Split</option>
                              <option value="attribute">Attribute Split</option>
                            </select>
                          </div>
                          {splitConfig.split_type === "attribute" && (
                            <div>
                              <label className="block text-xs font-semibold text-gray-500 uppercase tracking-wider mb-1.5">Split Attribute</label>
                              <select value={splitConfig.attribute || ""} onChange={(e) => setSplitConfig({ attribute: e.target.value || null })} className="w-full px-2 py-2 border border-gray-200 rounded-lg text-xs focus:ring-2 focus:ring-green-500 outline-none bg-white">
                                <option value="">Select…</option>
                                {SPLITTABLE_ATTRIBUTES.map((attr) => <option key={attr.key} value={attr.key}>{attr.label}</option>)}
                              </select>
                            </div>
                          )}
                        </div>
                        <div className="space-y-2">
                          <div className="flex items-center justify-between">
                            <span className="text-xs font-semibold text-gray-500 uppercase tracking-wider">Splits ({splitConfig.splits.length})</span>
                            <button onClick={addSplitEntry} className="px-2.5 py-1 text-xs bg-green-600 text-white rounded-lg hover:bg-green-700 transition-colors duration-150 active:scale-95">+ Add Split</button>
                          </div>
                          {splitConfig.splits.map((split, idx) => (
                            <div key={idx} className="flex items-center gap-2 bg-white rounded-lg p-2 border border-green-200">
                              <input type="text" value={split.name} onChange={(e) => updateSplitEntry(idx, { name: e.target.value })} placeholder="Split name" className="flex-1 min-w-0 px-2 py-1.5 border border-gray-200 rounded-md text-xs focus:ring-2 focus:ring-green-500 outline-none" />
                              {splitConfig.split_type === "percent" ? (
                                <div className="flex items-center gap-1 shrink-0">
                                  <input type="number" value={split.percent ?? ""} onChange={(e) => updateSplitEntry(idx, { percent: parseInt(e.target.value) || 0 })} placeholder="%" className="w-14 px-2 py-1.5 border border-gray-200 rounded-md text-xs text-right focus:ring-2 focus:ring-green-500 outline-none" min={0} max={100} />
                                  <span className="text-xs text-gray-500">%</span>
                                </div>
                              ) : (
                                <input type="text" value={split.value ?? ""} onChange={(e) => updateSplitEntry(idx, { value: e.target.value })} placeholder="Value" className="w-28 px-2 py-1.5 border border-gray-200 rounded-md text-xs focus:ring-2 focus:ring-green-500 outline-none" />
                              )}
                              <button onClick={() => removeSplitEntry(idx)} className="p-1 text-gray-400 hover:text-red-500 transition-colors duration-150 rounded">
                                <svg className="w-3.5 h-3.5" fill="none" viewBox="0 0 24 24" stroke="currentColor"><path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2.5} d="M6 18L18 6M6 6l12 12" /></svg>
                              </button>
                            </div>
                          ))}
                          {splitConfig.split_type === "percent" && splitConfig.splits.length > 0 && (
                            <p className="text-xs text-gray-500">
                              Total: {splitConfig.splits.reduce((s, x) => s + (x.percent || 0), 0)}%
                              {splitConfig.splits.reduce((s, x) => s + (x.percent || 0), 0) !== 100 && <span className="text-amber-600 ml-1 font-medium">(should be 100%)</span>}
                            </p>
                          )}
                        </div>
                      </div>
                    )}
                  </div>
                </div>
              </CollapsibleContent>
            </div>

            {/* Set Operations */}
            <div className="bg-white rounded-xl border border-gray-200 shadow-sm overflow-hidden">
              <button onClick={() => setShowSetOps(!showSetOps)} className="w-full px-4 py-3.5 flex items-center justify-between text-sm font-medium text-gray-700 hover:bg-gray-50/80 transition-colors duration-150">
                <div className="flex items-center gap-2">
                  <svg className="w-4 h-4 text-blue-500 flex-shrink-0" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                    <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M8 16H6a2 2 0 01-2-2V6a2 2 0 012-2h8a2 2 0 012 2v2m-6 12h8a2 2 0 002-2v-8a2 2 0 00-2-2h-8a2 2 0 00-2 2v8a2 2 0 002 2z" />
                  </svg>
                  <span>Set Operations</span>
                  {setOperation.enabled && (
                    <span className="px-2 py-0.5 text-[10px] rounded-full text-white font-semibold" style={{ backgroundColor: SET_OPERATION_LABELS[setOperation.operation].color }}>
                      {SET_OPERATION_LABELS[setOperation.operation].label}
                    </span>
                  )}
                </div>
                <ChevronIcon open={showSetOps} />
              </button>
              <CollapsibleContent open={showSetOps}>
                <div className="px-4 pb-5 border-t border-gray-100">
                  <div className="pt-4">
                    <label className="flex items-center gap-2.5 cursor-pointer mb-3">
                      <input type="checkbox" checked={setOperation.enabled} onChange={(e) => setSetOperation({ enabled: e.target.checked })} className="w-4 h-4 text-blue-600 rounded border-gray-300 focus:ring-blue-500" />
                      <span className="text-sm font-medium text-gray-700">Enable Set Operations</span>
                    </label>
                    {setOperation.enabled && (
                      <div className="space-y-4 p-3 bg-blue-50/70 rounded-xl border border-blue-100">
                        <div>
                          <label className="block text-xs font-semibold text-gray-500 uppercase tracking-wider mb-2">Operation Type</label>
                          <div className="grid grid-cols-2 gap-2">
                            {(Object.keys(SET_OPERATION_LABELS) as SetOperationType[]).map((op) => {
                              const config = SET_OPERATION_LABELS[op];
                              return (
                                <button key={op} onClick={() => setSetOperation({ operation: op })}
                                  className={`px-3 py-2.5 text-left rounded-xl border-2 transition-all duration-150 ${setOperation.operation === op ? "border-current bg-white shadow-sm" : "border-transparent bg-white/60 hover:bg-white"}`}
                                  style={{ color: setOperation.operation === op ? config.color : undefined }}
                                >
                                  <div className="text-xs font-semibold">{config.label}</div>
                                  <div className="text-[10px] text-gray-500 mt-0.5">{config.description}</div>
                                </button>
                              );
                            })}
                          </div>
                        </div>
                        <div className="flex items-center justify-center py-1">
                          <SetOperationVisual operation={setOperation.operation} />
                        </div>
                        <div className="space-y-2">
                          <div className="flex items-center justify-between">
                            <span className="text-xs font-semibold text-gray-500 uppercase tracking-wider">Combine with Segments ({setOperation.segments.length})</span>
                            <button onClick={() => setSetOperation({ segments: [...setOperation.segments, { segment_id: "", name: "" }] })} className="px-2.5 py-1 text-xs bg-blue-600 text-white rounded-lg hover:bg-blue-700 transition-colors duration-150 active:scale-95">+ Add Segment</button>
                          </div>
                          <p className="text-[10px] text-blue-700 bg-blue-100 px-3 py-1.5 rounded-lg">The current segment (above) is Segment A. Add other segments to combine with.</p>
                          {setOperation.segments.map((entry, idx) => (
                            <div key={idx} className="flex items-center gap-2 bg-white rounded-lg p-2 border border-blue-200">
                              <span className="text-xs font-bold text-gray-500 w-5 flex-shrink-0">{String.fromCharCode(66 + idx)}</span>
                              <input type="text" value={entry.name || ""} onChange={(e) => { const s = [...setOperation.segments]; s[idx] = { ...s[idx], name: e.target.value }; setSetOperation({ segments: s }); }} placeholder="Segment name or ID" className="flex-1 min-w-0 px-2 py-1.5 border border-gray-200 rounded-md text-xs focus:ring-2 focus:ring-blue-500 outline-none" />
                              <button onClick={() => setSetOperation({ segments: setOperation.segments.filter((_, i) => i !== idx) })} className="p-1 text-gray-400 hover:text-red-500 transition-colors duration-150 rounded">
                                <svg className="w-3.5 h-3.5" fill="none" viewBox="0 0 24 24" stroke="currentColor"><path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2.5} d="M6 18L18 6M6 6l12 12" /></svg>
                              </button>
                            </div>
                          ))}
                        </div>
                      </div>
                    )}
                  </div>
                </div>
              </CollapsibleContent>
            </div>

            {/* SQL Preview */}
            {compiledSQL && (
              <div className="bg-white rounded-xl border border-gray-200 shadow-sm overflow-hidden">
                <button onClick={() => setShowSQL(!showSQL)} className="w-full px-4 py-3.5 flex items-center justify-between text-sm font-medium text-gray-700 hover:bg-gray-50/80 transition-colors duration-150">
                  <div className="flex items-center gap-2">
                    <svg className="w-4 h-4 text-emerald-500 flex-shrink-0" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                      <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M4 7v10c0 2.21 3.582 4 8 4s8-1.79 8-4V7M4 7c0 2.21 3.582 4 8 4s8-1.79 8-4M4 7c0-2.21 3.582-4 8-4s8 1.79 8 4" />
                    </svg>
                    <span>Generated PostgreSQL SQL</span>
                  </div>
                  <ChevronIcon open={showSQL} />
                </button>
                <CollapsibleContent open={showSQL}>
                  <div className="relative group border-t border-gray-100">
                    <button
                      onClick={(e) => { e.stopPropagation(); navigator.clipboard.writeText(compiledSQL); setCopiedSQL(true); setTimeout(() => setCopiedSQL(false), 2000); }}
                      className="absolute top-2 right-2 p-1.5 bg-gray-800 hover:bg-gray-700 text-gray-400 hover:text-white rounded border border-gray-700 opacity-0 group-hover:opacity-100 transition-all duration-200 flex items-center gap-1"
                    >
                      {copiedSQL ? (
                        <><svg className="w-3.5 h-3.5 text-green-400" fill="none" viewBox="0 0 24 24" stroke="currentColor"><path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2.5} d="M5 13l4 4L19 7" /></svg><span className="text-[10px] text-green-400 font-medium">Copied</span></>
                      ) : (
                        <><svg className="w-3.5 h-3.5" fill="none" viewBox="0 0 24 24" stroke="currentColor"><path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M8 16H6a2 2 0 01-2-2V6a2 2 0 012-2h8a2 2 0 012 2v2m-6 12h8a2 2 0 002-2v-8a2 2 0 00-2-2h-8a2 2 0 00-2 2v8a2 2 0 002 2z" /></svg><span className="text-[10px] font-medium">Copy</span></>
                      )}
                    </button>
                    <pre className="p-4 bg-gray-900 text-green-400 text-xs font-mono overflow-x-auto min-h-[60px] leading-relaxed">{compiledSQL}</pre>
                  </div>
                </CollapsibleContent>
              </div>
            )}
          </div>

          {/* ── Right Sidebar ── */}
          <div className="w-full lg:w-80 flex-shrink-0 space-y-4">

            {/* Audience card */}
            <div className="bg-white rounded-xl border border-gray-200 p-4 shadow-sm">
              <h3 className="text-xs font-semibold text-gray-500 uppercase tracking-wider mb-3">Audience Size</h3>
              <div className="text-center py-4">
                {isEstimating ? (
                  <div className="flex flex-col items-center gap-2">
                    <div className="w-9 h-9 border-[3px] border-indigo-600 border-t-transparent rounded-full animate-spin" />
                    <span className="text-gray-400 text-sm">Querying DWH…</span>
                  </div>
                ) : audienceCount !== null ? (
                  <div>
                    <div className="text-4xl font-black text-indigo-600 tabular-nums">{audienceCount.toLocaleString()}</div>
                    <div className="text-xs text-gray-400 mt-1">matching profiles</div>
                  </div>
                ) : (
                  <p className="text-gray-400 text-sm px-2 leading-relaxed">Add conditions and click "Estimate Audience"</p>
                )}
              </div>

              {setOperationCounts && (
                <div className="mt-3 pt-3 border-t border-gray-100">
                  <div className="flex items-center gap-2 mb-2">
                    <div className="w-2 h-2 rounded-full flex-shrink-0" style={{ backgroundColor: SET_OPERATION_LABELS[setOperationCounts.operation as SetOperationType]?.color || "#6366f1" }} />
                    <span className="text-xs font-semibold text-gray-600">{SET_OPERATION_LABELS[setOperationCounts.operation as SetOperationType]?.label || setOperationCounts.operation} Result</span>
                  </div>
                  <div className="space-y-1.5">
                    {setOperationCounts.segment_counts.map((count: number, idx: number) => (
                      <div key={idx} className="flex items-center justify-between text-xs">
                        <span className="text-gray-500">Segment {String.fromCharCode(65 + idx)}</span>
                        <span className="font-semibold text-gray-700 tabular-nums">{count !== null ? count.toLocaleString() : "—"}</span>
                      </div>
                    ))}
                    <div className="flex items-center justify-between text-xs pt-1.5 border-t border-gray-100">
                      <span className="font-semibold text-gray-700">Combined</span>
                      <span className="font-black text-indigo-600 tabular-nums">{setOperationCounts.combined_count !== null ? setOperationCounts.combined_count.toLocaleString() : "—"}</span>
                    </div>
                  </div>
                </div>
              )}

              {splitCounts.length > 0 && (
                <div className="mt-3 pt-3 border-t border-gray-100">
                  <div className="flex items-center gap-2 mb-2">
                    <div className="w-2 h-2 rounded-full bg-green-500 flex-shrink-0" />
                    <span className="text-xs font-semibold text-gray-600">Split Breakdown</span>
                  </div>
                  <div className="space-y-1.5">
                    {splitCounts.map((split, idx) => (
                      <div key={idx} className="flex items-center justify-between text-xs">
                        <span className="text-gray-500 flex items-center gap-1 min-w-0 mr-2">
                          <span className="truncate">{split.name}</span>
                          {split.percent !== undefined && <span className="text-[10px] text-gray-400 flex-shrink-0">({split.percent}%)</span>}
                          {split.value !== undefined && <span className="text-[10px] text-gray-400 flex-shrink-0">({split.value})</span>}
                        </span>
                        <span className="font-semibold text-gray-700 tabular-nums flex-shrink-0">{split.count !== null ? split.count.toLocaleString() : "—"}</span>
                      </div>
                    ))}
                  </div>
                  {splitCounts.some(s => s.count !== null && s.count > 0) && (
                    <div className="mt-2 flex gap-0.5 h-2.5 rounded-full overflow-hidden bg-gray-100">
                      {splitCounts.map((split, idx) => {
                        const total = splitCounts.reduce((sum, s) => sum + (s.count || 0), 0);
                        const pct = total > 0 ? ((split.count || 0) / total) * 100 : 0;
                        const colors = ["#22c55e", "#3b82f6", "#f59e0b", "#ef4444", "#8b5cf6", "#06b6d4"];
                        return <div key={idx} className="h-full transition-all duration-500" style={{ width: `${pct}%`, backgroundColor: colors[idx % colors.length] }} title={`${split.name}: ${split.count?.toLocaleString()} (${pct.toFixed(1)}%)`} />;
                      })}
                    </div>
                  )}
                </div>
              )}
            </div>

            <AudienceSummaryPanel />

            {/* Attribute categories */}
            <div className="bg-white rounded-xl border border-gray-200 p-4 shadow-sm">
              <h3 className="text-xs font-semibold text-gray-500 uppercase tracking-wider mb-3">Attribute Categories</h3>
              <div className="space-y-0.5">
                {Object.entries(CATEGORY_CONFIG).map(([key, config]) => (
                  <button key={key} onClick={() => quickAddCondition(key)} className="w-full flex items-center gap-2.5 text-xs text-gray-600 hover:bg-indigo-50 hover:text-indigo-700 px-2 py-1.5 rounded-lg transition-colors duration-150 group text-left">
                    <div className="w-2 h-2 rounded-full flex-shrink-0 transition-transform duration-150 group-hover:scale-125" style={{ backgroundColor: config.color }} />
                    <span className="flex-1">{config.label}</span>
                    <svg className="w-3 h-3 opacity-0 group-hover:opacity-100 transition-opacity duration-150" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                      <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2.5} d="M12 4v16m8-8H4" />
                    </svg>
                  </button>
                ))}
              </div>
            </div>

            {/* Segment templates */}
            <div className="bg-white rounded-xl border border-gray-200 p-4 shadow-sm">
              <h3 className="text-xs font-semibold text-gray-500 uppercase tracking-wider mb-3">Segment Templates</h3>
              <div className="flex flex-wrap gap-1 mb-3">
                {["all", "marketing", "merch", "product", "cx", "finance"].map((fn) => (
                  <button key={fn} onClick={() => setTemplateFilter(fn)}
                    className={`px-2.5 py-0.5 text-xs rounded-full transition-colors duration-150 ${templateFilter === fn ? "bg-indigo-100 text-indigo-700 font-medium" : "bg-gray-100 text-gray-500 hover:bg-gray-200"}`}
                  >
                    {fn === "all" ? "All" : fn.charAt(0).toUpperCase() + fn.slice(1)}
                  </button>
                ))}
              </div>
              <div className="space-y-1.5 max-h-80 overflow-y-auto pr-0.5">
                {templates.filter((t) => templateFilter === "all" || t.business_function === templateFilter).map((template) => (
                  <button key={template.id} onClick={() => handleLoadTemplate(template)} className="w-full text-left px-3 py-2.5 text-xs bg-gray-50 hover:bg-indigo-50 rounded-xl transition-colors duration-150 group border border-transparent hover:border-indigo-100">
                    <div className="font-semibold text-gray-700 group-hover:text-indigo-600 mb-0.5">{template.name}</div>
                    <div className="text-gray-400 line-clamp-2 leading-relaxed mb-1">{template.description}</div>
                    <div className="flex gap-1 flex-wrap">
                      <span className="px-1.5 py-0.5 text-[10px] bg-gray-200 text-gray-600 rounded-md">{template.business_function}</span>
                      <span className="px-1.5 py-0.5 text-[10px] bg-gray-200 text-gray-600 rounded-md">{template.category}</span>
                    </div>
                  </button>
                ))}
                {templates.filter((t) => templateFilter === "all" || t.business_function === templateFilter).length === 0 && (
                  <p className="text-xs text-gray-400 text-center py-4">{selectedBrandCode ? "No templates for this filter" : "Select a brand to see templates"}</p>
                )}
              </div>
            </div>
          </div>
        </div>
      )}

      <Toast toast={toast} />
    </div>
  );
};

/* ── SetOperationVisual ── */
const SetOperationVisual: React.FC<{ operation: SetOperationType }> = ({ operation }) => {
  const getHighlight = () => {
    switch (operation) {
      case "union":           return { aOpacity: 0.4, bOpacity: 0.4, overlapOpacity: 0.6, label: "A + B" };
      case "overlap":         return { aOpacity: 0.1, bOpacity: 0.1, overlapOpacity: 0.6, label: "A ∩ B" };
      case "exclude_overlap": return { aOpacity: 0.4, bOpacity: 0.1, overlapOpacity: 0.1, label: "A − (A ∩ B)" };
      case "exclude":         return { aOpacity: 0.4, bOpacity: 0.1, overlapOpacity: 0.1, label: "A − B" };
    }
  };
  const { aOpacity, bOpacity, overlapOpacity, label } = getHighlight();
  const config = SET_OPERATION_LABELS[operation];
  return (
    <div className="flex flex-col items-center gap-1">
      <svg width="120" height="70" viewBox="0 0 120 70">
        <circle cx="42" cy="35" r="28" fill={config.color} fillOpacity={aOpacity} stroke={config.color} strokeWidth="1.5" />
        <circle cx="78" cy="35" r="28" fill={config.color} fillOpacity={bOpacity} stroke={config.color} strokeWidth="1.5" />
        <clipPath id={`clip-a-${operation}`}><circle cx="42" cy="35" r="28" /></clipPath>
        <circle cx="78" cy="35" r="28" fill={config.color} fillOpacity={overlapOpacity} clipPath={`url(#clip-a-${operation})`} />
        <text x="32" y="38" textAnchor="middle" fontSize="10" fill="#374151" fontWeight="700">A</text>
        <text x="88" y="38" textAnchor="middle" fontSize="10" fill="#374151" fontWeight="700">B</text>
      </svg>
      <span className="text-[10px] font-semibold" style={{ color: config.color }}>{label}</span>
    </div>
  );
};

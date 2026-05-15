/**
 * SegmentBuilder — Main segment builder page component.
 *
 * Combines:
 * - Brand selector
 * - Segment metadata (name, type)
 * - Visual rule builder (ConditionGroupUI)
 * - Rank & Split configuration
 * - Set Operations (Union, Overlap, Exclude)
 * - Audience count estimator with split/set-op breakdowns
 * - PostgreSQL SQL preview (for power users)
 * - Save/Publish actions
 */

import React, { useState } from "react";
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

const RPSGLogo: React.FC<{ size?: number }> = ({ size = 38 }) => (
  <svg width={size} height={size} viewBox="0 0 80 80" fill="none" xmlns="http://www.w3.org/2000/svg" aria-label="RP-Sanjiv Goenka Group">
    <g transform="translate(40,40)">
      {/* Primary petals — 8 at 45° increments */}
      <path d="M0,0 L-6,-11 L0,-36 L6,-11 Z" fill="#7B2D8B"/>
      <path d="M0,0 L-6,-11 L0,-36 L6,-11 Z" fill="#A030A0" transform="rotate(45)" opacity="0.82"/>
      <path d="M0,0 L-6,-11 L0,-36 L6,-11 Z" fill="#C73280" transform="rotate(90)"/>
      <path d="M0,0 L-6,-11 L0,-36 L6,-11 Z" fill="#E0402E" transform="rotate(135)" opacity="0.9"/>
      <path d="M0,0 L-6,-11 L0,-36 L6,-11 Z" fill="#F06820" transform="rotate(180)"/>
      <path d="M0,0 L-6,-11 L0,-36 L6,-11 Z" fill="#F5A010" transform="rotate(225)" opacity="0.85"/>
      <path d="M0,0 L-5,-9 L0,-30 L5,-9 Z"  fill="#F0B82A" transform="rotate(270)" opacity="0.75"/>
      <path d="M0,0 L-5,-9 L0,-30 L5,-9 Z"  fill="#8C35A5" transform="rotate(315)" opacity="0.78"/>
      {/* Secondary petals — shorter, between primaries for depth */}
      <path d="M0,0 L-4,-8 L0,-22 L4,-8 Z" fill="#C040B0" transform="rotate(22.5)"  opacity="0.6"/>
      <path d="M0,0 L-4,-8 L0,-22 L4,-8 Z" fill="#D83560" transform="rotate(67.5)"  opacity="0.6"/>
      <path d="M0,0 L-4,-8 L0,-22 L4,-8 Z" fill="#EC5030" transform="rotate(112.5)" opacity="0.6"/>
      <path d="M0,0 L-4,-8 L0,-22 L4,-8 Z" fill="#F58518" transform="rotate(157.5)" opacity="0.6"/>
      <path d="M0,0 L-4,-8 L0,-22 L4,-8 Z" fill="#F5B020" transform="rotate(202.5)" opacity="0.55"/>
      <path d="M0,0 L-3,-7 L0,-18 L3,-7 Z" fill="#E0A030" transform="rotate(247.5)" opacity="0.5"/>
      <path d="M0,0 L-3,-7 L0,-18 L3,-7 Z" fill="#9B40B0" transform="rotate(292.5)" opacity="0.55"/>
      <path d="M0,0 L-4,-8 L0,-22 L4,-8 Z" fill="#8530A0" transform="rotate(337.5)" opacity="0.6"/>
    </g>
  </svg>
);

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

  // Load templates when brand changes
  React.useEffect(() => {
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
          return {
            ...node,
            type: "group",
            id,
            conditions: (node.conditions || []).map(addIds),
          };
        }
        return { ...node, id };
      };
      const rulesWithIds = addIds(template.rules.root);
      loadRules(rulesWithIds);
      setSegmentName(template.name);
      setSegmentDescription(template.description);
    }
  };

  const handleEstimate = async () => {
    await estimateAudience();
  };

  const handleSave = async () => {
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
    alert(`Segment created: ${data.id}`);
  };

  return (
    <div className="min-h-screen bg-gray-50">
      {/* Top bar */}
      <div className="bg-white border-b border-gray-200 sticky top-0 z-40">
        <div className="max-w-7xl mx-auto px-6 py-3 flex items-center justify-between">
          <div className="flex items-center gap-6">
            {/* RPSG Branding */}
            <div className="flex items-center gap-3 shrink-0">
              <RPSGLogo size={38} />
              <span className="text-xl font-semibold tracking-wide text-gray-800" style={{ letterSpacing: "0.04em" }}>
                UNIFY 360
              </span>
            </div>
            {/* Divider */}
            <div className="w-px h-7 bg-gray-200 shrink-0" />
            <h1 className="text-sm font-semibold text-gray-500 shrink-0">
              Segment Builder
            </h1>
            {/* Mode toggle */}
            <div className="flex bg-gray-100 rounded-lg p-0.5">
              <button
                onClick={() => setBuilderMode("visual")}
                className={`px-3 py-1.5 text-xs font-medium rounded-md transition ${
                  builderMode === "visual"
                    ? "bg-white text-indigo-700 shadow-sm"
                    : "text-gray-500 hover:text-gray-700"
                }`}
              >
                Visual Builder
              </button>
              <button
                onClick={() => setBuilderMode("nl")}
                className={`px-3 py-1.5 text-xs font-medium rounded-md transition ${
                  builderMode === "nl"
                    ? "bg-white text-indigo-700 shadow-sm"
                    : "text-gray-500 hover:text-gray-700"
                }`}
              >
                Natural Language
              </button>
            </div>
            {isDirty && (
              <span className="px-2 py-0.5 text-xs bg-amber-100 text-amber-700 rounded-full">
                Unsaved changes
              </span>
            )}
          </div>
          <div className="flex items-center gap-3">
            <button
              onClick={resetRules}
              className="px-4 py-2 text-sm text-gray-600 hover:text-gray-900"
            >
              Reset
            </button>
            <button
              onClick={handleEstimate}
              disabled={isEstimating || !selectedBrandCode}
              className="px-4 py-2 text-sm bg-gray-100 hover:bg-gray-200 rounded-md disabled:opacity-50"
            >
              {isEstimating ? "Estimating..." : "Estimate Audience"}
            </button>
            <button
              onClick={handleSave}
              disabled={!segmentName || !selectedBrandCode}
              className="px-4 py-2 text-sm bg-indigo-600 text-white hover:bg-indigo-700 rounded-md disabled:opacity-50"
            >
              Save Segment
            </button>
          </div>
        </div>
      </div>

      {/* NL Mode — full-height chat interface */}
      {builderMode === "nl" && (
        <div className="max-w-5xl mx-auto px-6 py-6" style={{ height: "calc(100vh - 73px)" }}>
          <div className="bg-white rounded-lg border border-gray-200 h-full flex flex-col overflow-hidden">
            <NLSegmentPanel />
          </div>
        </div>
      )}

      {/* Visual Builder Mode */}
      {builderMode === "visual" && (
      <div className="max-w-7xl mx-auto px-6 py-6 flex gap-6">
        {/* Main builder area */}
        <div className="flex-1 space-y-6">
          {/* Segment metadata */}
          <div className="bg-white rounded-lg border border-gray-200 p-6 space-y-4">
            <div className="grid grid-cols-2 gap-4">
              <div>
                <label className="block text-sm font-medium text-gray-700 mb-1">
                  Segment Name
                </label>
                <input
                  type="text"
                  value={segmentName}
                  onChange={(e) => setSegmentName(e.target.value)}
                  placeholder="e.g., High-Value At-Risk Customers"
                  className="w-full px-3 py-2 border rounded-md text-sm"
                />
              </div>
              <div className="flex gap-4">
                <div className="flex-1">
                  <label className="block text-sm font-medium text-gray-700 mb-1">
                    Brand
                  </label>
                  <select
                    value={selectedBrandCode || ""}
                    onChange={(e) => setSelectedBrand(e.target.value)}
                    className="w-full px-3 py-2 border rounded-md text-sm"
                  >
                    <option value="">Select brand...</option>
                    {brands.map((b) => (
                      <option key={b.code} value={b.code}>
                        {b.name}
                      </option>
                    ))}
                  </select>
                </div>
                <div className="flex-1">
                  <label className="block text-sm font-medium text-gray-700 mb-1">
                    Type
                  </label>
                  <select
                    value={segmentType}
                    onChange={(e) => setSegmentType(e.target.value)}
                    className="w-full px-3 py-2 border rounded-md text-sm"
                  >
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
              <label className="block text-sm font-medium text-gray-700 mb-1">
                Description
              </label>
              <textarea
                value={segmentDescription}
                onChange={(e) => setSegmentDescription(e.target.value)}
                placeholder="Describe this segment's purpose..."
                rows={2}
                className="w-full px-3 py-2 border rounded-md text-sm"
              />
            </div>
          </div>

          {/* Builder / JSON tabs */}
          <div className="bg-white rounded-lg border border-gray-200">
            <div className="flex border-b">
              <button
                onClick={() => setActiveTab("builder")}
                className={`px-4 py-2 text-sm font-medium border-b-2 ${
                  activeTab === "builder"
                    ? "border-indigo-600 text-indigo-600"
                    : "border-transparent text-gray-500 hover:text-gray-700"
                }`}
              >
                Visual Builder
              </button>
              <button
                onClick={() => setActiveTab("json")}
                className={`px-4 py-2 text-sm font-medium border-b-2 ${
                  activeTab === "json"
                    ? "border-indigo-600 text-indigo-600"
                    : "border-transparent text-gray-500 hover:text-gray-700"
                }`}
              >
                JSON / Power User
              </button>
            </div>

            <div className="p-4">
              {activeTab === "builder" ? (
                <ConditionGroupUI group={rules} isRoot />
              ) : (
                <pre className="p-4 bg-gray-900 text-green-400 rounded-lg overflow-x-auto text-xs font-mono max-h-96">
                  {JSON.stringify(getSegmentDefinition(), null, 2)}
                </pre>
              )}
            </div>
          </div>

          {/* Rank & Split Panel */}
          <div className="bg-white rounded-lg border border-gray-200">
            <button
              onClick={() => setShowRankSplit(!showRankSplit)}
              className="w-full px-4 py-3 flex items-center justify-between text-sm font-medium text-gray-700 hover:bg-gray-50"
            >
              <div className="flex items-center gap-2">
                <svg className="w-4 h-4 text-indigo-500" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M3 4h13M3 8h9m-9 4h6m4 0l4-4m0 0l4 4m-4-4v12" />
                </svg>
                <span>Rank & Split</span>
                {(rankConfig.enabled || splitConfig.enabled) && (
                  <span className="px-2 py-0.5 text-[10px] bg-indigo-100 text-indigo-700 rounded-full">Active</span>
                )}
              </div>
              <svg
                className={`w-4 h-4 transition-transform ${showRankSplit ? "rotate-180" : ""}`}
                fill="none"
                viewBox="0 0 24 24"
                stroke="currentColor"
              >
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M19 9l-7 7-7-7" />
              </svg>
            </button>

            {showRankSplit && (
              <div className="px-4 pb-4 space-y-4 border-t">
                {/* Rank Configuration */}
                <div className="pt-4">
                  <div className="flex items-center gap-3 mb-3">
                    <label className="flex items-center gap-2 cursor-pointer">
                      <input
                        type="checkbox"
                        checked={rankConfig.enabled}
                        onChange={(e) => setRankConfig({ enabled: e.target.checked })}
                        className="w-4 h-4 text-indigo-600 rounded"
                      />
                      <span className="text-sm font-medium text-gray-700">Rank by Attribute</span>
                    </label>
                  </div>

                  {rankConfig.enabled && (
                    <div className="ml-6 space-y-3 p-3 bg-indigo-50 rounded-lg">
                      <div className="grid grid-cols-3 gap-3">
                        <div>
                          <label className="block text-xs font-medium text-gray-600 mb-1">Rank Attribute</label>
                          <select
                            value={rankConfig.attribute || ""}
                            onChange={(e) => setRankConfig({ attribute: e.target.value || null })}
                            className="w-full px-2 py-1.5 border rounded text-xs"
                          >
                            <option value="">Select attribute...</option>
                            {RANKABLE_ATTRIBUTES.map((attr) => (
                              <option key={attr.key} value={attr.key}>{attr.label}</option>
                            ))}
                          </select>
                        </div>
                        <div>
                          <label className="block text-xs font-medium text-gray-600 mb-1">Order</label>
                          <select
                            value={rankConfig.order}
                            onChange={(e) => setRankConfig({ order: e.target.value as "asc" | "desc" })}
                            className="w-full px-2 py-1.5 border rounded text-xs"
                          >
                            <option value="desc">Highest First (DESC)</option>
                            <option value="asc">Lowest First (ASC)</option>
                          </select>
                        </div>
                        <div>
                          <label className="block text-xs font-medium text-gray-600 mb-1">Limit (Top N)</label>
                          <input
                            type="number"
                            value={rankConfig.profile_limit ?? ""}
                            onChange={(e) => setRankConfig({ profile_limit: e.target.value ? parseInt(e.target.value) : null })}
                            placeholder="No limit"
                            className="w-full px-2 py-1.5 border rounded text-xs"
                          />
                        </div>
                      </div>
                      {rankConfig.attribute && (
                        <div className="text-xs text-indigo-600">
                          Ranking by <strong>{RANKABLE_ATTRIBUTES.find(a => a.key === rankConfig.attribute)?.label || rankConfig.attribute}</strong>
                          {" "}({rankConfig.order === "desc" ? "highest first" : "lowest first"})
                          {rankConfig.profile_limit ? `, top ${rankConfig.profile_limit.toLocaleString()}` : ""}
                        </div>
                      )}
                    </div>
                  )}
                </div>

                {/* Split Configuration */}
                <div className="border-t pt-4">
                  <div className="flex items-center gap-3 mb-3">
                    <label className="flex items-center gap-2 cursor-pointer">
                      <input
                        type="checkbox"
                        checked={splitConfig.enabled}
                        onChange={(e) => setSplitConfig({ enabled: e.target.checked })}
                        className="w-4 h-4 text-green-600 rounded"
                      />
                      <span className="text-sm font-medium text-gray-700">Split Segment</span>
                    </label>
                  </div>

                  {splitConfig.enabled && (
                    <div className="ml-6 space-y-3 p-3 bg-green-50 rounded-lg">
                      <div className="grid grid-cols-2 gap-3">
                        <div>
                          <label className="block text-xs font-medium text-gray-600 mb-1">Split Type</label>
                          <select
                            value={splitConfig.split_type}
                            onChange={(e) => setSplitConfig({ split_type: e.target.value as "percent" | "attribute" })}
                            className="w-full px-2 py-1.5 border rounded text-xs"
                          >
                            <option value="percent">Percentage Split</option>
                            <option value="attribute">Attribute Split</option>
                          </select>
                        </div>
                        {splitConfig.split_type === "attribute" && (
                          <div>
                            <label className="block text-xs font-medium text-gray-600 mb-1">Split Attribute</label>
                            <select
                              value={splitConfig.attribute || ""}
                              onChange={(e) => setSplitConfig({ attribute: e.target.value || null })}
                              className="w-full px-2 py-1.5 border rounded text-xs"
                            >
                              <option value="">Select attribute...</option>
                              {SPLITTABLE_ATTRIBUTES.map((attr) => (
                                <option key={attr.key} value={attr.key}>{attr.label}</option>
                              ))}
                            </select>
                          </div>
                        )}
                      </div>

                      {/* Split entries */}
                      <div className="space-y-2">
                        <div className="flex items-center justify-between">
                          <span className="text-xs font-medium text-gray-600">
                            Splits ({splitConfig.splits.length})
                          </span>
                          <button
                            onClick={addSplitEntry}
                            className="px-2 py-1 text-[10px] bg-green-600 text-white rounded hover:bg-green-700"
                          >
                            + Add Split
                          </button>
                        </div>

                        {splitConfig.splits.map((split, idx) => (
                          <div key={idx} className="flex items-center gap-2 bg-white rounded p-2 border border-green-200">
                            <input
                              type="text"
                              value={split.name}
                              onChange={(e) => updateSplitEntry(idx, { name: e.target.value })}
                              placeholder="Split name"
                              className="flex-1 px-2 py-1 border rounded text-xs"
                            />
                            {splitConfig.split_type === "percent" ? (
                              <div className="flex items-center gap-1">
                                <input
                                  type="number"
                                  value={split.percent ?? ""}
                                  onChange={(e) => updateSplitEntry(idx, { percent: parseInt(e.target.value) || 0 })}
                                  placeholder="%"
                                  className="w-16 px-2 py-1 border rounded text-xs text-right"
                                  min={0}
                                  max={100}
                                />
                                <span className="text-xs text-gray-500">%</span>
                              </div>
                            ) : (
                              <input
                                type="text"
                                value={split.value ?? ""}
                                onChange={(e) => updateSplitEntry(idx, { value: e.target.value })}
                                placeholder="Attribute value"
                                className="w-32 px-2 py-1 border rounded text-xs"
                              />
                            )}
                            <button
                              onClick={() => removeSplitEntry(idx)}
                              className="text-red-400 hover:text-red-600 text-xs px-1"
                            >
                              x
                            </button>
                          </div>
                        ))}

                        {splitConfig.split_type === "percent" && splitConfig.splits.length > 0 && (
                          <div className="text-xs text-gray-500">
                            Total: {splitConfig.splits.reduce((sum, s) => sum + (s.percent || 0), 0)}%
                            {splitConfig.splits.reduce((sum, s) => sum + (s.percent || 0), 0) !== 100 && (
                              <span className="text-amber-600 ml-1">(should be 100%)</span>
                            )}
                          </div>
                        )}
                      </div>
                    </div>
                  )}
                </div>
              </div>
            )}
          </div>

          {/* Set Operations Panel */}
          <div className="bg-white rounded-lg border border-gray-200">
            <button
              onClick={() => setShowSetOps(!showSetOps)}
              className="w-full px-4 py-3 flex items-center justify-between text-sm font-medium text-gray-700 hover:bg-gray-50"
            >
              <div className="flex items-center gap-2">
                <svg className="w-4 h-4 text-blue-500" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M8 16H6a2 2 0 01-2-2V6a2 2 0 012-2h8a2 2 0 012 2v2m-6 12h8a2 2 0 002-2v-8a2 2 0 00-2-2h-8a2 2 0 00-2 2v8a2 2 0 002 2z" />
                </svg>
                <span>Set Operations</span>
                {setOperation.enabled && (
                  <span
                    className="px-2 py-0.5 text-[10px] rounded-full text-white"
                    style={{ backgroundColor: SET_OPERATION_LABELS[setOperation.operation].color }}
                  >
                    {SET_OPERATION_LABELS[setOperation.operation].label}
                  </span>
                )}
              </div>
              <svg
                className={`w-4 h-4 transition-transform ${showSetOps ? "rotate-180" : ""}`}
                fill="none"
                viewBox="0 0 24 24"
                stroke="currentColor"
              >
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M19 9l-7 7-7-7" />
              </svg>
            </button>

            {showSetOps && (
              <div className="px-4 pb-4 border-t">
                <div className="pt-4">
                  <div className="flex items-center gap-3 mb-3">
                    <label className="flex items-center gap-2 cursor-pointer">
                      <input
                        type="checkbox"
                        checked={setOperation.enabled}
                        onChange={(e) => setSetOperation({ enabled: e.target.checked })}
                        className="w-4 h-4 text-blue-600 rounded"
                      />
                      <span className="text-sm font-medium text-gray-700">Enable Set Operations</span>
                    </label>
                  </div>

                  {setOperation.enabled && (
                    <div className="space-y-4 p-3 bg-blue-50 rounded-lg">
                      {/* Operation type selector */}
                      <div>
                        <label className="block text-xs font-medium text-gray-600 mb-2">Operation Type</label>
                        <div className="grid grid-cols-2 gap-2">
                          {(Object.keys(SET_OPERATION_LABELS) as SetOperationType[]).map((op) => {
                            const config = SET_OPERATION_LABELS[op];
                            return (
                              <button
                                key={op}
                                onClick={() => setSetOperation({ operation: op })}
                                className={`px-3 py-2 text-left rounded-lg border-2 transition-all ${
                                  setOperation.operation === op
                                    ? "border-current bg-white shadow-sm"
                                    : "border-transparent bg-white/60 hover:bg-white"
                                }`}
                                style={{ color: setOperation.operation === op ? config.color : undefined }}
                              >
                                <div className="text-xs font-semibold">{config.label}</div>
                                <div className="text-[10px] text-gray-500 mt-0.5">{config.description}</div>
                              </button>
                            );
                          })}
                        </div>
                      </div>

                      {/* Venn diagram visual */}
                      <div className="flex items-center justify-center py-2">
                        <SetOperationVisual operation={setOperation.operation} />
                      </div>

                      {/* Segment entries for set operation */}
                      <div className="space-y-2">
                        <div className="flex items-center justify-between">
                          <span className="text-xs font-medium text-gray-600">
                            Combine with Segments ({setOperation.segments.length})
                          </span>
                          <button
                            onClick={() => setSetOperation({
                              segments: [...setOperation.segments, { segment_id: "", name: "" }],
                            })}
                            className="px-2 py-1 text-[10px] bg-blue-600 text-white rounded hover:bg-blue-700"
                          >
                            + Add Segment
                          </button>
                        </div>

                        <div className="text-[10px] text-blue-700 bg-blue-100 px-2 py-1 rounded">
                          The current segment (above) is Segment A. Add other segments to combine with.
                        </div>

                        {setOperation.segments.map((entry, idx) => (
                          <div key={idx} className="flex items-center gap-2 bg-white rounded p-2 border border-blue-200">
                            <span className="text-xs font-medium text-gray-500 w-6">
                              {String.fromCharCode(66 + idx)}
                            </span>
                            <input
                              type="text"
                              value={entry.name || ""}
                              onChange={(e) => {
                                const newSegments = [...setOperation.segments];
                                newSegments[idx] = { ...newSegments[idx], name: e.target.value };
                                setSetOperation({ segments: newSegments });
                              }}
                              placeholder="Segment name or ID"
                              className="flex-1 px-2 py-1 border rounded text-xs"
                            />
                            <button
                              onClick={() => {
                                const newSegments = setOperation.segments.filter((_, i) => i !== idx);
                                setSetOperation({ segments: newSegments });
                              }}
                              className="text-red-400 hover:text-red-600 text-xs px-1"
                            >
                              x
                            </button>
                          </div>
                        ))}
                      </div>
                    </div>
                  )}
                </div>
              </div>
            )}
          </div>

          {/* SQL Preview */}
          {compiledSQL && (
            <div className="bg-white rounded-lg border border-gray-200">
              <button
                onClick={() => setShowSQL(!showSQL)}
                className="w-full px-4 py-3 flex items-center justify-between text-sm font-medium text-gray-700 hover:bg-gray-50"
              >
                <div className="flex items-center gap-2">
                  <svg className="w-4 h-4 text-emerald-500" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                    <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M4 7v10c0 2.21 3.582 4 8 4s8-1.79 8-4V7M4 7c0 2.21 3.582 4 8 4s8-1.79 8-4M4 7c0-2.21 3.582-4 8-4s8 1.79 8 4" />
                  </svg>
                  <span>Generated PostgreSQL SQL</span>
                </div>
                <svg
                  className={`w-4 h-4 transition-transform ${showSQL ? "rotate-180" : ""}`}
                  fill="none"
                  viewBox="0 0 24 24"
                  stroke="currentColor"
                >
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M19 9l-7 7-7-7" />
                </svg>
              </button>
              {showSQL && (
                <div className="relative group">
                  <button
                    onClick={(e) => {
                      e.stopPropagation();
                      navigator.clipboard.writeText(compiledSQL);
                      setCopiedSQL(true);
                      setTimeout(() => setCopiedSQL(false), 2000);
                    }}
                    className="absolute top-2 right-2 p-1.5 bg-gray-800 hover:bg-gray-700 text-gray-400 hover:text-white rounded border border-gray-700 opacity-0 group-hover:opacity-100 transition-all flex items-center gap-1"
                    title="Copy SQL"
                  >
                    {copiedSQL ? (
                      <>
                        <svg className="w-3.5 h-3.5 text-green-400" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                          <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M5 13l4 4L19 7" />
                        </svg>
                        <span className="text-[10px] text-green-400 font-medium">Copied</span>
                      </>
                    ) : (
                      <>
                        <svg className="w-3.5 h-3.5" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                          <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M8 16H6a2 2 0 01-2-2V6a2 2 0 012-2h8a2 2 0 012 2v2m-6 12h8a2 2 0 002-2v-8a2 2 0 00-2-2h-8a2 2 0 00-2 2v8a2 2 0 002 2z" />
                        </svg>
                        <span className="text-[10px] font-medium">Copy</span>
                      </>
                    )}
                  </button>
                  <pre className="p-4 bg-gray-900 text-green-400 text-xs font-mono overflow-x-auto border-t min-h-[60px]">
                    {compiledSQL}
                  </pre>
                </div>
              )}
            </div>
          )}
        </div>

        {/* Right sidebar — Audience & attribute reference */}
        <div className="w-80 space-y-4 flex-shrink-0">
          {/* Audience estimation card */}
          <div className="bg-white rounded-lg border border-gray-200 p-4">
            <h3 className="text-sm font-semibold text-gray-700 mb-3">
              Audience Size
            </h3>
            <div className="text-center py-4">
              {isEstimating ? (
                <div className="flex flex-col items-center gap-2">
                  <div className="w-8 h-8 border-2 border-indigo-600 border-t-transparent rounded-full animate-spin" />
                  <div className="text-gray-400 text-sm">Querying DWH...</div>
                </div>
              ) : audienceCount !== null ? (
                <div>
                  <div className="text-3xl font-bold text-indigo-600">
                    {audienceCount.toLocaleString()}
                  </div>
                  <div className="text-xs text-gray-500 mt-1">
                    matching profiles
                  </div>
                </div>
              ) : (
                <div className="text-gray-400 text-sm">
                  Add conditions and click "Estimate Audience"
                </div>
              )}
            </div>

            {/* Set Operation Counts */}
            {setOperationCounts && (
              <div className="mt-3 pt-3 border-t border-gray-100">
                <div className="flex items-center gap-2 mb-2">
                  <div
                    className="w-2 h-2 rounded-full"
                    style={{ backgroundColor: SET_OPERATION_LABELS[setOperationCounts.operation as SetOperationType]?.color || "#6366f1" }}
                  />
                  <span className="text-xs font-semibold text-gray-600">
                    {SET_OPERATION_LABELS[setOperationCounts.operation as SetOperationType]?.label || setOperationCounts.operation} Result
                  </span>
                </div>
                <div className="space-y-1.5">
                  {setOperationCounts.segment_counts.map((count: number, idx: number) => (
                    <div key={idx} className="flex items-center justify-between text-xs">
                      <span className="text-gray-500">
                        Segment {String.fromCharCode(65 + idx)}
                      </span>
                      <span className="font-medium text-gray-700">
                        {count !== null ? count.toLocaleString() : "—"}
                      </span>
                    </div>
                  ))}
                  <div className="flex items-center justify-between text-xs pt-1 border-t border-gray-100">
                    <span className="font-semibold text-gray-700">Combined</span>
                    <span className="font-bold text-indigo-600">
                      {setOperationCounts.combined_count !== null
                        ? setOperationCounts.combined_count.toLocaleString()
                        : "—"}
                    </span>
                  </div>
                </div>
              </div>
            )}

            {/* Split Counts */}
            {splitCounts.length > 0 && (
              <div className="mt-3 pt-3 border-t border-gray-100">
                <div className="flex items-center gap-2 mb-2">
                  <div className="w-2 h-2 rounded-full bg-green-500" />
                  <span className="text-xs font-semibold text-gray-600">Split Breakdown</span>
                </div>
                <div className="space-y-1.5">
                  {splitCounts.map((split, idx) => (
                    <div key={idx} className="flex items-center justify-between text-xs">
                      <span className="text-gray-500 flex items-center gap-1">
                        {split.name}
                        {split.percent !== undefined && (
                          <span className="text-[10px] text-gray-400">({split.percent}%)</span>
                        )}
                        {split.value !== undefined && (
                          <span className="text-[10px] text-gray-400">({split.value})</span>
                        )}
                      </span>
                      <span className="font-medium text-gray-700">
                        {split.count !== null ? split.count.toLocaleString() : "—"}
                      </span>
                    </div>
                  ))}
                </div>

                {/* Mini bar chart for splits */}
                {splitCounts.some(s => s.count !== null && s.count > 0) && (
                  <div className="mt-2 flex gap-0.5 h-3 rounded-full overflow-hidden bg-gray-100">
                    {splitCounts.map((split, idx) => {
                      const total = splitCounts.reduce((sum, s) => sum + (s.count || 0), 0);
                      const pct = total > 0 ? ((split.count || 0) / total) * 100 : 0;
                      const colors = ["#22c55e", "#3b82f6", "#f59e0b", "#ef4444", "#8b5cf6", "#06b6d4"];
                      return (
                        <div
                          key={idx}
                          className="h-full transition-all"
                          style={{
                            width: `${pct}%`,
                            backgroundColor: colors[idx % colors.length],
                          }}
                          title={`${split.name}: ${split.count?.toLocaleString()} (${pct.toFixed(1)}%)`}
                        />
                      );
                    })}
                  </div>
                )}
              </div>
            )}
          </div>

          <AudienceSummaryPanel />

          {/* Attribute categories reference */}
          <div className="bg-white rounded-lg border border-gray-200 p-4">
            <h3 className="text-sm font-semibold text-gray-700 mb-3">
              Attribute Categories
            </h3>
            <div className="space-y-1.5">
              {Object.entries(CATEGORY_CONFIG).map(([key, config]) => (
                <div
                  key={key}
                  onClick={() => quickAddCondition(key)}
                  className="flex items-center gap-2 text-xs text-gray-600 cursor-pointer hover:bg-indigo-50 hover:text-indigo-700 px-2 py-1 -mx-2 rounded transition-colors group"
                >
                  <div
                    className="w-2 h-2 rounded-full transition-transform group-hover:scale-125"
                    style={{ backgroundColor: config.color }}
                  />
                  <span className="flex-1">{config.label}</span>
                  <svg className="w-3 h-3 opacity-0 group-hover:opacity-100 transition-opacity" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                    <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M12 4v16m8-8H4" />
                  </svg>
                </div>
              ))}
            </div>
          </div>

          {/* Segment templates */}
          <div className="bg-white rounded-lg border border-gray-200 p-4">
            <h3 className="text-sm font-semibold text-gray-700 mb-2">
              Segment Templates
            </h3>

            {/* Filter by business function */}
            <div className="flex flex-wrap gap-1 mb-3">
              {["all", "marketing", "merch", "product", "cx", "finance"].map(
                (fn) => (
                  <button
                    key={fn}
                    onClick={() => setTemplateFilter(fn)}
                    className={`px-2 py-0.5 text-xs rounded-full ${
                      templateFilter === fn
                        ? "bg-indigo-100 text-indigo-700"
                        : "bg-gray-100 text-gray-500 hover:bg-gray-200"
                    }`}
                  >
                    {fn === "all" ? "All" : fn.charAt(0).toUpperCase() + fn.slice(1)}
                  </button>
                )
              )}
            </div>

            <div className="space-y-1.5 max-h-80 overflow-y-auto">
              {templates
                .filter(
                  (t) =>
                    templateFilter === "all" ||
                    t.business_function === templateFilter
                )
                .map((template) => (
                  <button
                    key={template.id}
                    onClick={() => handleLoadTemplate(template)}
                    className="w-full text-left px-3 py-2 text-xs bg-gray-50 hover:bg-indigo-50 hover:text-indigo-600 rounded transition-colors group"
                  >
                    <div className="font-medium text-gray-700 group-hover:text-indigo-600">
                      {template.name}
                    </div>
                    <div className="text-gray-400 mt-0.5 line-clamp-2">
                      {template.description}
                    </div>
                    <div className="flex gap-1 mt-1">
                      <span className="px-1.5 py-0.5 text-[10px] bg-gray-200 text-gray-600 rounded">
                        {template.business_function}
                      </span>
                      <span className="px-1.5 py-0.5 text-[10px] bg-gray-200 text-gray-600 rounded">
                        {template.category}
                      </span>
                    </div>
                  </button>
                ))}

              {templates.filter(
                (t) =>
                  templateFilter === "all" ||
                  t.business_function === templateFilter
              ).length === 0 && (
                <div className="text-xs text-gray-400 text-center py-3">
                  {selectedBrandCode
                    ? "No templates for this brand/filter"
                    : "Select a brand to see templates"}
                </div>
              )}
            </div>
          </div>
        </div>
      </div>
      )}
    </div>
  );
};

/**
 * Simple SVG Venn diagram visual for set operations.
 */
const SetOperationVisual: React.FC<{ operation: SetOperationType }> = ({ operation }) => {
  const getHighlight = () => {
    switch (operation) {
      case "union":
        return { aOpacity: 0.4, bOpacity: 0.4, overlapOpacity: 0.6, label: "A + B" };
      case "overlap":
        return { aOpacity: 0.1, bOpacity: 0.1, overlapOpacity: 0.6, label: "A & B" };
      case "exclude_overlap":
        return { aOpacity: 0.4, bOpacity: 0.1, overlapOpacity: 0.1, label: "A - (A & B)" };
      case "exclude":
        return { aOpacity: 0.4, bOpacity: 0.1, overlapOpacity: 0.1, label: "A - B" };
    }
  };

  const { aOpacity, bOpacity, overlapOpacity, label } = getHighlight();
  const config = SET_OPERATION_LABELS[operation];

  return (
    <div className="flex flex-col items-center gap-1">
      <svg width="120" height="70" viewBox="0 0 120 70">
        {/* Circle A */}
        <circle
          cx="42" cy="35" r="28"
          fill={config.color}
          fillOpacity={aOpacity}
          stroke={config.color}
          strokeWidth="1.5"
        />
        {/* Circle B */}
        <circle
          cx="78" cy="35" r="28"
          fill={config.color}
          fillOpacity={bOpacity}
          stroke={config.color}
          strokeWidth="1.5"
        />
        {/* Overlap highlight (using clip path illusion) */}
        <clipPath id={`clip-a-${operation}`}>
          <circle cx="42" cy="35" r="28" />
        </clipPath>
        <circle
          cx="78" cy="35" r="28"
          fill={config.color}
          fillOpacity={overlapOpacity}
          clipPath={`url(#clip-a-${operation})`}
        />
        {/* Labels */}
        <text x="32" y="38" textAnchor="middle" fontSize="10" fill="#374151" fontWeight="600">A</text>
        <text x="88" y="38" textAnchor="middle" fontSize="10" fill="#374151" fontWeight="600">B</text>
      </svg>
      <span className="text-[10px] font-medium" style={{ color: config.color }}>{label}</span>
    </div>
  );
};

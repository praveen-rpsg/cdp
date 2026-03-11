/**
 * SegmentBuilder — Main segment builder page component.
 *
 * Combines:
 * - Brand selector
 * - Segment metadata (name, type)
 * - Visual rule builder (ConditionGroupUI)
 * - Audience count estimator
 * - SQL preview (for power users)
 * - Save/Publish actions
 */

import React, { useState } from "react";
import { useSegmentStore } from "../../store/segmentStore";
import { CATEGORY_CONFIG } from "../../types/segment";
import { ConditionGroupUI } from "./ConditionGroupUI";

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
    setSegmentName,
    setSegmentDescription,
    setSegmentType,
    setSelectedBrand,
    setIsEstimating,
    setAudienceCount,
    setCompiledSQL,
    resetRules,
    loadRules,
    getSegmentDefinition,
  } = useSegmentStore();

  const [showSQL, setShowSQL] = useState(false);
  const [activeTab, setActiveTab] = useState<"builder" | "json">("builder");
  const [templates, setTemplates] = useState<any[]>([]);
  const [templateFilter, setTemplateFilter] = useState<string>("all");

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
      // Add client-side IDs to the loaded rule tree
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
    if (!selectedBrandCode) return;
    setIsEstimating(true);
    try {
      const response = await fetch("/api/v1/segments/estimate", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          brand_code: selectedBrandCode,
          rules: getSegmentDefinition(),
        }),
      });
      const data = await response.json();
      setAudienceCount(data.estimated_count);
      setCompiledSQL(data.sql);
    } catch (err) {
      console.error("Estimate failed:", err);
    } finally {
      setIsEstimating(false);
    }
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
        <div className="max-w-7xl mx-auto px-6 py-4 flex items-center justify-between">
          <div className="flex items-center gap-4">
            <h1 className="text-lg font-semibold text-gray-900">
              Segment Builder
            </h1>
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

          {/* SQL Preview */}
          {compiledSQL && (
            <div className="bg-white rounded-lg border border-gray-200">
              <button
                onClick={() => setShowSQL(!showSQL)}
                className="w-full px-4 py-3 flex items-center justify-between text-sm font-medium text-gray-700 hover:bg-gray-50"
              >
                <span>Generated Athena SQL</span>
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
                <pre className="p-4 bg-gray-900 text-green-400 text-xs font-mono overflow-x-auto border-t">
                  {compiledSQL}
                </pre>
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
                <div className="text-gray-400 text-sm">Calculating...</div>
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
          </div>

          {/* Attribute categories reference */}
          <div className="bg-white rounded-lg border border-gray-200 p-4">
            <h3 className="text-sm font-semibold text-gray-700 mb-3">
              Attribute Categories
            </h3>
            <div className="space-y-1.5">
              {Object.entries(CATEGORY_CONFIG).map(([key, config]) => (
                <div
                  key={key}
                  className="flex items-center gap-2 text-xs text-gray-600"
                >
                  <div
                    className="w-2 h-2 rounded-full"
                    style={{ backgroundColor: config.color }}
                  />
                  <span>{config.label}</span>
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
    </div>
  );
};

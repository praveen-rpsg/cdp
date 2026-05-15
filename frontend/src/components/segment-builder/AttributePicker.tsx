/**
 * AttributePicker — Searchable, categorized attribute selector.
 *
 * This is the dropdown that appears when a user clicks "Select attribute"
 * in a condition row. It groups attributes by category with search.
 */

import React, { useMemo, useState } from "react";
import { useSegmentStore } from "../../store/segmentStore";
import { CATEGORY_CONFIG, type AttributeDefinition } from "../../types/segment";

interface Props {
  onSelect: (attr: AttributeDefinition) => void;
  onClose: () => void;
  initialCategory?: string;
}

export const AttributePicker: React.FC<Props> = ({
  onSelect,
  onClose,
  initialCategory,
}) => {
  const [search, setSearch] = useState("");
  const { attributeCatalog, selectedBrandCode, isFetchingCatalog, fetchCatalog } = useSegmentStore();

  const filtered = useMemo(() => {
    // Ensure attributeCatalog is an array
    let attrs = Array.isArray(attributeCatalog) ? attributeCatalog : [];
    
    if (selectedBrandCode) {
      attrs = attrs.filter(
        (a) =>
          a.applicable_brands === null ||
          a.applicable_brands.includes(selectedBrandCode)
      );
    }

    // Apply initial category filter if search is empty
    if (initialCategory && !search) {
      attrs = attrs.filter((a) => a.category === initialCategory);
    }

    if (search) {
      const q = search.toLowerCase();
      attrs = attrs.filter(
        (a) =>
          a.label.toLowerCase().includes(q) ||
          a.key.toLowerCase().includes(q) ||
          a.description.toLowerCase().includes(q)
      );
    }
    return attrs;
  }, [attributeCatalog, selectedBrandCode, search, initialCategory]);

  const grouped = useMemo(() => {
    const map = new Map<string, AttributeDefinition[]>();
    for (const attr of filtered) {
      const cat = attr.category;
      if (!map.has(cat)) map.set(cat, []);
      map.get(cat)!.push(attr);
    }
    return map;
  }, [filtered]);

  return (
    <div className="absolute z-50 mt-1 w-96 bg-white border border-gray-200 rounded-lg shadow-xl max-h-96 overflow-hidden flex flex-col">
      {/* Search */}
      <div className="p-3 border-b flex items-center justify-between gap-2">
        <input
          type="text"
          placeholder="Search attributes..."
          value={search}
          onChange={(e) => setSearch(e.target.value)}
          className="flex-1 px-3 py-2 border rounded-md text-sm focus:outline-none focus:ring-2 focus:ring-indigo-500"
          autoFocus
        />
        <button
          onClick={onClose}
          className="p-1 px-2 text-gray-400 hover:text-gray-600 text-sm font-bold"
        >
          ×
        </button>
      </div>

      {/* Categorized list */}
      <div className="overflow-y-auto flex-1 min-h-[100px]">
        {isFetchingCatalog ? (
          <div className="p-12 flex flex-col items-center justify-center gap-3">
            <div className="w-8 h-8 border-4 border-indigo-600 border-t-transparent rounded-full animate-spin" />
            <div className="text-gray-500 text-sm animate-pulse font-medium">
              Loading Attribute Catalog...
            </div>
          </div>
        ) : (
          <>
            {Array.from(grouped.entries()).map(([category, attrs]) => {
              const config = CATEGORY_CONFIG[category as keyof typeof CATEGORY_CONFIG];
              return (
                <div key={category}>
                  <div
                    className="px-3 py-2 text-xs font-semibold text-gray-500 uppercase tracking-wider bg-gray-50 sticky top-0 z-10"
                    style={{ borderLeft: `3px solid ${config?.color || "#999"}` }}
                  >
                    {config?.label || category}
                  </div>
                  {attrs.map((attr) => (
                    <button
                      key={attr.key}
                      onClick={() => {
                        onSelect(attr);
                        onClose();
                      }}
                      className="w-full text-left px-4 py-2 hover:bg-indigo-50 flex items-start gap-2 text-sm transition-colors"
                    >
                      <div className="flex-1">
                        <div className="font-medium text-gray-900">
                          {attr.label}
                        </div>
                        <div className="text-xs text-gray-500 line-clamp-1">
                          {attr.description}
                          {attr.unit && (
                            <span className="ml-1 text-gray-400">
                              ({attr.unit})
                            </span>
                          )}
                        </div>
                      </div>
                      <span className="text-[10px] bg-gray-100 text-gray-500 px-1.5 py-0.5 rounded mt-0.5 uppercase tracking-tighter">
                        {attr.data_type}
                      </span>
                    </button>
                  ))}
                </div>
              );
            })}

            {filtered.length === 0 && (
              <div className="p-10 text-center flex flex-col items-center gap-4">
                <div className="w-12 h-12 bg-gray-100 rounded-full flex items-center justify-center text-gray-400">
                  <svg className="w-6 h-6" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                    <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M21 21l-6-6m2-5a7 7 0 11-14 0 7 7 0 0114 0z" />
                  </svg>
                </div>
                <div>
                  <p className="text-sm font-medium text-gray-900">
                    {search ? "No attributes found" : "Catalog Empty"}
                  </p>
                  <p className="text-xs text-gray-500 mt-1 max-w-[200px] mx-auto">
                    {search 
                      ? `No attributes matching "${search}" were found for the selected brand.`
                      : "We couldn't find any attributes for this brand or they failed to load."}
                  </p>
                </div>
                {!search && (
                  <button
                    onClick={() => fetchCatalog()}
                    className="px-4 py-2 text-xs font-semibold bg-indigo-600 text-white rounded-md hover:bg-indigo-700 transition-colors shadow-sm"
                  >
                    Reload Catalog
                  </button>
                )}
              </div>
            )}
          </>
        )}
      </div>

      {/* Footer */}
      <div className="p-2 border-t bg-gray-50 text-[10px] text-gray-400 text-center flex justify-between px-4 uppercase tracking-widest font-bold">
        <span>{selectedBrandCode || "System-Wide"}</span>
        <span>{filtered.length} attributes</span>
      </div>
    </div>
  );
};

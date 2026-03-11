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
}

export const AttributePicker: React.FC<Props> = ({ onSelect, onClose }) => {
  const [search, setSearch] = useState("");
  const { attributeCatalog, selectedBrandCode } = useSegmentStore();

  const filtered = useMemo(() => {
    let attrs = attributeCatalog;
    if (selectedBrandCode) {
      attrs = attrs.filter(
        (a) =>
          a.applicable_brands === null ||
          a.applicable_brands.includes(selectedBrandCode)
      );
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
  }, [attributeCatalog, selectedBrandCode, search]);

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
      <div className="p-3 border-b">
        <input
          type="text"
          placeholder="Search attributes..."
          value={search}
          onChange={(e) => setSearch(e.target.value)}
          className="w-full px-3 py-2 border rounded-md text-sm focus:outline-none focus:ring-2 focus:ring-indigo-500"
          autoFocus
        />
      </div>

      {/* Categorized list */}
      <div className="overflow-y-auto flex-1">
        {Array.from(grouped.entries()).map(([category, attrs]) => {
          const config = CATEGORY_CONFIG[category as keyof typeof CATEGORY_CONFIG];
          return (
            <div key={category}>
              <div
                className="px-3 py-2 text-xs font-semibold text-gray-500 uppercase tracking-wider bg-gray-50 sticky top-0"
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
                  className="w-full text-left px-4 py-2 hover:bg-indigo-50 flex items-start gap-2 text-sm"
                >
                  <div className="flex-1">
                    <div className="font-medium text-gray-900">
                      {attr.label}
                    </div>
                    <div className="text-xs text-gray-500">
                      {attr.description}
                      {attr.unit && (
                        <span className="ml-1 text-gray-400">
                          ({attr.unit})
                        </span>
                      )}
                    </div>
                  </div>
                  <span className="text-xs text-gray-400 mt-0.5">
                    {attr.data_type}
                  </span>
                </button>
              ))}
            </div>
          );
        })}

        {filtered.length === 0 && (
          <div className="p-6 text-center text-sm text-gray-500">
            No attributes found matching "{search}"
          </div>
        )}
      </div>

      {/* Footer */}
      <div className="p-2 border-t bg-gray-50 text-xs text-gray-500 text-center">
        {filtered.length} attributes available
      </div>
    </div>
  );
};

/**
 * MultiSelectDropdown — Searchable checkbox-based multi-select component.
 *
 * Features:
 * - Typeahead search to filter options
 * - Checkbox-style selection (click to toggle)
 * - Selected values shown as removable chips (up to 3, then "N more")
 * - Select All / Clear All
 * - Click-outside to close
 * - Keyboard accessible
 */

import React, { useState, useRef, useEffect, useMemo } from "react";

interface Props {
  options: string[];
  values: string[];           // currently selected values (array)
  onChange: (values: string[]) => void;
  placeholder?: string;
  disabled?: boolean;
}

export const MultiSelectDropdown: React.FC<Props> = ({
  options,
  values,
  onChange,
  placeholder = "Select values...",
  disabled = false,
}) => {
  const [open, setOpen] = useState(false);
  const [search, setSearch] = useState("");
  const containerRef = useRef<HTMLDivElement>(null);
  const searchRef = useRef<HTMLInputElement>(null);

  // Close on outside click
  useEffect(() => {
    const handler = (e: MouseEvent) => {
      if (containerRef.current && !containerRef.current.contains(e.target as Node)) {
        setOpen(false);
        setSearch("");
      }
    };
    document.addEventListener("mousedown", handler);
    return () => document.removeEventListener("mousedown", handler);
  }, []);

  // Focus search when opened
  useEffect(() => {
    if (open && searchRef.current) {
      searchRef.current.focus();
    }
  }, [open]);

  const filtered = useMemo(() => {
    if (!search) return options;
    const q = search.toLowerCase();
    return options.filter((o) => o.toLowerCase().includes(q));
  }, [options, search]);

  const toggle = (option: string) => {
    const next = values.includes(option)
      ? values.filter((v) => v !== option)
      : [...values, option];
    onChange(next);
  };

  const selectAll = () => {
    const allFiltered = filtered;
    const allSelected = allFiltered.every((o) => values.includes(o));
    if (allSelected) {
      // Deselect all filtered
      onChange(values.filter((v) => !allFiltered.includes(v)));
    } else {
      // Add all filtered that aren't already selected
      const toAdd = allFiltered.filter((o) => !values.includes(o));
      onChange([...values, ...toAdd]);
    }
  };

  const allFilteredSelected = filtered.length > 0 && filtered.every((o) => values.includes(o));

  // Display: show chips for up to 3, then badge for remainder
  const renderTrigger = () => {
    if (values.length === 0) {
      return <span className="text-gray-400">{placeholder}</span>;
    }

    const visible = values.slice(0, 3);
    const remaining = values.length - visible.length;

    return (
      <div className="flex flex-wrap gap-1 min-w-0">
        {visible.map((v) => (
          <span
            key={v}
            className="inline-flex items-center gap-1 px-2 py-0.5 bg-indigo-100 text-indigo-700 text-xs font-medium rounded-full max-w-[120px]"
          >
            <span className="truncate">{v.replace(/_/g, " ")}</span>
            <button
              onClick={(e) => {
                e.stopPropagation();
                toggle(v);
              }}
              className="hover:text-indigo-900 flex-shrink-0"
            >
              <svg className="w-2.5 h-2.5" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={3} d="M6 18L18 6M6 6l12 12" />
              </svg>
            </button>
          </span>
        ))}
        {remaining > 0 && (
          <span className="inline-flex items-center px-2 py-0.5 bg-gray-100 text-gray-600 text-xs font-medium rounded-full">
            +{remaining} more
          </span>
        )}
      </div>
    );
  };

  return (
    <div ref={containerRef} className="relative min-w-[200px] max-w-[380px]">
      {/* Trigger */}
      <button
        type="button"
        disabled={disabled}
        onClick={() => setOpen((o) => !o)}
        className={`w-full flex items-center justify-between gap-2 px-2.5 py-1.5 border rounded-md text-sm bg-white text-left transition-colors ${
          open ? "border-indigo-400 ring-2 ring-indigo-100" : "border-gray-300 hover:border-gray-400"
        } ${disabled ? "opacity-50 cursor-not-allowed" : "cursor-pointer"}`}
      >
        <div className="flex-1 min-w-0">{renderTrigger()}</div>
        <div className="flex items-center gap-1 flex-shrink-0">
          {values.length > 0 && (
            <span className="px-1.5 py-0.5 text-[10px] font-bold bg-indigo-600 text-white rounded-full">
              {values.length}
            </span>
          )}
          <svg
            className={`w-3.5 h-3.5 text-gray-400 transition-transform ${open ? "rotate-180" : ""}`}
            fill="none"
            viewBox="0 0 24 24"
            stroke="currentColor"
          >
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M19 9l-7 7-7-7" />
          </svg>
        </div>
      </button>

      {/* Dropdown */}
      {open && (
        <div className="absolute z-50 mt-1 left-0 min-w-full w-72 bg-white border border-gray-200 rounded-lg shadow-xl flex flex-col max-h-72">
          {/* Search */}
          <div className="p-2 border-b border-gray-100">
            <div className="relative">
              <svg
                className="absolute left-2 top-1/2 -translate-y-1/2 w-3.5 h-3.5 text-gray-400"
                fill="none"
                viewBox="0 0 24 24"
                stroke="currentColor"
              >
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M21 21l-6-6m2-5a7 7 0 11-14 0 7 7 0 0114 0z" />
              </svg>
              <input
                ref={searchRef}
                type="text"
                value={search}
                onChange={(e) => setSearch(e.target.value)}
                placeholder="Search..."
                className="w-full pl-7 pr-3 py-1.5 text-xs border border-gray-200 rounded focus:outline-none focus:border-indigo-400"
              />
            </div>
          </div>

          {/* Select All / Clear All controls */}
          <div className="flex items-center justify-between px-3 py-1.5 border-b border-gray-100 bg-gray-50">
            <button
              onClick={selectAll}
              className="text-xs font-medium text-indigo-600 hover:text-indigo-800"
            >
              {allFilteredSelected ? "Deselect all" : "Select all"}
            </button>
            {values.length > 0 && (
              <button
                onClick={() => onChange([])}
                className="text-xs text-red-500 hover:text-red-700 font-medium"
              >
                Clear all ({values.length})
              </button>
            )}
          </div>

          {/* Options list */}
          <div className="overflow-y-auto flex-1">
            {filtered.length === 0 ? (
              <div className="p-4 text-xs text-gray-400 text-center">No results for "{search}"</div>
            ) : (
              filtered.map((option) => {
                const checked = values.includes(option);
                return (
                  <button
                    key={option}
                    onClick={() => toggle(option)}
                    className={`w-full flex items-center gap-2.5 px-3 py-2 text-sm text-left hover:bg-indigo-50 transition-colors ${
                      checked ? "bg-indigo-50/50" : ""
                    }`}
                  >
                    {/* Checkbox */}
                    <div
                      className={`flex-shrink-0 w-4 h-4 rounded border-2 flex items-center justify-center transition-colors ${
                        checked
                          ? "bg-indigo-600 border-indigo-600"
                          : "border-gray-300 bg-white"
                      }`}
                    >
                      {checked && (
                        <svg className="w-2.5 h-2.5 text-white" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                          <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={3} d="M5 13l4 4L19 7" />
                        </svg>
                      )}
                    </div>
                    <span className="truncate">{option.replace(/_/g, " ")}</span>
                  </button>
                );
              })
            )}
          </div>

          {/* Footer */}
          {values.length > 0 && (
            <div className="px-3 py-1.5 border-t border-gray-100 bg-gray-50 text-xs text-gray-500">
              {values.length} of {options.length} selected
            </div>
          )}
        </div>
      )}
    </div>
  );
};

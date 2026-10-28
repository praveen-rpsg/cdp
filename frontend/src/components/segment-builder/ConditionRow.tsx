/**
 * ConditionRow — A single condition in the segment builder.
 *
 * Renders: [Attribute Picker] [Operator Select] [Value Input] [Remove]
 * Adapts the value input based on the attribute's data type.
 */

import React, { useState, useMemo, useEffect, useRef } from "react";
import { useSegmentStore } from "../../store/segmentStore";
import {
  OPERATOR_LABELS,
  type AttributeCondition,
  type AttributeDefinition,
} from "../../types/segment";
import { AttributePicker } from "./AttributePicker";
import { MultiSelectDropdown } from "./MultiSelectDropdown";

interface Props {
  condition: AttributeCondition;
}

// =============================================================================
// SERVER-SIDE SEARCH ATTRIBUTES (high-cardinality, e.g. phone numbers)
// =============================================================================

/** Minimum digits before the backend is queried. */
const SEARCH_MIN_DIGITS = 8;
/** Max results requested from the backend per search. */
const SEARCH_RESULT_LIMIT = 25;
/** Debounce between the last keystroke and the request. */
const SEARCH_DEBOUNCE_MS = 300;

/**
 * Attribute keys that use the database-backed prefix search instead of the
 * eager "load all values" dropdown.
 *
 * "identity.phone" is the catalog key for "Phone Number" (ATTRIBUTE_CATALOG).
 * Preferred long-term: have the backend attribute catalog expose
 *   value_search: { mode: "prefix", min_chars: 8 }
 * on the attribute, which is checked first below (no key list needed).
 */
const PHONE_SEARCH_ATTRIBUTE_KEYS = new Set<string>(["identity.phone"]);

/**
 * Brands where the phone attribute uses server-side search. Add
 * "spencers" / "natures_basket" here to switch them over too.
 */
const PHONE_SEARCH_BRANDS = new Set<string>(["power_cesc"]);

const getValueSearchConfig = (
  attr: AttributeDefinition | undefined,
  brandCode: string
): { minChars: number } | null => {
  if (!attr) return null;
  const meta = (attr as any).value_search;
  if (meta?.mode === "prefix") {
    return { minChars: Number(meta.min_chars) || SEARCH_MIN_DIGITS };
  }
  if (
    PHONE_SEARCH_ATTRIBUTE_KEYS.has(attr.key) &&
    PHONE_SEARCH_BRANDS.has(brandCode)
  ) {
    return { minChars: SEARCH_MIN_DIGITS };
  }
  return null;
};

export const ConditionRow: React.FC<Props> = ({ condition }) => {
  const [showPicker, setShowPicker] = useState(false);
  const [anchorRect, setAnchorRect] = useState<DOMRect | undefined>();
  const [navCategory, setNavCategory] = useState<string | undefined>(undefined);
  const triggerRef = useRef<HTMLButtonElement>(null);
  const { attributeCatalog, updateCondition, removeCondition, selectedBrandCode } =
    useSegmentStore();

  const selectedAttr = useMemo(
    () => attributeCatalog.find((a) => a.key === condition.attribute_key),
    [attributeCatalog, condition.attribute_key]
  );

  // Handle auto-open when coming from the sidebar categories panel
  useEffect(() => {
    if (condition._initialCategory) {
      setNavCategory(condition._initialCategory);
      if (triggerRef.current) setAnchorRect(triggerRef.current.getBoundingClientRect());
      setShowPicker(true);
      // Strip the transient property so it doesn't re-open or persist
      updateCondition(condition.id, { _initialCategory: undefined } as any);
    }
  }, [condition.id, condition._initialCategory, updateCondition]);

  const handleSelectAttribute = (attr: AttributeDefinition) => {
    const prevAttr = selectedAttr;
    const newOperators = attr.operators || [];

    // Smart replacement: keep operator if still valid for the new attribute
    const prevOperatorStillValid = newOperators.includes(condition.operator);

    // Keep value only if same data type AND operator is still valid
    const prevType = prevAttr?.data_type ?? "";
    const newType = attr.data_type;
    const sameType = prevType === newType;
    const keepValue = prevOperatorStillValid && sameType;

    updateCondition(condition.id, {
      attribute_key: attr.key,
      operator: prevOperatorStillValid ? condition.operator : (newOperators[0] || "equals"),
      value: keepValue ? condition.value : "",
      second_value: keepValue ? condition.second_value : undefined,
      _initialCategory: undefined,
    } as any);
  };
  const handleMultiSelectChange = (newValues: string[]) => {
    // Auto-upgrade operator based on current operator family
    const isNegated = ["not_equals", "not_in_list"].includes(condition.operator);
    const newOperator = isNegated
      ? "not_in_list"
      : "in_list";
    updateCondition(condition.id, { value: newValues, operator: newOperator });
  };

  const availableOperators = selectedAttr?.operators || [];

  const needsValue = ![
    "is_empty",
    "is_not_empty",
    "is_true",
    "is_false",
    "is_today",
    "is_this_week",
    "is_this_month",
    "is_this_quarter",
    "is_this_year",
    "exists",
    "not_exists",
  ].includes(condition.operator);

  const needsSecondValue = ["between", "not_between"].includes(
    condition.operator
  );

  return (
    <div className="flex items-center gap-2 p-3 bg-white rounded-lg border border-gray-200 hover:border-indigo-300 transition-colors group">
      {/* Attribute selector */}
      <div className="flex-shrink-0">
        <button
          ref={triggerRef}
          onClick={() => {
            if (!showPicker && triggerRef.current) {
              setAnchorRect(triggerRef.current.getBoundingClientRect());
            }
            setShowPicker(!showPicker);
          }}
          className="px-3 py-1.5 text-sm border rounded-md bg-gray-50 hover:bg-gray-100 min-w-[180px] text-left truncate"
        >
          {selectedAttr ? (
            <span className="font-medium">{selectedAttr.label}</span>
          ) : (
            <span className="text-gray-400">Select attribute...</span>
          )}
        </button>
        {showPicker && (
          <AttributePicker
            anchorRect={anchorRect}
            initialCategory={navCategory}
            onSelect={handleSelectAttribute}
            onClose={() => setShowPicker(false)}
          />
        )}
      </div>

      {/* Operator selector */}
      <select
        value={condition.operator}
        onChange={(e) =>
          updateCondition(condition.id, { operator: e.target.value })
        }
        className="px-2 py-1.5 text-sm border rounded-md bg-gray-50 min-w-[150px]"
      >
        {availableOperators.map((op) => (
          <option key={op} value={op}>
            {OPERATOR_LABELS[op] || op}
          </option>
        ))}
      </select>

      {/* Value input — adapts to data type */}
      {needsValue && (
        <ValueInput
          condition={condition}
          attr={selectedAttr}
          brandCode={selectedBrandCode ?? "spencers"}
          onChange={(val) => updateCondition(condition.id, { value: val })}
          onMultiSelectChange={handleMultiSelectChange}
        />
      )}

      {/* Second value for "between" operators */}
      {needsSecondValue && (
        <>
          <span className="text-sm text-gray-500">and</span>
          <ValueInput
            condition={condition}
            attr={selectedAttr}
            brandCode={selectedBrandCode ?? "spencers"}
            value={condition.second_value}
            onChange={(val) =>
              updateCondition(condition.id, { second_value: val })
            }
          />
        </>
      )}

      {/* Negate toggle */}
      <button
        onClick={() =>
          updateCondition(condition.id, { negate: !condition.negate })
        }
        className={`px-2 py-1 text-xs rounded border ${
          condition.negate
            ? "bg-red-50 border-red-300 text-red-600"
            : "bg-gray-50 border-gray-200 text-gray-400"
        }`}
        title="Negate this condition"
      >
        NOT
      </button>

      {/* Remove */}
      <button
        onClick={() => removeCondition(condition.id)}
        className="p-1.5 text-gray-400 hover:text-red-500 opacity-0 group-hover:opacity-100 transition-opacity"
        title="Remove condition"
      >
        <svg className="w-4 h-4" fill="none" viewBox="0 0 24 24" stroke="currentColor">
          <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M6 18L18 6M6 6l12 12" />
        </svg>
      </button>
    </div>
  );
};

// =============================================================================
// SEARCHABLE VALUE PICKER — database-backed prefix search (phone numbers)
// =============================================================================

interface SearchableValuePickerProps {
  attributeKey: string;
  brandCode: string;
  minChars: number;
  values: string[];
  onChange: (values: string[]) => void;
}

const SearchableValuePicker: React.FC<SearchableValuePickerProps> = ({
  attributeKey,
  brandCode,
  minChars,
  values,
  onChange,
}) => {
  const [text, setText] = useState("");
  const [results, setResults] = useState<string[]>([]);
  const [loading, setLoading] = useState(false);
  const [failed, setFailed] = useState(false);
  const [open, setOpen] = useState(false);
  const [menuRect, setMenuRect] = useState<DOMRect | null>(null);

  const wrapperRef = useRef<HTMLDivElement>(null);
  const menuRef = useRef<HTMLDivElement>(null);
  const inputRef = useRef<HTMLInputElement>(null);

  const digits = text.replace(/\D/g, "");
  const searchActive = digits.length >= minChars;
  const showMenu = open && searchActive;

  // Debounced server search. Cleanup aborts the in-flight request and cancels
  // the timer, so an older response can never overwrite a newer search.
  useEffect(() => {
    if (!searchActive) {
      setResults([]);
      setLoading(false);
      setFailed(false);
      return;
    }

    setLoading(true);
    setFailed(false);

    const controller = new AbortController();

    const timer = setTimeout(async () => {
      try {
        const res = await fetch(
          `/api/v1/segments/attributes/${encodeURIComponent(
            attributeKey
          )}/values?brand_code=${encodeURIComponent(
            brandCode
          )}&search=${encodeURIComponent(digits)}&limit=${SEARCH_RESULT_LIMIT}`,
          { signal: controller.signal }
        );
        if (!res.ok) throw new Error(`HTTP ${res.status}`);
        const data = await res.json();
        const list: string[] = (data.values || []).map((v: any) => String(v));

        // Defensive: only keep values that actually start with what was typed.
        setResults(list.filter((v) => v.startsWith(digits)));
      } catch {
        if (controller.signal.aborted) return;
        setResults([]);
        setFailed(true);
      } finally {
        if (!controller.signal.aborted) setLoading(false);
      }
    }, SEARCH_DEBOUNCE_MS);

    return () => {
      clearTimeout(timer);
      controller.abort();
    };
  }, [digits, searchActive, attributeKey, brandCode]);

  // Keep the (fixed-position) menu aligned with the input.
  useEffect(() => {
    if (!showMenu) return;
    const update = () => {
      if (wrapperRef.current) {
        setMenuRect(wrapperRef.current.getBoundingClientRect());
      }
    };
    update();
    window.addEventListener("resize", update);
    window.addEventListener("scroll", update, true);
    return () => {
      window.removeEventListener("resize", update);
      window.removeEventListener("scroll", update, true);
    };
  }, [showMenu, values.length]);

  // Close on outside click.
  useEffect(() => {
    const onDown = (e: MouseEvent) => {
      const t = e.target as Node;
      if (wrapperRef.current?.contains(t) || menuRef.current?.contains(t)) return;
      setOpen(false);
    };
    document.addEventListener("mousedown", onDown);
    return () => document.removeEventListener("mousedown", onDown);
  }, []);

  const selectable = results.filter((r) => !values.includes(r));

  const addValue = (v: string) => {
    if (!values.includes(v)) onChange([...values, v]);
    setText("");
    setOpen(false);
    inputRef.current?.focus();
  };

  const removeValue = (v: string) => onChange(values.filter((x) => x !== v));

  return (
    <div ref={wrapperRef} className="relative flex-1 min-w-[240px]">
      <div
        className="flex flex-wrap items-center gap-1 px-2 py-1 text-sm border rounded-md bg-white focus-within:ring-2 focus-within:ring-indigo-500 cursor-text"
        onClick={() => inputRef.current?.focus()}
      >
        {values.map((v) => (
          <span
            key={v}
            className="inline-flex items-center gap-1 px-2 py-0.5 text-xs font-mono bg-indigo-50 text-indigo-700 border border-indigo-200 rounded-full"
          >
            {v}
            <button
              type="button"
              onClick={(e) => {
                e.stopPropagation();
                removeValue(v);
              }}
              className="text-indigo-400 hover:text-indigo-700 leading-none"
              title="Remove"
            >
              ×
            </button>
          </span>
        ))}

        <input
          ref={inputRef}
          type="text"
          inputMode="numeric"
          value={text}
          onChange={(e) => {
            setText(e.target.value.replace(/\D/g, ""));
            setOpen(true);
          }}
          onFocus={() => setOpen(true)}
          onKeyDown={(e) => {
            if (e.key === "Escape") setOpen(false);
            if (e.key === "Backspace" && text === "" && values.length > 0) {
              removeValue(values[values.length - 1]);
            }
            if (e.key === "Enter" && selectable.length === 1) {
              e.preventDefault();
              addValue(selectable[0]);
            }
          }}
          placeholder={
            values.length === 0 ? `Type ${minChars}+ digits to search...` : ""
          }
          className="flex-1 min-w-[120px] py-0.5 text-sm font-mono outline-none bg-transparent"
        />
      </div>

      {showMenu && menuRect && (
        <div
          ref={menuRef}
          style={{
            position: "fixed",
            top: menuRect.bottom + 4,
            left: menuRect.left,
            width: Math.max(menuRect.width, 240),
            zIndex: 1000,
          }}
          className="max-h-60 overflow-y-auto bg-white border border-gray-200 rounded-md shadow-lg"
        >
          {loading ? (
            <div className="flex items-center gap-2 px-3 py-2 text-sm text-gray-400">
              <div className="w-3.5 h-3.5 border-2 border-indigo-400 border-t-transparent rounded-full animate-spin flex-shrink-0" />
              <span>Searching...</span>
            </div>
          ) : failed ? (
            <div className="px-3 py-2 text-sm text-red-500">
              Search failed. Keep typing to retry.
            </div>
          ) : selectable.length === 0 ? (
            <div className="px-3 py-2 text-sm text-gray-400">
              No matching values
            </div>
          ) : (
            selectable.map((r) => (
              <button
                key={r}
                type="button"
                onMouseDown={(e) => e.preventDefault()}
                onClick={() => addValue(r)}
                className="block w-full px-3 py-1.5 text-sm font-mono text-left hover:bg-indigo-50"
              >
                {r}
              </button>
            ))
          )}
        </div>
      )}
    </div>
  );
};

// =============================================================================
// VALUE INPUT — type-adaptive input component
// =============================================================================

interface ValueInputProps {
  condition: AttributeCondition;
  attr: AttributeDefinition | undefined;
  value?: any;
  brandCode?: string;
  onChange: (value: any) => void;
  onMultiSelectChange?: (values: string[]) => void;
}

const ValueInput: React.FC<ValueInputProps> = ({
  condition,
  attr,
  value,
  brandCode = "spencers",
  onChange,
  onMultiSelectChange,
}) => {
  const val = value ?? condition.value;
  const dataType = attr?.data_type || "string";

  // Dynamic values state — fetched from the DB when attr has a source_table
  const [dynamicOptions, setDynamicOptions] = useState<string[] | null>(null);
  const [loadingOptions, setLoadingOptions] = useState(false);

  const isMultiSelectOperator = ["equals", "not_equals", "in_list", "not_in_list"].includes(condition.operator);
  const hasDynamicSource = !!(attr?.source_table);
  const hasExampleValues = !!(attr?.example_values && attr.example_values.length > 0 && !Array.isArray(attr.example_values[0]));

  // High-cardinality attributes (phone number) use server-side search instead
  // of the eager "load all values" dropdown.
  const searchConfig = getValueSearchConfig(attr, brandCode);
  const useServerSearch =
    !!searchConfig &&
    (attr?.data_type === "string" || !attr?.data_type) &&
    isMultiSelectOperator;

  const shouldShowDropdown =
    !useServerSearch &&
    (attr?.data_type === "string" || !attr?.data_type) &&
    isMultiSelectOperator &&
    (hasDynamicSource || hasExampleValues);

  // Re-fetch whenever the attribute OR the brand changes
  useEffect(() => {
    if (!attr?.source_table || !shouldShowDropdown) return;

    setLoadingOptions(true);
    setDynamicOptions(null);

    fetch(
      `/api/v1/segments/attributes/${encodeURIComponent(attr.key)}/values?limit=2000&brand_code=${encodeURIComponent(brandCode)}`
    )
      .then((r) => r.json())
      .then((data) => {
        setDynamicOptions(data.values || []);
      })
      .catch(() => {
        setDynamicOptions(null);
      })
      .finally(() => setLoadingOptions(false));
  }, [attr?.key, attr?.source_table, shouldShowDropdown, brandCode]); // brandCode in deps → refetch on brand switch

  // Phone number → database-backed prefix search (no eager load)
  if (useServerSearch && attr && searchConfig) {
    const currentValues = Array.isArray(val)
      ? val.map((x: any) => String(x))
      : val && String(val).trim() !== ""
      ? [String(val)]
      : [];

    return (
      <SearchableValuePicker
        attributeKey={attr.key}
        brandCode={brandCode}
        minChars={searchConfig.minChars}
        values={currentValues}
        onChange={onMultiSelectChange || onChange}
      />
    );
  }

  // Categorical attributes → MultiSelectDropdown (dynamic or static)
  if (shouldShowDropdown) {
    if (loadingOptions) {
      return (
        <div className="flex items-center gap-2 px-3 py-1.5 text-sm text-gray-400 border rounded-md bg-gray-50 min-w-[200px]">
          <div className="w-3.5 h-3.5 border-2 border-indigo-400 border-t-transparent rounded-full animate-spin flex-shrink-0" />
          <span>Loading values...</span>
        </div>
      );
    }

    // Prefer dynamically fetched options, fall back to example_values
    const options = dynamicOptions ?? attr!.example_values.map((ev: any) => String(ev));
    const currentValues = Array.isArray(val)
      ? val
      : val && String(val).trim() !== ""
      ? [String(val)]
      : [];

    return (
      <MultiSelectDropdown
        options={options}
        values={currentValues}
        onChange={onMultiSelectChange || onChange}
        placeholder={`Select values... (${options.length} available)`}
      />
    );
  }

  // Legacy multi-value text input for "in_list" / "not_in_list" on non-enum attributes
  if (["in_list", "not_in_list", "contains_any", "contains_all"].includes(condition.operator)) {
    return (
      <input
        type="text"
        value={Array.isArray(val) ? val.join(", ") : val}
        onChange={(e) =>
          onChange(
            e.target.value
              .split(",")
              .map((s) => s.trim())
              .filter(Boolean)
          )
        }
        placeholder="value1, value2, ..."
        className="px-2 py-1.5 text-sm border rounded-md flex-1 min-w-[150px]"
      />
    );
  }

  // Days input for "in_last_n_days" etc.
  if (["in_last_n_days", "not_in_last_n_days", "in_next_n_days"].includes(condition.operator)) {
    return (
      <div className="flex items-center gap-1">
        <input
          type="number"
          value={val}
          onChange={(e) => onChange(parseInt(e.target.value) || 0)}
          className="px-2 py-1.5 text-sm border rounded-md w-20"
          min={1}
        />
        <span className="text-sm text-gray-500">days</span>
      </div>
    );
  }

  // Boolean — no value input needed for is_true/is_false
  if (dataType === "boolean") {
    return null;
  }

  // Date input
  if (dataType === "date" || dataType === "datetime") {
    return (
      <input
        type="date"
        value={val}
        onChange={(e) => onChange(e.target.value)}
        className="px-2 py-1.5 text-sm border rounded-md"
      />
    );
  }

  // Number input
  if (dataType === "integer" || dataType === "float") {
    return (
      <input
        type="number"
        value={val}
        onChange={(e) => {
          const parsed =
            dataType === "integer"
              ? parseInt(e.target.value)
              : parseFloat(e.target.value);
          onChange(isNaN(parsed) ? "" : parsed);
        }}
        className="px-2 py-1.5 text-sm border rounded-md w-32"
        step={dataType === "float" ? "0.01" : "1"}
      />
    );
  }

  // Default: text input
  return (
    <input
      type="text"
      value={Array.isArray(val) ? val.join(", ") : val}
      onChange={(e) => onChange(e.target.value)}
      placeholder="Enter value..."
      className="px-2 py-1.5 text-sm border rounded-md flex-1 min-w-[150px]"
    />
  );
};
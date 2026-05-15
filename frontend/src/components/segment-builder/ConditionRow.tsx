/**
 * ConditionRow — A single condition in the segment builder.
 *
 * Renders: [Attribute Picker] [Operator Select] [Value Input] [Remove]
 * Adapts the value input based on the attribute's data type.
 */

import React, { useState, useMemo, useEffect } from "react";
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

export const ConditionRow: React.FC<Props> = ({ condition }) => {
  const [showPicker, setShowPicker] = useState(false);
  const [navCategory, setNavCategory] = useState<string | undefined>(undefined);
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
      <div className="relative flex-shrink-0">
        <button
          onClick={() => setShowPicker(!showPicker)}
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
// VALUE INPUT — type-adaptive input component
// =============================================================================

interface ValueInputProps {
  condition: AttributeCondition;
  attr: AttributeDefinition | undefined;
  value?: any;
  onChange: (value: any) => void;
  onMultiSelectChange?: (values: string[]) => void;
}

const ValueInput: React.FC<ValueInputProps> = ({
  condition,
  attr,
  value,
  onChange,
  onMultiSelectChange,
}) => {
  const val = value ?? condition.value;
  const dataType = attr?.data_type || "string";
  const { selectedBrandCode } = useSegmentStore();

  // Dynamic values state — fetched from the DB when attr has a source_table
  const [dynamicOptions, setDynamicOptions] = useState<string[] | null>(null);
  const [loadingOptions, setLoadingOptions] = useState(false);

  const isMultiSelectOperator = ["equals", "not_equals", "in_list", "not_in_list"].includes(condition.operator);
  const hasDynamicSource = !!(attr?.source_table);
  const hasExampleValues = !!(attr?.example_values && attr.example_values.length > 0 && !Array.isArray(attr.example_values[0]));
  const shouldShowDropdown = (attr?.data_type === "string" || !attr?.data_type) && isMultiSelectOperator && (hasDynamicSource || hasExampleValues);

  // Fetch dynamic values from the backend when the attribute changes and it has a source_table
  useEffect(() => {
    if (!attr?.source_table || !shouldShowDropdown) return;

    setLoadingOptions(true);
    setDynamicOptions(null);

    const brandParam = selectedBrandCode ? `&brand_code=${encodeURIComponent(selectedBrandCode)}` : "";
    fetch(`/api/v1/segments/attributes/${encodeURIComponent(attr.key)}/values?limit=2000${brandParam}`)
      .then((r) => r.json())
      .then((data) => {
        setDynamicOptions(data.values || []);
      })
      .catch(() => {
        setDynamicOptions(null);
      })
      .finally(() => setLoadingOptions(false));
  }, [attr?.key, attr?.source_table, shouldShowDropdown, selectedBrandCode]);

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

    // Prefer dynamically fetched options, fall back to example_values when DB returns empty
    const options =
      dynamicOptions && dynamicOptions.length > 0
        ? dynamicOptions
        : attr!.example_values.map((ev: any) => String(ev)).filter(Boolean);
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

/**
 * ConditionRow — A single condition in the segment builder.
 *
 * Renders: [Attribute Picker] [Operator Select] [Value Input] [Remove]
 * Adapts the value input based on the attribute's data type.
 */

import React, { useState, useMemo } from "react";
import { useSegmentStore } from "../../store/segmentStore";
import {
  OPERATOR_LABELS,
  type AttributeCondition,
  type AttributeDefinition,
} from "../../types/segment";
import { AttributePicker } from "./AttributePicker";

interface Props {
  condition: AttributeCondition;
}

export const ConditionRow: React.FC<Props> = ({ condition }) => {
  const [showPicker, setShowPicker] = useState(false);
  const { attributeCatalog, updateCondition, removeCondition } =
    useSegmentStore();

  const selectedAttr = useMemo(
    () => attributeCatalog.find((a) => a.key === condition.attribute_key),
    [attributeCatalog, condition.attribute_key]
  );

  const handleSelectAttribute = (attr: AttributeDefinition) => {
    updateCondition(condition.id, {
      attribute_key: attr.key,
      operator: attr.operators[0] || "equals",
      value: "",
      second_value: undefined,
    });
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
}

const ValueInput: React.FC<ValueInputProps> = ({
  condition,
  attr,
  value,
  onChange,
}) => {
  const val = value ?? condition.value;
  const dataType = attr?.data_type || "string";

  // Multi-value input for "in_list" / "not_in_list"
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

  // Enum / example values dropdown
  if (attr?.example_values && attr.example_values.length > 0 && !Array.isArray(attr.example_values[0])) {
    return (
      <select
        value={val}
        onChange={(e) => onChange(e.target.value)}
        className="px-2 py-1.5 text-sm border rounded-md min-w-[130px]"
      >
        <option value="">Select...</option>
        {attr.example_values.map((ev: any) => (
          <option key={String(ev)} value={ev}>
            {String(ev).replace(/_/g, " ")}
          </option>
        ))}
      </select>
    );
  }

  // Default: text input
  return (
    <input
      type="text"
      value={val}
      onChange={(e) => onChange(e.target.value)}
      placeholder="Enter value..."
      className="px-2 py-1.5 text-sm border rounded-md flex-1 min-w-[150px]"
    />
  );
};

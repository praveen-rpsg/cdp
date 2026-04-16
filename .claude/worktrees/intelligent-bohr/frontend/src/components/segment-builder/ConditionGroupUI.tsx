/**
 * ConditionGroupUI — Recursive group renderer for the segment builder.
 *
 * Renders a group of conditions joined by AND/OR with:
 * - Toggle between AND/OR
 * - Add condition / Add nested group buttons
 * - Visual nesting with indentation and colored borders
 */

import React from "react";
import { useSegmentStore } from "../../store/segmentStore";
import type { ConditionGroup } from "../../types/segment";
import { ConditionRow } from "./ConditionRow";

interface Props {
  group: ConditionGroup;
  depth?: number;
  isRoot?: boolean;
}

const DEPTH_COLORS = [
  "border-indigo-400",
  "border-emerald-400",
  "border-amber-400",
  "border-rose-400",
  "border-cyan-400",
];

const DEPTH_BG = [
  "bg-indigo-50/30",
  "bg-emerald-50/30",
  "bg-amber-50/30",
  "bg-rose-50/30",
  "bg-cyan-50/30",
];

export const ConditionGroupUI: React.FC<Props> = ({
  group,
  depth = 0,
  isRoot = false,
}) => {
  const { addCondition, addGroup, toggleGroupOperator, removeCondition } =
    useSegmentStore();

  const borderColor = DEPTH_COLORS[depth % DEPTH_COLORS.length];
  const bgColor = DEPTH_BG[depth % DEPTH_BG.length];

  return (
    <div
      className={`relative rounded-lg border-l-4 ${borderColor} ${bgColor} ${
        isRoot ? "p-4" : "p-3 ml-4"
      }`}
    >
      {/* Group header */}
      <div className="flex items-center gap-2 mb-3">
        {/* AND/OR toggle */}
        <button
          onClick={() => toggleGroupOperator(group.id)}
          className={`px-3 py-1 text-xs font-bold rounded-full transition-colors ${
            group.logical_operator === "and"
              ? "bg-indigo-100 text-indigo-700 hover:bg-indigo-200"
              : "bg-amber-100 text-amber-700 hover:bg-amber-200"
          }`}
        >
          {group.logical_operator.toUpperCase()}
        </button>

        <span className="text-xs text-gray-500">
          {group.logical_operator === "and"
            ? "All conditions must match"
            : "Any condition can match"}
        </span>

        <div className="flex-1" />

        {/* Remove group (not root) */}
        {!isRoot && (
          <button
            onClick={() => removeCondition(group.id)}
            className="p-1 text-gray-400 hover:text-red-500 text-xs"
            title="Remove group"
          >
            Remove group
          </button>
        )}
      </div>

      {/* Conditions */}
      <div className="space-y-2">
        {group.conditions.map((condition, index) => (
          <React.Fragment key={condition.id}>
            {index > 0 && (
              <div className="flex items-center gap-2 py-1 pl-2">
                <span
                  className={`text-xs font-semibold ${
                    group.logical_operator === "and"
                      ? "text-indigo-500"
                      : "text-amber-500"
                  }`}
                >
                  {group.logical_operator.toUpperCase()}
                </span>
                <div className="flex-1 border-t border-dashed border-gray-200" />
              </div>
            )}

            {condition.type === "group" ? (
              <ConditionGroupUI
                group={condition as ConditionGroup}
                depth={depth + 1}
              />
            ) : condition.type === "attribute" ? (
              <ConditionRow condition={condition} />
            ) : (
              <div className="p-3 bg-white rounded border border-gray-200 text-sm text-gray-500">
                {condition.type} condition (coming soon)
              </div>
            )}
          </React.Fragment>
        ))}

        {group.conditions.length === 0 && (
          <div className="text-center py-6 text-sm text-gray-400">
            No conditions yet. Add a condition to start building your segment.
          </div>
        )}
      </div>

      {/* Add buttons */}
      <div className="flex items-center gap-2 mt-3 pt-3 border-t border-dashed border-gray-200">
        <button
          onClick={() => addCondition(group.id)}
          className="flex items-center gap-1 px-3 py-1.5 text-sm font-medium text-indigo-600 bg-indigo-50 hover:bg-indigo-100 rounded-md transition-colors"
        >
          <svg className="w-4 h-4" fill="none" viewBox="0 0 24 24" stroke="currentColor">
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M12 4v16m8-8H4" />
          </svg>
          Add Condition
        </button>

        <button
          onClick={() => addGroup(group.id)}
          className="flex items-center gap-1 px-3 py-1.5 text-sm font-medium text-gray-600 bg-gray-50 hover:bg-gray-100 rounded-md transition-colors"
        >
          <svg className="w-4 h-4" fill="none" viewBox="0 0 24 24" stroke="currentColor">
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M4 6h16M4 12h16M4 18h7" />
          </svg>
          Add Nested Group
        </button>

        <button
          onClick={() =>
            addCondition(group.id, {
              type: "event",
              id: `evt_${Date.now()}`,
              event_name: "",
              operator: "has_performed",
              time_window: { type: "all_time" },
              negate: false,
            })
          }
          className="flex items-center gap-1 px-3 py-1.5 text-sm font-medium text-emerald-600 bg-emerald-50 hover:bg-emerald-100 rounded-md transition-colors"
        >
          <svg className="w-4 h-4" fill="none" viewBox="0 0 24 24" stroke="currentColor">
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M13 10V3L4 14h7v7l9-11h-7z" />
          </svg>
          Add Event Condition
        </button>
      </div>
    </div>
  );
};

/**
 * ConditionGroupUI — Recursive group renderer with drag-and-drop reordering.
 *
 * Features:
 * - Drag-and-drop reordering via @dnd-kit/sortable
 * - Insert-between: hover to reveal "+" button between conditions
 * - Add condition at any position (not just bottom)
 * - AND/OR toggle
 * - Nested group support with visual depth indentation
 */

import React, { useState, useMemo } from "react";
import {
  DndContext,
  closestCenter,
  KeyboardSensor,
  PointerSensor,
  useSensor,
  useSensors,
  type DragEndEvent,
} from "@dnd-kit/core";
import {
  SortableContext,
  sortableKeyboardCoordinates,
  verticalListSortingStrategy,
} from "@dnd-kit/sortable";
import { restrictToVerticalAxis, restrictToParentElement } from "@dnd-kit/modifiers";

import { useSegmentStore } from "../../store/segmentStore";
import type { AttributeCondition, ConditionGroup } from "../../types/segment";
import { SortableConditionRow } from "./SortableConditionRow";

interface Props {
  group: ConditionGroup;
  depth?: number;
  isRoot?: boolean;
}

const DEPTH_BORDER = [
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

const OPERATOR_COLORS = {
  and: { badge: "bg-indigo-100 text-indigo-700 hover:bg-indigo-200", label: "text-indigo-400", hint: "All conditions must match" },
  or:  { badge: "bg-amber-100 text-amber-700 hover:bg-amber-200",   label: "text-amber-400",  hint: "Any condition can match" },
};

export const ConditionGroupUI: React.FC<Props> = ({
  group,
  depth = 0,
  isRoot = false,
}) => {
  const {
    addCondition,
    insertConditionAt,
    reorderConditions,
    addGroup,
    toggleGroupOperator,
    removeCondition,
  } = useSegmentStore();

  const [hoveredInsert, setHoveredInsert] = useState<number | null>(null);

  const borderColor = DEPTH_BORDER[depth % DEPTH_BORDER.length];
  const bgColor = DEPTH_BG[depth % DEPTH_BG.length];
  const opColors = OPERATOR_COLORS[group.logical_operator];

  // Only attribute conditions are sortable (groups are not draggable in this version)
  const sortableIds = useMemo(() => 
    group.conditions
      .filter((c) => c.type === "attribute")
      .map((c) => c.id),
    [group.conditions]
  );

  const sensors = useSensors(
    useSensor(PointerSensor, { activationConstraint: { distance: 4 } }),
    useSensor(KeyboardSensor, { coordinateGetter: sortableKeyboardCoordinates })
  );

  const handleDragEnd = (event: DragEndEvent) => {
    const { active, over } = event;
    if (!over || active.id === over.id) return;

    const fromIndex = group.conditions.findIndex((c) => c.id === active.id);
    const toIndex = group.conditions.findIndex((c) => c.id === over.id);

    if (fromIndex !== -1 && toIndex !== -1) {
      reorderConditions(group.id, fromIndex, toIndex);
    }
  };

  return (
    <div
      className={`relative rounded-xl border-l-4 ${borderColor} ${bgColor} ${
        isRoot ? "p-4" : "p-3 ml-4"
      }`}
    >
      {/* ── Group header ── */}
      <div className="flex items-center gap-2 mb-3">
        <button
          onClick={() => toggleGroupOperator(group.id)}
          className={`px-3 py-1 text-xs font-bold rounded-full transition-all ${opColors.badge}`}
          title="Toggle AND / OR"
        >
          {group.logical_operator.toUpperCase()}
        </button>
        <span className="text-xs text-gray-400">{opColors.hint}</span>
        <div className="flex-1" />
        {!isRoot && (
          <button
            onClick={() => removeCondition(group.id)}
            className="text-xs text-gray-400 hover:text-red-500 px-2 py-1 rounded hover:bg-red-50 transition-colors"
            title="Remove group"
          >
            Remove group
          </button>
        )}
      </div>

      {/* ── Conditions list with DnD ── */}
      <DndContext
        id={group.id}
        sensors={sensors}
        collisionDetection={closestCenter}
        modifiers={[restrictToVerticalAxis, restrictToParentElement]}
        onDragEnd={handleDragEnd}
      >
        <SortableContext items={sortableIds} strategy={verticalListSortingStrategy}>
          <div className="space-y-0">
            {group.conditions.length === 0 && (
              <div className="text-center py-8 text-sm text-gray-400 border-2 border-dashed border-gray-200 rounded-lg">
                No conditions yet. Add one below or click a category from the right panel.
              </div>
            )}

            {group.conditions.map((condition, index) => (
              <React.Fragment key={condition.id}>
                {index > 0 && (
                  <div
                    className="relative flex items-center gap-2 py-1"
                    onMouseEnter={() => setHoveredInsert(index)}
                    onMouseLeave={() => setHoveredInsert(null)}
                  >
                    <button
                      onClick={() => {
                        const condOpKey = condition.type === "group" ? "logical_operator_prefix" : "logical_operator";
                        const currentOp = (condition as any)[condOpKey] || group.logical_operator;
                        const nextOp = currentOp === "and" ? "or" : "and";
                        useSegmentStore.getState().updateCondition(condition.id, { [condOpKey]: nextOp });
                      }}
                      className={`px-2 py-0.5 text-[10px] font-bold rounded flex-shrink-0 transition-colors z-10 ${
                        ((condition.type === "group" ? (condition as ConditionGroup).logical_operator_prefix : (condition as any).logical_operator) || group.logical_operator) === "and"
                          ? "bg-indigo-100 text-indigo-700 hover:bg-indigo-200"
                          : "bg-amber-100 text-amber-700 hover:bg-amber-200"
                      }`}
                      title="Toggle AND / OR for this condition"
                    >
                      {((condition.type === "group" ? (condition as ConditionGroup).logical_operator_prefix : (condition as any).logical_operator) || group.logical_operator).toUpperCase()}
                    </button>
                    <div className="flex-1 border-t border-dashed border-gray-200" />

                    <button
                      onClick={() => insertConditionAt(group.id, index)}
                      className={`absolute right-0 flex items-center gap-1 px-2 py-0.5 text-[11px] font-medium text-indigo-600 bg-white border border-indigo-300 rounded-full shadow-sm hover:bg-indigo-50 transition-all ${
                        hoveredInsert === index
                          ? "opacity-100 translate-y-0"
                          : "opacity-0 pointer-events-none"
                      }`}
                      title={`Insert condition at position ${index + 1}`}
                    >
                      <svg className="w-3 h-3" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                        <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2.5} d="M12 4v16m8-8H4" />
                      </svg>
                      Insert here
                    </button>
                  </div>
                )}

                {condition.type === "group" ? (
                  <ConditionGroupUI
                    group={condition as ConditionGroup}
                    depth={depth + 1}
                  />
                ) : condition.type === "attribute" ? (
                  <SortableConditionRow condition={condition as AttributeCondition} />
                ) : (
                  <div className="p-3 bg-white rounded-lg border border-gray-200 text-sm text-gray-400 ml-5">
                    {condition.type} condition (coming soon)
                  </div>
                )}
              </React.Fragment>
            ))}
          </div>
        </SortableContext>
      </DndContext>

      {/* ── Toolbar ── */}
      <div className="flex items-center gap-2 mt-3 pt-3 border-t border-dashed border-gray-200 flex-wrap">
        <button
          onClick={() => addCondition(group.id)}
          className="flex items-center gap-1.5 px-3 py-1.5 text-sm font-medium text-indigo-600 bg-indigo-50 hover:bg-indigo-100 rounded-md transition-colors"
        >
          <svg className="w-3.5 h-3.5" fill="none" viewBox="0 0 24 24" stroke="currentColor">
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M12 4v16m8-8H4" />
          </svg>
          Add Condition
        </button>

        <button
          onClick={() => addGroup(group.id)}
          className="flex items-center gap-1.5 px-3 py-1.5 text-sm font-medium text-gray-600 bg-gray-50 hover:bg-gray-100 rounded-md transition-colors"
        >
          <svg className="w-3.5 h-3.5" fill="none" viewBox="0 0 24 24" stroke="currentColor">
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
          className="flex items-center gap-1.5 px-3 py-1.5 text-sm font-medium text-emerald-600 bg-emerald-50 hover:bg-emerald-100 rounded-md transition-colors"
        >
          <svg className="w-3.5 h-3.5" fill="none" viewBox="0 0 24 24" stroke="currentColor">
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M13 10V3L4 14h7v7l9-11h-7z" />
          </svg>
          Add Event Condition
        </button>
      </div>
    </div>
  );
};

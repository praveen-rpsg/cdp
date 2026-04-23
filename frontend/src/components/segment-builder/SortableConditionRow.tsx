/**
 * SortableConditionRow — Wraps ConditionRow with @dnd-kit/sortable.
 *
 * Kept as a thin wrapper so ConditionRow stays clean. Provides:
 * - Drag handle (⠿ gripper)
 * - CSS transform during drag for smooth animation
 * - isDragging visual feedback (opacity + ring)
 */

import React from "react";
import { useSortable } from "@dnd-kit/sortable";
import { CSS } from "@dnd-kit/utilities";
import type { AttributeCondition } from "../../types/segment";
import { ConditionRow } from "./ConditionRow";

interface Props {
  condition: AttributeCondition;
}

export const SortableConditionRow: React.FC<Props> = ({ condition }) => {
  const {
    attributes,
    listeners,
    setNodeRef,
    setActivatorNodeRef,
    transform,
    transition,
    isDragging,
  } = useSortable({ id: condition.id });

  const style: React.CSSProperties = {
    transform: CSS.Transform.toString(transform),
    transition,
    opacity: isDragging ? 0.4 : 1,
    zIndex: isDragging ? 50 : undefined,
  };

  return (
    <div ref={setNodeRef} style={style} {...attributes}>
      <div className={`flex items-center gap-1 ${isDragging ? "ring-2 ring-indigo-400 rounded-lg" : ""}`}>
        {/* Drag handle */}
        <button
          ref={setActivatorNodeRef}
          {...listeners}
          className="p-1.5 text-gray-300 hover:text-gray-500 cursor-grab active:cursor-grabbing flex-shrink-0 touch-none"
          title="Drag to reorder"
          tabIndex={-1}
        >
          <svg width="12" height="16" viewBox="0 0 12 20" fill="currentColor">
            <circle cx="4" cy="4" r="1.5" />
            <circle cx="4" cy="10" r="1.5" />
            <circle cx="4" cy="16" r="1.5" />
            <circle cx="9" cy="4" r="1.5" />
            <circle cx="9" cy="10" r="1.5" />
            <circle cx="9" cy="16" r="1.5" />
          </svg>
        </button>

        {/* Condition row takes the rest */}
        <div className="flex-1 min-w-0">
          <ConditionRow condition={condition} />
        </div>
      </div>
    </div>
  );
};

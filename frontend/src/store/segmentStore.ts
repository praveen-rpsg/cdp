/**
 * Segment Builder Store (Zustand)
 *
 * Manages the state of the segment builder UI:
 * - Current rule tree being built
 * - Selected brand context
 * - Audience estimate
 * - Attribute catalog cache
 */

import { create } from "zustand";
import type {
  AttributeCondition,
  AttributeDefinition,
  Brand,
  Condition,
  ConditionGroup,
  EventCondition,
  LogicalOperator,
  SegmentDefinition,
} from "../types/segment";

function generateId(): string {
  return `cond_${Date.now()}_${Math.random().toString(36).slice(2, 9)}`;
}

function createEmptyGroup(operator: LogicalOperator = "and"): ConditionGroup {
  return {
    type: "group",
    id: generateId(),
    logical_operator: operator,
    conditions: [],
  };
}

function createEmptyAttributeCondition(): AttributeCondition {
  return {
    type: "attribute",
    id: generateId(),
    attribute_key: "",
    operator: "equals",
    value: "",
    negate: false,
  };
}

interface SegmentBuilderState {
  // Current segment being built
  segmentName: string;
  segmentDescription: string;
  segmentType: string;
  rules: ConditionGroup;

  // Brand context
  selectedBrandCode: string | null;
  brands: Brand[];

  // Attribute catalog
  attributeCatalog: AttributeDefinition[];
  catalogLoaded: boolean;

  // Audience estimation
  audienceCount: number | null;
  isEstimating: boolean;
  compiledSQL: string | null;

  // UI state
  isDirty: boolean;

  // Actions
  setSegmentName: (name: string) => void;
  setSegmentDescription: (desc: string) => void;
  setSegmentType: (type: string) => void;
  setSelectedBrand: (code: string) => void;
  setBrands: (brands: Brand[]) => void;
  setAttributeCatalog: (attrs: AttributeDefinition[]) => void;
  setAudienceCount: (count: number | null) => void;
  setIsEstimating: (v: boolean) => void;
  setCompiledSQL: (sql: string | null) => void;

  // Rule tree mutations
  addCondition: (groupId: string, condition?: Condition) => void;
  addGroup: (parentGroupId: string, operator?: LogicalOperator) => void;
  removeCondition: (conditionId: string) => void;
  updateCondition: (conditionId: string, updates: Partial<Condition>) => void;
  toggleGroupOperator: (groupId: string) => void;
  resetRules: () => void;
  loadRules: (rules: ConditionGroup) => void;

  // Export
  getSegmentDefinition: () => SegmentDefinition;
}

/**
 * Recursively find and mutate a node in the condition tree.
 */
function findAndMutate(
  group: ConditionGroup,
  targetId: string,
  mutator: (parent: ConditionGroup, index: number) => void
): boolean {
  for (let i = 0; i < group.conditions.length; i++) {
    const cond = group.conditions[i];
    if (cond.id === targetId) {
      mutator(group, i);
      return true;
    }
    if (cond.type === "group") {
      if (findAndMutate(cond, targetId, mutator)) return true;
    }
  }
  return false;
}

function findGroup(
  group: ConditionGroup,
  groupId: string
): ConditionGroup | null {
  if (group.id === groupId) return group;
  for (const cond of group.conditions) {
    if (cond.type === "group") {
      const found = findGroup(cond, groupId);
      if (found) return found;
    }
  }
  return null;
}

export const useSegmentStore = create<SegmentBuilderState>((set, get) => ({
  segmentName: "",
  segmentDescription: "",
  segmentType: "dynamic",
  rules: createEmptyGroup("and"),
  selectedBrandCode: null,
  brands: [],
  attributeCatalog: [],
  catalogLoaded: false,
  audienceCount: null,
  isEstimating: false,
  compiledSQL: null,
  isDirty: false,

  setSegmentName: (name) => set({ segmentName: name, isDirty: true }),
  setSegmentDescription: (desc) =>
    set({ segmentDescription: desc, isDirty: true }),
  setSegmentType: (type) => set({ segmentType: type, isDirty: true }),
  setSelectedBrand: (code) =>
    set({ selectedBrandCode: code, audienceCount: null }),
  setBrands: (brands) => set({ brands }),
  setAttributeCatalog: (attrs) =>
    set({ attributeCatalog: attrs, catalogLoaded: true }),
  setAudienceCount: (count) => set({ audienceCount: count }),
  setIsEstimating: (v) => set({ isEstimating: v }),
  setCompiledSQL: (sql) => set({ compiledSQL: sql }),

  addCondition: (groupId, condition) => {
    const rules = structuredClone(get().rules);
    const group = findGroup(rules, groupId);
    if (group) {
      group.conditions.push(condition || createEmptyAttributeCondition());
    }
    set({ rules, isDirty: true, audienceCount: null });
  },

  addGroup: (parentGroupId, operator = "and") => {
    const rules = structuredClone(get().rules);
    const parent = findGroup(rules, parentGroupId);
    if (parent) {
      parent.conditions.push(createEmptyGroup(operator));
    }
    set({ rules, isDirty: true, audienceCount: null });
  },

  removeCondition: (conditionId) => {
    const rules = structuredClone(get().rules);
    findAndMutate(rules, conditionId, (parent, index) => {
      parent.conditions.splice(index, 1);
    });
    set({ rules, isDirty: true, audienceCount: null });
  },

  updateCondition: (conditionId, updates) => {
    const rules = structuredClone(get().rules);
    findAndMutate(rules, conditionId, (parent, index) => {
      parent.conditions[index] = { ...parent.conditions[index], ...updates } as Condition;
    });
    set({ rules, isDirty: true, audienceCount: null });
  },

  toggleGroupOperator: (groupId) => {
    const rules = structuredClone(get().rules);
    const group = findGroup(rules, groupId);
    if (group) {
      group.logical_operator =
        group.logical_operator === "and" ? "or" : "and";
    }
    set({ rules, isDirty: true, audienceCount: null });
  },

  resetRules: () =>
    set({
      rules: createEmptyGroup("and"),
      audienceCount: null,
      compiledSQL: null,
      isDirty: false,
    }),

  loadRules: (rules) => set({ rules, isDirty: false }),

  getSegmentDefinition: () => {
    const { rules } = get();
    // Strip client-side IDs for API submission
    const stripIds = (group: ConditionGroup): any => ({
      type: "group",
      logical_operator: group.logical_operator,
      conditions: group.conditions.map((c) => {
        if (c.type === "group") return stripIds(c);
        const { id, ...rest } = c as any;
        return rest;
      }),
    });
    return { root: stripIds(rules) };
  },
}));

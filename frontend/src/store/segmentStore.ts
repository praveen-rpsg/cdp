/**
 * Segment Builder Store (Zustand)
 *
 * Manages the state of the segment builder UI including:
 * - Current rule tree being built
 * - Rank & Split configuration
 * - Set operations (Union, Overlap, Exclude)
 * - Audience estimation with real counts
 * - Attribute catalog cache
 */

import { create } from "zustand";
import type {
  AttributeCondition,
  AttributeDefinition,
  Brand,
  Condition,
  ConditionGroup,
  LogicalOperator,
  RankConfig,
  SegmentDefinition,
  SetOperation,
  SetOperationType,
  SplitConfig,
  SplitCountResult,
  SplitEntry,
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

  // Rank & Split
  rankConfig: RankConfig;
  splitConfig: SplitConfig;
  splitCounts: SplitCountResult[];

  // Set Operations
  setOperation: SetOperation;
  setOperationCounts: { operation: string; combined_count: number | null; segment_counts: number[] } | null;

  // UI state
  isDirty: boolean;

  // Actions - Metadata
  setSegmentName: (name: string) => void;
  setSegmentDescription: (desc: string) => void;
  setSegmentType: (type: string) => void;
  setSelectedBrand: (code: string) => void;
  setBrands: (brands: Brand[]) => void;
  setAttributeCatalog: (attrs: AttributeDefinition[]) => void;
  setAudienceCount: (count: number | null) => void;
  setIsEstimating: (v: boolean) => void;
  setCompiledSQL: (sql: string | null) => void;

  // Actions - Rank & Split
  setRankConfig: (config: Partial<RankConfig>) => void;
  setSplitConfig: (config: Partial<SplitConfig>) => void;
  setSplitCounts: (counts: SplitCountResult[]) => void;
  addSplitEntry: () => void;
  removeSplitEntry: (index: number) => void;
  updateSplitEntry: (index: number, entry: Partial<SplitEntry>) => void;

  // Actions - Set Operations
  setSetOperation: (config: Partial<SetOperation>) => void;
  setSetOperationCounts: (counts: any) => void;

  // Rule tree mutations
  addCondition: (groupId: string, condition?: Condition) => void;
  quickAddCondition: (category: string) => void;
  insertConditionAt: (groupId: string, index: number, condition?: Condition) => void;
  reorderConditions: (groupId: string, fromIndex: number, toIndex: number) => void;
  addGroup: (parentGroupId: string, operator?: LogicalOperator) => void;
  removeCondition: (conditionId: string) => void;
  updateCondition: (conditionId: string, updates: Partial<Condition>) => void;
  toggleGroupOperator: (groupId: string) => void;
  resetRules: () => void;
  loadRules: (rules: ConditionGroup) => void;

  // Export
  getSegmentDefinition: () => SegmentDefinition;
}

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

  // Rank & Split defaults
  rankConfig: { enabled: false, attribute: null, order: "desc", profile_limit: null },
  splitConfig: { enabled: false, split_type: "percent", attribute: null, num_splits: 2, splits: [] },
  splitCounts: [],

  // Set operation defaults
  setOperation: { enabled: false, operation: "union", segments: [] },
  setOperationCounts: null,

  setSegmentName: (name) => set({ segmentName: name, isDirty: true }),
  setSegmentDescription: (desc) => set({ segmentDescription: desc, isDirty: true }),
  setSegmentType: (type) => set({ segmentType: type, isDirty: true }),
  setSelectedBrand: (code) => set({ selectedBrandCode: code, audienceCount: null }),
  setBrands: (brands) => set({ brands }),
  setAttributeCatalog: (attrs) => set({ attributeCatalog: attrs, catalogLoaded: true }),
  setAudienceCount: (count) => set({ audienceCount: count }),
  setIsEstimating: (v) => set({ isEstimating: v }),
  setCompiledSQL: (sql) => set({ compiledSQL: sql }),

  // Rank
  setRankConfig: (config) => set((state) => ({
    rankConfig: { ...state.rankConfig, ...config },
    isDirty: true,
    audienceCount: null,
  })),

  // Split
  setSplitConfig: (config) => set((state) => ({
    splitConfig: { ...state.splitConfig, ...config },
    isDirty: true,
  })),
  setSplitCounts: (counts) => set({ splitCounts: counts }),
  addSplitEntry: () => set((state) => ({
    splitConfig: {
      ...state.splitConfig,
      splits: [...state.splitConfig.splits, { name: `Split ${state.splitConfig.splits.length + 1}`, percent: 50 }],
    },
    isDirty: true,
  })),
  removeSplitEntry: (index) => set((state) => ({
    splitConfig: {
      ...state.splitConfig,
      splits: state.splitConfig.splits.filter((_, i) => i !== index),
    },
    isDirty: true,
  })),
  updateSplitEntry: (index, entry) => set((state) => ({
    splitConfig: {
      ...state.splitConfig,
      splits: state.splitConfig.splits.map((s, i) => i === index ? { ...s, ...entry } : s),
    },
    isDirty: true,
  })),

  // Set Operations
  setSetOperation: (config) => set((state) => ({
    setOperation: { ...state.setOperation, ...config },
    isDirty: true,
    audienceCount: null,
  })),
  setSetOperationCounts: (counts) => set({ setOperationCounts: counts }),

  addCondition: (groupId, condition) => {
    const rules = structuredClone(get().rules);
    const group = findGroup(rules, groupId);
    if (group) {
      group.conditions.push(condition || createEmptyAttributeCondition());
    }
    set({ rules, isDirty: true, audienceCount: null });
  },

  quickAddCondition: (category) => {
    const rules = structuredClone(get().rules);
    const root = rules;
    const condition: any = createEmptyAttributeCondition();
    condition._initialCategory = category;
    root.conditions.push(condition);
    set({ rules, isDirty: true, audienceCount: null });
  },

  insertConditionAt: (groupId, index, condition) => {
    const rules = structuredClone(get().rules);
    const group = findGroup(rules, groupId);
    if (group) {
      const newCond = condition || createEmptyAttributeCondition();
      group.conditions.splice(index, 0, newCond);
    }
    set({ rules, isDirty: true, audienceCount: null });
  },

  reorderConditions: (groupId, fromIndex, toIndex) => {
    const rules = structuredClone(get().rules);
    const group = findGroup(rules, groupId);
    if (group && fromIndex !== toIndex) {
      const [moved] = group.conditions.splice(fromIndex, 1);
      group.conditions.splice(toIndex, 0, moved);
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
      group.logical_operator = group.logical_operator === "and" ? "or" : "and";
    }
    set({ rules, isDirty: true, audienceCount: null });
  },

  resetRules: () =>
    set({
      rules: createEmptyGroup("and"),
      audienceCount: null,
      compiledSQL: null,
      isDirty: false,
      rankConfig: { enabled: false, attribute: null, order: "desc", profile_limit: null },
      splitConfig: { enabled: false, split_type: "percent", attribute: null, num_splits: 2, splits: [] },
      splitCounts: [],
      setOperation: { enabled: false, operation: "union", segments: [] },
      setOperationCounts: null,
    }),

  loadRules: (rules) => set({ rules, isDirty: false }),

  getSegmentDefinition: () => {
    const { rules, rankConfig, splitConfig, setOperation } = get();
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

    const def: any = { root: stripIds(rules) };

    // Include rank config if enabled
    if (rankConfig.enabled) {
      def.rank = rankConfig;
    }

    // Include split config if enabled
    if (splitConfig.enabled && splitConfig.splits.length > 0) {
      def.split = splitConfig;
    }

    // Include set operation if enabled
    if (setOperation.enabled && setOperation.segments.length > 0) {
      def.set_operation = setOperation;
    }

    return def;
  },
}));

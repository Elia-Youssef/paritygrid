import { describe, expect, it } from "vitest";

import type {
  ConflictPageResponse,
  ConflictResponse,
  ReconciliationResponse,
  RepairApplyResponse,
  RepairPlanResponse,
} from "./generated/schema";
import {
  conflictPageResponseSchema,
  reconciliationResponseSchema,
  repairActionSchema,
  repairApplyResponseSchema,
  repairPlanResponseSchema,
} from "./runtime-schemas";

/**
 * Fixtures are typed as the generated transport types, so every fixture is
 * compile-time checked against the OpenAPI contract the same way the wire
 * is. The `to<Shape>` assignments below are compile-time identity checks:
 * they compile only while each tightened schema's parsed output remains
 * assignable to its generated response type.
 */
const FINGERPRINT_A = "a".repeat(64);
const FINGERPRINT_B = "b".repeat(64);
const FINGERPRINT_C = "c".repeat(64);

const OBSERVED_AT = "2026-01-02 03:04:05";

const RECONCILIATION: ReconciliationResponse = {
  schema_version: 1,
  run_id: "run_recon-001",
  run_version: 4,
  state: "succeeded",
  observed_at: OBSERVED_AT,
  reconciliation_fingerprint: FINGERPRINT_A,
  source_input_identity: FINGERPRINT_B,
  target_input_identity: FINGERPRINT_C,
  total_count: 9,
  counts: {
    match: 3,
    missing_from_target: 1,
    missing_from_source: 1,
    field_mismatch: 1,
    duplicate_source: 1,
    duplicate_target: 1,
    duplicate_both: 1,
  },
  analytical_query_version: 2,
  reconciliation_observed_at: "2026-01-02 03:04:06",
};

const CONFLICT: ConflictResponse = {
  schema_version: 1,
  conflict_id: "conflict-1",
  canonical_key: "CANONICAL-KEY-1",
  classification: "field_mismatch",
  source_references: [{ position: 0, record_key: "source-key-1" }],
  target_references: [{ position: 0, record_key: "target-key-1" }],
  differences: [
    {
      field: "quantity",
      kind: "value_mismatch",
      source_text: "1",
      target_text: "2",
    },
  ],
  suggested_resolution: "update_target",
  created_at: OBSERVED_AT,
};

const CONFLICT_PAGE: ConflictPageResponse = {
  schema_version: 1,
  run_id: "run_recon-001",
  run_version: 4,
  state: "succeeded",
  observed_at: OBSERVED_AT,
  reconciliation_fingerprint: FINGERPRINT_A,
  limit: 50,
  next_cursor: null,
  items: [CONFLICT],
};

const REPAIR_PLAN: RepairPlanResponse = {
  schema_version: 1,
  plan_id: "plan-1",
  run_id: "run_recon-001",
  run_version: 4,
  state: "succeeded",
  observed_at: OBSERVED_AT,
  status: "proposed",
  reconciliation_fingerprint: FINGERPRINT_A,
  content_fingerprint: FINGERPRINT_B,
  created_at: OBSERVED_AT,
  approval: null,
  actions: [
    {
      schema_version: 1,
      action_id: "action-1",
      canonical_key: "canonical-key-1",
      kind: "update_target",
      status: "pending",
      before_sha256: FINGERPRINT_C,
      proposed_after_sha256: FINGERPRINT_B,
    },
  ],
};

const REPAIR_APPLY: RepairApplyResponse = {
  schema_version: 1,
  plan_id: "plan-1",
  run_id: "run_recon-001",
  run_version: 4,
  state: "succeeded",
  observed_at: OBSERVED_AT,
  status: "applied",
  reconciliation_fingerprint: FINGERPRINT_A,
  content_fingerprint: FINGERPRINT_B,
  disposition: "completed",
  resumed: false,
  effects: [],
};

// Compile-time: parsed output must stay assignable to the generated types.
const toReconciliation: (input: unknown) => ReconciliationResponse = (input) =>
  reconciliationResponseSchema.parse(input);
const toConflictPage: (input: unknown) => ConflictPageResponse = (input) =>
  conflictPageResponseSchema.parse(input);
const toRepairPlan: (input: unknown) => RepairPlanResponse = (input) =>
  repairPlanResponseSchema.parse(input);
const toRepairApply: (input: unknown) => RepairApplyResponse = (input) =>
  repairApplyResponseSchema.parse(input);

describe("reconciliation response schema", () => {
  it("accepts a canonical snapshot carrying all seven classification counts", () => {
    const parsed = reconciliationResponseSchema.safeParse(RECONCILIATION);
    expect(parsed.success).toBe(true);
    expect(toReconciliation(RECONCILIATION)).toEqual(RECONCILIATION);
  });

  it("rejects a counts key outside the classification closed set", () => {
    const alien = {
      ...RECONCILIATION,
      counts: { ...RECONCILIATION.counts, alien_kind: 1 },
    };
    const parsed = reconciliationResponseSchema.safeParse(alien);
    expect(parsed.success).toBe(false);
    if (!parsed.success) {
      expect(parsed.error.issues.some((issue) => issue.path.includes("counts"))).toBe(
        true,
      );
    }
  });

  it("rejects a missing classification count and an incoherent total", () => {
    const incompleteCounts = { ...RECONCILIATION.counts };
    delete incompleteCounts.duplicate_both;
    expect(
      reconciliationResponseSchema.safeParse({
        ...RECONCILIATION,
        counts: incompleteCounts,
      }).success,
    ).toBe(false);
    expect(
      reconciliationResponseSchema.safeParse({
        ...RECONCILIATION,
        total_count: RECONCILIATION.total_count + 1,
      }).success,
    ).toBe(false);
  });

  it("rejects fingerprints that are uppercase or not 64 lowercase hex", () => {
    expect(
      reconciliationResponseSchema.safeParse({
        ...RECONCILIATION,
        reconciliation_fingerprint: FINGERPRINT_A.toUpperCase(),
      }).success,
    ).toBe(false);
    expect(
      reconciliationResponseSchema.safeParse({
        ...RECONCILIATION,
        source_input_identity: "not-a-fingerprint",
      }).success,
    ).toBe(false);
    expect(
      reconciliationResponseSchema.safeParse({
        ...RECONCILIATION,
        target_input_identity: FINGERPRINT_C.slice(1),
      }).success,
    ).toBe(false);
  });

  it("rejects unknown members and non-integer counts", () => {
    expect(
      reconciliationResponseSchema.safeParse({ ...RECONCILIATION, extra: true })
        .success,
    ).toBe(false);
    expect(
      reconciliationResponseSchema.safeParse({
        ...RECONCILIATION,
        counts: { ...RECONCILIATION.counts, match: 1.5 },
      }).success,
    ).toBe(false);
  });
});

describe("conflict page response schema", () => {
  it("accepts a canonical page and preserves the generated type", () => {
    expect(conflictPageResponseSchema.safeParse(CONFLICT_PAGE).success).toBe(true);
    expect(toConflictPage(CONFLICT_PAGE)).toEqual(CONFLICT_PAGE);
  });

  it("rejects a difference kind outside the closed set", () => {
    const page = {
      ...CONFLICT_PAGE,
      items: [
        {
          ...CONFLICT,
          differences: [{ ...CONFLICT.differences[0], kind: "renamed_field" }],
        },
      ],
    };
    expect(conflictPageResponseSchema.safeParse(page).success).toBe(false);
  });

  it("rejects a classification outside the closed set", () => {
    const page = { ...CONFLICT_PAGE, items: [{ ...CONFLICT, classification: "typo" }] };
    expect(conflictPageResponseSchema.safeParse(page).success).toBe(false);
  });

  it("rejects a suggested resolution outside the classification mapping", () => {
    const alien = {
      ...CONFLICT_PAGE,
      items: [{ ...CONFLICT, suggested_resolution: "delete_both" }],
    };
    expect(conflictPageResponseSchema.safeParse(alien).success).toBe(false);
    const absent = {
      ...CONFLICT_PAGE,
      items: [{ ...CONFLICT, suggested_resolution: null }],
    };
    expect(conflictPageResponseSchema.safeParse(absent).success).toBe(false);
    expect(
      conflictPageResponseSchema.safeParse({
        ...CONFLICT_PAGE,
        items: [{ ...CONFLICT, classification: "match" }],
      }).success,
    ).toBe(false);
  });

  it("rejects a page reconciliation fingerprint that is not lowercase 64-hex", () => {
    expect(
      conflictPageResponseSchema.safeParse({
        ...CONFLICT_PAGE,
        reconciliation_fingerprint: FINGERPRINT_A.toUpperCase(),
      }).success,
    ).toBe(false);
  });

  it("enforces the backend cursor and difference bounds", () => {
    expect(
      conflictPageResponseSchema.safeParse({
        ...CONFLICT_PAGE,
        next_cursor: "lowercase-cursor",
      }).success,
    ).toBe(false);
    expect(
      conflictPageResponseSchema.safeParse({
        ...CONFLICT_PAGE,
        next_cursor: `CURSOR-${"A".repeat(64)}`,
      }).success,
    ).toBe(false);
    expect(
      conflictPageResponseSchema.safeParse({
        ...CONFLICT_PAGE,
        items: [
          {
            ...CONFLICT,
            differences: [{ ...CONFLICT.differences[0], source_text: "x".repeat(513) }],
          },
        ],
      }).success,
    ).toBe(false);
  });
});

describe("repair plan response schema", () => {
  it("accepts a canonical plan with an update_target action", () => {
    expect(repairPlanResponseSchema.safeParse(REPAIR_PLAN).success).toBe(true);
    expect(toRepairPlan(REPAIR_PLAN)).toEqual(REPAIR_PLAN);
  });

  it("accepts only the empty create_target before-state sentinel", () => {
    const sentinel = repairPlanResponseSchema.safeParse({
      ...REPAIR_PLAN,
      actions: [
        {
          schema_version: 1,
          action_id: "action-create",
          canonical_key: "CANONICAL-KEY-NEW",
          kind: "create_target",
          status: "pending",
          before_sha256: "",
          proposed_after_sha256: FINGERPRINT_B,
        },
      ],
    });
    expect(sentinel.success).toBe(true);

    const explicit = repairPlanResponseSchema.safeParse({
      ...REPAIR_PLAN,
      actions: [
        {
          schema_version: 1,
          action_id: "action-create",
          canonical_key: "CANONICAL-KEY-NEW",
          kind: "create_target",
          status: "pending",
          before_sha256: FINGERPRINT_C,
          proposed_after_sha256: FINGERPRINT_B,
        },
      ],
    });
    expect(explicit.success).toBe(false);
  });

  it("rejects an empty before_sha256 on update_target actions", () => {
    const plan = {
      ...REPAIR_PLAN,
      actions: [{ ...REPAIR_PLAN.actions[0], before_sha256: "" }],
    };
    const parsed = repairPlanResponseSchema.safeParse(plan);
    expect(parsed.success).toBe(false);
    if (!parsed.success) {
      expect(
        parsed.error.issues.some((issue) => issue.path.includes("before_sha256")),
      ).toBe(true);
    }
  });

  it("rejects unknown action statuses and unknown plan statuses", () => {
    const actionStatus = {
      ...REPAIR_PLAN,
      actions: [{ ...REPAIR_PLAN.actions[0], status: "skipped" }],
    };
    expect(repairPlanResponseSchema.safeParse(actionStatus).success).toBe(false);

    // "pending" is an action status, never a plan status.
    const planStatus = { ...REPAIR_PLAN, status: "pending" };
    expect(repairPlanResponseSchema.safeParse(planStatus).success).toBe(false);
    expect(
      repairPlanResponseSchema.safeParse({ ...REPAIR_PLAN, status: "approved" })
        .success,
    ).toBe(false);
    expect(
      repairPlanResponseSchema.safeParse({
        ...REPAIR_PLAN,
        status: "approved",
        approval: {
          schema_version: 1,
          approved_by: "operator",
          approved_at: OBSERVED_AT,
          correlation_id: "correlation-1",
          approval_schema_version: 1,
        },
      }).success,
    ).toBe(true);
  });

  it("rejects plan fingerprints that are not lowercase 64-hex", () => {
    expect(
      repairPlanResponseSchema.safeParse({
        ...REPAIR_PLAN,
        content_fingerprint: FINGERPRINT_B.toUpperCase(),
      }).success,
    ).toBe(false);
    expect(
      repairPlanResponseSchema.safeParse({
        ...REPAIR_PLAN,
        reconciliation_fingerprint: "0123456789abcdef",
      }).success,
    ).toBe(false);
  });
});

describe("repair action schema", () => {
  it("keeps kind permissive as display text", () => {
    const future = repairActionSchema.safeParse({
      schema_version: 1,
      action_id: "action-future",
      canonical_key: "canonical-key-1",
      kind: "not_yet_supported_kind",
      status: "pending",
      before_sha256: FINGERPRINT_C,
      proposed_after_sha256: FINGERPRINT_B,
    });
    // A hostile or future kind parses as text; the repair derivation layer
    // must visibly block it rather than the schema silently dropping it.
    expect(future.success).toBe(true);
  });

  it("denies the empty sentinel to any kind other than create_target", () => {
    const futureWithSentinel = repairActionSchema.safeParse({
      schema_version: 1,
      action_id: "action-future",
      canonical_key: "canonical-key-1",
      kind: "not_yet_supported_kind",
      status: "pending",
      before_sha256: "",
      proposed_after_sha256: FINGERPRINT_B,
    });
    expect(futureWithSentinel.success).toBe(false);
  });
});

describe("repair apply response schema", () => {
  it("accepts successful/failed acknowledgements and rejects 503-only dispositions", () => {
    for (const disposition of [
      "completed",
      "already_applied",
      "failed",
      "unresolved",
      "interrupted",
    ] as const) {
      const parsed = repairApplyResponseSchema.safeParse({
        ...REPAIR_APPLY,
        disposition,
        status: disposition === "failed" ? "failed" : "applied",
      });
      expect(parsed.success).toBe(
        disposition !== "unresolved" && disposition !== "interrupted",
      );
    }
    expect(toRepairApply(REPAIR_APPLY)).toEqual(REPAIR_APPLY);
  });

  it("rejects an unknown disposition", () => {
    expect(
      repairApplyResponseSchema.safeParse({ ...REPAIR_APPLY, disposition: "postponed" })
        .success,
    ).toBe(false);
  });

  it("requires resumed to be a boolean", () => {
    expect(
      repairApplyResponseSchema.safeParse({ ...REPAIR_APPLY, resumed: "no" }).success,
    ).toBe(false);
    expect(
      repairApplyResponseSchema.safeParse({ ...REPAIR_APPLY, resumed: 0 }).success,
    ).toBe(false);
    expect(
      repairApplyResponseSchema.safeParse({ ...REPAIR_APPLY, resumed: true }).success,
    ).toBe(true);
  });

  it("bounds the effects collection", () => {
    const atLimit = {
      ...REPAIR_APPLY,
      effects: Array.from({ length: 10_000 }, () => ({ key: "k" })),
    };
    expect(repairApplyResponseSchema.safeParse(atLimit).success).toBe(true);

    const overLimit = {
      ...REPAIR_APPLY,
      effects: Array.from({ length: 10_001 }, () => ({ key: "k" })),
    };
    expect(repairApplyResponseSchema.safeParse(overLimit).success).toBe(false);
  });

  it("requires lowercase 64-hex fingerprints", () => {
    expect(
      repairApplyResponseSchema.safeParse({
        ...REPAIR_APPLY,
        content_fingerprint: FINGERPRINT_B.slice(0, 63),
      }).success,
    ).toBe(false);
  });
});

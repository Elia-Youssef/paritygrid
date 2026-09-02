import { describe, expect, it } from "vitest";

import type {
  RepairActionResponse,
  RepairPlanResponse,
} from "../../api/generated/schema";
import {
  REPAIR_ACTION_STATUSES,
  REPAIR_APPLY_DISPOSITIONS,
  REPAIR_PLAN_STATUSES,
  SUPPORTED_ACTION_KINDS,
  actionSentinelProblem,
  applyBlockers,
  applyIdempotencyKey,
  approvalBlockers,
  approvalIdempotencyKey,
  describeDisposition,
  describePlanBlocker,
  describePlanStatus,
  isPlanApprovable,
  isSupportedActionKind,
  planActionSummary,
} from "./repair-contract";

const HEX_A = "a".repeat(64);
const HEX_B = "b".repeat(64);
const HEX_C = "c".repeat(64);

function makeAction(
  overrides: Partial<RepairActionResponse> = {},
): RepairActionResponse {
  return {
    action_id: "act-1",
    applied_at: null,
    before_sha256: HEX_B,
    canonical_key: "record-1",
    failed_at: null,
    kind: "update_target",
    proposed_after_sha256: HEX_C,
    status: "pending",
    target_version: null,
    ...overrides,
  };
}

function makePlan(overrides: Partial<RepairPlanResponse> = {}): RepairPlanResponse {
  return {
    actions: [makeAction()],
    applied_at: null,
    applying_at: null,
    approval: null,
    content_fingerprint: HEX_A,
    created_at: "2026-01-01 00:00:00",
    failed_at: null,
    observed_at: "2026-01-01 00:00:00",
    plan_id: "rp_plan-001",
    reconciliation_fingerprint: HEX_B,
    rejected_at: null,
    run_id: "run-001",
    run_version: 3,
    state: "reconciled",
    status: "proposed",
    ...overrides,
  };
}

const ready = { authoritativeLoaded: true, currentReconciliationFingerprint: HEX_B };

describe("repair contract value sets", () => {
  it("keeps the closed status, disposition, and kind sets", () => {
    expect(REPAIR_PLAN_STATUSES).toEqual([
      "proposed",
      "approved",
      "applying",
      "applied",
      "rejected",
      "failed",
    ]);
    expect(REPAIR_ACTION_STATUSES).toEqual(["pending", "applied", "failed"]);
    expect(REPAIR_APPLY_DISPOSITIONS).toEqual([
      "completed",
      "already_applied",
      "failed",
      "unresolved",
      "interrupted",
    ]);
    expect(SUPPORTED_ACTION_KINDS).toEqual(["create_target", "update_target"]);
  });

  it("classifies supported and foreign action kinds", () => {
    expect(isSupportedActionKind("create_target")).toBe(true);
    expect(isSupportedActionKind("update_target")).toBe(true);
    expect(isSupportedActionKind("delete_target")).toBe(false);
    expect(isSupportedActionKind("")).toBe(false);
  });

  it("approves only the proposed status", () => {
    for (const status of REPAIR_PLAN_STATUSES) {
      expect(isPlanApprovable(status)).toBe(status === "proposed");
    }
  });
});

describe("actionSentinelProblem", () => {
  it("accepts a create_target with the empty before sentinel", () => {
    expect(
      actionSentinelProblem(makeAction({ kind: "create_target", before_sha256: "" })),
    ).toBeNull();
  });

  it("rejects a create_target with a non-empty before value", () => {
    expect(
      actionSentinelProblem(
        makeAction({ kind: "create_target", before_sha256: HEX_B }),
      ),
    ).toContain("empty-string sentinel");
  });

  it("rejects an update_target with the empty sentinel as before state", () => {
    const problem = actionSentinelProblem(
      makeAction({ kind: "update_target", before_sha256: "" }),
    );
    expect(problem).toMatch(/update_target requires a before fingerprint/);
  });

  it("accepts an update_target with a valid 64-hex before value", () => {
    expect(actionSentinelProblem(makeAction())).toBeNull();
  });

  it("rejects an empty proposed-after fingerprint", () => {
    expect(actionSentinelProblem(makeAction({ proposed_after_sha256: "" }))).toMatch(
      /proposed-after fingerprint/,
    );
  });

  it("rejects an uppercase proposed-after fingerprint", () => {
    expect(
      actionSentinelProblem(makeAction({ proposed_after_sha256: HEX_A.toUpperCase() })),
    ).toMatch(/proposed-after fingerprint/);
  });

  it("reports a foreign kind as unsupported and named", () => {
    expect(actionSentinelProblem(makeAction({ kind: "delete_target" }))).toBe(
      "unsupported action kind delete_target",
    );
  });
});

describe("planActionSummary", () => {
  it("counts each closed action status and lists unsupported kinds visibly", () => {
    const plan = makePlan({
      actions: [
        makeAction({ action_id: "act-1", status: "pending" }),
        makeAction({ action_id: "act-2", status: "applied" }),
        makeAction({ action_id: "act-3", status: "applied" }),
        makeAction({ action_id: "act-4", status: "failed" }),
        makeAction({ action_id: "act-5", kind: "delete_target" }),
        makeAction({ action_id: "act-6", kind: "delete_target", status: "applied" }),
        makeAction({ action_id: "act-7", kind: "purge_target", status: "failed" }),
      ],
    });
    expect(planActionSummary(plan)).toEqual({
      total: 7,
      pending: 2,
      applied: 3,
      failed: 2,
      unsupported: ["delete_target", "purge_target"],
    });
  });

  it("returns zeros and an empty unsupported list for a clean plan", () => {
    expect(planActionSummary(makePlan())).toEqual({
      total: 1,
      pending: 1,
      applied: 0,
      failed: 0,
      unsupported: [],
    });
  });
});

describe("approvalBlockers", () => {
  it("allows a proposed plan with matching fingerprints and loaded state", () => {
    expect(approvalBlockers(makePlan(), ready)).toEqual({ codes: [] });
  });

  it("blocks without authoritative state and reports nothing else", () => {
    expect(approvalBlockers(null, ready)).toEqual({
      codes: ["no-authoritative-state"],
    });
    expect(
      approvalBlockers(makePlan(), {
        authoritativeLoaded: false,
        currentReconciliationFingerprint: HEX_B,
      }),
    ).toEqual({ codes: ["no-authoritative-state"] });
  });

  it("blocks a plan that is already approved by status", () => {
    const plan = makePlan({ status: "approved" });
    expect(approvalBlockers(plan, ready).codes).toContain("status-not-approvable");
  });

  it("blocks an applied plan as already applied", () => {
    const plan = makePlan({ status: "applied" });
    const codes = approvalBlockers(plan, ready).codes;
    expect(codes).toContain("already-applied");
    expect(codes).toContain("status-not-approvable");
  });

  it("blocks re-approval when a durable approval is present", () => {
    const plan = makePlan({
      status: "proposed",
      approval: {
        approval_schema_version: 1,
        approved_at: "2026-01-01 00:01:00",
        approved_by: "operator-1",
        correlation_id: "corr-1",
      },
    });
    expect(approvalBlockers(plan, ready).codes).toContain("already-approved");
  });

  it("blocks a plan carrying an unsupported action kind", () => {
    const plan = makePlan({
      actions: [makeAction({ kind: "delete_target", before_sha256: "" })],
    });
    expect(approvalBlockers(plan, ready).codes).toContain("unsupported-action");
  });

  it("blocks a plan with a fingerprint cross-field inconsistency", () => {
    const plan = makePlan({
      actions: [makeAction({ kind: "update_target", before_sha256: "" })],
    });
    expect(approvalBlockers(plan, ready).codes).toContain("unsupported-action");
  });

  it("blocks a stale plan whose run re-reconciled", () => {
    const current = {
      authoritativeLoaded: true,
      currentReconciliationFingerprint: HEX_C,
    };
    expect(approvalBlockers(makePlan(), current).codes).toContain("stale-fingerprint");
  });

  it("blocks when the current reconciliation fingerprint is missing", () => {
    const current = {
      authoritativeLoaded: true,
      currentReconciliationFingerprint: null,
    };
    expect(approvalBlockers(makePlan(), current).codes).toEqual([
      "fingerprint-missing",
    ]);
  });

  it("orders codes deterministically and without duplicates", () => {
    const plan = makePlan({
      status: "applied",
      approval: {
        approval_schema_version: 1,
        approved_at: "2026-01-01 00:01:00",
        approved_by: "operator-1",
        correlation_id: "corr-1",
      },
      actions: [makeAction({ kind: "delete_target", before_sha256: "" })],
    });
    const current = {
      authoritativeLoaded: true,
      currentReconciliationFingerprint: HEX_C,
    };
    const first = approvalBlockers(plan, current).codes;
    const second = approvalBlockers(plan, current).codes;
    expect(first).toEqual([
      "status-not-approvable",
      "already-applied",
      "already-approved",
      "unsupported-action",
      "stale-fingerprint",
    ]);
    expect(first).toEqual(second);
  });
});

describe("applyBlockers", () => {
  it("allows an approved plan with matching fingerprints", () => {
    const plan = makePlan({
      status: "approved",
      approval: {
        approval_schema_version: 1,
        approved_at: "2026-01-01 00:01:00",
        approved_by: "operator-1",
        correlation_id: "corr-1",
      },
    });
    expect(applyBlockers(plan, { ...ready, applyInFlight: false })).toEqual({
      codes: [],
    });
  });

  it("blocks a proposed plan because approval is absent", () => {
    expect(applyBlockers(makePlan(), { ...ready, applyInFlight: false }).codes).toEqual(
      ["status-not-approvable"],
    );
  });

  it("blocks an approved lifecycle without durable approval evidence", () => {
    expect(
      applyBlockers(makePlan({ status: "approved", approval: null }), {
        ...ready,
        applyInFlight: false,
      }).codes,
    ).toEqual(["approval-missing"]);
  });

  it("blocks an applying plan as owned by another application", () => {
    const plan = makePlan({ status: "applying" });
    expect(applyBlockers(plan, { ...ready, applyInFlight: false }).codes).toEqual([
      "status-not-approvable",
    ]);
  });

  it("blocks an applied plan as already applied", () => {
    const plan = makePlan({ status: "applied" });
    expect(applyBlockers(plan, { ...ready, applyInFlight: false }).codes).toEqual([
      "status-not-approvable",
      "already-applied",
    ]);
  });

  it("blocks rejected and failed plans as terminal", () => {
    expect(
      applyBlockers(makePlan({ status: "rejected" }), {
        ...ready,
        applyInFlight: false,
      }).codes,
    ).toEqual(["status-not-approvable"]);
    expect(
      applyBlockers(makePlan({ status: "failed" }), { ...ready, applyInFlight: false })
        .codes,
    ).toEqual(["status-not-approvable"]);
  });

  it("blocks while an apply request is in flight", () => {
    const plan = makePlan({
      status: "approved",
      approval: {
        approval_schema_version: 1,
        approved_at: "2026-01-01 00:01:00",
        approved_by: "operator-1",
        correlation_id: "corr-1",
      },
    });
    expect(applyBlockers(plan, { ...ready, applyInFlight: true }).codes).toEqual([
      "status-not-approvable",
    ]);
  });

  it("blocks a stale approved plan", () => {
    const plan = makePlan({
      status: "approved",
      approval: {
        approval_schema_version: 1,
        approved_at: "2026-01-01 00:01:00",
        approved_by: "operator-1",
        correlation_id: "corr-1",
      },
    });
    const current = {
      authoritativeLoaded: true,
      currentReconciliationFingerprint: HEX_C,
      applyInFlight: false,
    };
    expect(applyBlockers(plan, current).codes).toEqual(["stale-fingerprint"]);
  });

  it("blocks without authoritative state", () => {
    expect(
      applyBlockers(null, {
        authoritativeLoaded: true,
        currentReconciliationFingerprint: HEX_B,
        applyInFlight: false,
      }),
    ).toEqual({ codes: ["no-authoritative-state"] });
  });
});

describe("idempotency keys", () => {
  it("matches the portable key alphabet and stays within 128 characters", () => {
    const plan = makePlan({ plan_id: "rp_odd/id with spaces" });
    for (const key of [
      approvalIdempotencyKey(plan, "operator-1"),
      applyIdempotencyKey(plan),
    ]) {
      expect(key.length).toBeLessThanOrEqual(128);
      expect(key).toMatch(/^[A-Za-z0-9][A-Za-z0-9._:-]*$/);
    }
  });

  it("is deterministic for the same plan across repeated calls", () => {
    const plan = makePlan();
    expect(approvalIdempotencyKey(plan, "operator-1")).toBe(
      approvalIdempotencyKey(plan, "operator-1"),
    );
    expect(applyIdempotencyKey(plan)).toBe(applyIdempotencyKey(plan));
  });

  it("is stable under re-created plan objects with identical fields", () => {
    expect(approvalIdempotencyKey(makePlan(), "operator-1")).toBe(
      approvalIdempotencyKey(makePlan(), "operator-1"),
    );
    expect(applyIdempotencyKey(makePlan())).toBe(applyIdempotencyKey(makePlan()));
  });

  it("differs between approve and apply", () => {
    const plan = makePlan();
    expect(approvalIdempotencyKey(plan, "operator-1")).not.toBe(
      applyIdempotencyKey(plan),
    );
  });

  it("changes when the plan fingerprints change", () => {
    expect(approvalIdempotencyKey(makePlan(), "operator-1")).not.toBe(
      approvalIdempotencyKey(makePlan({ content_fingerprint: HEX_C }), "operator-1"),
    );
  });

  it("changes when the approval operator changes", () => {
    const plan = makePlan();
    expect(approvalIdempotencyKey(plan, "operator-1")).not.toBe(
      approvalIdempotencyKey(plan, "operator-2"),
    );
  });
});

describe("describePlanStatus", () => {
  it("maps every closed status to a labeled tone", () => {
    expect(describePlanStatus("proposed")).toEqual({ label: "Proposed", tone: "info" });
    expect(describePlanStatus("approved")).toEqual({ label: "Approved", tone: "info" });
    expect(describePlanStatus("applying")).toEqual({
      label: "Applying",
      tone: "warning",
    });
    expect(describePlanStatus("applied")).toEqual({
      label: "Applied",
      tone: "success",
    });
    expect(describePlanStatus("rejected")).toEqual({
      label: "Rejected",
      tone: "failure",
    });
    expect(describePlanStatus("failed")).toEqual({ label: "Failed", tone: "failure" });
  });

  it("renders a foreign status truthfully in a neutral tone", () => {
    const view = describePlanStatus("mystery");
    expect(view.tone).toBe("neutral");
    expect(view.label).toContain("mystery");
  });
});

describe("describeDisposition", () => {
  it("treats completed and already_applied as the two successes", () => {
    expect(describeDisposition("completed")).toEqual({
      label: "Completed",
      tone: "success",
      isSuccess: true,
    });
    const replay = describeDisposition("already_applied");
    expect(replay.isSuccess).toBe(true);
    expect(replay.label).toMatch(/idempotent replay/i);
  });

  it("treats failed, unresolved, and interrupted as non-successes", () => {
    expect(describeDisposition("failed").isSuccess).toBe(false);
    expect(describeDisposition("failed").tone).toBe("failure");
    const unresolved = describeDisposition("unresolved");
    expect(unresolved.isSuccess).toBe(false);
    expect(unresolved.label).toMatch(/retry resumes the same plan/i);
    const interrupted = describeDisposition("interrupted");
    expect(interrupted.isSuccess).toBe(false);
    expect(interrupted.label).toMatch(/retry to resume/i);
  });

  it("treats an unknown disposition as a non-success failure", () => {
    const view = describeDisposition("mystery");
    expect(view.isSuccess).toBe(false);
    expect(view.tone).toBe("failure");
  });
});

describe("describePlanBlocker", () => {
  it("gives every blocker code human-readable text", () => {
    for (const code of [
      "no-authoritative-state",
      "status-not-approvable",
      "already-applied",
      "already-approved",
      "unsupported-action",
      "stale-fingerprint",
      "fingerprint-missing",
    ] as const) {
      expect(describePlanBlocker(code).length).toBeGreaterThan(0);
    }
  });
});

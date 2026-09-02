/**
 * Pure contract rules for the reconciliation-and-repair review surface.
 *
 * Everything here is derived from the authoritative repair plan response
 * (GET /api/v1/repair-plans/{plan_id}) and from the verified backend
 * contract in src/paritygrid/api/schemas/repair.py. No React, no I/O, no
 * clocks: every function is deterministic so the same plan always yields
 * the same presentation and the same idempotency keys.
 *
 * Two safety rules are encoded here and must stay encoded:
 * - Only `create_target` and `update_target` actions exist in this product.
 *   Any other kind string is treated as unsupported and destructive: it is
 *   still displayed (never hidden) but it blocks approval and application.
 * - `before_sha256` is the empty string exactly when the action is a
 *   `create_target` (the sentinel for an absent before-state); an
 *   `update_target` action must always carry a valid 64-hex before
 *   fingerprint, and `proposed_after_sha256` is always valid 64-hex.
 */
import type {
  RepairActionResponse,
  RepairPlanResponse,
} from "../../api/generated/schema";

export const REPAIR_PLAN_STATUSES = [
  "proposed",
  "approved",
  "applying",
  "applied",
  "rejected",
  "failed",
] as const;

export type RepairPlanStatusValue = (typeof REPAIR_PLAN_STATUSES)[number];

export const REPAIR_ACTION_STATUSES = ["pending", "applied", "failed"] as const;

export type RepairActionStatusValue = (typeof REPAIR_ACTION_STATUSES)[number];

/**
 * Dispositions of POST /api/v1/repair-plans/{plan_id}/apply. `completed`
 * and `already_applied` are the two successes; `already_applied` is the
 * idempotent replay of a previously stored result. `unresolved` and
 * `interrupted` arrive with HTTP 503 and are recoverable: retrying resumes
 * the SAME plan, so a retry reuses the same idempotency key.
 */
export const REPAIR_APPLY_DISPOSITIONS = [
  "completed",
  "already_applied",
  "failed",
  "unresolved",
  "interrupted",
] as const;

export type RepairApplyDispositionValue = (typeof REPAIR_APPLY_DISPOSITIONS)[number];

export const SUPPORTED_ACTION_KINDS = ["create_target", "update_target"] as const;

export type SupportedActionKind = (typeof SUPPORTED_ACTION_KINDS)[number];

/** Backend fingerprint values are exactly 64 lowercase hex characters. */
const HEX_64 = /^[0-9a-f]{64}$/;

export function isSupportedActionKind(kind: string): kind is SupportedActionKind {
  return kind === "create_target" || kind === "update_target";
}

/**
 * Only a plan in the `proposed` status can be approved. Approved, applying,
 * applied, rejected, and failed plans are all past the approval window.
 */
export function isPlanApprovable(status: string): boolean {
  return status === "proposed";
}

/** Why a command (approve or apply) is currently refused, in fixed order. */
export type PlanBlockerCode =
  | "no-authoritative-state"
  | "status-not-approvable"
  | "already-applied"
  | "already-approved"
  | "approval-missing"
  | "unsupported-action"
  | "stale-fingerprint"
  | "fingerprint-missing";

export interface PlanBlockers {
  /** Deterministic: fixed canonical order, no duplicates. */
  codes: PlanBlockerCode[];
}

const BLOCKER_ORDER: readonly PlanBlockerCode[] = [
  "no-authoritative-state",
  "status-not-approvable",
  "already-applied",
  "already-approved",
  "approval-missing",
  "unsupported-action",
  "stale-fingerprint",
  "fingerprint-missing",
];

function blockersFrom(codes: readonly PlanBlockerCode[]): PlanBlockers {
  const present = new Set<PlanBlockerCode>(codes);
  return { codes: BLOCKER_ORDER.filter((code) => present.has(code)) };
}

/** Human-readable explanation for one blocker, shown in blocked panels. */
export function describePlanBlocker(code: PlanBlockerCode): string {
  switch (code) {
    case "no-authoritative-state":
      return "The authoritative plan has not been loaded, so no safety comparison is possible yet.";
    case "status-not-approvable":
      return "The plan status does not allow this command right now.";
    case "already-applied":
      return "The plan has already been applied; approval and further application are disabled.";
    case "already-approved":
      return "The plan already carries a durable approval and cannot be approved a second time.";
    case "approval-missing":
      return "The plan claims an approved lifecycle state but carries no durable approval evidence.";
    case "unsupported-action":
      return "The plan contains an unsupported or destructive action kind, or a fingerprint cross-field inconsistency.";
    case "stale-fingerprint":
      return "The run has re-reconciled since this plan was created, so the plan is stale and superseded.";
    case "fingerprint-missing":
      return "The current run reconciliation fingerprint is unavailable, so staleness cannot be ruled out.";
  }
}

function hasUnsupportedAction(plan: RepairPlanResponse): boolean {
  return plan.actions.some((action) => actionSentinelProblem(action) !== null);
}

/**
 * Fingerprint fence shared by both commands: the run's current
 * reconciliation fingerprint must be known and must still equal the
 * fingerprint the plan was derived from, otherwise the plan is stale
 * (the run re-reconciled) or the comparison is impossible.
 */
function fingerprintBlockers(
  plan: RepairPlanResponse,
  currentReconciliationFingerprint: string | null,
): PlanBlockerCode[] {
  if (currentReconciliationFingerprint === null) {
    return ["fingerprint-missing"];
  }
  return currentReconciliationFingerprint === plan.reconciliation_fingerprint
    ? []
    : ["stale-fingerprint"];
}

export interface RepairAuthoritativeState {
  authoritativeLoaded: boolean;
  currentReconciliationFingerprint: string | null;
}

/**
 * Reasons the approve command is refused. Empty codes means the explicit
 * confirmation form may be offered. Every refusal is visible, never silent.
 */
export function approvalBlockers(
  plan: RepairPlanResponse | null,
  current: RepairAuthoritativeState,
): PlanBlockers {
  if (plan === null || !current.authoritativeLoaded) {
    return blockersFrom(["no-authoritative-state"]);
  }
  const codes: PlanBlockerCode[] = [];
  if (!isPlanApprovable(plan.status)) {
    codes.push("status-not-approvable");
  }
  if (plan.status === "applied") {
    codes.push("already-applied");
  }
  if (plan.approval != null) {
    codes.push("already-approved");
  }
  if (hasUnsupportedAction(plan)) {
    codes.push("unsupported-action");
  }
  codes.push(...fingerprintBlockers(plan, current.currentReconciliationFingerprint));
  return blockersFrom(codes);
}

export interface RepairApplyState extends RepairAuthoritativeState {
  /** True while this client has an apply request in flight. */
  applyInFlight: boolean;
}

/**
 * Reasons the apply command is refused. Apply requires the `approved`
 * status (a durable approval must exist first): `proposed` is pre-approval,
 * `applying` means another application owns the plan, `applied` is finished
 * (apply is disabled permanently), and `rejected`/`failed` are terminal.
 */
export function applyBlockers(
  plan: RepairPlanResponse | null,
  current: RepairApplyState,
): PlanBlockers {
  if (plan === null || !current.authoritativeLoaded) {
    return blockersFrom(["no-authoritative-state"]);
  }
  const codes: PlanBlockerCode[] = [];
  if (plan.status !== "approved") {
    codes.push("status-not-approvable");
  }
  if (plan.status === "applied") {
    codes.push("already-applied");
  }
  if (plan.status === "approved" && plan.approval == null) {
    codes.push("approval-missing");
  }
  if (current.applyInFlight) {
    // Local in-flight guard: one application at a time from this client.
    // The status code carries the refusal because the union is shared with
    // approval and the plan status itself may still read `approved`.
    codes.push("status-not-approvable");
  }
  if (hasUnsupportedAction(plan)) {
    codes.push("unsupported-action");
  }
  codes.push(...fingerprintBlockers(plan, current.currentReconciliationFingerprint));
  return blockersFrom(codes);
}

/**
 * Cross-field check of one action's fingerprints. Returns a
 * human-readable problem string, or null when the action is internally
 * consistent. A foreign kind is itself the problem, but the action is
 * still displayed by callers (fail visibly, never silently).
 */
export function actionSentinelProblem(action: RepairActionResponse): string | null {
  if (!isSupportedActionKind(action.kind)) {
    return `unsupported action kind ${action.kind}`;
  }
  if (!HEX_64.test(action.proposed_after_sha256)) {
    return `action ${action.action_id}: proposed-after fingerprint must be 64 lowercase hex characters`;
  }
  if (action.kind === "create_target") {
    if (action.before_sha256 === "") {
      return null;
    }
    return `action ${action.action_id}: create_target before fingerprint must use the empty-string sentinel`;
  }
  if (!HEX_64.test(action.before_sha256)) {
    return `action ${action.action_id}: update_target requires a before fingerprint of 64 lowercase hex characters`;
  }
  return null;
}

export interface PlanActionSummary {
  total: number;
  pending: number;
  applied: number;
  failed: number;
  /**
   * Foreign kind strings found on the plan, deduplicated in
   * first-appearance order and bounded. Listed so the failure is visible.
   */
  unsupported: string[];
}

const MAX_UNSUPPORTED_LISTED = 10;

export function planActionSummary(plan: RepairPlanResponse): PlanActionSummary {
  let pending = 0;
  let applied = 0;
  let failed = 0;
  const unsupported: string[] = [];
  for (const action of plan.actions) {
    if (action.status === "pending") {
      pending += 1;
    } else if (action.status === "applied") {
      applied += 1;
    } else if (action.status === "failed") {
      failed += 1;
    }
    if (!isSupportedActionKind(action.kind) && !unsupported.includes(action.kind)) {
      if (unsupported.length < MAX_UNSUPPORTED_LISTED) {
        unsupported.push(action.kind);
      }
    }
  }
  return { total: plan.actions.length, pending, applied, failed, unsupported };
}

function keyIdentityFragment(planId: string): string {
  // The key alphabet is the server's portable identity alphabet; any other
  // character is replaced deterministically and the fragment is capped so
  // the composed key can never exceed the 128-character header bound.
  const sanitized = planId.replace(/[^A-Za-z0-9._:-]/g, "-").slice(0, 64);
  return sanitized === "" ? "plan" : sanitized;
}

function planMutationKey(prefix: string, plan: RepairPlanResponse): string {
  return `${prefix}-${keyIdentityFragment(plan.plan_id)}-${plan.content_fingerprint.slice(0, 16)}-${plan.reconciliation_fingerprint.slice(0, 16)}`;
}

/**
 * Stable idempotency key for one logical approval. Derived only from the
 * plan identity and its fingerprints, so every retry of the same approval
 * reuses the exact same key and the server replays the stored outcome.
 */
function approvalActorFragment(approvedBy: string): string {
  let hash = 2_166_136_261;
  for (let index = 0; index < approvedBy.length; index += 1) {
    hash ^= approvedBy.charCodeAt(index);
    hash = Math.imul(hash, 16_777_619);
  }
  return (hash >>> 0).toString(16).padStart(8, "0");
}

export function approvalIdempotencyKey(
  plan: RepairPlanResponse,
  approvedBy: string,
): string {
  return `${planMutationKey("apr", plan)}-${approvalActorFragment(approvedBy)}`;
}

/**
 * Stable idempotency key for one logical application. Same derivation as
 * the approval key under the `app-` prefix, so unresolved/interrupted
 * retries resume the same plan with the same key.
 */
export function applyIdempotencyKey(plan: RepairPlanResponse): string {
  return planMutationKey("app", plan);
}

export interface PlanStatusView {
  label: string;
  tone: "neutral" | "info" | "success" | "warning" | "failure";
}

export function describePlanStatus(status: string): PlanStatusView {
  switch (status) {
    case "proposed":
      return { label: "Proposed", tone: "info" };
    case "approved":
      return { label: "Approved", tone: "info" };
    case "applying":
      return { label: "Applying", tone: "warning" };
    case "applied":
      return { label: "Applied", tone: "success" };
    case "rejected":
      return { label: "Rejected", tone: "failure" };
    case "failed":
      return { label: "Failed", tone: "failure" };
    default:
      // A foreign status string is displayed truthfully, never mapped away.
      return {
        label:
          status === "" ? "Unknown status" : `Unrecognized: ${status.slice(0, 64)}`,
        tone: "neutral",
      };
  }
}

export interface ApplyDispositionView {
  label: string;
  tone: "success" | "warning" | "failure" | "neutral";
  isSuccess: boolean;
}

export function describeDisposition(disposition: string): ApplyDispositionView {
  switch (disposition) {
    case "completed":
      return { label: "Completed", tone: "success", isSuccess: true };
    case "already_applied":
      return {
        label: "Already applied — idempotent replay",
        tone: "success",
        isSuccess: true,
      };
    case "failed":
      return { label: "Failed", tone: "failure", isSuccess: false };
    case "unresolved":
      return {
        label: "Unresolved — recoverable, retry resumes the same plan",
        tone: "warning",
        isSuccess: false,
      };
    case "interrupted":
      return {
        label: "Interrupted — retry to resume",
        tone: "failure",
        isSuccess: false,
      };
    default:
      return { label: "Unknown outcome", tone: "failure", isSuccess: false };
  }
}

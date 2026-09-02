/**
 * Deterministic API fixtures for the Phase 18 browser workflows:
 * reconciliation summary, a 10,000-conflict logical dataset served as lazy
 * cursor pages, repair plans with approval and application commands, and
 * the repair durable event stream. Every body satisfies the strict runtime
 * schemas the production client enforces; identifiers, fingerprints, and
 * timestamps are fixed synthetic strings so screenshots are reproducible.
 */
import type { Page } from "@playwright/test";

import {
  capabilitiesE2E,
  durableFrameE2E,
  HEALTH_OK,
  runE2E,
  sseBody,
} from "./fixtures";

export const RECONCILE_RUN_ID = "run_e2e-reconcile-001";
export const REPAIR_PLAN_ID = "rpl_e2e-repair-001";
export const REPAIR_RUN_ID = "run_e2e-repair-run-001";

/** Fixed 64-hex fingerprints; letters differ so mismatches are visible. */
export const FINGERPRINT_RECON = "a1".repeat(32);
export const FINGERPRINT_RECON_NEW = "b2".repeat(32);
export const FINGERPRINT_SOURCE = "c3".repeat(32);
export const FINGERPRINT_TARGET = "d4".repeat(32);
export const FINGERPRINT_CONTENT = "e5".repeat(32);

export const PAGE_SIZE_E2E = 100;
export const TOTAL_CONFLICTS_E2E = 10_000;

/** Uppercase cursor tokens matching the server's canonical cursor pattern. */
export function conflictCursor(pageIndex: number): string {
  return `SKU-${String(pageIndex * PAGE_SIZE_E2E - 1).padStart(5, "0")}`;
}

const CONFLICT_CLASSIFICATIONS_E2E = [
  "missing_from_target",
  "missing_from_source",
  "field_mismatch",
  "duplicate_source",
  "duplicate_target",
  "duplicate_both",
] as const;

const DIFFERENCE_KINDS_E2E = [
  "value_mismatch",
  "missing_on_source",
  "missing_on_target",
  "null_on_source",
  "null_on_target",
  "type_mismatch",
] as const;

/** One synthetic conflict; content is fixed so screenshots are stable. */
export function conflictE2E(index: number): Record<string, unknown> {
  const classification =
    CONFLICT_CLASSIFICATIONS_E2E[index % CONFLICT_CLASSIFICATIONS_E2E.length];
  const kind = DIFFERENCE_KINDS_E2E[index % DIFFERENCE_KINDS_E2E.length];
  const key = `SKU-${String(index).padStart(5, "0")}`;
  const differences: Record<string, unknown>[] =
    classification === "field_mismatch"
      ? [
          {
            field: "price",
            kind: "value_mismatch",
            source_text: `1999.00-${String(index)}`,
            target_text: `2499.00-${String(index)}`,
          },
          {
            field: "title",
            kind,
            source_text:
              index % 3 === 0 ? "Cafe CHECKmark unicode title" : "Standard title",
            target_text: "Standard title",
          },
          {
            field: "notes",
            kind: "value_mismatch",
            source_text:
              "Long value that keeps going so the inspector can show truncation behavior for extended source payloads in a deterministic way",
            target_text: "Short",
          },
        ]
      : [];
  const suggestedResolution =
    classification === "missing_from_target"
      ? "create_target"
      : classification === "missing_from_source"
        ? "review_target_only"
        : classification === "field_mismatch"
          ? "update_target"
          : "review_duplicates";
  return {
    schema_version: 1,
    conflict_id: `cnf_e2e-${String(index).padStart(5, "0")}`,
    canonical_key: key,
    classification,
    source_references: [{ position: index, record_key: key }],
    target_references:
      classification === "missing_from_target"
        ? []
        : [{ position: index, record_key: key }],
    differences,
    suggested_resolution: suggestedResolution,
    created_at: "2026-03-01 12:00:00",
  };
}

export function reconciliationE2E(
  overrides: Record<string, unknown> = {},
): Record<string, unknown> {
  const counts: Record<string, number> = {
    match: 2_000,
    missing_from_target: 2_000,
    missing_from_source: 1_500,
    field_mismatch: 3_000,
    duplicate_source: 1_250,
    duplicate_target: 1_250,
    duplicate_both: 1_000,
  };
  return {
    schema_version: 1,
    run_id: RECONCILE_RUN_ID,
    run_version: 3,
    state: "succeeded",
    observed_at: "2026-03-01 12:30:00",
    reconciliation_fingerprint: FINGERPRINT_RECON,
    source_input_identity: FINGERPRINT_SOURCE,
    target_input_identity: FINGERPRINT_TARGET,
    total_count: 12_000,
    counts,
    analytical_query_version: 1,
    reconciliation_observed_at: "2026-03-01 12:00:05",
    ...overrides,
  };
}

/** A coherent all-match snapshot: records exist, but there are no conflicts. */
export function emptyReconciliationE2E(): Record<string, unknown> {
  return reconciliationE2E({
    total_count: 10_000,
    counts: {
      match: 10_000,
      missing_from_target: 0,
      missing_from_source: 0,
      field_mismatch: 0,
      duplicate_source: 0,
      duplicate_target: 0,
      duplicate_both: 0,
    },
  });
}

export interface ConflictIdentity {
  run_id: string;
  run_version: number;
  reconciliation_fingerprint: string;
}

export const RECONCILE_IDENTITY: ConflictIdentity = {
  run_id: RECONCILE_RUN_ID,
  run_version: 3,
  reconciliation_fingerprint: FINGERPRINT_RECON,
};

export function conflictsPageE2E(
  pageIndex: number,
  identity: ConflictIdentity = RECONCILE_IDENTITY,
): Record<string, unknown> {
  const start = pageIndex * PAGE_SIZE_E2E;
  const isLast = start + PAGE_SIZE_E2E >= TOTAL_CONFLICTS_E2E;
  return {
    schema_version: 1,
    run_id: identity.run_id,
    run_version: identity.run_version,
    state: "succeeded",
    observed_at: "2026-03-01 12:30:00",
    reconciliation_fingerprint: identity.reconciliation_fingerprint,
    limit: PAGE_SIZE_E2E,
    next_cursor: isLast ? null : conflictCursor(pageIndex + 1),
    items: Array.from({ length: PAGE_SIZE_E2E }, (_, offset) =>
      conflictE2E(start + offset),
    ),
  };
}

export interface ReconcileRouteOptions {
  /** Durable run version served for the run detail (drives the stale state). */
  readonly runVersion?: number;
  readonly summary?: Record<string, unknown>;
  /** Serve this one page body once (e.g. a mismatched snapshot page). */
  readonly overridePage?: { pageIndex: number; body: Record<string, unknown> };
}

export interface ReconcileRoutes {
  readonly conflictRequests: () => string[];
  readonly runFetches: () => number;
}

/** Install routes for the reconciliation workbench route. */
export async function installReconcileRoutes(
  page: Page,
  options: ReconcileRouteOptions = {},
): Promise<ReconcileRoutes> {
  const conflictRequests: string[] = [];
  let runFetches = 0;
  const runVersion = options.runVersion ?? 3;

  await page.route("**/healthz", (route) =>
    route.fulfill({ status: 200, contentType: "application/json", body: HEALTH_OK }),
  );
  await page.route("**/readyz", (route) =>
    route.fulfill({
      status: 200,
      contentType: "application/json",
      body: JSON.stringify({
        status: "ready",
        service: "ParityGrid",
        version: "0.1.0",
        detail: "migrations applied",
      }),
    }),
  );
  await page.route("**/api/v1/system/capabilities", (route) =>
    route.fulfill({
      status: 200,
      contentType: "application/json",
      body: JSON.stringify(capabilitiesE2E()),
    }),
  );
  await page.route(new RegExp(`/api/v1/runs/${RECONCILE_RUN_ID}$`), (route) => {
    runFetches += 1;
    return route.fulfill({
      status: 200,
      contentType: "application/json",
      body: JSON.stringify(
        runE2E({
          run_id: RECONCILE_RUN_ID,
          run_version: runVersion,
          state: "succeeded",
        }),
      ),
    });
  });
  await page.route(
    new RegExp(`/api/v1/runs/${RECONCILE_RUN_ID}/reconciliation`),
    (route) =>
      route.fulfill({
        status: 200,
        contentType: "application/json",
        body: JSON.stringify(options.summary ?? reconciliationE2E()),
      }),
  );
  await page.route(
    new RegExp(`/api/v1/runs/${RECONCILE_RUN_ID}/conflicts.*`),
    (route) => {
      const url = new URL(route.request().url());
      const cursor = url.searchParams.get("cursor");
      conflictRequests.push(url.search);
      const pageIndex =
        cursor === null
          ? 0
          : Array.from(
              { length: TOTAL_CONFLICTS_E2E / PAGE_SIZE_E2E - 1 },
              (_, index) => index + 1,
            ).find((index) => conflictCursor(index) === cursor);
      if (pageIndex === undefined) {
        return route.fulfill({
          status: 422,
          contentType: "application/problem+json",
          body: JSON.stringify({
            type: "about:blank",
            title: "Invalid cursor",
            status: 422,
          }),
        });
      }
      const override = options.overridePage;
      if (override?.pageIndex === pageIndex) {
        return route.fulfill({
          status: 200,
          contentType: "application/json",
          body: JSON.stringify(override.body),
        });
      }
      return route.fulfill({
        status: 200,
        contentType: "application/json",
        body: JSON.stringify(conflictsPageE2E(pageIndex)),
      });
    },
  );
  await page.route("**/api/v1/runs?*", (route) =>
    route.fulfill({
      status: 200,
      contentType: "application/json",
      body: JSON.stringify({
        schema_version: 1,
        items: [runE2E({ run_id: RECONCILE_RUN_ID, state: "succeeded" })],
        limit: 50,
        next_cursor: null,
      }),
    }),
  );
  return {
    conflictRequests: () => [...conflictRequests],
    runFetches: () => runFetches,
  };
}

// ---------------------------------------------------------------------------
// Repair plan fixtures and routes.
// ---------------------------------------------------------------------------

export function repairPlanE2E(
  overrides: Record<string, unknown> = {},
): Record<string, unknown> {
  // `approval: {}` is the supported shorthand for "carries a durable
  // approval"; a raw spread would otherwise inject an empty object that the
  // strict runtime schema correctly rejects.
  const {
    approval: approvalOverride,
    status: statusOverride,
    actions: actionsOverride,
    ...rest
  } = overrides;
  const status = typeof statusOverride === "string" ? statusOverride : "proposed";
  const approval =
    approvalOverride === undefined
      ? null
      : approvalOverride === null
        ? null
        : {
            approval_schema_version: 1,
            approved_at: "2026-03-01 12:10:00",
            approved_by: "operator-e2e",
            correlation_id: "corr-approve-001",
            schema_version: 1,
          };
  const applied = status === "applied";
  return {
    schema_version: 1,
    plan_id: REPAIR_PLAN_ID,
    run_id: REPAIR_RUN_ID,
    run_version: 4,
    state: "succeeded",
    observed_at: "2026-03-01 12:30:00",
    status,
    reconciliation_fingerprint: FINGERPRINT_RECON,
    content_fingerprint: FINGERPRINT_CONTENT,
    created_at: "2026-03-01 12:05:00",
    applying_at: status === "applying" ? "2026-03-01 12:19:00" : null,
    applied_at: applied ? "2026-03-01 12:20:00" : null,
    rejected_at: null,
    failed_at: status === "failed" ? "2026-03-01 12:20:00" : null,
    ...rest,
    approval,
    actions: actionsOverride ?? [
      {
        schema_version: 1,
        action_id: "rac_e2e-001",
        canonical_key: "sku-00001",
        kind: "create_target",
        status: applied ? "applied" : "pending",
        before_sha256: "",
        proposed_after_sha256: "11".repeat(32),
        applied_at: null,
        failed_at: null,
        target_version: null,
      },
      {
        schema_version: 1,
        action_id: "rac_e2e-002",
        canonical_key: "sku-00002",
        kind: "update_target",
        status: applied ? "applied" : "pending",
        before_sha256: "22".repeat(32),
        proposed_after_sha256: "33".repeat(32),
        applied_at: null,
        failed_at: null,
        target_version: null,
      },
    ],
  };
}

/** One repair durable frame bound to the fixture plan's fingerprints. */
export function repairFrameE2E(
  sequence: number,
  eventKind: string,
  payload: Record<string, unknown>,
): Record<string, unknown> {
  return durableFrameE2E(sequence, {
    run_id: REPAIR_RUN_ID,
    event_kind: eventKind,
    subject_kind: "run",
    subject_id: REPAIR_RUN_ID,
    occurred_at: "2026-03-01 12:20:00",
    payload,
  });
}

export const REPAIR_STARTED_PAYLOAD = {
  action_count: 2,
  content_fingerprint: FINGERPRINT_CONTENT,
  reconciliation_fingerprint: FINGERPRINT_RECON,
};

export const REPAIR_COMPLETED_PAYLOAD = REPAIR_STARTED_PAYLOAD;

export function repairApplyResponseE2E(
  disposition: string,
  overrides: Record<string, unknown> = {},
): Record<string, unknown> {
  return {
    schema_version: 1,
    plan_id: REPAIR_PLAN_ID,
    run_id: REPAIR_RUN_ID,
    run_version: 5,
    state: "succeeded",
    observed_at: "2026-03-01 12:20:05",
    status: disposition === "failed" ? "failed" : "applied",
    reconciliation_fingerprint: FINGERPRINT_RECON,
    content_fingerprint: FINGERPRINT_CONTENT,
    disposition,
    resumed: false,
    effects: [],
    ...overrides,
  };
}

export interface RepairRouteOptions {
  /** The plan body every GET returns (mutate via setPlan during the test). */
  readonly plan?: Record<string, unknown>;
  /**
   * Durable frames per connection. The handler models the server resume
   * contract: each connection serves only frames with a sequence greater
   * than the `after` resume position it requested.
   */
  readonly sseFrames?: Record<string, unknown>[][];
  /** Behavior of POST approve/apply; defaults to success acknowledgements. */
  readonly approveStatus?: number;
  readonly approveBody?: Record<string, unknown>;
  readonly applyStatus?: number;
  readonly applyBody?: Record<string, unknown>;
  /** Reconciliation fingerprint the fence query returns (staleness). */
  readonly fenceFingerprint?: string;
}

export interface RepairRoutes {
  readonly setPlan: (next: Record<string, unknown>) => void;
  readonly approvals: () => { body: string; key: string | null }[];
  readonly applies: () => { key: string | null }[];
  readonly planFetches: () => number;
  readonly sseConnections: () => string[];
}

/** Install routes for the repair review route. */
export async function installRepairRoutes(
  page: Page,
  options: RepairRouteOptions = {},
): Promise<RepairRoutes> {
  let plan = options.plan ?? repairPlanE2E();
  const approvals: { body: string; key: string | null }[] = [];
  const applies: { key: string | null }[] = [];
  let planFetches = 0;
  const sseUrls: string[] = [];

  await page.route("**/healthz", (route) =>
    route.fulfill({ status: 200, contentType: "application/json", body: HEALTH_OK }),
  );
  await page.route("**/readyz", (route) =>
    route.fulfill({
      status: 200,
      contentType: "application/json",
      body: JSON.stringify({
        status: "ready",
        service: "ParityGrid",
        version: "0.1.0",
        detail: "migrations applied",
      }),
    }),
  );
  await page.route("**/api/v1/system/capabilities", (route) =>
    route.fulfill({
      status: 200,
      contentType: "application/json",
      body: JSON.stringify(capabilitiesE2E()),
    }),
  );
  await page.route(new RegExp(`/api/v1/repair-plans/${REPAIR_PLAN_ID}$`), (route) => {
    planFetches += 1;
    return route.fulfill({
      status: 200,
      contentType: "application/json",
      body: JSON.stringify(plan),
    });
  });
  await page.route(
    new RegExp(`/api/v1/repair-plans/${REPAIR_PLAN_ID}/approve`),
    (route) => {
      const request = route.request();
      approvals.push({
        body: request.postData() ?? "",
        key: request.headers()["idempotency-key"] ?? null,
      });
      if (options.approveStatus !== undefined && options.approveStatus >= 400) {
        return route.fulfill({
          status: options.approveStatus,
          contentType: "application/problem+json",
          body: JSON.stringify({
            type: "about:blank",
            title: "Approval refused",
            status: options.approveStatus,
            detail: "the plan state does not allow approval",
          }),
        });
      }
      plan = options.approveBody ?? repairPlanE2E({ status: "approved", approval: {} });
      return route.fulfill({
        status: 200,
        contentType: "application/json",
        body: JSON.stringify(plan),
      });
    },
  );
  await page.route(
    new RegExp(`/api/v1/repair-plans/${REPAIR_PLAN_ID}/apply`),
    (route) => {
      const request = route.request();
      applies.push({ key: request.headers()["idempotency-key"] ?? null });
      const status = options.applyStatus ?? 200;
      if (status >= 400) {
        return route.fulfill({
          status,
          contentType: "application/problem+json",
          body: JSON.stringify({
            type: "about:blank",
            title: status === 503 ? "Application unresolved" : "Application refused",
            status,
            detail:
              status === 503
                ? "the application ended without a durable terminal outcome; retry resumes the same plan"
                : "the plan state does not allow application",
          }),
        });
      }
      plan = repairPlanE2E({ status: "applied", approval: {} });
      return route.fulfill({
        status: 200,
        contentType: "application/json",
        body: JSON.stringify(options.applyBody ?? repairApplyResponseE2E("completed")),
      });
    },
  );
  await page.route(
    new RegExp(`/api/v1/runs/${REPAIR_RUN_ID}/reconciliation`),
    (route) =>
      route.fulfill({
        status: 200,
        contentType: "application/json",
        body: JSON.stringify(
          reconciliationE2E({
            run_id: REPAIR_RUN_ID,
            run_version: 4,
            reconciliation_fingerprint: options.fenceFingerprint ?? FINGERPRINT_RECON,
          }),
        ),
      }),
  );
  await page.route("**/api/v1/runs?*", (route) =>
    route.fulfill({
      status: 200,
      contentType: "application/json",
      body: JSON.stringify({
        schema_version: 1,
        items: [runE2E({ run_id: REPAIR_RUN_ID, state: "succeeded" })],
        limit: 50,
        next_cursor: null,
      }),
    }),
  );
  await page.route(new RegExp(`/api/v1/stream/runs/${REPAIR_RUN_ID}.*`), (route) => {
    const url = new URL(route.request().url());
    sseUrls.push(url.href);
    const after = Number.parseInt(url.searchParams.get("after") ?? "0", 10) || 0;
    const frames = options.sseFrames ?? [];
    const body = sseBody(
      (frames[Math.min(sseUrls.length - 1, frames.length - 1)] ?? []).filter(
        (frame) => Number(frame.sequence) > after,
      ),
    );
    return route.fulfill({
      status: 200,
      contentType: "text/event-stream",
      body,
    });
  });
  await page.route(new RegExp(`/api/v1/runs/${REPAIR_RUN_ID}$`), (route) =>
    route.fulfill({
      status: 200,
      contentType: "application/json",
      body: JSON.stringify(
        runE2E({ run_id: REPAIR_RUN_ID, run_version: 4, state: "succeeded" }),
      ),
    }),
  );
  return {
    setPlan: (next: Record<string, unknown>) => {
      plan = next;
    },
    approvals: () => approvals.map((entry) => ({ ...entry })),
    applies: () => applies.map((entry) => ({ ...entry })),
    planFetches: () => planFetches,
    sseConnections: () => [...sseUrls],
  };
}

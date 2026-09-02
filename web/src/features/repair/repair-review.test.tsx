import { QueryClientProvider } from "@tanstack/react-query";
import { act, render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import type {
  RepairApplyResponse,
  RepairPlanResponse,
  ReconciliationResponse,
} from "../../api/generated/schema";
import { ApiRequestError } from "../../api/client";
import { createQueryClient } from "../../api/query-client";
import type {
  SseConnectionResult,
  SseMessage,
  SseTransport,
} from "../../live/durable-stream";
import { expectNoAccessibilityViolations } from "../../test/axe";
import { applyIdempotencyKey, approvalIdempotencyKey } from "./repair-contract";
import { RepairReview } from "./repair-review";

const clientMocks = vi.hoisted(() => ({
  fetchRepairPlan: vi.fn(),
  fetchReconciliation: vi.fn(),
  approveRepairPlan: vi.fn(),
  applyRepairPlan: vi.fn(),
}));

vi.mock("../../api/client", async (importOriginal) => {
  // The import-type annotation is required to keep the mock factory fully
  // typed against the real module surface.
  // eslint-disable-next-line @typescript-eslint/consistent-type-imports
  const actual = await importOriginal<typeof import("../../api/client")>();
  return {
    ...actual,
    fetchRepairPlan: clientMocks.fetchRepairPlan,
    fetchReconciliation: clientMocks.fetchReconciliation,
    approveRepairPlan: clientMocks.approveRepairPlan,
    applyRepairPlan: clientMocks.applyRepairPlan,
  };
});

const PLAN_ID = "rp_plan-001";
const RUN_ID = "run-001";
const HEX_A = "a".repeat(64);
const HEX_B = "b".repeat(64);
const HEX_C = "c".repeat(64);

function makePlan(overrides: Partial<RepairPlanResponse> = {}): RepairPlanResponse {
  return {
    actions: [
      {
        action_id: "act-1",
        applied_at: null,
        before_sha256: "",
        canonical_key: "record-1",
        failed_at: null,
        kind: "create_target",
        proposed_after_sha256: HEX_C,
        status: "pending",
        target_version: null,
      },
      {
        action_id: "act-2",
        applied_at: null,
        before_sha256: HEX_B,
        canonical_key: "record-2",
        failed_at: null,
        kind: "update_target",
        proposed_after_sha256: HEX_A,
        status: "pending",
        target_version: null,
      },
    ],
    applied_at: null,
    applying_at: null,
    approval: null,
    content_fingerprint: HEX_A,
    created_at: "2026-01-01 00:00:00",
    failed_at: null,
    observed_at: "2026-01-01 00:00:00",
    plan_id: PLAN_ID,
    reconciliation_fingerprint: HEX_B,
    rejected_at: null,
    run_id: RUN_ID,
    run_version: 3,
    state: "reconciled",
    status: "proposed",
    ...overrides,
  };
}

function approvedPlan(overrides: Partial<RepairPlanResponse> = {}): RepairPlanResponse {
  return makePlan({
    status: "approved",
    approval: {
      approval_schema_version: 1,
      approved_at: "2026-01-01 00:01:00",
      approved_by: "operator-1",
      correlation_id: "corr-approve-1",
    },
    ...overrides,
  });
}

function appliedPlan(): RepairPlanResponse {
  return approvedPlan({
    status: "applied",
    applied_at: "2026-01-01 00:02:00",
    actions: makePlan().actions.map((action) => ({
      ...action,
      status: "applied",
      applied_at: "2026-01-01 00:02:00",
    })),
  });
}

function makeReconciliation(
  overrides: Partial<ReconciliationResponse> = {},
): ReconciliationResponse {
  return {
    analytical_query_version: 1,
    counts: {},
    observed_at: "2026-01-01 00:00:00",
    reconciliation_fingerprint: HEX_B,
    reconciliation_observed_at: "2026-01-01 00:00:00",
    run_id: RUN_ID,
    run_version: 3,
    source_input_identity: HEX_A,
    state: "reconciled",
    target_input_identity: HEX_C,
    total_count: 0,
    ...overrides,
  };
}

function makeApplyResponse(
  overrides: Partial<RepairApplyResponse> = {},
): RepairApplyResponse {
  return {
    content_fingerprint: HEX_A,
    disposition: "completed",
    effects: [],
    observed_at: "2026-01-01 00:00:00",
    plan_id: PLAN_ID,
    reconciliation_fingerprint: HEX_B,
    resumed: false,
    run_id: RUN_ID,
    run_version: 3,
    state: "reconciled",
    status: "applied",
    ...overrides,
  };
}

function renderReview(sseTransport?: SseTransport): void {
  const queryClient = createQueryClient();
  render(
    <main>
      <QueryClientProvider client={queryClient}>
        <RepairReview planId={PLAN_ID} sseTransport={sseTransport} />
      </QueryClientProvider>
    </main>,
  );
}

interface FakeConnection {
  url: string;
  deliver: (message: SseMessage) => void;
  close: (result: SseConnectionResult) => void;
}

function createFakeStream(): {
  transport: SseTransport;
  connections: FakeConnection[];
} {
  const connections: FakeConnection[] = [];
  const transport: SseTransport = (url, callbacks) => {
    return new Promise<SseConnectionResult>((resolve) => {
      connections.push({
        url,
        deliver: (message) => {
          callbacks.onEvent(message);
        },
        close: (result) => {
          resolve(result);
        },
      });
    });
  };
  return { transport, connections };
}

function repairFrameMessage(overrides: {
  sequence: number;
  event: string;
  payload?: Record<string, unknown>;
}): SseMessage {
  const frame = {
    schema_version: 1,
    channel: "durable-events",
    sequence: overrides.sequence,
    run_id: RUN_ID,
    event_kind: overrides.event,
    subject_kind: "run",
    subject_id: RUN_ID,
    occurred_at: "2026-01-01 00:02:00",
    correlation_id: null,
    payload_schema_version: 1,
    payload: overrides.payload ?? {},
  };
  return {
    id: String(overrides.sequence),
    event: overrides.event,
    data: JSON.stringify(frame),
  };
}

/** A transport that never connects; keeps non-stream tests inert. */
const neverSettle = (): void => undefined;
const pendingTransport: SseTransport = () =>
  new Promise<SseConnectionResult>(neverSettle);

beforeEach(() => {
  clientMocks.fetchRepairPlan.mockReset();
  clientMocks.fetchReconciliation.mockReset();
  clientMocks.approveRepairPlan.mockReset();
  clientMocks.applyRepairPlan.mockReset();
  clientMocks.fetchReconciliation.mockResolvedValue(makeReconciliation());
});

afterEach(() => {
  vi.unstubAllGlobals();
});

describe("RepairReview", () => {
  it("renders the full review of a proposed plan and blocks a destructive action", async () => {
    const plan = makePlan({
      actions: [
        ...makePlan().actions,
        {
          action_id: "act-3",
          applied_at: null,
          before_sha256: "",
          canonical_key: "record-3",
          failed_at: null,
          kind: "delete_target",
          proposed_after_sha256: HEX_B,
          status: "pending",
          target_version: null,
        },
      ],
    });
    clientMocks.fetchRepairPlan.mockResolvedValue(plan);
    renderReview(pendingTransport);

    await screen.findByText(PLAN_ID);
    // Flush the run reconciliation fence resolution inside act.
    await act(async () => {
      await Promise.resolve();
    });
    expect(screen.getByTestId("plan-status")).toHaveTextContent("Proposed");
    expect(screen.getByTestId("plan-fingerprint")).toHaveTextContent(HEX_A);
    expect(screen.getByTestId("plan-reconciliation-fingerprint")).toHaveTextContent(
      HEX_B,
    );
    expect(screen.getByText(/run-001 · version 3 · state reconciled/)).toBeVisible();

    const table = screen.getByTestId("action-table");
    expect(table.textContent).toContain("none (create)");
    expect(table.textContent).toContain(HEX_B);
    expect(
      screen.getByText(/3 actions across 3 distinct canonical keys/),
    ).toBeVisible();
    expect(screen.getByTestId("action-unsupported-act-3")).toHaveTextContent(
      /unsupported\/destructive — blocked/,
    );
    expect(screen.getByText(/deletion actions do not exist/i)).toBeVisible();

    // The foreign kind blocks approval and application visibly.
    expect(screen.getByTestId("approval-blocked")).toBeVisible();
    expect(screen.getByTestId("apply-blocked")).toBeVisible();
    expect(screen.queryByTestId("approval-open")).toBeNull();
    expect(screen.queryByTestId("apply-button")).toBeNull();
    expect(clientMocks.approveRepairPlan).not.toHaveBeenCalled();
    expect(clientMocks.applyRepairPlan).not.toHaveBeenCalled();

    await expectNoAccessibilityViolations(document.body);
  });

  it("requires the explicit confirmation form before any approval POST", async () => {
    const plan = makePlan();
    clientMocks.fetchRepairPlan.mockResolvedValueOnce(plan).mockResolvedValue(
      makePlan({
        status: "approved",
        approval: {
          approval_schema_version: 1,
          approved_at: "2026-01-01 00:01:00",
          approved_by: "operator-1",
          correlation_id: "corr-approve-1",
        },
      }),
    );
    clientMocks.approveRepairPlan.mockResolvedValue(makePlan({ status: "approved" }));
    const user = userEvent.setup();
    renderReview(pendingTransport);

    await screen.findByTestId("approval-open");
    expect(screen.queryByTestId("approval-blocked")).toBeNull();
    await user.click(screen.getByTestId("approval-open"));

    const heading = await screen.findByText("Confirm approval");
    expect(heading).toBeVisible();
    // Focus moved to the confirmation heading.
    expect(heading).toHaveFocus();

    // 1) Submitting without an operator shows errors and sends nothing.
    await user.click(screen.getByTestId("approval-submit"));
    expect(screen.getByText(/operator identity is required/i)).toBeVisible();
    expect(screen.getByText(/content fingerprint must match/i)).toBeVisible();
    expect(clientMocks.approveRepairPlan).not.toHaveBeenCalled();

    // 2) A mismatched reviewed fingerprint blocks the POST as well.
    await user.type(screen.getByTestId("approval-operator"), "operator-1");
    await user.type(screen.getByTestId("approval-content-fingerprint"), "deadbeef");
    await user.click(
      screen.getByRole("button", { name: "Use displayed reconciliation fingerprint" }),
    );
    await user.click(screen.getByTestId("approval-submit"));
    expect(screen.getByText(/content fingerprint must match/i)).toBeVisible();
    expect(screen.queryByText(/operator identity is required/i)).toBeNull();
    expect(clientMocks.approveRepairPlan).not.toHaveBeenCalled();

    // 3) Exact reviewed fingerprints submit the exact body under the
    // deterministic key.
    await user.click(
      screen.getByRole("button", { name: "Use displayed content fingerprint" }),
    );
    await user.click(screen.getByTestId("approval-submit"));
    await waitFor(() => {
      expect(clientMocks.approveRepairPlan).toHaveBeenCalledTimes(1);
    });
    expect(clientMocks.approveRepairPlan.mock.calls[0]).toEqual([
      PLAN_ID,
      {
        schema_version: 1,
        approved_by: "operator-1",
        approved_content_fingerprint: HEX_A,
        approved_reconciliation_fingerprint: HEX_B,
      },
      { idempotencyKey: approvalIdempotencyKey(plan, "operator-1") },
    ]);

    // The authoritative plan query follows and shows the approved status.
    await waitFor(() => {
      expect(screen.getByTestId("plan-status")).toHaveTextContent("Approved");
    });
    expect(screen.getByText(/approved by/i)).toHaveTextContent("operator-1");
    expect(screen.queryByTestId("approval-open")).toBeNull();
  });

  it("blocks approval with a stale reason when the run re-reconciled", async () => {
    clientMocks.fetchRepairPlan.mockResolvedValue(makePlan());
    clientMocks.fetchReconciliation.mockResolvedValue(
      makeReconciliation({ reconciliation_fingerprint: HEX_C }),
    );
    renderReview(pendingTransport);

    await screen.findByTestId("approval-blocked");
    await waitFor(() => {
      expect(screen.getByTestId("approval-blocked")).toHaveTextContent(/re-reconciled/);
    });
    expect(screen.queryByTestId("approval-open")).toBeNull();
    expect(clientMocks.approveRepairPlan).not.toHaveBeenCalled();
  });

  it("blocks approval and disables apply for an already-applied plan", async () => {
    clientMocks.fetchRepairPlan.mockResolvedValue(appliedPlan());
    renderReview(pendingTransport);

    await screen.findByTestId("approval-blocked");
    expect(screen.getByTestId("approval-blocked")).toHaveTextContent(
      /already been applied/i,
    );
    expect(screen.getByTestId("apply-button")).toBeDisabled();
    expect(screen.getByTestId("apply-blocked")).toHaveTextContent(
      /already been applied/i,
    );
    expect(clientMocks.applyRepairPlan).not.toHaveBeenCalled();
  });

  it("applies an approved plan, disables apply afterwards, and refreshes on durable notifications", async () => {
    const plan = approvedPlan();
    clientMocks.fetchRepairPlan
      .mockResolvedValueOnce(plan)
      .mockResolvedValue(appliedPlan());
    clientMocks.applyRepairPlan.mockResolvedValue(makeApplyResponse());
    const stream = createFakeStream();
    const user = userEvent.setup();
    renderReview(stream.transport);

    const prepareApply = await screen.findByTestId("apply-open");
    expect(screen.queryByTestId("apply-blocked")).toBeNull();
    expect(clientMocks.applyRepairPlan).not.toHaveBeenCalled();

    await user.click(prepareApply);
    const applyButton = await screen.findByTestId("apply-button");
    expect(applyButton).toBeDisabled();
    expect(clientMocks.applyRepairPlan).not.toHaveBeenCalled();
    await user.click(screen.getByTestId("apply-confirmation"));
    expect(applyButton).toBeEnabled();

    await user.click(applyButton);
    await waitFor(() => {
      expect(clientMocks.applyRepairPlan).toHaveBeenCalledTimes(1);
    });
    expect(clientMocks.applyRepairPlan.mock.calls[0]).toEqual([
      PLAN_ID,
      { idempotencyKey: applyIdempotencyKey(plan) },
    ]);

    // Outcome success panel, and apply permanently disabled.
    await waitFor(() => {
      expect(screen.getByTestId("outcome-disposition")).toHaveTextContent("Completed");
    });
    await waitFor(() => {
      expect(screen.getByTestId("apply-button")).toBeDisabled();
    });
    expect(screen.getByTestId("apply-done-note")).toHaveTextContent(
      /disabled permanently/i,
    );

    // A durable application event refreshes the authoritative plan query.
    await waitFor(() => {
      expect(stream.connections.length).toBeGreaterThan(0);
    });
    const callsBefore = clientMocks.fetchRepairPlan.mock.calls.length;
    act(() => {
      stream.connections[0]?.deliver(
        repairFrameMessage({
          sequence: 1,
          event: "repair_action_applied",
          payload: { canonical_key: "record-1", outcome: "applied", target_version: 2 },
        }),
      );
    });
    await waitFor(() => {
      expect(clientMocks.fetchRepairPlan.mock.calls.length).toBeGreaterThan(
        callsBefore,
      );
    });

    await expectNoAccessibilityViolations(document.body);
  });

  it("treats a 503 apply response as recoverable and retries with the same key", async () => {
    const plan = approvedPlan();
    clientMocks.fetchRepairPlan.mockResolvedValue(plan);
    clientMocks.applyRepairPlan
      .mockRejectedValueOnce(
        new ApiRequestError(
          "problem",
          {
            title: "Service Unavailable",
            detail: "application unresolved",
            extensions: [],
          },
          503,
        ),
      )
      .mockResolvedValueOnce(makeApplyResponse({ disposition: "completed" }));
    const user = userEvent.setup();
    renderReview(pendingTransport);

    await user.click(await screen.findByTestId("apply-open"));
    await user.click(screen.getByTestId("apply-confirmation"));
    const applyButton = await screen.findByTestId("apply-button");
    await user.click(applyButton);
    expect(await screen.findByTestId("apply-retry")).toBeVisible();
    expect(screen.queryByTestId("outcome-disposition")).toBeNull();

    // The button re-enables for a retry; the retry reuses the same key.
    const retryButton = await screen.findByTestId("apply-button");
    expect(retryButton).toBeEnabled();
    await user.click(retryButton);
    await waitFor(() => {
      expect(clientMocks.applyRepairPlan).toHaveBeenCalledTimes(2);
    });
    const firstKey = (
      clientMocks.applyRepairPlan.mock.calls[0] as [string, { idempotencyKey: string }]
    )[1].idempotencyKey;
    const secondKey = (
      clientMocks.applyRepairPlan.mock.calls[1] as [string, { idempotencyKey: string }]
    )[1].idempotencyKey;
    expect(firstKey).toBe(secondKey);
    await waitFor(() => {
      expect(screen.getByTestId("outcome-disposition")).toHaveTextContent("Completed");
    });
    expect(screen.getByTestId("apply-button")).toBeDisabled();
  });

  it("shows the truthful plan-creation notice", async () => {
    clientMocks.fetchRepairPlan.mockResolvedValue(makePlan());
    renderReview(pendingTransport);

    await screen.findByText(PLAN_ID);
    expect(
      screen.getByText(
        /creating new repair plans from reconciliation projections is unavailable/i,
      ),
    ).toBeVisible();
    expect(screen.getByText(/fully supported/i)).toBeVisible();
  });
});

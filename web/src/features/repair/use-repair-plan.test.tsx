/**
 * Tests for the repair hooks: the authoritative plan query, the run
 * reconciliation fence, both mutation commands (exact request bodies and
 * stable idempotency keys across retries), and the durable progress hook
 * driven by an injected transport that mirrors the real SSE transport
 * contract from src/live/durable-stream.ts.
 *
 * The api/client module is mocked at its function boundary rather than at
 * global fetch: approveRepairPlan/applyRepairPlan are client-module
 * functions, and mocking here asserts exactly what the hooks send through
 * the typed client (body plus idempotency key), independent of transport
 * encoding.
 */
import { QueryClientProvider } from "@tanstack/react-query";
import { act, renderHook, waitFor } from "@testing-library/react";
import type { ReactNode } from "react";
import { beforeEach, describe, expect, it, vi } from "vitest";

import type {
  RepairApplyResponse,
  RepairPlanResponse,
  ReconciliationResponse,
} from "../../api/generated/schema";
import { ApiRequestError, isApiRequestError } from "../../api/client";
import { createQueryClient } from "../../api/query-client";
import type {
  SseConnectionResult,
  SseMessage,
  SseTransport,
} from "../../live/durable-stream";
import { applyIdempotencyKey, approvalIdempotencyKey } from "./repair-contract";
import {
  useApplyRepairPlan,
  useApproveRepairPlan,
  useRepairPlan,
  useRepairProgress,
  useRunReconciliationFingerprint,
} from "./use-repair-plan";

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

function approvedPlan(): RepairPlanResponse {
  return makePlan({
    status: "approved",
    approval: {
      approval_schema_version: 1,
      approved_at: "2026-01-01 00:01:00",
      approved_by: "operator-1",
      correlation_id: "corr-approve-1",
    },
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

function httpProblem(status: number, detail: string): ApiRequestError {
  return new ApiRequestError(
    "problem",
    { title: "Command failed", detail, extensions: [] },
    status,
  );
}

function renderWithQueryClient<T>(hook: () => T): {
  result: { current: T };
  queryClient: ReturnType<typeof createQueryClient>;
  unmount: () => void;
} {
  const queryClient = createQueryClient();
  const wrapper = ({ children }: { children: ReactNode }) => (
    <QueryClientProvider client={queryClient}>{children}</QueryClientProvider>
  );
  const rendered = renderHook(hook, { wrapper });
  return {
    result: rendered.result,
    queryClient,
    unmount: rendered.unmount,
  };
}

interface Harness {
  view: ReturnType<typeof useRepairPlan>;
  approval: ReturnType<typeof useApproveRepairPlan>;
  application: ReturnType<typeof useApplyRepairPlan>;
}

function useCommandHarness(planId: string): Harness {
  const view = useRepairPlan(planId);
  const approval = useApproveRepairPlan(planId, view.plan);
  const application = useApplyRepairPlan(planId, view.plan);
  return { view, approval, application };
}

// --- fake durable SSE transport mirroring the real transport contract ------

interface FakeConnection {
  url: string;
  deliver: (message: SseMessage) => void;
  close: (result: SseConnectionResult) => void;
}

interface FakeStream {
  transport: SseTransport;
  connections: FakeConnection[];
}

function createFakeStream(): FakeStream {
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

interface FrameOverrides {
  sequence: number;
  event: string;
  runId?: string;
  subjectId?: string;
  payload?: Record<string, unknown>;
  id?: string | null;
  eventIdMismatch?: boolean;
}

function repairFrameMessage(overrides: FrameOverrides): SseMessage {
  const frame = {
    schema_version: 1,
    channel: "durable-events",
    sequence: overrides.sequence,
    run_id: overrides.runId ?? RUN_ID,
    event_kind: overrides.event,
    subject_kind: "run",
    subject_id: overrides.subjectId ?? overrides.runId ?? RUN_ID,
    occurred_at: "2026-01-01 00:00:01",
    correlation_id: null,
    payload_schema_version: 1,
    payload: overrides.payload ?? {},
  };
  return {
    id: overrides.id === undefined ? String(overrides.sequence) : overrides.id,
    event: overrides.eventIdMismatch === true ? "some_other_event" : overrides.event,
    data: JSON.stringify(frame),
  };
}

interface ProgressHarness {
  view: ReturnType<typeof useRepairPlan>;
  progress: ReturnType<typeof useRepairProgress>;
}

function useProgressHarness(
  planId: string,
  sseTransport: SseTransport,
): ProgressHarness {
  const view = useRepairPlan(planId);
  // The run id is passed directly (as a route would), so the stream opens
  // before the plan query resolves and pre-binding frames are exercisable.
  const progress = useRepairProgress(RUN_ID, planId, view.plan, {
    sseTransport,
    scheduler: {
      schedule: (fn) => {
        fn();
        return () => undefined;
      },
    },
  });
  return { view, progress };
}

beforeEach(() => {
  clientMocks.fetchRepairPlan.mockReset();
  clientMocks.fetchReconciliation.mockReset();
  clientMocks.approveRepairPlan.mockReset();
  clientMocks.applyRepairPlan.mockReset();
});

describe("useRepairPlan", () => {
  it("loads the authoritative plan and passes an abort signal", async () => {
    const plan = makePlan();
    clientMocks.fetchRepairPlan.mockResolvedValue(plan);
    const { result } = renderWithQueryClient(() => useRepairPlan(PLAN_ID));

    expect(result.current.isPending).toBe(true);
    await waitFor(() => {
      expect(result.current.plan).not.toBeNull();
    });
    expect(result.current.plan).toEqual(plan);
    expect(result.current.error).toBeNull();
    const call = clientMocks.fetchRepairPlan.mock.calls[0] as unknown as [
      string,
      { signal: AbortSignal },
    ];
    expect(call[0]).toBe(PLAN_ID);
    expect(call[1].signal).toBeInstanceOf(AbortSignal);
  });

  it("surfaces query errors instead of plan data", async () => {
    clientMocks.fetchRepairPlan.mockRejectedValue(httpProblem(404, "plan unknown"));
    const { result } = renderWithQueryClient(() => useRepairPlan(PLAN_ID));

    await waitFor(() => {
      expect(result.current.error).not.toBeNull();
    });
    expect(isApiRequestError(result.current.error)).toBe(true);
    expect(result.current.plan).toBeNull();
  });

  it("refetches on demand and replaces the plan", async () => {
    clientMocks.fetchRepairPlan
      .mockResolvedValueOnce(makePlan())
      .mockResolvedValue(approvedPlan());
    const { result } = renderWithQueryClient(() => useRepairPlan(PLAN_ID));
    await waitFor(() => {
      expect(result.current.plan?.status).toBe("proposed");
    });

    act(() => {
      result.current.refetch();
    });
    await waitFor(() => {
      expect(result.current.plan?.status).toBe("approved");
    });
    expect(clientMocks.fetchRepairPlan).toHaveBeenCalledTimes(2);
  });
});

describe("useRunReconciliationFingerprint", () => {
  it("loads the current run fingerprint for the staleness fence", async () => {
    clientMocks.fetchReconciliation.mockResolvedValue(makeReconciliation());
    const { result } = renderWithQueryClient(() =>
      useRunReconciliationFingerprint(RUN_ID),
    );
    await waitFor(() => {
      expect(result.current.loaded).toBe(true);
    });
    expect(result.current.fingerprint).toBe(HEX_B);
    expect(result.current.error).toBeNull();
  });

  it("stays unloaded for a null run id", () => {
    clientMocks.fetchReconciliation.mockResolvedValue(makeReconciliation());
    const { result } = renderWithQueryClient(() =>
      useRunReconciliationFingerprint(null),
    );
    expect(result.current.loaded).toBe(false);
    expect(result.current.fingerprint).toBeNull();
    expect(clientMocks.fetchReconciliation).not.toHaveBeenCalled();
  });
});

describe("useApproveRepairPlan", () => {
  it("sends the exact approval body and a stable key across a 500 retry", async () => {
    const plan = makePlan();
    clientMocks.fetchRepairPlan.mockResolvedValue(plan);
    clientMocks.approveRepairPlan
      .mockRejectedValueOnce(httpProblem(500, "transient failure"))
      .mockResolvedValueOnce(approvedPlan());
    clientMocks.fetchRepairPlan.mockResolvedValue(approvedPlan());
    const { result } = renderWithQueryClient(() => useCommandHarness(PLAN_ID));
    await waitFor(() => {
      expect(result.current.view.plan).not.toBeNull();
    });

    const input = {
      approvedBy: "operator-1",
      contentFingerprint: HEX_A,
      reconciliationFingerprint: HEX_B,
    };
    act(() => {
      result.current.approval.approve(input);
    });
    await waitFor(() => {
      expect(result.current.approval.error).not.toBeNull();
    });
    expect(result.current.approval.done).toBe(false);

    // Operator retry after the transient failure: same logical approval,
    // same idempotency key.
    act(() => {
      result.current.approval.approve(input);
    });
    await waitFor(() => {
      expect(result.current.approval.done).toBe(true);
    });

    expect(clientMocks.approveRepairPlan).toHaveBeenCalledTimes(2);
    const expectedKey = approvalIdempotencyKey(plan, "operator-1");
    const firstCall = clientMocks.approveRepairPlan.mock.calls[0] as [
      string,
      Record<string, unknown>,
      { idempotencyKey: string },
    ];
    const secondCall = clientMocks.approveRepairPlan.mock.calls[1] as [
      string,
      Record<string, unknown>,
      { idempotencyKey: string },
    ];
    expect(firstCall[0]).toBe(PLAN_ID);
    expect(firstCall[1]).toEqual({
      schema_version: 1,
      approved_by: "operator-1",
      approved_content_fingerprint: HEX_A,
      approved_reconciliation_fingerprint: HEX_B,
    });
    expect(firstCall[2].idempotencyKey).toBe(expectedKey);
    expect(secondCall[1]).toEqual(firstCall[1]);
    expect(secondCall[2].idempotencyKey).toBe(expectedKey);

    // Success invalidates the plan query, so the authoritative view follows.
    await waitFor(() => {
      expect(result.current.view.plan?.status).toBe("approved");
    });
    expect(clientMocks.fetchRepairPlan.mock.calls.length).toBeGreaterThanOrEqual(2);
  });

  it("resets done when the plan id changes", async () => {
    const plan = makePlan();
    clientMocks.fetchRepairPlan.mockResolvedValue(plan);
    clientMocks.approveRepairPlan.mockResolvedValue(approvedPlan());
    const queryClient = createQueryClient();
    const wrapper = ({ children }: { children: ReactNode }) => (
      <QueryClientProvider client={queryClient}>{children}</QueryClientProvider>
    );
    const { result, rerender } = renderHook(
      ({ planId }: { planId: string }) => useCommandHarness(planId),
      { wrapper, initialProps: { planId: PLAN_ID } },
    );
    await waitFor(() => {
      expect(result.current.view.plan).not.toBeNull();
    });
    act(() => {
      result.current.approval.approve({
        approvedBy: "operator-1",
        contentFingerprint: HEX_A,
        reconciliationFingerprint: HEX_B,
      });
    });
    await waitFor(() => {
      expect(result.current.approval.done).toBe(true);
    });

    rerender({ planId: "rp_plan-002" });
    expect(result.current.approval.done).toBe(false);
  });
});

describe("useApplyRepairPlan", () => {
  it("sends no body with the stable apply key", async () => {
    const plan = approvedPlan();
    clientMocks.fetchRepairPlan.mockResolvedValue(plan);
    clientMocks.applyRepairPlan.mockResolvedValue(makeApplyResponse());
    const { result } = renderWithQueryClient(() => useCommandHarness(PLAN_ID));
    await waitFor(() => {
      expect(result.current.view.plan).not.toBeNull();
    });

    act(() => {
      result.current.application.apply();
    });
    await waitFor(() => {
      expect(result.current.application.result).not.toBeNull();
    });
    expect(clientMocks.applyRepairPlan).toHaveBeenCalledTimes(1);
    const call = clientMocks.applyRepairPlan.mock.calls[0] as [
      string,
      { idempotencyKey: string },
    ];
    expect(call[0]).toBe(PLAN_ID);
    expect(call[1]).toEqual({ idempotencyKey: applyIdempotencyKey(plan) });
    expect(result.current.application.error).toBeNull();
  });

  it("treats a 503 response as a recoverable failure, never a success", async () => {
    clientMocks.fetchRepairPlan.mockResolvedValue(approvedPlan());
    clientMocks.applyRepairPlan.mockRejectedValueOnce(
      httpProblem(503, "repair application is unresolved"),
    );
    const { result } = renderWithQueryClient(() => useCommandHarness(PLAN_ID));
    await waitFor(() => {
      expect(result.current.view.plan).not.toBeNull();
    });

    act(() => {
      result.current.application.apply();
    });
    await waitFor(() => {
      expect(result.current.application.error).not.toBeNull();
    });
    expect(isApiRequestError(result.current.application.error)).toBe(true);
    const error = result.current.application.error as ApiRequestError;
    expect(error.status).toBe(503);
    expect(result.current.application.result).toBeNull();
    expect(result.current.application.applying).toBe(false);
  });

  it("stores an already_applied success outcome", async () => {
    clientMocks.fetchRepairPlan.mockResolvedValue(approvedPlan());
    clientMocks.applyRepairPlan.mockResolvedValue(
      makeApplyResponse({ disposition: "already_applied", resumed: true }),
    );
    const { result } = renderWithQueryClient(() => useCommandHarness(PLAN_ID));
    await waitFor(() => {
      expect(result.current.view.plan).not.toBeNull();
    });

    act(() => {
      result.current.application.apply();
    });
    await waitFor(() => {
      expect(result.current.application.result?.disposition).toBe("already_applied");
    });
    expect(result.current.application.result?.resumed).toBe(true);
  });
});

describe("useRepairProgress", () => {
  it("quarantines frames that arrive before the plan binds, then accepts bound frames and refreshes the plan", async () => {
    const plan = makePlan();
    clientMocks.fetchRepairPlan.mockResolvedValue(plan);
    const stream = createFakeStream();
    const { result } = renderWithQueryClient(() =>
      useProgressHarness(PLAN_ID, stream.transport),
    );

    // A frame delivered before the authoritative plan loaded cannot be
    // proven relevant, so it is quarantined.
    act(() => {
      stream.connections[0]?.deliver(
        repairFrameMessage({
          sequence: 1,
          event: "repair_application_started",
          payload: {
            action_count: 2,
            content_fingerprint: HEX_A,
            reconciliation_fingerprint: HEX_B,
          },
        }),
      );
    });
    expect(result.current.progress.notifications).toHaveLength(0);

    await waitFor(() => {
      expect(result.current.view.plan).not.toBeNull();
    });
    expect(result.current.progress.quarantinedEvents).toBe(1);

    const fetchCallsBefore = clientMocks.fetchRepairPlan.mock.calls.length;
    act(() => {
      stream.connections[0]?.deliver(
        repairFrameMessage({
          sequence: 2,
          event: "repair_application_started",
          payload: {
            action_count: 2,
            content_fingerprint: HEX_A,
            reconciliation_fingerprint: HEX_B,
          },
        }),
      );
    });
    await waitFor(() => {
      expect(
        result.current.progress.notifications.some(
          (item) => item.sequence === 2 && item.kind === "repair_application_started",
        ),
      ).toBe(true);
    });
    await waitFor(() => {
      expect(clientMocks.fetchRepairPlan.mock.calls.length).toBeGreaterThan(
        fetchCallsBefore,
      );
    });
    expect(result.current.progress.quarantinedEvents).toBe(1);
  });

  it("ignores exact duplicate replay silently", async () => {
    clientMocks.fetchRepairPlan.mockResolvedValue(makePlan());
    const stream = createFakeStream();
    const { result } = renderWithQueryClient(() =>
      useProgressHarness(PLAN_ID, stream.transport),
    );
    await waitFor(() => {
      expect(result.current.view.plan).not.toBeNull();
    });

    const frame = repairFrameMessage({
      sequence: 1,
      event: "repair_application_started",
      payload: {
        content_fingerprint: HEX_A,
        reconciliation_fingerprint: HEX_B,
      },
    });
    act(() => {
      stream.connections[0]?.deliver(frame);
    });
    act(() => {
      stream.connections[0]?.deliver(frame);
    });
    await waitFor(() => {
      expect(result.current.progress.notifications).toHaveLength(1);
    });
    expect(result.current.progress.quarantinedEvents).toBe(0);
  });

  it("quarantines out-of-order frames instead of surfacing them", async () => {
    clientMocks.fetchRepairPlan.mockResolvedValue(makePlan());
    const stream = createFakeStream();
    const { result } = renderWithQueryClient(() =>
      useProgressHarness(PLAN_ID, stream.transport),
    );
    await waitFor(() => {
      expect(result.current.view.plan).not.toBeNull();
    });

    act(() => {
      stream.connections[0]?.deliver(
        repairFrameMessage({
          sequence: 1,
          event: "repair_application_started",
          payload: {
            content_fingerprint: HEX_A,
            reconciliation_fingerprint: HEX_B,
          },
        }),
      );
      stream.connections[0]?.deliver(
        repairFrameMessage({
          sequence: 2,
          event: "repair_application_completed",
          payload: {
            content_fingerprint: HEX_A,
            reconciliation_fingerprint: HEX_B,
          },
        }),
      );
    });
    await waitFor(() => {
      expect(result.current.progress.notifications).toHaveLength(2);
    });

    act(() => {
      stream.connections[0]?.deliver(
        repairFrameMessage({
          sequence: 1,
          event: "repair_application_started",
          payload: {
            content_fingerprint: HEX_A,
            reconciliation_fingerprint: HEX_B,
          },
        }),
      );
    });
    expect(result.current.progress.notifications).toHaveLength(2);
    expect(result.current.progress.quarantinedEvents).toBe(1);
  });

  it("quarantines application events whose fingerprints do not match the plan", async () => {
    clientMocks.fetchRepairPlan.mockResolvedValue(makePlan());
    const stream = createFakeStream();
    const { result } = renderWithQueryClient(() =>
      useProgressHarness(PLAN_ID, stream.transport),
    );
    await waitFor(() => {
      expect(result.current.view.plan).not.toBeNull();
    });

    act(() => {
      stream.connections[0]?.deliver(
        repairFrameMessage({
          sequence: 1,
          event: "repair_application_completed",
          payload: {
            action_count: 2,
            content_fingerprint: HEX_C,
            reconciliation_fingerprint: HEX_B,
          },
        }),
      );
    });
    expect(result.current.progress.notifications).toHaveLength(0);
    expect(result.current.progress.quarantinedEvents).toBe(1);
  });

  it("quarantines action events because canonical keys cannot bind an exact plan", async () => {
    clientMocks.fetchRepairPlan.mockResolvedValue(makePlan());
    const stream = createFakeStream();
    const { result } = renderWithQueryClient(() =>
      useProgressHarness(PLAN_ID, stream.transport),
    );
    await waitFor(() => {
      expect(result.current.view.plan).not.toBeNull();
    });

    act(() => {
      stream.connections[0]?.deliver(
        repairFrameMessage({
          sequence: 1,
          event: "repair_action_failed",
          payload: { canonical_key: "record-1", reason: "connector refused" },
        }),
      );
    });
    expect(result.current.progress.notifications).toHaveLength(0);
    expect(result.current.progress.quarantinedEvents).toBe(1);
  });

  it("quarantines foreign runs, malformed frames, and wrong subjects", async () => {
    clientMocks.fetchRepairPlan.mockResolvedValue(makePlan());
    const stream = createFakeStream();
    const { result } = renderWithQueryClient(() =>
      useProgressHarness(PLAN_ID, stream.transport),
    );
    await waitFor(() => {
      expect(result.current.view.plan).not.toBeNull();
    });

    act(() => {
      stream.connections[0]?.deliver(
        repairFrameMessage({
          sequence: 1,
          event: "repair_action_applied",
          runId: "run-other",
          payload: { canonical_key: "record-1", outcome: "applied" },
        }),
      );
      stream.connections[0]?.deliver({
        id: "2",
        event: "repair_action_applied",
        data: "{not json",
      });
      stream.connections[0]?.deliver(
        repairFrameMessage({
          sequence: 3,
          event: "repair_action_applied",
          subjectId: "not-the-run",
          payload: { canonical_key: "record-1", outcome: "applied" },
        }),
      );
      stream.connections[0]?.deliver(
        repairFrameMessage({
          sequence: 4,
          event: "reconciliation_completed",
          payload: {},
        }),
      );
    });
    expect(result.current.progress.notifications).toHaveLength(0);
    expect(result.current.progress.quarantinedEvents).toBe(4);
  });

  it("deduplicates a full replay after reconnect and resumes from the accepted sequence", async () => {
    clientMocks.fetchRepairPlan.mockResolvedValue(makePlan());
    const stream = createFakeStream();
    const { result } = renderWithQueryClient(() =>
      useProgressHarness(PLAN_ID, stream.transport),
    );
    await waitFor(() => {
      expect(result.current.view.plan).not.toBeNull();
    });

    const frames = [
      repairFrameMessage({
        sequence: 1,
        event: "repair_application_started",
        payload: {
          content_fingerprint: HEX_A,
          reconciliation_fingerprint: HEX_B,
        },
      }),
      repairFrameMessage({
        sequence: 2,
        event: "repair_application_completed",
        payload: {
          content_fingerprint: HEX_A,
          reconciliation_fingerprint: HEX_B,
        },
      }),
    ];
    act(() => {
      for (const frame of frames) {
        stream.connections[0]?.deliver(frame);
      }
    });
    await waitFor(() => {
      expect(result.current.progress.notifications).toHaveLength(2);
    });

    // The connection ends and the hook reconnects immediately (injected
    // scheduler), resuming from the last accepted sequence.
    await act(async () => {
      stream.connections[0]?.close({ outcome: "completed" });
      await Promise.resolve();
    });
    await waitFor(() => {
      expect(stream.connections.length).toBe(2);
    });
    expect(stream.connections[1]?.url).toContain(`after=2`);

    // The server replays durable history 1..n; nothing may duplicate.
    await act(async () => {
      for (const frame of frames) {
        stream.connections[1]?.deliver(frame);
      }
      await Promise.resolve();
    });
    const sequences = result.current.progress.notifications.map(
      (item) => item.sequence,
    );
    expect(sequences).toEqual([1, 2]);
    expect(new Set(sequences).size).toBe(sequences.length);
  });

  it("reports connection state transitions", async () => {
    clientMocks.fetchRepairPlan.mockResolvedValue(makePlan());
    const stream = createFakeStream();
    const { result } = renderWithQueryClient(() =>
      useProgressHarness(PLAN_ID, stream.transport),
    );
    await waitFor(() => {
      expect(result.current.progress.connected).toBe("connecting");
    });
    await waitFor(() => {
      expect(result.current.view.plan).not.toBeNull();
    });
    act(() => {
      stream.connections[0]?.deliver(
        repairFrameMessage({
          sequence: 1,
          event: "repair_application_started",
          payload: {
            content_fingerprint: HEX_A,
            reconciliation_fingerprint: HEX_B,
          },
        }),
      );
    });
    await waitFor(() => {
      expect(result.current.progress.connected).toBe("live");
    });
  });
});

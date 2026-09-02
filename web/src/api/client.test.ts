import { afterEach, describe, expect, it, vi } from "vitest";

import {
  approveRepairPlan,
  applyRepairPlan,
  createRepairPlan,
  createRun,
  controlRun,
  fetchConflicts,
  fetchHealth,
  fetchReconciliation,
  fetchRepairPlan,
  fetchRun,
  fetchRunPage,
  isApiRequestError,
  isRepairApplyResponse,
  isValidIdempotencyKey,
  runIdentity,
} from "./client";
import type {
  RepairApplyResponse,
  RepairApprovalRequestBody,
  RepairPlanResponse,
  RunResponse,
} from "./generated/schema";

const RUN: RunResponse = {
  run_id: "run-1",
  run_version: 3,
  state: "running",
  observed_at: "2026-01-01T00:00:00Z",
  created_at: "2026-01-01T00:00:00Z",
  started_at: null,
  finished_at: null,
  cancellation_requested_at: null,
  pipeline_id: "p1",
  pipeline_version: 1,
  runner_kind: "sequential",
  scenario_seed: null,
};

function jsonResponse(body: unknown, status = 200): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { "Content-Type": "application/json" },
  });
}

afterEach(() => {
  vi.unstubAllGlobals();
});

describe("typed REST client", () => {
  it("rejects an unknown run-state filter before issuing a request", () => {
    const fetchMock = vi.fn<typeof fetch>();
    vi.stubGlobal("fetch", fetchMock);
    expect(() => fetchRunPage({ state: "not-a-state" as never })).toThrow(RangeError);
    expect(fetchMock).not.toHaveBeenCalled();
  });

  it("returns the generated-typed payload on success", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn(() => Promise.resolve(jsonResponse(RUN))),
    );
    const run = await fetchRun("run-1");
    expect(run).toEqual(RUN);
    expect(global.fetch).toHaveBeenCalledWith(
      "/api/v1/runs/run-1",
      expect.objectContaining({ method: "GET", cache: "no-store" }),
    );
  });

  it("encodes path segments and applies pagination parameters", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn(() =>
        Promise.resolve(jsonResponse({ items: [RUN], limit: 10, next_cursor: "c2" })),
      ),
    );
    await fetchRunsPage();
    expect(global.fetch).toHaveBeenCalledWith(
      "/api/v1/runs?limit=10&cursor=c2",
      expect.anything(),
    );

    vi.stubGlobal(
      "fetch",
      vi.fn(() => Promise.resolve(jsonResponse(RUN))),
    );
    await fetchRun("run/with slash");
    expect(global.fetch).toHaveBeenCalledWith(
      "/api/v1/runs/run%2Fwith%20slash",
      expect.anything(),
    );
  });

  it("propagates cancellation without wrapping it in ApiRequestError", async () => {
    const controller = new AbortController();
    vi.stubGlobal(
      "fetch",
      vi.fn(() => {
        controller.abort();
        return Promise.reject(
          new DOMException("The operation was aborted.", "AbortError"),
        );
      }),
    );
    await expect(
      fetchRun("run-1", { signal: controller.signal }),
    ).rejects.not.toSatisfy(isApiRequestError);
  });

  it("normalizes network failures into a bounded generic error", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn(() => {
        const leak = new TypeError(
          "fetch failed: socket hang up at C:\\\\hidden\\\\path",
        );
        return Promise.reject(leak);
      }),
    );
    const error = await fetchRun("run-1").catch((caught: unknown) => caught);
    expect(isApiRequestError(error)).toBe(true);
    if (isApiRequestError(error)) {
      expect(error.kind).toBe("network");
      expect(error.status).toBeUndefined();
      expect(error.problem.title).toBe("Request failed");
      expect(error.message).not.toContain("socket hang up");
    }
  });

  it("parses Problem Details error bodies into the safe view with codes", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn(() =>
        Promise.resolve(
          new Response(
            JSON.stringify({
              type: "https://paritygrid.dev/problems/stream-sequence-ahead",
              title: "Stream resume position is ahead of durable history",
              status: 409,
              detail: "restart the stream from an earlier sequence",
              code: "stream_sequence_ahead",
            }),
            { status: 409, headers: { "Content-Type": "application/problem+json" } },
          ),
        ),
      ),
    );
    const error = await fetchRun("run-1").catch((caught: unknown) => caught);
    expect(isApiRequestError(error)).toBe(true);
    if (isApiRequestError(error)) {
      expect(error.kind).toBe("problem");
      expect(error.status).toBe(409);
      expect(error.problemCode).toBe("stream_sequence_ahead");
      expect(error.problem.detail).toBe("restart the stream from an earlier sequence");
    }
  });

  it("maps non-JSON and non-Problem error bodies to the generic model", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn(() =>
        Promise.resolve(
          new Response("Exception in thread: secretpath C:\\\\srv\\\\app.py", {
            status: 500,
            headers: { "Content-Type": "text/plain" },
          }),
        ),
      ),
    );
    const error = await fetchRun("run-1").catch((caught: unknown) => caught);
    expect(isApiRequestError(error)).toBe(true);
    if (isApiRequestError(error)) {
      expect(error.kind).toBe("problem");
      expect(error.problem.title).toBe("Request failed");
      expect(error.problem.extensions).toEqual([]);
      expect(error.message).not.toContain("secretpath");
    }
  });

  it("rejects successful responses that are not JSON", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn(() =>
        Promise.resolve(
          new Response("<html>hello</html>", {
            status: 200,
            headers: { "Content-Type": "text/html" },
          }),
        ),
      ),
    );
    await expect(fetchRun("run-1")).rejects.toMatchObject({ kind: "invalid-response" });
  });

  it("rejects successful responses with invalid JSON", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn(() =>
        Promise.resolve(
          new Response("{broken", {
            status: 200,
            headers: { "Content-Type": "application/json" },
          }),
        ),
      ),
    );
    await expect(fetchRun("run-1")).rejects.toMatchObject({ kind: "invalid-response" });
  });

  it("rejects payloads failing the coherence guards", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn(() =>
        Promise.resolve(
          jsonResponse({ items: "not-a-list", limit: 10, next_cursor: null }),
        ),
      ),
    );
    await expect(fetchRun("run-1")).rejects.toMatchObject({ kind: "invalid-response" });

    vi.stubGlobal(
      "fetch",
      vi.fn(() =>
        Promise.resolve(
          jsonResponse({
            items: [],
            limit: 10,
            next_cursor: null,
            run_id: "run-1",
            run_version: 1,
            state: "running",
            observed_at: "x",
            // reconciliation_fingerprint missing
          }),
        ),
      ),
    );
    await expect(fetchConflicts("run-1")).rejects.toMatchObject({
      kind: "invalid-response",
    });

    vi.stubGlobal(
      "fetch",
      vi.fn(() => Promise.resolve(jsonResponse({ ...RUN, pipeline_id: undefined }))),
    );
    await expect(fetchRun("run-1")).rejects.toMatchObject({
      kind: "invalid-response",
    });

    vi.stubGlobal(
      "fetch",
      vi.fn(() =>
        Promise.resolve(
          jsonResponse({
            items: [{ conflict_id: "only-one-field" }],
            limit: 10,
            next_cursor: null,
            observed_at: RUN.observed_at,
            reconciliation_fingerprint: "fingerprint",
            run_id: RUN.run_id,
            run_version: RUN.run_version,
            state: RUN.state,
          }),
        ),
      ),
    );
    await expect(fetchConflicts("run-1")).rejects.toMatchObject({
      kind: "invalid-response",
    });

    vi.stubGlobal(
      "fetch",
      vi.fn(() =>
        Promise.resolve(
          jsonResponse({
            schema_version: 1,
            run_id: "run-other",
            run_version: 1,
            state: "succeeded",
            observed_at: RUN.observed_at,
            reconciliation_fingerprint: "a".repeat(64),
            source_input_identity: "b".repeat(64),
            target_input_identity: "c".repeat(64),
            total_count: 0,
            counts: {
              match: 0,
              missing_from_target: 0,
              missing_from_source: 0,
              field_mismatch: 0,
              duplicate_source: 0,
              duplicate_target: 0,
              duplicate_both: 0,
            },
            analytical_query_version: 1,
            reconciliation_observed_at: RUN.observed_at,
          }),
        ),
      ),
    );
    await expect(fetchReconciliation("run-1")).rejects.toMatchObject({
      kind: "invalid-response",
    });
  });

  it("rejects a health body that does not match the generated shape", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn(() => Promise.resolve(jsonResponse({ unexpected: true }))),
    );
    await expect(fetchHealth()).rejects.toMatchObject({ kind: "invalid-response" });

    vi.stubGlobal(
      "fetch",
      vi.fn(() => Promise.resolve(jsonResponse({ status: "ok", version: "0.1.0" }))),
    );
    await expect(fetchHealth()).rejects.toMatchObject({ kind: "invalid-response" });
  });

  it("sends generated-typed mutation bodies with idempotency keys", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn(() => Promise.resolve(jsonResponse(RUN, 201))),
    );
    await createRun(
      {
        run_id: "run-9",
        pipeline_id: "p1",
        pipeline_version: 2,
        runner_kind: "sequential",
        scenario_seed: 5,
      },
      { idempotencyKey: "retry-safe-key" },
    );
    const [, init] = (global.fetch as ReturnType<typeof vi.fn>).mock.calls[0] as [
      string,
      { method: string; body: string; headers: Record<string, string> },
    ];
    expect(init.method).toBe("POST");
    expect(init.headers["Idempotency-Key"]).toBe("retry-safe-key");
    expect(init.headers["Content-Type"]).toBe("application/json");
    expect(JSON.parse(init.body)).toMatchObject({ run_id: "run-9" });
  });

  it("sends bodyless run controls and rejects a mismatched acknowledgement", async () => {
    const fetchMock = vi.fn<typeof fetch>(() =>
      Promise.resolve(jsonResponse({ ...RUN, state: "paused", run_version: 4 })),
    );
    vi.stubGlobal("fetch", fetchMock);

    await controlRun("run-1", "pause", { idempotencyKey: "pause-run-1-v3" });
    const firstCall = fetchMock.mock.calls[0];
    expect(firstCall).toBeDefined();
    if (firstCall === undefined) {
      throw new Error("the pause request was not sent");
    }
    const [path, init] = firstCall;
    expect(path).toBe("/api/v1/runs/run-1/pause");
    expect(init?.method).toBe("POST");
    expect(init?.body).toBeUndefined();
    expect(new Headers(init?.headers).get("Idempotency-Key")).toBe("pause-run-1-v3");

    vi.stubGlobal(
      "fetch",
      vi.fn(() => Promise.resolve(jsonResponse({ ...RUN, run_id: "run-other" }))),
    );
    await expect(
      controlRun("run-1", "cancel", { idempotencyKey: "cancel-run-1-v4" }),
    ).rejects.toMatchObject({ kind: "invalid-response" });
  });

  it("sends repair-plan creation with the generated request shape", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn(() =>
        Promise.resolve(
          jsonResponse(
            {
              plan_id: "plan-1",
              actions: [],
              content_fingerprint: "a".repeat(64),
              created_at: RUN.created_at,
              observed_at: RUN.observed_at,
              reconciliation_fingerprint: "b".repeat(64),
              run_id: RUN.run_id,
              run_version: RUN.run_version,
              state: RUN.state,
              status: "proposed",
            },
            201,
          ),
        ),
      ),
    );
    await createRepairPlan("run-1", {
      schema_version: 1,
      source: { connector_id: "c1", input_identity: "a".repeat(64), observations: [] },
      target: { connector_id: "c2", input_identity: "b".repeat(64), observations: [] },
    });
    const [path] = (global.fetch as ReturnType<typeof vi.fn>).mock.calls[0] as [string];
    expect(path).toBe("/api/v1/runs/run-1/repair-plans");
  });

  it("replays a mutation only with the caller's same idempotency key", async () => {
    const fetchMock = vi.fn<
      (input: RequestInfo | URL, init?: RequestInit) => Promise<Response>
    >(() => Promise.resolve(jsonResponse(RUN, 201)));
    vi.stubGlobal("fetch", fetchMock);
    const request = {
      run_id: "run-replay",
      pipeline_id: "p1",
      pipeline_version: 2,
      runner_kind: "sequential",
      scenario_seed: null,
    };

    await createRun(request, { idempotencyKey: "stable-replay-key" });
    await createRun(request, { idempotencyKey: "stable-replay-key" });

    expect(fetchMock).toHaveBeenCalledTimes(2);
    for (const call of fetchMock.mock.calls) {
      expect(call[1]?.headers).toMatchObject({
        "Idempotency-Key": "stable-replay-key",
      });
    }
  });

  it("refuses malformed idempotency keys before any request", async () => {
    vi.stubGlobal("fetch", vi.fn());
    await expect(
      createRun(
        {
          run_id: "run-9",
          pipeline_id: "p1",
          pipeline_version: 2,
          runner_kind: "sequential",
          scenario_seed: null,
        },
        { idempotencyKey: "bad\nkey" },
      ),
    ).rejects.toBeInstanceOf(RangeError);
    expect(global.fetch).not.toHaveBeenCalled();
    expect(isValidIdempotencyKey("ok-key_1")).toBe(true);
    expect(isValidIdempotencyKey("A.b:c-1")).toBe(true);
    expect(isValidIdempotencyKey("")).toBe(false);
    expect(isValidIdempotencyKey("x".repeat(129))).toBe(false);
    for (const malformed of [" bad", "bad key", "@bad", "_bad", "bad!"]) {
      expect(isValidIdempotencyKey(malformed)).toBe(false);
    }
  });
});

async function fetchRunsPage(): Promise<unknown> {
  const { fetchRuns } = await import("./client");
  return fetchRuns({ limit: 10, cursor: "c2" });
}

describe("run identity guard", () => {
  it("validates the coherence block", () => {
    expect(runIdentity(RUN)).toEqual({
      run_id: "run-1",
      run_version: 3,
      state: "running",
      observed_at: "2026-01-01T00:00:00Z",
    });
    expect(runIdentity({ ...RUN, run_version: 1.5 })).toBeNull();
    expect(runIdentity({ ...RUN, run_id: "" })).toBeNull();
    expect(runIdentity({ ...RUN, observed_at: 5 })).toBeNull();
    expect(runIdentity("text")).toBeNull();
  });
});

describe("response parsing edges", () => {
  it("caps unreachable error bodies as a generic failure", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn(() =>
        Promise.resolve({
          ok: false,
          status: 500,
          headers: new Headers({ "Content-Type": "application/problem+json" }),
          text: () => Promise.reject(new TypeError("body stream lost")),
        } as unknown as Response),
      ),
    );
    const error = await fetchRun("run-1").catch((caught: unknown) => caught);
    expect(isApiRequestError(error)).toBe(true);
    if (isApiRequestError(error)) {
      expect(error.problem.title).toBe("Request failed");
      expect(error.status).toBe(500);
    }
    vi.unstubAllGlobals();
  });

  it("accepts the maximum idempotency key length", () => {
    expect(isValidIdempotencyKey("k".repeat(128))).toBe(true);
  });

  it("requires a JSON content type on success", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn(() =>
        Promise.resolve(
          new Response("{}", { status: 200, headers: { "Content-Type": "" } }),
        ),
      ),
    );
    await expect(fetchHealth()).rejects.toMatchObject({ kind: "invalid-response" });
    vi.unstubAllGlobals();
  });
});

const APPROVAL_REQUEST: RepairApprovalRequestBody = {
  schema_version: 1,
  approved_by: "operator-1",
  approved_content_fingerprint: "c".repeat(64),
  approved_reconciliation_fingerprint: "b".repeat(64),
};

const APPROVED_PLAN: RepairPlanResponse = {
  schema_version: 1,
  plan_id: "plan-1",
  run_id: RUN.run_id,
  run_version: RUN.run_version,
  state: RUN.state,
  observed_at: RUN.observed_at,
  status: "approved",
  reconciliation_fingerprint: "b".repeat(64),
  content_fingerprint: "c".repeat(64),
  created_at: RUN.created_at,
  approval: {
    approval_schema_version: 1,
    approved_at: RUN.observed_at,
    approved_by: "operator-1",
    correlation_id: "corr-1",
    schema_version: 1,
  },
  actions: [],
};

const APPLY_COMPLETED: RepairApplyResponse = {
  schema_version: 1,
  plan_id: "plan-1",
  run_id: RUN.run_id,
  run_version: RUN.run_version,
  state: RUN.state,
  observed_at: RUN.observed_at,
  status: "applied",
  reconciliation_fingerprint: "b".repeat(64),
  content_fingerprint: "c".repeat(64),
  disposition: "completed",
  resumed: false,
  effects: [{ action_id: "action-1", outcome: "created" }],
};

describe("repair plan approval and application", () => {
  it("rejects a repair plan body that does not match the requested route id", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn(() =>
        Promise.resolve(jsonResponse({ ...APPROVED_PLAN, plan_id: "plan-other" }, 200)),
      ),
    );
    await expect(fetchRepairPlan("plan-1")).rejects.toMatchObject({
      kind: "invalid-response",
    });
  });

  it("sends the approval body and idempotency key to the approve route", async () => {
    const fetchMock = vi.fn(() => Promise.resolve(jsonResponse(APPROVED_PLAN, 200)));
    vi.stubGlobal("fetch", fetchMock);
    const plan = await approveRepairPlan("plan-1", APPROVAL_REQUEST, {
      idempotencyKey: "approve-plan-1",
    });
    expect(plan).toEqual(APPROVED_PLAN);
    expect(isRepairApplyResponse(plan)).toBe(false);
    const [path, init] = fetchMock.mock.calls[0] as unknown as [
      string,
      { method: string; body: string; headers: Record<string, string> },
    ];
    expect(path).toBe("/api/v1/repair-plans/plan-1/approve");
    expect(init.method).toBe("POST");
    expect(init.headers["Idempotency-Key"]).toBe("approve-plan-1");
    expect(init.headers["Content-Type"]).toBe("application/json");
    expect(JSON.parse(init.body)).toEqual(APPROVAL_REQUEST);
  });

  it("refuses malformed approval bodies before any request", async () => {
    const fetchMock = vi.fn<typeof fetch>();
    vi.stubGlobal("fetch", fetchMock);
    const emptyApprover = {
      ...APPROVAL_REQUEST,
      approved_by: "",
    };
    await expect(approveRepairPlan("plan-1", emptyApprover)).rejects.toBeInstanceOf(
      RangeError,
    );
    const longApprover = {
      ...APPROVAL_REQUEST,
      approved_by: "x".repeat(129),
    };
    await expect(approveRepairPlan("plan-1", longApprover)).rejects.toBeInstanceOf(
      RangeError,
    );
    await expect(
      approveRepairPlan("plan-1", {
        ...APPROVAL_REQUEST,
        approved_content_fingerprint: "C".repeat(64),
      }),
    ).rejects.toBeInstanceOf(RangeError);
    await expect(
      approveRepairPlan("plan-1", {
        ...APPROVAL_REQUEST,
        approved_reconciliation_fingerprint: "short",
      }),
    ).rejects.toBeInstanceOf(RangeError);
    expect(fetchMock).not.toHaveBeenCalled();
  });

  it("refuses a malformed idempotency key before any request", async () => {
    vi.stubGlobal("fetch", vi.fn());
    await expect(
      approveRepairPlan("plan-1", APPROVAL_REQUEST, { idempotencyKey: "bad key" }),
    ).rejects.toBeInstanceOf(RangeError);
    await expect(
      applyRepairPlan("plan-1", { idempotencyKey: "bad key" }),
    ).rejects.toBeInstanceOf(RangeError);
    expect(global.fetch).not.toHaveBeenCalled();
  });

  it("maps a conflicting approval to the Problem Details error model", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn(() =>
        Promise.resolve(
          new Response(
            JSON.stringify({
              title: "Plan is not in the proposed state",
              status: 409,
              detail: "only proposed plans can be approved",
            }),
            { status: 409, headers: { "Content-Type": "application/problem+json" } },
          ),
        ),
      ),
    );
    const error = await approveRepairPlan("plan-1", APPROVAL_REQUEST).catch(
      (caught: unknown) => caught,
    );
    expect(isApiRequestError(error)).toBe(true);
    if (isApiRequestError(error)) {
      expect(error.kind).toBe("problem");
      expect(error.status).toBe(409);
      expect(error.problem.detail).toBe("only proposed plans can be approved");
    }
  });

  it("maps a server failure on approval to a problem error with the status", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn(() =>
        Promise.resolve(
          new Response(JSON.stringify({ title: "Unexpected failure", status: 500 }), {
            status: 500,
            headers: { "Content-Type": "application/problem+json" },
          }),
        ),
      ),
    );
    const error = await approveRepairPlan("plan-1", APPROVAL_REQUEST).catch(
      (caught: unknown) => caught,
    );
    expect(isApiRequestError(error)).toBe(true);
    if (isApiRequestError(error)) {
      expect(error.kind).toBe("problem");
      expect(error.status).toBe(500);
    }
  });

  it("sends apply with no body and the idempotency key, parsing the outcome", async () => {
    const fetchMock = vi.fn(() => Promise.resolve(jsonResponse(APPLY_COMPLETED, 200)));
    vi.stubGlobal("fetch", fetchMock);
    const outcome = await applyRepairPlan("plan-1", {
      idempotencyKey: "apply-plan-1",
    });
    expect(outcome).toEqual(APPLY_COMPLETED);
    expect(isRepairApplyResponse(outcome)).toBe(true);
    const [path, init] = fetchMock.mock.calls[0] as unknown as [
      string,
      { method: string; body?: string; headers: Record<string, string> },
    ];
    expect(path).toBe("/api/v1/repair-plans/plan-1/apply");
    expect(init.method).toBe("POST");
    expect(init.body).toBeUndefined();
    expect(init.headers["Idempotency-Key"]).toBe("apply-plan-1");
    expect(init.headers["Content-Type"]).toBeUndefined();
  });

  it("rejects command acknowledgements for a different plan or fingerprint", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn(() =>
        Promise.resolve(
          jsonResponse({ ...APPROVED_PLAN, content_fingerprint: "d".repeat(64) }),
        ),
      ),
    );
    await expect(approveRepairPlan("plan-1", APPROVAL_REQUEST)).rejects.toMatchObject({
      kind: "invalid-response",
    });

    vi.stubGlobal(
      "fetch",
      vi.fn(() =>
        Promise.resolve(jsonResponse({ ...APPLY_COMPLETED, plan_id: "plan-other" })),
      ),
    );
    await expect(applyRepairPlan("plan-1")).rejects.toMatchObject({
      kind: "invalid-response",
    });
  });

  it("surfaces an interrupted application (503) as a recoverable problem error", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn(() =>
        Promise.resolve(
          new Response(
            JSON.stringify({
              title: "Repair application interrupted",
              status: 503,
              detail: "application ended in an interrupted state; retry is safe",
            }),
            { status: 503, headers: { "Content-Type": "application/problem+json" } },
          ),
        ),
      ),
    );
    const error = await applyRepairPlan("plan-1", {
      idempotencyKey: "apply-plan-1",
    }).catch((caught: unknown) => caught);
    expect(isApiRequestError(error)).toBe(true);
    if (isApiRequestError(error)) {
      expect(error.kind).toBe("problem");
      expect(error.status).toBe(503);
      expect(error.problem.detail).toContain("retry is safe");
    }
  });

  it("rejects an apply acknowledgement outside the disposition contract", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn(() =>
        Promise.resolve(jsonResponse({ ...APPLY_COMPLETED, disposition: "postponed" })),
      ),
    );
    await expect(applyRepairPlan("plan-1")).rejects.toMatchObject({
      kind: "invalid-response",
    });
  });
});

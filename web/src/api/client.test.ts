import { afterEach, describe, expect, it, vi } from "vitest";

import {
  createRepairPlan,
  createRun,
  fetchConflicts,
  fetchHealth,
  fetchRun,
  isApiRequestError,
  isValidIdempotencyKey,
  runIdentity,
} from "./client";
import type { RunResponse } from "./generated/schema";

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

  it("sends repair-plan creation with the generated request shape", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn(() =>
        Promise.resolve(
          jsonResponse(
            {
              plan_id: "plan-1",
              actions: [],
              content_fingerprint: "fp",
              created_at: RUN.created_at,
              observed_at: RUN.observed_at,
              reconciliation_fingerprint: "reconciliation-fp",
              run_id: RUN.run_id,
              run_version: RUN.run_version,
              state: RUN.state,
              status: "pending",
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
    expect(isValidIdempotencyKey("")).toBe(false);
    expect(isValidIdempotencyKey("x".repeat(129))).toBe(false);
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

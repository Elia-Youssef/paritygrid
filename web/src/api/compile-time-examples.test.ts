import { afterEach, describe, expect, it, vi } from "vitest";

import { mutationExamples, queryExamples } from "./compile-time-examples";
import type {
  CreateRunTakesGeneratedRequest,
  RunListIsGenerated,
  RunQueryIsGenerated,
} from "./compile-time-examples";

// Compile-time proof: if any client signature drifted from the generated
// contract, the exported `Expect<Equal<...>>` aliases fail `tsc -b` before
// any test can run (see `typeAssertionTuple` below, which only type-checks
// when every element is `true`).

afterEach(() => {
  vi.unstubAllGlobals();
});

type TrueTuple = [
  RunQueryIsGenerated,
  RunListIsGenerated,
  CreateRunTakesGeneratedRequest,
];

const typeAssertionTuple: TrueTuple = [true, true, true];

function jsonOk(body: unknown, status = 200): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { "Content-Type": "application/json" },
  });
}

describe("generated-client compile-time examples", () => {
  it("holds every generated-type identity assertion true", () => {
    expect(typeAssertionTuple).toEqual([true, true, true]);
  });

  it("runs the typed query examples against the JSON contract", async () => {
    const runPayload = {
      run_id: "run-1",
      run_version: 1,
      state: "new",
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
    vi.stubGlobal(
      "fetch",
      vi.fn(() =>
        Promise.resolve(
          jsonOk({
            items: [runPayload],
            limit: 50,
            next_cursor: null,
          }),
        ),
      ),
    );

    const page = await queryExamples.runList();
    expect(page.items[0]?.run_id).toBe("run-1");
    expect(global.fetch).toHaveBeenCalledWith(
      "/api/v1/runs?limit=50",
      expect.anything(),
    );

    vi.stubGlobal(
      "fetch",
      vi.fn(() =>
        Promise.resolve(
          jsonOk({
            items: [],
            limit: 25,
            next_cursor: null,
            observed_at: "2026-01-01T00:00:00Z",
            run_id: "run-1",
            run_version: 1,
            state: "running",
            reconciliation_fingerprint: "fp",
          }),
        ),
      ),
    );
    const conflicts = await queryExamples.conflicts();
    expect(conflicts.reconciliation_fingerprint).toBe("fp");

    vi.stubGlobal(
      "fetch",
      vi.fn(() =>
        Promise.resolve(
          jsonOk({
            run_id: "run-1",
            run_version: 1,
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
          }),
        ),
      ),
    );
    const run = await queryExamples.run();
    expect(run.state).toBe("running");

    vi.stubGlobal(
      "fetch",
      vi.fn(() =>
        Promise.resolve(
          jsonOk({
            run_id: "run-1",
            run_version: 1,
            state: "running",
            observed_at: "2026-01-01T00:00:00Z",
            reconciliation_observed_at: "2026-01-01T00:00:01Z",
            reconciliation_fingerprint: "fp-1",
            source_input_identity: "a",
            target_input_identity: "b",
            analytical_query_version: 1,
            total_count: 0,
            counts: {},
          }),
        ),
      ),
    );
    const reconciliation = await queryExamples.reconciliation();
    expect(reconciliation.reconciliation_fingerprint).toBe("fp-1");

    vi.stubGlobal(
      "fetch",
      vi.fn(() =>
        Promise.resolve(
          jsonOk({
            service: "ParityGrid",
            version: "0.1.0",
            runners: [],
            subordinate_pools: [],
            features: [],
            limits: {
              artifact_chunk_bytes: 1_048_576,
              idempotency_lease_seconds: 60,
              max_concurrent_requests: 64,
              max_json_depth: 32,
              max_page_size: 100,
              max_request_body_bytes: 1_048_576,
              request_timeout_seconds: 30,
            },
            sqlite: {
              busy_timeout_ms: 5_000,
              journal_mode: "wal",
              library_version: "3.50.0",
              minimum_supported_version: "3.35.0",
              supports_json_sql: true,
              supports_returning: true,
              synchronous_level: 1,
              threadsafety: 3,
            },
          }),
        ),
      ),
    );
    const capabilities = await queryExamples.capabilities();
    expect(capabilities.service).toBe("ParityGrid");
  });

  it("exposes the typed mutation example with idempotency support", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn(() =>
        Promise.resolve(
          jsonOk(
            {
              run_id: "run-x",
              run_version: 0,
              state: "new",
              observed_at: "2026-01-01T00:00:00Z",
              created_at: "2026-01-01T00:00:00Z",
              started_at: null,
              finished_at: null,
              cancellation_requested_at: null,
              pipeline_id: "p1",
              pipeline_version: 1,
              runner_kind: "sequential",
              scenario_seed: null,
            },
            201,
          ),
        ),
      ),
    );

    const run = await mutationExamples.createRun({
      run_id: "run-x",
      pipeline_id: "p1",
      pipeline_version: 1,
      runner_kind: "sequential",
      scenario_seed: null,
    });
    expect(run.run_id).toBe("run-x");
    const [, init] = (global.fetch as ReturnType<typeof vi.fn>).mock.calls[0] as [
      string,
      { headers: Record<string, string> },
    ];
    expect(init.headers["Idempotency-Key"]).toBe("demo-key-1");
  });
});

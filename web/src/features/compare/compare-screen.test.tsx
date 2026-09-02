/**
 * Comparison screen tests (P17.6): incompatible runs block comparison with
 * exact machine-readable reasons; compatible runs compare correctness
 * before speed; unavailable metrics state their reason instead of implying
 * an equivalence; identity facts come only from durable run rows.
 */
import { QueryClientProvider } from "@tanstack/react-query";
import { cleanup, render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { createBrowserRouter, RouterProvider } from "react-router";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import { ComparisonScreen } from "./compare-screen";
import {
  compareCompatibility,
  fingerprintAgreement,
  parseCompareRunIds,
} from "./compare-derivations";
import { appQueryClient } from "../../api/query-client";

function run(
  index: number,
  overrides: Record<string, unknown> = {},
): Record<string, unknown> {
  return {
    schema_version: 1,
    run_id: `run_cmp-${String(index).padStart(3, "0")}`,
    run_version: 3,
    state: "succeeded",
    observed_at: "2026-01-01T00:01:00Z",
    created_at: "2026-01-01T00:00:00Z",
    started_at: "2026-01-01T00:00:02Z",
    finished_at: "2026-01-01T00:00:32Z",
    cancellation_requested_at: null,
    pipeline_id: "pip_a",
    pipeline_version: 1,
    runner_kind: index === 0 ? "sequential" : "asyncio",
    scenario_seed: 7,
    execution_evidence_fingerprint: "a".repeat(64),
    execution_evidence_fingerprint_version: 2,
    ...overrides,
  };
}

function stubRunApi(runs: Record<string, unknown>[]): {
  requests: string[];
} {
  const requests: string[] = [];
  const fetchMock = vi.fn<typeof fetch>((input: RequestInfo | URL) => {
    const url =
      typeof input === "string" ? input : input instanceof URL ? input.href : input.url;
    requests.push(url);
    if (url === "/api/v1/runs?limit=50") {
      return Promise.resolve(
        new Response(
          JSON.stringify({
            schema_version: 1,
            items: runs,
            limit: 50,
            next_cursor: null,
          }),
          { status: 200, headers: { "Content-Type": "application/json" } },
        ),
      );
    }
    const match = /\/api\/v1\/runs\/(run_[a-z0-9-]+)$/.exec(url);
    if (match !== null) {
      const run = runs.find((candidate) => candidate.run_id === match[1]);
      if (run !== undefined) {
        return Promise.resolve(
          new Response(JSON.stringify(run), {
            status: 200,
            headers: { "Content-Type": "application/json" },
          }),
        );
      }
      return Promise.resolve(
        new Response(
          JSON.stringify({ type: "about:blank", title: "not found", status: 404 }),
          { status: 404, headers: { "Content-Type": "application/problem+json" } },
        ),
      );
    }
    if (url.includes("/api/v1/system/capabilities")) {
      return Promise.resolve(
        new Response(
          JSON.stringify({
            schema_version: 1,
            service: "ParityGrid",
            version: "0.1.0",
            runners: [
              { schema_version: 1, strategy_id: "sequential", available: true },
              {
                schema_version: 1,
                strategy_id: "asyncio",
                available: false,
                unavailability_reason: "event loop unavailable",
              },
            ],
            subordinate_pools: [],
            features: [],
            sqlite: {
              schema_version: 1,
              library_version: "3.45",
              minimum_supported_version: "3.35",
              journal_mode: "wal",
              synchronous_level: 1,
              busy_timeout_ms: 5000,
              threadsafety: 1,
              supports_json_sql: true,
              supports_returning: true,
            },
            limits: {
              schema_version: 1,
              artifact_chunk_bytes: 65536,
              idempotency_lease_seconds: 30,
              max_concurrent_requests: 8,
              max_json_depth: 64,
              max_page_size: 100,
              max_request_body_bytes: 1048576,
              request_timeout_seconds: 15,
            },
          }),
          { status: 200, headers: { "Content-Type": "application/json" } },
        ),
      );
    }
    return Promise.resolve(new Response("nope", { status: 404 }));
  });
  vi.stubGlobal("fetch", fetchMock);
  return { requests };
}

function renderCompare(path: string): void {
  window.history.replaceState({}, "", path);
  const router = createBrowserRouter([
    { path: "/app/compare", element: <ComparisonScreen /> },
  ]);
  render(
    <QueryClientProvider client={appQueryClient}>
      <RouterProvider router={router} />
    </QueryClientProvider>,
  );
}

describe("comparison derivations", () => {
  it("blocks on every mismatch reason and reports them together", () => {
    const verdict = compareCompatibility([
      {
        run_id: "run_a",
        pipeline_id: "pip_a",
        pipeline_version: 1,
        scenario_seed: 7,
        execution_evidence_fingerprint_version: 2,
      },
      {
        run_id: "run_b",
        pipeline_id: "pip_other",
        pipeline_version: 2,
        scenario_seed: 9,
        execution_evidence_fingerprint_version: 1,
      },
    ] as never);
    expect(verdict.comparable).toBe(false);
    if (!verdict.comparable) {
      const codes = verdict.failures.map((failure) => failure.code);
      expect(codes).toContain("pipeline_identity_mismatch");
      expect(codes).toContain("pipeline_version_mismatch");
      expect(codes).toContain("scenario_seed_mismatch");
      expect(codes).toContain("evidence_format_mismatch");
    }
  });

  it("treats a missing fingerprint version as an unavailable format", () => {
    const verdict = compareCompatibility([
      {
        run_id: "run_a",
        state: "succeeded",
        pipeline_id: "pip_a",
        pipeline_version: 1,
        scenario_seed: 7,
        execution_evidence_fingerprint_version: 2,
      },
      {
        run_id: "run_b",
        state: "succeeded",
        pipeline_id: "pip_a",
        pipeline_version: 1,
        scenario_seed: 7,
        execution_evidence_fingerprint_version: null,
      },
    ] as never);
    expect(verdict.comparable).toBe(false);
    if (!verdict.comparable) {
      expect(verdict.failures[0]?.code).toBe("evidence_format_unavailable");
    }
  });

  it("agrees fingerprints only when every run finalizes to one value", () => {
    const agree = fingerprintAgreement([
      { execution_evidence_fingerprint: "a".repeat(64) },
      { execution_evidence_fingerprint: "a".repeat(64) },
    ] as never);
    expect(agree.agree).toBe(true);
    const disagree = fingerprintAgreement([
      { execution_evidence_fingerprint: "a".repeat(64) },
      { execution_evidence_fingerprint: null },
    ] as never);
    expect(disagree.agree).toBe(false);
  });

  it("parses bounded, deduplicated run ids from the URL", () => {
    const search = new URLSearchParams(
      "runs=run_one,run_two,run_one,not-a-run,run_three,run_four",
    );
    expect(parseCompareRunIds(search)).toEqual(["run_one", "run_two", "run_three"]);
    expect(parseCompareRunIds(new URLSearchParams(""))).toEqual([]);
  });

  it("blocks when comparison input identity or finalized evidence is unavailable", () => {
    const verdict = compareCompatibility([
      run(0, { scenario_seed: null }),
      run(1, { execution_evidence_fingerprint: null }),
    ] as never);
    expect(verdict.comparable).toBe(false);
    if (!verdict.comparable) {
      const codes = verdict.failures.map((failure) => failure.code);
      expect(codes).toContain("scenario_seed_unavailable");
      expect(codes).toContain("execution_evidence_unavailable");
    }
  });

  it("blocks comparison while any run has not reached a terminal state", () => {
    // A fingerprint on a non-terminal row must not enable a comparison:
    // finalization is validated in its own right, not inferred from
    // fingerprint presence.
    const verdict = compareCompatibility([
      run(0),
      run(1, { state: "running", finished_at: null }),
    ] as never);
    expect(verdict.comparable).toBe(false);
    if (!verdict.comparable) {
      expect(verdict.failures.map((failure) => failure.code)).toContain(
        "run_not_finalized",
      );
    }
  });

  it("compares only states that may carry finalized execution evidence", () => {
    for (const state of ["succeeded", "partially_succeeded"]) {
      const verdict = compareCompatibility([run(0), run(1, { state })] as never);
      expect(verdict.comparable).toBe(true);
    }
    for (const state of ["failed", "cancelled"]) {
      const verdict = compareCompatibility([run(0), run(1, { state })] as never);
      expect(verdict.comparable).toBe(false);
      if (!verdict.comparable) {
        expect(verdict.failures.map((failure) => failure.code)).toContain(
          "run_not_finalized",
        );
      }
    }
  });
});

describe("ComparisonScreen", () => {
  beforeEach(() => {
    appQueryClient.clear();
  });

  afterEach(() => {
    cleanup();
    vi.unstubAllGlobals();
    window.history.replaceState({}, "", "/");
  });

  it("blocks incompatible runs and lists the exact blocking reasons", async () => {
    stubRunApi([
      run(0),
      run(1, {
        pipeline_version: 2,
        scenario_seed: 9,
        execution_evidence_fingerprint_version: 1,
      }),
    ]);
    renderCompare("/app/compare?runs=run_cmp-000,run_cmp-001");

    const blocked = await screen.findByTestId("compare-blocked");
    expect(blocked).toBeVisible();
    expect(blocked).toHaveTextContent("pipeline_version_mismatch");
    expect(blocked).toHaveTextContent("scenario_seed_mismatch");
    expect(blocked).toHaveTextContent("evidence_format_mismatch");
    expect(screen.queryByTestId("compare-correctness")).toBeNull();
  });

  it("blocks speed when compatible runs disagree on correctness", async () => {
    stubRunApi([
      run(0),
      run(1, {
        execution_evidence_fingerprint: "b".repeat(64),
        runner_kind: "asyncio",
      }),
    ]);
    renderCompare("/app/compare?runs=run_cmp-000,run_cmp-001");

    // Correctness section renders before speed in the document order.
    const correctness = await screen.findByTestId("compare-correctness");
    expect(correctness).toHaveTextContent("Execution fingerprint agreement");
    expect(correctness).toHaveTextContent("disagree");
    const blocked = screen.getByTestId("compare-speed-blocked");
    const correctnessPosition = correctness.compareDocumentPosition(blocked);
    expect(correctnessPosition & Node.DOCUMENT_POSITION_FOLLOWING).toBeTruthy();
    expect(blocked).toHaveTextContent("Correctness is an absolute gate");
    expect(screen.queryByTestId("compare-speed")).toBeNull();
  });

  it("compares agreeing runs with honest unavailable rows", async () => {
    stubRunApi([run(0), run(1, { runner_kind: "asyncio" })]);
    renderCompare("/app/compare?runs=run_cmp-000,run_cmp-001");

    const correctness = await screen.findByTestId("compare-correctness");
    expect(correctness).toHaveTextContent("agree");
    const speed = screen.getByTestId("compare-speed");

    // Duration derives from durable timestamps; every other requested
    // metric states why it is absent instead of showing a number.
    expect(speed).toHaveTextContent("30.000 s");
    expect(screen.getAllByTestId("compare-unavailable").length).toBeGreaterThanOrEqual(
      6,
    );
    expect(screen.getByText(/implying an equivalence/i)).toBeInTheDocument();
    expect(screen.getByTestId("compare-duration-chart")).toBeInTheDocument();
  });

  it("reports agreement when fingerprints match", async () => {
    stubRunApi([run(0), run(1)]);
    renderCompare("/app/compare?runs=run_cmp-000,run_cmp-001");
    expect(await screen.findByTestId("compare-correctness")).toHaveTextContent("agree");
  });

  it("blocks when a selected run cannot be loaded", async () => {
    stubRunApi([run(0)]);
    renderCompare("/app/compare?runs=run_cmp-000,run_missing-1");

    const blocked = await screen.findByText("Comparison blocked");
    expect(blocked).toBeVisible();
    await waitFor(() => {
      expect(screen.getByText(/run_missing-1 could not be loaded/)).toBeInTheDocument();
    });
    expect(screen.queryByTestId("compare-correctness")).toBeNull();
  });

  it("does not compare a subset when one of three selected runs fails", async () => {
    stubRunApi([run(0), run(1)]);
    renderCompare("/app/compare?runs=run_cmp-000,run_cmp-001,run_missing-002");

    expect(await screen.findByText("Comparison blocked")).toBeVisible();
    expect(screen.getByText(/run_missing-002 could not be loaded/)).toBeVisible();
    expect(screen.queryByTestId("compare-correctness")).toBeNull();
  });

  it("selects runs through URL-backed checkboxes", async () => {
    stubRunApi([run(0), run(1)]);
    renderCompare("/app/compare");
    await userEvent.click(await screen.findByTestId("compare-check-run_cmp-000"));
    expect(window.location.search).toContain("runs=run_cmp-000");
    await userEvent.click(screen.getByTestId("compare-check-run_cmp-001"));
    expect(window.location.search).toContain("runs=run_cmp-000%2Crun_cmp-001");
    expect(await screen.findByTestId("compare-dashboard")).toBeInTheDocument();
  });
});

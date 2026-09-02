import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { cleanup, render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { createBrowserRouter, RouterProvider } from "react-router";
import { afterEach, describe, expect, it, vi } from "vitest";

import { OperationsOverview } from "./operations-overview";

function capabilitiesBody(): string {
  return JSON.stringify({
    schema_version: 1,
    service: "ParityGrid",
    version: "0.1.0",
    runners: [
      { schema_version: 1, strategy_id: "sequential", available: true },
      {
        schema_version: 1,
        strategy_id: "process",
        available: false,
        unavailability_reason: "spawn unsupported",
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
      artifact_chunk_bytes: 1,
      idempotency_lease_seconds: 2,
      max_concurrent_requests: 1,
      max_json_depth: 1,
      max_page_size: 1,
      max_request_body_bytes: 1,
      request_timeout_seconds: 1,
    },
  });
}

const RUN_PAGE = {
  schema_version: 1,
  items: [
    {
      schema_version: 1,
      run_id: "run_scenario-01",
      run_version: 2,
      state: "succeeded",
      observed_at: "2026-01-01 00:00:00",
      created_at: "2026-01-01 00:00:00",
      started_at: null,
      finished_at: "2026-01-01 00:01:00",
      cancellation_requested_at: null,
      pipeline_id: "pip_demo-alpha",
      pipeline_version: 1,
      runner_kind: "sequential",
      scenario_seed: null,
    },
  ],
  limit: 10,
  next_cursor: null,
};

type FetchMock = (
  input: RequestInfo | URL,
  init?: RequestInit,
) => Response | Promise<Response>;

function stubApis(
  options: { runs?: Response; readiness?: Response; capabilities?: Response } = {},
): void {
  const fetchMock = vi.fn<FetchMock>((input: RequestInfo | URL) => {
    const url =
      typeof input === "string" ? input : input instanceof URL ? input.href : input.url;
    const okJson = (body: unknown): Response =>
      new Response(JSON.stringify(body), {
        status: 200,
        headers: { "Content-Type": "application/json" },
      });
    if (url.includes("/api/v1/runs")) {
      const response = options.runs ?? okJson(RUN_PAGE);
      return Promise.resolve(response);
    }
    if (url.includes("/readyz")) {
      const response =
        options.readiness ??
        okJson({
          status: "ready",
          service: "ParityGrid",
          version: "0.1.0",
          detail: "migrations applied",
        });
      return Promise.resolve(response);
    }
    if (url.includes("/api/v1/system/capabilities")) {
      const response = options.capabilities ?? okJson(JSON.parse(capabilitiesBody()));
      return Promise.resolve(response);
    }
    return Promise.resolve(new Response("nope", { status: 404 }));
  });
  vi.stubGlobal("fetch", fetchMock);
}

function renderOverview(): QueryClient {
  window.history.replaceState({}, "", "/app");
  const queryClient = new QueryClient({
    defaultOptions: { queries: { retry: false } },
  });
  const router = createBrowserRouter([
    { path: "/app", element: <OperationsOverview /> },
  ]);
  render(
    <QueryClientProvider client={queryClient}>
      <RouterProvider router={router} />
    </QueryClientProvider>,
  );
  return queryClient;
}

afterEach(() => {
  cleanup();
  vi.unstubAllGlobals();
});

describe("operations overview", () => {
  it("presents durable runs, runner availability, and readiness with sources", async () => {
    stubApis();
    renderOverview();

    expect(await screen.findByText("run_scenario-01")).toBeVisible();
    expect(screen.getByText("Runner availability")).toBeVisible();
    expect(screen.getAllByText("available").length).toBeGreaterThan(0);
    expect(screen.getByText("spawn unsupported")).toBeVisible();
    expect(screen.getByText("migrations applied")).toBeVisible();
    // Metric definitions name their REST source.
    expect(screen.getByText(/GET \/api\/v1\/runs/)).toBeVisible();
  });

  it("renders unavailable facts as unavailable instead of zero", async () => {
    stubApis({
      capabilities: new Response("server restarting", { status: 503 }),
      readiness: new Response("server restarting", { status: 503 }),
    });
    renderOverview();

    expect(
      await screen.findByText("Runner availability is unavailable right now."),
    ).toBeVisible();
    expect(screen.getByText("Readiness is unavailable right now.")).toBeVisible();
    // The runs table still renders from its successful query.
    expect(await screen.findByText("run_scenario-01")).toBeVisible();
  });

  it("shows the empty state when no runs are recorded", async () => {
    stubApis({
      runs: new Response(
        JSON.stringify({ schema_version: 1, items: [], limit: 10, next_cursor: null }),
        { status: 200, headers: { "Content-Type": "application/json" } },
      ),
    });
    renderOverview();
    expect(await screen.findByText("No runs recorded yet")).toBeVisible();
  });

  it("offers retry when the run page fails", async () => {
    stubApis({ runs: new Response("boom", { status: 500 }) });
    renderOverview();
    expect(await screen.findByText("Recent runs unavailable")).toBeVisible();
    await userEvent.setup().click(screen.getByRole("button", { name: "Retry" }));
  });

  it("keeps the last coherent run page visible and marked stale while refreshing", async () => {
    stubApis();
    const queryClient = renderOverview();
    expect(await screen.findByText("run_scenario-01")).toBeVisible();

    // The next refetch never resolves, so the table must keep showing the
    // last coherent durable page with a stale marker instead of degrading.
    const fetchMock = fetch as unknown as { mockImplementation: (fn: unknown) => void };
    fetchMock.mockImplementation(() => new Promise<Response>(() => undefined));
    // Not awaited on purpose: the invalidated refetches never settle.
    void queryClient.invalidateQueries();

    expect(
      await screen.findByText("Refreshing; showing the last coherent durable page."),
    ).toBeVisible();
    expect(screen.getByText("run_scenario-01")).toBeVisible();
  });
});

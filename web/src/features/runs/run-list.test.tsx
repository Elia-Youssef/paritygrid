/**
 * Run list component tests (P17.1): deterministic pagination, closed-set
 * filtering, selection, and every required view state, all driven through
 * the URL so reload, back, forward, and direct entry behave identically.
 */
import { QueryClientProvider } from "@tanstack/react-query";
import { cleanup, render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { createBrowserRouter, RouterProvider } from "react-router";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import { RunList } from "./run-list";
import { appQueryClient } from "../../api/query-client";

function run(index: number, state = "succeeded"): Record<string, unknown> {
  const terminal = ["succeeded", "partially_succeeded", "failed", "cancelled"].includes(
    state,
  );
  return {
    schema_version: 1,
    run_id: `run_page-${String(index).padStart(3, "0")}`,
    run_version: 1,
    state,
    observed_at: "2026-01-01T00:00:00Z",
    created_at: "2026-01-01T00:00:00Z",
    started_at: null,
    finished_at: terminal ? "2026-01-01T00:01:00Z" : null,
    cancellation_requested_at: null,
    pipeline_id: "pip_lib-001",
    pipeline_version: 1,
    runner_kind: "sequential",
    scenario_seed: 7,
    execution_evidence_fingerprint: null,
    execution_evidence_fingerprint_version: null,
  };
}

interface StubOptions {
  pages?: Record<string, Record<string, unknown>[]>;
  fail?: boolean;
}

function stubRunApi(options: StubOptions = {}): { requests: string[] } {
  const requests: string[] = [];
  const pages = options.pages ?? {};
  const fetchMock = vi.fn<typeof fetch>((input: RequestInfo | URL) => {
    const url =
      typeof input === "string" ? input : input instanceof URL ? input.href : input.url;
    requests.push(url);
    if (options.fail === true) {
      return Promise.resolve(
        new Response(
          JSON.stringify({
            type: "about:blank",
            title: "boom",
            status: 500,
          }),
          { status: 500, headers: { "Content-Type": "application/problem+json" } },
        ),
      );
    }
    const params = new URLSearchParams(url.split("?")[1] ?? "");
    const cursor = params.get("cursor") ?? "first";
    const items = pages[cursor] ?? [];
    const nextCursor = cursor === "first" ? "run_page-024" : null;
    return Promise.resolve(
      new Response(
        JSON.stringify({
          schema_version: 1,
          items,
          limit: Number.parseInt(params.get("limit") ?? "25", 10),
          next_cursor: nextCursor,
        }),
        { status: 200, headers: { "Content-Type": "application/json" } },
      ),
    );
  });
  vi.stubGlobal("fetch", fetchMock);
  return { requests };
}

function renderAt(path: string): void {
  window.history.replaceState({}, "", path);
  const router = createBrowserRouter([{ path: "/app/runs", element: <RunList /> }]);
  render(
    <QueryClientProvider client={appQueryClient}>
      <RouterProvider router={router} />
    </QueryClientProvider>,
  );
}

describe("RunList", () => {
  beforeEach(() => {
    appQueryClient.clear();
  });

  afterEach(() => {
    cleanup();
    vi.unstubAllGlobals();
    window.history.replaceState({}, "", "/");
  });

  it("renders a durable page with states, pagination, and a successor notice", async () => {
    stubRunApi({
      pages: {
        first: [run(1, "running"), run(2, "failed"), run(3, "paused")],
      },
    });
    renderAt("/app/runs");

    const row1 = await screen.findByTestId("run-row-run_page-001");
    expect(row1).toBeVisible();
    expect(row1).toHaveTextContent("running");
    expect(screen.getByTestId("run-row-run_page-002")).toHaveTextContent("failed");
    // A next_cursor exists: the page says a further page exists, never a
    // global total.
    expect(screen.getByTestId("runs-page-status")).toHaveTextContent(
      "a further durable page exists",
    );
    expect(screen.getByTestId("runs-next")).toBeEnabled();
    expect(screen.queryByTestId("runs-prev")).toBeNull();
    expect(screen.getByTestId("run-row-run_page-003")).toHaveTextContent("paused");
  });

  it("paginates deterministically through the URL cursor", async () => {
    const { requests } = stubRunApi({
      pages: {
        first: [run(1)],
        "run_page-024": [run(25)],
      },
    });
    renderAt("/app/runs");
    await userEvent.click(await screen.findByTestId("runs-next"));

    await waitFor(() => {
      expect(screen.getByTestId("run-row-run_page-025")).toBeVisible();
    });
    expect(window.location.search).toContain("cursor=run_page-024");
    // The second request carries the opaque cursor verbatim.
    expect(requests.some((url) => url.includes("cursor=run_page-024"))).toBe(true);
    expect(screen.getByTestId("runs-prev")).toBeEnabled();
    expect(screen.getByTestId("runs-page-status")).toHaveTextContent(
      "no further durable page",
    );
  });

  it("returns a direct-entry cursor page to the first page without browser history", async () => {
    stubRunApi({
      pages: {
        first: [run(1)],
        "run_page-024": [run(25)],
      },
    });
    renderAt("/app/runs?cursor=run_page-024&limit=25");
    await screen.findByTestId("run-row-run_page-025");

    await userEvent.click(screen.getByRole("button", { name: "Back to first page" }));
    await screen.findByTestId("run-row-run_page-001");
    expect(window.location.pathname).toBe("/app/runs");
    expect(window.location.search).not.toContain("cursor=");
  });

  it("filters by state through the URL and resets the cursor", async () => {
    const { requests } = stubRunApi({
      pages: { first: [run(1, "failed")] },
    });
    renderAt("/app/runs");
    await screen.findByTestId("run-row-run_page-001");

    await userEvent.selectOptions(screen.getByTestId("runs-state-filter"), "failed");

    await waitFor(() => {
      expect(window.location.search).toContain("state=failed");
    });
    expect(window.location.search).not.toContain("cursor=");
    await waitFor(() => {
      expect(requests.some((url) => url.includes("state=failed"))).toBe(true);
    });
    expect(await screen.findByRole("status")).toHaveTextContent(
      "Filtered to durable state",
    );
  });

  it("restores filter, page size, and selection from the URL on reload and direct entry", async () => {
    stubRunApi({ pages: { first: [run(1, "cancelled")] } });
    renderAt("/app/runs?state=cancelled&limit=10&selected=run_page-001");

    await screen.findByTestId("run-row-run_page-001");
    expect(screen.getByTestId("runs-state-filter")).toHaveValue("cancelled");
    expect(screen.getByTestId("runs-limit")).toHaveValue("10");
    expect(screen.getByTestId("run-select-run_page-001")).toHaveAttribute(
      "aria-pressed",
      "true",
    );
    expect(screen.getByTestId("run-row-run_page-001").className).toContain("bg-active");
  });

  it("selection is URL-backed and reversible", async () => {
    stubRunApi({ pages: { first: [run(1), run(2)] } });
    renderAt("/app/runs");
    await userEvent.click(await screen.findByTestId("run-select-run_page-001"));

    await waitFor(() => {
      expect(window.location.search).toContain("selected=run_page-001");
    });
    expect(screen.getByTestId("run-select-run_page-001")).toHaveAttribute(
      "aria-pressed",
      "true",
    );

    await userEvent.click(screen.getByTestId("run-select-run_page-001"));
    await waitFor(() => {
      expect(window.location.search).not.toContain("selected=");
    });
  });

  it("shows an explicit loading state before data arrives", async () => {
    stubRunApi({ pages: { first: [run(1)] } });
    renderAt("/app/runs");
    // The pending state renders synchronously before the stub resolves.
    expect(screen.getByRole("status").textContent).toContain("Loading runs");
    await screen.findByTestId("runs-tbody");
  });

  it("shows an explicit empty state distinct from errors", async () => {
    stubRunApi({ pages: { first: [] } });
    renderAt("/app/runs");
    expect(await screen.findByText("No runs recorded yet")).toBeVisible();
  });

  it("renders errors with retry and never substitutes partial data", async () => {
    const { requests } = stubRunApi({ fail: true });
    renderAt("/app/runs");
    expect(await screen.findByRole("alert")).toHaveTextContent("Runs unavailable");
    expect(screen.queryByTestId("runs-tbody")).toBeNull();
    await userEvent.click(screen.getByRole("button", { name: "Retry" }));
    await waitFor(() => {
      expect(requests.length).toBeGreaterThan(1);
    });
  });

  it("rejects unbounded or unknown query parameters deterministically", async () => {
    const { requests } = stubRunApi({ pages: { first: [run(1)] } });
    renderAt("/app/runs?state=NOT_A_STATE&limit=99999&cursor=" + "x".repeat(300));
    await screen.findByTestId("runs-tbody");
    // Unknown state and limit fall back to the unfiltered default page
    // size; the oversized cursor is dropped entirely.
    expect(screen.getByTestId("runs-state-filter")).toHaveValue("");
    expect(screen.getByTestId("runs-limit")).toHaveValue("25");
    expect(requests.some((url) => url.includes("state=NOT_A_STATE"))).toBe(false);
    expect(requests[0]).not.toContain("cursor=");
  });
});

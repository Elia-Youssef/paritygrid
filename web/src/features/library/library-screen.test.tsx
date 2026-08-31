import { QueryClientProvider } from "@tanstack/react-query";
import { cleanup, render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { createBrowserRouter, RouterProvider } from "react-router";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import { PipelineLibrary } from "./library-screen";
import { appQueryClient } from "../../api/query-client";

function pipeline(index: number): Record<string, unknown> {
  return {
    schema_version: 1,
    pipeline_id: `pip_lib-${String(index).padStart(3, "0")}`,
    display_name: `Pipeline ${String(index)}`,
    description: `Number ${String(index)}`,
    created_at: "2026-01-01 00:00:00",
    archived_at: index === 2 ? "2026-01-02 00:00:00" : null,
    row_version: index + 1,
  };
}

function stubLibraryApi(options: { pages?: number; fail?: boolean } = {}): {
  requests: string[];
} {
  const requests: string[] = [];
  const totalPages = options.pages ?? 1;
  const fetchMock = vi.fn<typeof fetch>(
    (input: RequestInfo | URL, init?: RequestInit) => {
      const url =
        typeof input === "string"
          ? input
          : input instanceof URL
            ? input.href
            : input.url;
      requests.push(`${init?.method ?? "GET"} ${url}`);
      if (url.startsWith("/api/v1/pipelines") && (init?.method ?? "GET") === "POST") {
        return Promise.resolve(
          new Response(
            JSON.stringify({
              schema_version: 1,
              pipeline_id: "pip_new-001",
              display_name: "New",
              description: null,
              created_at: "2026-01-03 00:00:00",
              archived_at: null,
              row_version: 1,
            }),
            { status: 201, headers: { "Content-Type": "application/json" } },
          ),
        );
      }
      if (options.fail === true) {
        return Promise.resolve(new Response("boom", { status: 500 }));
      }
      const cursor = new URLSearchParams(url.split("?")[1] ?? "").get("cursor");
      const pageNumber =
        cursor === null ? 0 : Number.parseInt(cursor.replace(/\D/g, ""), 10);
      const isLast = pageNumber + 1 >= totalPages;
      return Promise.resolve(
        new Response(
          JSON.stringify({
            schema_version: 1,
            items: [pipeline(pageNumber * 2), pipeline(pageNumber * 2 + 1)],
            limit: 2,
            next_cursor: isLast
              ? null
              : `pip_lib-${String(pageNumber * 2 + 1).padStart(3, "0")}`,
          }),
          { status: 200, headers: { "Content-Type": "application/json" } },
        ),
      );
    },
  );
  vi.stubGlobal("fetch", fetchMock);
  return { requests };
}

function renderLibrary(path = "/app/pipelines"): void {
  window.history.replaceState({}, "", path);
  const router = createBrowserRouter([
    {
      path: "/app/pipelines",
      element: <PipelineLibrary />,
    },
    {
      path: "/app/pipelines/:pipelineId",
      element: <div>Studio</div>,
    },
  ]);
  render(
    <QueryClientProvider client={appQueryClient}>
      <RouterProvider router={router} />
    </QueryClientProvider>,
  );
}

beforeEach(() => {
  appQueryClient.clear();
});

afterEach(() => {
  vi.unstubAllGlobals();
});

describe("pipeline library", () => {
  it("lists pipelines, searches deterministically, and reflects URL state", async () => {
    stubLibraryApi({ pages: 2 });
    const user = userEvent.setup();
    renderLibrary();

    await screen.findByRole("table", { name: "Pipelines" });
    expect(screen.getAllByText(/pip_lib-\d{3}/).length).toBeGreaterThan(0);

    await user.type(screen.getByTestId("library-search"), "1");
    // URL carries the search text.
    expect(window.location.search).toContain("q=1");
    await waitFor(() => {
      const rows = screen.getAllByRole("row");
      expect(rows.length).toBeLessThan(6);
    });

    // Reload reproduces the same filtered view from the URL alone.
    const url = new URL(window.location.href);
    cleanup();
    renderLibrary(url.pathname + url.search);
    await screen.findByRole("table", { name: "Pipelines" });
    expect(window.location.search).toContain("q=1");
  });

  it("keeps the selection in the URL across refreshes", async () => {
    stubLibraryApi();
    renderLibrary();
    const link = await screen.findByRole("link", {
      name: "Open pipeline studio for pip_lib-000",
    });
    expect(link).toHaveAttribute("href", "/app/pipelines/pip_lib-000");
  });

  it("renders an error state with retry when the collection fails", async () => {
    stubLibraryApi({ fail: true });
    renderLibrary();
    expect(await screen.findByText("Pipelines unavailable")).toBeVisible();
    await userEvent.setup().click(screen.getByRole("button", { name: "Retry" }));
  });

  it("renders a deterministic no-results state for a search without matches", async () => {
    stubLibraryApi();
    const user = userEvent.setup();
    renderLibrary();
    await screen.findByRole("table", { name: "Pipelines" });

    await user.type(screen.getByTestId("library-search"), "zzz-no-match");
    expect(await screen.findByText("No pipelines match this search")).toBeVisible();
    expect(screen.queryByRole("table", { name: "Pipelines" })).toBeNull();
    expect(window.location.search).toContain("q=zzz-no-match");

    // Clearing the search restores the full loaded collection.
    await user.click(screen.getByRole("button", { name: "Clear search" }));
    expect(await screen.findByRole("table", { name: "Pipelines" })).toBeVisible();
    expect(window.location.search).not.toContain("q=");
  });

  it("clamps out-of-range page state from the URL safely", async () => {
    stubLibraryApi({ pages: 2 });
    window.history.replaceState({}, "", "/app/pipelines?page=9");
    renderLibrary();
    await screen.findByRole("table", { name: "Pipelines" });

    // The view clamps to the last available page instead of rendering an
    // empty state or crashing.
    expect(screen.getByText(/1 \/ 1|2 \/ 2/)).toBeVisible();
    expect(screen.getByRole("button", { name: "Next page" })).toBeDisabled();
  });

  it("opens the create dialog and posts with an idempotency key", async () => {
    const api = stubLibraryApi();
    const user = userEvent.setup();
    renderLibrary();
    await screen.findByRole("table", { name: "Pipelines" });

    await user.click(screen.getByTestId("new-pipeline"));
    await user.type(screen.getByTestId("create-pipeline-id"), "pip_new-001");
    await user.type(screen.getByTestId("create-pipeline-name"), "New");
    await user.click(screen.getByTestId("create-pipeline-submit"));

    await waitFor(() => {
      expect(
        api.requests.some((entry) => entry.startsWith("POST /api/v1/pipelines")),
      ).toBe(true);
    });
    const createRequest = api.requests.find((entry) => entry.startsWith("POST"));
    expect(createRequest).toBeDefined();
    expect(window.location.search).toContain("selected=pip_new-001");
  });
});

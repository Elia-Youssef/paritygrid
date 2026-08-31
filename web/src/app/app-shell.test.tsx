import { render, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, describe, expect, it, vi } from "vitest";

import { App } from "./App";
import { appQueryClient } from "../api/query-client";
import { expectNoAccessibilityViolations } from "../test/axe";

const HEALTH_OK = JSON.stringify({
  status: "ok",
  service: "ParityGrid",
  version: "0.1.0",
});

/**
 * The console screens consume several durable APIs; route the stub by URL
 * so each fixture satisfies its consumer and health behavior stays exact.
 */
function stubAppApis(options: { healthStatus?: number } = {}): {
  fetchMock: ReturnType<typeof vi.fn>;
  healthCalls: () => number;
  setHealthy: () => void;
} {
  let healthy = (options.healthStatus ?? 200) === 200;
  let healthCallCount = 0;
  const fetchMock = vi.fn<typeof fetch>((input: RequestInfo | URL) => {
    const url =
      typeof input === "string" ? input : input instanceof URL ? input.href : input.url;
    if (url.includes("/healthz")) {
      healthCallCount += 1;
      const status = healthy ? 200 : 503;
      const body = healthy ? HEALTH_OK : "restarting";
      return Promise.resolve(
        new Response(body, {
          status,
          headers: {
            "Content-Type": healthy ? "application/json" : "text/plain",
          },
        }),
      );
    }
    if (url.includes("/readyz")) {
      return Promise.resolve(
        new Response(
          JSON.stringify({
            status: "ready",
            service: "ParityGrid",
            version: "0.1.0",
            detail: "migrations applied",
          }),
          { status: 200, headers: { "Content-Type": "application/json" } },
        ),
      );
    }
    if (url.includes("/api/v1/runs")) {
      return Promise.resolve(
        new Response(
          JSON.stringify({
            schema_version: 1,
            items: [],
            limit: 10,
            next_cursor: null,
          }),
          {
            status: 200,
            headers: { "Content-Type": "application/json" },
          },
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
            runners: [],
            subordinate_pools: [],
            features: [],
            sqlite: {
              schema_version: 1,
              library_version: "3",
              minimum_supported_version: "3",
              journal_mode: "wal",
              synchronous_level: 1,
              threadsafety: 1,
              supports_json_sql: true,
              supports_returning: true,
            },
            limits: {
              schema_version: 1,
              artifact_chunk_bytes: 1,
              idempotency_lease_seconds: 1,
              max_concurrent_requests: 1,
              max_json_depth: 1,
              max_page_size: 1,
              max_request_body_bytes: 1,
              request_timeout_seconds: 1,
            },
          }),
          { status: 200, headers: { "Content-Type": "application/json" } },
        ),
      );
    }
    return Promise.resolve(new Response("not found", { status: 404 }));
  });
  vi.stubGlobal("fetch", fetchMock);
  return {
    fetchMock,
    healthCalls: () => healthCallCount,
    setHealthy: () => {
      healthy = true;
    },
  };
}

function stubHealthApi(status = 200): void {
  stubAppApis({ healthStatus: status });
}

function load(path: string): void {
  window.history.replaceState({}, "", path);
}

function breadcrumbLabels(trail: HTMLElement): string[] {
  return Array.from(trail.querySelectorAll("li"))
    .map((item) => (item.textContent ?? "").trim())
    .filter((label) => label !== "");
}

afterEach(() => {
  vi.unstubAllGlobals();
  appQueryClient.clear();
  load("/");
});

describe("operations shell", () => {
  it("leads keyboard users past navigation with a working skip link", async () => {
    stubHealthApi();
    const user = userEvent.setup();
    load("/app");
    render(<App />);

    await user.tab();
    expect(document.activeElement).toHaveTextContent("Skip to content");

    await user.keyboard("{Enter}");
    expect(window.location.hash).toBe("#main-content");
    expect(document.getElementById("main-content")).not.toBeNull();
  });

  it("exposes semantic landmarks and persistent navigation", () => {
    stubHealthApi();
    load("/app");
    render(<App />);

    expect(screen.getByRole("banner")).toBeVisible();
    expect(screen.getAllByRole("navigation", { name: "Primary" })).toHaveLength(2);
    expect(screen.getByRole("main")).toBeVisible();
    for (const label of ["Overview", "Pipelines", "Runs", "Compare", "System"]) {
      expect(screen.getAllByRole("link", { name: label }).length).toBeGreaterThan(0);
    }
  });

  it("shows the environment and data-root identity as text", () => {
    stubHealthApi();
    load("/app");
    render(<App />);

    const sidebar = screen.getByRole("complementary");
    expect(within(sidebar).getByText("Local workspace")).toBeVisible();
    expect(within(sidebar).getByText(".paritygrid/")).toBeVisible();
  });

  it("retains environment and run context in the compact shell bar", () => {
    stubHealthApi();
    load("/app/runs/run-42");
    render(<App />);

    const compactContext = screen.getByRole("group", {
      name: "Current environment and operation",
    });
    expect(within(compactContext).getByText("Local workspace")).toBeVisible();
    expect(within(compactContext).getByText(".paritygrid/")).toBeVisible();
    expect(within(compactContext).getByText("Run run-42")).toBeVisible();
    expect(compactContext.className).toContain("lg:hidden");
  });

  it("confirms an online API", async () => {
    stubHealthApi(200);
    load("/app");
    render(<App />);

    expect(await screen.findByText("API online")).toBeVisible();
  });

  it("surfaces an unreachable API as a failure state", async () => {
    stubHealthApi(503);
    load("/app");
    render(<App />);

    expect(await screen.findByText("API unavailable")).toBeVisible();
  });

  it("allows an unavailable API connection to recover", async () => {
    const apis = stubAppApis({ healthStatus: 503 });
    const user = userEvent.setup();
    load("/app");
    render(<App />);

    expect(await screen.findByText("API unavailable")).toBeVisible();
    apis.setHealthy();
    const badgeCluster = screen.getByText("API unavailable").closest("div");
    expect(badgeCluster).not.toBeNull();
    await user.click(
      within(badgeCluster as HTMLElement).getByRole("button", { name: "Retry" }),
    );

    expect(await screen.findByText("API online")).toBeVisible();
    expect(apis.healthCalls()).toBeGreaterThanOrEqual(2);
  });

  it("keeps exactly one navigation in the accessibility tree per layout", () => {
    stubHealthApi();
    load("/app");
    const { container } = render(<App />);

    const navs = container.querySelectorAll("nav[aria-label='Primary']");
    expect(navs).toHaveLength(2);
    // The sidebar lives inside an aside that is display:none below the
    // breakpoint; the compact strip is display:none at wide widths. Both
    // share one accessible label but only one participates per viewport.
    const sidebar = navs[0]?.closest("aside");
    expect(sidebar?.className).toContain("hidden");
    expect(sidebar?.className).toContain("lg:flex");
    expect(navs[1]?.className).toContain("lg:hidden");
  });

  it("renders breadcrumbs for nested operational routes", () => {
    stubHealthApi();
    load("/app/runs/run-42/reconcile");
    render(<App />);

    const trail = screen.getByRole("navigation", { name: "Breadcrumb" });
    expect(breadcrumbLabels(trail)).toEqual([
      "Operations",
      "Runs",
      "run-42",
      "Reconcile",
    ]);
    expect(screen.getByText("Reconcile")).toHaveAttribute("aria-current", "page");
  });

  it("keeps the command palette reachable from the shell", () => {
    stubHealthApi();
    load("/app");
    render(<App />);

    expect(screen.getByRole("button", { name: /Commands/ })).toHaveAttribute(
      "aria-haspopup",
      "dialog",
    );
  });

  it("passes an automated accessibility scan of the console shell", async () => {
    stubHealthApi();
    load("/app/runs/run-42/reconcile");
    const { container } = render(<App />);

    await expectNoAccessibilityViolations(container);
  });

  it("moves focus to the page heading after shell navigation", async () => {
    stubHealthApi();
    const user = userEvent.setup();
    load("/app");
    render(<App />);

    const sidebar = screen.getByRole("complementary");
    await user.click(within(sidebar).getByRole("link", { name: "System" }));
    await waitFor(() => {
      expect(document.activeElement).toHaveTextContent("System health");
    });
    expect(document.activeElement).toHaveAttribute("data-page-title");
  });
});

/**
 * System capabilities tests (P17.7): every reported capability resolves to
 * available, unavailable with its structured reason, or unsupported;
 * unsupported is never healthy and a failed capability report renders as an
 * error instead of a healthy default.
 */
import { QueryClientProvider } from "@tanstack/react-query";
import { cleanup, render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import { SystemCapabilities } from "./system-screen";
import {
  deriveRunnerCapabilities,
  derivePoolCapabilities,
  deriveSqliteCapabilities,
  deriveProfileCapabilities,
} from "./capability-derivations";
import type { CapabilitiesValue } from "../../api/schemas";
import { appQueryClient } from "../../api/query-client";

function capabilities(
  overrides: Partial<Record<string, unknown>> = {},
): CapabilitiesValue {
  return {
    schema_version: 1,
    service: "ParityGrid",
    version: "0.1.0",
    runners: [
      { schema_version: 1, strategy_id: "sequential", available: true },
      {
        schema_version: 1,
        strategy_id: "asyncio",
        available: false,
        unavailability_reason: "no running event loop",
      },
    ],
    subordinate_pools: [
      {
        schema_version: 1,
        pool_id: "interpreter",
        available: false,
        unavailability_reason: "interpreter startup failed",
      },
    ],
    features: [{ name: "runtime-profile-loopback", available: true }],
    sqlite: {
      schema_version: 1,
      library_version: "3.45",
      minimum_supported_version: "3.35",
      journal_mode: "wal",
      synchronous_level: 1,
      busy_timeout_ms: 5000,
      threadsafety: 1,
      supports_json_sql: true,
      supports_returning: false,
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
    ...overrides,
  };
}

function stubSystemApi(body: unknown, ok = true): { requests: string[] } {
  const requests: string[] = [];
  const fetchMock = vi.fn<typeof fetch>((input: RequestInfo | URL) => {
    const url =
      typeof input === "string" ? input : input instanceof URL ? input.href : input.url;
    requests.push(url);
    if (url.includes("/api/v1/system/capabilities")) {
      if (!ok || body === null) {
        return Promise.resolve(
          new Response(
            JSON.stringify({ type: "about:blank", title: "boom", status: 500 }),
            { status: 500, headers: { "Content-Type": "application/problem+json" } },
          ),
        );
      }
      return Promise.resolve(
        new Response(JSON.stringify(body), {
          status: 200,
          headers: { "Content-Type": "application/json" },
        }),
      );
    }
    if (url === "/readyz") {
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
    if (url === "/healthz") {
      return Promise.resolve(
        new Response(
          JSON.stringify({ status: "ok", service: "ParityGrid", version: "0.1.0" }),
          { status: 200, headers: { "Content-Type": "application/json" } },
        ),
      );
    }
    return Promise.resolve(new Response("nope", { status: 404 }));
  });
  vi.stubGlobal("fetch", fetchMock);
  return { requests };
}

describe("capability derivations", () => {
  it("maps absent runner strategies to unsupported, not available", () => {
    const rows = deriveRunnerCapabilities(capabilities());
    const sequential = rows.find((row) => row.id === "runner-sequential");
    const asyncio = rows.find((row) => row.id === "runner-asyncio");
    const threaded = rows.find((row) => row.id === "runner-threaded");
    expect(sequential?.status).toBe("available");
    expect(asyncio?.status).toBe("unavailable");
    expect(asyncio?.reason).toBe("no running event loop");
    expect(threaded?.status).toBe("unsupported");
    expect(rows.some((row) => row.id === "runner-process")).toBe(false);
    expect(threaded?.reason).toBeNull();
  });

  it("reports empty pool and profile collections as unsupported", () => {
    const pools = derivePoolCapabilities(capabilities({ subordinate_pools: [] }));
    expect(pools[0]?.status).toBe("unsupported");
    const features = deriveProfileCapabilities(capabilities({ features: [] }));
    expect(features[0]?.status).toBe("unsupported");
  });

  it("resolves SQLite feature booleans to available or unavailable", () => {
    const rows = deriveSqliteCapabilities(capabilities());
    const returning = rows.find((row) => row.label === "RETURNING support");
    expect(returning?.status).toBe("unavailable");
    expect(returning?.reason).toContain("does not provide");
    const json = rows.find((row) => row.label === "JSON SQL support");
    expect(json?.status).toBe("available");
  });
});

describe("SystemCapabilities", () => {
  beforeEach(() => {
    appQueryClient.clear();
  });

  afterEach(() => {
    cleanup();
    vi.unstubAllGlobals();
  });

  it("reports available, unavailable, unsupported, readiness, and limits", async () => {
    stubSystemApi(capabilities());
    render(
      <QueryClientProvider client={appQueryClient}>
        <SystemCapabilities />
      </QueryClientProvider>,
    );

    // Wait for the runner rows (not just the section shell) to appear.
    await screen.findByText("Runner strategy sequential");
    const runners = screen.getByTestId("system-runners");
    expect(runners).toHaveTextContent("available");
    expect(runners).toHaveTextContent("reason: no running event loop");
    expect(runners).toHaveTextContent("unsupported");

    expect(screen.getByText("Worker pool interpreter")).toBeInTheDocument();
    expect(screen.getByText(/interpreter startup failed/)).toBeInTheDocument();
    expect(screen.getByText("runtime-profile-loopback")).toBeInTheDocument();
    expect(screen.getByText("max_page_size")).toBeInTheDocument();
    expect(screen.getByText("migrations applied")).toBeInTheDocument();
  });

  it("renders a failed capability report as an error, never as healthy", async () => {
    stubSystemApi("", false);
    render(
      <QueryClientProvider client={appQueryClient}>
        <SystemCapabilities />
      </QueryClientProvider>,
    );
    const alert = await screen.findByRole("alert");
    expect(alert).toHaveTextContent("Capability report unavailable");
    expect(screen.queryByText("available")).toBeNull();

    await userEvent.click(screen.getByRole("button", { name: "Retry" }));
    expect(screen.getByRole("alert")).toBeInTheDocument();
  });
});

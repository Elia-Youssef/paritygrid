import { render, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, describe, expect, it, vi } from "vitest";

import { App } from "./App";
import { appQueryClient } from "../api/query-client";

function stubHealthApi(): void {
  vi.stubGlobal(
    "fetch",
    vi
      .fn<typeof fetch>()
      .mockResolvedValue(
        new Response(
          JSON.stringify({ status: "ok", service: "ParityGrid", version: "0.1.0" }),
          { status: 200, headers: { "Content-Type": "application/json" } },
        ),
      ),
  );
}

function load(path: string): void {
  window.history.replaceState({}, "", path);
}

function currentNavigationLabels(): string[] {
  return screen
    .getAllByRole("navigation", { name: "Primary" })
    .flatMap((nav) =>
      Array.from(nav.querySelectorAll("[aria-current='page']")).map(
        (link) => link.textContent ?? "",
      ),
    );
}

afterEach(() => {
  vi.unstubAllGlobals();
  appQueryClient.clear();
  load("/");
  document.title = "";
});

describe("application routes", () => {
  it.each([
    ["/", "Reconciliation you can prove.", "ParityGrid"],
    ["/app", "Operations overview", "Operations overview · ParityGrid console"],
    ["/app/pipelines", "Pipeline library", "Pipeline library · ParityGrid console"],
    ["/app/pipelines/pl-9", "Pipeline studio", "Pipeline pl-9 · ParityGrid console"],
    ["/app/runs", "Run history", "Run history · ParityGrid console"],
    ["/app/runs/run-7", "Live execution", "Run run-7 · ParityGrid console"],
    [
      "/app/runs/run-7/reconcile",
      "Reconciliation workbench",
      "Reconcile · ParityGrid console",
    ],
    ["/app/repairs/rp-3", "Repair review", "Repair plan rp-3 · ParityGrid console"],
    ["/app/compare", "Runner comparison", "Runner comparison · ParityGrid console"],
    ["/app/system", "System health", "System health · ParityGrid console"],
  ])(
    "deep load %s renders the %s screen with its document title",
    (path, heading, title) => {
      stubHealthApi();
      load(path);
      render(<App />);

      expect(screen.getByRole("heading", { level: 1, name: heading })).toBeVisible();
      expect(document.title).toBe(title);
    },
  );

  it("keeps unknown console addresses inside the console layout", () => {
    stubHealthApi();
    load("/app/does-not-exist");
    render(<App />);

    expect(
      screen.getByRole("heading", { level: 1, name: "Screen not found" }),
    ).toBeVisible();
    expect(screen.getByRole("link", { name: "Operations overview" })).toBeVisible();
  });

  it("keeps unknown public addresses on the public layout", () => {
    stubHealthApi();
    load("/does-not-exist");
    render(<App />);

    expect(
      screen.getByRole("heading", { level: 1, name: "Screen not found" }),
    ).toBeVisible();
    expect(screen.getByRole("link", { name: "ParityGrid home" })).toBeVisible();
  });

  it("navigates from the public overview into the console", async () => {
    stubHealthApi();
    const user = userEvent.setup();
    load("/");
    render(<App />);

    await user.click(screen.getByRole("link", { name: "Open operations console" }));

    await waitFor(() => {
      expect(window.location.pathname).toBe("/app");
    });
    expect(
      screen.getByRole("heading", { level: 1, name: "Operations overview" }),
    ).toBeVisible();
    expect(document.title).toBe("Operations overview · ParityGrid console");
  });

  it("moves focus to the destination heading on client navigation", async () => {
    stubHealthApi();
    const user = userEvent.setup();
    load("/app");
    render(<App />);

    const sidebar = screen.getByRole("complementary");
    await user.click(within(sidebar).getByRole("link", { name: "Runs" }));

    await waitFor(() => {
      expect(window.location.pathname).toBe("/app/runs");
    });
    expect(
      screen.getByRole("heading", { level: 1, name: "Run history" }),
    ).toBeVisible();

    const focused = document.activeElement;
    expect(focused).not.toBeNull();
    expect(focused).toHaveAttribute("data-page-title");
    expect(focused).toHaveTextContent("Run history");
  });

  it("marks the same active page in every exposed navigation", async () => {
    stubHealthApi();
    const user = userEvent.setup();
    load("/app");
    render(<App />);

    expect(currentNavigationLabels()).toEqual(["Overview", "Overview"]);

    const sidebar = screen.getByRole("complementary");
    await user.click(within(sidebar).getByRole("link", { name: "Compare" }));

    await waitFor(() => {
      expect(window.location.pathname).toBe("/app/compare");
    });
    expect(currentNavigationLabels()).toEqual(["Compare", "Compare"]);
  });

  it("shows the operational context for a run screen", () => {
    stubHealthApi();
    load("/app/runs/run-77");
    render(<App />);

    // Desktop and compact contexts coexist in jsdom because breakpoint CSS is
    // not evaluated there; browsers expose one at a time.
    expect(screen.getAllByText("Run run-77")).toHaveLength(2);
  });

  it("shows the operational context for a repair plan screen", () => {
    stubHealthApi();
    load("/app/repairs/rp-8");
    render(<App />);

    expect(screen.getAllByText("Repair plan rp-8")).toHaveLength(2);
  });

  it("omits operational context outside detail screens", () => {
    stubHealthApi();
    load("/app/pipelines");
    render(<App />);

    expect(screen.queryByText(/^Repair plan /)).toBeNull();
    expect(screen.queryByText(/^Run /)).toBeNull();
  });
});

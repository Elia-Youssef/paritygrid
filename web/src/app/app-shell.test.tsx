import { render, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, describe, expect, it, vi } from "vitest";

import { App } from "./App";
import { expectNoAccessibilityViolations } from "../test/axe";

function stubHealthApi(status = 200): void {
  vi.stubGlobal(
    "fetch",
    vi
      .fn<typeof fetch>()
      .mockResolvedValue(new Response('{"status":"ok"}', { status })),
  );
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
    const fetchMock = vi
      .fn<typeof fetch>()
      .mockResolvedValueOnce(new Response('{"status":"unavailable"}', { status: 503 }))
      .mockResolvedValueOnce(new Response('{"status":"ok"}', { status: 200 }));
    vi.stubGlobal("fetch", fetchMock);
    const user = userEvent.setup();
    load("/app");
    render(<App />);

    expect(await screen.findByText("API unavailable")).toBeVisible();
    await user.click(screen.getByRole("button", { name: "Retry" }));

    expect(await screen.findByText("API online")).toBeVisible();
    expect(fetchMock).toHaveBeenCalledTimes(2);
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

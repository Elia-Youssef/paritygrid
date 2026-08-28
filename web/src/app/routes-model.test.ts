import { describe, expect, it } from "vitest";

import {
  buildBreadcrumbs,
  formatOperationContext,
  normalizePath,
  resolveOperationContext,
  resolveRouteTitle,
} from "./routes-model";

describe("normalizePath", () => {
  it("strips trailing slashes and keeps the root", () => {
    expect(normalizePath("/")).toBe("/");
    expect(normalizePath("/app/")).toBe("/app");
    expect(normalizePath("/app/runs/")).toBe("/app/runs");
    expect(normalizePath("")).toBe("/");
  });
});

describe("buildBreadcrumbs", () => {
  it("builds an empty trail outside the console", () => {
    expect(buildBreadcrumbs("/")).toEqual([]);
    expect(buildBreadcrumbs("/public")).toEqual([]);
  });

  it("labels static segments and links every level", () => {
    expect(buildBreadcrumbs("/app")).toEqual([
      { href: "/app", label: "Operations", monospace: false },
    ]);
    expect(buildBreadcrumbs("/app/runs")).toEqual([
      { href: "/app", label: "Operations", monospace: false },
      { href: "/app/runs", label: "Runs", monospace: false },
    ]);
  });

  it("marks dynamic identifiers as monospace crumbs", () => {
    const trail = buildBreadcrumbs("/app/runs/run-42/reconcile");
    expect(trail.map((crumb) => crumb.label)).toEqual([
      "Operations",
      "Runs",
      "run-42",
      "Reconcile",
    ]);
    expect(trail[2]).toMatchObject({ monospace: true, href: "/app/runs/run-42" });
  });

  it("treats unknown segments as identifiers", () => {
    const trail = buildBreadcrumbs("/app/unknown-segment");
    expect(trail[1]).toMatchObject({ label: "unknown-segment", monospace: true });
  });

  it("caps the trail for pathological addresses", () => {
    const trail = buildBreadcrumbs(`/app/a/b/c/d/e/f/g/h/i/j`);
    expect(trail).toHaveLength(8);
    expect(trail.at(-1)?.label).toBe("g");
  });
});

describe("resolveRouteTitle", () => {
  it("uses the public title at the root", () => {
    expect(resolveRouteTitle("/")).toBe("ParityGrid");
  });

  it.each([
    ["/app", "Operations overview"],
    ["/app/pipelines", "Pipeline library"],
    ["/app/runs", "Run history"],
    ["/app/compare", "Runner comparison"],
    ["/app/system", "System health"],
  ])("titles %s as %s", (path, title) => {
    expect(resolveRouteTitle(path)).toBe(title);
  });

  it("names operational identifiers by their parent area", () => {
    expect(resolveRouteTitle("/app/pipelines/pl-1")).toBe("Pipeline pl-1");
    expect(resolveRouteTitle("/app/runs/run-9")).toBe("Run run-9");
    expect(resolveRouteTitle("/app/repairs/rp-3")).toBe("Repair plan rp-3");
  });

  it("uses the static leaf label for nested operational screens", () => {
    expect(resolveRouteTitle("/app/runs/run-9/reconcile")).toBe("Reconcile");
  });

  it("falls back to the site name outside the console", () => {
    expect(resolveRouteTitle("/nowhere")).toBe("ParityGrid");
  });

  it("uses the identifier when no area builder applies", () => {
    expect(resolveRouteTitle("/app/anything/xyz")).toBe("xyz");
  });
});

describe("resolveOperationContext", () => {
  it("detects run context on run screens including reconcile", () => {
    expect(resolveOperationContext("/app/runs/run-42")).toEqual({
      kind: "run",
      id: "run-42",
    });
    expect(resolveOperationContext("/app/runs/run-42/reconcile")).toEqual({
      kind: "run",
      id: "run-42",
    });
  });

  it("detects repair context", () => {
    expect(resolveOperationContext("/app/repairs/rp-7")).toEqual({
      kind: "repair",
      id: "rp-7",
    });
  });

  it("returns nothing outside operational detail screens", () => {
    expect(resolveOperationContext("/app/runs")).toBeNull();
    expect(resolveOperationContext("/app")).toBeNull();
    expect(resolveOperationContext("/")).toBeNull();
  });

  it("does not match prefixed identifiers", () => {
    expect(resolveOperationContext("/app/runful")).toBeNull();
  });
});

describe("formatOperationContext", () => {
  it("names runs and repair plans", () => {
    expect(formatOperationContext({ kind: "run", id: "run-1" })).toBe("Run run-1");
    expect(formatOperationContext({ kind: "repair", id: "rp-2" })).toBe(
      "Repair plan rp-2",
    );
  });
});

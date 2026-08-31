import { readFileSync } from "node:fs";
import { resolve } from "node:path";

import { describe, expect, it } from "vitest";

import { compositeOver, contrastRatio, meetsMinimumContrast } from "./lib/contrast";
import { colorTokens, loadDesignTokens } from "./lib/tokens";

const tokens = loadDesignTokens();
const stylesheet = readFileSync(resolve(process.cwd(), "src/styles.css"), "utf8");

/**
 * Committed palette snapshot. A deliberate palette change updates this
 * table in the same change; anything unintended fails here.
 */
const expectedColors = {
  "--background": "#071017",
  "--foreground": "#ecf6f8",
  "--surface": "#0b1720",
  "--surface-elevated": "#10212c",
  "--surface-quiet": "#09131b",
  "--muted": "#91a7ad",
  "--muted-strong": "#b7c8cc",
  "--border": "#2a4652",
  "--border-strong": "#52727f",
  "--focus-ring": "#72e6eb",
  "--primary": "#42d4da",
  "--primary-foreground": "#031013",
  "--secondary": "#19303b",
  "--secondary-foreground": "#d9edef",
  "--state-active": "#42d4da",
  "--state-verified": "#42cd91",
  "--state-warning": "#e9b859",
  "--state-failure": "#ef6f79",
  "--state-stale": "#c9a15f",
  "--state-paused": "#a896ed",
  "--state-cancelled": "#d98d9b",
  "--chart-cyan": "#42d4da",
  "--chart-green": "#42cd91",
  "--chart-amber": "#e9b859",
  "--chart-violet": "#a896ed",
  "--chart-grid": "#2a4652",
  "--runner-sequential": "#b9c7cb",
  "--runner-threaded": "#42d4da",
  "--runner-async": "#42cd91",
  "--runner-process": "#a896ed",
  "--queue-idle": "#527b8f",
  "--queue-busy": "#b8893c",
  "--queue-saturated": "#d85f6b",
} as const;

type ColorToken = keyof typeof expectedColors;

function color(token: ColorToken): string {
  return expectedColors[token];
}

const requiredNonColorTokens = [
  "--font-sans",
  "--font-mono",
  "--text-2xs",
  "--text-xs",
  "--text-sm",
  "--text-base",
  "--text-lg",
  "--text-display",
  "--text-hero",
  "--leading-hero",
  "--tracking-label",
  "--tracking-brand",
  "--tracking-eyebrow",
  "--tracking-console",
  "--tracking-hero",
  "--space-1",
  "--space-2",
  "--space-3",
  "--space-4",
  "--space-6",
  "--space-8",
  "--space-12",
  "--radius-sm",
  "--radius-md",
  "--radius-lg",
  "--shadow-elevated",
  "--shadow-overlay",
  "--motion-fast",
  "--motion-base",
  "--motion-slow",
  "--ease-standard",
  "--shell-sidebar-width",
  "--shell-header-height",
  "--content-max",
  "--layout-overview-hero-columns",
  "--layout-overview-content-columns",
  "--layout-overview-readiness-columns",
] as const;

describe("semantic design token snapshot", () => {
  it("defines exactly the committed color palette", () => {
    expect(colorTokens(tokens)).toEqual(expectedColors);
  });

  it.each(requiredNonColorTokens)("defines %s", (token) => {
    expect(tokens[token]).toBeTruthy();
  });
});

describe("WCAG 2.1 AA text contrast", () => {
  const surfaces: readonly ColorToken[] = [
    "--background",
    "--surface",
    "--surface-elevated",
    "--surface-quiet",
  ];

  it.each(surfaces)("foreground on %s", (surface) => {
    const ratio = contrastRatio(color("--foreground"), color(surface));
    expect(meetsMinimumContrast(ratio)).toBe(true);
  });

  it.each(surfaces)("muted on %s", (surface) => {
    const ratio = contrastRatio(color("--muted"), color(surface));
    expect(meetsMinimumContrast(ratio)).toBe(true);
  });

  it.each(surfaces)("muted-strong on %s", (surface) => {
    const ratio = contrastRatio(color("--muted-strong"), color(surface));
    expect(meetsMinimumContrast(ratio)).toBe(true);
  });

  it("action foregrounds meet AA on their own action colors", () => {
    expect(
      meetsMinimumContrast(
        contrastRatio(color("--primary-foreground"), color("--primary")),
      ),
    ).toBe(true);
    expect(
      meetsMinimumContrast(
        contrastRatio(color("--secondary-foreground"), color("--secondary")),
      ),
    ).toBe(true);
  });

  it("destructive foreground meets AA on the failure color", () => {
    const ratio = contrastRatio(color("--background"), color("--state-failure"));
    expect(meetsMinimumContrast(ratio)).toBe(true);
  });

  it.each([
    "--state-active",
    "--state-verified",
    "--state-warning",
    "--state-failure",
    "--state-stale",
    "--state-paused",
    "--state-cancelled",
  ] as const)("status color %s meets AA on the page background", (state) => {
    const ratio = contrastRatio(color(state), color("--background"));
    expect(meetsMinimumContrast(ratio)).toBe(true);
  });

  it.each([
    "--state-active",
    "--state-verified",
    "--state-warning",
    "--state-failure",
    "--state-stale",
    "--state-paused",
    "--state-cancelled",
  ] as const)("status color %s meets AA on its own badge tint", (state) => {
    // Badges composite 10% of the state color over the surface; the state
    // color itself is the text color.
    const badgeBackground = compositeOver(color(state), color("--surface"), 0.1);
    const ratio = contrastRatio(color(state), badgeBackground);
    expect(meetsMinimumContrast(ratio)).toBe(true);
  });
});

describe("WCAG 2.1 AA non-text contrast", () => {
  it("focus ring is visible against every surface", () => {
    for (const surface of [
      "--background",
      "--surface",
      "--surface-elevated",
    ] as const) {
      const ratio = contrastRatio(color("--focus-ring"), color(surface));
      expect(meetsMinimumContrast(ratio, "large")).toBe(true);
    }
  });

  it("component boundaries (border-strong) separate from every surface", () => {
    for (const surface of [
      "--background",
      "--surface",
      "--surface-elevated",
    ] as const) {
      const ratio = contrastRatio(color("--border-strong"), color(surface));
      expect(meetsMinimumContrast(ratio, "large")).toBe(true);
    }
  });

  it("primary action color is distinguishable from the background", () => {
    const ratio = contrastRatio(color("--primary"), color("--background"));
    expect(meetsMinimumContrast(ratio, "large")).toBe(true);
  });

  it.each(["--queue-idle", "--queue-busy", "--queue-saturated"] as const)(
    "queue saturation band %s separates from the surface",
    (band) => {
      const ratio = contrastRatio(color(band), color("--surface"));
      expect(meetsMinimumContrast(ratio, "large")).toBe(true);
    },
  );

  it.each([
    "--runner-sequential",
    "--runner-threaded",
    "--runner-async",
    "--runner-process",
  ] as const)("runner identity color %s separates from the surface", (runner) => {
    const ratio = contrastRatio(color(runner), color("--surface"));
    expect(meetsMinimumContrast(ratio, "large")).toBe(true);
  });

  it.each([
    "--chart-cyan",
    "--chart-green",
    "--chart-amber",
    "--chart-violet",
  ] as const)("chart series %s separates from the surface", (series) => {
    const ratio = contrastRatio(color(series), color("--surface"));
    expect(meetsMinimumContrast(ratio, "large")).toBe(true);
  });
});

describe("responsive and motion policy", () => {
  it("keeps a 320px floor for very narrow devices", () => {
    expect(stylesheet).toContain("min-width: 320px");
  });

  it("declares a reduced-motion override", () => {
    expect(stylesheet).toContain("prefers-reduced-motion: reduce");
  });

  it("drives the shell layout from tokens", () => {
    expect(stylesheet).toContain("--shell-sidebar-width: 16rem");
    expect(stylesheet).toContain("--shell-header-height: 4rem");
    expect(stylesheet).toContain("--content-max: 92rem");
  });
});

import { readFileSync } from "node:fs";
import { resolve } from "node:path";

import { describe, expect, it } from "vitest";

const tokenSource = readFileSync(resolve(process.cwd(), "src/styles.css"), "utf8");

const requiredTokens = [
  "--background",
  "--foreground",
  "--surface",
  "--surface-elevated",
  "--muted",
  "--border",
  "--focus-ring",
  "--primary",
  "--secondary",
  "--state-active",
  "--state-verified",
  "--state-warning",
  "--state-failure",
  "--state-stale",
  "--state-paused",
  "--state-cancelled",
  "--chart-cyan",
  "--runner-sequential",
  "--queue-saturated",
  "--font-sans",
  "--font-mono",
  "--space-4",
  "--radius-md",
  "--shadow-elevated",
  "--motion-base",
  "--ease-standard",
] as const;

describe("semantic design token contract", () => {
  it.each(requiredTokens)("defines %s", (token) => {
    expect(tokenSource).toContain(`${token}:`);
  });

  it("includes a reduced-motion policy", () => {
    expect(tokenSource).toContain("prefers-reduced-motion: reduce");
  });
});

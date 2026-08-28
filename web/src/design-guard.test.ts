import { readdirSync, readFileSync, statSync } from "node:fs";
import { join, resolve } from "node:path";

import { describe, expect, it } from "vitest";

/**
 * Hard gate: feature and component code communicates state exclusively
 * through semantic design tokens. Raw color literals anywhere in a TSX file
 * would bypass the design layer, so they fail this scan.
 */

const COLOR_LITERAL_PATTERN = /#[0-9a-fA-F]{3,8}\b|\brgba?\(/;

/**
 * Typography and layout values that have a semantic token must be consumed
 * through their Tailwind utility. Arbitrary values are deliberately rejected
 * here so a future feature cannot silently fork the shared visual language.
 */
const TOKEN_BYPASS_PATTERN =
  /\b(?:text|leading|tracking|grid-cols)-\[(?!var\(--)[^\]]+\]|\bmax-w-\[(?!var\(--)[^\]]+\]/;

function collectTsxFiles(directory: string): string[] {
  const entries: string[] = [];
  for (const name of readdirSync(directory)) {
    const path = join(directory, name);
    if (statSync(path).isDirectory()) {
      if (name === "test") {
        continue;
      }
      entries.push(...collectTsxFiles(path));
      continue;
    }
    if (name.endsWith(".tsx")) {
      entries.push(path);
    }
  }
  return entries;
}

describe("design-token guard", () => {
  it("finds no raw color literals in component or feature code", () => {
    const sourceRoot = resolve(process.cwd(), "src");
    const offenders = collectTsxFiles(sourceRoot).filter((path) =>
      COLOR_LITERAL_PATTERN.test(readFileSync(path, "utf8")),
    );
    expect(offenders).toEqual([]);
  });

  it("finds no arbitrary typography or layout values that bypass semantic tokens", () => {
    const sourceRoot = resolve(process.cwd(), "src");
    const offenders = collectTsxFiles(sourceRoot).filter((path) =>
      TOKEN_BYPASS_PATTERN.test(readFileSync(path, "utf8")),
    );
    expect(offenders).toEqual([]);
  });
});

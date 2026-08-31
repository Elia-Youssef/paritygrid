/**
 * Reader for the semantic design tokens committed in `src/styles.css`.
 *
 * Tests snapshot the parsed token table and execute contrast checks against
 * it, so the stylesheet stays the single source of truth for the palette.
 */

import { readFileSync } from "node:fs";
import { resolve } from "node:path";

export type TokenTable = Readonly<Record<string, string>>;

const CUSTOM_PROPERTY_PATTERN = /(--[a-z0-9-]+)\s*:\s*([^;]+);/gi;

/**
 * Extract every custom property from the `:root` token block of the
 * stylesheet. Later declarations win, matching CSS cascade behavior.
 */
export function parseTokenBlock(stylesheet: string): TokenTable {
  const rootStart = stylesheet.indexOf(":root");
  if (rootStart < 0) {
    return {};
  }

  const blockStart = stylesheet.indexOf("{", rootStart);
  const blockEnd = stylesheet.indexOf("}", blockStart);
  if (blockStart < 0 || blockEnd < 0) {
    return {};
  }

  const tokens: Record<string, string> = {};
  const block = stylesheet.slice(blockStart + 1, blockEnd);
  for (const match of block.matchAll(CUSTOM_PROPERTY_PATTERN)) {
    const [, name, rawValue] = match;
    if (name === undefined || rawValue === undefined) {
      continue;
    }
    tokens[name.toLowerCase()] = rawValue.trim();
  }
  return tokens;
}

export function loadDesignTokens(
  stylesheetPath = resolve(process.cwd(), "src/styles.css"),
): TokenTable {
  return parseTokenBlock(readFileSync(stylesheetPath, "utf8"));
}

const HEX_COLOR_PATTERN = /^#[0-9a-f]{6}$/i;

export function isHexColor(value: string): boolean {
  return HEX_COLOR_PATTERN.test(value);
}

export function colorTokens(tokens: TokenTable): Record<string, string> {
  return Object.fromEntries(
    Object.entries(tokens).filter((entry): entry is [string, string] =>
      isHexColor(entry[1]),
    ),
  );
}

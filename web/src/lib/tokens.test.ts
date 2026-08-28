import { describe, expect, it } from "vitest";

import { colorTokens, isHexColor, parseTokenBlock } from "./tokens";

describe("parseTokenBlock", () => {
  it("extracts custom properties from the root block", () => {
    const stylesheet = `
      @import "tailwindcss";
      :root {
        --background: #071017;
        --motion-fast: 120ms;
      }
      @theme inline { --color-background: var(--background); }
    `;
    expect(parseTokenBlock(stylesheet)).toEqual({
      "--background": "#071017",
      "--motion-fast": "120ms",
    });
  });

  it("keeps the last declaration when a token is overridden", () => {
    const stylesheet = ":root { --accent: #111111; --accent: #222222; }";
    expect(parseTokenBlock(stylesheet)["--accent"]).toBe("#222222");
  });

  it("normalizes token names case-insensitively", () => {
    const stylesheet = ":root { --Accent: #112233; }";
    expect(parseTokenBlock(stylesheet)["--accent"]).toBe("#112233");
  });

  it("returns nothing when the stylesheet has no root block", () => {
    expect(parseTokenBlock("body { color: red; }")).toEqual({});
  });

  it("returns nothing when the root block is unterminated", () => {
    expect(parseTokenBlock(":root { --background: #071017;")).toEqual({});
  });
});

describe("color token selection", () => {
  it("keeps only six-digit hex values", () => {
    const tokens = {
      "--background": "#071017",
      "--motion-fast": "120ms",
      "--short": "#abc",
    };
    expect(colorTokens(tokens)).toEqual({ "--background": "#071017" });
  });

  it("identifies hex colors", () => {
    expect(isHexColor("#071017")).toBe(true);
    expect(isHexColor("#07101")).toBe(false);
    expect(isHexColor("071017")).toBe(false);
  });
});

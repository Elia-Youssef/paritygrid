import { describe, expect, it } from "vitest";

import {
  compositeOver,
  contrastRatio,
  meetsMinimumContrast,
  parseHexColor,
  relativeLuminance,
} from "./contrast";

describe("parseHexColor", () => {
  it("parses six-digit hex into sRGB channels", () => {
    expect(parseHexColor("#42d4da")).toEqual({ red: 0x42, green: 0xd4, blue: 0xda });
  });

  it("parses three-digit hex by doubling digits", () => {
    expect(parseHexColor("#0af")).toEqual({ red: 0x00, green: 0xaa, blue: 0xff });
  });

  it("rejects values that are not hex colors", () => {
    expect(() => parseHexColor("42d4da")).toThrow();
    expect(() => parseHexColor("#12345")).toThrow();
    expect(() => parseHexColor("#gggggg")).toThrow();
  });
});

describe("relativeLuminance", () => {
  it("returns 0 for black and 1 for white", () => {
    expect(relativeLuminance(parseHexColor("#000000"))).toBe(0);
    expect(relativeLuminance(parseHexColor("#ffffff"))).toBeCloseTo(1, 5);
  });

  it("weights green highest per the WCAG coefficients", () => {
    const green = relativeLuminance({ red: 0, green: 255, blue: 0 });
    const red = relativeLuminance({ red: 255, green: 0, blue: 0 });
    const blue = relativeLuminance({ red: 0, green: 0, blue: 255 });
    expect(green).toBeGreaterThan(red);
    expect(red).toBeGreaterThan(blue);
  });
});

describe("contrastRatio", () => {
  it("caps at 21:1 for black against white", () => {
    expect(contrastRatio("#000000", "#ffffff")).toBeCloseTo(21, 5);
  });

  it("is symmetric regardless of argument order", () => {
    expect(contrastRatio("#071017", "#ecf6f8")).toBeCloseTo(
      contrastRatio("#ecf6f8", "#071017"),
      10,
    );
  });
});

describe("compositeOver", () => {
  it("composites a translucent layer over an opaque background", () => {
    // Half cyan over black is exactly half of each channel.
    expect(compositeOver("#42d4da", "#000000", 0.5)).toBe("#216a6d");
  });

  it("is transparent at alpha zero and opaque at alpha one", () => {
    expect(compositeOver("#42d4da", "#0b1720", 0)).toBe("#0b1720");
    expect(compositeOver("#42d4da", "#0b1720", 1)).toBe("#42d4da");
  });

  it("rejects out-of-range alpha", () => {
    expect(() => compositeOver("#ffffff", "#000000", -0.1)).toThrow();
    expect(() => compositeOver("#ffffff", "#000000", 1.1)).toThrow();
  });
});

describe("meetsMinimumContrast", () => {
  it("applies 4.5:1 for normal text and 3:1 for large text", () => {
    expect(meetsMinimumContrast(4.5)).toBe(true);
    expect(meetsMinimumContrast(4.49)).toBe(false);
    expect(meetsMinimumContrast(3, "large")).toBe(true);
    expect(meetsMinimumContrast(2.99, "large")).toBe(false);
  });
});

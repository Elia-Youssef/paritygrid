/**
 * WCAG 2.1 relative-luminance and contrast-ratio math over sRGB hex colors.
 *
 * The design-token tests execute this module against the committed palette so
 * contrast failures are caught at test time rather than in review.
 */

export interface SrgbColor {
  red: number;
  green: number;
  blue: number;
}

const HEX_PATTERN = /^#([0-9a-f]{3}|[0-9a-f]{6})$/i;

export function parseHexColor(value: string): SrgbColor {
  if (!HEX_PATTERN.test(value)) {
    throw new Error(`Unsupported color value: ${value}`);
  }

  const digits = value.slice(1);
  const expanded =
    digits.length === 3
      ? digits
          .split("")
          .map((digit) => digit + digit)
          .join("")
      : digits;

  return {
    red: Number.parseInt(expanded.slice(0, 2), 16),
    green: Number.parseInt(expanded.slice(2, 4), 16),
    blue: Number.parseInt(expanded.slice(4, 6), 16),
  };
}

function channelLuminance(channel: number): number {
  const linear = channel / 255;
  return linear <= 0.03928 ? linear / 12.92 : ((linear + 0.055) / 1.055) ** 2.4;
}

export function relativeLuminance(color: SrgbColor): number {
  return (
    0.2126 * channelLuminance(color.red) +
    0.7152 * channelLuminance(color.green) +
    0.0722 * channelLuminance(color.blue)
  );
}

export function contrastRatio(first: string, second: string): number {
  const firstLuminance = relativeLuminance(parseHexColor(first));
  const secondLuminance = relativeLuminance(parseHexColor(second));
  const lighter = Math.max(firstLuminance, secondLuminance);
  const darker = Math.min(firstLuminance, secondLuminance);
  return (lighter + 0.05) / (darker + 0.05);
}

/** Composite a translucent foreground over an opaque background (alpha 0-1). */
export function compositeOver(
  foreground: string,
  background: string,
  alpha: number,
): string {
  if (alpha < 0 || alpha > 1) {
    throw new Error(`Alpha must be between 0 and 1: ${alpha}`);
  }

  const front = parseHexColor(foreground);
  const back = parseHexColor(background);
  const mix = (frontChannel: number, backChannel: number): number =>
    Math.round(frontChannel * alpha + backChannel * (1 - alpha));
  const channels = [
    mix(front.red, back.red),
    mix(front.green, back.green),
    mix(front.blue, back.blue),
  ];

  return `#${channels
    .map((channel) => channel.toString(16).padStart(2, "0"))
    .join("")}`;
}

export type TextScale = "normal" | "large";

const MINIMUM_CONTRAST: Record<TextScale, number> = {
  normal: 4.5,
  large: 3,
};

export function meetsMinimumContrast(
  ratio: number,
  scale: TextScale = "normal",
): boolean {
  return ratio >= MINIMUM_CONTRAST[scale];
}

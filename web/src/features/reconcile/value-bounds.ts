/**
 * Bounded, redacted transformations for untrusted values, kept separate
 * from the components that render them so both the presentation files and
 * the tests can share one implementation. Difference texts received from
 * the server are hostile input: they may contain credential-shaped
 * strings, control characters, markup-looking content, or very long
 * payloads. Nothing here produces HTML; every result is inert text.
 */
import { FIELD_DIFFERENCE_KINDS, type FieldDifferenceKind } from "./classifications";

/** Display bound for a single value; longer values are truncated. */
export const MAX_DISPLAY_LENGTH = 512;

const TRUNCATION_SUFFIX = "…";

// The control-character range is the point of this pattern; the shared
// diagnostic bounding in lib/problem-details uses the same narrow disable.
// eslint-disable-next-line no-control-regex
const CONTROL_CHARACTERS = /[\u0000-\u001f\u007f-\u009f]/g;

/**
 * Credential-bearing shapes redacted before text reaches the screen. The
 * intent mirrors the shared Problem Details redaction: assignment-style
 * secrets, query-string credentials, userinfo in URLs, and bearer tokens
 * all collapse to [redacted] while benign text passes through unchanged.
 */
const SECRET_VALUE_PATTERNS: readonly {
  pattern: RegExp;
  replacement: string;
}[] = [
  // name: value / name = value assignments, including "Bearer " prefixes.
  {
    pattern:
      /(\b(?:authorization|cookie|token|secret|password|passphrase|api[-_]?key|credential)\b["']?\s*(?::|=)\s*["']?)(?:bearer\s+)?[^"'\s,;}\]]+/gi,
    replacement: "$1[redacted]",
  },
  // Standalone bearer credentials, including JWT-shaped tokens.
  {
    pattern: /\bbearer\s+[A-Za-z0-9._~+/-]+={0,2}/gi,
    replacement: "Bearer [redacted]",
  },
  // Credentials embedded in URL query strings.
  {
    pattern:
      /([?&](?:authorization|cookie|token|secret|password|passphrase|api[-_]?key|credential)=)[^&#\s]*/gi,
    replacement: "$1[redacted]",
  },
  // Userinfo in absolute URLs (scheme://user:pass@host); the separator is
  // kept so the redacted URL still reads as a URL.
  {
    pattern: /([a-z][a-z0-9+.-]*:\/\/)[^@/\s]+(@)/gi,
    replacement: "$1[redacted]$2",
  },
];

/**
 * Replace secret-shaped content with [redacted]. Deterministic: the same
 * input always produces the same output.
 */
export function redactSecretLike(value: string): string {
  return SECRET_VALUE_PATTERNS.reduce(
    (redacted, entry) => redacted.replace(entry.pattern, entry.replacement),
    value,
  );
}

/**
 * Strip control characters (replaced with a single space, mirroring the
 * shared diagnostic bounding) and cap the length with a trailing ellipsis.
 */
export function boundDisplayValue(
  value: string,
  maxLength = MAX_DISPLAY_LENGTH,
): string {
  const printable = value.replace(CONTROL_CHARACTERS, " ");
  if (printable.length <= maxLength) {
    return printable;
  }
  return printable.slice(0, maxLength) + TRUNCATION_SUFFIX;
}

/**
 * The difference `kind` from the server is the authoritative statement of
 * whether a side is missing or explicitly null — never guess it from the
 * rendered text. Returns a fixed human phrase for the four missing/null
 * kinds and null when both sides carry comparable text.
 */
export function describeMissingOrNull(kind: FieldDifferenceKind): string | null {
  switch (kind) {
    case "missing_on_source":
      return "absent on the source side";
    case "missing_on_target":
      return "absent on the target side";
    case "null_on_source":
      return "explicitly null on the source side";
    case "null_on_target":
      return "explicitly null on the target side";
    case "value_mismatch":
    case "type_mismatch":
      return null;
  }
}

/**
 * Checked conversion of a server-supplied difference kind to the contract's
 * closed set. Returns null for anything unrecognized so callers can fail
 * safe with a visible note instead of inventing a label.
 */
export function toFieldDifferenceKind(value: string): FieldDifferenceKind | null {
  for (const candidate of FIELD_DIFFERENCE_KINDS) {
    if (candidate === value) {
      return candidate;
    }
  }
  return null;
}

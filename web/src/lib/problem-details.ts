/**
 * Bounded, text-only presentation model for RFC 9457 Problem Details
 * payloads. API and upstream strings are untrusted: the parser whitelists
 * fields, caps sizes, redacts secret-shaped extension names, and never
 * carries stack traces or nested diagnostic structures into the UI.
 */

export interface ProblemDetailsView {
  title: string;
  status?: number;
  type?: string;
  detail?: string;
  instance?: string;
  extensions: readonly ProblemDetailsExtension[];
}

export interface ProblemDetailsExtension {
  name: string;
  value: string;
}

/** RFC 9457 standard members presented by the shared error state. */
const STANDARD_MEMBERS = ["type", "title", "detail", "status", "instance"] as const;

export const MAX_FIELD_LENGTH = 512;
export const MAX_EXTENSION_FIELDS = 8;
export const GENERIC_ERROR_TITLE = "Request failed";

const TRUNCATION_SUFFIX = "…";

const REDACTED_VALUE = "[redacted]";

const SECRET_NAME_PATTERN =
  /(authorization|cookie|token|secret|password|passphrase|api[-_]?key|credential)/i;

const SECRET_VALUE_PATTERNS: readonly RegExp[] = [
  /(\b(?:authorization|cookie|token|secret|password|passphrase|api[-_]?key|credential)\b["']?\s*(?::|=)\s*["']?)(?:bearer\s+)?[^"'\s,;}\]]+/gi,
  /([?&](?:authorization|cookie|token|secret|password|passphrase|api[-_]?key|credential)=)[^&#\s]*/gi,
  /([a-z][a-z0-9+.-]*:\/\/)[^@/\s]+@/gi,
];

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

/** Strip control characters (including C1) and cap length for safe display. */
export function boundText(value: string, maxLength = MAX_FIELD_LENGTH): string {
  const printable = value.replace(
    // eslint-disable-next-line no-control-regex
    /[\u0000-\u001f\u007f-\u009f]/g,
    " ",
  );
  if (printable.length <= maxLength) {
    return printable;
  }
  return printable.slice(0, maxLength) + TRUNCATION_SUFFIX;
}

/**
 * Remove common credential-bearing shapes from untrusted text before it can
 * reach a browser diagnostic. Field-name redaction handles structured
 * payloads; this catches the same values embedded in otherwise innocuous
 * standard members such as `detail` or `instance`.
 */
function redactSecretValues(value: string): string {
  return SECRET_VALUE_PATTERNS.reduce(
    (redacted, pattern) => redacted.replace(pattern, "$1[redacted]"),
    value,
  );
}

function boundStatus(value: unknown): number | undefined {
  if (typeof value !== "number" || !Number.isInteger(value)) {
    return undefined;
  }
  return value >= 100 && value <= 599 ? value : undefined;
}

function boundMember(value: unknown): string | undefined {
  if (typeof value !== "string") {
    return undefined;
  }
  const bounded = redactSecretValues(boundText(value)).trim();
  return bounded.length > 0 ? bounded : undefined;
}

function extensionValue(value: unknown): string {
  if (typeof value === "string") {
    return redactSecretValues(boundText(value));
  }
  if (typeof value === "number" || typeof value === "boolean") {
    return boundText(String(value));
  }
  return "[structured value omitted]";
}

function parseExtensions(member: Record<string, unknown>): ProblemDetailsExtension[] {
  const extensions: ProblemDetailsExtension[] = [];
  for (const name of Object.keys(member)) {
    if ((STANDARD_MEMBERS as readonly string[]).includes(name)) {
      continue;
    }
    const value = SECRET_NAME_PATTERN.test(name)
      ? REDACTED_VALUE
      : extensionValue(member[name]);
    extensions.push({ name: boundText(name, 128), value });
    if (extensions.length === MAX_EXTENSION_FIELDS) {
      break;
    }
  }
  return extensions;
}

/**
 * Convert an untrusted payload into the bounded presentation model. Any
 * input that is not a Problem Details object collapses to the generic title
 * so hostile or malformed bodies can never shape the rendered output.
 */
export function parseProblemDetails(input: unknown): ProblemDetailsView {
  if (!isRecord(input)) {
    return { title: GENERIC_ERROR_TITLE, extensions: [] };
  }

  const status = boundStatus(input.status);
  const title = boundMember(input.title) ?? GENERIC_ERROR_TITLE;

  return {
    title,
    status,
    type: boundMember(input.type),
    detail: boundMember(input.detail),
    instance: boundMember(input.instance),
    extensions: parseExtensions(input),
  };
}

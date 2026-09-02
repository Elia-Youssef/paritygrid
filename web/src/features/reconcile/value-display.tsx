/**
 * Inert rendering of untrusted values. All bounding, control-character
 * stripping, and secret redaction lives in ./value-bounds so the rules are
 * testable independently of the component; this file only chooses markup.
 * Nothing here ever produces HTML.
 */
import {
  boundDisplayValue,
  MAX_DISPLAY_LENGTH,
  redactSecretLike,
} from "./value-bounds";

export interface BoundedValueProps {
  text: string;
  maxLength?: number;
  /** Render as a monospace block (multi-line friendly) instead of inline. */
  multiline?: boolean;
}

/**
 * Render an untrusted value as inert React text: control characters
 * stripped, length bounded, secret-shaped content redacted. Truncation is
 * announced with a visible note so a silently clipped value can never be
 * mistaken for the complete one.
 */
export function BoundedValue(props: BoundedValueProps): React.JSX.Element {
  const maxLength = props.maxLength ?? MAX_DISPLAY_LENGTH;
  const stripped = boundDisplayValue(props.text, Number.POSITIVE_INFINITY);
  const truncated = stripped.length > maxLength;
  const safe = redactSecretLike(
    truncated ? stripped.slice(0, maxLength) + "…" : stripped,
  );

  const truncationNote = truncated ? (
    <span className="ml-2 text-2xs font-semibold uppercase tracking-label text-warning">
      value truncated
    </span>
  ) : null;

  if (props.multiline === true) {
    return (
      <>
        <pre className="max-w-full overflow-x-auto whitespace-pre-wrap break-words rounded border border-border bg-surface-quiet p-2 font-mono text-xs text-foreground">
          {safe}
        </pre>
        {truncationNote}
      </>
    );
  }
  return (
    <>
      <span className="break-all font-mono text-xs text-foreground">{safe}</span>
      {truncationNote}
    </>
  );
}

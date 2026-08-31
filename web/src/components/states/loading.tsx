export interface LoadingProps {
  /** Visible status text; the state is never conveyed by animation alone. */
  label?: string;
}

/**
 * Work-in-progress status. Exposed as a live region so assistive tech
 * announces the wait; the spinner is decorative.
 */
export function Loading({ label = "Loading" }: LoadingProps) {
  return (
    <div
      role="status"
      aria-live="polite"
      className="flex items-center gap-3 text-sm text-muted-strong"
    >
      <span
        aria-hidden="true"
        className="size-4 shrink-0 animate-spin rounded-full border-2 border-border-strong border-t-primary motion-reduce:animate-none"
      />
      <span>{label}…</span>
    </div>
  );
}

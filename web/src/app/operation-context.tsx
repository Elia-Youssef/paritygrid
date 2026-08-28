import { Activity, Wrench } from "lucide-react";

import { formatOperationContext, resolveOperationContext } from "./routes-model";

/**
 * Persistent operational context for the top bar: the run or repair plan a
 * nested screen is working on, derived from the current location.
 */
export function OperationContextIndicator({
  pathname,
  compact = false,
}: {
  pathname: string;
  compact?: boolean;
}) {
  const context = resolveOperationContext(pathname);

  if (context === null) {
    return null;
  }

  const Icon = context.kind === "run" ? Activity : Wrench;

  return (
    <span
      className={
        compact
          ? "inline-flex min-w-0 items-center gap-2 rounded-full border border-border bg-surface px-2.5 py-1"
          : "hidden min-w-0 items-center gap-2 rounded-full border border-border bg-surface px-3 py-1 md:inline-flex"
      }
    >
      <Icon className="size-3.5 shrink-0 text-active" aria-hidden="true" />
      <span className="font-mono text-2xs text-muted-strong">
        {formatOperationContext(context)}
      </span>
    </span>
  );
}

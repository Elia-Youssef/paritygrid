import { OctagonAlert } from "lucide-react";
import type { ReactNode, Ref } from "react";

import type { ProblemDetailsView } from "../../lib/problem-details";
import { ProblemDetailsBlock } from "./problem-details-block";

export interface ErrorStateProps {
  title?: string;
  description?: string;
  /** Structured API failure, pre-bounded by parseProblemDetails. */
  problem?: ProblemDetailsView;
  /** Recovery affordances, e.g. a retry Button. */
  action?: ReactNode;
  /** Focus target supplied by a recovery boundary after it replaces content. */
  focusRef?: Ref<HTMLDivElement>;
}

/**
 * Failure presentation for data that could not be loaded or an operation
 * that did not succeed. The message is text, and Problem Details members
 * render as inert text with bounded size.
 */
export function ErrorState({
  title = "Request failed",
  description,
  problem,
  action,
  focusRef,
}: ErrorStateProps) {
  return (
    <div
      ref={focusRef}
      role="alert"
      tabIndex={focusRef === undefined ? undefined : -1}
      className="rounded-md border border-failure/40 bg-failure/5 p-6"
    >
      <div className="flex items-start gap-3">
        <OctagonAlert
          className="mt-0.5 size-5 shrink-0 text-failure"
          aria-hidden="true"
        />
        <div className="min-w-0">
          <p className="text-sm font-semibold text-foreground">{title}</p>
          {description && (
            <p className="mt-1 text-sm text-muted-strong">{description}</p>
          )}
        </div>
      </div>
      {problem && <ProblemDetailsBlock problem={problem} />}
      {action && <div className="mt-4 flex flex-wrap gap-2">{action}</div>}
    </div>
  );
}

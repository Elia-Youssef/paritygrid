/**
 * Presentational application-progress panel for one repair plan.
 *
 * Authority labels are enforced visually here: the plan status badge and the
 * per-action counts come from the authoritative plan response, while the
 * durable SSE event list is explicitly labeled as notifications only — never
 * authoritative state. Apply outcomes render from the stored apply response
 * disposition; unresolved and interrupted outcomes are always presented as
 * recoverable (retry resumes the same plan), and a 503 error can never
 * render as success.
 */
import { isApiRequestError } from "../../api/client";
import type { RepairApplyResponse } from "../../api/generated/schema";
import { Loading } from "../../components/states/loading";
import { StatusBadge } from "../../components/ui/status-badge";
import { describeDisposition, describePlanStatus } from "./repair-contract";
import type { RepairProgress as RepairProgressView } from "./use-repair-plan";

export interface RepairActionCounts {
  total: number;
  pending: number;
  applied: number;
  failed: number;
}

export interface RepairProgressProps {
  planStatus: string;
  progress: RepairProgressView;
  /** Authoritative per-action counts from the plan response. */
  actionSummary: RepairActionCounts;
  applyResult: RepairApplyResponse | null;
  /** The apply failure, if any; a 503 renders as a recoverable failure. */
  applyError: unknown;
  isApplying: boolean;
}

/** Untrusted diagnostic text is bounded before it can reach the DOM. */
function bounded(value: string, maxLength = 512): string {
  const flattened = value.replace(/\s+/g, " ").trim();
  return flattened.length > maxLength ? `${flattened.slice(0, maxLength)}…` : flattened;
}

const OUTCOME_TONE_CLASS: Record<string, string> = {
  success: "border-verified/40 bg-verified/10",
  warning: "border-warning/40 bg-warning/10",
  failure: "border-failure/40 bg-failure/10",
  neutral: "border-border bg-surface-quiet",
};

const MAX_RENDERED_NOTIFICATIONS = 50;

/** Maps a contract tone onto the badge states the design system defines. */
function toneToBadgeState(
  tone: "neutral" | "info" | "success" | "warning" | "failure",
): "active" | "verified" | "warning" | "failure" | "neutral" {
  switch (tone) {
    case "info":
      return "active";
    case "success":
      return "verified";
    case "warning":
      return "warning";
    case "failure":
      return "failure";
    case "neutral":
      return "neutral";
  }
}

export function RepairProgress({
  planStatus,
  progress,
  actionSummary,
  applyResult,
  applyError,
  isApplying,
}: RepairProgressProps): React.JSX.Element {
  const status = describePlanStatus(planStatus);
  const disposition =
    applyResult !== null ? describeDisposition(applyResult.disposition) : null;
  const recoverable503 =
    applyResult === null && isApiRequestError(applyError) && applyError.status === 503;
  const applyFailedWithoutDisposition =
    applyResult === null && applyError !== null && !recoverable503;
  // The hook bounds its list; the render bound is a second fence so an
  // oversized prop can never blow up the DOM.
  const renderedNotifications = progress.notifications.slice(
    -MAX_RENDERED_NOTIFICATIONS,
  );

  return (
    <div
      data-testid="repair-progress"
      className="space-y-4 rounded-lg border border-border p-4"
    >
      <div className="flex flex-wrap items-center gap-3">
        <h3 className="text-sm font-semibold text-foreground">Application progress</h3>
        <StatusBadge state={toneToBadgeState(status.tone)}>{status.label}</StatusBadge>
        {isApplying && <Loading label="Applying repair" />}
      </div>

      <p className="text-xs text-muted-strong">
        Authoritative action counts (from the plan response): {actionSummary.total}{" "}
        total — {actionSummary.pending} pending, {actionSummary.applied} applied,{" "}
        {actionSummary.failed} failed.
      </p>

      <section aria-label="Durable application events" className="space-y-2">
        <h4 className="text-2xs font-semibold tracking-label text-muted-strong uppercase">
          Durable application events (notifications only — not authoritative state)
        </h4>
        {progress.notifications.length === 0 ? (
          <p className="text-xs text-muted-strong">
            No durable application events received yet.
          </p>
        ) : (
          <ol data-testid="progress-notifications" className="space-y-1">
            {renderedNotifications.map((notification) => (
              <li
                key={notification.sequence}
                className="font-mono text-2xs text-muted-strong"
              >
                sequence {String(notification.sequence)} · {notification.kind} ·{" "}
                {notification.at}
              </li>
            ))}
          </ol>
        )}
        <p className="text-2xs text-muted">
          Stream connection: {progress.connected}. Terminal state is never taken from
          these events; the plan response and the apply result are the only authorities.
        </p>
      </section>

      {progress.quarantinedEvents > 0 && (
        <p
          role="alert"
          data-testid="progress-quarantined"
          className="rounded-md border border-warning/40 bg-warning/10 p-3 text-xs text-foreground"
        >
          {String(progress.quarantinedEvents)} durable stream frame
          {progress.quarantinedEvents === 1 ? "" : "s"} could not be bound to this exact
          plan (foreign run, wrong channel, sequence anomaly, mismatched fingerprint, or
          unknown canonical key) and were ignored. Nothing from them was applied to this
          view.
        </p>
      )}

      <section aria-label="Application outcome" className="space-y-2">
        <h4 className="text-2xs font-semibold tracking-label text-muted-strong uppercase">
          Application outcome
        </h4>
        <div data-testid="progress-outcome" className="space-y-2">
          {disposition !== null && (
            <div
              className={`rounded-md border p-3 ${
                OUTCOME_TONE_CLASS[disposition.tone] ?? OUTCOME_TONE_CLASS.neutral
              }`}
            >
              <p className="text-sm font-semibold text-foreground">
                <span data-testid="outcome-disposition">{disposition.label}</span>
                {applyResult?.resumed ? " (resumed a previous attempt)" : ""}
              </p>
              {disposition.isSuccess && applyResult !== null && (
                <p className="mt-1 text-xs text-muted-strong">
                  {applyResult.disposition === "already_applied"
                    ? "The server replayed the stored outcome of a previously completed application. Apply stays disabled for this plan."
                    : "Every action of this plan reached its proposed after-state. Apply stays disabled for this plan."}
                </p>
              )}
              {applyResult !== null && applyResult.disposition === "failed" && (
                <p className="mt-1 text-xs text-muted-strong">
                  The server stored a failed application outcome for this plan. Review
                  the authoritative plan status;{" "}
                  {applyError !== null && isApiRequestError(applyError)
                    ? `diagnostic: ${bounded(applyError.problem.detail ?? applyError.problem.title)}`
                    : "no additional diagnostic text is available."}
                </p>
              )}
              {(applyResult?.disposition === "unresolved" ||
                applyResult?.disposition === "interrupted") && (
                <div data-testid="apply-retry" className="mt-2">
                  <p className="text-xs font-semibold text-foreground">
                    Recoverable failure — retry resumes the same plan.
                  </p>
                  <p className="mt-1 text-xs text-muted-strong">
                    This outcome is not a success. Use the retry control in the apply
                    section; the retry reuses the same idempotency key and the same
                    plan.
                  </p>
                </div>
              )}
            </div>
          )}

          {recoverable503 && (
            <div className="rounded-md border border-failure/40 bg-failure/10 p-3">
              <p className="text-sm font-semibold text-foreground">
                Recoverable failure (503) — the application did not complete
              </p>
              <div data-testid="apply-retry" className="mt-2">
                <p className="text-xs text-muted-strong">
                  Retry resumes the same plan. The retry reuses the same idempotency
                  key, so the server replays or resumes the stored application instead
                  of starting a new one.
                </p>
              </div>
            </div>
          )}

          {applyFailedWithoutDisposition && (
            <div className="rounded-md border border-failure/40 bg-failure/10 p-3">
              <p className="text-sm font-semibold text-foreground">
                The apply command failed — this is not a success
              </p>
              <p className="mt-1 text-xs text-muted-strong">
                {applyError !== null && isApiRequestError(applyError)
                  ? `Diagnostic: ${bounded(applyError.problem.detail ?? applyError.problem.title)}`
                  : "The command could not be completed."}
              </p>
            </div>
          )}

          {disposition === null &&
            !recoverable503 &&
            !applyFailedWithoutDisposition && (
              <p className="text-xs text-muted-strong">
                {isApplying
                  ? "Application is in progress; waiting for the authoritative outcome."
                  : "No application attempt has been recorded for this plan yet."}
              </p>
            )}
        </div>
      </section>
    </div>
  );
}

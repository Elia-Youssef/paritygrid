/**
 * Presentational reconciliation summary. Every displayed value is carried
 * by visible text (labeled definition rows), never by layout alone: run
 * identity, both input identities, the reconciliation fingerprint, all
 * seven classification counts including zeros, both observation times, and
 * the analytical query version. The component never fetches; loading,
 * error, and page-level empty states belong to the owning screen.
 */
import type { ReactNode } from "react";

import type { ReconciliationResponse } from "../../api/generated/schema";
import { StatusBadge } from "../../components/ui/status-badge";
import {
  RECONCILIATION_CLASSIFICATIONS,
  classificationLabel,
  classificationTone,
  reconciliationConflictCount,
} from "./classifications";

export interface ReconciliationSummaryProps {
  summary: ReconciliationResponse;
  /** Renders the refresh-required banner for the held snapshot. */
  stale?: boolean;
  /** The quarantined newer snapshot described by the stale banner. */
  quarantinedSummary?: ReconciliationResponse | null;
  /** Adopts the quarantined snapshot; the button renders only when one is held. */
  onRefresh?: () => void;
  /** Hides the repair-plan availability notice while repair is blocked. */
  repairActionsDisabled?: boolean;
  /** Inline "no conflicts" note for a run whose reconciliation found nothing. */
  empty?: boolean;
  /** Slot for the screen's lower content (conflict table area). */
  children?: ReactNode;
}

/** Long identifiers are sliced with an ellipsis; all values here are safe hex. */
function bounded(value: string): string {
  return value.length > 64 ? `${value.slice(0, 64)}…` : value;
}

/** Maps a classification tone onto an existing design-token text color. */
function toneTextClass(
  tone: "neutral" | "info" | "success" | "warning" | "failure",
): string {
  switch (tone) {
    case "success":
      return "text-verified";
    case "warning":
      return "text-warning";
    case "failure":
      return "text-failure";
    case "info":
      return "text-active";
    case "neutral":
      return "text-foreground";
  }
}

/** Run-state badge tone; the state text itself carries the meaning. */
function runStateBadgeTone(
  state: string,
): "active" | "verified" | "failure" | "paused" | "cancelled" | "neutral" {
  switch (state) {
    case "queued":
    case "running":
    case "pausing":
    case "resuming":
    case "cancelling":
      return "active";
    case "succeeded":
    case "partially_succeeded":
      return "verified";
    case "failed":
      return "failure";
    case "paused":
      return "paused";
    case "cancelled":
      return "cancelled";
    default:
      return "neutral";
  }
}

function IdentityRow({
  term,
  children,
}: {
  term: string;
  children: ReactNode;
}): React.JSX.Element {
  return (
    <div className="flex flex-col gap-0.5 sm:flex-row sm:items-baseline sm:gap-3">
      <dt className="min-w-56 shrink-0 text-2xs font-semibold tracking-label text-muted-strong uppercase">
        {term}
      </dt>
      <dd className="text-xs text-foreground">{children}</dd>
    </div>
  );
}

export function ReconciliationSummary({
  summary,
  stale = false,
  quarantinedSummary = null,
  onRefresh,
  repairActionsDisabled = false,
  empty = false,
  children,
}: ReconciliationSummaryProps): React.JSX.Element {
  const conflictCount = reconciliationConflictCount(summary.counts);
  return (
    <div
      data-testid="reconciliation-summary"
      className="space-y-4 rounded-lg border border-border p-4"
    >
      {stale && quarantinedSummary !== null && (
        <div
          role="alert"
          data-testid="reconciliation-stale-banner"
          className="space-y-2 rounded-md border border-stale/40 bg-stale/10 p-3"
        >
          <p className="text-sm font-semibold text-foreground">
            Refresh required: a newer reconciliation snapshot is held unmerged
          </p>
          <p className="text-xs text-muted-strong">
            A newer snapshot with reconciliation fingerprint{" "}
            <span className="font-mono">
              {bounded(quarantinedSummary.reconciliation_fingerprint)}
            </span>{" "}
            observed at {quarantinedSummary.observed_at} is incompatible with the
            rendered view, so it was not merged. Repair actions stay disabled until it
            is adopted.
          </p>
          {onRefresh !== undefined && (
            <button
              type="button"
              data-testid="reconciliation-refresh"
              className="rounded border border-border px-3 py-1.5 text-xs text-foreground"
              onClick={onRefresh}
            >
              Adopt quarantined snapshot
            </button>
          )}
        </div>
      )}

      <section aria-label="Reconciliation identity" className="space-y-2">
        <h3 className="text-sm font-semibold text-foreground">Reconciliation</h3>
        <dl className="space-y-1.5">
          <IdentityRow term="Run">
            <span className="font-mono">{bounded(summary.run_id)}</span>
          </IdentityRow>
          <IdentityRow term="Run version">
            <span className="font-mono">{String(summary.run_version)}</span>
          </IdentityRow>
          <IdentityRow term="Run state">
            <StatusBadge state={runStateBadgeTone(summary.state)}>
              {summary.state}
            </StatusBadge>
          </IdentityRow>
          <IdentityRow term="Reconciliation fingerprint">
            <span className="font-mono">
              {bounded(summary.reconciliation_fingerprint)}
            </span>
          </IdentityRow>
          <IdentityRow term="Source input identity">
            <span className="font-mono">{bounded(summary.source_input_identity)}</span>
          </IdentityRow>
          <IdentityRow term="Target input identity">
            <span className="font-mono">{bounded(summary.target_input_identity)}</span>
          </IdentityRow>
          <IdentityRow term="Total records">{String(summary.total_count)}</IdentityRow>
          <IdentityRow term="Conflict records">{String(conflictCount)}</IdentityRow>
          <IdentityRow term="Analytical query version">
            <span className="font-mono">
              {String(summary.analytical_query_version)}
            </span>
          </IdentityRow>
          <IdentityRow term="Authoritative observation">
            {summary.reconciliation_observed_at}
          </IdentityRow>
          <IdentityRow term="Response observed at">{summary.observed_at}</IdentityRow>
        </dl>
      </section>

      <section aria-label="Classification counts" className="space-y-2">
        <h3 className="text-sm font-semibold text-foreground">Classification counts</h3>
        <dl className="space-y-1.5">
          {RECONCILIATION_CLASSIFICATIONS.map((classification) => {
            const count = summary.counts[classification] ?? 0;
            return (
              <IdentityRow
                key={classification}
                term={classificationLabel(classification)}
              >
                <span
                  data-testid={`count-${classification}`}
                  className={`font-mono ${toneTextClass(classificationTone(classification))}`}
                >
                  {String(count)}
                </span>
              </IdentityRow>
            );
          })}
        </dl>
      </section>

      {empty && (
        <p data-testid="reconciliation-empty" className="text-xs text-muted-strong">
          No conflicts detected for this reconciliation.
        </p>
      )}

      {!repairActionsDisabled && (
        <p data-testid="repair-creation-notice" className="text-xs text-muted-strong">
          Repair-plan creation is not available from this screen: creating a plan
          requires the exact complete source and target observations, which the
          reconciliation and conflict projections do not carry. Review, approval, and
          application of existing repair plans remain supported.
        </p>
      )}

      {children}
    </div>
  );
}

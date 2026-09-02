import { useQuery, useQueryClient } from "@tanstack/react-query";
import { useParams } from "react-router";
import { useMemo, useState } from "react";

import { fetchRun, isApiRequestError } from "../../api/client";
import { queryKeys } from "../../api/query-keys";
import { ErrorState } from "../../components/states/error-state";
import { Loading } from "../../components/states/loading";
import { StaleNotice } from "../../components/states/stale-notice";
import { UnavailableNotice } from "../../components/states/unavailable-notice";
import {
  reconciliationConflictCount,
  type ReconciliationClassificationValue,
} from "../../features/reconcile/classifications";
import { ConflictInspector } from "../../features/reconcile/conflict-inspector";
import { ConflictTable } from "../../features/reconcile/conflict-table";
import { conflictKey } from "../../features/reconcile/conflict-data";
import { checkIdentityCoherence } from "../../features/reconcile/coherence";
import { ReconciliationSummary } from "../../features/reconcile/reconciliation-summary";
import { useConflicts } from "../../features/reconcile/use-conflicts";
import { useReconciliation } from "../../features/reconcile/use-reconciliation";

/**
 * The reconciliation workbench for one run (P18.1–P18.3): a coherent
 * summary snapshot, a bounded paged conflict table, and the selected
 * conflict's field-level evidence.
 *
 * Identity rules enforced by this composition:
 * - The rendered summary must name the route's run and, once the durable
 *   run detail is loaded, must agree with it on run version and
 *   reconciliation fingerprint. Any disagreement fences the conflict data
 *   (identity becomes null, so pages and selection reset), renders a
 *   refresh-required state, and disables repair entry points.
 * - A quarantined newer reconciliation snapshot is never merged; the
 *   operator adopts it explicitly.
 */
export function ReconciliationScreen() {
  const { runId = "" } = useParams();
  const queryClient = useQueryClient();
  const [classificationFilter, setClassificationFilter] =
    useState<ReconciliationClassificationValue | null>(null);

  const runQuery = useQuery({
    queryKey: queryKeys.runDetail(runId),
    queryFn: ({ signal }) => fetchRun(runId, { signal }),
    enabled: runId !== "",
    staleTime: 0,
    retry: 0,
  });

  const reconciliation = useReconciliation(runId);
  const summary = reconciliation.summary;
  const totalConflictCount =
    summary === null ? 0 : reconciliationConflictCount(summary.counts);

  // The authoritative identity the conflict table may bind to: the summary
  // must belong to this route's run, and once the run detail is loaded both
  // sources must agree. Anything else fences the conflict data entirely.
  const run = runQuery.data;
  const identity = useMemo(() => {
    if (summary === null) {
      return null;
    }
    if (summary.run_id !== runId) {
      return null;
    }
    if (run?.run_id === runId) {
      const verdict = checkIdentityCoherence(
        {
          run_id: run.run_id,
          run_version: run.run_version,
          reconciliation_fingerprint: summary.reconciliation_fingerprint,
        },
        {
          run_id: summary.run_id,
          run_version: summary.run_version,
          reconciliation_fingerprint: summary.reconciliation_fingerprint,
        },
      );
      if (!verdict.ok) {
        return null;
      }
    }
    return {
      run_id: summary.run_id,
      run_version: summary.run_version,
      reconciliation_fingerprint: summary.reconciliation_fingerprint,
    };
  }, [runId, run, summary]);

  const runAdvanced =
    summary !== null &&
    run?.run_id === runId &&
    run?.run_version !== summary.run_version;

  const conflicts = useConflicts(runId, identity);
  const selectedConflict =
    conflicts.state.selection === null
      ? null
      : (conflicts.state.conflicts.find(
          (conflict) => conflict.conflict_id === conflicts.state.selection?.conflictId,
        ) ?? null);

  const refreshAll = (): void => {
    void queryClient.invalidateQueries({ queryKey: queryKeys.runsRoot() });
    reconciliation.refetch();
    void runQuery.refetch();
  };

  if (runId === "") {
    return (
      <ErrorState
        title="Run unavailable"
        description="No run identity was provided, so no reconciliation can be shown."
      />
    );
  }

  return (
    <div className="flex min-h-0 flex-1 flex-col">
      <div className="border-b border-border px-4 py-2">
        <h1
          data-page-title
          tabIndex={-1}
          className="text-sm font-semibold text-foreground"
        >
          Reconciliation workbench
        </h1>
      </div>
      <div className="min-h-0 flex-1 overflow-y-auto p-4">
        <div className="space-y-4" data-testid="reconciliation-screen">
          {reconciliation.isPending && <Loading label="Loading reconciliation" />}
          {reconciliation.error !== null && (
            <ErrorState
              title="Reconciliation unavailable"
              description="The reconciliation snapshot for this run could not be loaded."
              problem={
                isApiRequestError(reconciliation.error)
                  ? reconciliation.error.problem
                  : undefined
              }
              action={
                <button
                  type="button"
                  data-testid="reconciliation-retry"
                  className="rounded border border-border px-3 py-1.5 text-xs text-foreground"
                  onClick={reconciliation.refetch}
                >
                  Retry
                </button>
              }
            />
          )}
          {runAdvanced && (
            <div className="space-y-2">
              <StaleNotice
                label="Run state advanced past the rendered snapshot"
                detail={`The durable run is at version ${String(runQuery.data?.run_version ?? 0)} while the rendered reconciliation snapshot was computed under version ${String(summary?.run_version ?? 0)}. Conflict data is fenced and repair actions stay disabled until both sources agree again.`}
              />
              <button
                type="button"
                data-testid="reconciliation-refresh-state"
                className="rounded border border-border px-3 py-1.5 text-xs text-foreground"
                onClick={refreshAll}
              >
                Refresh state
              </button>
            </div>
          )}
          {summary !== null && summary.run_id !== runId && (
            <UnavailableNotice detail="The reconciliation response names a different run than this address, so nothing from it is rendered." />
          )}
          {summary !== null && summary.run_id === runId && (
            <ReconciliationSummary
              summary={summary}
              stale={reconciliation.incompatible}
              quarantinedSummary={reconciliation.quarantined}
              onRefresh={
                reconciliation.incompatible
                  ? reconciliation.adoptQuarantined
                  : undefined
              }
              repairActionsDisabled={reconciliation.incompatible || runAdvanced}
              empty={totalConflictCount === 0}
            >
              <section
                aria-label="Conflicts"
                className="grid gap-4 border-t border-border pt-4 xl:grid-cols-5"
              >
                <div className="xl:col-span-3">
                  <ConflictTable
                    state={conflicts.state}
                    totalLogicalCount={totalConflictCount}
                    classificationFilter={classificationFilter}
                    onClassificationFilterChange={setClassificationFilter}
                    onLoadMore={conflicts.loadMore}
                    isFetchingPage={conflicts.isFetchingPage}
                    onSelect={(conflict) => conflicts.select(conflict.conflict_id)}
                    selectedConflictId={conflicts.state.selection?.conflictId ?? null}
                    empty={totalConflictCount === 0}
                  />
                  {conflicts.state.rejectedPages > 0 && (
                    <button
                      type="button"
                      data-testid="conflict-recovery"
                      className="mt-3 rounded border border-border px-3 py-1.5 text-xs text-foreground"
                      onClick={() => {
                        conflicts.reset();
                        refreshAll();
                      }}
                    >
                      Discard stale pages and reload
                    </button>
                  )}
                  {conflicts.pageError !== null && (
                    <div className="mt-3">
                      <ErrorState
                        title="Conflict page failed"
                        description="The next conflict page could not be loaded; the rows already loaded remain usable."
                        problem={
                          isApiRequestError(conflicts.pageError)
                            ? conflicts.pageError.problem
                            : undefined
                        }
                      />
                    </div>
                  )}
                </div>
                <div className="xl:col-span-2">
                  <h2 className="mb-2 text-sm font-semibold text-foreground">
                    Conflict inspector
                  </h2>
                  <ConflictInspector
                    conflict={selectedConflict}
                    fingerprint={
                      conflicts.state.identity?.reconciliation_fingerprint ?? null
                    }
                    selectedKey={
                      selectedConflict !== null && conflicts.state.identity !== null
                        ? conflictKey(
                            selectedConflict,
                            conflicts.state.identity.reconciliation_fingerprint,
                          )
                        : null
                    }
                  />
                </div>
              </section>
            </ReconciliationSummary>
          )}
        </div>
      </div>
    </div>
  );
}

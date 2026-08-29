/**
 * Stable, parameter-complete query-key factories. Every parameter that can
 * change a result appears in the key so distinct results can never collide
 * in one cache entry. Keys are hierarchical: invalidating the collection
 * root invalidates every run-scoped entry beneath it.
 */
import type { PageParams } from "./client";

export const queryKeys = {
  health: () => ["health"] as const,

  capabilities: () => ["system", "capabilities"] as const,

  /** Root for all run-scoped data; invalidate to refresh everything run-related. */
  runsRoot: () => ["runs"] as const,

  /** Run collection page; limit and cursor both change the result. */
  runList: (params: PageParams) =>
    [
      "runs",
      "list",
      { cursor: params.cursor ?? null, limit: params.limit ?? 50 },
    ] as const,

  /** Detail for one run. */
  runDetail: (runId: string) => ["runs", runId, "detail"] as const,

  /** Live channel state for one run (durable stream + telemetry). */
  runLive: (runId: string) => ["runs", runId, "live"] as const,

  /** Reconciliation summary for one run. */
  reconciliation: (runId: string) => ["runs", runId, "reconciliation"] as const,

  /**
   * One persisted conflict page. The page content depends on the run
   * identity and its reconciliation fingerprint; callers that fetch across
   * fingerprint changes must pass them so incompatible pages never share
   * (and can never merge into) one cache entry.
   */
  conflicts: (
    runId: string,
    params: PageParams & { reconciliationFingerprint?: string; runVersion?: number },
  ) =>
    [
      "runs",
      runId,
      "conflicts",
      {
        cursor: params.cursor ?? null,
        limit: params.limit ?? 50,
        reconciliationFingerprint: params.reconciliationFingerprint ?? null,
        runVersion: params.runVersion ?? null,
      },
    ] as const,

  /** Repair plan detail. */
  repairPlan: (planId: string) => ["repair-plans", planId] as const,
};

/**
 * Conflict pages may be combined only when the run identity and the
 * reconciliation fingerprint they were persisted under match exactly.
 * Pagination never merges data across fingerprints or run versions.
 */
export function canMergeConflictPages(
  base: { run_id: string; run_version: number; reconciliation_fingerprint: string },
  page: { run_id: string; run_version: number; reconciliation_fingerprint: string },
): boolean {
  return (
    base.run_id === page.run_id &&
    base.run_version === page.run_version &&
    base.reconciliation_fingerprint === page.reconciliation_fingerprint
  );
}

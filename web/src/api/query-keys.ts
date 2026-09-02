/**
 * Stable, parameter-complete query-key factories. Every parameter that can
 * change a result appears in the key so distinct results can never collide
 * in one cache entry. Keys are hierarchical: invalidating the collection
 * root invalidates every run-scoped entry beneath it.
 */
import type { PageParams, RunPageParams } from "./client";

export const queryKeys = {
  health: () => ["health"] as const,

  readiness: () => ["system", "readiness"] as const,

  capabilities: () => ["system", "capabilities"] as const,

  /** Connector collection page used for inspector bindings. */
  connectors: (params: PageParams & { includeArchived?: boolean }) =>
    [
      "connectors",
      "list",
      {
        cursor: params.cursor ?? null,
        includeArchived: params.includeArchived ?? false,
        limit: params.limit ?? 100,
      },
    ] as const,

  /** Root for all pipeline-scoped data. */
  pipelinesRoot: () => ["pipelines"] as const,

  /**
   * Pipeline library page from the REST collection. Library search text is
   * URL state filtered deterministically on top of fetched pages, so the
   * REST result key carries exactly the parameters the server accepts.
   */
  pipelineList: (params: PageParams & { includeArchived?: boolean }) =>
    [
      "pipelines",
      "list",
      {
        cursor: params.cursor ?? null,
        includeArchived: params.includeArchived ?? false,
        limit: params.limit ?? 50,
      },
    ] as const,

  /** One pipeline identity with its mutable display metadata. */
  pipelineDetail: (pipelineId: string) => ["pipelines", pipelineId, "detail"] as const,

  /** The immutable latest-version frontier (0 = never published). */
  pipelineFrontier: (pipelineId: string) =>
    ["pipelines", pipelineId, "version-frontier"] as const,

  /** One immutable published pipeline version. */
  pipelineVersion: (pipelineId: string, version: number) =>
    ["pipelines", pipelineId, "versions", { version }] as const,

  /** Root for all run-scoped data; invalidate to refresh everything run-related. */
  runsRoot: () => ["runs"] as const,

  /** Run collection page; limit, cursor, and state filter all change the result. */
  runList: (params: RunPageParams) =>
    [
      "runs",
      "list",
      {
        cursor: params.cursor ?? null,
        limit: params.limit ?? 50,
        state: params.state ?? null,
      },
    ] as const,

  /** Detail for one run. */
  runDetail: (runId: string) => ["runs", runId, "detail"] as const,

  /**
   * One comparison over an exact, ordered set of runs. The run ids are part
   * of the key so two different selections never share one cache entry.
   */
  runCompare: (runIds: readonly string[]) =>
    ["runs", "compare", { runIds: [...runIds] }] as const,

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

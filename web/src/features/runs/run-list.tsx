/**
 * Run list (P17.1): deterministic cursor pagination and filters fully
 * backed by the URL, so back, forward, reload, and direct entry restore the
 * exact view and selection. Only durable REST facts are shown; a page with
 * a successor cursor is presented as one bounded page, never as the global
 * total, and unavailable states are never rendered as zero.
 */
import { useQuery } from "@tanstack/react-query";
import { Link, useSearchParams } from "react-router";

import { fetchRunPage, type RunPageParams } from "../../api/client";
import { queryKeys } from "../../api/query-keys";
import { EmptyState } from "../../components/states/empty-state";
import { ErrorState } from "../../components/states/error-state";
import { Loading } from "../../components/states/loading";
import { StatusBadge } from "../../components/ui/status-badge";
import { runStateTone, RUN_STATES } from "./run-derivations";
import { PAGE_LIMITS, parseRunListParams, RUNS_FILTER_TESTID } from "./run-list-state";

export function RunList() {
  const [search, setSearch] = useSearchParams();
  const url = parseRunListParams(search);

  const update = (mutate: (next: URLSearchParams) => void) => {
    const next = new URLSearchParams(search);
    mutate(next);
    setSearch(next);
  };

  const params: RunPageParams = {
    limit: url.limit,
    cursor: url.cursor ?? undefined,
    state: url.state ?? undefined,
  };
  const runs = useQuery({
    queryKey: queryKeys.runList(params),
    queryFn: ({ signal }) => fetchRunPage(params, { signal }),
    staleTime: 15_000,
    retry: 0,
  });

  return (
    <div className="space-y-4">
      <div className="flex flex-wrap items-end gap-3">
        <div>
          <label
            htmlFor="runs-state"
            className="block text-2xs uppercase tracking-wide text-muted"
          >
            State filter
          </label>
          <select
            id="runs-state"
            data-testid={RUNS_FILTER_TESTID}
            className="mt-1 rounded border border-border bg-surface px-2 py-1 text-xs text-foreground"
            value={url.state ?? ""}
            onChange={(event) => {
              const value = event.target.value;
              update((next) => {
                if (value === "") {
                  next.delete("state");
                } else {
                  next.set("state", value);
                }
                // A filter change invalidates any cursor: pages belong to
                // one filtered view only.
                next.delete("cursor");
              });
            }}
          >
            <option value="">All states</option>
            {RUN_STATES.map((state) => (
              <option key={state} value={state}>
                {state}
              </option>
            ))}
          </select>
        </div>
        <div>
          <label
            htmlFor="runs-limit"
            className="block text-2xs uppercase tracking-wide text-muted"
          >
            Page size
          </label>
          <select
            id="runs-limit"
            data-testid="runs-limit"
            className="mt-1 rounded border border-border bg-surface px-2 py-1 text-xs text-foreground"
            value={String(url.limit)}
            onChange={(event) => {
              const value = event.target.value;
              update((next) => {
                next.set("limit", value);
                next.delete("cursor");
              });
            }}
          >
            {PAGE_LIMITS.map((limit) => (
              <option key={limit} value={String(limit)}>
                {String(limit)} per page
              </option>
            ))}
          </select>
        </div>
        {url.state !== null && (
          <p className="text-2xs text-muted" role="status">
            Filtered to durable state <span className="font-mono">{url.state}</span>.
          </p>
        )}
      </div>

      <section aria-label="Runs" className="rounded-lg border border-border">
        <div className="flex items-center justify-between gap-2 border-b border-border px-4 py-2">
          <h2 className="text-sm font-semibold text-foreground">Runs</h2>
          <p className="text-2xs text-muted">
            Source: <span className="font-mono">GET /api/v1/runs</span> (limit{" "}
            {String(url.limit)}
            {url.state === null ? "" : `, state=${url.state}`}). One bounded durable
            page; a cursor below means more pages exist.
          </p>
        </div>
        {runs.isPending ? (
          <div className="p-4">
            <Loading label="Loading runs" />
          </div>
        ) : runs.isError ? (
          <div className="p-4">
            <ErrorState
              title="Runs unavailable"
              description="The durable run page could not be loaded. No partial page is substituted."
              action={
                <button
                  type="button"
                  className="rounded border border-border px-3 py-1.5 text-xs text-foreground"
                  onClick={() => {
                    void runs.refetch();
                  }}
                >
                  Retry
                </button>
              }
            />
          </div>
        ) : runs.data.items.length === 0 ? (
          <div className="p-4">
            {url.cursor !== null ? (
              <EmptyState
                title="No runs on this page"
                description="The cursor resolved to an empty page; the run may have rotated out of retention or the cursor is stale."
                action={
                  <Link
                    className="rounded border border-border px-3 py-1.5 text-xs text-foreground"
                    to="/app/runs"
                  >
                    Back to the first page
                  </Link>
                }
              />
            ) : (
              <EmptyState
                title={
                  url.state === null ? "No runs recorded yet" : `No ${url.state} runs`
                }
                description="Durable runs appear here as execution workflows record them."
              />
            )}
          </div>
        ) : (
          <>
            {runs.isFetching && (
              <p
                className="border-b border-border px-4 py-1 text-2xs text-warning"
                role="status"
                data-testid="runs-refreshing"
              >
                Refreshing; showing the last coherent durable page.
              </p>
            )}
            <div className="overflow-x-auto">
              <table className="w-full text-left text-xs" aria-label="Durable runs">
                <thead>
                  <tr className="border-b border-border text-muted">
                    <th scope="col" className="px-4 py-1.5 font-medium">
                      Run
                    </th>
                    <th scope="col" className="px-4 py-1.5 font-medium">
                      State
                    </th>
                    <th scope="col" className="px-4 py-1.5 font-medium">
                      Pipeline
                    </th>
                    <th scope="col" className="px-4 py-1.5 font-medium">
                      Runner
                    </th>
                    <th scope="col" className="px-4 py-1.5 font-medium">
                      Created
                    </th>
                    <th scope="col" className="px-4 py-1.5 font-medium">
                      Finished
                    </th>
                    <th scope="col" className="px-4 py-1.5 font-medium">
                      Actions
                    </th>
                  </tr>
                </thead>
                <tbody data-testid="runs-tbody">
                  {runs.data.items.map((run) => {
                    const selected = run.run_id === url.selected;
                    return (
                      <tr
                        key={run.run_id}
                        data-testid={`run-row-${run.run_id}`}
                        aria-selected={selected}
                        className={`border-b border-border/60 last:border-b-0 ${
                          selected ? "bg-active/10" : ""
                        }`}
                      >
                        <td className="px-4 py-1.5 font-mono text-foreground">
                          {run.run_id}
                        </td>
                        <td className="px-4 py-1.5">
                          <StatusBadge state={runStateTone(run.state)}>
                            {run.state}
                          </StatusBadge>
                        </td>
                        <td className="px-4 py-1.5 font-mono text-muted">
                          {run.pipeline_id} v{String(run.pipeline_version)}
                        </td>
                        <td className="px-4 py-1.5 text-muted">{run.runner_kind}</td>
                        <td className="px-4 py-1.5 font-mono text-muted">
                          {run.created_at}
                        </td>
                        <td className="px-4 py-1.5 font-mono text-muted">
                          {run.finished_at ?? "unavailable"}
                        </td>
                        <td className="px-4 py-1.5 text-right">
                          <div className="flex justify-end gap-1">
                            <button
                              type="button"
                              data-testid={`run-select-${run.run_id}`}
                              aria-pressed={selected}
                              className="rounded border border-border px-2 py-1 text-2xs text-foreground"
                              onClick={() => {
                                update((next) => {
                                  if (selected) {
                                    next.delete("selected");
                                  } else {
                                    next.set("selected", run.run_id);
                                  }
                                });
                              }}
                            >
                              {selected ? "Deselect" : "Select"}
                            </button>
                            <Link
                              className="rounded border border-border px-2 py-1 text-2xs text-foreground underline"
                              to={`/app/runs/${encodeURIComponent(run.run_id)}`}
                              aria-label={`Open run ${run.run_id}`}
                            >
                              Open
                            </Link>
                          </div>
                        </td>
                      </tr>
                    );
                  })}
                </tbody>
              </table>
            </div>
            <div className="flex items-center justify-between gap-2 border-t border-border px-4 py-2">
              <p className="text-2xs text-muted" data-testid="runs-page-status">
                {url.cursor === null ? "First page" : "Subsequent page"}
                {runs.data.next_cursor === null
                  ? "; no further durable page."
                  : "; a further durable page exists."}
              </p>
              <div className="flex gap-2">
                {url.cursor !== null && (
                  <button
                    type="button"
                    data-testid="runs-prev"
                    className="rounded border border-border px-3 py-1.5 text-xs text-foreground"
                    onClick={() => {
                      // Opaque cursors are not reversible. Clearing it is a
                      // deterministic recovery for direct-entry URLs too;
                      // browser history may not contain a prior run page.
                      update((next) => {
                        next.delete("cursor");
                      });
                    }}
                  >
                    Back to first page
                  </button>
                )}
                {runs.data.next_cursor !== null && (
                  <button
                    type="button"
                    data-testid="runs-next"
                    className="rounded border border-border px-3 py-1.5 text-xs text-foreground"
                    onClick={() => {
                      update((next) => {
                        next.set("cursor", runs.data.next_cursor ?? "");
                      });
                    }}
                  >
                    Next page
                  </button>
                )}
              </div>
            </div>
          </>
        )}
      </section>
    </div>
  );
}

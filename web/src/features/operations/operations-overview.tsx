/**
 * Operations overview: durable REST facts only. No SSE or WebSocket
 * connections exist on this screen — live observability arrives with
 * Phase 17. Every metric names its source and definition, unavailable
 * facts render as "unavailable" (never zero or healthy), and the last
 * coherent data is retained while a refetch is stale.
 */
import { useQuery } from "@tanstack/react-query";
import { Link } from "react-router";

import {
  fetchCapabilitiesDetail,
  fetchReadiness,
  fetchRunPage,
} from "../../api/client";
import { queryKeys } from "../../api/query-keys";
import { EmptyState } from "../../components/states/empty-state";
import { ErrorState } from "../../components/states/error-state";
import { Loading } from "../../components/states/loading";
import { StatusBadge } from "../../components/ui/status-badge";

export function OperationsOverview() {
  const runs = useQuery({
    queryKey: queryKeys.runList({ limit: 10 }),
    queryFn: ({ signal }) => fetchRunPage({ limit: 10 }, { signal }),
    staleTime: 15_000,
  });
  const capabilities = useQuery({
    queryKey: queryKeys.capabilities(),
    queryFn: ({ signal }) => fetchCapabilitiesDetail({ signal }),
    staleTime: 60_000,
  });
  const readiness = useQuery({
    queryKey: queryKeys.readiness(),
    queryFn: ({ signal }) => fetchReadiness({ signal }),
    staleTime: 15_000,
  });

  return (
    <div className="space-y-6 p-6">
      <div>
        <h1
          data-page-title
          tabIndex={-1}
          className="text-lg font-semibold text-foreground"
        >
          Operations overview
        </h1>
        <p className="mt-1 max-w-2xl text-sm text-muted">
          Durable facts from the accepted REST boundary only. Live progress, telemetry,
          and event replay arrive with the execution work and are deliberately absent
          here.
        </p>
      </div>

      <section aria-label="Run history" className="rounded-lg border border-border">
        <div className="flex items-center justify-between gap-2 border-b border-border px-4 py-2">
          <h2 className="text-sm font-semibold text-foreground">Recent runs</h2>
          <p className="text-2xs text-muted">
            Source: latest <span className="font-mono">GET /api/v1/runs</span> page
            (limit 10). This is the most recent durable page, not a global total.
          </p>
        </div>
        {runs.isPending ? (
          <div className="p-4">
            <Loading label="Loading recent runs" />
          </div>
        ) : runs.isError ? (
          <div className="p-4">
            <ErrorState
              title="Recent runs unavailable"
              description="The durable run page could not be loaded. Earlier data is never substituted."
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
            <EmptyState
              title="No runs recorded yet"
              description="Durable runs appear here after the execution workflows begin."
            />
          </div>
        ) : (
          <>
            {runs.isFetching && runs.data.items.length > 0 && (
              <p
                className="border-b border-border px-4 py-1 text-2xs text-warning"
                role="status"
              >
                Refreshing; showing the last coherent durable page.
              </p>
            )}
            <table
              className="w-full text-left text-xs"
              aria-label="Recent durable runs"
            >
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
                    Observed
                  </th>
                </tr>
              </thead>
              <tbody>
                {runs.data.items.map((run) => (
                  <tr
                    key={run.run_id}
                    className="border-b border-border/60 last:border-b-0"
                  >
                    <td className="px-4 py-1.5 font-mono text-foreground">
                      {run.run_id}
                    </td>
                    <td className="px-4 py-1.5">
                      <StatusBadge
                        state={
                          run.state === "succeeded" ||
                          run.state === "partially_succeeded"
                            ? "verified"
                            : run.state === "failed"
                              ? "failure"
                              : run.state === "paused"
                                ? "paused"
                                : run.state === "cancelled"
                                  ? "cancelled"
                                  : "active"
                        }
                      >
                        {run.state}
                      </StatusBadge>
                    </td>
                    <td className="px-4 py-1.5 font-mono text-muted">
                      {run.pipeline_id} v{String(run.pipeline_version)}
                    </td>
                    <td className="px-4 py-1.5 text-muted">{run.runner_kind}</td>
                    <td className="px-4 py-1.5 font-mono text-muted">
                      {run.observed_at}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </>
        )}
      </section>

      <div className="grid gap-4 lg:grid-cols-2">
        <section
          aria-label="Runner availability"
          className="rounded-lg border border-border p-4"
        >
          <h2 className="text-sm font-semibold text-foreground">Runner availability</h2>
          <p className="mt-1 text-2xs text-muted">
            Source: <span className="font-mono">GET /api/v1/system/capabilities</span>.
            A runner is available when its strategy reports availability.
          </p>
          {capabilities.isPending ? (
            <div className="mt-3">
              <Loading label="Loading capabilities" />
            </div>
          ) : capabilities.isError ? (
            <p className="mt-3 text-xs text-warning" role="status">
              Runner availability is unavailable right now.
            </p>
          ) : (
            <ul className="mt-3 space-y-1.5">
              {capabilities.data.runners.map((runner) => (
                <li
                  key={runner.strategy_id}
                  className="flex items-center gap-2 text-xs"
                >
                  <StatusBadge state={runner.available ? "verified" : "neutral"}>
                    {runner.available ? "available" : "unavailable"}
                  </StatusBadge>
                  <span className="font-mono text-foreground">
                    {runner.strategy_id}
                  </span>
                  {runner.unavailability_reason !== null &&
                    runner.unavailability_reason !== undefined && (
                      <span className="text-muted">{runner.unavailability_reason}</span>
                    )}
                </li>
              ))}
            </ul>
          )}
        </section>

        <section
          aria-label="Storage readiness"
          className="rounded-lg border border-border p-4"
        >
          <h2 className="text-sm font-semibold text-foreground">Storage readiness</h2>
          <p className="mt-1 text-2xs text-muted">
            Source: <span className="font-mono">GET /readyz</span>. Verifies migrations
            and required storage; liveness is reported separately by the shell.
          </p>
          {readiness.isPending ? (
            <div className="mt-3">
              <Loading label="Loading readiness" />
            </div>
          ) : readiness.isError ? (
            <p className="mt-3 text-xs text-warning" role="status">
              Readiness is unavailable right now.
            </p>
          ) : (
            <div className="mt-3 flex items-center gap-2 text-xs">
              <StatusBadge
                state={readiness.data.status === "ready" ? "verified" : "warning"}
              >
                {readiness.data.status}
              </StatusBadge>
              <span className="text-muted">{readiness.data.detail}</span>
            </div>
          )}
        </section>
      </div>

      <section
        aria-label="Next workflows"
        className="rounded-lg border border-border p-4"
      >
        <h2 className="text-sm font-semibold text-foreground">Workflows</h2>
        <ul className="mt-2 space-y-1 text-xs">
          <li>
            <Link
              className="text-active underline hover:text-foreground"
              to="/app/pipelines"
            >
              Pipeline library
            </Link>
            <span className="text-muted">
              {" "}
              — author and publish immutable pipeline versions.
            </span>
          </li>
          <li>
            <span className="font-medium text-muted">Live execution</span>
            <span className="text-muted">
              {" "}
              — arrives with the execution observability phase.
            </span>
          </li>
          <li>
            <span className="font-medium text-muted">Reconciliation workbench</span>
            <span className="text-muted">
              {" "}
              — arrives with the reconciliation phase.
            </span>
          </li>
        </ul>
      </section>
    </div>
  );
}

/**
 * Runner comparison dashboard (P17.6). Selection is URL-backed and bounded;
 * comparison happens only after explicit compatibility gates pass, is
 * ordered correctness-first, and renders unsupported metrics as explicitly
 * unavailable with their reason. Execution-evidence agreement is never
 * presented as reconciliation, target-state, or repair equivalence.
 */
import { useQuery } from "@tanstack/react-query";
import { useSearchParams } from "react-router";
import { useMemo } from "react";

import type { RunResponse } from "../../api/generated/schema";
import { fetchCapabilitiesDetail, fetchRun, fetchRunPage } from "../../api/client";
import { queryKeys } from "../../api/query-keys";
import { EmptyState } from "../../components/states/empty-state";
import { ErrorState } from "../../components/states/error-state";
import { Loading } from "../../components/states/loading";
import { StatusBadge } from "../../components/ui/status-badge";
import { runStateTone } from "../runs/run-derivations";
import {
  MAX_COMPARED,
  UNAVAILABLE_COMPARISON_ROWS,
  compareCompatibility,
  durationRow,
  fingerprintAgreement,
  parseCompareRunIds,
} from "./compare-derivations";

function formatSecondsValue(seconds: number | null): string {
  return seconds === null ? "unavailable" : `${seconds.toFixed(3)} s`;
}

export function ComparisonDashboard({ runIds }: { runIds: readonly string[] }) {
  const queries = useQuery({
    queryKey: queryKeys.runCompare([...runIds]),
    queryFn: async ({ signal }) => {
      const results = await Promise.allSettled(
        runIds.map((runId) => fetchRun(runId, { signal })),
      );
      return results.map((result) => ({
        run: result.status === "fulfilled" ? result.value : null,
      }));
    },
    enabled: runIds.length >= 2,
    retry: 0,
    staleTime: 15_000,
  });

  const capabilities = useQuery({
    queryKey: queryKeys.capabilities(),
    queryFn: ({ signal }) => fetchCapabilitiesDetail({ signal }),
    staleTime: 60_000,
    retry: 0,
  });

  if (queries.isPending) {
    return <Loading label="Loading runs for comparison" />;
  }
  if (queries.isError) {
    return (
      <ErrorState
        title="Comparison unavailable"
        description="The runs could not be loaded for comparison."
        action={
          <button
            type="button"
            className="rounded border border-border px-3 py-1.5 text-xs text-foreground"
            onClick={() => {
              void queries.refetch();
            }}
          >
            Retry
          </button>
        }
      />
    );
  }

  const loaded = queries.data;
  const failures: string[] = [];
  const runs: RunResponse[] = [];
  for (const [index, entry] of loaded.entries()) {
    if (entry.run === null) {
      failures.push(
        `Run ${runIds[index] ?? "unknown"} could not be loaded; it may not exist or the API refused the request.`,
      );
    } else {
      runs.push(entry.run);
    }
  }
  if (failures.length > 0 || runs.length < 2) {
    return (
      <div className="space-y-3">
        <ErrorState
          title="Comparison blocked"
          description="Every selected run must load before a comparison can be made."
        />
        {failures.map((failure) => (
          <p
            key={failure}
            role="status"
            className="rounded border border-warning/40 bg-warning/10 px-3 py-2 text-xs text-warning"
          >
            {failure}
          </p>
        ))}
      </div>
    );
  }

  const compatibility = compareCompatibility(runs);
  const agreement = fingerprintAgreement(runs);
  const duration = durationRow(runs);

  return (
    <div className="space-y-4" data-testid="compare-dashboard">
      {failures.length > 0 && (
        <div>
          {failures.map((failure) => (
            <p
              key={failure}
              role="status"
              className="mb-2 rounded border border-warning/40 bg-warning/10 px-3 py-2 text-xs text-warning"
            >
              {failure}
            </p>
          ))}
        </div>
      )}

      {!compatibility.comparable && (
        <section
          aria-label="Comparison blocked"
          className="rounded-lg border border-failure/40 bg-failure/10 p-4"
          data-testid="compare-blocked"
        >
          <h2 className="text-sm font-semibold text-failure">
            Comparison blocked: incompatible runs
          </h2>
          <p className="mt-1 text-xs text-foreground">
            These runs may not be compared. Every blocking reason is listed; nothing is
            merged across incompatible identities or formats.
          </p>
          <ul className="mt-2 space-y-1 text-xs text-foreground">
            {compatibility.failures.map((failure) => (
              <li key={failure.code}>
                <span className="font-mono text-2xs text-failure">{failure.code}</span>{" "}
                — {failure.explanation}
              </li>
            ))}
          </ul>
        </section>
      )}

      <section aria-label="Compared runs" className="rounded-lg border border-border">
        <div className="border-b border-border px-4 py-2">
          <h2 className="text-sm font-semibold text-foreground">Compared runs</h2>
          <p className="text-2xs text-muted">
            Source: <span className="font-mono">{"GET /api/v1/runs/{run_id}"}</span> per
            run. Different runner strategies are expected — that is the subject of the
            comparison.
          </p>
        </div>
        <div className="overflow-x-auto">
          <table className="w-full text-left text-xs" aria-label="Run identities">
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
                  Seed
                </th>
                <th scope="col" className="px-4 py-1.5 font-medium">
                  Evidence format
                </th>
              </tr>
            </thead>
            <tbody>
              {runs.map((run) => (
                <tr
                  key={run.run_id}
                  className="border-b border-border/60 last:border-b-0"
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
                    {run.scenario_seed === null
                      ? "unavailable"
                      : String(run.scenario_seed)}
                  </td>
                  <td className="px-4 py-1.5 font-mono text-muted">
                    {run.execution_evidence_fingerprint_version === null
                      ? "unavailable"
                      : `v${String(run.execution_evidence_fingerprint_version)}`}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </section>

      {capabilities.isError && (
        <p className="text-xs text-warning" role="status">
          Runner capability context is unavailable right now; comparison results below
          are unaffected but carry no capability guarantee.
        </p>
      )}

      {compatibility.comparable && (
        <>
          <section
            aria-label="Correctness comparison"
            className="rounded-lg border border-border"
          >
            <div className="border-b border-border px-4 py-2">
              <h2 className="text-sm font-semibold text-foreground">
                Correctness first
              </h2>
              <p className="text-2xs text-muted">
                Source: execution-evidence fingerprints carried by the durable run rows.
                Agreement of execution evidence never implies agreement of
                reconciliation classifications, repair contents, or target state.
              </p>
            </div>
            <div className="overflow-x-auto">
              <table
                className="w-full text-left text-xs"
                aria-label="Correctness results"
              >
                <tbody data-testid="compare-correctness">
                  <tr className="border-b border-border/60">
                    <th
                      scope="row"
                      className="px-4 py-1.5 text-left font-medium text-muted"
                    >
                      Execution-evidence format
                    </th>
                    {runs.map((run) => (
                      <td
                        key={run.run_id}
                        className="px-4 py-1.5 font-mono text-foreground"
                      >
                        {run.execution_evidence_fingerprint_version === null
                          ? "unavailable"
                          : `version ${String(run.execution_evidence_fingerprint_version)}`}
                      </td>
                    ))}
                  </tr>
                  <tr className="border-b border-border/60">
                    <th
                      scope="row"
                      className="px-4 py-1.5 text-left font-medium text-muted"
                    >
                      Execution fingerprint agreement
                    </th>
                    {runs.map((run, index) => (
                      <td key={run.run_id} className="px-4 py-1.5 text-foreground">
                        <StatusBadge state={agreement.agree ? "verified" : "warning"}>
                          {agreement.agree ? "agree" : "disagree"}
                        </StatusBadge>
                        <span className="ml-2 font-mono text-2xs text-muted">
                          {agreement.fingerprints[index] === null
                            ? "unavailable"
                            : `${agreement.fingerprints[index]?.slice(0, 12)}…`}
                        </span>
                      </td>
                    ))}
                  </tr>
                </tbody>
              </table>
            </div>
          </section>

          {agreement.agree ? (
            <section
              aria-label="Speed comparison"
              className="rounded-lg border border-border"
            >
              <div className="border-b border-border px-4 py-2">
                <h2 className="text-sm font-semibold text-foreground">Speed</h2>
                <p className="text-2xs text-muted">
                  Only shown once correctness is on the record. Duration derives from
                  durable timestamps; every other requested metric has no accepted
                  source and is listed as unavailable, never zero.
                </p>
              </div>
              <div className="overflow-x-auto">
                <table className="w-full text-left text-xs" aria-label="Speed results">
                  <thead>
                    <tr className="border-b border-border text-muted">
                      <th scope="col" className="px-4 py-1.5 font-medium">
                        Metric
                      </th>
                      {runs.map((run) => (
                        <th
                          scope="col"
                          key={run.run_id}
                          className="px-4 py-1.5 font-medium font-mono"
                        >
                          {run.run_id}
                        </th>
                      ))}
                      <th scope="col" className="px-4 py-1.5 font-medium">
                        Unit
                      </th>
                    </tr>
                  </thead>
                  <tbody data-testid="compare-speed">
                    <tr className="border-b border-border/60">
                      <th
                        scope="row"
                        className="px-4 py-1.5 text-left font-medium text-muted"
                      >
                        {duration.name}
                      </th>
                      {duration.values.map((value, index) => (
                        <td
                          key={index}
                          className="px-4 py-1.5 font-mono text-foreground"
                        >
                          {formatSecondsValue(value)}
                        </td>
                      ))}
                      <td className="px-4 py-1.5 text-muted">{duration.unit}</td>
                    </tr>
                    {UNAVAILABLE_COMPARISON_ROWS.map((row) => (
                      <tr
                        key={row.name}
                        className="border-b border-border/60 last:border-b-0"
                      >
                        <th
                          scope="row"
                          className="px-4 py-1.5 text-left font-medium text-muted"
                        >
                          {row.name}
                        </th>
                        <td
                          colSpan={runs.length}
                          className="px-4 py-1.5 text-muted"
                          data-testid="compare-unavailable"
                        >
                          unavailable — {row.reason}
                        </td>
                        <td className="px-4 py-1.5 text-muted">—</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
              <DurationChart duration={duration} runs={runs.map((run) => run.run_id)} />
            </section>
          ) : (
            <section
              aria-label="Speed comparison blocked"
              className="rounded-lg border border-warning/40 bg-warning/10 p-4"
              data-testid="compare-speed-blocked"
            >
              <h2 className="text-sm font-semibold text-warning">
                Speed comparison blocked
              </h2>
              <p className="mt-1 text-xs text-foreground">
                Execution-evidence fingerprints disagree. Correctness is an absolute
                gate, so timing and resource comparisons are not presented.
              </p>
            </section>
          )}
        </>
      )}
    </div>
  );
}

function DurationChart({
  duration,
  runs,
}: {
  duration: { readonly values: readonly (number | null)[]; readonly unit: string };
  runs: readonly string[];
}) {
  const present = duration.values.filter((value) => value !== null);
  if (present.length < 2) {
    return (
      <p className="border-t border-border px-4 py-2 text-2xs text-muted" role="status">
        Duration chart unavailable — fewer than two runs expose a durable duration.
      </p>
    );
  }
  const max = Math.max(...present.map((value) => value ?? 0), 0.0001);
  return (
    <figure className="border-t border-border px-4 py-3">
      <figcaption className="text-2xs text-muted">
        Run duration, seconds, zero-based axis — the same values as the table above.
      </figcaption>
      <svg
        viewBox={`0 0 600 ${String(runs.length * 30 + 10)}`}
        className="mt-2 w-full max-w-xl"
        role="img"
        aria-label={`Bar chart comparing run duration in seconds from zero to ${max.toFixed(3)}; identical values are in the speed table.`}
        data-testid="compare-duration-chart"
      >
        {duration.values.map((value, index) => {
          const width = value === null ? 0 : Math.max((value / max) * 560, 2);
          return (
            <g key={index}>
              <rect
                x={0}
                y={index * 30 + 4}
                width={width}
                height={16}
                rx={3}
                className="fill-active/60"
              />
              <text x={width + 6} y={index * 30 + 16} className="fill-current text-2xs">
                {formatSecondsValue(value)}
              </text>
            </g>
          );
        })}
      </svg>
    </figure>
  );
}

export function ComparisonScreen() {
  const [search, setSearch] = useSearchParams();
  const runIds = useMemo(() => parseCompareRunIds(search), [search]);
  const selection = useQuery({
    queryKey: queryKeys.runList({ limit: 50 }),
    queryFn: ({ signal }) => fetchRunPage({ limit: 50 }, { signal }),
    staleTime: 15_000,
    retry: 0,
  });

  const toggleRun = (runId: string) => {
    const next = new URLSearchParams(search);
    const current = parseCompareRunIds(search);
    const updated = current.includes(runId)
      ? current.filter((id) => id !== runId)
      : [...current, runId].slice(0, MAX_COMPARED);
    if (updated.length === 0) {
      next.delete("runs");
    } else {
      next.set("runs", updated.join(","));
    }
    setSearch(next);
  };

  return (
    <div className="min-h-0 flex-1 overflow-y-auto p-6">
      <h1
        data-page-title
        tabIndex={-1}
        className="text-lg font-semibold text-foreground"
      >
        Runner comparison
      </h1>
      <p className="mt-1 max-w-2xl text-sm text-muted">
        Compare compatible runs correctness-first. Incompatible runs are blocked with
        the exact reason; unsupported metrics say so instead of implying an equivalence.
      </p>

      <section
        aria-label="Run selection"
        className="mt-4 rounded-lg border border-border"
      >
        <div className="border-b border-border px-4 py-2">
          <h2 className="text-sm font-semibold text-foreground">Select runs</h2>
          <p className="text-2xs text-muted">
            Source: latest <span className="font-mono">GET /api/v1/runs</span> page
            (limit 50). Select two or three runs; selection is URL-backed.
          </p>
        </div>
        {selection.isPending ? (
          <div className="p-4">
            <Loading label="Loading runs" />
          </div>
        ) : selection.isError ? (
          <div className="p-4">
            <ErrorState
              title="Run selection unavailable"
              description="The durable run page could not be loaded."
              action={
                <button
                  type="button"
                  className="rounded border border-border px-3 py-1.5 text-xs text-foreground"
                  onClick={() => {
                    void selection.refetch();
                  }}
                >
                  Retry
                </button>
              }
            />
          </div>
        ) : selection.data.items.length === 0 ? (
          <div className="p-4">
            <EmptyState
              title="No runs recorded yet"
              description="Runs appear here after execution workflows record them."
            />
          </div>
        ) : (
          <ul className="divide-y divide-border/60">
            {selection.data.items.map((run) => {
              const checked = runIds.includes(run.run_id);
              return (
                <li key={run.run_id} className="flex items-center gap-3 px-4 py-2">
                  <input
                    type="checkbox"
                    id={`compare-${run.run_id}`}
                    data-testid={`compare-check-${run.run_id}`}
                    className="size-3.5"
                    checked={checked}
                    onChange={() => {
                      toggleRun(run.run_id);
                    }}
                  />
                  <label
                    htmlFor={`compare-${run.run_id}`}
                    className="flex flex-1 flex-wrap items-center gap-2 text-xs"
                  >
                    <span className="font-mono text-foreground">{run.run_id}</span>
                    <StatusBadge state={runStateTone(run.state)}>
                      {run.state}
                    </StatusBadge>
                    <span className="font-mono text-muted">
                      {run.pipeline_id} v{String(run.pipeline_version)}
                    </span>
                    <span className="text-muted">{run.runner_kind}</span>
                  </label>
                </li>
              );
            })}
          </ul>
        )}
      </section>

      {runIds.length >= 2 ? (
        <div className="mt-4">
          <ComparisonDashboard runIds={runIds} />
        </div>
      ) : (
        <p className="mt-4 text-xs text-muted" role="status">
          Select at least two runs to compare; at most {String(MAX_COMPARED)}.
        </p>
      )}
      <p className="mt-4 text-2xs text-muted">
        Runner strategy availability and environment context:{" "}
        <a className="text-active underline" href="/app/system">
          system capabilities
        </a>
        .
      </p>
    </div>
  );
}

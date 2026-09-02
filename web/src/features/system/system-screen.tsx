/**
 * System capabilities page (P17.7). Every runtime capability is reported as
 * exactly one of available, unavailable with its structured reason, or
 * unsupported. Capability fetch failures render as errors, never as a
 * healthy default; unsupported entries never look like a green check.
 */
import { useQuery } from "@tanstack/react-query";

import {
  fetchCapabilitiesDetail,
  fetchHealthDetail,
  fetchReadiness,
} from "../../api/client";
import { queryKeys } from "../../api/query-keys";
import { ErrorState } from "../../components/states/error-state";
import { Loading } from "../../components/states/loading";
import { StatusBadge } from "../../components/ui/status-badge";
import type { CapabilityStatus } from "./capability-derivations";
import {
  derivePoolCapabilities,
  deriveProfileCapabilities,
  deriveRunnerCapabilities,
  deriveSqliteCapabilities,
} from "./capability-derivations";

export function CapabilityStatusBadge({ status }: { status: CapabilityStatus }) {
  if (status === "available") {
    return (
      <StatusBadge state="verified">
        <span aria-hidden="true" className="mr-1 font-mono">
          ✓
        </span>
        available
      </StatusBadge>
    );
  }
  if (status === "unavailable") {
    return (
      <StatusBadge state="warning">
        <span aria-hidden="true" className="mr-1 font-mono">
          ✗
        </span>
        unavailable
      </StatusBadge>
    );
  }
  return (
    <StatusBadge state="neutral">
      <span aria-hidden="true" className="mr-1 font-mono">
        ○
      </span>
      unsupported
    </StatusBadge>
  );
}

export function SystemCapabilities() {
  const capabilities = useQuery({
    queryKey: queryKeys.capabilities(),
    queryFn: ({ signal }) => fetchCapabilitiesDetail({ signal }),
    staleTime: 60_000,
    retry: 0,
  });
  const readiness = useQuery({
    queryKey: queryKeys.readiness(),
    queryFn: ({ signal }) => fetchReadiness({ signal }),
    staleTime: 15_000,
    retry: 0,
  });
  const health = useQuery({
    queryKey: queryKeys.health(),
    queryFn: ({ signal }) => fetchHealthDetail({ signal }),
    staleTime: 15_000,
    retry: 0,
  });

  return (
    <div className="space-y-4">
      <section
        aria-label="Runner strategies"
        className="rounded-lg border border-border"
        data-testid="system-runners"
      >
        <div className="border-b border-border px-4 py-2">
          <h2 className="text-sm font-semibold text-foreground">Runner strategies</h2>
          <p className="text-2xs text-muted">
            Source: <span className="font-mono">GET /api/v1/system/capabilities</span>.
            A strategy absent from the runtime report is unsupported — it is never
            presented as healthy.
          </p>
        </div>
        {capabilities.isPending ? (
          <div className="p-4">
            <Loading label="Loading capabilities" />
          </div>
        ) : capabilities.isError ? (
          <div className="p-4">
            <ErrorState
              title="Capability report unavailable"
              description="The runtime capability report could not be loaded; runner availability is unknown and nothing is assumed healthy."
              action={
                <button
                  type="button"
                  className="rounded border border-border px-3 py-1.5 text-xs text-foreground"
                  onClick={() => {
                    void capabilities.refetch();
                  }}
                >
                  Retry
                </button>
              }
            />
          </div>
        ) : (
          <ul className="divide-y divide-border/60">
            {deriveRunnerCapabilities(capabilities.data).map((row) => (
              <li
                key={row.id}
                className="flex flex-wrap items-center gap-3 px-4 py-2 text-xs"
              >
                <CapabilityStatusBadge status={row.status} />
                <span className="font-medium text-foreground">{row.label}</span>
                {row.reason !== null && (
                  <span className="text-warning" role="status">
                    reason: {row.reason}
                  </span>
                )}
                {row.detail !== null && (
                  <span className="text-muted">{row.detail}</span>
                )}
              </li>
            ))}
          </ul>
        )}
      </section>

      <section
        aria-label="Subordinate worker pools"
        className="rounded-lg border border-border"
      >
        <div className="border-b border-border px-4 py-2">
          <h2 className="text-sm font-semibold text-foreground">
            Subordinate worker pools
          </h2>
          <p className="text-2xs text-muted">
            Source: <span className="font-mono">GET /api/v1/system/capabilities</span>.
          </p>
        </div>
        {capabilities.isPending ? (
          <div className="p-4">
            <Loading label="Loading worker pools" />
          </div>
        ) : capabilities.isError ? (
          <p className="px-4 py-3 text-xs text-warning" role="status">
            Worker pool availability is unavailable right now.
          </p>
        ) : (
          <ul className="divide-y divide-border/60">
            {derivePoolCapabilities(capabilities.data).map((row) => (
              <li
                key={row.id}
                className="flex flex-wrap items-center gap-3 px-4 py-2 text-xs"
              >
                <CapabilityStatusBadge status={row.status} />
                <span className="font-medium text-foreground">{row.label}</span>
                {row.reason !== null && (
                  <span className="text-warning" role="status">
                    reason: {row.reason}
                  </span>
                )}
                {row.detail !== null && (
                  <span className="text-muted">{row.detail}</span>
                )}
              </li>
            ))}
          </ul>
        )}
      </section>

      <section
        aria-label="SQLite capabilities"
        className="rounded-lg border border-border"
      >
        <div className="border-b border-border px-4 py-2">
          <h2 className="text-sm font-semibold text-foreground">SQLite capabilities</h2>
          <p className="text-2xs text-muted">
            Source: <span className="font-mono">GET /api/v1/system/capabilities</span>.
          </p>
        </div>
        {capabilities.isPending ? (
          <div className="p-4">
            <Loading label="Loading SQLite capabilities" />
          </div>
        ) : capabilities.isError ? (
          <p className="px-4 py-3 text-xs text-warning" role="status">
            SQLite capability facts are unavailable right now.
          </p>
        ) : (
          <div className="overflow-x-auto">
            <table
              className="w-full text-left text-xs"
              aria-label="SQLite capabilities"
            >
              <thead>
                <tr className="border-b border-border text-muted">
                  <th scope="col" className="px-4 py-1.5 font-medium">
                    Capability
                  </th>
                  <th scope="col" className="px-4 py-1.5 font-medium">
                    Value
                  </th>
                  <th scope="col" className="px-4 py-1.5 font-medium">
                    Status
                  </th>
                </tr>
              </thead>
              <tbody>
                {deriveSqliteCapabilities(capabilities.data).map((row) => (
                  <tr
                    key={row.label}
                    className="border-b border-border/60 last:border-b-0"
                  >
                    <th
                      scope="row"
                      className="px-4 py-1.5 text-left font-medium text-foreground"
                    >
                      {row.label}
                    </th>
                    <td className="px-4 py-1.5 font-mono text-muted">{row.value}</td>
                    <td className="px-4 py-1.5">
                      <CapabilityStatusBadge status={row.status} />
                      {row.reason !== null && (
                        <span className="ml-2 text-2xs text-warning">{row.reason}</span>
                      )}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </section>

      <section
        aria-label="Storage readiness"
        className="rounded-lg border border-border"
      >
        <div className="border-b border-border px-4 py-2">
          <h2 className="text-sm font-semibold text-foreground">Storage readiness</h2>
          <p className="text-2xs text-muted">
            Source: <span className="font-mono">GET /readyz</span> and{" "}
            <span className="font-mono">GET /healthz</span>. Readiness verifies
            migrations and required storage.
          </p>
        </div>
        <div className="space-y-2 p-4 text-xs">
          {readiness.isPending || health.isPending ? (
            <Loading label="Loading storage readiness" />
          ) : readiness.isError || health.isError ? (
            <ErrorState
              title="Storage readiness unavailable"
              description="Readiness or liveness could not be observed. Storage state is unknown and is not reported as ready."
              action={
                <button
                  type="button"
                  className="rounded border border-border px-3 py-1.5 text-xs text-foreground"
                  onClick={() => {
                    void readiness.refetch();
                    void health.refetch();
                  }}
                >
                  Retry
                </button>
              }
            />
          ) : (
            <>
              <div className="flex items-center gap-2">
                <StatusBadge
                  state={readiness.data.status === "ready" ? "verified" : "warning"}
                >
                  {readiness.data.status}
                </StatusBadge>
                <span className="text-muted">{readiness.data.detail}</span>
              </div>
              <div className="flex items-center gap-2">
                <StatusBadge
                  state={health.data.status === "ok" ? "verified" : "failure"}
                >
                  {health.data.status}
                </StatusBadge>
                <span className="font-mono text-muted">
                  service {health.data.service} v{health.data.version}
                </span>
              </div>
            </>
          )}
        </div>
      </section>

      <section
        aria-label="Optional runtime profiles"
        className="rounded-lg border border-border"
      >
        <div className="border-b border-border px-4 py-2">
          <h2 className="text-sm font-semibold text-foreground">
            Optional runtime profiles
          </h2>
          <p className="text-2xs text-muted">
            Source: <span className="font-mono">GET /api/v1/system/capabilities</span>{" "}
            features.
          </p>
        </div>
        {capabilities.isPending ? (
          <div className="p-4">
            <Loading label="Loading runtime profiles" />
          </div>
        ) : capabilities.isError ? (
          <p className="px-4 py-3 text-xs text-warning" role="status">
            Optional runtime profiles are unavailable right now.
          </p>
        ) : (
          <ul className="divide-y divide-border/60">
            {deriveProfileCapabilities(capabilities.data).map((row) => (
              <li
                key={row.id}
                className="flex flex-wrap items-center gap-3 px-4 py-2 text-xs"
              >
                <CapabilityStatusBadge status={row.status} />
                <span className="font-medium text-foreground">{row.label}</span>
                {row.reason !== null && (
                  <span className="text-warning" role="status">
                    reason: {row.reason}
                  </span>
                )}
                {row.detail !== null && (
                  <span className="text-muted">{row.detail}</span>
                )}
              </li>
            ))}
          </ul>
        )}
      </section>

      <section
        aria-label="Accepted operational limits"
        className="rounded-lg border border-border"
      >
        <div className="border-b border-border px-4 py-2">
          <h2 className="text-sm font-semibold text-foreground">
            Accepted operational limits
          </h2>
          <p className="text-2xs text-muted">
            Source: <span className="font-mono">GET /api/v1/system/capabilities</span>{" "}
            limits.
          </p>
        </div>
        {capabilities.isPending ? (
          <div className="p-4">
            <Loading label="Loading operational limits" />
          </div>
        ) : capabilities.isError ? (
          <p className="px-4 py-3 text-xs text-warning" role="status">
            Operational limits are unavailable right now.
          </p>
        ) : (
          <div className="overflow-x-auto">
            <table className="w-full text-left text-xs" aria-label="Operational limits">
              <thead>
                <tr className="border-b border-border text-muted">
                  <th scope="col" className="px-4 py-1.5 font-medium">
                    Limit
                  </th>
                  <th scope="col" className="px-4 py-1.5 font-medium">
                    Value
                  </th>
                </tr>
              </thead>
              <tbody>
                {Object.entries(capabilities.data.limits).map(([name, value]) => (
                  <tr key={name} className="border-b border-border/60 last:border-b-0">
                    <th
                      scope="row"
                      className="px-4 py-1.5 text-left font-mono font-medium text-foreground"
                    >
                      {name}
                    </th>
                    <td className="px-4 py-1.5 font-mono text-muted">
                      {String(value)}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </section>
    </div>
  );
}

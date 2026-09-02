/**
 * Capability status derivation for the system page. Every reported runtime
 * capability resolves to exactly one of three states: available,
 * unavailable with its structured reason, or unsupported — meaning the
 * runtime's capability report does not mention it at all. Unsupported is
 * never rendered as healthy and failures are never hidden.
 */
import type { CapabilitiesValue } from "../../api/schemas";

export type CapabilityStatus = "available" | "unavailable" | "unsupported";

export interface CapabilityRow {
  readonly id: string;
  readonly label: string;
  readonly status: CapabilityStatus;
  /** Structured reason; present exactly when status is "unavailable". */
  readonly reason: string | null;
  /** Detail rendered for available entries (for example a version). */
  readonly detail: string | null;
}

/** Runner strategies the frontend registry knows about. */
export const KNOWN_RUNNER_STRATEGIES = ["sequential", "threaded", "asyncio"] as const;

/**
 * Map the runtime's reported runner strategies onto the known list. A
 * strategy absent from the report is unsupported, not healthy.
 */
export function deriveRunnerCapabilities(
  capabilities: CapabilitiesValue,
): readonly CapabilityRow[] {
  const reported = new Map(
    capabilities.runners.map((runner) => [runner.strategy_id, runner]),
  );
  const rows: CapabilityRow[] = KNOWN_RUNNER_STRATEGIES.map((strategy) => {
    const runner = reported.get(strategy);
    if (runner === undefined) {
      return {
        id: `runner-${strategy}`,
        label: `Runner strategy ${strategy}`,
        status: "unsupported",
        reason: null,
        detail: null,
      };
    }
    return {
      id: `runner-${strategy}`,
      label: `Runner strategy ${strategy}`,
      status: runner.available ? "available" : "unavailable",
      reason: runner.available
        ? null
        : (runner.unavailability_reason ?? "reported unavailable without a reason"),
      detail: runner.available ? "reported available" : null,
    };
  });
  for (const [strategy, runner] of reported) {
    if ((KNOWN_RUNNER_STRATEGIES as readonly string[]).includes(strategy)) {
      continue;
    }
    rows.push({
      id: `runner-${strategy}`,
      label: `Runner strategy ${strategy}`,
      status: runner.available ? "available" : "unavailable",
      reason: runner.available
        ? null
        : (runner.unavailability_reason ?? "reported unavailable without a reason"),
      detail: runner.available ? "reported available" : null,
    });
  }
  return rows;
}

/** Subordinate worker pools reported by the runtime. */
export function derivePoolCapabilities(
  capabilities: CapabilitiesValue,
): readonly CapabilityRow[] {
  if (capabilities.subordinate_pools.length === 0) {
    return [
      {
        id: "pools-none",
        label: "Subordinate worker pools",
        status: "unsupported",
        reason: null,
        detail: null,
      },
    ];
  }
  return capabilities.subordinate_pools.map((pool) => ({
    id: `pool-${pool.pool_id}`,
    label: `Worker pool ${pool.pool_id}`,
    status: pool.available ? "available" : "unavailable",
    reason: pool.available
      ? null
      : (pool.unavailability_reason ?? "reported unavailable without a reason"),
    detail: pool.available ? "reported available" : null,
  }));
}

export interface SqliteCapabilityRow {
  readonly label: string;
  readonly value: string;
  readonly status: CapabilityStatus;
  readonly reason: string | null;
}

/** SQLite facts; feature booleans resolve to the same three states. */
export function deriveSqliteCapabilities(
  capabilities: CapabilitiesValue,
): readonly SqliteCapabilityRow[] {
  const sqlite = capabilities.sqlite;
  const booleanRow = (label: string, supported: boolean): SqliteCapabilityRow => ({
    label,
    value: supported ? "yes" : "no",
    status: supported ? "available" : "unavailable",
    reason: supported ? null : "this SQLite build does not provide the feature",
  });
  return [
    {
      label: "SQLite library version",
      value: `${sqlite.library_version} (minimum supported ${sqlite.minimum_supported_version})`,
      status: "available",
      reason: null,
    },
    {
      label: "Journal mode",
      value: sqlite.journal_mode,
      status: "available",
      reason: null,
    },
    {
      label: "Synchronous level",
      value: String(sqlite.synchronous_level),
      status: "available",
      reason: null,
    },
    {
      label: "Threadsafety",
      value: String(sqlite.threadsafety),
      status: sqlite.threadsafety > 0 ? "available" : "unavailable",
      reason:
        sqlite.threadsafety > 0
          ? null
          : "the SQLite build reports no threading support",
    },
    booleanRow("JSON SQL support", sqlite.supports_json_sql),
    booleanRow("RETURNING support", sqlite.supports_returning),
  ];
}

/** Optional runtime profiles (features) reported by the runtime. */
export function deriveProfileCapabilities(
  capabilities: CapabilitiesValue,
): readonly CapabilityRow[] {
  if (capabilities.features.length === 0) {
    return [
      {
        id: "features-none",
        label: "Optional runtime profiles",
        status: "unsupported",
        reason: null,
        detail: "this runtime reports no optional profiles",
      },
    ];
  }
  return capabilities.features.map((feature) => ({
    id: `feature-${feature.name}`,
    label: feature.name,
    status: feature.available ? "available" : "unavailable",
    reason: feature.available ? null : "reported unavailable by the runtime",
    detail: feature.available ? "reported available" : null,
  }));
}

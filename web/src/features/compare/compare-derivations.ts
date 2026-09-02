/**
 * Correctness-first runner comparison derivations. Two runs may be compared
 * only when every accepted identity and evidence-format field agrees:
 * pipeline identity and version, scenario seed, evidence finalization, and
 * the execution-evidence fingerprint format version. Runner strategy may
 * differ — that difference is the subject of the comparison. Metrics the
 * accepted REST boundary does not expose are rendered as explicitly
 * unavailable with their reason; they are never inferred, and agreement of
 * execution evidence never implies agreement of reconciliation, target
 * state, or repair effects.
 */
import type { RunResponse } from "../../api/generated/schema";
import { runDurationSeconds } from "../runs/run-derivations";

/** Maximum number of runs one comparison may include. */
export const MAX_COMPARED = 3;

/**
 * The durable states in which the persistence contract permits finalized
 * execution evidence. Failed and cancelled runs are terminal, but do not
 * carry an execution-evidence fingerprint and therefore are not comparable.
 */
const EVIDENCE_FINALIZED_RUN_STATES: ReadonlySet<string> = new Set([
  "succeeded",
  "partially_succeeded",
]);

/**
 * Parse the bounded `runs` URL parameter: comma-separated run identities,
 * deduplicated, capped at the comparison maximum.
 */
export function parseCompareRunIds(search: URLSearchParams): string[] {
  const raw = search.get("runs") ?? "";
  const seen = new Set<string>();
  const ids: string[] = [];
  for (const candidate of raw.split(",")) {
    const trimmed = candidate.trim();
    if (
      /^run_[a-z0-9]+(?:-[a-z0-9]+)*$/.test(trimmed) &&
      trimmed.length >= 7 &&
      trimmed.length <= 68 &&
      !seen.has(trimmed)
    ) {
      seen.add(trimmed);
      ids.push(trimmed);
    }
    if (ids.length === MAX_COMPARED) {
      break;
    }
  }
  return ids;
}

export interface CompareEntry {
  readonly run: RunResponse;
}

export interface CompatibilityFailure {
  /** Machine-stable reason code rendered alongside the human explanation. */
  readonly code: string;
  readonly explanation: string;
}

export type ComparisonVerdict =
  | { readonly comparable: false; readonly failures: readonly CompatibilityFailure[] }
  | { readonly comparable: true; readonly failures: readonly [] };

/**
 * Decide whether the selected runs may be compared at all. Every failure is
 * reported so the operator sees the complete set of blocking reasons.
 */
export function compareCompatibility(runs: readonly RunResponse[]): ComparisonVerdict {
  const failures: CompatibilityFailure[] = [];
  const first = runs[0];
  if (first === undefined) {
    return { comparable: false, failures: [] };
  }
  for (const run of runs) {
    if (!EVIDENCE_FINALIZED_RUN_STATES.has(run.state)) {
      failures.push({
        code: "run_not_finalized",
        explanation: `${run.run_id} is ${run.state}; execution evidence is only finalized for a succeeded or partially succeeded run.`,
      });
    }
  }
  for (const run of runs.slice(1)) {
    if (run.pipeline_id !== first.pipeline_id) {
      failures.push({
        code: "pipeline_identity_mismatch",
        explanation: `${run.run_id} executes pipeline ${run.pipeline_id}, not ${first.pipeline_id}.`,
      });
    }
    if (run.pipeline_version !== first.pipeline_version) {
      failures.push({
        code: "pipeline_version_mismatch",
        explanation: `${run.run_id} executes pipeline version ${String(run.pipeline_version)}, not ${String(first.pipeline_version)}.`,
      });
    }
    if (run.scenario_seed !== first.scenario_seed) {
      failures.push({
        code: "scenario_seed_mismatch",
        explanation: `${run.run_id} uses scenario seed ${String(run.scenario_seed)}, not ${String(first.scenario_seed)}; the work inputs are not the same.`,
      });
    }
  }
  if (runs.some((run) => run.scenario_seed === null)) {
    failures.push({
      code: "scenario_seed_unavailable",
      explanation:
        "At least one run has no scenario seed, so equivalent comparison inputs cannot be established from the accepted run API.",
    });
  }
  const versions = new Set(
    runs.map((run) => run.execution_evidence_fingerprint_version),
  );
  if (versions.has(null) || versions.has(undefined)) {
    failures.push({
      code: "evidence_format_unavailable",
      explanation:
        "At least one run has not finalized execution evidence, so the evidence format cannot be checked.",
    });
  } else if (versions.size > 1) {
    failures.push({
      code: "evidence_format_mismatch",
      explanation: `Execution-evidence format versions differ (${[...versions]
        .map((version) => String(version))
        .join(" vs ")}); the documents are not comparable.`,
    });
  }
  if (runs.some((run) => run.execution_evidence_fingerprint == null)) {
    failures.push({
      code: "execution_evidence_unavailable",
      explanation:
        "At least one run has not finalized an execution-evidence fingerprint, so correctness cannot be evaluated before timing.",
    });
  }
  return failures.length === 0
    ? { comparable: true, failures: [] }
    : { comparable: false, failures };
}

export interface UnavailableMetric {
  readonly name: string;
  readonly reason: string;
}

export interface MetricRow {
  readonly name: string;
  /** Per-run value; null renders as explicitly unavailable. */
  readonly values: readonly (string | null)[];
  readonly unit: string;
  readonly kind: "identity" | "correctness" | "speed" | "resource";
}

export interface AgreedFingerprints {
  readonly agree: boolean;
  readonly fingerprints: readonly (string | null)[];
}

/** Execution fingerprint agreement: the correctness verdict row. */
export function fingerprintAgreement(runs: readonly RunResponse[]): AgreedFingerprints {
  const fingerprints = runs.map((run) => run.execution_evidence_fingerprint ?? null);
  const present = fingerprints.filter((value) => value !== null);
  const agree = present.length === runs.length && new Set(present).size === 1;
  return { agree, fingerprints };
}

export interface SpeedRow {
  readonly name: string;
  readonly values: readonly (number | null)[];
  readonly unit: string;
}

/** Duration is the one speed metric durable REST timestamps support. */
export function durationRow(runs: readonly RunResponse[]): SpeedRow {
  return {
    name: "Run duration",
    values: runs.map((run) => runDurationSeconds(run)),
    unit: "seconds",
  };
}

/**
 * Metrics named by the phase goal whose accepted sources do not exist for
 * the browser: no REST route exposes statistics, retry counts, memory, or
 * database timings, and queue/service timing is ephemeral per-run telemetry
 * that cannot be compared after the fact. Each row states that reason.
 */
export const UNAVAILABLE_COMPARISON_ROWS: readonly UnavailableMetric[] = [
  {
    name: "Accepted and quarantined counts",
    reason: "not exposed by the accepted run API; no bounded source",
  },
  {
    name: "Repair-effect evidence",
    reason:
      "repair equivalence is never claimed from runner execution evidence; no repair inputs are part of this comparison",
  },
  {
    name: "Throughput (records/second)",
    reason: "record counts are not exposed by the accepted run API",
  },
  {
    name: "Task latency p50 / p95 / p99",
    reason:
      "attempt latency statistics are internal to the analytics engine and have no accepted route",
  },
  {
    name: "Queue wait and active service time",
    reason:
      "ephemeral per-run telemetry only; it is never persisted and cannot be compared afterwards",
  },
  {
    name: "Retry counts",
    reason: "attempt histories are durable but not exposed by the accepted run API",
  },
  {
    name: "Peak memory and in-flight work",
    reason: "no bounded source in the accepted run API",
  },
  {
    name: "SQLite transaction and DuckDB query durations",
    reason: "no bounded source in the accepted run API",
  },
];

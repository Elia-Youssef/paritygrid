/**
 * Pure derivations turning accepted durable and telemetry inputs into the
 * bounded views the observability screens render. Nothing here mutates
 * state, every value carries its own source, and telemetry-derived numbers
 * are always marked advisory so they can never be presented as durable
 * facts. Unknown or malformed identities are counted as diagnostics instead
 * of being merged into known ones.
 */
import type { RunResponse } from "../../api/generated/schema";
import { RUN_STATES, runStateSchema, type RunStateValue } from "../../api/schemas";
import type { DurableEventFrame, TelemetryRecord } from "../../live/protocol";
import type { TelemetrySlice } from "../../live/live-state";

// ---------------------------------------------------------------------------
// Run state presentation
// ---------------------------------------------------------------------------

/** The closed durable `RunState` set served by the accepted API. */
export { RUN_STATES };
export type { RunStateValue };

export function isRunStateValue(value: string): value is RunStateValue {
  return runStateSchema.safeParse(value).success;
}

export type RunStateTone =
  | "active"
  | "verified"
  | "warning"
  | "failure"
  | "paused"
  | "cancelled"
  | "stale"
  | "neutral";

/** Non-color badge tone for each durable run state; text carries the state. */
export function runStateTone(state: string): RunStateTone {
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

// ---------------------------------------------------------------------------
// Per-node durable overlay
// ---------------------------------------------------------------------------

export interface NodeRunState {
  readonly nodeId: string;
  /** Last durable observation for the node; text is rendered verbatim. */
  readonly state: string;
  /** Highest attempt number observed for the node, when events carry one. */
  readonly attempts: number | null;
  /** Durable sequence of the last accepted event mentioning the node. */
  readonly lastSequence: number;
  /** Durable occurrence timestamp of that event. */
  readonly lastOccurredAt: string;
}

export interface NodeOverlayResult {
  readonly states: ReadonlyMap<string, NodeRunState>;
  /** Events that mentioned node identities outside the known document. */
  readonly foreignNodeEvents: number;
}

interface NodeEventObservation {
  state: string;
  attempts: number | null;
}

/**
 * Map one durable frame onto a node observation. Only known node identities
 * from the fetched pipeline document are interpreted; work-item subjects
 * without a parsable, known `node_id` are counted as diagnostics.
 */
function observeNodeEvent(
  frame: DurableEventFrame,
  knownNodeIds: ReadonlySet<string>,
): { nodeId: string; observation: NodeEventObservation } | null {
  const candidate = frame.payload.node_id;
  if (typeof candidate !== "string" || !knownNodeIds.has(candidate)) {
    return null;
  }
  const rawAttempt = frame.payload.attempt_number;
  const attempts =
    typeof rawAttempt === "number" && Number.isInteger(rawAttempt) && rawAttempt > 0
      ? rawAttempt
      : null;
  return {
    nodeId: candidate,
    observation: { state: frame.event_kind, attempts },
  };
}

/**
 * Derive per-node durable overlay state from the accepted event log. Events
 * are applied oldest to newest so the newest durable fact wins; unknown
 * node identities never merge into known ones.
 */
export function deriveNodeOverlay(
  events: readonly DurableEventFrame[],
  knownNodeIds: readonly string[],
): NodeOverlayResult {
  const known = new Set(knownNodeIds);
  const states = new Map<string, NodeRunState>();
  let foreignNodeEvents = 0;
  for (const frame of [...events].reverse()) {
    const observed = observeNodeEvent(frame, known);
    if (observed === null) {
      const mentioned = frame.payload.node_id;
      if (typeof mentioned === "string" && mentioned !== "" && !known.has(mentioned)) {
        foreignNodeEvents += 1;
      }
      continue;
    }
    const previous = states.get(observed.nodeId);
    const attempts =
      observed.observation.attempts ?? (previous ? previous.attempts : null);
    states.set(observed.nodeId, {
      nodeId: observed.nodeId,
      state: observed.observation.state,
      attempts,
      lastSequence: frame.sequence,
      lastOccurredAt: frame.occurred_at,
    });
  }
  return { states, foreignNodeEvents };
}

// ---------------------------------------------------------------------------
// Worker swimlane spans
// ---------------------------------------------------------------------------

export interface SwimlaneSpan {
  /** Durable work-item identity (the SSE subject id). */
  readonly workItemId: string;
  readonly nodeId: string;
  readonly attempt: number | null;
  /** Worker identity only when a durable event carried one explicitly. */
  readonly worker: string | null;
  /** ISO timestamp of the durable claim (start of execution). */
  readonly startedAt: string;
  /** ISO timestamp of the durable terminal observation, when present. */
  readonly finishedAt: string | null;
  /** Active service seconds computed from durable timestamps only. */
  readonly serviceSeconds: number | null;
  readonly outcome: string | null;
  readonly lastSequence: number;
}

export interface SwimlaneResult {
  readonly spans: readonly SwimlaneSpan[];
  /** Claims without a terminal event yet; rendered as still active. */
  readonly activeClaims: number;
  /** Events carrying uninterpretable work identities. */
  readonly unpairedEvents: number;
}

interface ClaimRecord {
  nodeId: string;
  attempt: number | null;
  worker: string | null;
  startedAt: string;
  sequence: number;
}

function workDurationSeconds(startedAt: string, finishedAt: string): number | null {
  const start = Date.parse(startedAt);
  const end = Date.parse(finishedAt);
  if (Number.isNaN(start) || Number.isNaN(end) || end < start) {
    return null;
  }
  return (end - start) / 1000;
}

const WORK_OUTCOMES = new Set([
  "work_succeeded",
  "work_failed",
  "work_quarantined",
  "work_cancelled",
]);

/**
 * Build worker swimlane spans from durable events only. Events are applied
 * oldest to newest: a claim opens a span keyed by the durable work-item
 * identity (the first claim fixes the start), a lease renewal updates the
 * worker label without moving the start, and a terminal event for the same
 * identity closes the span with a durable service duration. Queue time is
 * deliberately absent here: it is not a durable fact (advisory telemetry
 * may enrich it in the queue panels instead).
 */
export function deriveSwimlane(events: readonly DurableEventFrame[]): SwimlaneResult {
  const claims = new Map<string, ClaimRecord>();
  const spans = new Map<string, SwimlaneSpan>();
  let unpairedEvents = 0;

  // Live durable state stores the timeline newest-first. Sorting a copy by
  // sequence makes the derivation robust to either caller order and ensures
  // claims are observed before their terminal outcomes.
  for (const frame of [...events].sort(
    (left, right) => left.sequence - right.sequence,
  )) {
    if (frame.subject_kind !== "work_item") {
      continue;
    }
    const workItemId = frame.subject_id;
    const nodeId =
      typeof frame.payload.node_id === "string" ? frame.payload.node_id : null;
    const rawAttempt = frame.payload.attempt_number;
    const attempt =
      typeof rawAttempt === "number" && Number.isInteger(rawAttempt) && rawAttempt > 0
        ? rawAttempt
        : null;
    const worker =
      typeof frame.payload.worker_identity === "string"
        ? frame.payload.worker_identity
        : typeof frame.payload.lease_id === "string"
          ? frame.payload.lease_id
          : null;

    if (
      frame.event_kind === "work_claimed" ||
      frame.event_kind === "work_claim_renewed"
    ) {
      if (nodeId === null) {
        unpairedEvents += 1;
        continue;
      }
      const existing = claims.get(workItemId);
      if (existing === undefined) {
        // The first claim fixes the span start; renewals never move it.
        claims.set(workItemId, {
          nodeId,
          attempt,
          worker,
          startedAt: frame.occurred_at,
          sequence: frame.sequence,
        });
      } else if (worker !== null) {
        claims.set(workItemId, { ...existing, worker });
      }
      continue;
    }

    if (WORK_OUTCOMES.has(frame.event_kind)) {
      const claim = claims.get(workItemId);
      if (claim === undefined) {
        // Result without a seen claim (for example replay starting later):
        // still durable, but not enough to draw a span honestly.
        unpairedEvents += 1;
        continue;
      }
      const spanKey = `${workItemId}\u0000${String(attempt ?? claim.attempt ?? "unknown")}`;
      spans.set(spanKey, {
        workItemId,
        nodeId: claim.nodeId,
        attempt: attempt ?? claim.attempt,
        worker: claim.worker,
        startedAt: claim.startedAt,
        finishedAt: frame.occurred_at,
        serviceSeconds: workDurationSeconds(claim.startedAt, frame.occurred_at),
        outcome: frame.event_kind,
        lastSequence: frame.sequence,
      });
      claims.delete(workItemId);
      continue;
    }

    if (nodeId === null) {
      unpairedEvents += 1;
    }
  }

  // Claims without a terminal event are still real durable observations:
  // they render as open spans with an explicit "active" completion instead
  // of disappearing from the timeline.
  for (const [workItemId, claim] of claims) {
    const spanKey = `${workItemId}\u0000${String(claim.attempt ?? "unknown")}`;
    spans.set(spanKey, {
      workItemId,
      nodeId: claim.nodeId,
      attempt: claim.attempt,
      worker: claim.worker,
      startedAt: claim.startedAt,
      finishedAt: null,
      serviceSeconds: null,
      outcome: null,
      lastSequence: claim.sequence,
    });
  }

  return {
    spans: [...spans.values()],
    activeClaims: claims.size,
    unpairedEvents,
  };
}

// ---------------------------------------------------------------------------
// Advisory queue and saturation metrics
// ---------------------------------------------------------------------------

export interface QueueMetricView {
  readonly name: string;
  readonly definition: string;
  readonly unit: string;
  /** Raw latest value; null means the metric is absent from telemetry. */
  readonly value: number | null;
  /** Observation timestamp (micros) of the value, null when absent. */
  readonly observedMicros: number | null;
  readonly source: string;
}

export interface QueuePanelView {
  readonly metrics: readonly QueueMetricView[];
  /** Saturation is computed only when depth and capacity are both present. */
  readonly saturationPercent: number | null;
  readonly saturationSupported: boolean;
  readonly stale: boolean;
  readonly health: TelemetrySlice["health"];
  readonly lastObservedMicros: number;
}

function latestMetric(
  records: readonly TelemetryRecord[],
  suffix: string,
): { value: number; observedMicros: number } | null {
  for (const record of records) {
    for (const metric of record.metrics) {
      if (!metric.name.endsWith(suffix)) {
        continue;
      }
      if (typeof metric.value !== "number" || !Number.isFinite(metric.value)) {
        continue;
      }
      return { value: metric.value, observedMicros: record.observed_at_micros };
    }
  }
  return null;
}

function coherentSaturation(
  records: readonly TelemetryRecord[],
): { depth: number; capacity: number; observedMicros: number } | null {
  for (const record of records) {
    let depth: number | null = null;
    let capacity: number | null = null;
    for (const metric of record.metrics) {
      if (!Number.isFinite(metric.value)) {
        continue;
      }
      if (metric.name.endsWith("queue_depth")) {
        depth = metric.value;
      } else if (metric.name.endsWith("queue_capacity")) {
        capacity = metric.value;
      }
    }
    if (
      depth !== null &&
      capacity !== null &&
      depth >= 0 &&
      capacity > 0 &&
      depth <= capacity
    ) {
      return { depth, capacity, observedMicros: record.observed_at_micros };
    }
  }
  return null;
}

/**
 * Project the advisory telemetry slice into bounded queue panels. Values
 * come only from telemetry records and always carry their observation
 * timestamp; absent metrics render as explicitly unavailable rather than
 * zero. Saturation requires both depth and a positive capacity.
 */
export function deriveQueuePanels(
  telemetry: TelemetrySlice,
  runId: string,
): QueuePanelView {
  const depth = latestMetric(telemetry.records, "queue_depth");
  const capacity = latestMetric(telemetry.records, "queue_capacity");
  const active = latestMetric(telemetry.records, "active_capacity");
  const source = `advisory telemetry GET /api/v1/live/runs/${runId.replace(/[^A-Za-z0-9_-]/g, "")}`;
  const metrics: QueueMetricView[] = [
    {
      name: "Queue depth",
      definition: "Work items waiting in the writer queue for a lease.",
      unit: "work items",
      value: depth?.value ?? null,
      observedMicros: depth?.observedMicros ?? null,
      source,
    },
    {
      name: "Queue capacity",
      definition: "Bounded number of work items the writer queue accepts.",
      unit: "work items",
      value: capacity?.value ?? null,
      observedMicros: capacity?.observedMicros ?? null,
      source,
    },
    {
      name: "In-flight work",
      definition: "Work items currently leased and executing.",
      unit: "work items",
      value: active?.value ?? null,
      observedMicros: active?.observedMicros ?? null,
      source,
    },
  ];
  let saturationPercent: number | null = null;
  let saturationSupported = false;
  const saturation = coherentSaturation(telemetry.records);
  if (saturation !== null && saturation.observedMicros > 0) {
    saturationSupported = true;
    saturationPercent = Math.round((saturation.depth / saturation.capacity) * 100);
  }
  return {
    metrics,
    saturationPercent,
    saturationSupported,
    stale: telemetry.health === "stale" || telemetry.health === "recovering",
    health: telemetry.health,
    lastObservedMicros: telemetry.lastObservedMicros,
  };
}

/** Format a telemetry observation timestamp as a bounded UTC string. */
export function formatObservation(micros: number | null): string {
  if (micros === null || micros <= 0) {
    return "unavailable";
  }
  return new Date(micros / 1000).toISOString();
}

// ---------------------------------------------------------------------------
// Duration formatting shared by the swimlane and comparison screens
// ---------------------------------------------------------------------------

export function formatSeconds(seconds: number | null): string {
  if (seconds === null || !Number.isFinite(seconds)) {
    return "unavailable";
  }
  return `${seconds.toFixed(3)} s`;
}

/** Duration between two ISO timestamps in seconds, null when impossible. */
export function durationSeconds(
  startedAt: string | null,
  finishedAt: string | null,
): number | null {
  if (startedAt === null || finishedAt === null) {
    return null;
  }
  return workDurationSeconds(startedAt, finishedAt);
}

/** Total run duration used by comparison; falls back to creation time. */
export function runDurationSeconds(run: RunResponse): number | null {
  const start = run.started_at ?? run.created_at;
  return durationSeconds(start, run.finished_at);
}

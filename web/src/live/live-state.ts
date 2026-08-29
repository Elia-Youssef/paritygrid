/**
 * Deterministic state reconciliation for one run across the three data
 * sources: authoritative REST snapshots, durable ordered SSE events, and
 * ephemeral WebSocket telemetry.
 *
 * Precedence rules (enforced by construction, not by convention):
 * - Durable REST/SSE state always outranks telemetry. Telemetry lives in a
 *   separate slice with its own reducer and can never write durable fields.
 * - A REST snapshot is applied only when its run identity matches and its
 *   `run_version` is greater than or equal to the current one; late query
 *   responses carrying older versions are rejected whole.
 * - Durable events advance only in exact contiguous steps; the transport
 *   guarantees contiguity and this reducer re-checks it before mutating.
 * - Incompatible identities or versions are rejected, never merged.
 * - Recovery retains the last coherent durable view and marks it
 *   stale/recovering until authoritative data arrives.
 * - Inputs are untrusted: run-shaped payloads inside event payloads are
 *   validated before use, and unknown event kinds advance only the
 *   sequence (they are logged, never interpreted).
 *
 * Every function is pure: identical inputs produce identical outputs and
 * rejections return the SAME state reference so subscribers do not re-render.
 */
import type { DurableEventFrame } from "./protocol";
import { runSnapshotSchema } from "./protocol";
import type { TelemetryBatch, TelemetryRecord, TelemetrySnapshot } from "./protocol";
import type { RecoveryReason } from "./durable-stream";
import type { RunResponse } from "../api/generated/schema";
import { runIdentity, type RunIdentity } from "../api/client";

export const MAX_EVENT_LOG = 100;

/** Snapshot of one run received from an authoritative REST response. */
export interface RunSnapshot extends RunIdentity {
  run: RunResponse;
}

export type DurableStatus =
  "idle" | "loading" | "ready" | "stale" | "recovering" | "error";

export interface DurableRunState {
  readonly runId: string;
  /** Last accepted durable sequence for this run. */
  readonly acceptedSequence: number;
  /** The last coherent authoritative view; retained through recovery. */
  readonly run: RunResponse | null;
  readonly status: DurableStatus;
  /** Bounded newest-first log of accepted durable event summaries. */
  readonly events: readonly DurableEventFrame[];
  readonly lastError: string | null;
}

export function initialDurableRunState(runId: string): DurableRunState {
  return {
    runId,
    acceptedSequence: 0,
    run: null,
    status: "idle",
    events: [],
    lastError: null,
  };
}

/**
 * Apply one authoritative REST snapshot. Returns the same reference when
 * the snapshot must not change state (foreign run, older version).
 */
export function applyRunSnapshot(
  state: DurableRunState,
  snapshot: RunResponse,
): DurableRunState {
  const identity = runIdentity(snapshot);
  if (identity?.run_id !== state.runId) {
    // A foreign identity is an anomaly worth surfacing, unlike a mere
    // stale refresh.
    return reject(state, "foreign or malformed run snapshot");
  }
  if ((state.run?.run_version ?? Number.NEGATIVE_INFINITY) > snapshot.run_version) {
    // A late query response must never regress newer durable state. This
    // is expected under races, so it is a silent no-op: the reference is
    // kept stable and no diagnostic is raised.
    return state;
  }
  if (state.run !== null && !hasCompatibleRunDefinition(state.run, snapshot)) {
    return reject(state, "incompatible pipeline or runner identity");
  }
  return {
    ...state,
    run: snapshot,
    status: "ready",
    lastError: null,
  };
}

export function markLoading(state: DurableRunState): DurableRunState {
  return state.status === "ready" ? state : { ...state, status: "loading" };
}

export function markError(state: DurableRunState, detail: string): DurableRunState {
  return { ...state, status: "error", lastError: detail };
}

/**
 * Enter recovery after a durable gap or incompatibility. The last coherent
 * view is retained and visibly marked stale.
 */
export function markRecovering(
  state: DurableRunState,
  reason: RecoveryReason,
): DurableRunState {
  return { ...state, status: "recovering", lastError: describeReason(reason) };
}

/** Reconnects were exhausted; the coherent view is retained but stale. */
export function markStale(state: DurableRunState, detail: string): DurableRunState {
  return { ...state, status: "stale", lastError: detail };
}

/** Human-readable, bounded description of a recovery condition. */
export function describeReason(reason: RecoveryReason): string {
  switch (reason.kind) {
    case "sequence-gap":
      return `durable sequence gap: expected ${String(reason.expected)}, received ${String(reason.received)}`;
    case "stream-ahead":
      return reason.code;
    case "malformed-frame":
    case "incompatible-frame":
      return reason.detail;
    default:
      return reason.kind;
  }
}

/** Authoritative recovery finished; resume from the recovered sequence. */
export function markRecovered(
  state: DurableRunState,
  acceptedSequence: number,
): DurableRunState {
  return {
    ...state,
    acceptedSequence,
    status: state.run === null ? "loading" : "ready",
    lastError: null,
  };
}

/**
 * Apply one validated durable frame. Contiguity is re-checked here: the
 * transport already enforces it, but reducer correctness must not depend on
 * caller discipline. Rejections return the same reference.
 */
export function applyDurableEvent(
  state: DurableRunState,
  frame: DurableEventFrame,
): DurableRunState {
  const incompatibility = durableFrameCompatibility(state, frame);
  if (incompatibility !== null) {
    return markRecovering(state, incompatibility);
  }
  const expected = state.acceptedSequence + 1;
  if (frame.sequence <= state.acceptedSequence) {
    // Duplicate or already-applied event: idempotent no-op.
    return state;
  }
  if (frame.sequence > expected) {
    return markRecovering(state, {
      kind: "sequence-gap",
      expected,
      received: frame.sequence,
    });
  }

  const events = [frame, ...state.events].slice(0, MAX_EVENT_LOG);
  const next: DurableRunState = {
    ...state,
    acceptedSequence: frame.sequence,
    events,
  };
  // Some durable events carry a newer authoritative run projection (for
  // example lifecycle transitions). Apply it under the same version guard
  // as a REST snapshot; unknown event kinds only advance the sequence.
  const embedded = embeddedRunSnapshot(frame);
  if (embedded === null) {
    return next;
  }
  return applyRunSnapshot(next, embedded);
}

/**
 * Check coherence fields that require the current durable state. Transport
 * validation cannot decide whether an embedded run projection belongs to a
 * different immutable pipeline definition.
 */
export function durableFrameCompatibility(
  state: DurableRunState,
  frame: DurableEventFrame,
): RecoveryReason | null {
  if (frame.run_id !== state.runId) {
    return { kind: "incompatible-frame", detail: `foreign run id ${frame.run_id}` };
  }
  const candidate = frame.payload.run;
  if (candidate === undefined) {
    return null;
  }
  const parsed = runSnapshotSchema.safeParse(candidate);
  if (!parsed.success) {
    return frame.event_kind === "run.updated"
      ? { kind: "incompatible-frame", detail: "run.updated payload is malformed" }
      : null;
  }
  const embedded = parsed.data;
  if (embedded.run_id !== frame.run_id) {
    return {
      kind: "incompatible-frame",
      detail: "embedded run id disagrees with envelope run id",
    };
  }
  if (
    embedded !== null &&
    state.run !== null &&
    !hasCompatibleRunDefinition(state.run, embedded)
  ) {
    return {
      kind: "incompatible-frame",
      detail: "embedded run has an incompatible pipeline or runner identity",
    };
  }
  return null;
}

function hasCompatibleRunDefinition(
  current: RunResponse,
  candidate: RunResponse,
): boolean {
  return (
    current.pipeline_id === candidate.pipeline_id &&
    current.pipeline_version === candidate.pipeline_version &&
    current.runner_kind === candidate.runner_kind &&
    current.scenario_seed === candidate.scenario_seed
  );
}

/**
 * Extract a run projection from a durable payload when the event carries
 * one. The payload is untrusted, so the projection is validated with the
 * shared Zod schema and identity-checked against the frame; anything shaped
 * differently is ignored rather than interpreted. The inferred schema type
 * is structurally assignable to the generated `RunResponse`, so no type
 * assertion is involved.
 */
export function embeddedRunSnapshot(frame: DurableEventFrame): RunResponse | null {
  const candidate = frame.payload.run;
  if (typeof candidate !== "object" || candidate === null || Array.isArray(candidate)) {
    return null;
  }
  const parsed = runSnapshotSchema.safeParse(candidate);
  if (!parsed.success || parsed.data.run_id !== frame.run_id) {
    return null;
  }
  return parsed.data;
}

function reject(state: DurableRunState, detail: string): DurableRunState {
  return { ...state, lastError: detail };
}

// ---------------------------------------------------------------------------
// Telemetry slice: deliberately separate state, never merged into durable
// truth. A type error, not a runtime check, keeps these worlds apart.
// ---------------------------------------------------------------------------

export type TelemetryHealth =
  "idle" | "live" | "stale" | "recovering" | "disconnected" | "unavailable";

export interface TelemetrySlice {
  readonly health: TelemetryHealth;
  /** Highest accepted observation timestamp, in microseconds. */
  readonly lastObservedMicros: number;
  /** Cumulative dropped-record counter across sampled batches. */
  readonly droppedTotal: number;
  readonly sampledBatches: number;
  /** Latest accepted records, newest observation first, bounded. */
  readonly records: readonly TelemetryRecord[];
  readonly snapshotObserved: boolean;
}

export const MAX_TELEMETRY_RECORDS = 128;

export function initialTelemetrySlice(): TelemetrySlice {
  return {
    health: "idle",
    lastObservedMicros: 0,
    droppedTotal: 0,
    sampledBatches: 0,
    records: [],
    snapshotObserved: false,
  };
}

function acceptRecords(
  slice: TelemetrySlice,
  records: readonly TelemetryRecord[],
): TelemetrySlice {
  let last = slice.lastObservedMicros;
  const accepted: TelemetryRecord[] = [];
  for (const record of records) {
    // Telemetry must move forward in time; stale or out-of-order
    // observations are dropped without error.
    if (record.observed_at_micros <= last) {
      continue;
    }
    last = record.observed_at_micros;
    accepted.push(record);
  }
  if (accepted.length === 0) {
    return slice;
  }
  return {
    ...slice,
    lastObservedMicros: last,
    records: [...accepted.reverse(), ...slice.records].slice(0, MAX_TELEMETRY_RECORDS),
  };
}

/**
 * Apply the initial telemetry snapshot. The first frame after connect must
 * be the snapshot; later snapshots are treated like any other batch.
 */
export function applyTelemetrySnapshot(
  slice: TelemetrySlice,
  frame: TelemetrySnapshot,
): TelemetrySlice {
  const accepted = acceptRecords(slice, frame.records);
  return { ...accepted, health: "live", snapshotObserved: true };
}

/**
 * Apply one sampled batch. Dropped-record metadata accumulates; sampling
 * and loss never invalidate anything durable. A batch carrying no new
 * records, no drops, and an already-live health is a silent no-op.
 */
export function applyTelemetryBatch(
  slice: TelemetrySlice,
  frame: TelemetryBatch,
): TelemetrySlice {
  const accepted = acceptRecords(slice, frame.records);
  if (accepted === slice && frame.dropped === 0 && slice.health === "live") {
    return slice;
  }
  return {
    ...accepted,
    health: "live",
    droppedTotal: accepted.droppedTotal + frame.dropped,
    sampledBatches: accepted.sampledBatches + 1,
  };
}

export function markTelemetryDisconnected(slice: TelemetrySlice): TelemetrySlice {
  return { ...slice, health: "disconnected" };
}

export function markTelemetryStale(slice: TelemetrySlice): TelemetrySlice {
  return { ...slice, health: "stale" };
}

export function markTelemetryReconnecting(slice: TelemetrySlice): TelemetrySlice {
  return { ...slice, health: "recovering" };
}

export function markTelemetryUnavailable(slice: TelemetrySlice): TelemetrySlice {
  return { ...slice, health: "unavailable" };
}

// ---------------------------------------------------------------------------
// Combined view
// ---------------------------------------------------------------------------

export interface LiveRunView {
  readonly runId: string;
  readonly durable: DurableRunState;
  readonly telemetry: TelemetrySlice;
  /**
   * Derived convenience flags. Telemetry health is reported separately so
   * consumers can never present it as durable execution truth.
   */
  readonly durableReady: boolean;
  readonly durableRecovering: boolean;
  readonly telemetryLive: boolean;
}

export function createLiveRunView(
  durable: DurableRunState,
  telemetry: TelemetrySlice,
): LiveRunView {
  return {
    runId: durable.runId,
    durable,
    telemetry,
    durableReady: durable.status === "ready" && durable.run !== null,
    durableRecovering: durable.status === "recovering",
    telemetryLive: telemetry.health === "live" || telemetry.health === "stale",
  };
}

/** Identity projection used by cache-coherence checks and tests. */
export function snapshotIdentity(snapshot: RunResponse): RunIdentity {
  return {
    run_id: snapshot.run_id,
    run_version: snapshot.run_version,
    state: snapshot.state,
    observed_at: snapshot.observed_at,
  };
}

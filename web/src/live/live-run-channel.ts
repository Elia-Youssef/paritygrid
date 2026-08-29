/**
 * Headless orchestrator binding one run's authoritative query cache, its
 * durable SSE stream, and its advisory telemetry socket into one coherent,
 * externally observable live view.
 *
 * Ownership: one channel instance owns one run. Route or run changes create
 * a new channel and dispose the old one; disposal tears down both
 * transports and stops every update.
 *
 * Coherence: all authoritative writes — prefetches, refetches, mutation
 * results observed through the query cache, and run projections carried by
 * durable events — flow through the version-guarded reducer, so no racing
 * source can regress newer durable state. A durable gap marks the view
 * recovering, refetches authoritative REST state, and resumes the stream
 * from the recovered accepted sequence.
 */
import type { QueryCacheNotifyEvent, QueryClient } from "@tanstack/react-query";

import {
  fetchRun,
  isApiRequestError,
  isRunResponse,
  type RequestLike,
} from "../api/client";
import { queryKeys } from "../api/query-keys";
import {
  DurableRunStream,
  type DurableStreamStatus,
  type RecoveryReason,
  type Scheduler,
  type SseTransport,
} from "./durable-stream";
import {
  TelemetrySocket,
  type TelemetryConnection,
  type TelemetryTransportFactory,
} from "./telemetry-socket";
import {
  applyDurableEvent,
  applyRunSnapshot,
  applyTelemetryBatch,
  applyTelemetrySnapshot,
  createLiveRunView,
  describeReason,
  durableFrameCompatibility,
  embeddedRunSnapshot,
  initialDurableRunState,
  initialTelemetrySlice,
  markError,
  markLoading,
  markRecovering,
  markStale,
  markTelemetryDisconnected,
  markTelemetryReconnecting,
  markTelemetryStale,
  markTelemetryUnavailable,
  type DurableRunState,
  type LiveRunView,
  type TelemetrySlice,
} from "./live-state";
import type { DurableEventFrame } from "./protocol";

export interface ChannelSnapshot {
  readonly view: LiveRunView;
  readonly stream: DurableStreamStatus;
  readonly telemetryConnection: TelemetryConnection;
}

export type RunQueryFn = (
  runId: string,
  init?: RequestLike,
) => ReturnType<typeof fetchRun>;

export interface LiveRunChannelOptions {
  runId: string;
  queryClient: QueryClient;
  sseTransport: SseTransport;
  telemetryTransport: TelemetryTransportFactory;
  /** Injected clock and scheduler for deterministic tests. */
  scheduler?: Scheduler;
  now?: () => number;
  /** Override for the authoritative run query in tests. */
  runQueryFn?: RunQueryFn;
}

export class LiveRunChannel {
  private readonly runId: string;
  private readonly queryClient: QueryClient;
  private readonly runQueryFn: RunQueryFn;
  private readonly stream: DurableRunStream;
  private readonly telemetrySocket: TelemetrySocket;
  private readonly unsubscribeQueryCache: () => void;

  private durable: DurableRunState;
  private telemetry: TelemetrySlice = initialTelemetrySlice();
  private telemetryConnection: TelemetryConnection = { kind: "idle" };
  private snapshot: ChannelSnapshot;
  private readonly listeners = new Set<() => void>();
  private disposed = false;

  constructor(options: LiveRunChannelOptions) {
    this.runId = options.runId;
    this.queryClient = options.queryClient;
    this.runQueryFn = options.runQueryFn ?? fetchRun;
    this.durable = initialDurableRunState(options.runId);

    this.stream = new DurableRunStream({
      runId: options.runId,
      transport: options.sseTransport,
      scheduler: options.scheduler,
      handlers: {
        onFrame: (frame) => this.handleFrame(frame),
        onConnecting: () => {
          // Connection attempts are visible through the stream status.
        },
        onRecovery: (reason, acceptedSequence) =>
          this.handleRecovery(reason, acceptedSequence),
        onExhausted: (reason) => this.handleExhausted(reason),
        onStatus: () => {
          // Mirror every transport transition into the published snapshot.
          this.publish();
        },
      },
    });

    this.telemetrySocket = new TelemetrySocket({
      runId: options.runId,
      transport: options.telemetryTransport,
      scheduler: options.scheduler,
      now: options.now,
      handlers: {
        onSnapshot: (frame) => {
          this.telemetry = applyTelemetrySnapshot(this.telemetry, frame);
          this.publish();
        },
        onBatch: (frame) => {
          this.telemetry = applyTelemetryBatch(this.telemetry, frame);
          this.publish();
        },
        onPong: () => {
          // Liveness is tracked by the socket; a pong changes no state.
        },
        onConnection: (connection) => this.handleTelemetryConnection(connection),
      },
    });

    this.snapshot = this.buildSnapshot();
    this.unsubscribeQueryCache = this.queryClient.getQueryCache().subscribe((event) => {
      this.handleCacheEvent(event);
    });
  }

  getSnapshot = (): ChannelSnapshot => this.snapshot;

  subscribe = (listener: () => void): (() => void) => {
    this.listeners.add(listener);
    return () => {
      this.listeners.delete(listener);
    };
  };

  /** Load authoritative state, then open both live transports. */
  async start(): Promise<void> {
    if (this.disposed) {
      return;
    }
    this.durable = markLoading(this.durable);
    this.publish();
    try {
      const run = await this.queryClient.fetchQuery({
        queryKey: queryKeys.runDetail(this.runId),
        queryFn: ({ signal }) => this.runQueryFn(this.runId, { signal }),
      });
      if (this.disposed) {
        return;
      }
      this.durable = applyRunSnapshot(this.durable, run);
    } catch (error) {
      if (this.disposed) {
        return;
      }
      this.durable = markError(this.durable, describeError(error));
    }
    this.publish();
    this.stream.start();
    this.telemetrySocket.start();
  }

  dispose(): void {
    if (this.disposed) {
      return;
    }
    this.disposed = true;
    this.unsubscribeQueryCache();
    this.stream.dispose();
    this.telemetrySocket.dispose();
    this.telemetry = markTelemetryDisconnected(this.telemetry);
    this.telemetryConnection = {
      kind: "disconnected",
      terminal: true,
      reason: "disposed",
    };
    this.publish();
    this.listeners.clear();
  }

  getDurableState(): DurableRunState {
    return this.durable;
  }

  getTelemetrySlice(): TelemetrySlice {
    return this.telemetry;
  }

  private handleFrame(frame: DurableEventFrame): RecoveryReason | null {
    if (this.disposed) {
      return { kind: "connection-closed" };
    }
    const incompatibility = durableFrameCompatibility(this.durable, frame);
    if (incompatibility !== null) {
      return incompatibility;
    }
    const before = this.durable;
    this.durable = applyDurableEvent(this.durable, frame);
    if (this.durable !== before && embeddedRunSnapshot(frame) !== null) {
      // The event carried a newer authoritative run projection: refresh the
      // cached REST view so queries and durable state converge.
      void this.queryClient.invalidateQueries({
        queryKey: queryKeys.runDetail(this.runId),
        refetchType: "all",
      });
    }
    this.publish();
    return null;
  }

  private async handleRecovery(
    reason: RecoveryReason,
    acceptedSequence: number,
  ): Promise<number> {
    if (this.disposed) {
      return acceptedSequence;
    }
    this.durable = markRecovering(this.durable, reason);
    this.publish();
    try {
      await this.queryClient.invalidateQueries({
        queryKey: queryKeys.runDetail(this.runId),
        refetchType: "all",
      });
    } catch {
      // A failed refetch must not block resuming from the last coherent
      // sequence; its outcome still arrives through the guarded cache
      // subscription.
    }
    return acceptedSequence;
  }

  private handleExhausted(reason: RecoveryReason): void {
    if (this.disposed) {
      return;
    }
    this.durable = markStale(this.durable, describeReason(reason));
    this.publish();
  }

  private handleTelemetryConnection(connection: TelemetryConnection): void {
    if (this.disposed) {
      return;
    }
    this.telemetryConnection = connection;
    this.telemetry = withTelemetryHealth(this.telemetry, connection);
    this.publish();
  }

  private handleCacheEvent(event: QueryCacheNotifyEvent): void {
    if (this.disposed || event.type !== "updated") {
      return;
    }
    if (
      JSON.stringify(event.query.queryKey) !==
      JSON.stringify(queryKeys.runDetail(this.runId))
    ) {
      return;
    }
    const action = event.action;
    const data: unknown = action?.type === "success" ? action.data : undefined;
    if (!isRunResponse(data)) {
      return;
    }
    const before = this.durable;
    this.durable = applyRunSnapshot(this.durable, data);
    if (this.durable !== before) {
      this.publish();
    }
  }

  private buildSnapshot(): ChannelSnapshot {
    return {
      view: createLiveRunView(this.durable, this.telemetry),
      stream: this.stream.getStatus(),
      telemetryConnection: this.telemetryConnection,
    };
  }

  private publish(): void {
    this.snapshot = this.buildSnapshot();
    for (const listener of this.listeners) {
      listener();
    }
  }
}

function withTelemetryHealth(
  slice: TelemetrySlice,
  connection: TelemetryConnection,
): TelemetrySlice {
  switch (connection.kind) {
    case "connecting":
    case "idle":
      return slice.health === "live" ? slice : { ...slice, health: "recovering" };
    case "live":
      return { ...slice, health: "live" };
    case "stale":
      return markTelemetryStale(slice);
    case "recovering":
      return markTelemetryReconnecting(slice);
    case "disconnected":
      return connection.reason === "run-not-found" || connection.reason === "capacity"
        ? markTelemetryUnavailable(slice)
        : markTelemetryDisconnected(slice);
  }
}

function describeError(error: unknown): string {
  if (isApiRequestError(error)) {
    // Only the bounded, parsed Problem view is ever surfaced.
    return error.problem.detail ?? error.problem.title;
  }
  if (error instanceof Error && error.message !== "") {
    return error.message;
  }
  return "authoritative run fetch failed";
}

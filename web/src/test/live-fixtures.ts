/**
 * Deterministic fixtures for live-transport tests. Every asynchronous
 * boundary (timers, streams, sockets) is injected and driven manually, so
 * no test depends on wall-clock timing.
 */
import type {
  SseConnectionResult,
  SseMessage,
  SseTransport,
} from "../live/durable-stream";
import type { Scheduler } from "../live/durable-stream";
import type { TelemetryTransportFactory } from "../live/telemetry-socket";
import type { RunResponse } from "../api/generated/schema";

/** Manually advanced scheduler that doubles as an injected clock. */
export class ManualScheduler implements Scheduler {
  now = 0;
  private counter = 0;
  private tasks = new Map<number, { at: number; fn: () => void; cancelled: boolean }>();

  schedule(fn: () => void, delayMs: number): () => void {
    const id = (this.counter += 1);
    this.tasks.set(id, { at: this.now + delayMs, fn, cancelled: false });
    return () => {
      const task = this.tasks.get(id);
      if (task !== undefined) {
        task.cancelled = true;
      }
    };
  }

  /** Run every task due within the next `ms` milliseconds, in order. */
  advance(ms: number): void {
    const horizon = this.now + ms;
    for (;;) {
      const due = [...this.tasks.entries()]
        .filter(([, task]) => !task.cancelled && task.at <= horizon)
        .sort((a, b) => a[1].at - b[1].at || a[0] - b[0])[0];
      const entry = due === undefined ? undefined : this.tasks.get(due[0]);
      if (due === undefined || entry === undefined) {
        break;
      }
      this.tasks.delete(due[0]);
      this.now = Math.max(this.now, entry.at);
      entry.fn();
    }
    this.now = horizon;
  }

  pendingCount(): number {
    return [...this.tasks.values()].filter((task) => !task.cancelled).length;
  }
}

type SseCallbacks = Parameters<SseTransport>[1];

/** Controllable SSE connection the test drives explicitly. */
export class FakeSseConnection {
  readonly url: string;
  readonly callbacks: SseCallbacks;
  private resolveCurrent: ((result: SseConnectionResult) => void) | null = null;
  private closed = false;

  constructor(url: string, callbacks: SseCallbacks) {
    this.url = url;
    this.callbacks = callbacks;
  }

  emit(message: SseMessage): void {
    if (!this.closed) {
      this.callbacks.onEvent(message);
    }
  }

  emitData(
    jsonText: string,
    eventName: string | null = null,
    id: string | null = null,
  ): void {
    this.emit({ id, event: eventName, data: jsonText });
  }

  end(result: SseConnectionResult): void {
    this.closed = true;
    this.resolveCurrent?.(result);
    this.resolveCurrent = null;
  }

  abort(): void {
    this.closed = true;
    this.resolveCurrent?.({ outcome: "completed" });
    this.resolveCurrent = null;
  }

  isSettled(): boolean {
    return this.resolveCurrent === null;
  }

  static transport(): {
    transport: SseTransport;
    connections: FakeSseConnection[];
  } {
    const connections: FakeSseConnection[] = [];
    const impl: SseTransport = (url, callbacks) => {
      const connection = new FakeSseConnection(url, callbacks);
      connections.push(connection);
      return new Promise<SseConnectionResult>((resolve) => {
        connection.resolveCurrent = resolve;
      });
    };
    return { transport: impl, connections };
  }
}

type WsCallbacks = Parameters<TelemetryTransportFactory>[1];

/** Controllable WebSocket handle the test drives explicitly. */
export class FakeTelemetrySocket {
  readonly url: string;
  readonly callbacks: WsCallbacks;
  readonly sent: string[] = [];
  closedWith: number | null = null;

  constructor(url: string, callbacks: WsCallbacks) {
    this.url = url;
    this.callbacks = callbacks;
  }

  open(): void {
    this.callbacks.onOpen();
  }

  message(text: string): void {
    this.callbacks.onMessage(text);
  }

  close(code: number): void {
    if (this.closedWith === null) {
      this.closedWith = code;
      this.callbacks.onClose({ code });
    }
  }

  static factory(): {
    factory: TelemetryTransportFactory;
    sockets: FakeTelemetrySocket[];
  } {
    const sockets: FakeTelemetrySocket[] = [];
    const impl: TelemetryTransportFactory = (url, callbacks) => {
      const socket = new FakeTelemetrySocket(url, callbacks);
      sockets.push(socket);
      return {
        send: (text: string) => {
          socket.sent.push(text);
        },
        close: () => {
          socket.closedWith ??= -1;
        },
      };
    };
    return { factory: impl, sockets };
  }
}

// ---------------------------------------------------------------------------
// Wire-shape builders
// ---------------------------------------------------------------------------

export const RUN_ID = "run-live-1";

export function runResponse(
  version: number,
  overrides: Partial<RunResponse> = {},
): RunResponse {
  return {
    run_id: RUN_ID,
    run_version: version,
    state: version >= 3 ? "completed" : version === 2 ? "running" : "new",
    observed_at: `2026-01-01T00:00:0${String(Math.min(version, 9))}Z`,
    created_at: "2026-01-01T00:00:00Z",
    started_at: version >= 2 ? "2026-01-01T00:00:01Z" : null,
    finished_at: version >= 3 ? "2026-01-01T00:00:09Z" : null,
    cancellation_requested_at: null,
    pipeline_id: "pipeline-1",
    pipeline_version: 1,
    runner_kind: "sequential",
    scenario_seed: 7,
    ...overrides,
  };
}

export interface DurableFrameOverrides {
  run_id?: string;
  event_kind?: string;
  subject_kind?: string;
  subject_id?: string;
  occurred_at?: string;
  payload_schema_version?: number;
  payload?: Record<string, unknown>;
  channel?: string;
  schema_version?: number;
  sequence?: number | string;
}

export function durableFrameJson(
  sequence: number,
  overrides: DurableFrameOverrides = {},
): string {
  return JSON.stringify({
    schema_version: 1,
    channel: "durable-events",
    sequence,
    run_id: RUN_ID,
    event_kind: "run.progressed",
    subject_kind: "run",
    subject_id: RUN_ID,
    occurred_at: "2026-01-01T00:00:02Z",
    payload_schema_version: 1,
    payload: {},
    ...overrides,
  });
}

export function runCarryingFrameJson(
  sequence: number,
  run: RunResponse,
  overrides: DurableFrameOverrides = {},
): string {
  return durableFrameJson(sequence, {
    event_kind: "run.updated",
    payload: { run },
    ...overrides,
  });
}

export function telemetryRecordJson(
  observedAtMicros: number,
  overrides: Record<string, unknown> = {},
): Record<string, unknown> {
  return {
    schema_version: 1,
    observed_at_micros: observedAtMicros,
    run_id: RUN_ID,
    metrics: [{ name: "queue.depth", kind: "gauge", value: 3, labels: { node: "n1" } }],
    ...overrides,
  };
}

export function snapshotJson(
  records: Record<string, unknown>[] = [telemetryRecordJson(1_000)],
): string {
  return JSON.stringify({
    schema_version: 1,
    channel: "telemetry",
    advisory: true,
    records,
  });
}

export function batchJson(records: Record<string, unknown>[], dropped = 0): string {
  return JSON.stringify({
    schema_version: 1,
    channel: "telemetry",
    advisory: true,
    sampled: true,
    dropped,
    records,
  });
}

export const pongJson = JSON.stringify({
  schema_version: 1,
  channel: "telemetry",
  type: "pong",
});

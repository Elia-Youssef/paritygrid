/**
 * Advisory telemetry WebSocket client for `/api/v1/live/runs/{run_id}`.
 *
 * Telemetry is disposable, lossy, and never authoritative: this client
 * keeps it in its own state slice and can never touch durable run truth.
 * Only strictly validated frames are accepted — the initial snapshot,
 * sampled batches with dropped-record metadata, and pong replies; anything
 * else is rejected without changing state. Observations must advance in
 * time; stale or out-of-order records are dropped. Disconnects and
 * staleness change only the telemetry connection state.
 *
 * One socket per owner, bounded deterministic reconnects, and complete
 * cleanup: after disposal no listener, timer, or socket callback runs.
 */
import {
  parseTelemetryMessage,
  type TelemetryBatch,
  type TelemetryPong,
  type TelemetrySnapshot,
} from "./protocol";
import type { Scheduler } from "./durable-stream";
import { setTimeoutScheduler } from "./durable-stream";

export type TelemetryConnection =
  | { kind: "idle" }
  | { kind: "connecting" }
  | { kind: "live" }
  | { kind: "stale" }
  | { kind: "recovering"; attempt: number }
  | { kind: "disconnected"; terminal: boolean; reason: TelemetryCloseReason };

export type TelemetryCloseReason =
  | "run-not-found"
  | "capacity"
  | "policy"
  | "attempts-exhausted"
  | "connection-closed"
  | "disposed";

export interface TelemetryFrameHandlers {
  onSnapshot: (frame: TelemetrySnapshot, receivedAtMs: number) => void;
  onBatch: (frame: TelemetryBatch, receivedAtMs: number) => void;
  onPong: (frame: TelemetryPong, receivedAtMs: number) => void;
  onConnection: (connection: TelemetryConnection) => void;
}

export interface TelemetrySocketOptions {
  runId: string;
  handlers: TelemetryFrameHandlers;
  transport: TelemetryTransportFactory;
  scheduler?: Scheduler;
  now?: () => number;
  /** Liveness bound: frames older than this mark the connection stale. */
  staleAfterMs?: number;
  /** Interval between keepalive pings and staleness checks. */
  keepAliveIntervalMs?: number;
  maxAttempts?: number;
  backoffDelay?: (attempt: number) => number;
}

/**
 * Opens one WebSocket and delivers parsed JSON text messages. Factory form
 * so tests inject deterministic fake sockets; the production factory wraps
 * the browser WebSocket.
 */
export type TelemetryTransportFactory = (
  url: string,
  callbacks: {
    signal: AbortSignal;
    onOpen: () => void;
    onMessage: (text: string) => void;
    onClose: (info: { code: number }) => void;
    onError: () => void;
  },
) => { send: (text: string) => void; close: () => void };

export const TELEMETRY_BACKOFF_BASE_MS = 500;
export const TELEMETRY_BACKOFF_MAX_MS = 4_000;
export const DEFAULT_STALE_AFTER_MS = 10_000;
export const DEFAULT_KEEP_ALIVE_MS = 5_000;
export const RUN_NOT_FOUND_CLOSE = 4404;
export const CAPACITY_CLOSE = 1013;

export function telemetryBackoffDelay(attempt: number): number {
  return Math.min(
    TELEMETRY_BACKOFF_BASE_MS * 2 ** (attempt - 1),
    TELEMETRY_BACKOFF_MAX_MS,
  );
}

/** Production transport backed by the browser WebSocket. */
export function createBrowserTelemetryTransport(): TelemetryTransportFactory {
  return (url, callbacks) => {
    const socket = new WebSocket(url);
    const onAbort = () => {
      socket.close();
    };
    callbacks.signal.addEventListener("abort", onAbort, { once: true });
    socket.onopen = () => callbacks.onOpen();
    socket.onmessage = (event) => {
      if (typeof event.data === "string") {
        callbacks.onMessage(event.data);
      }
    };
    socket.onclose = (event) => {
      callbacks.signal.removeEventListener("abort", onAbort);
      callbacks.onClose({ code: event.code });
    };
    socket.onerror = () => callbacks.onError();
    return {
      send: (text) => {
        if (socket.readyState === WebSocket.OPEN) {
          socket.send(text);
        }
      },
      close: () => {
        callbacks.signal.removeEventListener("abort", onAbort);
        socket.close();
      },
    };
  };
}

export class TelemetrySocket {
  private readonly runId: string;
  private readonly handlers: TelemetryFrameHandlers;
  private readonly transport: TelemetryTransportFactory;
  private readonly scheduler: Scheduler;
  private readonly now: () => number;
  private readonly staleAfterMs: number;
  private readonly keepAliveIntervalMs: number;
  private readonly maxAttempts: number;
  private readonly backoffDelay: (attempt: number) => number;

  private disposed = false;
  private open = false;
  private attemptsUsed = 0;
  private connection: TelemetryConnection = { kind: "idle" };
  private lastFrameAt: number | null = null;
  private snapshotReceived = false;
  private socket: { send: (text: string) => void; close: () => void } | null = null;
  private abortController: AbortController | null = null;
  private keepAliveCancel: (() => void) | null = null;
  private reconnectCancel: (() => void) | null = null;

  constructor(options: TelemetrySocketOptions) {
    this.runId = options.runId;
    this.handlers = options.handlers;
    this.transport = options.transport;
    this.scheduler = options.scheduler ?? setTimeoutScheduler;
    this.now = options.now ?? (() => Date.now());
    this.staleAfterMs = options.staleAfterMs ?? DEFAULT_STALE_AFTER_MS;
    this.keepAliveIntervalMs = options.keepAliveIntervalMs ?? DEFAULT_KEEP_ALIVE_MS;
    this.maxAttempts = options.maxAttempts ?? 5;
    this.backoffDelay = options.backoffDelay ?? telemetryBackoffDelay;
  }

  getConnection(): TelemetryConnection {
    return this.connection;
  }

  /** Open the socket. A second start while active is a no-op. */
  start(): void {
    if (this.disposed || this.open || this.abortController !== null) {
      return;
    }
    this.attemptsUsed = 0;
    this.connect();
  }

  dispose(): void {
    if (this.disposed) {
      return;
    }
    this.disposed = true;
    this.open = false;
    this.abortController?.abort();
    this.abortController = null;
    this.socket?.close();
    this.socket = null;
    this.stopTimers();
    // Applied directly: the guarded setter is already silenced by disposal,
    // but observers still learn the terminal state.
    this.connection = {
      kind: "disconnected",
      terminal: true,
      reason: "disposed",
    };
    this.handlers.onConnection(this.connection);
  }

  private stopTimers(): void {
    this.keepAliveCancel?.();
    this.keepAliveCancel = null;
    this.reconnectCancel?.();
    this.reconnectCancel = null;
  }

  /** Re-evaluate staleness against the injected clock. */
  checkStaleness(): void {
    if (this.disposed || !this.open || this.lastFrameAt === null) {
      return;
    }
    if (this.now() - this.lastFrameAt > this.staleAfterMs) {
      this.setConnection({ kind: "stale" });
    }
  }

  private setConnection(connection: TelemetryConnection): void {
    if (this.disposed) {
      return;
    }
    this.connection = connection;
    this.handlers.onConnection(connection);
  }

  private connect(): void {
    if (this.disposed || this.open || this.abortController !== null) {
      return;
    }
    const controller = new AbortController();
    this.abortController = controller;
    this.setConnection({ kind: "connecting" });
    const handle = this.transport(
      `/api/v1/live/runs/${encodeURIComponent(this.runId)}`,
      {
        signal: controller.signal,
        onOpen: () => {
          if (this.disposed) {
            return;
          }
          this.open = true;
          this.snapshotReceived = false;
          this.lastFrameAt = this.now();
          this.setConnection({ kind: "live" });
          this.startKeepAlive();
        },
        onMessage: (text) => {
          if (this.disposed || !this.open) {
            return;
          }
          this.handleMessage(text);
        },
        onClose: (info) => {
          if (this.disposed) {
            return;
          }
          this.open = false;
          this.snapshotReceived = false;
          this.abortController = null;
          this.stopKeepAlive();
          this.handleClose(info.code);
        },
        onError: () => {
          if (this.disposed || !this.open) {
            return;
          }
          // Errors are followed by a close event; nothing to do here beyond
          // treating the connection as no longer live.
          this.open = false;
          this.stopKeepAlive();
        },
      },
    );
    this.socket = handle;
  }

  private handleMessage(text: string): void {
    const receivedAt = this.now();
    const parsed = parseTelemetryMessage(text);
    if (!parsed.ok) {
      // Invalid frames are rejected whole: telemetry is disposable and a
      // malformed frame never justifies touching any state beyond metrics.
      return;
    }
    if (!this.snapshotReceived && parsed.message.kind !== "snapshot") {
      // The server contract requires an authoritative advisory snapshot as
      // the first frame of every connection. Premature batches/pongs cannot
      // initialize freshness or state.
      return;
    }
    if (
      (parsed.message.kind === "snapshot" || parsed.message.kind === "batch") &&
      !parsed.message.frame.records.every((record) => record.run_id === this.runId)
    ) {
      // A frame that carries another run's observations is rejected whole —
      // never partially applied.
      return;
    }
    this.lastFrameAt = receivedAt;
    if (parsed.message.kind === "snapshot") {
      this.snapshotReceived = true;
    }
    if (this.connection.kind === "stale") {
      // A validated frame proves the channel is delivering again.
      this.setConnection({ kind: "live" });
    }
    switch (parsed.message.kind) {
      case "snapshot":
        this.handlers.onSnapshot(parsed.message.frame, receivedAt);
        break;
      case "batch":
        this.handlers.onBatch(parsed.message.frame, receivedAt);
        break;
      case "pong":
        this.handlers.onPong(parsed.message.frame, receivedAt);
        break;
    }
  }

  private handleClose(code: number): void {
    if (code === RUN_NOT_FOUND_CLOSE || code === CAPACITY_CLOSE) {
      // Terminal: an unknown run or a saturated channel will not improve by
      // reconnecting.
      this.setConnection({
        kind: "disconnected",
        terminal: true,
        reason: code === RUN_NOT_FOUND_CLOSE ? "run-not-found" : "capacity",
      });
      return;
    }
    this.attemptsUsed += 1;
    if (this.attemptsUsed >= this.maxAttempts) {
      this.setConnection({
        kind: "disconnected",
        terminal: true,
        reason: "attempts-exhausted",
      });
      return;
    }
    const attempt = this.attemptsUsed;
    this.setConnection({ kind: "recovering", attempt });
    const delay = this.backoffDelay(attempt);
    this.reconnectCancel?.();
    this.reconnectCancel = this.scheduler.schedule(() => {
      this.reconnectCancel = null;
      if (this.disposed) {
        return;
      }
      this.connect();
    }, delay);
  }

  private startKeepAlive(): void {
    this.stopKeepAlive();
    const tick = (): void => {
      this.keepAliveCancel = null;
      if (this.disposed || !this.open) {
        return;
      }
      this.socket?.send(JSON.stringify({ type: "ping" }));
      this.checkStaleness();
      this.keepAliveCancel = this.scheduler.schedule(tick, this.keepAliveIntervalMs);
    };
    this.keepAliveCancel = this.scheduler.schedule(tick, this.keepAliveIntervalMs);
  }

  private stopKeepAlive(): void {
    this.keepAliveCancel?.();
    this.keepAliveCancel = null;
  }
}

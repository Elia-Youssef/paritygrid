/**
 * Durable SSE client for `GET /api/v1/stream/runs/{run_id}`.
 *
 * Ownership rules enforced here:
 * - Resume uses the `after` query parameter only (never `Last-Event-ID` —
 *   the server rejects supplying both).
 * - Delivery must be exactly contiguous: the next accepted sequence equals
 *   the previous accepted sequence plus one. Already-applied or older
 *   events are ignored without regressing state; gaps are never skipped.
 * - Malformed envelopes, incompatible versions, foreign run ids, transport
 *   failures, and `409 stream_sequence_ahead` are explicit recovery
 *   conditions: the last coherent view is retained, the recovery callback
 *   refetches authoritative state, and the stream resumes from the
 *   recovered sequence only.
 * - Reconnection is bounded and deterministic: an injected scheduler drives
 *   exponential backoff, attempts are counted across recovery cycles, and
 *   exhaustion lands in a terminal disconnected state until an explicit
 *   restart.
 * - Disposal aborts the in-flight request, cancels timers, and silences
 *   every handler: no callback runs after disposal.
 */
import { createSseParser } from "./sse-parser";
import { parseDurableFrame, type DurableEventFrame } from "./protocol";
import { parseProblemDetails } from "../lib/problem-details";

export type RecoveryReason =
  | { kind: "sequence-gap"; expected: number; received: number }
  | { kind: "malformed-frame"; detail: string }
  | { kind: "incompatible-frame"; detail: string }
  | { kind: "stream-ahead"; code: string }
  | { kind: "resume-rejected"; code: string }
  | { kind: "run-not-found" }
  | { kind: "connection-closed" };

export interface DurableStreamHandlers {
  /**
   * Validate and apply a contiguous durable frame. Returning a recovery
   * reason rejects it before the transport advances its accepted sequence.
   */
  onFrame: (frame: DurableEventFrame) => RecoveryReason | null | void;
  /** A connection attempt is starting from the given resume position. */
  onConnecting: (resumeFrom: number) => void;
  /**
   * A recovery condition was hit. The handler must refetch authoritative
   * state and resolve with the sequence to resume from. The stream pauses
   * (last coherent state retained) until the promise settles.
   */
  onRecovery: (reason: RecoveryReason, acceptedSequence: number) => Promise<number>;
  /** Reconnect attempts were exhausted or the run is unknowable. */
  onExhausted: (reason: RecoveryReason, attemptsUsed: number) => void;
  /** Any status transition, so owners can mirror the transport state. */
  onStatus?: (status: DurableStreamStatus) => void;
}

export interface DurableStreamStatus {
  kind: "idle" | "connecting" | "live" | "recovering" | "disconnected";
  acceptedSequence: number;
  resumeFrom: number;
  attemptsUsed: number;
  reason?: RecoveryReason;
}

export interface Scheduler {
  /** Schedule `fn` after `delayMs`; return a canceller. */
  schedule: (fn: () => void, delayMs: number) => () => void;
}

export const setTimeoutScheduler: Scheduler = {
  schedule: (fn, delayMs) => {
    const handle = setTimeout(fn, delayMs);
    return () => clearTimeout(handle);
  },
};

export interface SseMessage {
  id: string | null;
  event: string | null;
  data: string;
}

export interface SseConnectionResult {
  outcome: "completed" | "http-error";
  status?: number;
  problemCode?: string;
}

/**
 * Transport abstraction over one SSE connection. Implementations parse the
 * byte stream and deliver complete events; they resolve when the connection
 * ends and report the terminal HTTP status with the Problem `code` when the
 * server rejected the resume.
 */
export type SseTransport = (
  url: string,
  callbacks: {
    signal: AbortSignal;
    onEvent: (message: SseMessage) => void;
  },
) => Promise<SseConnectionResult>;

export const BACKOFF_BASE_MS = 500;
export const BACKOFF_MAX_MS = 8_000;
export const DEFAULT_MAX_ATTEMPTS = 5;
const MAX_ERROR_BODY_CHARS = 8_192;

/** Deterministic exponential delay for the nth (1-based) reconnect. */
export function defaultBackoffDelay(attempt: number): number {
  return Math.min(BACKOFF_BASE_MS * 2 ** (attempt - 1), BACKOFF_MAX_MS);
}

/**
 * Production transport: fetch-based SSE over a bounded decoded stream. A
 * hand-rolled transport is required because EventSource cannot surface the
 * status or Problem body of a rejected resume and owns a hidden reconnect
 * loop that would bypass the bounded policy.
 */
export function createFetchSseTransport(): SseTransport {
  return async (url, callbacks) => {
    const response = await fetch(url, {
      cache: "no-store",
      headers: { Accept: "text/event-stream" },
      signal: callbacks.signal,
    });
    if (!response.ok) {
      let problemCode: string | undefined;
      try {
        const bodyText = (await response.text()).slice(0, MAX_ERROR_BODY_CHARS);
        const problem = parseProblemDetails(JSON.parse(bodyText) as unknown);
        problemCode = problem.extensions.find(
          (member) => member.name === "code",
        )?.value;
      } catch {
        problemCode = undefined;
      }
      return { outcome: "http-error", status: response.status, problemCode };
    }
    const body = response.body;
    if (body === null) {
      return { outcome: "completed" };
    }
    const reader = body.getReader();
    const decoder = new TextDecoder();
    const parser = createSseParser();
    try {
      for (;;) {
        const { done, value } = await reader.read();
        if (done) {
          break;
        }
        for (const message of parser.push(decoder.decode(value, { stream: true }))) {
          callbacks.onEvent(message);
        }
      }
      for (const message of parser.flush()) {
        callbacks.onEvent(message);
      }
    } finally {
      reader.releaseLock();
    }
    return { outcome: "completed" };
  };
}

export interface DurableStreamOptions {
  runId: string;
  handlers: DurableStreamHandlers;
  transport: SseTransport;
  scheduler?: Scheduler;
  /** Deterministic reconnect delay for the nth (1-based) attempt. */
  backoffDelay?: (attempt: number) => number;
  maxAttempts?: number;
  /** Sequence to start from; defaults to full replay (`after=0`). */
  initialAfter?: number;
}

export class DurableRunStream {
  private readonly runId: string;
  private readonly handlers: DurableStreamHandlers;
  private readonly transport: SseTransport;
  private readonly scheduler: Scheduler;
  private readonly backoffDelay: (attempt: number) => number;
  private readonly maxAttempts: number;

  private acceptedSequence: number;
  private attemptsUsed = 0;
  private disposed = false;
  private connecting = false;
  private abortController: AbortController | null = null;
  private cancelTimer: (() => void) | null = null;
  private status: DurableStreamStatus;

  constructor(options: DurableStreamOptions) {
    this.runId = options.runId;
    this.handlers = options.handlers;
    this.transport = options.transport;
    this.scheduler = options.scheduler ?? setTimeoutScheduler;
    this.backoffDelay = options.backoffDelay ?? defaultBackoffDelay;
    this.maxAttempts = options.maxAttempts ?? DEFAULT_MAX_ATTEMPTS;
    this.acceptedSequence = options.initialAfter ?? 0;
    this.status = {
      kind: "idle",
      acceptedSequence: this.acceptedSequence,
      resumeFrom: this.acceptedSequence,
      attemptsUsed: 0,
    };
  }

  getStatus(): DurableStreamStatus {
    return this.status;
  }

  getAcceptedSequence(): number {
    return this.acceptedSequence;
  }

  /** Begin (or explicitly restart) the stream from the accepted sequence. */
  start(): void {
    if (this.disposed || this.connecting) {
      return;
    }
    this.attemptsUsed = 0;
    this.cancelTimer?.();
    this.cancelTimer = null;
    void this.connect();
  }

  /** Resume from an explicit sequence after authoritative recovery. */
  resumeFrom(sequence: number): void {
    if (this.disposed || this.connecting) {
      return;
    }
    this.acceptedSequence = sequence;
    this.status = {
      kind: "idle",
      acceptedSequence: sequence,
      resumeFrom: sequence,
      attemptsUsed: 0,
      reason: undefined,
    };
    this.cancelTimer?.();
    this.cancelTimer = null;
    void this.connect();
  }

  /** Abort the connection, cancel timers, and silence every handler. */
  dispose(): void {
    this.disposed = true;
    this.abortController?.abort();
    this.abortController = null;
    this.cancelTimer?.();
    this.cancelTimer = null;
    this.connecting = false;
    this.status = { ...this.status, kind: "disconnected", reason: undefined };
  }

  private setStatus(patch: Partial<DurableStreamStatus>): void {
    this.status = { ...this.status, ...patch };
    this.handlers.onStatus?.(this.status);
  }

  private async connect(): Promise<void> {
    if (this.disposed || this.connecting) {
      return;
    }
    this.connecting = true;
    const controller = new AbortController();
    this.abortController = controller;
    const resumeFrom = this.acceptedSequence;
    this.setStatus({ kind: "connecting", resumeFrom, reason: undefined });
    this.handlers.onConnecting(resumeFrom);

    const url = `/api/v1/stream/runs/${encodeURIComponent(this.runId)}?after=${resumeFrom}`;
    let result: SseConnectionResult;
    try {
      result = await this.transport(url, {
        signal: controller.signal,
        onEvent: (message) => {
          this.handleMessage(message);
        },
      });
    } catch {
      result = { outcome: "completed" };
    }
    this.connecting = false;
    this.abortController = null;
    if (this.disposed) {
      return;
    }
    this.handleConnectionEnd(result);
  }

  private handleMessage(message: SseMessage): void {
    if (this.disposed || this.status.kind === "recovering") {
      return;
    }
    let parsedJson: unknown;
    try {
      parsedJson = JSON.parse(message.data) as unknown;
    } catch {
      void this.enterRecovery({
        kind: "malformed-frame",
        detail: "data is not valid JSON",
      });
      return;
    }

    const frameResult = parseDurableFrame(parsedJson);
    if (!frameResult.ok) {
      void this.enterRecovery({
        kind: "malformed-frame",
        detail: `envelope rejected (${frameResult.reason})`,
      });
      return;
    }

    const frame = frameResult.frame;
    if (frame.run_id !== this.runId) {
      void this.enterRecovery({
        kind: "incompatible-frame",
        detail: `foreign run id ${frame.run_id}`,
      });
      return;
    }
    if (frame.subject_kind === "run" && frame.subject_id !== frame.run_id) {
      void this.enterRecovery({
        kind: "incompatible-frame",
        detail: "run subject id disagrees with envelope run id",
      });
      return;
    }
    if (message.event !== null && message.event !== frame.event_kind) {
      void this.enterRecovery({
        kind: "incompatible-frame",
        detail: "SSE event name disagrees with envelope event_kind",
      });
      return;
    }
    if (message.id !== null && message.id !== String(frame.sequence)) {
      void this.enterRecovery({
        kind: "incompatible-frame",
        detail: "SSE id disagrees with envelope sequence",
      });
      return;
    }

    const expected = this.acceptedSequence + 1;
    if (frame.sequence <= this.acceptedSequence) {
      // Already-applied or older compatible event: ignore, never regress.
      return;
    }
    if (frame.sequence > expected) {
      void this.enterRecovery({
        kind: "sequence-gap",
        expected,
        received: frame.sequence,
      });
      return;
    }

    const rejected = this.handlers.onFrame(frame);
    if (rejected !== undefined && rejected !== null) {
      void this.enterRecovery(rejected);
      return;
    }
    this.acceptedSequence = frame.sequence;
    this.setStatus({ kind: "live", acceptedSequence: frame.sequence });
  }

  private handleConnectionEnd(result: SseConnectionResult): void {
    if (this.disposed) {
      return;
    }
    if (result.outcome === "http-error") {
      if (result.status === 409) {
        // The resume position is ahead of the durable frontier. Durable
        // history is never pruned, so the coherent recovery restarts the
        // replay from the beginning; the reducer ignores older events.
        void this.enterRecovery(
          { kind: "stream-ahead", code: result.problemCode ?? "stream_sequence_ahead" },
          { resumeFromZero: true },
        );
        return;
      }
      if (result.status === 404) {
        // An unknown run can never stream; report once, do not churn.
        const reason: RecoveryReason = { kind: "run-not-found" };
        this.setStatus({ kind: "disconnected", reason });
        this.handlers.onExhausted(reason, this.attemptsUsed);
        return;
      }
      if (result.status === 400) {
        void this.enterRecovery(
          {
            kind: "resume-rejected",
            code: result.problemCode ?? "invalid_resume_position",
          },
          { resumeFromZero: true },
        );
        return;
      }
    }
    // Clean close or transient server failure: bounded reconnect from the
    // accepted sequence.
    this.scheduleReconnect({ kind: "connection-closed" });
  }

  private async enterRecovery(
    reason: RecoveryReason,
    options: { resumeFromZero?: boolean } = {},
  ): Promise<void> {
    if (this.disposed) {
      return;
    }
    this.abortController?.abort();
    this.abortController = null;
    this.connecting = false;
    this.attemptsUsed += 1;
    if (this.attemptsUsed >= this.maxAttempts) {
      this.setStatus({ kind: "disconnected", reason });
      this.handlers.onExhausted(reason, this.attemptsUsed);
      return;
    }
    this.setStatus({ kind: "recovering", reason });
    try {
      const resumeSequence = await this.handlers.onRecovery(
        reason,
        this.acceptedSequence,
      );
      if (this.disposed) {
        return;
      }
      const target = options.resumeFromZero === true ? 0 : resumeSequence;
      this.acceptedSequence = target;
      this.setStatus({ acceptedSequence: target, resumeFrom: target });
      void this.connect();
    } catch {
      if (this.disposed) {
        return;
      }
      this.scheduleReconnect(reason);
    }
  }

  private scheduleReconnect(reason: RecoveryReason): void {
    if (this.disposed) {
      return;
    }
    this.attemptsUsed += 1;
    if (this.attemptsUsed >= this.maxAttempts) {
      this.setStatus({ kind: "disconnected", reason });
      this.handlers.onExhausted(reason, this.attemptsUsed);
      return;
    }
    this.setStatus({ kind: "recovering", reason });
    const delay = this.backoffDelay(this.attemptsUsed);
    this.cancelTimer?.();
    this.cancelTimer = this.scheduler.schedule(() => {
      this.cancelTimer = null;
      if (this.disposed) {
        return;
      }
      void this.connect();
    }, delay);
  }
}

import { describe, expect, it, vi } from "vitest";

import {
  BACKOFF_MAX_MS,
  defaultBackoffDelay,
  DurableRunStream,
  type DurableStreamHandlers,
  type RecoveryReason,
  type SseTransport,
} from "./durable-stream";
import {
  durableFrameJson,
  FakeSseConnection,
  ManualScheduler,
  runCarryingFrameJson,
  runResponse,
  RUN_ID,
} from "../test/live-fixtures";

interface Recorded {
  frames: number[];
  connecting: number[];
  recoveries: { reason: RecoveryReason; accepted: number }[];
  exhausted: { reason: RecoveryReason; attempts: number }[];
}

function recordingHandlers(delegates: Partial<DurableStreamHandlers> = {}): {
  recorded: Recorded;
  handlers: DurableStreamHandlers;
} {
  const recorded: Recorded = {
    frames: [],
    connecting: [],
    recoveries: [],
    exhausted: [],
  };
  const handlers: DurableStreamHandlers = {
    onFrame: (frame) => {
      recorded.frames.push(frame.sequence);
      delegates.onFrame?.(frame);
    },
    onConnecting: (resumeFrom) => {
      recorded.connecting.push(resumeFrom);
      delegates.onConnecting?.(resumeFrom);
    },
    onRecovery: (reason, acceptedSequence) => {
      recorded.recoveries.push({ reason, accepted: acceptedSequence });
      return (
        delegates.onRecovery?.(reason, acceptedSequence) ??
        Promise.resolve(acceptedSequence)
      );
    },
    onExhausted: (reason, attemptsUsed) => {
      recorded.exhausted.push({ reason, attempts: attemptsUsed });
      delegates.onExhausted?.(reason, attemptsUsed);
    },
  };
  return { recorded, handlers };
}

/** Drain the microtask queue so resolved transport promises run. */
async function flushMicrotasks(): Promise<void> {
  for (let tick = 0; tick < 10; tick += 1) {
    await Promise.resolve();
  }
}

describe("durable run stream", () => {
  it("connects initially from after=0 and goes live on contiguous frames", () => {
    const { transport, connections } = FakeSseConnection.transport();
    const { recorded, handlers } = recordingHandlers();
    const stream = new DurableRunStream({
      runId: RUN_ID,
      handlers,
      transport,
      scheduler: new ManualScheduler(),
    });
    stream.start();

    expect(connections).toHaveLength(1);
    expect(connections[0]?.url).toBe(`/api/v1/stream/runs/${RUN_ID}?after=0`);
    expect(recorded.connecting).toEqual([0]);

    connections[0]?.emitData(durableFrameJson(1));
    connections[0]?.emitData(durableFrameJson(2));
    connections[0]?.emitData(durableFrameJson(3));
    expect(recorded.frames).toEqual([1, 2, 3]);
    expect(stream.getAcceptedSequence()).toBe(3);
    expect(stream.getStatus()).toMatchObject({ kind: "live", acceptedSequence: 3 });
  });

  it("ignores duplicate and older frames without regressing the sequence", () => {
    const { transport, connections } = FakeSseConnection.transport();
    const { handlers } = recordingHandlers();
    const stream = new DurableRunStream({
      runId: RUN_ID,
      handlers,
      transport,
      scheduler: new ManualScheduler(),
    });
    stream.start();
    connections[0]?.emitData(durableFrameJson(1));
    connections[0]?.emitData(durableFrameJson(2));

    connections[0]?.emitData(durableFrameJson(2));
    connections[0]?.emitData(durableFrameJson(1));

    expect(stream.getAcceptedSequence()).toBe(2);
    expect(stream.getStatus().kind).toBe("live");
  });

  it("treats a sequence gap as recovery and resumes from the recovered sequence", async () => {
    const { transport, connections } = FakeSseConnection.transport();
    const scheduler = new ManualScheduler();
    const { recorded, handlers } = recordingHandlers({
      onRecovery: () => Promise.resolve(2),
    });
    const stream = new DurableRunStream({
      runId: RUN_ID,
      handlers,
      transport,
      scheduler,
    });
    stream.start();

    connections[0]?.emitData(durableFrameJson(1));
    connections[0]?.emitData(durableFrameJson(3));

    expect(recorded.frames).toEqual([1]);
    expect(recorded.recoveries).toHaveLength(1);
    expect(recorded.recoveries[0]?.reason).toEqual({
      kind: "sequence-gap",
      expected: 2,
      received: 3,
    });
    expect(stream.getStatus()).toMatchObject({ kind: "recovering" });
    // The failed connection was aborted during recovery.
    expect(connections[0]?.callbacks.signal.aborted).toBe(true);

    await flushMicrotasks();
    expect(connections).toHaveLength(2);
    expect(connections[1]?.url).toBe(`/api/v1/stream/runs/${RUN_ID}?after=2`);
    connections[1]?.emitData(durableFrameJson(3));
    expect(recorded.frames).toEqual([1, 3]);
    expect(stream.getAcceptedSequence()).toBe(3);
  });

  it("does not spend retry budget when an aborted connection settles during recovery", async () => {
    const scheduler = new ManualScheduler();
    const callbacks: Parameters<SseTransport>[1][] = [];
    const transport: SseTransport = (_url, connectionCallbacks) => {
      callbacks.push(connectionCallbacks);
      return new Promise((resolve) => {
        connectionCallbacks.signal.addEventListener(
          "abort",
          () => {
            resolve({ outcome: "completed" });
          },
          { once: true },
        );
      });
    };
    let finishRecovery: ((sequence: number) => void) | undefined;
    const recovery = new Promise<number>((resolve) => {
      finishRecovery = resolve;
    });
    const { recorded, handlers } = recordingHandlers({
      onRecovery: () => recovery,
    });
    const stream = new DurableRunStream({
      runId: RUN_ID,
      handlers,
      transport,
      scheduler,
      maxAttempts: 2,
    });
    stream.start();

    callbacks[0]?.onEvent({ id: null, event: null, data: durableFrameJson(1) });
    callbacks[0]?.onEvent({ id: null, event: null, data: durableFrameJson(3) });
    await flushMicrotasks();

    expect(stream.getStatus()).toMatchObject({ kind: "recovering", attemptsUsed: 1 });
    expect(recorded.exhausted).toHaveLength(0);
    expect(scheduler.pendingCount()).toBe(0);

    finishRecovery?.(1);
    await flushMicrotasks();
    expect(callbacks).toHaveLength(2);
    expect(stream.getStatus()).toMatchObject({ kind: "connecting", attemptsUsed: 1 });
    expect(recorded.exhausted).toHaveLength(0);
  });

  it("recovers from a malformed data payload without advancing", () => {
    const { transport, connections } = FakeSseConnection.transport();
    const { recorded, handlers } = recordingHandlers();
    const stream = new DurableRunStream({
      runId: RUN_ID,
      handlers,
      transport,
      scheduler: new ManualScheduler(),
    });
    stream.start();

    connections[0]?.emitData(durableFrameJson(1));
    connections[0]?.emitData("not-json{");

    expect(stream.getAcceptedSequence()).toBe(1);
    expect(recorded.recoveries[0]?.reason.kind).toBe("malformed-frame");
    expect(recorded.recoveries[0]?.accepted).toBe(1);
  });

  it.each([
    ["wrong schema version", durableFrameJson(2, { schema_version: 2 })],
    ["wrong channel", durableFrameJson(2, { channel: "telemetry" })],
    [
      "unsupported payload schema version",
      durableFrameJson(2, { payload_schema_version: 3 }),
    ],
    [
      "non-integer payload_schema_version",
      durableFrameJson(2, { payload_schema_version: 1.5 }),
    ],
  ])("recovers on incompatible envelopes: %s", (_name, data) => {
    const { transport, connections } = FakeSseConnection.transport();
    const { recorded, handlers } = recordingHandlers();
    const stream = new DurableRunStream({
      runId: RUN_ID,
      handlers,
      transport,
      scheduler: new ManualScheduler(),
    });
    stream.start();
    connections[0]?.emitData(durableFrameJson(1));
    connections[0]?.emitData(data);

    expect(stream.getAcceptedSequence()).toBe(1);
    expect(recorded.recoveries[0]?.reason.kind).toBe("malformed-frame");
  });

  it("accepts the current version-2 work payload without weakening the envelope", () => {
    const { transport, connections } = FakeSseConnection.transport();
    const { recorded, handlers } = recordingHandlers();
    const stream = new DurableRunStream({
      runId: RUN_ID,
      handlers,
      transport,
      scheduler: new ManualScheduler(),
    });
    stream.start();
    connections[0]?.emitData(
      durableFrameJson(1, {
        event_kind: "work_claimed",
        payload_schema_version: 2,
      }),
    );

    expect(stream.getAcceptedSequence()).toBe(1);
    expect(recorded.recoveries).toEqual([]);
  });

  it("recovers on a foreign run id and on id disagreement", () => {
    const { transport, connections } = FakeSseConnection.transport();
    const { recorded, handlers } = recordingHandlers();
    const stream = new DurableRunStream({
      runId: RUN_ID,
      handlers,
      transport,
      scheduler: new ManualScheduler(),
    });
    stream.start();

    connections[0]?.emitData(durableFrameJson(1, { run_id: "other-run" }));
    expect(recorded.recoveries[0]?.reason).toEqual({
      kind: "incompatible-frame",
      detail: "foreign run id other-run",
    });

    // An id line disagreeing with the envelope sequence is incompatible too.
    connections[0]?.end({ outcome: "completed" });
    stream.resumeFrom(0);
    connections[1]?.emitData(durableFrameJson(1), "run.progressed", "9");
    expect(
      recorded.recoveries.some(
        (entry) =>
          entry.reason.kind === "incompatible-frame" &&
          entry.reason.detail.includes("SSE id disagrees"),
      ),
    ).toBe(true);
  });

  it("treats 409 stream_sequence_ahead as recovery and replays from zero", async () => {
    const { transport, connections } = FakeSseConnection.transport();
    const scheduler = new ManualScheduler();
    const { recorded, handlers } = recordingHandlers();
    const stream = new DurableRunStream({
      runId: RUN_ID,
      handlers,
      transport,
      scheduler,
    });
    // A client restoring cached state may claim an accepted sequence the
    // server no longer recognizes as durable history.
    stream.resumeFrom(41);

    connections[0]?.end({
      outcome: "http-error",
      status: 409,
      problemCode: "stream_sequence_ahead",
    });
    await flushMicrotasks();

    expect(recorded.recoveries[0]?.reason).toEqual({
      kind: "stream-ahead",
      code: "stream_sequence_ahead",
    });
    expect(connections).toHaveLength(2);
    // Durable history is never pruned, so a coherent recovery restarts the
    // full replay; the reducer ignores older events.
    expect(connections[1]?.url).toBe(`/api/v1/stream/runs/${RUN_ID}?after=0`);
    connections[1]?.emitData(durableFrameJson(1));
    connections[1]?.emitData(durableFrameJson(2));
    expect(recorded.frames).toEqual([1, 2]);
    expect(stream.getAcceptedSequence()).toBe(2);
  });

  it("treats a 400 resume rejection as recovery from zero", async () => {
    const { transport, connections } = FakeSseConnection.transport();
    const { recorded, handlers } = recordingHandlers();
    const stream = new DurableRunStream({
      runId: RUN_ID,
      handlers,
      transport,
      scheduler: new ManualScheduler(),
    });
    stream.start();
    connections[0]?.end({
      outcome: "http-error",
      status: 400,
      problemCode: "invalid_resume_position",
    });
    await flushMicrotasks();

    expect(recorded.recoveries[0]?.reason).toEqual({
      kind: "resume-rejected",
      code: "invalid_resume_position",
    });
    expect(connections[1]?.url.endsWith("?after=0")).toBe(true);
  });

  it("stops without reconnecting when the run is unknown (404)", async () => {
    const { transport, connections } = FakeSseConnection.transport();
    const scheduler = new ManualScheduler();
    const { recorded, handlers } = recordingHandlers();
    const stream = new DurableRunStream({
      runId: RUN_ID,
      handlers,
      transport,
      scheduler,
    });
    stream.start();
    connections[0]?.end({ outcome: "http-error", status: 404 });
    await flushMicrotasks();

    expect(stream.getStatus()).toMatchObject({ kind: "disconnected" });
    expect(recorded.exhausted).toHaveLength(1);
    expect(scheduler.pendingCount()).toBe(0);
  });

  it("reconnects a clean close with deterministic bounded backoff", async () => {
    const { transport, connections } = FakeSseConnection.transport();
    const scheduler = new ManualScheduler();
    const { recorded, handlers } = recordingHandlers();
    const stream = new DurableRunStream({
      runId: RUN_ID,
      handlers,
      transport,
      scheduler,
    });
    stream.start();

    const delays: number[] = [];
    for (let attempt = 0; attempt < 4; attempt += 1) {
      const before = scheduler.now;
      connections[connections.length - 1]?.end({ outcome: "completed" });
      await flushMicrotasks();
      const expectedDelay = 500 * 2 ** attempt;
      scheduler.advance(expectedDelay);
      delays.push(scheduler.now - before);
    }
    // Attempts 1..4 scheduled 500/1000/2000/4000 ms backoff.
    expect(delays).toEqual([500, 1_000, 2_000, 4_000]);
    expect(connections).toHaveLength(5);

    // The fifth failure exhausts the bounded budget.
    connections[4]?.end({ outcome: "completed" });
    await flushMicrotasks();
    expect(stream.getStatus().kind).toBe("disconnected");
    expect(recorded.exhausted).toHaveLength(1);
    expect(recorded.exhausted[0]?.attempts).toBe(5);
    expect(scheduler.pendingCount()).toBe(0);
  });

  it("counts recovery cycles against the bounded attempt budget", async () => {
    const { transport, connections } = FakeSseConnection.transport();
    const scheduler = new ManualScheduler();
    const { recorded, handlers } = recordingHandlers();
    const stream = new DurableRunStream({
      runId: RUN_ID,
      handlers,
      transport,
      scheduler,
      maxAttempts: 3,
    });
    stream.start();

    // Three consecutive recovery conditions exhaust the budget.
    for (let cycle = 0; cycle < 3; cycle += 1) {
      connections[connections.length - 1]?.emitData("garbage{");
      await flushMicrotasks();
    }
    expect(stream.getStatus().kind).toBe("disconnected");
    expect(recorded.exhausted).toHaveLength(1);
    expect(scheduler.pendingCount()).toBe(0);
  });

  it("aborts the connection and cancels timers on disposal, then stays silent", async () => {
    const { transport, connections } = FakeSseConnection.transport();
    const scheduler = new ManualScheduler();
    const recovery =
      vi.fn<(reason: RecoveryReason, accepted: number) => Promise<number>>();
    recovery.mockImplementation((_reason, accepted) => Promise.resolve(accepted));
    const { recorded, handlers } = recordingHandlers({ onRecovery: recovery });
    const stream = new DurableRunStream({
      runId: RUN_ID,
      handlers,
      transport,
      scheduler,
      maxAttempts: 5,
    });
    stream.start();
    connections[0]?.emitData(durableFrameJson(1));
    expect(recorded.frames).toEqual([1]);

    // A recovery pending its refetch must be silenced by dispose().
    connections[0]?.emitData("garbage{");
    await flushMicrotasks();
    expect(recovery).toHaveBeenCalledTimes(1);
    stream.dispose();

    expect(connections[0]?.callbacks.signal.aborted).toBe(true);
    expect(scheduler.pendingCount()).toBe(0);
    expect(stream.getStatus().kind).toBe("disconnected");

    const connectionCount = connections.length;
    await flushMicrotasks();
    expect(connections).toHaveLength(connectionCount);

    // Late frames after disposal are ignored entirely.
    connections[0]?.emitData(durableFrameJson(2));
    expect(recorded.frames).toEqual([1]);
  });

  it("resumes from an explicit sequence with a fresh attempt budget", () => {
    const { transport, connections } = FakeSseConnection.transport();
    const { recorded, handlers } = recordingHandlers();
    const stream = new DurableRunStream({
      runId: RUN_ID,
      handlers,
      transport,
      scheduler: new ManualScheduler(),
    });
    stream.resumeFrom(12);

    expect(connections).toHaveLength(1);
    expect(connections[0]?.url).toBe(`/api/v1/stream/runs/${RUN_ID}?after=12`);
    connections[0]?.emitData(durableFrameJson(13));
    expect(recorded.frames).toEqual([13]);
  });

  it("delivers run-carrying frames to the reducer through onFrame", () => {
    const { transport, connections } = FakeSseConnection.transport();
    const { recorded, handlers } = recordingHandlers();
    const stream = new DurableRunStream({
      runId: RUN_ID,
      handlers,
      transport,
      scheduler: new ManualScheduler(),
    });
    stream.start();
    connections[0]?.emitData(runCarryingFrameJson(1, runResponse(2)));
    expect(recorded.frames).toEqual([1]);
  });

  it("uses the deterministic exponential backoff curve", () => {
    expect(defaultBackoffDelay(1)).toBe(500);
    expect(defaultBackoffDelay(2)).toBe(1_000);
    expect(defaultBackoffDelay(3)).toBe(2_000);
    expect(defaultBackoffDelay(4)).toBe(4_000);
    expect(defaultBackoffDelay(5)).toBe(8_000);
    expect(defaultBackoffDelay(50)).toBe(BACKOFF_MAX_MS);
  });
});

// ---------------------------------------------------------------------------
// Production fetch transport
// ---------------------------------------------------------------------------

import { createFetchSseTransport, type SseMessage } from "./durable-stream";

function sseResponse(chunks: string[]): Response {
  const encoder = new TextEncoder();
  const stream = new ReadableStream<Uint8Array>({
    start: (controller) => {
      for (const chunk of chunks) {
        controller.enqueue(encoder.encode(chunk));
      }
      controller.close();
    },
  });
  return new Response(stream, {
    status: 200,
    headers: { "Content-Type": "text/event-stream" },
  });
}

function problemResponse(body: unknown, status: number, contentType: string): Response {
  return new Response(typeof body === "string" ? body : JSON.stringify(body), {
    status,
    headers: { "Content-Type": contentType },
  });
}

describe("fetch SSE transport", () => {
  it("parses the streamed event source into complete SSE messages", async () => {
    const events: SseMessage[] = [];
    const frame = durableFrameJson(1);
    vi.stubGlobal(
      "fetch",
      vi.fn(() =>
        Promise.resolve(
          sseResponse([
            `retry: 3000\n\n`,
            `id: 1\nevent: run.progressed\ndata: ${frame}\n\n`,
            ": paritygrid-heartbeat\n\n",
          ]),
        ),
      ),
    );
    const transport = createFetchSseTransport();
    const result = await transport("/api/v1/stream/runs/run-1?after=0", {
      signal: new AbortController().signal,
      onEvent: (message) => {
        events.push(message);
      },
    });

    expect(result).toEqual({ outcome: "completed" });
    expect(events).toContainEqual({ id: "1", event: "run.progressed", data: frame });
    expect(events).toHaveLength(1);
    vi.unstubAllGlobals();
  });

  it("reassembles frames split across stream chunks", async () => {
    const events: SseMessage[] = [];
    const frame = durableFrameJson(2);
    vi.stubGlobal(
      "fetch",
      vi.fn(() =>
        Promise.resolve(
          sseResponse([
            `id: 2\nevent: run.prog`,
            `ressed\ndata: ${frame.slice(0, 12)}`,
            frame.slice(12),
            "\n\n",
          ]),
        ),
      ),
    );
    const transport = createFetchSseTransport();
    await transport("/api/v1/stream/runs/run-1?after=1", {
      signal: new AbortController().signal,
      onEvent: (message) => {
        events.push(message);
      },
    });
    expect(events).toEqual([{ id: "2", event: "run.progressed", data: frame }]);
    vi.unstubAllGlobals();
  });

  it("surfaces the rejected resume status with its Problem code", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn(() =>
        Promise.resolve(
          problemResponse(
            { title: "ahead", status: 409, code: "stream_sequence_ahead" },
            409,
            "application/problem+json",
          ),
        ),
      ),
    );
    const transport = createFetchSseTransport();
    const result = await transport("/api/v1/stream/runs/run-1?after=99", {
      signal: new AbortController().signal,
      onEvent: () => undefined,
    });
    expect(result).toEqual({
      outcome: "http-error",
      status: 409,
      problemCode: "stream_sequence_ahead",
    });
    vi.unstubAllGlobals();
  });

  it("keeps the problem code undefined for non-JSON error bodies", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn(() =>
        Promise.resolve(problemResponse("gateway exploded", 502, "text/plain")),
      ),
    );
    const transport = createFetchSseTransport();
    const result = await transport("/api/v1/stream/runs/run-1", {
      signal: new AbortController().signal,
      onEvent: () => undefined,
    });
    expect(result).toEqual({ outcome: "http-error", status: 502 });
    vi.unstubAllGlobals();
  });

  it("propagates transport rejections to the stream's error path", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn(() => Promise.reject(new TypeError("network down"))),
    );
    const transport = createFetchSseTransport();
    await expect(
      transport("/api/v1/stream/runs/run-1", {
        signal: new AbortController().signal,
        onEvent: () => undefined,
      }),
    ).rejects.toBeInstanceOf(TypeError);
    vi.unstubAllGlobals();
  });
});

describe("durable stream guard branches", () => {
  it("ignores start and resume requests while a connection is already opening", async () => {
    const { transport, connections } = FakeSseConnection.transport();
    const { handlers } = recordingHandlers();
    const stream = new DurableRunStream({
      runId: RUN_ID,
      handlers,
      transport,
      scheduler: new ManualScheduler(),
    });

    stream.start();
    stream.start();
    stream.resumeFrom(5);
    expect(connections).toHaveLength(1);
    connections[0]?.end({ outcome: "completed" });
    await flushMicrotasks();
    // resumeFrom(5) was rejected while connecting; the clean close schedules
    // the bounded reconnect instead.
    expect(stream.getStatus().resumeFrom).toBe(0);
  });

  it("cancels a pending reconnect timer on disposal", async () => {
    const { transport, connections } = FakeSseConnection.transport();
    const scheduler = new ManualScheduler();
    const { handlers } = recordingHandlers();
    const stream = new DurableRunStream({
      runId: RUN_ID,
      handlers,
      transport,
      scheduler,
    });
    stream.start();
    connections[0]?.end({ outcome: "completed" });
    await flushMicrotasks();
    expect(scheduler.pendingCount()).toBe(1);

    stream.dispose();
    expect(scheduler.pendingCount()).toBe(0);
    scheduler.advance(10_000);
    expect(connections).toHaveLength(1);
  });

  it("routes a foreign event name disagreement through recovery", () => {
    const { transport, connections } = FakeSseConnection.transport();
    const { recorded, handlers } = recordingHandlers();
    const stream = new DurableRunStream({
      runId: RUN_ID,
      handlers,
      transport,
      scheduler: new ManualScheduler(),
    });
    stream.start();
    // event line matching event_kind is accepted silently...
    connections[0]?.emitData(durableFrameJson(1), "run.progressed");
    expect(recorded.frames).toEqual([1]);
    // ...but a mismatched event line is an incompatible frame.
    connections[0]?.emitData(durableFrameJson(2), "other.kind");
    expect(
      recorded.recoveries.some(
        (entry) =>
          entry.reason.kind === "incompatible-frame" &&
          entry.reason.detail.includes("event name"),
      ),
    ).toBe(true);
  });
});

describe("transport failure routing", () => {
  it("schedules a bounded reconnect when the transport itself rejects", async () => {
    const scheduler = new ManualScheduler();
    const { recorded, handlers } = recordingHandlers();
    const stream = new DurableRunStream({
      runId: RUN_ID,
      handlers,
      transport: () => Promise.reject(new TypeError("socket died")),
      scheduler,
    });
    stream.start();
    await flushMicrotasks();

    expect(stream.getStatus()).toMatchObject({ kind: "recovering" });
    expect(scheduler.pendingCount()).toBe(1);
    scheduler.advance(500);
    expect(stream.getStatus().kind).toBe("connecting");
    expect(recorded.exhausted).toHaveLength(0);
  });

  it("treats other HTTP errors (503) as reconnectable transport failures", async () => {
    const { transport, connections } = FakeSseConnection.transport();
    const scheduler = new ManualScheduler();
    const { handlers } = recordingHandlers();
    const stream = new DurableRunStream({
      runId: RUN_ID,
      handlers,
      transport,
      scheduler,
    });
    stream.start();
    connections[0]?.end({
      outcome: "http-error",
      status: 503,
      problemCode: "overloaded",
    });
    await flushMicrotasks();

    expect(stream.getStatus()).toMatchObject({ kind: "recovering" });
    scheduler.advance(500);
    expect(connections).toHaveLength(2);
  });

  it("falls back to canonical problem codes when the body is unreadable", async () => {
    const { transport, connections } = FakeSseConnection.transport();
    const scheduler = new ManualScheduler();
    const { recorded, handlers } = recordingHandlers();
    const stream = new DurableRunStream({
      runId: RUN_ID,
      handlers,
      transport,
      scheduler,
    });
    stream.start();
    connections[0]?.end({ outcome: "http-error", status: 409 });
    await flushMicrotasks();

    expect(recorded.recoveries[0]?.reason).toEqual({
      kind: "stream-ahead",
      code: "stream_sequence_ahead",
    });
  });
});

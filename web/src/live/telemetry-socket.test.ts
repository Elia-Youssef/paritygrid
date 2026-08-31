import { describe, expect, it, vi } from "vitest";

import {
  TelemetrySocket,
  type TelemetryCloseReason,
  type TelemetryConnection,
  type TelemetryFrameHandlers,
  type TelemetryTransportFactory,
} from "./telemetry-socket";
import {
  batchJson,
  durableFrameJson,
  FakeTelemetrySocket,
  ManualScheduler,
  pongJson,
  RUN_ID,
  snapshotJson,
  telemetryRecordJson,
} from "../test/live-fixtures";

interface Recorded {
  snapshots: number[];
  batches: { records: number; dropped: number }[];
  pongs: number[];
  connections: TelemetryConnection[];
}

function recordingHandlers(): { recorded: Recorded; handlers: TelemetryFrameHandlers } {
  const recorded: Recorded = { snapshots: [], batches: [], pongs: [], connections: [] };
  const handlers: TelemetryFrameHandlers = {
    onSnapshot: (frame) => {
      recorded.snapshots.push(frame.records.length);
    },
    onBatch: (frame) => {
      recorded.batches.push({ records: frame.records.length, dropped: frame.dropped });
    },
    onPong: () => {
      recorded.pongs.push(recorded.pongs.length);
    },
    onConnection: (connection) => {
      recorded.connections.push(connection);
    },
  };
  return { recorded, handlers };
}

type Handlers = ReturnType<typeof recordingHandlers>["handlers"];

function makeSocket(
  factory: TelemetryTransportFactory,
  scheduler: ManualScheduler,
  handlers: Handlers,
  options: {
    maxAttempts?: number;
    staleAfterMs?: number;
    keepAliveIntervalMs?: number;
  } = {},
): TelemetrySocket {
  return new TelemetrySocket({
    runId: RUN_ID,
    handlers,
    transport: factory,
    scheduler,
    now: () => scheduler.now,
    maxAttempts: options.maxAttempts,
    staleAfterMs: options.staleAfterMs ?? 10_000,
    keepAliveIntervalMs: options.keepAliveIntervalMs ?? 5_000,
  });
}

describe("telemetry socket", () => {
  it("opens one socket per owner and reaches live on open", () => {
    const { factory, sockets } = FakeTelemetrySocket.factory();
    const scheduler = new ManualScheduler();
    const { handlers } = recordingHandlers();
    const socket = makeSocket(factory, scheduler, handlers);
    socket.start();
    socket.start();

    expect(sockets).toHaveLength(1);
    expect(sockets[0]?.url).toBe(`/api/v1/live/runs/${RUN_ID}`);
    sockets[0]?.open();

    expect(socket.getConnection()).toMatchObject({ kind: "live" });
  });

  it("delivers the initial snapshot, sampled batches, and pongs", () => {
    const { factory, sockets } = FakeTelemetrySocket.factory();
    const scheduler = new ManualScheduler();
    const { recorded, handlers } = recordingHandlers();
    const socket = makeSocket(factory, scheduler, handlers);
    socket.start();
    sockets[0]?.open();

    sockets[0]?.message(
      snapshotJson([telemetryRecordJson(1_000), telemetryRecordJson(2_000)]),
    );
    sockets[0]?.message(batchJson([telemetryRecordJson(3_000)], 5));
    sockets[0]?.message(pongJson);

    expect(recorded.snapshots).toEqual([2]);
    expect(recorded.batches).toEqual([{ records: 1, dropped: 5 }]);
    expect(recorded.pongs).toHaveLength(1);
    expect(socket.getConnection()).toMatchObject({ kind: "live" });
  });

  it("rejects sampled batches until each connection receives its initial snapshot", () => {
    const { factory, sockets } = FakeTelemetrySocket.factory();
    const scheduler = new ManualScheduler();
    const { recorded, handlers } = recordingHandlers();
    const socket = makeSocket(factory, scheduler, handlers);
    socket.start();
    sockets[0]?.open();

    sockets[0]?.message(batchJson([telemetryRecordJson(2_000)], 4));
    expect(recorded.batches).toEqual([]);

    sockets[0]?.message(snapshotJson());
    sockets[0]?.message(batchJson([telemetryRecordJson(2_000)], 4));
    expect(recorded.snapshots).toEqual([1]);
    expect(recorded.batches).toEqual([{ records: 1, dropped: 4 }]);
  });

  it("ignores malformed, foreign-transport, and foreign-run frames without state changes", () => {
    const { factory, sockets } = FakeTelemetrySocket.factory();
    const scheduler = new ManualScheduler();
    const { recorded, handlers } = recordingHandlers();
    const socket = makeSocket(factory, scheduler, handlers);
    socket.start();
    sockets[0]?.open();

    sockets[0]?.message("{not json");
    // A durable-events envelope over the telemetry channel is rejected: the
    // transports can never be confused.
    sockets[0]?.message(durableFrameJson(1));
    sockets[0]?.message(
      snapshotJson([telemetryRecordJson(1_000, { run_id: "other-run" })]),
    );

    expect(recorded.snapshots).toEqual([]);
    expect(recorded.batches).toEqual([]);
    expect(socket.getConnection()).toMatchObject({ kind: "live" });
  });

  it("sends bounded keepalive pings on the injected schedule", () => {
    const { factory, sockets } = FakeTelemetrySocket.factory();
    const scheduler = new ManualScheduler();
    const { handlers } = recordingHandlers();
    const socket = makeSocket(factory, scheduler, handlers, {
      keepAliveIntervalMs: 5_000,
    });
    socket.start();
    sockets[0]?.open();

    scheduler.advance(5_000);
    scheduler.advance(5_000);

    const ping = JSON.stringify({ type: "ping" });
    expect(sockets[0]?.sent).toEqual([ping, ping]);
  });

  it("marks the connection stale after silence and back to live on a frame", () => {
    const { factory, sockets } = FakeTelemetrySocket.factory();
    const scheduler = new ManualScheduler();
    const { recorded, handlers } = recordingHandlers();
    const socket = makeSocket(factory, scheduler, handlers, {
      keepAliveIntervalMs: 5_000,
      staleAfterMs: 10_000,
    });
    socket.start();
    sockets[0]?.open();
    sockets[0]?.message(snapshotJson());

    scheduler.advance(10_000);
    expect(socket.getConnection()).toMatchObject({ kind: "live" });

    scheduler.advance(5_001);
    expect(socket.getConnection()).toMatchObject({ kind: "stale" });

    sockets[0]?.message(batchJson([telemetryRecordJson(20_000)]));
    expect(socket.getConnection()).toMatchObject({ kind: "live" });
    expect(recorded.batches).toHaveLength(1);
  });

  it("reconnects with bounded deterministic backoff after an abnormal close", () => {
    const { factory, sockets } = FakeTelemetrySocket.factory();
    const scheduler = new ManualScheduler();
    const { handlers } = recordingHandlers();
    const socket = makeSocket(factory, scheduler, handlers);
    socket.start();
    sockets[0]?.open();

    sockets[0]?.close(1006);
    expect(socket.getConnection()).toMatchObject({ kind: "recovering", attempt: 1 });
    scheduler.advance(500);
    expect(sockets).toHaveLength(2);
    sockets[1]?.open();

    // Repeated closes exhaust the bounded budget (5 attempts).
    for (let attempt = 2; attempt <= 5; attempt += 1) {
      sockets[attempt - 1]?.close(1006);
      if (attempt < 5) {
        expect(socket.getConnection()).toMatchObject({ kind: "recovering", attempt });
        scheduler.advance(500 * 2 ** (attempt - 1));
      }
    }
    expect(socket.getConnection()).toMatchObject({
      kind: "disconnected",
      terminal: true,
      reason: "attempts-exhausted",
    });
    expect(scheduler.pendingCount()).toBe(0);
  });

  it("never reconnects after terminal close codes", () => {
    const scheduler = new ManualScheduler();
    const { handlers } = recordingHandlers();

    for (const [code, reason] of [
      [4404, "run-not-found"],
      [1013, "capacity"],
    ] as const) {
      const single = FakeTelemetrySocket.factory();
      const socket = makeSocket(single.factory, scheduler, handlers);
      socket.start();
      single.sockets[0]?.open();
      single.sockets[0]?.close(code);
      expect(socket.getConnection()).toEqual({
        kind: "disconnected",
        terminal: true,
        reason,
      } satisfies TelemetryConnection & { reason: TelemetryCloseReason });
      expect(scheduler.pendingCount()).toBe(0);
    }
  });

  it("stops updating after disposal and closes the socket", () => {
    const { factory, sockets } = FakeTelemetrySocket.factory();
    const scheduler = new ManualScheduler();
    const { recorded, handlers } = recordingHandlers();
    const socket = makeSocket(factory, scheduler, handlers);
    socket.start();
    sockets[0]?.open();
    sockets[0]?.message(snapshotJson());

    socket.dispose();
    expect(socket.getConnection()).toMatchObject({
      kind: "disconnected",
      terminal: true,
    });
    expect(sockets[0]?.closedWith).not.toBeNull();

    // Everything after disposal is a no-op.
    sockets[0]?.message(batchJson([telemetryRecordJson(9_000)]));
    sockets[0]?.open();
    sockets[0]?.close(1006);
    scheduler.advance(60_000);

    expect(recorded.batches).toEqual([]);
    expect(recorded.connections.filter((entry) => entry.kind === "recovering")).toEqual(
      [],
    );
  });

  it("drops out-of-order observations inside accepted batches", () => {
    const { factory, sockets } = FakeTelemetrySocket.factory();
    const scheduler = new ManualScheduler();
    const { recorded, handlers } = recordingHandlers();
    const socket = makeSocket(factory, scheduler, handlers);
    socket.start();
    sockets[0]?.open();

    // The socket validates frames; ordering is the reducer's rule, so the
    // socket delivers the batch and the slice records only forward time.
    sockets[0]?.message(snapshotJson([telemetryRecordJson(5_000)]));
    sockets[0]?.message(
      batchJson([
        telemetryRecordJson(4_000),
        telemetryRecordJson(6_000),
        telemetryRecordJson(6_000),
      ]),
    );

    expect(recorded.batches).toEqual([{ records: 3, dropped: 0 }]);
    expect(socket.getConnection()).toMatchObject({ kind: "live" });
  });

  it("reports connection transitions to subscribers in order", () => {
    const { factory, sockets } = FakeTelemetrySocket.factory();
    const scheduler = new ManualScheduler();
    const { recorded, handlers } = recordingHandlers();
    const socket = makeSocket(factory, scheduler, handlers);
    socket.start();
    sockets[0]?.open();
    sockets[0]?.close(1013);

    expect(recorded.connections.map((entry) => entry.kind)).toEqual([
      "connecting",
      "live",
      "disconnected",
    ]);
  });

  it("cancels scheduled work when disposed before the socket opens", () => {
    const { factory, sockets } = FakeTelemetrySocket.factory();
    const scheduler = new ManualScheduler();
    const { handlers } = recordingHandlers();
    const socket = makeSocket(factory, scheduler, handlers);
    socket.start();
    socket.dispose();

    scheduler.advance(60_000);
    expect(sockets[0]?.sent).toEqual([]);
    expect(scheduler.pendingCount()).toBe(0);
  });
});

// ---------------------------------------------------------------------------
// Production browser WebSocket transport
// ---------------------------------------------------------------------------

import { createBrowserTelemetryTransport } from "./telemetry-socket";

class FakeBrowserSocket {
  static instances: FakeBrowserSocket[] = [];
  readonly url: string;
  sent: string[] = [];
  closed = false;
  readyState = 0;
  onopen: (() => void) | null = null;
  onmessage: ((event: { data: unknown }) => void) | null = null;
  onclose: ((event: { code: number }) => void) | null = null;
  onerror: (() => void) | null = null;

  constructor(url: string) {
    this.url = url;
    FakeBrowserSocket.instances.push(this);
  }

  send(text: string): void {
    if (this.readyState === 1) {
      this.sent.push(text);
    }
  }

  close(): void {
    this.closed = true;
  }
}

describe("browser telemetry transport", () => {
  it("wraps the browser WebSocket lifecycle into the socket contract", () => {
    FakeBrowserSocket.instances = [];
    vi.stubGlobal("WebSocket", FakeBrowserSocket);
    try {
      const scheduler = new ManualScheduler();
      const { recorded, handlers } = recordingHandlers();
      const socket = new TelemetrySocket({
        runId: RUN_ID,
        handlers,
        transport: createBrowserTelemetryTransport("http://console.example/app/runs"),
        scheduler,
        now: () => scheduler.now,
      });
      socket.start();

      const instance = FakeBrowserSocket.instances[0];
      expect(instance?.url).toBe(`ws://console.example/api/v1/live/runs/${RUN_ID}`);

      instance?.onopen?.();
      expect(socket.getConnection()).toMatchObject({ kind: "live" });

      instance?.onmessage?.({ data: snapshotJson() });
      expect(recorded.snapshots).toEqual([1]);

      instance?.onmessage?.({ data: 42 });
      expect(recorded.snapshots).toEqual([1]);

      instance?.onerror?.();
      instance?.onclose?.({ code: 1006 });
      expect(socket.getConnection()).toMatchObject({ kind: "recovering", attempt: 1 });

      scheduler.advance(500);
      expect(FakeBrowserSocket.instances).toHaveLength(2);

      // Disposal closes the socket the owner currently holds.
      const current = FakeBrowserSocket.instances[1];
      socket.dispose();
      expect(current?.closed).toBe(true);
      expect(socket.getConnection()).toMatchObject({ kind: "disconnected" });
    } finally {
      vi.unstubAllGlobals();
    }
  });

  it("closes a browser socket when disposal aborts mid-connect", () => {
    FakeBrowserSocket.instances = [];
    vi.stubGlobal("WebSocket", FakeBrowserSocket);
    try {
      const scheduler = new ManualScheduler();
      const { handlers } = recordingHandlers();
      const socket = new TelemetrySocket({
        runId: RUN_ID,
        handlers,
        transport: createBrowserTelemetryTransport(),
        scheduler,
        now: () => scheduler.now,
      });
      socket.start();
      socket.dispose();
      expect(FakeBrowserSocket.instances[0]?.closed).toBe(true);
    } finally {
      vi.unstubAllGlobals();
    }
  });

  it("uses a secure WebSocket URL when the page origin is HTTPS", () => {
    FakeBrowserSocket.instances = [];
    vi.stubGlobal("WebSocket", FakeBrowserSocket);
    try {
      const scheduler = new ManualScheduler();
      const { handlers } = recordingHandlers();
      const socket = new TelemetrySocket({
        runId: RUN_ID,
        handlers,
        transport: createBrowserTelemetryTransport("https://console.example/app/"),
        scheduler,
        now: () => scheduler.now,
      });
      socket.start();

      expect(FakeBrowserSocket.instances[0]?.url).toBe(
        `wss://console.example/api/v1/live/runs/${RUN_ID}`,
      );
      socket.dispose();
    } finally {
      vi.unstubAllGlobals();
    }
  });
});

describe("socket message guards", () => {
  it("ignores messages that arrive before the socket opens", () => {
    const { factory, sockets } = FakeTelemetrySocket.factory();
    const scheduler = new ManualScheduler();
    const { recorded, handlers } = recordingHandlers();
    const socket = makeSocket(factory, scheduler, handlers);
    socket.start();

    sockets[0]?.message(snapshotJson());
    expect(recorded.snapshots).toEqual([]);
    expect(socket.getConnection()).toMatchObject({ kind: "connecting" });
  });
});

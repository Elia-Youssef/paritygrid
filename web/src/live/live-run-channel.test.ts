import { describe, expect, it, vi } from "vitest";

import { QueryClient } from "@tanstack/react-query";

import { LiveRunChannel, type ChannelSnapshot } from "./live-run-channel";
import { queryKeys } from "../api/query-keys";
import {
  batchJson,
  durableFrameJson,
  FakeSseConnection,
  FakeTelemetrySocket,
  ManualScheduler,
  runCarryingFrameJson,
  runResponse,
  RUN_ID,
  snapshotJson,
  telemetryRecordJson,
} from "../test/live-fixtures";

interface Harness {
  channel: LiveRunChannel;
  queryClient: QueryClient;
  sse: ReturnType<typeof FakeSseConnection.transport>;
  ws: ReturnType<typeof FakeTelemetrySocket.factory>;
  scheduler: ManualScheduler;
  runQuery: ReturnType<typeof vi.fn>;
  snapshots: ChannelSnapshot[];
  flush: () => Promise<void>;
}

function makeChannel(runVersion = 1): Harness {
  const queryClient = new QueryClient();
  const sse = FakeSseConnection.transport();
  const ws = FakeTelemetrySocket.factory();
  const scheduler = new ManualScheduler();
  const runQuery = vi.fn<(runId: string) => Promise<ReturnType<typeof runResponse>>>();
  runQuery.mockImplementation(() => Promise.resolve(runResponse(runVersion)));

  const snapshots: ChannelSnapshot[] = [];
  const channel = new LiveRunChannel({
    runId: RUN_ID,
    queryClient,
    sseTransport: sse.transport,
    telemetryTransport: ws.factory,
    scheduler,
    now: () => scheduler.now,
    runQueryFn: (runId) => runQuery(runId),
  });
  channel.subscribe(() => snapshots.push(channel.getSnapshot()));

  return {
    channel,
    queryClient,
    sse,
    ws,
    scheduler,
    runQuery,
    snapshots,
    flush: async () => {
      for (let tick = 0; tick < 10; tick += 1) {
        await Promise.resolve();
      }
    },
  };
}

describe("live run channel", () => {
  it("loads authoritative state first, then opens both transports", async () => {
    const h = makeChannel();
    await h.channel.start();

    expect(h.runQuery).toHaveBeenCalledTimes(1);
    expect(h.channel.getDurableState().run?.run_version).toBe(1);
    expect(h.channel.getDurableState().status).toBe("ready");
    expect(h.sse.connections).toHaveLength(1);
    expect(h.sse.connections[0]?.url).toBe(`/api/v1/stream/runs/${RUN_ID}?after=0`);
    expect(h.ws.sockets).toHaveLength(1);
  });

  it("applies newer query-cache writes and rejects late older ones", async () => {
    const h = makeChannel();
    await h.channel.start();

    // A refetch or mutation result lands in the cache (version 3).
    h.queryClient.setQueryData(queryKeys.runDetail(RUN_ID), runResponse(3));
    await h.flush();
    expect(h.channel.getDurableState().run?.run_version).toBe(3);

    // A racing query response written later with an older version is a
    // silent no-op.
    h.queryClient.setQueryData(queryKeys.runDetail(RUN_ID), runResponse(2));
    await h.flush();
    expect(h.channel.getDurableState().run?.run_version).toBe(3);
  });

  it("applies durable events, advances the sequence, and ignores duplicates", async () => {
    const h = makeChannel();
    await h.channel.start();

    h.sse.connections[0]?.emitData(durableFrameJson(1));
    h.sse.connections[0]?.emitData(durableFrameJson(2));
    h.sse.connections[0]?.emitData(durableFrameJson(2));

    const durable = h.channel.getDurableState();
    expect(durable.acceptedSequence).toBe(2);
    expect(durable.events).toHaveLength(2);
  });

  it("invalidates and refetches the run query when an event carries a newer run", async () => {
    const h = makeChannel(1);
    await h.channel.start();
    expect(h.runQuery).toHaveBeenCalledTimes(1);

    // The durable event carries the authoritative v2 projection.
    h.runQuery.mockResolvedValue(runResponse(2));
    h.sse.connections[0]?.emitData(runCarryingFrameJson(1, runResponse(2)));
    await h.flush();

    // Invalidation timing: the refetch happens because of the event.
    expect(h.runQuery).toHaveBeenCalledTimes(2);
    expect(h.channel.getDurableState().run?.run_version).toBe(2);
    // No invalidation for events without a run projection.
    h.sse.connections[0]?.emitData(durableFrameJson(2));
    await h.flush();
    expect(h.runQuery).toHaveBeenCalledTimes(2);
  });

  it("recovers without accepting an embedded incompatible pipeline version", async () => {
    const h = makeChannel(1);
    await h.channel.start();

    h.sse.connections[0]?.emitData(
      runCarryingFrameJson(1, runResponse(2, { pipeline_version: 2 })),
    );
    expect(h.channel.getDurableState()).toMatchObject({
      acceptedSequence: 0,
      status: "recovering",
    });
    expect(h.channel.getDurableState().run?.pipeline_version).toBe(1);

    await h.flush();
    expect(h.sse.connections).toHaveLength(2);
    expect(h.sse.connections[1]?.url).toBe(`/api/v1/stream/runs/${RUN_ID}?after=0`);
  });

  it("recovers from a gap: marks recovering, refetches, resumes from accepted", async () => {
    const h = makeChannel();
    await h.channel.start();

    h.sse.connections[0]?.emitData(durableFrameJson(1));
    h.sse.connections[0]?.emitData(durableFrameJson(4));

    // Synchronously, the gap retains the last coherent view and marks it
    // recovering.
    const gapped = h.channel.getDurableState();
    expect(gapped.status).toBe("recovering");
    expect(gapped.run?.run_version).toBe(1);
    expect(gapped.lastError).toContain("gap");
    expect(gapped.acceptedSequence).toBe(1);

    await h.flush();
    // Authoritative recovery refetched the run query, restoring a coherent
    // ready view, and the stream resumed from the recovered sequence.
    expect(h.runQuery).toHaveBeenCalledTimes(2);
    expect(h.sse.connections).toHaveLength(2);
    expect(h.sse.connections[1]?.url).toBe(`/api/v1/stream/runs/${RUN_ID}?after=1`);
    expect(h.channel.getDurableState().status).toBe("ready");
    expect(h.channel.getSnapshot().stream.kind).toBe("connecting");
  });

  it("keeps durable truth isolated from telemetry", async () => {
    const h = makeChannel();
    await h.channel.start();

    h.ws.sockets[0]?.open();
    h.ws.sockets[0]?.message(snapshotJson());
    h.ws.sockets[0]?.message(batchJson([telemetryRecordJson(9_000)], 3));

    const view = h.channel.getSnapshot().view;
    expect(view.telemetry.lastObservedMicros).toBe(9_000);
    expect(view.telemetry.records).toHaveLength(2);
    expect(view.telemetry.droppedTotal).toBe(3);
    expect(view.durable.run?.run_version).toBe(1);
    expect(view.durable.acceptedSequence).toBe(0);
    expect(view.durable.status).toBe("ready");

    // Disconnecting telemetry never changes durable state.
    h.ws.sockets[0]?.close(4404);
    await h.flush();
    const after = h.channel.getSnapshot().view;
    expect(after.telemetry.health).toBe("unavailable");
    expect(after.durable.status).toBe("ready");
    expect(after.durable.run?.run_version).toBe(1);
  });

  it("reports an authoritative fetch failure through the safe problem view", async () => {
    const queryClient = new QueryClient();
    const sse = FakeSseConnection.transport();
    const ws = FakeTelemetrySocket.factory();
    const channel = new LiveRunChannel({
      runId: RUN_ID,
      queryClient,
      sseTransport: sse.transport,
      telemetryTransport: ws.factory,
      scheduler: new ManualScheduler(),
      runQueryFn: () =>
        Promise.reject(
          new (class extends Error {
            readonly kind = "problem";
            readonly status = 404;
            readonly problem = { title: "Run not found", extensions: [] as [] };
            constructor() {
              super("Run not found");
            }
          })(),
        ),
    });
    const snapshots: ChannelSnapshot[] = [];
    channel.subscribe(() => snapshots.push(channel.getSnapshot()));
    await channel.start();

    const durable = channel.getDurableState();
    expect(durable.status).toBe("error");
    expect(durable.lastError).toBe("Run not found");
    expect(snapshots.length).toBeGreaterThan(0);
  });

  it("stops every update after disposal", async () => {
    const h = makeChannel();
    await h.channel.start();
    h.ws.sockets[0]?.open();
    const durableBefore = JSON.stringify(h.channel.getDurableState());

    h.channel.dispose();
    expect(h.channel.getSnapshot().stream.kind).toBe("disconnected");
    // Disposal itself settles the observable state once.
    const snapshotAfterDispose = JSON.stringify(h.channel.getSnapshot());

    // Both transports were aborted/closed: late events are ignored.
    h.sse.connections[0]?.emitData(durableFrameJson(1));
    h.sse.connections[0]?.emitData("garbage{");
    h.ws.sockets[0]?.message(snapshotJson());
    h.ws.sockets[0]?.close(1006);
    h.queryClient.setQueryData(queryKeys.runDetail(RUN_ID), runResponse(9));
    await h.flush();

    expect(JSON.stringify(h.channel.getDurableState())).toBe(durableBefore);
    expect(JSON.stringify(h.channel.getSnapshot())).toBe(snapshotAfterDispose);
    expect(h.channel.getSnapshot().telemetryConnection.kind).toBe("disconnected");
  });

  it("notifies subscribers when frames are accepted", async () => {
    const h = makeChannel();
    await h.channel.start();
    const count = h.snapshots.length;
    h.sse.connections[0]?.emitData(durableFrameJson(1));
    expect(h.snapshots.length).toBeGreaterThan(count);
    expect(h.channel.getSnapshot().stream.acceptedSequence).toBe(1);
    expect(h.channel.getSnapshot().stream.kind).toBe("live");
  });
});

describe("live run channel lifecycle edges", () => {
  it("marks durable state stale when reconnect attempts are exhausted", async () => {
    const h = makeChannel();
    await h.channel.start();
    h.ws.sockets[0]?.open();

    // Five transport-level failures exhaust the stream's bounded budget.
    let connectionIndex = 0;
    for (let attempt = 0; attempt < 5; attempt += 1) {
      h.sse.connections[connectionIndex]?.end({ outcome: "completed" });
      await h.flush();
      h.scheduler.advance(8_000);
      connectionIndex += 1;
    }
    await h.flush();

    expect(h.channel.getSnapshot().stream.kind).toBe("disconnected");
    expect(h.channel.getDurableState().status).toBe("stale");
  });

  it("mirrors telemetry connection health transitions", async () => {
    const h = makeChannel();
    await h.channel.start();
    const socket = h.ws.sockets[0];
    socket?.open();

    // Disconnect and reconnect attempts map to the recovering slice health.
    socket?.close(1006);
    await h.flush();
    expect(h.channel.getSnapshot().view.telemetry.health).toBe("recovering");
  });

  it("ignores query-cache events for other keys and removals", async () => {
    const h = makeChannel();
    await h.channel.start();
    const before = JSON.stringify(h.channel.getDurableState());

    h.queryClient.setQueryData(["unrelated", "key"], { any: "value" });
    h.queryClient.removeQueries({ queryKey: ["unrelated", "key"] });
    await h.flush();

    expect(JSON.stringify(h.channel.getDurableState())).toBe(before);
  });

  it("start after disposal is a no-op and double disposal is safe", async () => {
    const h = makeChannel();
    h.channel.dispose();
    await h.channel.start();
    h.channel.dispose();

    expect(h.channel.getDurableState().status).toBe("idle");
    expect(h.sse.connections).toHaveLength(0);
    expect(h.ws.sockets).toHaveLength(0);
  });

  it("unsubscribes from the query cache on disposal", () => {
    const queryClient = new QueryClient();
    const unsubscribe = vi.fn();
    vi.spyOn(queryClient.getQueryCache(), "subscribe").mockReturnValue(unsubscribe);
    const sse = FakeSseConnection.transport();
    const ws = FakeTelemetrySocket.factory();
    const channel = new LiveRunChannel({
      runId: RUN_ID,
      queryClient,
      sseTransport: sse.transport,
      telemetryTransport: ws.factory,
    });

    channel.dispose();
    channel.dispose();
    expect(unsubscribe).toHaveBeenCalledTimes(1);
  });
});

describe("telemetry health mirroring", () => {
  it("marks the telemetry slice stale after silence and live again on frames", async () => {
    const h = makeChannel();
    await h.channel.start();
    h.ws.sockets[0]?.open();
    h.ws.sockets[0]?.message(snapshotJson());

    // Advance past the staleness bound (keepalive tick runs the check).
    h.scheduler.advance(15_001);
    expect(h.channel.getSnapshot().view.telemetry.health).toBe("stale");
    expect(h.channel.getSnapshot().telemetryConnection.kind).toBe("stale");

    h.ws.sockets[0]?.message(batchJson([telemetryRecordJson(99_000)]));
    expect(h.channel.getSnapshot().view.telemetry.health).toBe("live");
  });
});

describe("recovery and cache guard branches", () => {
  it("resumes from the accepted sequence even when the recovery refetch fails", async () => {
    const h = makeChannel();
    await h.channel.start();
    const invalidate = vi
      .spyOn(h.queryClient, "invalidateQueries")
      .mockRejectedValueOnce(new TypeError("offline"));

    h.sse.connections[0]?.emitData(durableFrameJson(1));
    h.sse.connections[0]?.emitData(durableFrameJson(3));
    await h.flush();

    expect(invalidate).toHaveBeenCalled();
    // The stream still reconnects from the last coherent sequence.
    expect(h.sse.connections).toHaveLength(2);
    expect(h.sse.connections[1]?.url).toBe(`/api/v1/stream/runs/${RUN_ID}?after=1`);
  });

  it("ignores cache writes that are not run-shaped", async () => {
    const h = makeChannel();
    await h.channel.start();

    h.queryClient.setQueryData(queryKeys.runDetail(RUN_ID), "garbage");
    await h.flush();
    expect(h.channel.getDurableState().run?.run_version).toBe(1);
  });
});

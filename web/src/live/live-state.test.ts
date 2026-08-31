import { describe, expect, it } from "vitest";

import type { DurableEventFrame } from "./protocol";
import {
  applyDurableEvent,
  applyRunSnapshot,
  applyTelemetryBatch,
  applyTelemetrySnapshot,
  createLiveRunView,
  initialDurableRunState,
  initialTelemetrySlice,
  markError,
  markLoading,
  markRecovered,
  markRecovering,
  markStale,
  markTelemetryDisconnected,
  markTelemetryStale,
  markTelemetryUnavailable,
  MAX_EVENT_LOG,
  MAX_TELEMETRY_RECORDS,
} from "./live-state";
import {
  batchJson,
  durableFrameJson,
  RUN_ID,
  runCarryingFrameJson,
  runResponse,
  snapshotJson,
  telemetryRecordJson,
} from "../test/live-fixtures";
import {
  parseDurableFrame,
  parseTelemetryMessage,
  type TelemetryBatch,
  type TelemetrySnapshot,
} from "./protocol";

function frame(
  sequence: number,
  overrides: Record<string, unknown> = {},
): DurableEventFrame {
  const result = parseDurableFrame(
    JSON.parse(durableFrameJson(sequence, overrides)) as unknown,
  );
  if (!result.ok) {
    throw new Error(`fixture rejected: ${result.reason}`);
  }
  return result.frame;
}

function runFrame(
  sequence: number,
  run: ReturnType<typeof runResponse>,
): DurableEventFrame {
  const result = parseDurableFrame(
    JSON.parse(runCarryingFrameJson(sequence, run)) as unknown,
  );
  if (!result.ok) {
    throw new Error("run-carrying fixture rejected");
  }
  return result.frame;
}

function parseSnapshot(text: string): TelemetrySnapshot {
  const parsed = parseTelemetryMessage(text);
  if (!parsed.ok || parsed.message.kind !== "snapshot") {
    throw new Error("snapshot fixture rejected");
  }
  return parsed.message.frame;
}

function parseBatch(text: string): TelemetryBatch {
  const parsed = parseTelemetryMessage(text);
  if (!parsed.ok || parsed.message.kind !== "batch") {
    throw new Error("batch fixture rejected");
  }
  return parsed.message.frame;
}

describe("durable run state", () => {
  it("starts idle with sequence zero and no run", () => {
    const state = initialDurableRunState(RUN_ID);
    expect(state).toMatchObject({
      runId: RUN_ID,
      acceptedSequence: 0,
      run: null,
      status: "idle",
      events: [],
    });
  });

  it("applies an authoritative snapshot and becomes ready", () => {
    const state = applyRunSnapshot(initialDurableRunState(RUN_ID), runResponse(1));
    expect(state.status).toBe("ready");
    expect(state.run?.run_version).toBe(1);
    expect(state.lastError).toBeNull();
  });

  it("surfaces foreign or malformed snapshots without mutating durable fields", () => {
    const state = applyRunSnapshot(initialDurableRunState(RUN_ID), runResponse(1));
    const foreign = applyRunSnapshot(state, runResponse(9, { run_id: "other" }));
    expect(foreign.run?.run_version).toBe(1);
    expect(foreign.lastError).toContain("foreign");

    const malformed = applyRunSnapshot(state, {
      ...runResponse(9),
      run_version: 2.5,
    });
    expect(malformed.run?.run_version).toBe(1);
    expect(malformed.lastError).toContain("foreign");
  });

  it("never lets a late older snapshot regress newer durable state", () => {
    let state = applyRunSnapshot(initialDurableRunState(RUN_ID), runResponse(1));
    state = applyRunSnapshot(state, runResponse(2));
    const late = applyRunSnapshot(state, runResponse(1));

    expect(late).toBe(state);
    expect(late.run?.run_version).toBe(2);
  });

  it("rejects incompatible pipeline and runner definitions", () => {
    const state = applyRunSnapshot(initialDurableRunState(RUN_ID), runResponse(1));
    const pipelineMismatch = applyRunSnapshot(
      state,
      runResponse(2, { pipeline_version: 2 }),
    );
    const runnerMismatch = applyRunSnapshot(
      state,
      runResponse(2, { runner_kind: "threaded" }),
    );

    expect(pipelineMismatch.run).toEqual(state.run);
    expect(pipelineMismatch.lastError).toContain("incompatible pipeline");
    expect(runnerMismatch.run).toEqual(state.run);
    expect(runnerMismatch.lastError).toContain("incompatible pipeline");
  });

  it("accepts equal-version refreshes (idempotent, not a regression)", () => {
    const state = applyRunSnapshot(initialDurableRunState(RUN_ID), runResponse(2));
    const refreshed = applyRunSnapshot(state, runResponse(2, { state: "running" }));
    expect(refreshed).not.toBe(state);
    expect(refreshed.run?.state).toBe("running");
  });

  it("applies contiguous durable events and keeps a bounded log", () => {
    let state = initialDurableRunState(RUN_ID);
    for (let sequence = 1; sequence <= 5; sequence += 1) {
      state = applyDurableEvent(state, frame(sequence));
    }
    expect(state.acceptedSequence).toBe(5);
    expect(state.events).toHaveLength(5);
    expect(state.events[0]?.sequence).toBe(5);
  });

  it("bounds the event log", () => {
    let state = initialDurableRunState(RUN_ID);
    for (let sequence = 1; sequence <= MAX_EVENT_LOG + 25; sequence += 1) {
      state = applyDurableEvent(state, frame(sequence));
    }
    expect(state.events).toHaveLength(MAX_EVENT_LOG);
  });

  it("treats duplicate and older events as idempotent no-ops", () => {
    let state = applyDurableEvent(initialDurableRunState(RUN_ID), frame(1));
    state = applyDurableEvent(state, frame(2));
    const same = applyDurableEvent(state, frame(2));
    const older = applyDurableEvent(state, frame(1));
    expect(same).toBe(state);
    expect(older).toBe(state);
    expect(state.acceptedSequence).toBe(2);
  });

  it("marks recovery on a gap, retaining the last coherent view", () => {
    let state = applyRunSnapshot(initialDurableRunState(RUN_ID), runResponse(1));
    state = applyDurableEvent(state, frame(1));
    const gapped = applyDurableEvent(state, frame(4));

    expect(gapped.status).toBe("recovering");
    expect(gapped.run).toEqual(state.run);
    expect(gapped.acceptedSequence).toBe(1);
    expect(gapped.lastError).toContain("gap");
  });

  it("rejects events for a foreign run id", () => {
    const state = applyDurableEvent(
      initialDurableRunState(RUN_ID),
      frame(1, { run_id: "other" }),
    );
    expect(state.acceptedSequence).toBe(0);
    expect(state.lastError).toContain("foreign run id");
  });

  it("applies an embedded newer run projection under the version guard", () => {
    let state = applyRunSnapshot(initialDurableRunState(RUN_ID), runResponse(1));
    state = applyDurableEvent(state, runFrame(1, runResponse(2)));
    expect(state.acceptedSequence).toBe(1);
    expect(state.run?.run_version).toBe(2);

    // An event embedding an OLDER run than durable state carries is still
    // logged and sequenced, but the projection is rejected.
    state = applyDurableEvent(state, runFrame(2, runResponse(1)));
    expect(state.acceptedSequence).toBe(2);
    expect(state.run?.run_version).toBe(2);
  });

  it("enters recovery without advancing an incompatible embedded projection", () => {
    const state = applyRunSnapshot(initialDurableRunState(RUN_ID), runResponse(1));
    const incompatible = applyDurableEvent(
      state,
      runFrame(1, runResponse(2, { pipeline_id: "pipeline-other" })),
    );

    expect(incompatible.status).toBe("recovering");
    expect(incompatible.acceptedSequence).toBe(0);
    expect(incompatible.run).toEqual(state.run);
    expect(incompatible.lastError).toContain("incompatible pipeline");
  });

  it("recovers from foreign or malformed run.updated projections", () => {
    let state = applyDurableEvent(
      initialDurableRunState(RUN_ID),
      runFrame(1, runResponse(2, { run_id: "other" })),
    );
    expect(state.status).toBe("recovering");
    expect(state.acceptedSequence).toBe(0);
    expect(state.run).toBeNull();

    state = applyDurableEvent(
      initialDurableRunState(RUN_ID),
      frame(1, {
        event_kind: "run.updated",
        payload: { run: { run_id: RUN_ID, junk: true } },
      }),
    );
    expect(state.status).toBe("recovering");
    expect(state.acceptedSequence).toBe(0);
    expect(state.run).toBeNull();
  });

  it("marks loading, recovering, recovered, stale, and error explicitly", () => {
    let state = initialDurableRunState(RUN_ID);
    state = markLoading(state);
    expect(state.status).toBe("loading");

    state = markRecovering(state, { kind: "connection-closed" });
    expect(state.status).toBe("recovering");

    state = markRecovered(state, 7);
    expect(state).toMatchObject({ acceptedSequence: 7, status: "loading" });

    state = markStale(state, "attempts exhausted");
    expect(state.status).toBe("stale");

    state = markError(state, "problem");
    expect(state.status).toBe("error");

    // markLoading does not downgrade an already-ready view.
    const ready = applyRunSnapshot(initialDurableRunState(RUN_ID), runResponse(1));
    expect(markLoading(ready)).toBe(ready);
  });

  it("produces six-way race orderings that all converge on the newest version", () => {
    // Three authoritative sources progress the run to version 3: a query
    // snapshot (v1), a durable event carrying v2, and a refetch snapshot
    // (v3). Every delivery order must end at version 3.
    const snapshotV1 = runResponse(1);
    const eventV2 = runFrame(1, runResponse(2));
    const snapshotV3 = runResponse(3);

    const permutations: ("s1" | "e2" | "s3")[][] = [
      ["s1", "e2", "s3"],
      ["s1", "s3", "e2"],
      ["e2", "s1", "s3"],
      ["e2", "s3", "s1"],
      ["s3", "s1", "e2"],
      ["s3", "e2", "s1"],
    ];

    for (const order of permutations) {
      let state = initialDurableRunState(RUN_ID);
      for (const step of order) {
        if (step === "s1") {
          state = applyRunSnapshot(state, snapshotV1);
        } else if (step === "e2") {
          state = applyDurableEvent(state, eventV2);
        } else {
          state = applyRunSnapshot(state, snapshotV3);
        }
      }
      expect(state.run?.run_version, `order ${order.join("<")}`).toBe(3);
      expect(state.acceptedSequence, `order ${order.join("<")} sequence`).toBe(
        order.includes("e2") ? 1 : 0,
      );
    }
  });

  it("keeps a late mutation-refetch response from overwriting an event result", () => {
    // A repair application completes through a durable event (v5); a query
    // refetch issued before the event resolves late with v4.
    let state = applyRunSnapshot(initialDurableRunState(RUN_ID), runResponse(4));
    state = applyDurableEvent(state, runFrame(1, runResponse(5)));
    state = applyRunSnapshot(state, runResponse(4));
    expect(state.run?.run_version).toBe(5);
    expect(state.acceptedSequence).toBe(1);
  });
});

describe("telemetry slice", () => {
  it("stays a separate slice with no durable fields", () => {
    const slice = initialTelemetrySlice();
    expect(slice).toMatchObject({
      health: "idle",
      lastObservedMicros: 0,
      droppedTotal: 0,
      records: [],
    });
    // Structural isolation: telemetry has no keys in common with durable
    // state shape (run/version/sequence), by type and by runtime keys.
    for (const key of Object.keys(slice)) {
      expect([
        "run",
        "runId",
        "acceptedSequence",
        "status",
        "run_version",
      ]).not.toContain(key);
    }
  });

  it("accepts the initial snapshot and tracks records newest-first", () => {
    const slice = applyTelemetrySnapshot(
      initialTelemetrySlice(),
      parseSnapshot(
        snapshotJson([telemetryRecordJson(1_000), telemetryRecordJson(2_000)]),
      ),
    );
    expect(slice.health).toBe("live");
    expect(slice.snapshotObserved).toBe(true);
    expect(slice.records[0]?.observed_at_micros).toBe(2_000);
    expect(slice.lastObservedMicros).toBe(2_000);
  });

  it("accumulates dropped-record metadata and sampled batch counts", () => {
    let slice = applyTelemetrySnapshot(
      initialTelemetrySlice(),
      parseSnapshot(snapshotJson()),
    );
    slice = applyTelemetryBatch(
      slice,
      parseBatch(batchJson([telemetryRecordJson(2_000)], 3)),
    );
    slice = applyTelemetryBatch(
      slice,
      parseBatch(batchJson([telemetryRecordJson(3_000)], 4)),
    );
    expect(slice.droppedTotal).toBe(7);
    expect(slice.sampledBatches).toBe(2);
  });

  it("drops stale and out-of-order observations", () => {
    let slice = applyTelemetrySnapshot(
      initialTelemetrySlice(),
      parseSnapshot(snapshotJson([telemetryRecordJson(5_000)])),
    );
    slice = applyTelemetryBatch(
      slice,
      parseBatch(
        batchJson([
          telemetryRecordJson(4_000),
          telemetryRecordJson(6_000),
          telemetryRecordJson(6_000),
        ]),
      ),
    );
    expect(slice.lastObservedMicros).toBe(6_000);
    expect(slice.records.map((record) => record.observed_at_micros)).toEqual([
      6_000, 5_000,
    ]);
  });

  it("returns the same reference when a batch contains only stale records", () => {
    const slice = applyTelemetrySnapshot(
      initialTelemetrySlice(),
      parseSnapshot(snapshotJson([telemetryRecordJson(5_000)])),
    );
    const same = applyTelemetryBatch(
      slice,
      parseBatch(batchJson([telemetryRecordJson(4_000)], 0)),
    );
    expect(same).toBe(slice);
  });

  it("bounds the retained record list", () => {
    let slice = initialTelemetrySlice();
    const records: ReturnType<typeof telemetryRecordJson>[] = [];
    for (let index = 1; index <= MAX_TELEMETRY_RECORDS + 20; index += 1) {
      records.push(telemetryRecordJson(index * 100));
    }
    slice = applyTelemetryBatch(slice, parseBatch(batchJson(records)));
    expect(slice.records).toHaveLength(MAX_TELEMETRY_RECORDS);
  });

  it("tracks stale, reconnecting, disconnected, and unavailable health", () => {
    const slice = applyTelemetrySnapshot(
      initialTelemetrySlice(),
      parseSnapshot(snapshotJson()),
    );
    expect(markTelemetryStale(slice).health).toBe("stale");
    expect(markTelemetryDisconnected(slice).health).toBe("disconnected");
    expect(markTelemetryUnavailable(slice).health).toBe("unavailable");
  });
});

describe("combined live view", () => {
  it("telemetry can never write durable truth", () => {
    const durable = applyRunSnapshot(initialDurableRunState(RUN_ID), runResponse(2));
    const before = JSON.stringify(durable);

    let telemetry = applyTelemetrySnapshot(
      initialTelemetrySlice(),
      parseSnapshot(snapshotJson()),
    );
    telemetry = applyTelemetryBatch(
      telemetry,
      parseBatch(batchJson([telemetryRecordJson(99_000)], 12)),
    );
    telemetry = markTelemetryDisconnected(telemetry);

    // Durable state is untouched by construction (separate reducer output),
    // and the view reports telemetry health separately from durable status.
    expect(JSON.stringify(durable)).toBe(before);
    const view = createLiveRunView(durable, telemetry);
    expect(view.durableReady).toBe(true);
    expect(view.durable.run?.run_version).toBe(2);
    expect(view.telemetry.health).toBe("disconnected");
    expect(view.telemetryLive).toBe(false);
    expect(view.telemetry.droppedTotal).toBe(12);
  });

  it("flags durable recovery while telemetry keeps flowing", () => {
    const durable = markRecovering(
      applyRunSnapshot(initialDurableRunState(RUN_ID), runResponse(1)),
      { kind: "sequence-gap", expected: 2, received: 5 },
    );
    const telemetry = applyTelemetrySnapshot(
      initialTelemetrySlice(),
      parseSnapshot(snapshotJson()),
    );
    const view = createLiveRunView(durable, telemetry);
    expect(view.durableRecovering).toBe(true);
    expect(view.telemetryLive).toBe(true);
    expect(view.durable.run?.run_version).toBe(1);
  });
});

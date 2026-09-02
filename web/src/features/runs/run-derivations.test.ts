/**
 * Pure derivation tests for the observability screens: durable node
 * overlays that reject foreign identities, swimlane spans that pair claims
 * with outcomes, and queue panels that render absent metrics as
 * unavailable — never zero — and compute saturation only when supported.
 */
import { describe, expect, it } from "vitest";

import type { DurableEventFrame, TelemetryRecord } from "../../live/protocol";
import { initialTelemetrySlice } from "../../live/live-state";
import {
  deriveNodeOverlay,
  deriveQueuePanels,
  deriveSwimlane,
  isRunStateValue,
  runDurationSeconds,
  runStateTone,
} from "./run-derivations";

function frame(
  sequence: number,
  eventKind: string,
  overrides: Partial<DurableEventFrame> = {},
): DurableEventFrame {
  return {
    schema_version: 1,
    channel: "durable-events",
    sequence,
    run_id: "run-a",
    event_kind: eventKind,
    subject_kind: "work_item",
    subject_id: "work-1",
    occurred_at: "2026-01-01T00:00:10Z",
    payload_schema_version: 1,
    payload: {},
    ...overrides,
  };
}

describe("run state presentation", () => {
  it("maps every durable state to a non-color tone and rejects unknown values", () => {
    expect(runStateTone("running")).toBe("active");
    expect(runStateTone("succeeded")).toBe("verified");
    expect(runStateTone("partially_succeeded")).toBe("verified");
    expect(runStateTone("failed")).toBe("failure");
    expect(runStateTone("paused")).toBe("paused");
    expect(runStateTone("cancelled")).toBe("cancelled");
    expect(runStateTone("something_unheard_of")).toBe("neutral");
    expect(isRunStateValue("queued")).toBe(true);
    expect(isRunStateValue("COMPLETELY-BOGUS")).toBe(false);
  });
});

describe("deriveNodeOverlay", () => {
  it("applies events oldest to newest so the newest durable fact wins", () => {
    const events = [
      frame(3, "work_created", {
        sequence: 3,
        payload: { node_id: "nod_a" },
      }),
      frame(4, "work_claimed", {
        sequence: 4,
        payload: { node_id: "nod_a", attempt_number: 1 },
      }),
      frame(5, "work_succeeded", {
        sequence: 5,
        payload: { node_id: "nod_a", attempt_number: 1 },
      }),
    ].reverse();
    const result = deriveNodeOverlay(events, ["nod_a"]);
    expect(result.states.get("nod_a")?.state).toBe("work_succeeded");
    expect(result.states.get("nod_a")?.attempts).toBe(1);
    expect(result.states.get("nod_a")?.lastSequence).toBe(5);
    expect(result.foreignNodeEvents).toBe(0);
  });

  it("never merges node identities outside the known document", () => {
    const events = [
      frame(1, "work_claimed", {
        payload: { node_id: "nod_stranger", attempt_number: 2 },
      }),
      frame(2, "work_created", { payload: { node_id: "nod_a" } }),
    ];
    const result = deriveNodeOverlay(events, ["nod_a"]);
    expect(result.states.has("nod_stranger")).toBe(false);
    expect(result.states.get("nod_a")?.state).toBe("work_created");
    expect(result.foreignNodeEvents).toBe(1);
  });

  it("keeps the highest attempt seen for a node", () => {
    const events = [
      frame(1, "work_claimed", {
        payload: { node_id: "nod_a", attempt_number: 1 },
      }),
      frame(2, "work_failed", { payload: { node_id: "nod_a", attempt_number: 1 } }),
      frame(3, "work_claimed", {
        payload: { node_id: "nod_a", attempt_number: 2 },
      }),
      frame(4, "work_succeeded", {
        payload: { node_id: "nod_a", attempt_number: 2 },
      }),
    ].reverse();
    const result = deriveNodeOverlay(events, ["nod_a"]);
    expect(result.states.get("nod_a")?.attempts).toBe(2);
  });
});

describe("deriveSwimlane", () => {
  it("pairs a claim with its terminal outcome into one span with service time", () => {
    const events = [
      frame(1, "work_created", {
        subject_id: "work-1",
        payload: { node_id: "nod_a" },
      }),
      frame(2, "work_claimed", {
        subject_id: "work-1",
        occurred_at: "2026-01-01T00:00:10Z",
        payload: { node_id: "nod_a", attempt_number: 1, worker_identity: "worker-7" },
      }),
      frame(3, "work_succeeded", {
        subject_id: "work-1",
        occurred_at: "2026-01-01T00:00:13Z",
        payload: { node_id: "nod_a", attempt_number: 1 },
      }),
    ];
    const result = deriveSwimlane(events);
    expect(result.spans).toHaveLength(1);
    const span = result.spans[0];
    expect(span?.workItemId).toBe("work-1");
    expect(span?.nodeId).toBe("nod_a");
    expect(span?.attempt).toBe(1);
    expect(span?.worker).toBe("worker-7");
    expect(span?.startedAt).toBe("2026-01-01T00:00:10Z");
    expect(span?.finishedAt).toBe("2026-01-01T00:00:13Z");
    expect(span?.serviceSeconds).toBe(3);
    expect(span?.outcome).toBe("work_succeeded");
    expect(result.activeClaims).toBe(0);
    expect(result.unpairedEvents).toBe(0);
  });

  it("processes the live newest-first log in durable sequence order", () => {
    const events = [
      frame(1, "work_claimed", {
        subject_id: "work-newest-first",
        occurred_at: "2026-01-01T00:00:10Z",
        payload: { node_id: "nod_a", attempt_number: 1 },
      }),
      frame(2, "work_succeeded", {
        subject_id: "work-newest-first",
        occurred_at: "2026-01-01T00:00:13Z",
        payload: { node_id: "nod_a", attempt_number: 1 },
      }),
    ].reverse();
    const result = deriveSwimlane(events);
    expect(result.spans).toHaveLength(1);
    expect(result.spans[0]?.outcome).toBe("work_succeeded");
    expect(result.spans[0]?.serviceSeconds).toBe(3);
    expect(result.activeClaims).toBe(0);
    expect(result.unpairedEvents).toBe(0);
  });

  it("retains one durable span for every retry attempt", () => {
    const events = [
      frame(1, "work_claimed", {
        subject_id: "work-retry",
        occurred_at: "2026-01-01T00:00:01Z",
        payload: { node_id: "nod_a", attempt_number: 1 },
      }),
      frame(2, "work_failed", {
        subject_id: "work-retry",
        occurred_at: "2026-01-01T00:00:02Z",
        payload: { node_id: "nod_a", attempt_number: 1 },
      }),
      frame(3, "work_claimed", {
        subject_id: "work-retry",
        occurred_at: "2026-01-01T00:00:03Z",
        payload: { node_id: "nod_a", attempt_number: 2 },
      }),
      frame(4, "work_succeeded", {
        subject_id: "work-retry",
        occurred_at: "2026-01-01T00:00:05Z",
        payload: { node_id: "nod_a", attempt_number: 2 },
      }),
    ].reverse();
    const result = deriveSwimlane(events);
    expect(result.spans.map((span) => span.attempt)).toEqual([1, 2]);
    expect(result.spans.map((span) => span.outcome)).toEqual([
      "work_failed",
      "work_succeeded",
    ]);
  });

  it("keeps an unmatched claim active and counts uninterpretable work events", () => {
    const events = [
      frame(1, "work_claimed", {
        subject_id: "work-open",
        payload: { node_id: "nod_a", attempt_number: 1 },
      }),
      frame(2, "work_succeeded", {
        subject_id: "work-ghost",
        payload: { node_id: "nod_a" },
      }),
    ];
    const result = deriveSwimlane(events);
    // The unmatched claim renders as an open span; the result without a
    // seen claim is counted and never invented into a span.
    expect(result.spans).toHaveLength(1);
    expect(result.spans[0]?.workItemId).toBe("work-open");
    expect(result.spans[0]?.finishedAt).toBeNull();
    expect(result.spans[0]?.serviceSeconds).toBeNull();
    expect(result.activeClaims).toBe(1);
    expect(result.unpairedEvents).toBe(1);
  });

  it("ignores run-subject events entirely", () => {
    const result = deriveSwimlane([
      frame(1, "run_created", { subject_kind: "run", subject_id: "run-a" }),
    ]);
    expect(result.spans).toHaveLength(0);
    expect(result.activeClaims).toBe(0);
  });
});

function telemetryRecord(
  observedMicros: number,
  metrics: { name: string; value: number }[],
): TelemetryRecord {
  return {
    schema_version: 1,
    observed_at_micros: observedMicros,
    run_id: "run-a",
    metrics: metrics.map((metric) => ({
      name: metric.name,
      kind: "gauge",
      value: metric.value,
      labels: {},
    })),
  };
}

describe("deriveQueuePanels", () => {
  it("computes saturation only from a coherent depth and positive capacity", () => {
    const slice = {
      ...initialTelemetrySlice(),
      health: "live",
      lastObservedMicros: 5_000,
    } as const;
    const view = deriveQueuePanels(
      {
        ...slice,
        records: [
          telemetryRecord(5_000, [
            { name: "writer_queue_depth", value: 3 },
            { name: "writer_queue_capacity", value: 8 },
          ]),
        ],
      },
      "run-a",
    );
    const depth = view.metrics.find((metric) => metric.name === "Queue depth");
    const capacity = view.metrics.find((metric) => metric.name === "Queue capacity");
    expect(depth?.value).toBe(3);
    expect(capacity?.value).toBe(8);
    expect(depth?.unit).toBe("work items");
    expect(depth?.observedMicros).toBe(5_000);
    expect(view.saturationSupported).toBe(true);
    expect(view.saturationPercent).toBe(38);
  });

  it("renders absent metrics as unavailable and never as zero", () => {
    const view = deriveQueuePanels(initialTelemetrySlice(), "run-a");
    for (const metric of view.metrics) {
      expect(metric.value).toBeNull();
      expect(metric.observedMicros).toBeNull();
      expect(metric.source).toContain("/api/v1/live/runs/");
    }
    expect(view.saturationSupported).toBe(false);
    expect(view.saturationPercent).toBeNull();
  });

  it("refuses saturation when capacity is zero even with depth present", () => {
    const view = deriveQueuePanels(
      {
        ...initialTelemetrySlice(),
        health: "live",
        lastObservedMicros: 9,
        records: [
          telemetryRecord(9, [
            { name: "writer_queue_depth", value: 0 },
            { name: "writer_queue_capacity", value: 0 },
          ]),
        ],
      },
      "run-a",
    );
    expect(view.saturationSupported).toBe(false);
    expect(view.saturationPercent).toBeNull();
  });

  it("refuses to combine depth and capacity from different observations", () => {
    const view = deriveQueuePanels(
      {
        ...initialTelemetrySlice(),
        health: "live",
        lastObservedMicros: 10,
        records: [
          telemetryRecord(10, [{ name: "writer_queue_depth", value: 3 }]),
          telemetryRecord(9, [{ name: "writer_queue_capacity", value: 8 }]),
        ],
      },
      "run-a",
    );
    expect(view.metrics.find((metric) => metric.name === "Queue depth")?.value).toBe(3);
    expect(view.metrics.find((metric) => metric.name === "Queue capacity")?.value).toBe(
      8,
    );
    expect(view.saturationSupported).toBe(false);
    expect(view.saturationPercent).toBeNull();
  });

  it("refuses an out-of-bounds saturation observation", () => {
    const view = deriveQueuePanels(
      {
        ...initialTelemetrySlice(),
        health: "live",
        lastObservedMicros: 11,
        records: [
          telemetryRecord(11, [
            { name: "writer_queue_depth", value: 9 },
            { name: "writer_queue_capacity", value: 8 },
          ]),
        ],
      },
      "run-a",
    );
    expect(view.saturationSupported).toBe(false);
    expect(view.saturationPercent).toBeNull();
  });

  it("marks the panel stale when the telemetry health says so", () => {
    const view = deriveQueuePanels(
      { ...initialTelemetrySlice(), health: "stale" },
      "run-a",
    );
    expect(view.stale).toBe(true);
  });
});

describe("runDurationSeconds", () => {
  it("prefers started_at and reports null without a finished timestamp", () => {
    expect(
      runDurationSeconds({
        run_id: "run-a",
        run_version: 1,
        state: "succeeded",
        observed_at: "2026-01-01T00:01:00Z",
        created_at: "2026-01-01T00:00:00Z",
        started_at: "2026-01-01T00:00:02Z",
        finished_at: "2026-01-01T00:00:32Z",
        cancellation_requested_at: null,
        pipeline_id: "pip-a",
        pipeline_version: 1,
        runner_kind: "sequential",
        scenario_seed: 1,
      }),
    ).toBe(30);
    expect(
      runDurationSeconds({
        run_id: "run-a",
        run_version: 1,
        state: "running",
        observed_at: "2026-01-01T00:01:00Z",
        created_at: "2026-01-01T00:00:00Z",
        started_at: "2026-01-01T00:00:02Z",
        finished_at: null,
        cancellation_requested_at: null,
        pipeline_id: "pip-a",
        pipeline_version: 1,
        runner_kind: "sequential",
        scenario_seed: 1,
      }),
    ).toBeNull();
  });
});

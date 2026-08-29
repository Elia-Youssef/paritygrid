import { describe, expect, it } from "vitest";

import {
  DURABLE_CHANNEL,
  parseDurableFrame,
  parseTelemetryMessage,
  runSnapshotSchema,
  TELEMETRY_CHANNEL,
} from "./protocol";
import {
  batchJson,
  durableFrameJson,
  pongJson,
  RUN_ID,
  runResponse,
  snapshotJson,
  telemetryRecordJson,
} from "../test/live-fixtures";

describe("durable frame validation", () => {
  it("accepts a canonical durable envelope", () => {
    const result = parseDurableFrame(JSON.parse(durableFrameJson(4)) as unknown);
    expect(result.ok).toBe(true);
    if (result.ok) {
      expect(result.frame.sequence).toBe(4);
      expect(result.frame.channel).toBe(DURABLE_CHANNEL);
      expect(result.frame.run_id).toBe(RUN_ID);
    }
  });

  it("rejects non-object input", () => {
    for (const input of [null, 42, "frame", [], true]) {
      expect(parseDurableFrame(input).ok).toBe(false);
    }
  });

  it("rejects wrong schema version and channel", () => {
    expect(
      parseDurableFrame(
        JSON.parse(durableFrameJson(1, { schema_version: 2 })) as unknown,
      ).ok,
    ).toBe(false);
    expect(
      parseDurableFrame(
        JSON.parse(durableFrameJson(1, { channel: "telemetry" })) as unknown,
      ).ok,
    ).toBe(false);
  });

  it("rejects non-integer, zero, and overflowing sequences", () => {
    expect(parseDurableFrame(JSON.parse(durableFrameJson(0)) as unknown).ok).toBe(
      false,
    );
    expect(
      parseDurableFrame(JSON.parse(durableFrameJson(1, { sequence: 1.5 })) as unknown)
        .ok,
    ).toBe(false);
    expect(
      parseDurableFrame(
        JSON.parse(durableFrameJson(1, { sequence: 2_147_483_648 })) as unknown,
      ).ok,
    ).toBe(false);
    expect(
      parseDurableFrame(JSON.parse(durableFrameJson(1, { sequence: "3" })) as unknown)
        .ok,
    ).toBe(false);
  });

  it("rejects hostile payloads without coercing them", () => {
    const hostile = JSON.parse(durableFrameJson(1, { payload: undefined })) as Record<
      string,
      unknown
    >;
    expect(parseDurableFrame(hostile).ok).toBe(false);
    expect(
      parseDurableFrame(JSON.parse(durableFrameJson(1, { run_id: "" })) as unknown).ok,
    ).toBe(false);
  });

  it("keeps an unknown payload member out of the validated frame type", () => {
    const result = parseDurableFrame(
      JSON.parse(
        durableFrameJson(1, { payload: { extra: { nested: true } } }),
      ) as unknown,
    );
    expect(result.ok).toBe(true);
    if (result.ok) {
      // The frame carries the payload verbatim for kind-specific handling;
      // identity and ordering members are the only trusted fields.
      expect(result.frame.payload).toEqual({ extra: { nested: true } });
    }
  });
});

describe("run snapshot schema", () => {
  it("accepts a canonical run projection", () => {
    const parsed = runSnapshotSchema.safeParse(runResponse(2));
    expect(parsed.success).toBe(true);
  });

  it("rejects incompatible identity members", () => {
    expect(
      runSnapshotSchema.safeParse({ ...runResponse(2), run_version: 1.5 }).success,
    ).toBe(false);
    expect(
      runSnapshotSchema.safeParse({ ...runResponse(2), pipeline_version: 0 }).success,
    ).toBe(false);
    expect(
      runSnapshotSchema.safeParse({ ...runResponse(2), scenario_seed: "seven" })
        .success,
    ).toBe(false);
  });
});

describe("telemetry message validation", () => {
  it("classifies the initial snapshot", () => {
    const result = parseTelemetryMessage(snapshotJson());
    expect(result.ok).toBe(true);
    if (result.ok) {
      expect(result.message.kind).toBe("snapshot");
      if (result.message.kind === "snapshot") {
        expect(result.message.frame.channel).toBe(TELEMETRY_CHANNEL);
        expect(result.message.frame.advisory).toBe(true);
      }
    }
  });

  it("classifies a sampled batch with dropped metadata", () => {
    const result = parseTelemetryMessage(batchJson([telemetryRecordJson(2_000)], 4));
    expect(result.ok).toBe(true);
    if (result.ok && result.message.kind === "batch") {
      expect(result.message.frame.sampled).toBe(true);
      expect(result.message.frame.dropped).toBe(4);
    } else {
      expect.unreachable("expected a batch frame");
    }
  });

  it("classifies pong frames", () => {
    const result = parseTelemetryMessage(pongJson);
    expect(result.ok).toBe(true);
    if (result.ok) {
      expect(result.message.kind).toBe("pong");
    }
  });

  it("rejects non-text and non-JSON messages", () => {
    expect(parseTelemetryMessage(42).ok).toBe(false);
    expect(parseTelemetryMessage("{not json").ok).toBe(false);
  });

  it("rejects a durable-events envelope sent over the live channel", () => {
    const result = parseTelemetryMessage(durableFrameJson(1));
    expect(result.ok).toBe(false);
  });

  it("rejects snapshots missing the advisory marker or wrong version", () => {
    const noAdvisory = JSON.stringify({
      schema_version: 1,
      channel: "telemetry",
      records: [],
    });
    expect(parseTelemetryMessage(noAdvisory).ok).toBe(false);
    const wrongVersion = JSON.stringify({
      schema_version: 2,
      channel: "telemetry",
      advisory: true,
      records: [],
    });
    expect(parseTelemetryMessage(wrongVersion).ok).toBe(false);
  });

  it("rejects batches with a false sampled marker", () => {
    const frame = JSON.stringify({
      schema_version: 1,
      channel: "telemetry",
      advisory: true,
      sampled: false,
      dropped: 0,
      records: [],
    });
    expect(parseTelemetryMessage(frame).ok).toBe(false);
  });

  it("rejects records with malformed observation timestamps", () => {
    expect(parseTelemetryMessage(snapshotJson([telemetryRecordJson(-1)])).ok).toBe(
      false,
    );
    expect(parseTelemetryMessage(snapshotJson([telemetryRecordJson(1.5)])).ok).toBe(
      false,
    );
    expect(
      parseTelemetryMessage(snapshotJson([telemetryRecordJson(10, { run_id: "" })])).ok,
    ).toBe(false);
  });
});

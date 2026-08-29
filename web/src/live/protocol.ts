/**
 * Runtime validation for the two live transports. REST, SSE, and WebSocket
 * input is untrusted: generated TypeScript types describe shapes at compile
 * time only, so every stream frame is parsed here with Zod before any state
 * can see it. Unknown members are stripped (never merged into state); a
 * frame that fails validation is rejected whole — nothing is coerced.
 *
 * Wire shapes mirror src/paritygrid/api/schemas/streaming.py, where the
 * durable SSE envelope and the advisory telemetry envelope are deliberately
 * distinct.
 */
import { z } from "zod";

import { runResponseSchema } from "../api/runtime-schemas";

export const DURABLE_CHANNEL = "durable-events";
export const TELEMETRY_CHANNEL = "telemetry";

export const MIN_SEQUENCE = 1;

/** Per-run durable sequence numbers use the same bound as the server. */
export const DURABLE_SEQUENCE_MAX = 2_147_483_647;

const boundedVersion = z.number().int().min(1).max(1_000_000);

export const durableEventFrameSchema = z
  .object({
    schema_version: z.literal(1),
    channel: z.literal(DURABLE_CHANNEL),
    sequence: z.number().int().min(MIN_SEQUENCE).max(DURABLE_SEQUENCE_MAX),
    run_id: z.string().min(1).max(256),
    event_kind: z.string().min(1).max(128),
    subject_kind: z.string().min(1).max(128),
    subject_id: z.string().min(1).max(256),
    occurred_at: z.string().min(1).max(64),
    correlation_id: z.string().max(128).nullish(),
    payload_schema_version: z.literal(1),
    payload: z.record(z.string(), z.unknown()),
  })
  .strict();

export type DurableEventFrame = z.infer<typeof durableEventFrameSchema>;

/**
 * Validation for an authoritative run projection embedded in a durable
 * payload. The inferred type is structurally assignable to the generated
 * `RunResponse`, so validated projections can flow into durable state
 * without any type assertion.
 */
export const runSnapshotSchema = runResponseSchema;

export const telemetryMetricSchema = z
  .object({
    name: z.string().min(1).max(128),
    kind: z.string().min(1).max(64),
    value: z.number().int().min(-2_147_483_648).max(2_147_483_647),
    labels: z.record(z.string(), z.string()),
  })
  .strict();

export const telemetryRecordSchema = z
  .object({
    schema_version: boundedVersion,
    observed_at_micros: z.number().int().min(0),
    run_id: z.string().min(1).max(256),
    metrics: z.array(telemetryMetricSchema).max(256),
  })
  .strict();

export type TelemetryRecord = z.infer<typeof telemetryRecordSchema>;

/** First frame after the socket opens: a bounded snapshot, never sampled. */
export const telemetrySnapshotSchema = z
  .object({
    schema_version: z.literal(1),
    channel: z.literal(TELEMETRY_CHANNEL),
    advisory: z.literal(true),
    records: z.array(telemetryRecordSchema).max(256),
  })
  .strict();

export type TelemetrySnapshot = z.infer<typeof telemetrySnapshotSchema>;

/** Later batches: unmistakably marked advisory, sampled, and lossy. */
export const telemetryBatchSchema = z
  .object({
    schema_version: z.literal(1),
    channel: z.literal(TELEMETRY_CHANNEL),
    advisory: z.literal(true),
    sampled: z.literal(true),
    dropped: z.number().int().min(0),
    records: z.array(telemetryRecordSchema).max(256),
  })
  .strict();

export type TelemetryBatch = z.infer<typeof telemetryBatchSchema>;

/** Reply to the client's only allowed message, {"type": "ping"}. */
export const telemetryPongSchema = z
  .object({
    schema_version: z.literal(1),
    channel: z.literal(TELEMETRY_CHANNEL),
    type: z.literal("pong"),
  })
  .strict();

export type TelemetryPong = z.infer<typeof telemetryPongSchema>;

export type ValidatedTelemetryFrame =
  | { kind: "snapshot"; frame: TelemetrySnapshot }
  | { kind: "batch"; frame: TelemetryBatch }
  | { kind: "pong"; frame: TelemetryPong };

export type FrameRejection =
  { ok: true; frame: DurableEventFrame } | { ok: false; reason: string };

function rejection(error: z.ZodError): { ok: false; reason: string } {
  const first = error.issues[0];
  const path = first === undefined ? "<root>" : first.path.join(".") || "<root>";
  return { ok: false, reason: `${path}: ${first?.message ?? "invalid frame"}` };
}

/**
 * Validate one SSE `data:` payload as a durable event frame. The SSE event
 * name and id line are validated by the stream client against this frame.
 */
export function parseDurableFrame(input: unknown): FrameRejection {
  const parsed = durableEventFrameSchema.safeParse(input);
  return parsed.success ? { ok: true, frame: parsed.data } : rejection(parsed.error);
}

export type TelemetryParse =
  { ok: true; message: ValidatedTelemetryFrame } | { ok: false; reason: string };

/**
 * Classify and validate one WebSocket text message. Distinguishes the
 * initial snapshot, sampled batches, and pong frames; anything else —
 * including a durable-events envelope sent over the live channel — is
 * rejected so the two transports can never be confused.
 */
export function parseTelemetryMessage(input: unknown): TelemetryParse {
  if (typeof input !== "string") {
    return { ok: false, reason: "telemetry frame must be a JSON text message" };
  }

  let parsedJson: unknown;
  try {
    parsedJson = JSON.parse(input) as unknown;
  } catch {
    return { ok: false, reason: "telemetry frame is not valid JSON" };
  }

  if (
    typeof parsedJson === "object" &&
    parsedJson !== null &&
    !Array.isArray(parsedJson) &&
    (parsedJson as Record<string, unknown>).type === "pong"
  ) {
    const pong = telemetryPongSchema.safeParse(parsedJson);
    return pong.success
      ? { ok: true, message: { kind: "pong", frame: pong.data } }
      : rejection(pong.error);
  }

  const asRecord =
    typeof parsedJson === "object" && parsedJson !== null && !Array.isArray(parsedJson)
      ? (parsedJson as Record<string, unknown>)
      : undefined;
  if (asRecord !== undefined && typeof asRecord.sampled === "boolean") {
    const batch = telemetryBatchSchema.safeParse(parsedJson);
    return batch.success
      ? { ok: true, message: { kind: "batch", frame: batch.data } }
      : rejection(batch.error);
  }

  const snapshot = telemetrySnapshotSchema.safeParse(parsedJson);
  return snapshot.success
    ? { ok: true, message: { kind: "snapshot", frame: snapshot.data } }
    : rejection(snapshot.error);
}

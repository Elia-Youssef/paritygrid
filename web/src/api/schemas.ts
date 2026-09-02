/**
 * Complete, strict runtime schemas for every REST response Phase 16
 * consumes. Generated TypeScript declarations describe shapes at compile
 * time only; each response is parsed here in full — including nested
 * documents — before it can reach query state. Unknown members, wrong
 * versions, malformed identifiers/timestamps/hashes, negative or fractional
 * versions, and mismatched publication frontiers are rejected whole: no
 * defaults are substituted for authoritative data.
 *
 * Types are structurally tied to `./generated/schema` via the compile-time
 * identity assertions in `schemas.test.ts`.
 */
import { z } from "zod";

import {
  PIPELINE_PLANNER_FORMAT_VERSION,
  pipelineDocumentSchema,
} from "../pipelines/document";

// ---------------------------------------------------------------------------
// Shared primitives
// ---------------------------------------------------------------------------

/** Server timestamps render as `YYYY-MM-DD HH:MM:SS[.frac][Z|±HH:MM]`. */
export const timestampSchema = z
  .string()
  .regex(/^\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})?$/, {
    message: "must be a UTC or offset timestamp",
  });

const prefixedId = (prefix: string) =>
  z.string().regex(new RegExp(`^${prefix}_[a-z0-9]+(?:-[a-z0-9]+)*$`), {
    message: `must be a canonical ${prefix}_ identifier`,
  });

export const pipelineIdSchema = prefixedId("pip");
const connectorIdSchema = prefixedId("con");
const runIdSchema = prefixedId("run");

const sha256Schema = z.string().regex(/^[0-9a-f]{64}$/, {
  message: "must be a lowercase 64-hex digest",
});

const schemaVersion1 = z.literal(1);
const positiveVersion = z.number().int().min(1).max(2_147_483_647);

export const RUN_STATES = [
  "queued",
  "running",
  "pausing",
  "paused",
  "resuming",
  "succeeded",
  "partially_succeeded",
  "failed",
  "cancelling",
  "cancelled",
] as const;
export type RunStateValue = (typeof RUN_STATES)[number];
export const runStateSchema = z.enum(RUN_STATES);

export const connectorCapabilitiesSchema = z
  .object({
    read: z.boolean().optional(),
    write: z.boolean().optional(),
    async_io: z.boolean().optional(),
    blocking_io: z.boolean().optional(),
    idempotency: z.boolean().optional(),
    schema_discovery: z.boolean().optional(),
  })
  .strict();

const connectorSnapshotCapabilitiesSchema = z
  .object({
    read: z.boolean(),
    write: z.boolean(),
    async_io: z.boolean(),
    blocking_io: z.boolean(),
    idempotency: z.boolean(),
    schema_discovery: z.boolean(),
  })
  .strict();

const openDocument = z.record(z.string(), z.unknown());

const connectorReferenceSnapshotSchema = z
  .object({
    reference_name: z
      .string()
      .min(1)
      .max(128)
      .regex(/^[a-z][a-z0-9]*(?:[._-][a-z0-9]+)*$/),
    environment_variable_name: z
      .string()
      .min(1)
      .max(128)
      .regex(/^[A-Z][A-Z0-9_]*$/),
  })
  .strict();

const connectorBindingSnapshotSchema = z
  .object({
    connector_id: connectorIdSchema,
    kind: z
      .string()
      .min(1)
      .max(96)
      .regex(/^[a-z][a-z0-9]*(?:[._-][a-z0-9]+)*$/),
    revision: positiveVersion,
    configuration: openDocument,
    capabilities: connectorSnapshotCapabilitiesSchema,
    schema_discovery: openDocument.nullable(),
    secret_references: z.array(connectorReferenceSnapshotSchema).max(64),
  })
  .strict();

export const publishedPipelineSpecificationSchema = z
  .object({
    published_specification_version: schemaVersion1,
    pipeline: pipelineDocumentSchema,
    connector_bindings: z.array(connectorBindingSnapshotSchema).max(256),
  })
  .strict();

// ---------------------------------------------------------------------------
// Pipelines
// ---------------------------------------------------------------------------

export const pipelineResponseSchema = z
  .object({
    schema_version: schemaVersion1,
    pipeline_id: pipelineIdSchema,
    display_name: z.string().min(1).max(128),
    description: z.string().min(1).max(1_024).nullable(),
    created_at: timestampSchema,
    archived_at: timestampSchema.nullable(),
    row_version: z.number().int().min(1),
  })
  .strict();

export type PipelineResponseValue = z.infer<typeof pipelineResponseSchema>;

export const pipelinePageSchema = z
  .object({
    schema_version: schemaVersion1,
    items: z.array(pipelineResponseSchema).max(100),
    limit: z.number().int().min(1).max(100),
    next_cursor: pipelineIdSchema.nullable(),
  })
  .strict();

/** Publication acknowledgement: validated, never trusted from generated types. */
export const pipelineVersionAckSchema = z
  .object({
    schema_version: schemaVersion1,
    pipeline_id: pipelineIdSchema,
    version: positiveVersion,
    specification_sha256: sha256Schema,
    planner_format_version: z
      .number()
      .int()
      .min(1)
      .max(PIPELINE_PLANNER_FORMAT_VERSION),
    published_at: timestampSchema,
  })
  .strict();

export type PipelineVersionAckValue = z.infer<typeof pipelineVersionAckSchema>;

/** The immutable latest-version frontier; 0 means never published. */
export const pipelineVersionFrontierSchema = z
  .object({
    schema_version: schemaVersion1,
    pipeline_id: pipelineIdSchema,
    latest_version: z.number().int().min(0).max(2_147_483_647),
    published: z.boolean(),
  })
  .strict()
  .refine((frontier) => frontier.published === frontier.latest_version > 0, {
    message: "published must exactly reflect the frontier",
  });

export type PipelineVersionFrontierValue = z.infer<
  typeof pipelineVersionFrontierSchema
>;

/** One immutable published version; the embedded document is fully parsed. */
export const pipelineVersionSchema = z
  .object({
    schema_version: schemaVersion1,
    pipeline_id: pipelineIdSchema,
    version: positiveVersion,
    specification: publishedPipelineSpecificationSchema,
    specification_sha256: sha256Schema,
    planner_format_version: z
      .number()
      .int()
      .min(1)
      .max(PIPELINE_PLANNER_FORMAT_VERSION),
    published_at: timestampSchema,
  })
  .strict();

export type PipelineVersionValue = z.infer<typeof pipelineVersionSchema>;

// ---------------------------------------------------------------------------
// Connectors (inspector bindings)
// ---------------------------------------------------------------------------

export const connectorResponseSchema = z
  .object({
    schema_version: schemaVersion1,
    connector_id: connectorIdSchema,
    kind: z.string().min(1).max(128),
    display_name: z.string().min(1).max(128),
    created_at: timestampSchema,
    updated_at: timestampSchema,
    archived_at: timestampSchema.nullable(),
    revision: z.number().int().min(1),
    row_version: z.number().int().min(1),
    capabilities: connectorCapabilitiesSchema,
    configuration: openDocument,
    schema_discovery: openDocument.nullable(),
    secret_references: z
      .array(
        z
          .object({
            reference_name: z.string().min(1).max(128),
            environment_variable_name: z.string().min(1).max(128),
          })
          .strict(),
      )
      .max(64),
  })
  .strict();

export type ConnectorResponseValue = z.infer<typeof connectorResponseSchema>;

export const connectorPageSchema = z
  .object({
    schema_version: schemaVersion1,
    items: z.array(connectorResponseSchema).max(100),
    limit: z.number().int().min(1).max(100),
    next_cursor: connectorIdSchema.nullable(),
  })
  .strict();

// ---------------------------------------------------------------------------
// Operations overview (durable REST only)
// ---------------------------------------------------------------------------

export const healthSchema = z
  .object({
    status: z.enum(["ok"]),
    service: z.string().min(1).max(128),
    version: z.string().min(1).max(64),
  })
  .strict();

export const readinessSchema = z
  .object({
    status: z.enum(["ready", "not_ready"]),
    service: z.string().min(1).max(128),
    version: z.string().min(1).max(64),
    detail: z.string().min(1).max(1_024),
  })
  .strict();

export const runResponseSchema = z
  .object({
    schema_version: schemaVersion1,
    run_id: runIdSchema,
    run_version: z.number().int().min(0),
    state: runStateSchema,
    observed_at: timestampSchema,
    created_at: timestampSchema,
    started_at: timestampSchema.nullable(),
    finished_at: timestampSchema.nullable(),
    cancellation_requested_at: timestampSchema.nullable(),
    pipeline_id: pipelineIdSchema,
    pipeline_version: positiveVersion,
    runner_kind: z.string().min(1).max(64),
    scenario_seed: z.number().int().nullable(),
    execution_evidence_fingerprint: z
      .string()
      .regex(/^[0-9a-f]{64}$/)
      .nullable()
      .optional(),
    execution_evidence_fingerprint_version: z
      .number()
      .int()
      .min(1)
      .nullable()
      .optional(),
  })
  .strict()
  .superRefine((run, context) => {
    const fingerprintPresent = run.execution_evidence_fingerprint != null;
    const versionPresent = run.execution_evidence_fingerprint_version != null;
    if (fingerprintPresent !== versionPresent) {
      context.addIssue({
        code: "custom",
        message: "execution-evidence fingerprint and version must be present together",
        path: ["execution_evidence_fingerprint"],
      });
    }
    if (
      fingerprintPresent &&
      run.state !== "succeeded" &&
      run.state !== "partially_succeeded"
    ) {
      context.addIssue({
        code: "custom",
        message: "execution evidence is only valid for an evidence-finalized run",
        path: ["state"],
      });
    }
    if (
      ["succeeded", "partially_succeeded", "failed", "cancelled"].includes(run.state) &&
      run.finished_at === null
    ) {
      context.addIssue({
        code: "custom",
        message: "terminal runs must include a finish timestamp",
        path: ["finished_at"],
      });
    }
  });

export type RunResponseValue = z.infer<typeof runResponseSchema>;

export const runPageSchema = z
  .object({
    schema_version: schemaVersion1,
    items: z.array(runResponseSchema).max(100),
    limit: z.number().int().min(1).max(100),
    next_cursor: runIdSchema.nullable(),
  })
  .strict();

export const operationalLimitsSchema = z
  .object({
    schema_version: schemaVersion1,
    artifact_chunk_bytes: z.number().int().min(1),
    // RuntimeSettings exposes these as bounded floats. Preserve fractional
    // values while rejecting measurements the backend cannot produce.
    idempotency_lease_seconds: z.number().positive().max(86_400),
    max_concurrent_requests: z.number().int().min(1),
    max_json_depth: z.number().int().min(1),
    max_page_size: z.number().int().min(1),
    max_request_body_bytes: z.number().int().min(1),
    request_timeout_seconds: z.number().min(0.1).max(300),
  })
  .strict()
  .superRefine((limits, context) => {
    if (limits.idempotency_lease_seconds <= limits.request_timeout_seconds) {
      context.addIssue({
        code: "custom",
        message: "idempotency lease must exceed the request timeout",
        path: ["idempotency_lease_seconds"],
      });
    }
  });

export const runnerStrategySchema = z
  .object({
    schema_version: schemaVersion1,
    strategy_id: z.string().min(1).max(64),
    available: z.boolean(),
    unavailability_reason: z.string().max(1_024).nullable().optional(),
  })
  .strict();

export const capabilitiesSchema = z
  .object({
    schema_version: schemaVersion1,
    service: z.string().min(1).max(128),
    version: z.string().min(1).max(64),
    runners: z.array(runnerStrategySchema).max(64),
    subordinate_pools: z
      .array(
        z
          .object({
            schema_version: schemaVersion1,
            pool_id: z.string().min(1).max(64),
            available: z.boolean(),
            unavailability_reason: z.string().max(1_024).nullable().optional(),
          })
          .strict(),
      )
      .max(64),
    features: z
      .array(
        z.object({ name: z.string().min(1).max(128), available: z.boolean() }).strict(),
      )
      .max(128),
    sqlite: z
      .object({
        schema_version: schemaVersion1,
        library_version: z.string().min(1).max(64),
        minimum_supported_version: z.string().min(1).max(64),
        journal_mode: z.string().min(1).max(32),
        synchronous_level: z.number().int().min(0),
        // The runtime always reports its configured SQLite busy timeout.
        busy_timeout_ms: z.number().int().min(0),
        threadsafety: z.number().int().min(0),
        supports_json_sql: z.boolean(),
        supports_returning: z.boolean(),
      })
      .strict(),
    limits: operationalLimitsSchema,
  })
  .strict();

export type CapabilitiesValue = z.infer<typeof capabilitiesSchema>;

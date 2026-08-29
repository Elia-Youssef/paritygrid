/**
 * Runtime validators for JSON returned by the generated REST boundary.
 *
 * The OpenAPI-generated declarations provide compile-time types only. These
 * schemas validate every required member before untrusted JSON is narrowed to
 * a generated response type. `satisfies ZodType<GeneratedType>` keeps each
 * validator coupled to the generated contract when that contract changes.
 */
import { z } from "zod";

import type {
  CapabilitiesResponse,
  ConflictPageResponse,
  HealthResponse,
  ReconciliationResponse,
  RepairPlanResponse,
  RunPageResponse,
  RunResponse,
} from "./generated/schema";

const schemaVersion = z.literal(1).optional();
const identifier = z.string().min(1).max(256);
const shortText = z.string().min(1).max(256);
const timestamp = z.string().min(1).max(64);
const fingerprint = z.string().min(1).max(128);
const nonNegativeInteger = z.number().int().min(0);
const positiveInteger = z.number().int().min(1);

export const runResponseSchema = z
  .object({
    cancellation_requested_at: timestamp.nullable(),
    created_at: timestamp,
    execution_evidence_fingerprint: fingerprint.nullable().optional(),
    execution_evidence_fingerprint_version: nonNegativeInteger.nullable().optional(),
    finished_at: timestamp.nullable(),
    observed_at: timestamp,
    pipeline_id: identifier,
    pipeline_version: positiveInteger,
    run_id: identifier,
    run_version: nonNegativeInteger,
    runner_kind: shortText,
    scenario_seed: z.number().int().nullable(),
    schema_version: schemaVersion,
    started_at: timestamp.nullable(),
    state: shortText,
  })
  .strict() satisfies z.ZodType<RunResponse>;

export const runPageResponseSchema = z
  .object({
    items: z.array(runResponseSchema).max(100),
    limit: z.number().int().min(1).max(100),
    next_cursor: z.string().min(1).max(4096).nullable(),
    schema_version: schemaVersion,
  })
  .strict() satisfies z.ZodType<RunPageResponse>;

export const healthResponseSchema = z
  .object({
    service: shortText,
    status: z.literal("ok").optional(),
    version: shortText,
  })
  .strict() satisfies z.ZodType<HealthResponse>;

const featureSchema = z.object({ available: z.boolean(), name: shortText }).strict();
const runnerSchema = z
  .object({
    available: z.boolean(),
    schema_version: schemaVersion,
    strategy_id: identifier,
    unavailability_reason: z.string().max(1024).nullable().optional(),
  })
  .strict();
const subordinatePoolSchema = z
  .object({
    available: z.boolean(),
    pool_id: identifier,
    schema_version: schemaVersion,
    unavailability_reason: z.string().max(1024).nullable().optional(),
  })
  .strict();
const limitsSchema = z
  .object({
    artifact_chunk_bytes: positiveInteger,
    idempotency_lease_seconds: z.number().positive(),
    max_concurrent_requests: positiveInteger,
    max_json_depth: positiveInteger,
    max_page_size: positiveInteger,
    max_request_body_bytes: positiveInteger,
    request_timeout_seconds: z.number().positive(),
    schema_version: schemaVersion,
  })
  .strict();
const sqliteSchema = z
  .object({
    busy_timeout_ms: nonNegativeInteger,
    journal_mode: shortText,
    library_version: shortText,
    minimum_supported_version: shortText,
    schema_version: schemaVersion,
    supports_json_sql: z.boolean(),
    supports_returning: z.boolean(),
    synchronous_level: nonNegativeInteger,
    threadsafety: nonNegativeInteger,
  })
  .strict();

export const capabilitiesResponseSchema = z
  .object({
    features: z.array(featureSchema).max(256),
    limits: limitsSchema,
    runners: z.array(runnerSchema).max(256),
    schema_version: schemaVersion,
    service: shortText,
    sqlite: sqliteSchema,
    subordinate_pools: z.array(subordinatePoolSchema).max(256),
    version: shortText,
  })
  .strict() satisfies z.ZodType<CapabilitiesResponse>;

export const reconciliationResponseSchema = z
  .object({
    analytical_query_version: positiveInteger,
    counts: z.record(z.string().min(1).max(128), nonNegativeInteger),
    observed_at: timestamp,
    reconciliation_fingerprint: fingerprint,
    reconciliation_observed_at: timestamp,
    run_id: identifier,
    run_version: nonNegativeInteger,
    schema_version: schemaVersion,
    source_input_identity: fingerprint,
    state: shortText,
    target_input_identity: fingerprint,
    total_count: nonNegativeInteger,
  })
  .strict() satisfies z.ZodType<ReconciliationResponse>;

const conflictReferenceSchema = z
  .object({ position: nonNegativeInteger, record_key: identifier })
  .strict();
const conflictDifferenceSchema = z
  .object({
    field: shortText,
    kind: shortText,
    source_text: z.string().max(65_536),
    target_text: z.string().max(65_536),
  })
  .strict();
const conflictSchema = z
  .object({
    canonical_key: identifier,
    classification: shortText,
    conflict_id: identifier,
    created_at: timestamp,
    differences: z.array(conflictDifferenceSchema).max(1024),
    schema_version: schemaVersion,
    source_references: z.array(conflictReferenceSchema).max(1024),
    suggested_resolution: z.string().max(65_536).nullable(),
    target_references: z.array(conflictReferenceSchema).max(1024),
  })
  .strict();

export const conflictPageResponseSchema = z
  .object({
    items: z.array(conflictSchema).max(100),
    limit: z.number().int().min(1).max(100),
    next_cursor: z.string().min(1).max(4096).nullable(),
    observed_at: timestamp,
    reconciliation_fingerprint: fingerprint,
    run_id: identifier,
    run_version: nonNegativeInteger,
    schema_version: schemaVersion,
    state: shortText,
  })
  .strict() satisfies z.ZodType<ConflictPageResponse>;

const repairActionSchema = z
  .object({
    action_id: identifier,
    applied_at: timestamp.nullable().optional(),
    before_sha256: fingerprint,
    canonical_key: identifier,
    failed_at: timestamp.nullable().optional(),
    kind: shortText,
    proposed_after_sha256: fingerprint,
    schema_version: schemaVersion,
    status: shortText,
    target_version: nonNegativeInteger.nullable().optional(),
  })
  .strict();
const repairApprovalSchema = z
  .object({
    approval_schema_version: positiveInteger,
    approved_at: timestamp,
    approved_by: identifier,
    correlation_id: identifier,
    schema_version: schemaVersion,
  })
  .strict();

export const repairPlanResponseSchema = z
  .object({
    actions: z.array(repairActionSchema).max(10_000),
    applied_at: timestamp.nullable().optional(),
    applying_at: timestamp.nullable().optional(),
    approval: repairApprovalSchema.nullable().optional(),
    content_fingerprint: fingerprint,
    created_at: timestamp,
    failed_at: timestamp.nullable().optional(),
    observed_at: timestamp,
    plan_id: identifier,
    reconciliation_fingerprint: fingerprint,
    rejected_at: timestamp.nullable().optional(),
    run_id: identifier,
    run_version: nonNegativeInteger,
    schema_version: schemaVersion,
    state: shortText,
    status: shortText,
  })
  .strict() satisfies z.ZodType<RepairPlanResponse>;

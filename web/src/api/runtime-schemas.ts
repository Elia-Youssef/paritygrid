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
  ConflictDifference,
  ConflictPageResponse,
  ConflictReference,
  ConflictResponse,
  HealthResponse,
  ReconciliationResponse,
  RepairActionResponse,
  RepairApplyResponse,
  RepairPlanResponse,
  RunPageResponse,
  RunResponse,
} from "./generated/schema";

const schemaVersion = z.literal(1).optional();
const identifier = z.string().min(1).max(256);
const shortText = z.string().min(1).max(256);
const timestamp = z.string().min(1).max(64);
const fingerprint = z.string().min(1).max(128);
/** Lowercase 64-hex digest, the format the server emits for every coherence hash. */
const sha256 = z.string().regex(/^[0-9a-f]{64}$/);
const nonNegativeInteger = z.number().int().min(0);
const positiveInteger = z.number().int().min(1);

/**
 * Closed reconciliation vocabulary, mirroring the backend
 * `CLASSIFICATION_VALUES` contract. Unknown classification keys must fail
 * the parse instead of being silently ignored: an unrecognized class would
 * otherwise hide conflicts from operators.
 */
export const RECONCILIATION_CLASSIFICATION_KEYS = [
  "match",
  "missing_from_target",
  "missing_from_source",
  "field_mismatch",
  "duplicate_source",
  "duplicate_target",
  "duplicate_both",
] as const;
export type ReconciliationClassificationKey =
  (typeof RECONCILIATION_CLASSIFICATION_KEYS)[number];

/** Closed field-difference kinds (backend `FieldDifferenceKind`). */
export const FIELD_DIFFERENCE_KIND_KEYS = [
  "value_mismatch",
  "missing_on_source",
  "missing_on_target",
  "null_on_source",
  "null_on_target",
  "type_mismatch",
] as const;
export type FieldDifferenceKindKey = (typeof FIELD_DIFFERENCE_KIND_KEYS)[number];

/** Closed review dispositions (backend `SuggestedResolution`). */
export const SUGGESTED_RESOLUTION_KEYS = [
  "none",
  "create_target",
  "update_target",
  "review_target_only",
  "review_duplicates",
] as const;
export type SuggestedResolutionKey = (typeof SUGGESTED_RESOLUTION_KEYS)[number];

/** Stored repair-plan lifecycle (backend `RepairPlanStatus`). */
export const REPAIR_PLAN_STATUS_KEYS = [
  "proposed",
  "approved",
  "applying",
  "applied",
  "rejected",
  "failed",
] as const;
export type RepairPlanStatusKey = (typeof REPAIR_PLAN_STATUS_KEYS)[number];

/** Per-action application result (backend `RepairActionStatus`). */
export const REPAIR_ACTION_STATUS_KEYS = ["pending", "applied", "failed"] as const;
export type RepairActionStatusKey = (typeof REPAIR_ACTION_STATUS_KEYS)[number];

/** Closed outcomes of one application attempt (backend disposition set). */
export const REPAIR_APPLY_DISPOSITION_KEYS = [
  "completed",
  "already_applied",
  "failed",
  "unresolved",
  "interrupted",
] as const;
export type RepairApplyDispositionKey = (typeof REPAIR_APPLY_DISPOSITION_KEYS)[number];

/** Whether `value` is a lowercase 64-hex digest as emitted by the server. */
export function isLowercaseSha256Hex(value: string): boolean {
  return /^[0-9a-f]{64}$/.test(value);
}

const CLASSIFICATION_KEY_SET: ReadonlySet<string> = new Set(
  RECONCILIATION_CLASSIFICATION_KEYS,
);

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
    reconciliation_fingerprint: sha256,
    reconciliation_observed_at: timestamp,
    run_id: identifier,
    run_version: nonNegativeInteger,
    schema_version: schemaVersion,
    source_input_identity: sha256,
    state: shortText,
    target_input_identity: sha256,
    total_count: nonNegativeInteger,
  })
  .strict()
  // The wire record stays open at the type level (the generated contract is
  // `dict[str, int]`), but every key must belong to the classification closed
  // set: an unknown class in a counts payload is a contract break and is
  // rejected visibly rather than dropped from the rendered totals.
  .superRefine((response, context) => {
    for (const key of RECONCILIATION_CLASSIFICATION_KEYS) {
      if (!(key in response.counts)) {
        context.addIssue({
          code: "custom",
          message: `counts is missing required reconciliation classification "${key}"`,
          path: ["counts", key],
        });
      }
    }
    for (const key of Object.keys(response.counts)) {
      if (!CLASSIFICATION_KEY_SET.has(key)) {
        context.addIssue({
          code: "custom",
          message: `counts key "${key}" is outside the reconciliation classification closed set`,
          path: ["counts", key],
        });
      }
    }
    const countedTotal = Object.values(response.counts).reduce(
      (total, count) => total + count,
      0,
    );
    if (countedTotal !== response.total_count) {
      context.addIssue({
        code: "custom",
        message: "total_count must equal the sum of every classification count",
        path: ["total_count"],
      });
    }
  }) satisfies z.ZodType<ReconciliationResponse>;

export const conflictReferenceSchema = z
  .object({ position: nonNegativeInteger, record_key: identifier })
  .strict() satisfies z.ZodType<ConflictReference>;

export const conflictDifferenceSchema = z
  .object({
    field: z.string().min(1).max(96),
    kind: z.enum(FIELD_DIFFERENCE_KIND_KEYS),
    source_text: z.string().max(512),
    target_text: z.string().max(512),
  })
  .strict() satisfies z.ZodType<ConflictDifference>;

export const conflictSchema = z
  .object({
    canonical_key: z
      .string()
      .min(1)
      .max(64)
      .regex(/^[A-Z0-9]+(?:-[A-Z0-9]+)*$/),
    classification: z.enum(
      RECONCILIATION_CLASSIFICATION_KEYS.filter((value) => value !== "match") as [
        "missing_from_target",
        "missing_from_source",
        "field_mismatch",
        "duplicate_source",
        "duplicate_target",
        "duplicate_both",
      ],
    ),
    conflict_id: identifier,
    created_at: timestamp,
    differences: z.array(conflictDifferenceSchema).max(128),
    schema_version: schemaVersion,
    source_references: z.array(conflictReferenceSchema).max(1024),
    suggested_resolution: z.enum(SUGGESTED_RESOLUTION_KEYS).nullable(),
    target_references: z.array(conflictReferenceSchema).max(1024),
  })
  .strict()
  .superRefine((conflict, context) => {
    const expectedResolution =
      conflict.classification === "missing_from_target"
        ? "create_target"
        : conflict.classification === "missing_from_source"
          ? "review_target_only"
          : conflict.classification === "field_mismatch"
            ? "update_target"
            : "review_duplicates";
    if (conflict.suggested_resolution !== expectedResolution) {
      context.addIssue({
        code: "custom",
        message: `suggested_resolution must be ${expectedResolution} for ${conflict.classification}`,
        path: ["suggested_resolution"],
      });
    }
  }) satisfies z.ZodType<ConflictResponse>;

export const conflictPageResponseSchema = z
  .object({
    items: z.array(conflictSchema).max(100),
    limit: z.number().int().min(1).max(100),
    next_cursor: z
      .string()
      .min(1)
      .max(64)
      .regex(/^[A-Z0-9]+(?:-[A-Z0-9]+)*$/)
      .nullable(),
    observed_at: timestamp,
    reconciliation_fingerprint: sha256,
    run_id: identifier,
    run_version: nonNegativeInteger,
    schema_version: schemaVersion,
    state: shortText,
  })
  .strict() satisfies z.ZodType<ConflictPageResponse>;

/**
 * `before_sha256` is serialized as the empty string exactly when the server
 * has no prior target record — the create_target sentinel. The union accepts
 * both wire shapes; the cross-field refinement below pins the sentinel to
 * create_target actions so an update_target plan can never lose its
 * expected-prior-state guard, and `kind` itself stays a display string so a
 * hostile or future kind remains visible instead of being silently dropped.
 */
export const repairActionSchema = z
  .object({
    action_id: identifier,
    applied_at: timestamp.nullable().optional(),
    before_sha256: z.union([z.literal(""), sha256]),
    canonical_key: identifier,
    failed_at: timestamp.nullable().optional(),
    kind: shortText,
    proposed_after_sha256: sha256,
    schema_version: schemaVersion,
    status: z.enum(REPAIR_ACTION_STATUS_KEYS),
    target_version: nonNegativeInteger.nullable().optional(),
  })
  .strict()
  .superRefine((action, context) => {
    if (action.kind === "create_target" && action.before_sha256 !== "") {
      context.addIssue({
        code: "custom",
        message: "create_target before_sha256 must use the empty-string sentinel",
        path: ["before_sha256"],
      });
    } else if (action.before_sha256 === "" && action.kind !== "create_target") {
      context.addIssue({
        code: "custom",
        message: 'before_sha256 sentinel "" is only valid for create_target actions',
        path: ["before_sha256"],
      });
    }
  }) satisfies z.ZodType<RepairActionResponse>;

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
    content_fingerprint: sha256,
    created_at: timestamp,
    failed_at: timestamp.nullable().optional(),
    observed_at: timestamp,
    plan_id: identifier,
    reconciliation_fingerprint: sha256,
    rejected_at: timestamp.nullable().optional(),
    run_id: identifier,
    run_version: nonNegativeInteger,
    schema_version: schemaVersion,
    // `state` is the parent run's state string and stays open (run states are
    // a separate closed set owned by the run schemas); `status` is the plan's
    // own stored lifecycle.
    state: shortText,
    status: z.enum(REPAIR_PLAN_STATUS_KEYS),
  })
  .strict()
  .superRefine((plan, context) => {
    const requiresApproval = ["approved", "applying", "applied", "failed"].includes(
      plan.status,
    );
    if (requiresApproval && plan.approval == null) {
      context.addIssue({
        code: "custom",
        message: `${plan.status} repair plans require durable approval evidence`,
        path: ["approval"],
      });
    }
    if (
      (plan.status === "proposed" || plan.status === "rejected") &&
      plan.approval != null
    ) {
      context.addIssue({
        code: "custom",
        message: `${plan.status} repair plans cannot carry approval evidence`,
        path: ["approval"],
      });
    }
  }) satisfies z.ZodType<RepairPlanResponse>;

export const repairApplyResponseSchema = z
  .object({
    content_fingerprint: sha256,
    disposition: z.enum(REPAIR_APPLY_DISPOSITION_KEYS),
    effects: z.array(z.record(z.string(), z.unknown())).max(10_000),
    observed_at: timestamp,
    plan_id: identifier,
    reconciliation_fingerprint: sha256,
    resumed: z.boolean(),
    run_id: identifier,
    run_version: nonNegativeInteger,
    schema_version: schemaVersion,
    state: shortText,
    status: z.enum(REPAIR_PLAN_STATUS_KEYS),
  })
  .strict()
  .superRefine((response, context) => {
    if (
      response.disposition === "unresolved" ||
      response.disposition === "interrupted"
    ) {
      context.addIssue({
        code: "custom",
        message: `${response.disposition} is transported as 503 Problem Details, never a successful JSON acknowledgement`,
        path: ["disposition"],
      });
    }
    if (
      (response.disposition === "completed" ||
        response.disposition === "already_applied") &&
      response.status !== "applied"
    ) {
      context.addIssue({
        code: "custom",
        message: `${response.disposition} requires applied plan status`,
        path: ["status"],
      });
    }
    if (response.disposition === "failed" && response.status !== "failed") {
      context.addIssue({
        code: "custom",
        message: "failed disposition requires failed plan status",
        path: ["status"],
      });
    }
  }) satisfies z.ZodType<RepairApplyResponse>;

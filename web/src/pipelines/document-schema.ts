/**
 * Zod wire schemas for the versioned pipeline document envelope. Kept as a
 * thin construction layer over the untrusted-input boundary; the semantic
 * model and canonical serialization live in `document.ts`.
 */
import { z } from "zod";

// ---------------------------------------------------------------------------

// Backend prefixed identifiers bound the payload to 3-64 characters.
const identifier = (prefix: string) =>
  z
    .string()
    .min(prefix.length + 1 + 3)
    .max(prefix.length + 1 + 64)
    .regex(new RegExp(`^${prefix}_[a-z0-9]+(?:-[a-z0-9]+)*$`), {
      message: `must be a canonical ${prefix}_ identifier`,
    });

const boundedText = (max: number) => z.string().min(1).max(max);

/** Port names use the backend canonical lowercase-dashed form. */
const portNameSchema = z
  .string()
  .min(1)
  .max(64)
  .regex(/^[a-z][a-z0-9]*(?:-[a-z0-9]+)*$/, {
    message: "must be a canonical port name",
  });

const scalarConfiguration = z.record(
  z.string(),
  z.union([z.string().max(1_024), z.number().int(), z.boolean()]),
);

export const PIPELINE_DOCUMENT_SCHEMA_VERSION = 1;
export const PIPELINE_CANONICAL_FORMAT_VERSION = 1;
export const MAX_PIPELINE_NODES = 256;
export const MAX_PIPELINE_EDGES = 4_096;
export const MAX_PIPELINE_LAYOUT_COORDINATE = 1_000_000;
export const MAX_PIPELINE_CONFIGURATION_VERSION = 2_147_483_647;

export const RESOURCE_POLICY_DEFAULTS = {
  max_concurrency: 4,
  max_in_flight: 16,
  memory_limit_bytes: 536_870_912,
  operation_timeout_seconds: 60,
  queue_capacity: 256,
} as const;

export interface ResourcePolicyValue {
  readonly max_concurrency: number;
  readonly max_in_flight: number;
  readonly memory_limit_bytes: number;
  readonly operation_timeout_seconds: number;
  readonly queue_capacity: number;
}

export const RESOURCE_POLICY_LIMITS = {
  max_concurrency: [1, 64],
  max_in_flight: [1, 1_024],
  memory_limit_bytes: [67_108_864, 16_777_216_000],
  operation_timeout_seconds: [1, 3_600],
  queue_capacity: [1, 65_536],
} as const;

export const resourcePolicySchema = z
  .object({
    max_concurrency: z
      .number()
      .int()
      .min(RESOURCE_POLICY_LIMITS.max_concurrency[0])
      .max(RESOURCE_POLICY_LIMITS.max_concurrency[1])
      .optional(),
    max_in_flight: z
      .number()
      .int()
      .min(RESOURCE_POLICY_LIMITS.max_in_flight[0])
      .max(RESOURCE_POLICY_LIMITS.max_in_flight[1])
      .optional(),
    memory_limit_bytes: z
      .number()
      .int()
      .min(RESOURCE_POLICY_LIMITS.memory_limit_bytes[0])
      .max(RESOURCE_POLICY_LIMITS.memory_limit_bytes[1])
      .optional(),
    operation_timeout_seconds: z
      .number()
      .int()
      .min(RESOURCE_POLICY_LIMITS.operation_timeout_seconds[0])
      .max(RESOURCE_POLICY_LIMITS.operation_timeout_seconds[1])
      .optional(),
    queue_capacity: z
      .number()
      .int()
      .min(RESOURCE_POLICY_LIMITS.queue_capacity[0])
      .max(RESOURCE_POLICY_LIMITS.queue_capacity[1])
      .optional(),
  })
  .strict();

export const pipelineNodeSchema = z
  .object({
    id: identifier("nod"),
    kind: boundedText(96).regex(/^[a-z][a-z0-9]*(?:\.[a-z][a-z0-9]*)*$/),
    configuration_version: z
      .number()
      .int()
      .min(1)
      .max(MAX_PIPELINE_CONFIGURATION_VERSION),
    configuration: scalarConfiguration,
    connector_id: z.union([identifier("con"), z.null()]),
  })
  .strict();

export const pipelineEdgeSchema = z
  .object({
    source_node_id: identifier("nod"),
    source_port: portNameSchema,
    target_node_id: identifier("nod"),
    target_port: portNameSchema,
  })
  .strict();

export const pipelineLayoutSchema = z
  .object({
    node_id: identifier("nod"),
    x: z
      .number()
      .int()
      .min(-MAX_PIPELINE_LAYOUT_COORDINATE)
      .max(MAX_PIPELINE_LAYOUT_COORDINATE),
    y: z
      .number()
      .int()
      .min(-MAX_PIPELINE_LAYOUT_COORDINATE)
      .max(MAX_PIPELINE_LAYOUT_COORDINATE),
  })
  .strict();

export const pipelineDocumentSchema = z
  .object({
    schema_version: z.literal(PIPELINE_DOCUMENT_SCHEMA_VERSION),
    canonical_format_version: z.literal(PIPELINE_CANONICAL_FORMAT_VERSION),
    nodes: z.array(pipelineNodeSchema).max(MAX_PIPELINE_NODES),
    edges: z.array(pipelineEdgeSchema).max(MAX_PIPELINE_EDGES),
    resource_policy: resourcePolicySchema,
    layout: z.array(pipelineLayoutSchema).max(MAX_PIPELINE_NODES),
  })
  .strict();

export type PipelineNodeValue = z.infer<typeof pipelineNodeSchema>;
export type PipelineEdgeValue = z.infer<typeof pipelineEdgeSchema>;
export type PipelineLayoutValue = z.infer<typeof pipelineLayoutSchema>;
export interface PipelineDocumentWire {
  readonly schema_version: 1;
  readonly canonical_format_version: 1;
  readonly nodes: readonly PipelineNodeValue[];
  readonly edges: readonly PipelineEdgeValue[];
  readonly resource_policy: ResourcePolicyValue;
  readonly layout: readonly PipelineLayoutValue[];
}

export interface PipelineDocumentValue {
  readonly schemaVersion: 1;
  readonly canonicalFormatVersion: 1;
  readonly nodes: readonly PipelineNodeValue[];
  readonly edges: readonly PipelineEdgeValue[];
  readonly resourcePolicy: ResourcePolicyValue;
  readonly layout: readonly PipelineLayoutValue[];
}

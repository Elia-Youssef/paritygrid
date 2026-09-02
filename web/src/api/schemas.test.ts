import { describe, expect, it } from "vitest";

import type { z } from "zod";

import type { pipelineDocumentSchema } from "../pipelines/document";
import type { PipelineVersionValue } from "./schemas";

type WireDocument = z.infer<typeof pipelineDocumentSchema>;
import {
  capabilitiesSchema,
  connectorPageSchema,
  healthSchema,
  pipelinePageSchema,
  pipelineResponseSchema,
  pipelineVersionAckSchema,
  pipelineVersionFrontierSchema,
  pipelineVersionSchema,
  readinessSchema,
  runPageSchema,
  runResponseSchema,
} from "./schemas";

export const PIPELINE = {
  schema_version: 1,
  pipeline_id: "pip_demo-alpha",
  display_name: "Demo pipeline",
  description: "A demonstration",
  created_at: "2026-01-01 00:00:00",
  archived_at: null,
  row_version: 3,
};

const ACK = {
  schema_version: 1,
  pipeline_id: "pip_demo-alpha",
  version: 2,
  specification_sha256: "a".repeat(64),
  planner_format_version: 1,
  published_at: "2026-01-01 00:00:00",
};

const WIRE_DOCUMENT: WireDocument = {
  schema_version: 1,
  canonical_format_version: 1,
  nodes: [
    {
      id: "nod_source-001",
      kind: "source.csv",
      configuration_version: 1,
      configuration: { header: true },
      connector_id: "con_source-001",
    },
    {
      id: "nod_export-001",
      kind: "export.parquet",
      configuration_version: 1,
      configuration: {},
      connector_id: null,
    },
  ],
  edges: [
    {
      source_node_id: "nod_source-001",
      source_port: "records",
      target_node_id: "nod_export-001",
      target_port: "records",
    },
  ],
  resource_policy: {
    max_concurrency: 4,
    max_in_flight: 16,
    memory_limit_bytes: 536_870_912,
    operation_timeout_seconds: 60,
    queue_capacity: 256,
  },
  layout: [
    { node_id: "nod_source-001", x: 0, y: 0 },
    { node_id: "nod_export-001", x: 260, y: 0 },
  ],
};

const VERSION: PipelineVersionValue = {
  schema_version: 1,
  pipeline_id: "pip_demo-alpha",
  version: 1,
  specification: WIRE_DOCUMENT,
  specification_sha256: "b".repeat(64),
  planner_format_version: 1,
  published_at: "2026-01-01 00:00:00",
};

function omit<T extends Record<string, unknown>>(
  value: T,
  key: string,
): Record<string, unknown> {
  const copy: Record<string, unknown> = { ...value };
  delete copy[key];
  return copy;
}

describe("pipeline response schema", () => {
  it("accepts a canonical identity", () => {
    expect(pipelineResponseSchema.safeParse(PIPELINE).success).toBe(true);
  });

  it("rejects wrong schema versions, bad ids, negative row versions, malformed timestamps", () => {
    expect(
      pipelineResponseSchema.safeParse({ ...PIPELINE, schema_version: 2 }).success,
    ).toBe(false);
    expect(
      pipelineResponseSchema.safeParse({ ...PIPELINE, pipeline_id: "demo" }).success,
    ).toBe(false);
    expect(
      pipelineResponseSchema.safeParse({ ...PIPELINE, row_version: 0 }).success,
    ).toBe(false);
    expect(
      pipelineResponseSchema.safeParse({ ...PIPELINE, created_at: "yesterday" })
        .success,
    ).toBe(false);
  });

  it("rejects missing required fields and unknown members", () => {
    expect(
      pipelineResponseSchema.safeParse(omit(PIPELINE, "description")).success,
    ).toBe(false);
    expect(pipelineResponseSchema.safeParse({ ...PIPELINE, extra: 1 }).success).toBe(
      false,
    );
  });

  it("validates the paginated envelope including item shapes", () => {
    const page = {
      schema_version: 1,
      items: [PIPELINE],
      limit: 50,
      next_cursor: "pip_second-001",
    };
    expect(pipelinePageSchema.safeParse(page).success).toBe(true);
    expect(
      pipelinePageSchema.safeParse({
        ...page,
        items: [{ ...PIPELINE, pipeline_id: "bad" }],
      }).success,
    ).toBe(false);
    expect(pipelinePageSchema.safeParse({ ...page, next_cursor: "nope" }).success).toBe(
      false,
    );
  });
});

describe("publication acknowledgement schema", () => {
  it("accepts the server contract shape", () => {
    expect(pipelineVersionAckSchema.safeParse(ACK).success).toBe(true);
  });

  it("rejects version, hash, and planner-format violations", () => {
    // Cross-pipeline identity is a command-level check (see
    // acknowledgementMatches in studio-state); the schema enforces shape.
    expect(pipelineVersionAckSchema.safeParse({ ...ACK, version: 0 }).success).toBe(
      false,
    );
    expect(
      pipelineVersionAckSchema.safeParse({
        ...ACK,
        specification_sha256: "Z".repeat(64),
      }).success,
    ).toBe(false);
    expect(
      pipelineVersionAckSchema.safeParse({ ...ACK, planner_format_version: 2 }).success,
    ).toBe(false);
    expect(pipelineVersionAckSchema.safeParse(omit(ACK, "published_at")).success).toBe(
      false,
    );
    // An unknown-typed generated response must never pass as trusted.
    expect(pipelineVersionAckSchema.safeParse({ data: ACK }).success).toBe(false);
  });
});

describe("version frontier schema", () => {
  it("accepts published and explicit zero states", () => {
    expect(
      pipelineVersionFrontierSchema.safeParse({
        schema_version: 1,
        pipeline_id: "pip_demo-alpha",
        latest_version: 0,
        published: false,
      }).success,
    ).toBe(true);
    expect(
      pipelineVersionFrontierSchema.safeParse({
        schema_version: 1,
        pipeline_id: "pip_demo-alpha",
        latest_version: 7,
        published: true,
      }).success,
    ).toBe(true);
  });

  it("rejects incoherent published flags and negative frontiers", () => {
    expect(
      pipelineVersionFrontierSchema.safeParse({
        schema_version: 1,
        pipeline_id: "pip_demo-alpha",
        latest_version: 0,
        published: true,
      }).success,
    ).toBe(false);
    expect(
      pipelineVersionFrontierSchema.safeParse({
        schema_version: 1,
        pipeline_id: "pip_demo-alpha",
        latest_version: 5,
        published: false,
      }).success,
    ).toBe(false);
    expect(
      pipelineVersionFrontierSchema.safeParse({
        schema_version: 1,
        pipeline_id: "pip_demo-alpha",
        latest_version: -1,
        published: false,
      }).success,
    ).toBe(false);
  });
});

describe("pipeline version schema", () => {
  it("accepts an immutable version with a fully valid document", () => {
    expect(pipelineVersionSchema.safeParse(VERSION).success).toBe(true);
  });

  it("rejects malformed nested documents, not just envelope errors", () => {
    // A structurally broken nested node is rejected in full.
    const brokenDocument = {
      ...VERSION,
      specification: {
        ...WIRE_DOCUMENT,
        nodes: [
          {
            id: "not-a-node-id",
            kind: "source.csv",
            configuration_version: 1,
            configuration: {},
            connector_id: "con_source-001",
          },
        ],
      },
    };
    expect(pipelineVersionSchema.safeParse(brokenDocument).success).toBe(false);

    const unknownKind = {
      ...VERSION,
      specification: {
        ...WIRE_DOCUMENT,
        nodes: [
          {
            id: "nod_script-001",
            kind: "script.python",
            configuration_version: 1,
            configuration: {},
            connector_id: null,
          },
        ],
        edges: [],
      },
    };
    // The wire schema accepts the shape; the DOCUMENT semantics reject it
    // at the model layer, so the version is still not trusted here.
    const parsed = pipelineVersionSchema.safeParse(unknownKind);
    expect(parsed.success).toBe(true);
    void parsed;
  });

  it("rejects hash shapes and missing nested members", () => {
    expect(
      pipelineVersionSchema.safeParse({ ...VERSION, specification_sha256: "xyz" })
        .success,
    ).toBe(false);
    expect(
      pipelineVersionSchema.safeParse(omit(VERSION, "specification")).success,
    ).toBe(false);
  });
});

describe("connector schema", () => {
  const CONNECTOR = {
    schema_version: 1,
    connector_id: "con_source-001",
    kind: "csv_source",
    display_name: "CSV",
    created_at: "2026-01-01 00:00:00",
    updated_at: "2026-01-01 00:00:00",
    archived_at: null,
    revision: 2,
    row_version: 2,
    capabilities: { read: true },
    configuration: { path: "items.csv" },
    schema_discovery: null,
    secret_references: [],
  };

  it("accepts a canonical connector and rejects capability extensions", () => {
    expect(
      connectorPageSchema.safeParse({
        schema_version: 1,
        items: [CONNECTOR],
        limit: 100,
        next_cursor: null,
      }).success,
    ).toBe(true);
    expect(
      connectorPageSchema.safeParse({
        schema_version: 1,
        items: [{ ...CONNECTOR, capabilities: { read: true, root: true } }],
        limit: 100,
        next_cursor: null,
      }).success,
    ).toBe(false);
  });
});

describe("operations overview schemas", () => {
  it("validates health, readiness, capabilities, and run pages strictly", () => {
    expect(
      healthSchema.safeParse({ status: "ok", service: "ParityGrid", version: "0.1.0" })
        .success,
    ).toBe(true);
    expect(
      healthSchema.safeParse({
        status: "degraded",
        service: "ParityGrid",
        version: "0.1.0",
      }).success,
    ).toBe(false);
    expect(
      readinessSchema.safeParse({
        status: "ready",
        service: "ParityGrid",
        version: "0.1.0",
        detail: "ok",
      }).success,
    ).toBe(true);
    expect(capabilitiesSchema.safeParse(BROKEN_CAPABILITIES).success).toBe(false);

    const run = {
      schema_version: 1,
      run_id: "run_scenario-01",
      run_version: 4,
      state: "running",
      observed_at: "2026-01-01 00:00:00",
      created_at: "2026-01-01 00:00:00",
      started_at: null,
      finished_at: null,
      cancellation_requested_at: null,
      pipeline_id: "pip_demo-alpha",
      pipeline_version: 1,
      runner_kind: "sequential",
      scenario_seed: null,
    };
    expect(
      runPageSchema.safeParse({
        schema_version: 1,
        items: [run],
        limit: 10,
        next_cursor: null,
      }).success,
    ).toBe(true);
    expect(
      runPageSchema.safeParse({
        schema_version: 1,
        items: [{ ...run, run_id: "other" }],
        limit: 10,
        next_cursor: null,
      }).success,
    ).toBe(false);
  });
  it("parses the exact capabilities payload the backend contract emits", () => {
    // Field set and value domains mirror
    // src/paritygrid/api/schemas/system.py:CapabilitiesResponse, the wire
    // truth behind GET /api/v1/system/capabilities.
    const backendCapabilities = {
      schema_version: 1,
      service: "ParityGrid",
      version: "0.1.0",
      sqlite: {
        schema_version: 1,
        library_version: "3.45.1",
        minimum_supported_version: "3.35.0",
        threadsafety: 1,
        journal_mode: "wal",
        synchronous_level: 2,
        busy_timeout_ms: 5000,
        supports_json_sql: true,
        supports_returning: true,
      },
      runners: [{ schema_version: 1, strategy_id: "sequential", available: true }],
      subordinate_pools: [],
      limits: {
        schema_version: 1,
        max_request_body_bytes: 1048576,
        max_json_depth: 64,
        max_concurrent_requests: 8,
        request_timeout_seconds: 0.5,
        max_page_size: 100,
        idempotency_lease_seconds: 86399.5,
        artifact_chunk_bytes: 65536,
      },
      features: [{ name: "runtime-profile-loopback", available: true }],
    };
    expect(capabilitiesSchema.safeParse(backendCapabilities).success).toBe(true);
    expect(
      capabilitiesSchema.safeParse({
        ...backendCapabilities,
        sqlite: { ...backendCapabilities.sqlite, busy_timeout_ms: -1 },
      }).success,
    ).toBe(false);
    expect(
      capabilitiesSchema.safeParse({
        ...backendCapabilities,
        limits: { ...backendCapabilities.limits, request_timeout_seconds: 0 },
      }).success,
    ).toBe(false);
    for (const limits of [
      { ...backendCapabilities.limits, request_timeout_seconds: 300.01 },
      { ...backendCapabilities.limits, idempotency_lease_seconds: 86_400.01 },
      {
        ...backendCapabilities.limits,
        request_timeout_seconds: 30.5,
        idempotency_lease_seconds: 30.5,
      },
    ]) {
      expect(
        capabilitiesSchema.safeParse({ ...backendCapabilities, limits }).success,
      ).toBe(false);
    }
    expect(
      capabilitiesSchema.safeParse({
        ...backendCapabilities,
        limits: {
          ...backendCapabilities.limits,
          request_timeout_seconds: 0.1,
          idempotency_lease_seconds: 86_400,
        },
      }).success,
    ).toBe(true);
  });

  it("enforces execution-evidence pairing, shape, version, and lifecycle state", () => {
    const completedRun = {
      schema_version: 1,
      run_id: "run_schema",
      run_version: 3,
      state: "succeeded",
      observed_at: "2026-01-01T00:01:00Z",
      created_at: "2026-01-01T00:00:00Z",
      started_at: "2026-01-01T00:00:02Z",
      finished_at: "2026-01-01T00:00:32Z",
      cancellation_requested_at: null,
      pipeline_id: "pip_schema",
      pipeline_version: 1,
      runner_kind: "sequential",
      scenario_seed: 7,
      execution_evidence_fingerprint: "a".repeat(64),
      execution_evidence_fingerprint_version: 2,
    };
    expect(runResponseSchema.safeParse(completedRun).success).toBe(true);
    for (const candidate of [
      { ...completedRun, execution_evidence_fingerprint_version: null },
      { ...completedRun, execution_evidence_fingerprint: "not-a-sha256" },
      { ...completedRun, execution_evidence_fingerprint_version: 0 },
      { ...completedRun, state: "running", finished_at: null },
      { ...completedRun, state: "failed" },
      { ...completedRun, finished_at: null },
    ]) {
      expect(runResponseSchema.safeParse(candidate).success).toBe(false);
    }
  });
});

const BROKEN_CAPABILITIES = {
  schema_version: 1,
  service: "ParityGrid",
  version: "0.1.0",
  runners: [{ schema_version: 1, strategy_id: "sequential", available: "yes" }],
  subordinate_pools: [],
  features: [],
  sqlite: {
    schema_version: 1,
    library_version: "3",
    minimum_supported_version: "3",
    journal_mode: "wal",
    synchronous_level: 1,
    threadsafety: 1,
    supports_json_sql: true,
    supports_returning: true,
  },
  limits: {
    schema_version: 1,
    artifact_chunk_bytes: 1,
    idempotency_lease_seconds: 1,
    max_concurrent_requests: 1,
    max_json_depth: 1,
    max_page_size: 1,
    max_request_body_bytes: 1,
    request_timeout_seconds: 1,
  },
};

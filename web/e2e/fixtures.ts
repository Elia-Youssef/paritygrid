/**
 * Deterministic API fixtures for the Phase 16 browser workflows. Every body
 * satisfies the same strict runtime schemas the production client enforces;
 * requests flow through the real query, decoding, mutation, routing, and
 * editor layers in the built SPA.
 */
import type { Page, WebSocketRoute } from "@playwright/test";

export const PIPELINE_ID = "pip_e2e-001";

export const HEALTH_OK = JSON.stringify({
  status: "ok",
  service: "ParityGrid",
  version: "0.1.0",
});

export const PIPELINE_BODY = JSON.stringify({
  schema_version: 1,
  pipeline_id: PIPELINE_ID,
  display_name: 'E2E <script>alert("x")</script> pipeline',
  description: null,
  created_at: "2026-01-01 00:00:00",
  archived_at: null,
  row_version: 1,
});

export const FRONTIER_UNPUBLISHED = JSON.stringify({
  schema_version: 1,
  pipeline_id: PIPELINE_ID,
  latest_version: 0,
  published: false,
});

export const CONNECTORS_PAGE = JSON.stringify({
  schema_version: 1,
  items: [
    {
      schema_version: 1,
      connector_id: "con_source-001",
      kind: "csv_source",
      display_name: "CSV source",
      created_at: "2026-01-01 00:00:00",
      updated_at: "2026-01-01 00:00:00",
      archived_at: null,
      revision: 1,
      row_version: 1,
      capabilities: { read: true },
      configuration: { path: "items.csv" },
      schema_discovery: null,
      secret_references: [],
    },
  ],
  limit: 100,
  next_cursor: null,
});

export interface PublishBehavior {
  status: number;
  body: string;
}

export type PublishHandler = (
  attempt: number,
  body: string | null,
  key: string | null,
) => PublishBehavior | Promise<PublishBehavior>;

export interface StudioRoutesOptions {
  frontier?: string;
  pipelineBody?: string;
  publish?: PublishHandler;
  runsBody?: string;
  runsStatus?: number;
  readinessStatus?: number;
}

export interface CapturedPublish {
  body: string | null;
  idempotencyKey: string | null;
}

export interface StudioRoutes {
  publishes: CapturedPublish[];
  setFrontier: (next: string) => void;
}

/** Install route interceptors for the studio and its dependencies. */
export async function installStudioRoutes(
  page: Page,
  options: StudioRoutesOptions = {},
): Promise<StudioRoutes> {
  const publishes: CapturedPublish[] = [];
  let attempt = 0;
  let frontier = options.frontier ?? FRONTIER_UNPUBLISHED;

  await page.route("**/healthz", (route) =>
    route.fulfill({ status: 200, contentType: "application/json", body: HEALTH_OK }),
  );
  await page.route("**/api/v1/pipelines*", (route) => {
    if (route.request().method() === "POST") {
      return route.fulfill({
        status: 201,
        contentType: "application/json",
        body: JSON.stringify({
          schema_version: 1,
          pipeline_id: "pip_created-001",
          display_name: "Created",
          description: null,
          created_at: "2026-01-03 00:00:00",
          archived_at: null,
          row_version: 1,
        }),
      });
    }
    return route.fulfill({
      status: 200,
      contentType: "application/json",
      body: JSON.stringify({
        schema_version: 1,
        items: [
          {
            schema_version: 1,
            pipeline_id: PIPELINE_ID,
            display_name: "E2E <b>bold</b> pipeline",
            description: "Authoring demo",
            created_at: "2026-01-01 00:00:00",
            archived_at: null,
            row_version: 1,
          },
          {
            schema_version: 1,
            pipeline_id: "pip_second-001",
            display_name: "Second pipeline",
            description: "Another",
            created_at: "2026-01-02 00:00:00",
            archived_at: null,
            row_version: 2,
          },
        ],
        limit: 50,
        next_cursor: null,
      }),
    });
  });
  await page.route(`**/api/v1/pipelines/${PIPELINE_ID}`, (route) =>
    route.fulfill({
      status: 200,
      contentType: "application/json",
      body: options.pipelineBody ?? PIPELINE_BODY,
    }),
  );
  await page.route("**/version-frontier", (route) =>
    route.fulfill({ status: 200, contentType: "application/json", body: frontier }),
  );
  await page.route("**/api/v1/connectors**", (route) =>
    route.fulfill({
      status: 200,
      contentType: "application/json",
      body: CONNECTORS_PAGE,
    }),
  );
  await page.route("**/api/v1/runs*", (route) =>
    route.fulfill({
      status: options.runsStatus ?? 200,
      contentType: "application/json",
      body:
        options.runsBody ??
        JSON.stringify({
          schema_version: 1,
          items: [
            {
              schema_version: 1,
              run_id: "run_scenario-01",
              run_version: 1,
              state: "succeeded",
              observed_at: "2026-01-01 00:00:00",
              created_at: "2026-01-01 00:00:00",
              started_at: null,
              finished_at: "2026-01-01 00:01:00",
              cancellation_requested_at: null,
              pipeline_id: PIPELINE_ID,
              pipeline_version: 1,
              runner_kind: "sequential",
              scenario_seed: null,
            },
          ],
          limit: 10,
          next_cursor: null,
        }),
    }),
  );
  await page.route("**/readyz", (route) =>
    route.fulfill({
      status: options.readinessStatus ?? 200,
      contentType: "application/json",
      body: JSON.stringify({
        status: "ready",
        service: "ParityGrid",
        version: "0.1.0",
        detail: "migrations applied",
      }),
    }),
  );
  await page.route("**/api/v1/system/capabilities", (route) =>
    route.fulfill({
      status: 200,
      contentType: "application/json",
      body: JSON.stringify({
        schema_version: 1,
        service: "ParityGrid",
        version: "0.1.0",
        runners: [{ schema_version: 1, strategy_id: "sequential", available: true }],
        subordinate_pools: [],
        features: [],
        sqlite: {
          schema_version: 1,
          library_version: "3.45",
          minimum_supported_version: "3.35",
          journal_mode: "wal",
          synchronous_level: 1,
          busy_timeout_ms: 5000,
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
      }),
    }),
  );
  await page.route("**/api/v1/pipelines/*/versions", async (route) => {
    attempt += 1;
    const request = route.request();
    publishes.push({
      body: request.postData() ?? null,
      idempotencyKey: request.headers()["idempotency-key"] ?? null,
    });
    const fallback = {
      status: 201,
      body: JSON.stringify({
        schema_version: 1,
        pipeline_id: PIPELINE_ID,
        version: 1,
        specification_sha256: "a".repeat(64),
        planner_format_version: 1,
        published_at: "2026-01-01 00:00:05",
      }),
    };
    const behavior =
      (await options.publish?.(
        attempt,
        request.postData(),
        request.headers()["idempotency-key"] ?? null,
      )) ?? fallback;
    if (behavior.status >= 200 && behavior.status < 300) {
      const acknowledgement = JSON.parse(behavior.body) as {
        pipeline_id?: unknown;
        version?: unknown;
      };
      if (
        acknowledgement.pipeline_id === PIPELINE_ID &&
        typeof acknowledgement.version === "number"
      ) {
        frontier = JSON.stringify({
          schema_version: 1,
          pipeline_id: PIPELINE_ID,
          latest_version: acknowledgement.version,
          published: true,
        });
      }
    }
    await route.fulfill({
      status: behavior.status,
      contentType: "application/json",
      body: behavior.body,
    });
  });

  return {
    publishes,
    setFrontier(next: string): void {
      frontier = next;
    },
  };
}

// ---------------------------------------------------------------------------
// Phase 17 run fixtures: durable REST pages, the durable SSE stream, and the
// advisory telemetry WebSocket are all deterministic and injected below.
// ---------------------------------------------------------------------------

export const RUN_ID_E2E = "run_e2e-live-001";

export function runE2E(
  overrides: Record<string, unknown> = {},
): Record<string, unknown> {
  const state = typeof overrides.state === "string" ? overrides.state : "running";
  const terminal = ["succeeded", "partially_succeeded", "failed", "cancelled"].includes(
    state,
  );
  return {
    schema_version: 1,
    run_id: RUN_ID_E2E,
    run_version: 2,
    state,
    observed_at: "2026-01-01T00:00:02Z",
    created_at: "2026-01-01T00:00:00Z",
    started_at: "2026-01-01T00:00:01Z",
    finished_at: terminal ? "2026-01-01T00:00:31Z" : null,
    cancellation_requested_at: null,
    pipeline_id: "pip_e2e-001",
    pipeline_version: 1,
    runner_kind: "sequential",
    scenario_seed: 7,
    execution_evidence_fingerprint: null,
    execution_evidence_fingerprint_version: null,
    ...overrides,
  };
}

export function pipelineVersionE2E(): Record<string, unknown> {
  return {
    schema_version: 1,
    pipeline_id: "pip_e2e-001",
    version: 1,
    specification: {
      published_specification_version: 1,
      connector_bindings: [],
      pipeline: {
        schema_version: 1,
        canonical_format_version: 1,
        nodes: [
          {
            id: "nod_e2e-source-001",
            kind: "source.csv",
            configuration_version: 1,
            configuration: {},
            connector_id: null,
          },
          {
            id: "nod_e2e-export-001",
            kind: "export.parquet",
            configuration_version: 1,
            configuration: {},
            connector_id: null,
          },
        ],
        edges: [
          {
            source_node_id: "nod_e2e-source-001",
            source_port: "records",
            target_node_id: "nod_e2e-export-001",
            target_port: "records",
          },
        ],
        resource_policy: {},
        layout: [
          { node_id: "nod_e2e-source-001", x: 0, y: 0 },
          { node_id: "nod_e2e-export-001", x: 300, y: 0 },
        ],
      },
    },
    specification_sha256: "a".repeat(64),
    planner_format_version: 1,
    published_at: "2026-01-01T00:00:00Z",
  };
}

export function capabilitiesE2E(): Record<string, unknown> {
  return {
    schema_version: 1,
    service: "ParityGrid",
    version: "0.1.0",
    runners: [
      { schema_version: 1, strategy_id: "sequential", available: true },
      {
        schema_version: 1,
        strategy_id: "asyncio",
        available: false,
        unavailability_reason: "no running event loop",
      },
    ],
    subordinate_pools: [],
    features: [],
    sqlite: {
      schema_version: 1,
      library_version: "3.45",
      minimum_supported_version: "3.35",
      journal_mode: "wal",
      synchronous_level: 1,
      busy_timeout_ms: 5000,
      threadsafety: 1,
      supports_json_sql: true,
      supports_returning: true,
    },
    limits: {
      schema_version: 1,
      artifact_chunk_bytes: 65536,
      idempotency_lease_seconds: 30,
      max_concurrent_requests: 8,
      max_json_depth: 64,
      max_page_size: 100,
      max_request_body_bytes: 1048576,
      request_timeout_seconds: 15,
    },
  };
}

/** One durable wire frame as the SSE transport delivers it. */
export function durableFrameE2E(
  sequence: number,
  overrides: Record<string, unknown> = {},
): Record<string, unknown> {
  return {
    schema_version: 1,
    channel: "durable-events",
    sequence,
    run_id: RUN_ID_E2E,
    event_kind: "run.progressed",
    subject_kind: "run",
    subject_id: RUN_ID_E2E,
    occurred_at: "2026-01-01T00:00:02Z",
    payload_schema_version: 1,
    payload: {},
    ...overrides,
  };
}

/** Render frames as an SSE body with id/event/data lines. */
export function sseBody(frames: Record<string, unknown>[]): string {
  return frames
    .map(
      (frame) =>
        `id: ${String(frame.sequence)}\nevent: ${String(frame.event_kind)}\ndata: ${JSON.stringify(frame)}\n\n`,
    )
    .join("");
}

export interface RunRouteOptions {
  /** One SSE body per connection; the last one repeats for later attempts. */
  readonly sseBodies?: string[];
  readonly runsPageBody?: string;
  readonly runsPageStatus?: number;
}

export interface RunRoutes {
  /** Every SSE connection URL, in connection order (resume positions). */
  readonly sseConnections: () => string[];
  readonly restRunFetches: () => number;
}

/** Install route interceptors for the run list, run detail, and the stream. */
export async function installRunRoutes(
  page: Page,
  options: RunRouteOptions = {},
): Promise<RunRoutes> {
  const sseUrls: string[] = [];
  let restRunFetches = 0;
  const bodies = options.sseBodies ?? [sseBody([durableFrameE2E(1)])];

  await page.route("**/healthz", (route) =>
    route.fulfill({ status: 200, contentType: "application/json", body: HEALTH_OK }),
  );
  await page.route("**/readyz", (route) =>
    route.fulfill({
      status: 200,
      contentType: "application/json",
      body: JSON.stringify({
        status: "ready",
        service: "ParityGrid",
        version: "0.1.0",
        detail: "migrations applied",
      }),
    }),
  );
  await page.route("**/api/v1/system/capabilities", (route) =>
    route.fulfill({
      status: 200,
      contentType: "application/json",
      body: JSON.stringify(capabilitiesE2E()),
    }),
  );
  await page.route(/\/api\/v1\/runs\?/, (route) => {
    const url = route.request().url();
    if (url.includes("state=") || url.includes("limit=")) {
      return route.fulfill({
        status: options.runsPageStatus ?? 200,
        contentType: "application/json",
        body:
          options.runsPageBody ??
          JSON.stringify({
            schema_version: 1,
            items: [{ ...runE2E(), execution_evidence_fingerprint: null }],
            limit: 10,
            next_cursor: null,
          }),
      });
    }
    return route.continue();
  });
  await page.route("**/api/v1/runs/*", (route) => {
    const url = route.request().url();
    if (url.includes("/stream/") || url.includes("/live/")) {
      return route.continue();
    }
    if (url.endsWith(`/${RUN_ID_E2E}`)) {
      restRunFetches += 1;
      return route.fulfill({
        status: 200,
        contentType: "application/json",
        body: JSON.stringify(runE2E()),
      });
    }
    return route.continue();
  });
  await page.route("**/api/v1/pipelines/pip_e2e-001/versions/1", (route) =>
    route.fulfill({
      status: 200,
      contentType: "application/json",
      body: JSON.stringify(pipelineVersionE2E()),
    }),
  );
  await page.route("**/api/v1/stream/runs/**", (route) => {
    const url = route.request().url();
    sseUrls.push(url);
    const body = bodies[Math.min(sseUrls.length - 1, bodies.length - 1)] ?? "";
    return route.fulfill({
      status: 200,
      contentType: "text/event-stream",
      body,
    });
  });
  return {
    sseConnections: () => [...sseUrls],
    restRunFetches: () => restRunFetches,
  };
}

/** Route the advisory telemetry socket; returns every live connection. */
export async function installTelemetrySocket(
  page: Page,
): Promise<{ connections: WebSocketRoute[] }> {
  const connections: WebSocketRoute[] = [];
  await page.routeWebSocket(/\/api\/v1\/live\/runs\/.*/, (ws) => {
    connections.push(ws);
    ws.onMessage(() => {
      // Pings from the client need no reply for the staleness contract.
    });
  });
  return { connections };
}

export function telemetrySnapshotE2E(
  depthValue: number,
  observedMicros = 1_000,
): string {
  return JSON.stringify({
    schema_version: 1,
    channel: "telemetry",
    advisory: true,
    records: [
      {
        schema_version: 1,
        observed_at_micros: observedMicros,
        run_id: RUN_ID_E2E,
        metrics: [
          { name: "writer_queue_depth", kind: "gauge", value: depthValue, labels: {} },
        ],
      },
    ],
  });
}

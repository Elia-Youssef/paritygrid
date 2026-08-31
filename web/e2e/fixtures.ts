/**
 * Deterministic API fixtures for the Phase 16 browser workflows. Every body
 * satisfies the same strict runtime schemas the production client enforces;
 * requests flow through the real query, decoding, mutation, routing, and
 * editor layers in the built SPA.
 */
import type { Page } from "@playwright/test";

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
              state: "completed",
              observed_at: "2026-01-01 00:00:00",
              created_at: "2026-01-01 00:00:00",
              started_at: null,
              finished_at: null,
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

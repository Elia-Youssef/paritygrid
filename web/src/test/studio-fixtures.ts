/**
 * URL-routed fetch fixtures for studio component tests. Every fixture
 * satisfies the same strict runtime schemas the production client enforces,
 * and requests flow through the real query/mutation/editor layers.
 */
import { vi } from "vitest";

export const PIPELINE_ID = "pip_demo-alpha";

export const PIPELINE_BODY = {
  schema_version: 1,
  pipeline_id: PIPELINE_ID,
  display_name: "Demo pipeline",
  description: "Demonstration pipeline",
  created_at: "2026-01-01 00:00:00",
  archived_at: null,
  row_version: 1,
};

export const FRONTIER_UNPUBLISHED = {
  schema_version: 1,
  pipeline_id: PIPELINE_ID,
  latest_version: 0,
  published: false,
};

export const CONNECTOR = {
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
};

export const ACK = {
  schema_version: 1,
  pipeline_id: PIPELINE_ID,
  version: 1,
  specification_sha256: "a".repeat(64),
  planner_format_version: 1,
  published_at: "2026-01-01 00:00:05",
};

function json(body: unknown, status = 200): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { "Content-Type": "application/json" },
  });
}

export interface CapturedRequest {
  url: string;
  method: string;
  body: string | null;
  idempotencyKey: string | null;
}

export interface StudioApiOptions {
  frontier?: typeof FRONTIER_UNPUBLISHED;
  onPublish?: (request: CapturedRequest) => Response | Promise<Response>;
  onFetchVersion?: (request: CapturedRequest) => Response | Promise<Response>;
  onFetchConnectors?: (request: CapturedRequest) => Response | Promise<Response>;
}

export interface StudioApi {
  requests: CapturedRequest[];
  publishRequests: CapturedRequest[];
  setFrontier: (frontier: typeof FRONTIER_UNPUBLISHED) => void;
}

/** Install the studio fixture set as the global fetch. */
export function stubStudioApi(options: StudioApiOptions = {}): StudioApi {
  const state: StudioApi = {
    requests: [],
    publishRequests: [],
    setFrontier: (frontier) => {
      currentFrontier = frontier;
    },
  };
  let currentFrontier = options.frontier ?? FRONTIER_UNPUBLISHED;
  const fetchMock = vi.fn<typeof fetch>(
    (input: RequestInfo | URL, init?: RequestInit) => {
      const url =
        typeof input === "string"
          ? input
          : input instanceof URL
            ? input.href
            : input.url;
      const method = init?.method ?? "GET";
      const request: CapturedRequest = {
        url,
        method,
        body: typeof init?.body === "string" ? init.body : null,
        idempotencyKey: headerValue(init?.headers, "Idempotency-Key"),
      };
      state.requests.push(request);

      if (url.endsWith(`/api/v1/pipelines/${PIPELINE_ID}`)) {
        return Promise.resolve(json(PIPELINE_BODY));
      }
      if (url.endsWith("/version-frontier")) {
        return Promise.resolve(json(currentFrontier));
      }
      if (url.includes("/versions/")) {
        const handler =
          options.onFetchVersion ?? (() => new Response("missing", { status: 404 }));
        return Promise.resolve(handler(request));
      }
      if (url.startsWith("/api/v1/connectors")) {
        if (options.onFetchConnectors !== undefined) {
          return Promise.resolve(options.onFetchConnectors(request));
        }
        return Promise.resolve(
          json({
            schema_version: 1,
            items: [CONNECTOR],
            limit: 100,
            next_cursor: null,
          }),
        );
      }
      if (url.endsWith("/versions") && method === "POST") {
        state.publishRequests.push(request);
        const handler = options.onPublish ?? (() => json(ACK, 201));
        return Promise.resolve(handler(request)).then(async (response) => {
          if (response.ok) {
            const acknowledgement = (await response.clone().json()) as {
              pipeline_id?: unknown;
              version?: unknown;
            };
            if (
              acknowledgement.pipeline_id === PIPELINE_ID &&
              typeof acknowledgement.version === "number"
            ) {
              currentFrontier = {
                schema_version: 1,
                pipeline_id: PIPELINE_ID,
                latest_version: acknowledgement.version,
                published: true,
              };
            }
          }
          return response;
        });
      }
      return Promise.resolve(new Response("not found", { status: 404 }));
    },
  );
  vi.stubGlobal("fetch", fetchMock);
  return state;
}

function headerValue(headers: HeadersInit | undefined, name: string): string | null {
  if (headers === undefined) {
    return null;
  }
  if (headers instanceof Headers) {
    return headers.get(name);
  }
  if (Array.isArray(headers)) {
    return (
      headers.find(([key]) => key.toLowerCase() === name.toLowerCase())?.[1] ?? null
    );
  }
  const record = headers;
  return record[name] ?? null;
}

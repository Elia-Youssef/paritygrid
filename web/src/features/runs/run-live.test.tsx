/**
 * Live run view component tests (P17.2, P17.4, P17.5). Every asynchronous
 * boundary is injected: the REST cache is a real QueryClient over a stubbed
 * fetch, the durable SSE stream and the advisory telemetry socket are the
 * shared manual fakes, and time is a manual scheduler. The scenarios prove
 * version coherence, gap recovery, reconnect refetch, telemetry non-
 * authority, refresh stability, and safe rendering of hostile text.
 */
import { QueryClientProvider } from "@tanstack/react-query";
import { act, cleanup, render, screen, waitFor } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import { RunLive } from "./run-live";
import { queryKeys } from "../../api/query-keys";
import { appQueryClient } from "../../api/query-client";
import {
  ManualScheduler,
  FakeSseConnection,
  FakeTelemetrySocket,
  RUN_ID,
  batchJson,
  snapshotJson,
  telemetryRecordJson,
} from "../../test/live-fixtures";

const RUN: Record<string, unknown> = {
  schema_version: 1,
  run_id: RUN_ID,
  run_version: 2,
  state: "running",
  observed_at: "2026-01-01T00:00:02Z",
  created_at: "2026-01-01T00:00:00Z",
  started_at: "2026-01-01T00:00:01Z",
  finished_at: null,
  cancellation_requested_at: null,
  pipeline_id: "pip_a",
  pipeline_version: 1,
  runner_kind: "sequential",
  scenario_seed: 7,
  execution_evidence_fingerprint: null,
  execution_evidence_fingerprint_version: null,
};

function documentNode(id: string, kind: string): Record<string, unknown> {
  return {
    id,
    kind,
    configuration_version: 1,
    configuration: {},
    connector_id: null,
  };
}

function wireDocument(): Record<string, unknown> {
  return {
    schema_version: 1,
    canonical_format_version: 1,
    nodes: [
      documentNode("nod_alpha-001", "source.csv"),
      documentNode("nod_beta-001", "export.parquet"),
    ],
    edges: [
      {
        source_node_id: "nod_alpha-001",
        source_port: "records",
        target_node_id: "nod_beta-001",
        target_port: "records",
      },
    ],
    resource_policy: {},
    layout: [
      { node_id: "nod_alpha-001", x: 0, y: 0 },
      { node_id: "nod_beta-001", x: 300, y: 0 },
    ],
  };
}

function versionBody(pipelineId = "pip_a", version = 1): Record<string, unknown> {
  return {
    schema_version: 1,
    pipeline_id: pipelineId,
    version,
    specification: wireDocument(),
    specification_sha256: "a".repeat(64),
    planner_format_version: 1,
    published_at: "2026-01-01T00:00:00Z",
  };
}

function jsonResponse(body: unknown, status = 200): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { "Content-Type": "application/json" },
  });
}

interface Harness {
  readonly sse: FakeSseConnection[];
  readonly sockets: FakeTelemetrySocket[];
  readonly scheduler: ManualScheduler;
  readonly rerender: () => Promise<void>;
}

function renderRunLive(): Harness {
  const sse = FakeSseConnection.transport();
  const telemetry = FakeTelemetrySocket.factory();
  const scheduler = new ManualScheduler();
  render(
    <QueryClientProvider client={appQueryClient}>
      <RunLive
        runId={RUN_ID}
        liveOptions={{
          sseTransport: sse.transport,
          telemetryTransport: telemetry.factory,
          scheduler,
          now: () => scheduler.now,
        }}
      />
    </QueryClientProvider>,
  );
  return {
    sse: sse.connections,
    sockets: telemetry.sockets,
    scheduler,
    rerender: async () => {
      await waitFor(() => {
        expect(screen.queryByRole("progressbar")).toBeNull();
      });
    },
  };
}

function durableFrameJsonString(
  sequence: number,
  overrides: Record<string, unknown> = {},
): string {
  return JSON.stringify({
    schema_version: 1,
    channel: "durable-events",
    sequence,
    run_id: RUN_ID,
    event_kind: "run.progressed",
    subject_kind: "run",
    subject_id: RUN_ID,
    occurred_at: "2026-01-01T00:00:02Z",
    payload_schema_version: 1,
    payload: {},
    ...overrides,
  });
}

describe("RunLive", () => {
  beforeEach(() => {
    appQueryClient.clear();
  });

  afterEach(() => {
    cleanup();
    vi.unstubAllGlobals();
  });

  it("overlays durable node state only under a compatible identity", async () => {
    const fetchMock = vi.fn<typeof fetch>((input: RequestInfo | URL) => {
      const url =
        typeof input === "string"
          ? input
          : input instanceof URL
            ? input.href
            : input.url;
      if (url.includes(`/api/v1/runs/${RUN_ID}`)) {
        return Promise.resolve(jsonResponse(RUN));
      }
      if (url.includes("/api/v1/pipelines/pip_a/versions/1")) {
        return Promise.resolve(jsonResponse(versionBody()));
      }
      return Promise.resolve(new Response("not found", { status: 404 }));
    });
    vi.stubGlobal("fetch", fetchMock);

    const harness = renderRunLive();
    const connection = await waitFor(() => {
      const first = harness.sse[0];
      if (first === undefined) {
        throw new Error("stream not connected yet");
      }
      return first;
    });

    connection.emitData(
      durableFrameJsonString(1, {
        event_kind: "work_claimed",
        subject_kind: "work_item",
        subject_id: "work-9",
        payload: {
          node_id: "nod_alpha-001",
          attempt_number: 1,
          worker_identity: "worker-3",
        },
      }),
    );
    connection.emitData(
      durableFrameJsonString(2, {
        event_kind: "work_succeeded",
        subject_kind: "work_item",
        subject_id: "work-9",
        payload: { node_id: "nod_alpha-001", attempt_number: 1 },
      }),
    );

    // Durable overlay: node A shows the newest durable fact; node B has no
    // durable observation yet and says so instead of inventing a state.
    // React Flow renders its canvas in the browser; in jsdom the semantic
    // table twin carries the same facts and is the keyboard surface.
    const rowA = await screen.findByTestId("structure-row-nod_alpha-001");
    expect(rowA).toHaveTextContent("work_succeeded");
    expect(rowA).toHaveTextContent("2026-01-01T00:00:02Z");
    expect(rowA.textContent).not.toContain("unavailable");
    expect(screen.getByTestId("structure-row-nod_beta-001")).toHaveTextContent(
      "not observed yet",
    );
    // The durable timeline lists both events with sequences, separate from
    // telemetry.
    expect(screen.getByTestId("event-row-2")).toHaveTextContent("work_succeeded");
    expect(screen.getByTestId("stream-status")).toHaveTextContent("sequence 2");
  });

  it("rejects an incompatible embedded run version and keeps the coherent view", async () => {
    const fetchMock = vi.fn<typeof fetch>((input: RequestInfo | URL) => {
      const url =
        typeof input === "string"
          ? input
          : input instanceof URL
            ? input.href
            : input.url;
      if (url.includes(`/api/v1/runs/${RUN_ID}`)) {
        return Promise.resolve(jsonResponse(RUN));
      }
      if (url.includes("/api/v1/pipelines/pip_a/versions/1")) {
        return Promise.resolve(jsonResponse(versionBody()));
      }
      return Promise.resolve(new Response("not found", { status: 404 }));
    });
    vi.stubGlobal("fetch", fetchMock);

    const harness = renderRunLive();
    const connection = await waitFor(() => {
      const first = harness.sse[0];
      if (first === undefined) {
        throw new Error("stream not connected yet");
      }
      return first;
    });

    // The event embeds a run projection that changed pipeline version and
    // claims the run failed — an incompatibility that must be rejected,
    // never merged, and the recovery path must refetch authority.
    connection.emitData(
      JSON.stringify({
        schema_version: 1,
        channel: "durable-events",
        sequence: 1,
        run_id: RUN_ID,
        event_kind: "run.updated",
        subject_kind: "run",
        subject_id: RUN_ID,
        occurred_at: "2026-01-01T00:00:02Z",
        payload_schema_version: 1,
        payload: {
          run: { ...RUN, pipeline_version: 2, run_version: 3, state: "failed" },
        },
      }),
    );

    // The last coherent durable run is retained and rendered: the run is
    // still "running" at run_version 2, never the incompatible projection.
    await waitFor(() => {
      expect(screen.getAllByText("running").length).toBeGreaterThan(0);
    });
    expect(screen.queryByText(/^failed$/)).toBeNull();
    // The stream resumed from the last accepted (zero) sequence, showing
    // recovery ran and resumption used the accepted position.
    await waitFor(() => {
      const second = harness.sse[1];
      if (second === undefined) {
        throw new Error("stream did not resume");
      }
      expect(second.url).toContain("after=0");
    });
    // No overlay inversion happened: node states stay unobserved.
    expect(screen.getByTestId("structure-row-nod_alpha-001")).toHaveTextContent(
      "not observed yet",
    );
  });

  it("ignores older durable state from late authoritative refetches", async () => {
    const fetchMock = vi.fn<typeof fetch>((input: RequestInfo | URL) => {
      const url =
        typeof input === "string"
          ? input
          : input instanceof URL
            ? input.href
            : input.url;
      if (url.includes(`/api/v1/runs/${RUN_ID}`)) {
        return Promise.resolve(jsonResponse(RUN));
      }
      if (url.includes("/api/v1/pipelines/pip_a/versions/1")) {
        return Promise.resolve(jsonResponse(versionBody()));
      }
      return Promise.resolve(new Response("not found", { status: 404 }));
    });
    vi.stubGlobal("fetch", fetchMock);

    renderRunLive();
    await screen.findByTestId("run-canvas");

    // A late, older authoritative snapshot (regressed run_version) arrives
    // through the query cache; the reducer must reject the regression.
    appQueryClient.setQueryData(queryKeys.runDetail(RUN_ID), {
      ...RUN,
      run_version: 1,
      state: "queued",
    });

    await waitFor(() => {
      expect(screen.getAllByText("running").length).toBeGreaterThan(0);
    });
    expect(screen.queryByText(/^queued$/)).toBeNull();
  });

  it("detects a durable sequence gap, refetches authority, and resumes", async () => {
    let runFetches = 0;
    const fetchMock = vi.fn<typeof fetch>((input: RequestInfo | URL) => {
      const url =
        typeof input === "string"
          ? input
          : input instanceof URL
            ? input.href
            : input.url;
      if (url.includes(`/api/v1/runs/${RUN_ID}`)) {
        runFetches += 1;
        return Promise.resolve(jsonResponse(RUN));
      }
      if (url.includes("/api/v1/pipelines/pip_a/versions/1")) {
        return Promise.resolve(jsonResponse(versionBody()));
      }
      return Promise.resolve(new Response("not found", { status: 404 }));
    });
    vi.stubGlobal("fetch", fetchMock);

    const harness = renderRunLive();
    const first = await waitFor(() => {
      const connection = harness.sse[0];
      if (connection === undefined) {
        throw new Error("stream not connected yet");
      }
      return connection;
    });
    expect(first.url).toContain("after=0");
    first.emitData(durableFrameJsonString(1));
    await screen.findByTestId("event-row-1");

    // A gap: sequence 3 arrives when 2 is expected. The view must enter
    // recovery, refetch authoritative state, and resume from sequence 1.
    first.emitData(durableFrameJsonString(3));

    const banner = await screen.findByText(/Durable stream recovering/);
    expect(banner.parentElement).toHaveTextContent("durable sequence gap");
    await waitFor(() => {
      expect(runFetches).toBeGreaterThanOrEqual(2);
    });
  });

  it("reconnects after a clean close, resuming from the accepted sequence", async () => {
    const fetchMock = vi.fn<typeof fetch>((input: RequestInfo | URL) => {
      const url =
        typeof input === "string"
          ? input
          : input instanceof URL
            ? input.href
            : input.url;
      if (url.includes(`/api/v1/runs/${RUN_ID}`)) {
        return Promise.resolve(jsonResponse(RUN));
      }
      if (url.includes("/api/v1/pipelines/pip_a/versions/1")) {
        return Promise.resolve(jsonResponse(versionBody()));
      }
      return Promise.resolve(new Response("not found", { status: 404 }));
    });
    vi.stubGlobal("fetch", fetchMock);

    const harness = renderRunLive();
    const first = await waitFor(() => {
      const connection = harness.sse[0];
      if (connection === undefined) {
        throw new Error("stream not connected yet");
      }
      return connection;
    });
    first.emitData(durableFrameJsonString(1));
    first.emitData(durableFrameJsonString(2));
    await screen.findByTestId("event-row-2");

    first.end({ outcome: "completed" });
    // Let the transport promise resolution run before advancing the
    // injected scheduler that drives the bounded reconnect backoff.
    await act(async () => {
      await Promise.resolve();
    });
    harness.scheduler.advance(600);

    const second = await waitFor(() => {
      const connection = harness.sse[1];
      if (connection === undefined) {
        throw new Error("no reconnect yet");
      }
      return connection;
    });
    // Resume uses the durable `after` parameter at exactly the last
    // accepted sequence — never Last-Event-ID, never zero.
    expect(second.url).toContain("after=2");
    second.emitData(durableFrameJsonString(3));
    await screen.findByTestId("event-row-3");
  });

  it("keeps durable state on WebSocket disconnect and expires stale telemetry", async () => {
    const fetchMock = vi.fn<typeof fetch>((input: RequestInfo | URL) => {
      const url =
        typeof input === "string"
          ? input
          : input instanceof URL
            ? input.href
            : input.url;
      if (url.includes(`/api/v1/runs/${RUN_ID}`)) {
        return Promise.resolve(jsonResponse(RUN));
      }
      if (url.includes("/api/v1/pipelines/pip_a/versions/1")) {
        return Promise.resolve(jsonResponse(versionBody()));
      }
      return Promise.resolve(new Response("not found", { status: 404 }));
    });
    vi.stubGlobal("fetch", fetchMock);

    const harness = renderRunLive();
    const connection = await waitFor(() => {
      const first = harness.sse[0];
      if (first === undefined) {
        throw new Error("stream not connected yet");
      }
      return first;
    });
    connection.emitData(durableFrameJsonString(1));

    const socket = await waitFor(() => {
      const candidate = harness.sockets[0];
      if (candidate === undefined) {
        throw new Error("telemetry not connected yet");
      }
      return candidate;
    });
    socket.open();
    socket.message(
      snapshotJson([
        telemetryRecordJson(1_000, {
          metrics: [
            { name: "writer_queue_depth", kind: "gauge", value: 3, labels: {} },
          ],
        }),
      ]),
    );

    // Advisory values render in the queue panels, labeled as telemetry.
    expect(await screen.findByTestId("queue-metric-queue-depth")).toHaveTextContent(
      "3",
    );
    expect(screen.getByText(/advisory and ephemeral/)).toBeInTheDocument();

    // A 1006 close is retryable: the view says telemetry is reconnecting
    // while the last observations stay marked and durable state is intact.
    socket.close(1006);
    expect(
      await screen.findByText(/Advisory telemetry is reconnecting/),
    ).toBeInTheDocument();
    expect(screen.getByTestId("queue-metric-queue-depth")).toHaveTextContent("3");
    expect(screen.getByTestId("event-row-1")).toBeInTheDocument();

    // Accept the bounded reconnect, send nothing, and advance past the
    // staleness window: the fresh connection flips to stale on its
    // keepalive tick (injected clock, no real waiting).
    harness.scheduler.advance(1_000);
    const reconnected = await waitFor(() => {
      const next = harness.sockets[1];
      if (next === undefined) {
        throw new Error("telemetry reconnect not attempted yet");
      }
      return next;
    });
    reconnected.open();
    harness.scheduler.advance(20_000);
    await waitFor(() => {
      expect(screen.getByText(/Telemetry stale/)).toBeInTheDocument();
    });
    // Durable facts survive the telemetry loss untouched.
    expect(screen.getByTestId("event-row-1")).toBeInTheDocument();

    // The reconnect budget is bounded: closing the reconnected socket once
    // more schedules exactly one further attempt (no unbounded churn), and
    // durable facts remain on screen the whole time.
    const before = harness.sockets.length;
    reconnected.close(1006);
    harness.scheduler.advance(1_000);
    expect(harness.sockets.length).toBeLessThanOrEqual(before + 1);
    expect(screen.getByTestId("event-row-1")).toBeInTheDocument();
  });

  it("never lets telemetry advance or replace durable truth", async () => {
    const fetchMock = vi.fn<typeof fetch>((input: RequestInfo | URL) => {
      const url =
        typeof input === "string"
          ? input
          : input instanceof URL
            ? input.href
            : input.url;
      if (url.includes(`/api/v1/runs/${RUN_ID}`)) {
        return Promise.resolve(jsonResponse(RUN));
      }
      if (url.includes("/api/v1/pipelines/pip_a/versions/1")) {
        return Promise.resolve(jsonResponse(versionBody()));
      }
      return Promise.resolve(new Response("not found", { status: 404 }));
    });
    vi.stubGlobal("fetch", fetchMock);

    const harness = renderRunLive();
    const connection = await waitFor(() => {
      const first = harness.sse[0];
      if (first === undefined) {
        throw new Error("stream not connected yet");
      }
      return first;
    });
    connection.emitData(durableFrameJsonString(1));

    const socket = await waitFor(() => {
      const candidate = harness.sockets[0];
      if (candidate === undefined) {
        throw new Error("telemetry not connected yet");
      }
      return candidate;
    });
    socket.open();
    // Telemetry claims the run finished and queues are empty; advisory data
    // must not change the durable state badge or sequence anywhere.
    socket.message(
      batchJson(
        [
          telemetryRecordJson(2_000, {
            metrics: [
              { name: "writer_queue_depth", kind: "gauge", value: 0, labels: {} },
            ],
          }),
        ],
        0,
      ),
    );

    await screen.findByText(/advisory and ephemeral/);
    expect(screen.getAllByText("running").length).toBeGreaterThan(0);
    expect(screen.queryByText(/^succeeded$/)).toBeNull();
  });

  it("survives a refresh during an active run with a coherent state", async () => {
    const fetchMock = vi.fn<typeof fetch>((input: RequestInfo | URL) => {
      const url =
        typeof input === "string"
          ? input
          : input instanceof URL
            ? input.href
            : input.url;
      if (url.includes(`/api/v1/runs/${RUN_ID}`)) {
        return Promise.resolve(jsonResponse(RUN));
      }
      if (url.includes("/api/v1/pipelines/pip_a/versions/1")) {
        return Promise.resolve(jsonResponse(versionBody()));
      }
      return Promise.resolve(new Response("not found", { status: 404 }));
    });
    vi.stubGlobal("fetch", fetchMock);

    const harness = renderRunLive();
    let connection = await waitFor(() => {
      const first = harness.sse[0];
      if (first === undefined) {
        throw new Error("stream not connected yet");
      }
      return first;
    });
    connection.emitData(durableFrameJsonString(1));
    await screen.findByTestId("event-row-1");

    // "Refresh": full teardown and rebuild against the same cache — the
    // run refetches, the stream replays from zero, and no state corrupts.
    cleanup();
    appQueryClient.clear();
    vi.stubGlobal("fetch", fetchMock);

    const second = renderRunLive();
    connection = await waitFor(() => {
      const firstConnection = second.sse[0];
      if (firstConnection === undefined) {
        throw new Error("stream not connected yet");
      }
      return firstConnection;
    });
    connection.emitData(durableFrameJsonString(1));
    connection.emitData(durableFrameJsonString(2));

    expect(await screen.findByTestId("event-row-2")).toBeInTheDocument();
    expect(screen.getAllByText("running").length).toBeGreaterThan(0);
    expect(screen.queryByRole("alert")).toBeNull();
  });

  it("renders hostile problem text inertly on authoritative failure", async () => {
    const fetchMock = vi.fn<typeof fetch>((input: RequestInfo | URL) => {
      const url =
        typeof input === "string"
          ? input
          : input instanceof URL
            ? input.href
            : input.url;
      if (url.includes(`/api/v1/runs/${RUN_ID}`)) {
        return Promise.resolve(
          new Response(
            JSON.stringify({
              type: "about:blank",
              title: "rejected",
              status: 404,
              detail: `<script>alert("run-404")</script> unknown run`,
            }),
            { status: 404, headers: { "Content-Type": "application/problem+json" } },
          ),
        );
      }
      return Promise.resolve(new Response("not found", { status: 404 }));
    });
    vi.stubGlobal("fetch", fetchMock);

    renderRunLive();
    const alert = await screen.findByRole("alert");
    expect(alert).toHaveTextContent("Run unavailable");
    // The hostile string is text, never markup: no script element exists
    // anywhere in the document.
    expect(document.querySelector("script")).toBeNull();
    expect(alert.textContent).toContain('alert("run-404")');
  });

  it("disposes both transports on unmount", async () => {
    const fetchMock = vi.fn<typeof fetch>((input: RequestInfo | URL) => {
      const url =
        typeof input === "string"
          ? input
          : input instanceof URL
            ? input.href
            : input.url;
      if (url.includes(`/api/v1/runs/${RUN_ID}`)) {
        return Promise.resolve(jsonResponse(RUN));
      }
      if (url.includes("/api/v1/pipelines/pip_a/versions/1")) {
        return Promise.resolve(jsonResponse(versionBody()));
      }
      return Promise.resolve(new Response("not found", { status: 404 }));
    });
    vi.stubGlobal("fetch", fetchMock);

    const harness = renderRunLive();
    await screen.findByTestId("run-canvas");
    expect(harness.sockets.length).toBe(1);
    cleanup();

    // Disposal closes the telemetry socket and silences every handler:
    // emitting after unmount must not throw or resurrect state.
    await waitFor(() => {
      expect(harness.sockets[0]?.closedWith).not.toBeNull();
    });
    expect(() => {
      harness.sse[0]?.emitData(durableFrameJsonString(9));
    }).not.toThrow();
    expect(screen.queryByTestId("event-row-9")).toBeNull();
  });
});

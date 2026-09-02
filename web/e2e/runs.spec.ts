/**
 * Phase 17 browser evidence. Every transport is faked deterministically at
 * the browser boundary: REST via route interception, the durable SSE stream
 * via scripted bodies per connection, and advisory telemetry via a routed
 * WebSocket. Virtual time drives staleness; nothing waits on wall-clock
 * timing.
 */
import { expect, test, type Page } from "@playwright/test";

import {
  RUN_ID_E2E,
  capabilitiesE2E,
  durableFrameE2E,
  installRunRoutes,
  installTelemetrySocket,
  runE2E,
  sseBody,
  telemetrySnapshotE2E,
} from "./fixtures";

async function openRun(
  page: Page,
  sseBodies?: string[],
): Promise<{ sseConnections: () => string[]; restRunFetches: () => number }> {
  const routes = await installRunRoutes(page, { sseBodies });
  await page.goto(`/app/runs/${RUN_ID_E2E}`);
  await expect(page.getByTestId("run-canvas")).toBeVisible();
  return routes;
}

test.describe("P17.1 run list", () => {
  test("1 — filters runs and restores filter, page size, and selection from the URL", async ({
    page,
  }) => {
    const running = runE2E({ state: "running" });
    const failed = runE2E({ run_id: "run_e2e-failed-1", state: "failed" });
    await installRunRoutes(page, {
      runsPageBody: JSON.stringify({
        schema_version: 1,
        items: [running, failed],
        limit: 25,
        next_cursor: null,
      }),
    });

    await page.goto("/app/runs");
    await expect(page.getByTestId("run-row-run_e2e-live-001")).toBeVisible();

    await page.getByTestId("runs-state-filter").selectOption("failed");
    await expect(page).toHaveURL(/state=failed/);

    // Reload restores the filtered view from the URL alone.
    await page.reload();
    await expect(page).toHaveURL(/state=failed/);
    await expect(page.getByTestId("runs-state-filter")).toHaveValue("failed");

    // Direct entry with combined bounded parameters behaves identically.
    await page.goto("/app/runs?state=running&limit=10&selected=run_e2e-live-001");
    await expect(page.getByTestId("runs-state-filter")).toHaveValue("running");
    await expect(page.getByTestId("runs-limit")).toHaveValue("10");
    await expect(page.getByTestId("run-select-run_e2e-live-001")).toHaveAttribute(
      "aria-pressed",
      "true",
    );
  });
});

test.describe("P17.2 live run view", () => {
  test("2 — opens an active run from the list with durable identity", async ({
    page,
  }) => {
    const failed = runE2E({ run_id: "run_e2e-failed-1", state: "failed" });
    await installRunRoutes(page, {
      runsPageBody: JSON.stringify({
        schema_version: 1,
        items: [runE2E(), failed],
        limit: 25,
        next_cursor: null,
      }),
    });
    await page.goto("/app/runs");
    await page.getByRole("link", { name: `Open run ${RUN_ID_E2E}` }).click();
    await expect(page).toHaveURL(new RegExp(`/app/runs/${RUN_ID_E2E}$`));
    await expect(page.getByTestId("run-canvas")).toBeVisible();
    await expect(page.getByTestId("structure-row-nod_e2e-source-001")).toContainText(
      "not observed yet",
    );
  });

  test("3 — refreshes during execution and keeps one coherent state", async ({
    page,
  }) => {
    const body = sseBody([
      durableFrameE2E(1, {
        event_kind: "work_claimed",
        subject_kind: "work_item",
        subject_id: "work-1",
        payload: { node_id: "nod_e2e-source-001", attempt_number: 1 },
      }),
      durableFrameE2E(2, {
        event_kind: "work_succeeded",
        subject_kind: "work_item",
        subject_id: "work-1",
        payload: { node_id: "nod_e2e-source-001", attempt_number: 1 },
      }),
    ]);
    await openRun(page, [body]);

    const row = page.getByTestId("structure-row-nod_e2e-source-001");
    await expect(row).toContainText("work_succeeded");

    // A full refresh replays durable history from zero: the same coherent
    // state is rebuilt, never corrupted or regressed.
    await page.reload();
    await expect(page.getByTestId("run-canvas")).toBeVisible();
    await expect(page.getByTestId("structure-row-nod_e2e-source-001")).toContainText(
      "work_succeeded",
    );
    await expect(page.getByTestId("event-timeline-tbody")).toContainText(
      "work_claimed",
    );
  });

  test("4 — recovers a durable sequence gap with an authoritative refetch and resume", async ({
    page,
  }) => {
    const gapFrame = durableFrameE2E(3, {
      event_kind: "work_created",
      subject_kind: "work_item",
      subject_id: "work-2",
      payload: { node_id: "nod_e2e-export-001" },
    });
    const resume = sseBody([
      durableFrameE2E(2, {
        event_kind: "work_claimed",
        subject_kind: "work_item",
        subject_id: "work-2",
        payload: { node_id: "nod_e2e-export-001", attempt_number: 1 },
      }),
      gapFrame,
    ]);
    const routes = await openRun(page, [
      sseBody([durableFrameE2E(1), gapFrame]),
      resume,
    ]);

    // The gap is never hidden: recovery refetches authoritative state and
    // resumes from the last accepted sequence (1), skipping the gap.
    await expect
      .poll(() => routes.restRunFetches(), { timeout: 5_000 })
      .toBeGreaterThanOrEqual(2);
    await expect
      .poll(() => routes.sseConnections().some((url) => url.includes("after=1")), {
        timeout: 5_000,
      })
      .toBe(true);
    // The formerly out-of-order event is applied only after sequence 2
    // arrives on replay, proving the recovered frontier reached 3.
    await expect(page.getByTestId("event-row-2")).toBeVisible();
    await expect(page.getByTestId("event-row-3")).toBeVisible();
    await expect(page.getByTestId("event-timeline-tbody")).toContainText(
      "work_claimed",
    );
  });

  test("4b — rejects an incompatible pipeline version and retains the coherent run", async ({
    page,
  }) => {
    const incompatible = sseBody([
      durableFrameE2E(1, {
        event_kind: "run.updated",
        payload: {
          run: runE2E({ pipeline_version: 2, run_version: 3, state: "failed" }),
        },
      }),
    ]);
    await openRun(page, [incompatible]);

    // The durable run keeps its accepted identity; the incompatible
    // projection is rejected, never merged.
    await expect(
      page.getByText(/Durable stream (recovering|stale)/).first(),
    ).toBeVisible();
    await expect(page.getByText(/^running$/).first()).toBeVisible();
  });

  test("5 — keeps durable state when the telemetry socket disconnects", async ({
    page,
  }) => {
    const telemetry = await installTelemetrySocket(page);
    const routes = await openRun(page, [sseBody([durableFrameE2E(1)])]);

    // Reconnect attempts arrive at the socket route; accept the first and
    // deliver a snapshot, then close it from the server side.
    const sockets = telemetry.connections;
    await expect
      .poll(() => sockets.length, { timeout: 5_000 })
      .toBeGreaterThanOrEqual(1);
    sockets[0].send(telemetrySnapshotE2E(3));
    await expect(page.getByTestId("queue-metric-queue-depth")).toContainText("3");
    await expect(page.getByText(/advisory and ephemeral/i)).toBeVisible();

    await sockets[0].close();
    await expect(
      page.getByText(/Advisory telemetry (is reconnecting|disconnected)/),
    ).toBeVisible();
    // Telemetry loss never touches durable truth: the event stays, the
    // run identity stays, and the last advisory value stays labeled.
    await expect(page.getByTestId("event-row-1")).toBeVisible();
    await expect(page.getByTestId("queue-metric-queue-depth")).toContainText("3");
    expect(routes.restRunFetches()).toBeGreaterThanOrEqual(1);
  });

  test("6 — labels telemetry stale after the freshness window expires", async ({
    page,
  }) => {
    await page.clock.install();
    const telemetry = await installTelemetrySocket(page);
    await openRun(page, [sseBody([durableFrameE2E(1)])]);

    await expect
      .poll(() => telemetry.connections.length, { timeout: 5_000 })
      .toBeGreaterThanOrEqual(1);
    telemetry.connections[0].send(telemetrySnapshotE2E(2));
    await expect(page.getByTestId("queue-metric-queue-depth")).toContainText("2");

    // Virtual time crosses the staleness window; the next keepalive tick
    // flips the channel to stale and the panel says so explicitly.
    await page.clock.runFor(25_000);
    await expect(page.getByText(/Telemetry stale/)).toBeVisible();
    await expect(page.getByTestId("queue-metric-queue-depth")).toContainText("2");
  });
});

test.describe("P17.3 accessible timeline", () => {
  test("7 — navigates the structure table, swimlane table, and zoom by keyboard", async ({
    page,
  }) => {
    const body = sseBody([
      durableFrameE2E(1, {
        event_kind: "work_claimed",
        subject_kind: "work_item",
        subject_id: "work-1",
        payload: { node_id: "nod_e2e-source-001", attempt_number: 1 },
      }),
      durableFrameE2E(2, {
        event_kind: "work_succeeded",
        subject_kind: "work_item",
        subject_id: "work-1",
        payload: { node_id: "nod_e2e-source-001", attempt_number: 1 },
      }),
    ]);
    await openRun(page, [body]);

    // The graph has a semantic table twin with the same durable facts.
    const structure = page.getByRole("table", {
      name: "Pipeline structure with durable state",
    });
    await expect(structure).toBeVisible();
    await expect(structure).toContainText("work_succeeded");

    // Keyboard: focus the swimlane zoom control and change the window.
    await page.getByTestId("swimlane-zoom").focus();
    await expect(page.getByTestId("swimlane-zoom")).toBeFocused();
    await page.keyboard.press("ArrowDown");
    await expect(page.getByTestId("swimlane-zoom")).toHaveValue(/half|quarter/);

    // The event timeline is a plain keyboard-reachable table.
    const timeline = page.getByRole("table", { name: "Durable event timeline" });
    await expect(timeline).toBeVisible();
    await expect(timeline).toContainText("1");
  });
});

test.describe("P17.6 runner comparison", () => {
  test("8 — compares compatible runs correctness-first", async ({ page }) => {
    await installRunRoutes(page);
    const agree = runE2E({
      state: "succeeded",
      finished_at: "2026-01-01T00:00:31Z",
      execution_evidence_fingerprint: "a".repeat(64),
      execution_evidence_fingerprint_version: 2,
    });
    const compatible = runE2E({
      run_id: "run_e2e-async-01",
      runner_kind: "asyncio",
      state: "succeeded",
      finished_at: "2026-01-01T00:00:41Z",
      execution_evidence_fingerprint: "a".repeat(64),
      execution_evidence_fingerprint_version: 2,
    });
    await page.route("**/api/v1/runs?*", (route) =>
      route.fulfill({
        status: 200,
        contentType: "application/json",
        body: JSON.stringify({
          schema_version: 1,
          items: [agree, compatible],
          limit: 50,
          next_cursor: null,
        }),
      }),
    );
    await page.route("**/api/v1/runs/run_e2e-live-001", (route) =>
      route.fulfill({
        status: 200,
        contentType: "application/json",
        body: JSON.stringify(agree),
      }),
    );
    await page.route("**/api/v1/runs/run_e2e-async-01", (route) =>
      route.fulfill({
        status: 200,
        contentType: "application/json",
        body: JSON.stringify(compatible),
      }),
    );

    await page.goto(`/app/compare?runs=${RUN_ID_E2E},run_e2e-async-01`);
    const correctness = page.getByTestId("compare-correctness");
    await expect(correctness).toBeVisible();
    // Correctness is rendered before speed in document order.
    const speed = page.getByTestId("compare-speed");
    const order = await correctness.evaluate((correctnessEl, speedSelector) => {
      const speedEl = document.querySelector(speedSelector);
      if (speedEl === null) {
        return false;
      }
      return (
        (correctnessEl.compareDocumentPosition(speedEl) &
          Node.DOCUMENT_POSITION_FOLLOWING) !==
        0
      );
    }, "[data-testid=compare-speed]");
    expect(order).toBe(true);
    await expect(correctness).toContainText("agree");
    // Duration values are the durable timestamps; unsupported metrics say
    // why they are absent.
    await expect(speed).toContainText("30.000 s");
    await expect(speed).toContainText("40.000 s");
    await expect(speed).toContainText("unavailable —");
    await expect(page.getByText(/implying an equivalence/i)).toBeVisible();
  });

  test("9 — blocks incompatible runner comparison with the exact reason", async ({
    page,
  }) => {
    await installRunRoutes(page);
    const foreign = runE2E({
      run_id: "run_e2e-async-01",
      runner_kind: "asyncio",
      state: "succeeded",
      pipeline_version: 2,
      execution_evidence_fingerprint: "b".repeat(64),
      execution_evidence_fingerprint_version: 1,
    });
    const sequential = runE2E({
      state: "succeeded",
      execution_evidence_fingerprint: "a".repeat(64),
      execution_evidence_fingerprint_version: 2,
    });
    await page.route("**/api/v1/runs/run_e2e-live-001", (route) =>
      route.fulfill({
        status: 200,
        contentType: "application/json",
        body: JSON.stringify(sequential),
      }),
    );
    await page.route("**/api/v1/runs?*", (route) =>
      route.fulfill({
        status: 200,
        contentType: "application/json",
        body: JSON.stringify({
          schema_version: 1,
          items: [sequential, foreign],
          limit: 50,
          next_cursor: null,
        }),
      }),
    );
    await page.route("**/api/v1/runs/run_e2e-async-01", (route) =>
      route.fulfill({
        status: 200,
        contentType: "application/json",
        body: JSON.stringify(foreign),
      }),
    );

    await page.goto(`/app/compare?runs=${RUN_ID_E2E},run_e2e-async-01`);
    const blocked = page.getByTestId("compare-blocked");
    await expect(blocked).toBeVisible();
    await expect(blocked).toContainText("pipeline_version_mismatch");
    await expect(blocked).toContainText("evidence_format_mismatch");
    await expect(page.getByTestId("compare-correctness")).toHaveCount(0);
    await expect(page.getByTestId("compare-speed")).toHaveCount(0);
  });
});

test.describe("P17.7 system capabilities", () => {
  test("10 — reports available, unavailable, unsupported, and failure states", async ({
    page,
  }) => {
    await installRunRoutes(page);
    let failing = false;
    await page.route("**/api/v1/system/capabilities", (route) => {
      if (failing) {
        return route.fulfill({ status: 500, body: "boom" });
      }
      return route.fulfill({
        status: 200,
        contentType: "application/json",
        body: JSON.stringify(capabilitiesE2E()),
      });
    });

    await page.goto("/app/system");
    const runners = page.getByTestId("system-runners");
    await expect(runners).toContainText("Runner strategy sequential");
    await expect(runners).toContainText("reason: no running event loop");
    await expect(runners).toContainText("unsupported");
    await expect(page.getByText("migrations applied")).toBeVisible();
    await expect(page.getByText("max_page_size")).toBeVisible();

    // A capability failure is an error, never a healthy default.
    failing = true;
    await page.reload();
    await expect(page.getByText("Capability report unavailable")).toBeVisible();
    await expect(runners).toContainText("unknown");
  });
});

test.describe("Responsive and reduced motion", () => {
  test("11 — keeps the run list usable at 600px and honors reduced motion", async ({
    page,
  }) => {
    await page.setViewportSize({ width: 600, height: 900 });
    await page.emulateMedia({ reducedMotion: "reduce" });
    await installRunRoutes(page, {
      sseBodies: [
        sseBody([
          durableFrameE2E(1, {
            event_kind: "work_claimed",
            subject_kind: "work_item",
            subject_id: "work-1",
            payload: { node_id: "nod_e2e-source-001", attempt_number: 1 },
          }),
          durableFrameE2E(2, {
            event_kind: "work_succeeded",
            subject_kind: "work_item",
            subject_id: "work-1",
            payload: { node_id: "nod_e2e-source-001", attempt_number: 1 },
          }),
        ]),
      ],
    });

    await page.goto("/app/runs");
    await expect(page.getByTestId("run-row-run_e2e-live-001")).toBeVisible();
    await expect(page.getByTestId("runs-state-filter")).toBeVisible();

    await page.goto(`/app/runs/${RUN_ID_E2E}`);
    await expect(page.getByTestId("run-canvas")).toBeVisible();
    // The semantic tables keep every essential state visible at narrow
    // widths, and the decoration chart declares reduced-motion support.
    await expect(
      page.getByRole("table", { name: "Pipeline structure with durable state" }),
    ).toBeVisible();
    await expect(page.getByTestId("swimlane-svg")).toHaveClass(/motion-reduce/);
  });
});

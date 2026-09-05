/**
 * Phase 20 canonical Chromium scenario: the REAL application end to end
 * against the REAL packaged demo backend. Zero mocks — no `page.route` and
 * no `routeWebSocket` anywhere in this file; every fact is served by the
 * spawned `paritygrid demo` backend (see `./demo-harness`), which carries
 * the pre-executed canonical story (run, reconciliation, conflicts, applied
 * repair plan) plus the three canonical engine runs with equal
 * execution-evidence fingerprints.
 *
 * All waits are observable-state waits (`expect` and `expect.poll`); there
 * are no fixed sleeps and no visual-regression snapshots. The proof drives
 * pause, resume, and cancel through rendered controls, never by bypassing
 * the UI with request-context calls.
 */
import { expect, test, type Page } from "@playwright/test";

import { startDemoBackend, type DemoBackend } from "./demo-harness";

test.use({ viewport: { width: 1440, height: 900 } });

// Generous per-test headroom for a real backend on a developer machine;
// the assertions themselves are observable-state waits.
test.setTimeout(120_000);

let backend: DemoBackend | null = null;

/** Absolute URL on the real demo backend (the preview server is irrelevant). */
function demoUrl(pathname: string): string {
  if (backend === null) {
    throw new Error("the demo backend is not running");
  }
  return new URL(pathname, backend.baseUrl).toString();
}

test.beforeAll(async () => {
  // Inside a hook, setTimeout() bounds the hook itself.
  test.setTimeout(600_000);
  backend = await startDemoBackend();
});

test.afterAll(async () => {
  test.setTimeout(120_000);
  const handle = backend;
  backend = null;
  if (handle !== null) {
    // Asserts process exit, a dead port, and a surviving demo root.
    await handle.stop();
  }
});

/** Drive one retry-safe command only through its rendered UI control. */
async function driveRunControl(
  page: Page,
  action: "pause" | "resume" | "cancel",
  targetState: "paused" | "running" | "cancelled",
): Promise<void> {
  const identity = page.getByRole("region", { name: "Run identity" });
  const button = page.getByTestId(`run-${action}`);
  await expect(button).toBeEnabled();
  const [response] = await Promise.all([
    page.waitForResponse((candidate) => {
      const request = candidate.request();
      return (
        request.method() === "POST" &&
        new URL(candidate.url()).pathname.endsWith(`/${action}`)
      );
    }),
    button.click(),
  ]);
  const responseBody = (await response.json()) as Record<string, unknown>;
  expect(
    response.ok(),
    `${action} returned HTTP ${String(response.status())}: ${JSON.stringify(responseBody)}`,
  ).toBe(true);
  expect(responseBody.state).toBe(targetState);

  await expect
    .poll(
      async () => {
        if (await identity.getByText(targetState, { exact: true }).isVisible()) {
          return targetState;
        }
        // A resumed run may complete between two browser observations. That
        // terminal success proves resume won; requiring the transient
        // `running` label would turn correct fast completion into a race.
        if (
          action === "resume" &&
          (await identity.getByText("succeeded", { exact: true }).isVisible())
        ) {
          return targetState;
        }
        return "pending";
      },
      { timeout: 30_000 },
    )
    .toBe(targetState);
  if (action === "pause") {
    // The durable SSE frame can render PAUSED before the HTTP mutation has
    // returned. Do not begin resume until the UI has also released the
    // mutation lock and made the next control actionable.
    await expect(page.getByTestId("run-resume")).toBeEnabled();
  }
  await expect(page.getByRole("alert")).toHaveCount(0);
}

test("01 real backend health and direct-entry application shells", async ({
  page,
  request,
}) => {
  const health = await request.get(demoUrl("healthz"));
  expect(health.ok()).toBe(true);
  const healthBody = (await health.json()) as Record<string, unknown>;
  expect(healthBody).toMatchObject({ status: "ok", service: "ParityGrid" });

  // Direct entry to the packaged application's public overview.
  await page.goto(demoUrl("/"));
  await expect(
    page.getByRole("heading", { name: "Reconciliation you can prove." }),
  ).toBeVisible();

  // Direct entry deep into the operations shell (SPA fallback on the backend).
  await page.goto(demoUrl("/app"));
  await expect(
    page.getByRole("heading", { name: "Operations overview" }),
  ).toBeVisible();
  await expect(
    page.getByText("Durable facts from the accepted REST boundary only."),
  ).toBeVisible();
});

test("02 run list shows the canonical story run as succeeded", async ({ page }) => {
  await page.goto(demoUrl("/app/runs"));
  await expect(page.getByRole("heading", { name: "Run history" })).toBeVisible();

  const storyRow = page.getByTestId("run-row-run_canonical-demo");
  await expect(storyRow).toBeVisible();
  await expect(storyRow).toContainText("pip_canonical-demo v1");
  await expect(storyRow.getByText("succeeded", { exact: true })).toBeVisible();

  // The three canonical engine runs exist side by side with the story run.
  for (const offset of ["0001", "0002", "0003"]) {
    await expect(page.getByTestId(`run-row-run_can-engine-${offset}`)).toBeVisible();
  }
});

test("03 live run detail renders durable retry, quarantine, and graph truth", async ({
  page,
}) => {
  // The run page renders the authoritative REST identity.
  await page.goto(demoUrl("/app/runs/run_canonical-demo"));
  const identity = page.getByRole("region", { name: "Run identity" });
  await expect(
    identity.getByRole("heading", { name: "run_canonical-demo" }),
  ).toBeVisible();
  await expect(identity.getByText("succeeded", { exact: true })).toBeVisible();
  await expect(identity.getByText("run_version 43")).toBeVisible();
  await expect(identity.getByText(/evidence [0-9a-f]{64}/)).toBeVisible();

  // Version-2 work and reconciliation payloads stay live, so the scripted
  // retry and source quarantine are both visible durable facts.
  const timeline = page.getByTestId("event-timeline");
  await expect(timeline.getByText("work_retry_wait", { exact: true })).toBeVisible();
  await expect(page.getByTestId("run-quarantine")).toContainText(
    "4 source records quarantined",
  );
  await expect(page.getByText("Durable stream stale")).toHaveCount(0);

  // The real immutable published-specification envelope parses and renders
  // the graph plus its accessible semantic table.
  await expect(page.getByTestId("run-canvas")).toBeVisible();
  await expect(page.getByTestId("structure-row-nod_can-reconcile")).toBeVisible();
  await expect(
    page.getByText("The pipeline version document is unavailable"),
  ).toHaveCount(0);
});

test("04 a public run executes through the launcher and recovers across reloads", async ({
  page,
  request,
}) => {
  const runId = "run-can-e2e-threaded".replace("run-can", "run_can");
  const created = await request.post(demoUrl("api/v1/runs"), {
    headers: { "Idempotency-Key": `e2e-demo-create-threaded-${String(Date.now())}` },
    data: {
      run_id: runId,
      pipeline_id: "pip_canonical-demo",
      pipeline_version: 1,
      runner_kind: "threaded",
      scenario_seed: 19,
    },
  });
  expect(created.status()).toBe(201);
  const createdBody = (await created.json()) as Record<string, unknown>;
  expect(createdBody).toMatchObject({ run_id: runId, state: "queued" });

  // The demo's execution launcher durably admits the public run: the run
  // leaves queued through a real durable start transition.
  await expect
    .poll(async () => {
      const response = await request.get(demoUrl(`api/v1/runs/${runId}`));
      return ((await response.json()) as Record<string, unknown>).state;
    })
    .toBe("running");

  // Reload recovery while the paced engine is executing: a fresh load
  // returns to a coherent live view from the durable REST/SSE sources
  // alone — no error boundary, and the run identity survives the reload.
  await page.goto(demoUrl(`/app/runs/${runId}`));
  const identity = page.getByRole("region", { name: "Run identity" });
  await expect(identity.getByRole("heading", { name: runId })).toBeVisible();
  await page.reload();
  await expect(identity.getByRole("heading", { name: runId })).toBeVisible();
  await expect(page.getByText("Run unavailable")).toHaveCount(0);

  // The paced engine run reaches its durable success with execution
  // evidence, through the ordinary product boundary.
  await expect
    .poll(
      async () => {
        const response = await request.get(demoUrl(`api/v1/runs/${runId}`));
        return ((await response.json()) as Record<string, unknown>).state;
      },
      { timeout: 60_000 },
    )
    .toBe("succeeded");
  const finalResponse = await request.get(demoUrl(`api/v1/runs/${runId}`));
  const final = (await finalResponse.json()) as Record<string, unknown>;
  expect(final.execution_evidence_fingerprint).toMatch(/^[0-9a-f]{64}$/);
});

test("05 pause and resume an executing public run through the product controls", async ({
  page,
  request,
}) => {
  const runId = "run-can-e2e-pause".replace("run-can", "run_can");
  const created = await request.post(demoUrl("api/v1/runs"), {
    headers: { "Idempotency-Key": `e2e-demo-create-pause-${String(Date.now())}` },
    data: {
      run_id: runId,
      pipeline_id: "pip_canonical-demo",
      pipeline_version: 1,
      runner_kind: "asyncio",
      scenario_seed: 19,
    },
  });
  expect(created.status()).toBe(201);

  // Wait for the launcher's durable start and at least one post-bootstrap
  // engine fact. The RUNNING transition precedes owner registration, so the
  // state alone is not sufficient evidence that controls are ready.
  await expect
    .poll(async () => {
      const response = await request.get(demoUrl(`api/v1/runs/${runId}`));
      const run = (await response.json()) as Record<string, unknown>;
      return run.state === "running" && Number(run.run_version) >= 15
        ? "control-ready"
        : "pending";
    })
    .toBe("control-ready");
  await page.goto(demoUrl(`/app/runs/${runId}`));
  const identity = page.getByRole("region", { name: "Run identity" });
  await expect(identity.getByRole("heading", { name: runId })).toBeVisible();
  await driveRunControl(page, "pause", "paused");

  // Resume through the product control; the run completes durably.
  await driveRunControl(page, "resume", "running");
  await expect
    .poll(
      async () => {
        const response = await request.get(demoUrl(`api/v1/runs/${runId}`));
        return ((await response.json()) as Record<string, unknown>).state;
      },
      { timeout: 60_000 },
    )
    .toBe("succeeded");
});

test("06 safe cancellation of a separate executing public run", async ({
  page,
  request,
}) => {
  const runId = "run-can-e2e-cancel".replace("run-can", "run_can");
  const created = await request.post(demoUrl("api/v1/runs"), {
    headers: { "Idempotency-Key": `e2e-demo-create-cancel-${String(Date.now())}` },
    data: {
      run_id: runId,
      pipeline_id: "pip_canonical-demo",
      pipeline_version: 1,
      runner_kind: "asyncio",
      scenario_seed: 19,
    },
  });
  expect(created.status()).toBe(201);

  // Cancel while the paced engine is executing: the engine drains and
  // durably cancels its own work through the product control.
  await expect
    .poll(async () => {
      const response = await request.get(demoUrl(`api/v1/runs/${runId}`));
      return ((await response.json()) as Record<string, unknown>).state;
    })
    .toBe("running");
  await page.goto(demoUrl(`/app/runs/${runId}`));
  const identity = page.getByRole("region", { name: "Run identity" });
  await expect(identity.getByRole("heading", { name: runId })).toBeVisible();
  await driveRunControl(page, "cancel", "cancelled");
  await expect(identity.getByText("cancelled", { exact: true })).toBeVisible();
  await expect(page.getByText("Run unavailable")).toHaveCount(0);
});

test("07 reconciliation workbench shows the canonical summary and a working filter", async ({
  page,
}) => {
  await page.goto(demoUrl("/app/runs/run_canonical-demo/reconcile"));

  const summary = page.getByTestId("reconciliation-summary");
  await expect(summary).toBeVisible();

  // The reconciliation fingerprint is rendered as a full 64-hex string
  // (alongside the two 64-hex input identities).
  await expect(summary.getByText(/^[0-9a-f]{64}$/)).toHaveCount(3);

  // The canonical classification counts, served by the real analytics path.
  await expect(summary.getByTestId("count-match")).toHaveText("23");
  await expect(summary.getByTestId("count-missing_from_target")).toHaveText("6");
  await expect(summary.getByTestId("count-missing_from_source")).toHaveText("4");
  await expect(summary.getByTestId("count-field_mismatch")).toHaveText("3");
  await expect(summary.getByTestId("count-duplicate_source")).toHaveText("4");
  await expect(summary.getByTestId("count-duplicate_target")).toHaveText("2");
  await expect(summary.getByTestId("count-duplicate_both")).toHaveText("1");

  // The conflict table: 20 non-match conflicts, resident on one page.
  const table = page.getByTestId("conflict-table");
  const rows = table.locator("tbody tr");
  await expect(rows).toHaveCount(20);

  // The classification filter is a deterministic client-side view.
  const filter = page.getByTestId("conflict-filter");
  await filter.selectOption({ label: "Field mismatch" });
  await expect(rows).toHaveCount(3);
  await expect(table.getByText(/filtered view of loaded data/)).toBeVisible();

  await filter.selectOption({ label: "Missing from target" });
  await expect(rows).toHaveCount(6);

  await filter.selectOption({ label: "All" });
  await expect(rows).toHaveCount(20);

  // Selecting a conflict announces it and drives the field-level inspector.
  await rows.first().click();
  await expect(page.getByTestId("conflict-selected-announce")).toContainText(
    "selected",
  );
});

test("08 the applied canonical repair plan blocks approval and application", async ({
  page,
}) => {
  // The canonical story already approved and applied its plan; the review
  // screen must render that terminal state truthfully. The interactive
  // approve/apply flows with deterministic fixtures are the Phase 18
  // suite's job (mocked); here the applied/verified state and the
  // permanently disabled apply control are what the real product offers.
  await page.goto(demoUrl(`/app/repairs/${backend?.repairPlanId ?? ""}`));

  const review = page.getByTestId("repair-review");
  await expect(review).toBeVisible();
  await expect(page.getByTestId("plan-status")).toHaveText(/Applied/);
  await expect(page.getByTestId("plan-fingerprint")).toHaveText(/^[0-9a-f]{64}$/);
  await expect(page.getByTestId("plan-reconciliation-fingerprint")).toHaveText(
    /^[0-9a-f]{64}$/,
  );

  const actions = page.getByTestId("action-table").locator("tbody tr");
  await expect(actions).toHaveCount(9);
  await expect(page.getByTestId("action-table")).toContainText("applied");

  // Approval is blocked for an applied plan; apply is disabled permanently
  // once the plan is authoritative-applied.
  await expect(page.getByTestId("approval-blocked")).toBeVisible();
  await expect(page.getByTestId("apply-button")).toBeDisabled();
  await expect(page.getByTestId("apply-done-note")).toContainText(
    "apply is disabled permanently",
  );
});

test("09 runner comparison proves execution-evidence agreement correctness-first", async ({
  page,
}) => {
  await page.goto(demoUrl("/app/compare"));
  await expect(page.getByRole("heading", { name: "Runner comparison" })).toBeVisible();

  // Select the three canonical engine runs (sequential, threaded, asyncio
  // strategies over the identical canonical plan) — selection is URL-backed.
  // The checkbox is controlled through a router navigation, so the state
  // assertion (not the click) carries the wait.
  for (const offset of ["0001", "0002", "0003"]) {
    const checkbox = page.getByTestId(`compare-check-run_can-engine-${offset}`);
    await checkbox.click();
    await expect(checkbox).toBeChecked();
  }

  const dashboard = page.getByTestId("compare-dashboard");
  await expect(dashboard).toBeVisible();
  await expect(page.getByTestId("compare-blocked")).toHaveCount(0);

  // Correctness first: the three runs agree on the execution-evidence
  // fingerprint (the only equivalence the product claims).
  const correctness = page.getByTestId("compare-correctness");
  await expect(correctness.getByText("agree", { exact: true })).toHaveCount(3);

  // With correctness on the record, the speed section renders with real
  // durable durations plus the explicitly-unavailable metric rows.
  await expect(page.getByTestId("compare-speed")).toBeVisible();
  await expect(page.getByTestId("compare-duration-chart")).toBeVisible();
  await expect(page.getByTestId("compare-unavailable").first()).toBeVisible();
});

test("10 unknown API routes answer 404 problem+json, never the SPA", async ({
  request,
}) => {
  const response = await request.get(demoUrl("api/v1/nonexistent"));
  expect(response.status()).toBe(404);
  expect(response.headers()["content-type"]).toContain("application/problem+json");
  const problem = (await response.json()) as Record<string, unknown>;
  expect(problem.status).toBe(404);
  expect(typeof problem.title).toBe("string");
});

/** The exact production shell policy from `security_headers.py` (P22.3). */
const PRODUCTION_SHELL_CSP =
  "default-src 'none'; script-src 'self'; style-src 'self'; " +
  "img-src 'self'; font-src 'self'; connect-src 'self'; " +
  "object-src 'none'; frame-ancestors 'none'; base-uri 'none'; " +
  "form-action 'self'";

test("11 the real packaged app serves the production CSP on the wire", async ({
  request,
}) => {
  const shell = await request.get(demoUrl("/"));
  expect(shell.status()).toBe(200);
  expect(shell.headers()["content-security-policy"]).toBe(PRODUCTION_SHELL_CSP);
  expect(shell.headers()["x-content-type-options"]).toBe("nosniff");
  expect(shell.headers()["x-frame-options"]).toBe("DENY");
  expect(shell.headers()["referrer-policy"]).toBe("no-referrer");
  const html = await shell.text();
  expect(html).toMatch(/src="\/assets\/[^"]+\.js"/);
  // The packaged shell references no remote origin anywhere.
  expect(html).not.toContain("https://");
  expect(html).not.toContain("http://");
});

test("12 inline script is blocked and never executes under the real packaged app", async ({
  page,
}) => {
  await page.addInitScript(() => {
    const recorded: { directive: string; blocked: string }[] = [];
    (window as unknown as Record<string, unknown>).__cspViolations = recorded;
    document.addEventListener("securitypolicyviolation", (event) => {
      recorded.push({
        directive: event.effectiveDirective,
        blocked: event.blockedURI,
      });
    });
  });
  await page.goto(demoUrl("/"));
  await expect(page.getByRole("banner")).toBeVisible();

  await page.evaluate(() => {
    const marker = document.createElement("script");
    marker.textContent = "window.__inlineRan = true;";
    document.body.append(marker);
  });
  await expect
    .poll(() =>
      page.evaluate(() =>
        (
          (window as unknown as Record<string, unknown>).__cspViolations as {
            directive: string;
            blocked: string;
          }[]
        ).some(
          (entry) =>
            (entry.directive === "script-src" ||
              entry.directive === "script-src-elem") &&
            entry.blocked !== "eval",
        ),
      ),
    )
    .toBe(true);
  expect(
    await page.evaluate(
      () => (window as unknown as Record<string, unknown>).__inlineRan,
    ),
  ).toBeUndefined();
});

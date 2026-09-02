/**
 * Phase 18 browser evidence for the repair review route: plan review
 * before approval, explicit fingerprint confirmation, stale-plan blocking,
 * destructive-action blocking, idempotent application with durable event
 * progress, replay-safe reconnects, and 503 recovery. Every transport is
 * faked at the browser boundary; nothing touches a network.
 */
import { expect, test, type Page } from "@playwright/test";

import {
  FINGERPRINT_CONTENT,
  FINGERPRINT_RECON,
  FINGERPRINT_RECON_NEW,
  REPAIR_PLAN_ID,
  REPAIR_COMPLETED_PAYLOAD,
  REPAIR_STARTED_PAYLOAD,
  installRepairRoutes,
  repairApplyResponseE2E,
  repairFrameE2E,
  repairPlanE2E,
} from "./fixtures-p18";

async function confirmApplication(page: Page): Promise<void> {
  await page.getByTestId("apply-open").click();
  const submit = page.getByTestId("apply-button");
  await expect(submit).toBeDisabled();
  await page.getByTestId("apply-confirmation").check();
  await expect(submit).toBeEnabled();
  await submit.click();
}

test.describe("P18.4 repair-plan review and approval", () => {
  test("1 — reviews the full proposed plan before any approval", async ({ page }) => {
    const routes = await installRepairRoutes(page);
    await page.goto(`/app/repairs/${REPAIR_PLAN_ID}`);
    await expect(page.getByTestId("repair-review")).toBeVisible();

    await expect(page.getByTestId("plan-status")).toHaveText("Proposed");
    await expect(page.getByTestId("plan-fingerprint")).toHaveText(FINGERPRINT_CONTENT);
    await expect(page.getByTestId("plan-reconciliation-fingerprint")).toHaveText(
      FINGERPRINT_RECON,
    );
    await expect(page.getByTestId("action-table")).toBeVisible();
    // The create_target sentinel renders as an explicit none (create) note.
    await expect(page.getByTestId("before-sentinel-rac_e2e-001")).toHaveText(
      "none (create)",
    );
    await expect(
      page.getByText("Deletion actions do not exist in this product."),
    ).toBeVisible();
    // The truthful creation limitation is stated on the review surface.
    await expect(
      page.getByText(
        /Creating new repair plans from reconciliation projections is unavailable/,
      ),
    ).toBeVisible();
    expect(routes.approvals().length).toBe(0);
    expect(routes.applies().length).toBe(0);
  });

  test("2 — blocks a plan carrying an unsupported destructive action", async ({
    page,
  }) => {
    await installRepairRoutes(page, {
      plan: repairPlanE2E({
        actions: [
          ...(repairPlanE2E().actions as Record<string, unknown>[]),
          {
            schema_version: 1,
            action_id: "rac_e2e-003",
            canonical_key: "sku-00003",
            kind: "delete_target",
            status: "pending",
            before_sha256: "44".repeat(32),
            proposed_after_sha256: "55".repeat(32),
            applied_at: null,
            failed_at: null,
            target_version: null,
          },
        ],
      }),
    });
    await page.goto(`/app/repairs/${REPAIR_PLAN_ID}`);
    await expect(page.getByTestId("repair-review")).toBeVisible();

    // The foreign kind stays visible and labeled, and blocks both commands.
    await expect(
      page.getByTestId("action-table").getByText("delete_target"),
    ).toBeVisible();
    await expect(page.getByTestId("action-unsupported-rac_e2e-003")).toBeVisible();
    await expect(page.getByTestId("approval-blocked")).toBeVisible();
    await expect(page.getByTestId("apply-blocked")).toBeVisible();
    await expect(page.getByTestId("approval-submit")).toHaveCount(0);
    await expect(page.getByTestId("apply-button")).toHaveCount(0);
  });

  test("3 — rejects mismatched reviewed fingerprints without any POST", async ({
    page,
  }) => {
    const routes = await installRepairRoutes(page);
    await page.goto(`/app/repairs/${REPAIR_PLAN_ID}`);
    await expect(page.getByTestId("repair-review")).toBeVisible();

    await page.getByTestId("approval-open").click();
    await expect(page.getByText("Confirm approval")).toBeVisible();

    // No operator: both fingerprint fields may be filled, submit still refused.
    await page.getByTestId("approval-content-fingerprint").fill("deadbeef");
    await page.getByTestId("approval-submit").click();
    await expect(page.getByText(/Operator identity is required/i)).toBeVisible();
    expect(routes.approvals().length).toBe(0);

    await page.getByTestId("approval-operator").fill("operator-browser");
    await page.getByTestId("approval-submit").click();
    await expect(
      page.getByText(/must match the displayed plan content fingerprint/i),
    ).toBeVisible();
    expect(routes.approvals().length).toBe(0);
  });

  test("4 — approves with the exact reviewed fingerprints and one stable key", async ({
    page,
  }) => {
    const routes = await installRepairRoutes(page);
    await page.goto(`/app/repairs/${REPAIR_PLAN_ID}`);
    await expect(page.getByTestId("repair-review")).toBeVisible();

    await page.getByTestId("approval-open").click();
    await page.getByTestId("approval-operator").fill("operator-browser");
    await page.getByTestId("approval-content-fingerprint").fill(FINGERPRINT_CONTENT);
    await page
      .getByTestId("approval-reconciliation-fingerprint")
      .fill(FINGERPRINT_RECON);
    await page.getByTestId("approval-submit").click();

    await expect(page.getByTestId("plan-status")).toHaveText("Approved");
    expect(routes.approvals().length).toBe(1);
    const approval = routes.approvals()[0];
    const body = JSON.parse(approval.body) as Record<string, unknown>;
    expect(body).toEqual({
      schema_version: 1,
      approved_by: "operator-browser",
      approved_content_fingerprint: FINGERPRINT_CONTENT,
      approved_reconciliation_fingerprint: FINGERPRINT_RECON,
    });
    expect(approval.key).toMatch(/^[A-Za-z0-9][A-Za-z0-9._:-]*$/);
    expect((approval.key ?? "").length).toBeLessThanOrEqual(128);
    // Re-approval of an approved plan is blocked with a durable-approval reason.
    await expect(page.getByTestId("approval-blocked")).toBeVisible();
  });
});

test.describe("P18.5 application, progress, and recovery", () => {
  test("5 — rejects a stale plan whose reconciliation fingerprint moved on", async ({
    page,
  }) => {
    await installRepairRoutes(page, {
      plan: repairPlanE2E({ status: "approved", approval: {} }),
      fenceFingerprint: FINGERPRINT_RECON_NEW,
    });
    await page.goto(`/app/repairs/${REPAIR_PLAN_ID}`);
    await expect(page.getByTestId("repair-review")).toBeVisible();

    await expect(page.getByTestId("approval-blocked")).toBeVisible();
    await expect(page.getByTestId("apply-blocked")).toBeVisible();
    await expect(
      page.getByTestId("apply-blocked").getByText(/plan is stale and superseded/),
    ).toBeVisible();
    // The apply control stays visible for the approved plan but is inert.
    await expect(page.getByTestId("apply-button")).toBeDisabled();
  });

  test("6 — applies an approved plan, observes durable progress, and disables apply", async ({
    page,
  }) => {
    const routes = await installRepairRoutes(page, {
      plan: repairPlanE2E({ status: "approved", approval: {} }),
      sseFrames: [
        [
          repairFrameE2E(1, "repair_application_started", REPAIR_STARTED_PAYLOAD),
          repairFrameE2E(2, "repair_action_applied", {
            canonical_key: "sku-00001",
            outcome: "applied",
            target_version: 1,
          }),
        ],
      ],
    });
    await page.goto(`/app/repairs/${REPAIR_PLAN_ID}`);
    await expect(page.getByTestId("repair-review")).toBeVisible();

    await confirmApplication(page);

    await expect(page.getByTestId("outcome-disposition")).toHaveText("Completed");
    await expect(page.getByTestId("repair-progress")).toBeVisible();
    await expect(page.getByTestId("progress-notifications")).toContainText(
      "repair_application_started",
    );
    // Durable notifications refreshed the authoritative plan query.
    await waitForPlanRefresh(routes);
    // Completion disables apply permanently (authoritative applied status).
    await expect(page.getByTestId("apply-button")).toBeDisabled();
    await expect(page.getByTestId("apply-done-note")).toBeVisible();
    expect(routes.applies().length).toBe(1);
    expect(routes.applies()[0].key).toMatch(/^app-/);
  });

  test("7 — replays duplicate durable events after reconnect without duplication", async ({
    page,
  }) => {
    const approvedFrame = (): Record<string, unknown> =>
      repairFrameE2E(2, "repair_plan_approved", {
        content_fingerprint: FINGERPRINT_CONTENT,
        reconciliation_fingerprint: FINGERPRINT_RECON,
      });
    // The reconnecting connection replays durable history 1..2 and adds 3;
    // the fixture serves each connection only what its `after` position
    // requested, exactly like the durable stream contract requires.
    await installRepairRoutes(page, {
      plan: repairPlanE2E({ status: "approved", approval: {} }),
      sseFrames: [
        [
          repairFrameE2E(1, "repair_application_started", REPAIR_STARTED_PAYLOAD),
          approvedFrame(),
        ],
        [
          repairFrameE2E(1, "repair_application_started", REPAIR_STARTED_PAYLOAD),
          approvedFrame(),
          repairFrameE2E(3, "repair_application_completed", REPAIR_COMPLETED_PAYLOAD),
        ],
      ],
    });
    await page.goto(`/app/repairs/${REPAIR_PLAN_ID}`);
    await expect(page.getByTestId("repair-progress")).toBeVisible();

    await expect(page.getByTestId("progress-notifications")).toContainText(
      "repair_application_completed",
    );
    const items = page.getByTestId("progress-notifications").locator("li");
    await expect(items).toHaveText([/sequence 1/, /sequence 2/, /sequence 3/]);
    // Every replayed frame advanced the view exactly once.
    await expect(page.getByTestId("progress-quarantined")).toHaveCount(0);
  });

  test("8 — treats a 503 as recoverable and retries under the same key", async ({
    page,
  }) => {
    const routes = await installRepairRoutes(page, {
      plan: repairPlanE2E({ status: "approved", approval: {} }),
      applyStatus: 503,
    });
    await page.goto(`/app/repairs/${REPAIR_PLAN_ID}`);
    await expect(page.getByTestId("repair-review")).toBeVisible();

    await confirmApplication(page);
    await expect(page.getByText(/Recoverable failure \(503\)/)).toBeVisible();
    await expect(page.getByText(/Retry resumes the same plan/)).toBeVisible();
    // A 503 is never rendered as success.
    await expect(page.getByTestId("outcome-disposition")).toHaveCount(0);

    // Recover the route (last registered handler wins): the retry resumes
    // the SAME plan and must carry the SAME idempotency key.
    const retryKeys: (string | null)[] = [];
    await page.route("**/apply", (route) => {
      retryKeys.push(route.request().headers()["idempotency-key"] ?? null);
      return route.fulfill({
        status: 200,
        contentType: "application/json",
        body: JSON.stringify(repairApplyResponseE2E("completed")),
      });
    });
    await page.getByTestId("apply-button").click();
    await expect(page.getByTestId("outcome-disposition")).toHaveText("Completed");

    const applies = routes.applies();
    expect(applies.length).toBe(1);
    expect(retryKeys.length).toBe(1);
    expect(retryKeys[0]).toBe(applies[0].key);
    expect(applies[0].key).toMatch(/^app-/);
  });
});

async function waitForPlanRefresh(routes: {
  planFetches: () => number;
}): Promise<void> {
  await expect.poll(() => routes.planFetches() > 1).toBe(true);
}

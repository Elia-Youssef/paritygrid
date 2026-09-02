/**
 * P18.6 deterministic visual baselines for the reconciliation and repair
 * workbench. Every capture renders one fixed canonical state: static
 * synthetic data, fixed timestamps, a declared viewport, dark color
 * scheme, reduced motion, disabled animations, hidden caret, no live
 * network dependency, and stable ordering. Baselines are generated once
 * and clean reruns must not differ; thresholds are never loosened to hide
 * instability.
 */
import { expect, test, type Locator, type Page } from "@playwright/test";

import {
  FINGERPRINT_CONTENT,
  FINGERPRINT_RECON,
  PAGE_SIZE_E2E,
  RECONCILE_RUN_ID,
  REPAIR_PLAN_ID,
  REPAIR_COMPLETED_PAYLOAD,
  REPAIR_STARTED_PAYLOAD,
  emptyReconciliationE2E,
  installReconcileRoutes,
  installRepairRoutes,
  repairFrameE2E,
  repairPlanE2E,
} from "./fixtures-p18";

test.use({
  viewport: { width: 1280, height: 800 },
  colorScheme: "dark",
  contextOptions: { reducedMotion: "reduce" as const },
});

/**
 * Captures one content element as the baseline. Element-scoped captures are
 * used instead of full-page captures because the shell's sticky navigation
 * renders at scroll-dependent positions, which makes stitched full-page
 * captures nondeterministic; the sticky chrome is outside the content
 * elements and never enters their bounding boxes.
 */
async function screenshot(
  page: Page,
  locator: Locator,
  name: string,
  mask: Locator[] = [],
): Promise<void> {
  // The shell's sticky chrome paints at scroll-dependent positions inside
  // captures that exceed the viewport; pinning the scroll to the top keeps
  // that overlay at one deterministic place for every capture.
  await page.evaluate(() => window.scrollTo(0, 0));
  await expect(locator).toHaveScreenshot(name, {
    animations: "disabled",
    caret: "hide",
    mask,
    maskColor: "#071017",
  });
}

async function confirmApplication(page: Page): Promise<void> {
  await page.getByTestId("apply-open").click();
  await page.getByTestId("apply-confirmation").check();
  await page.getByTestId("apply-button").click();
}

test("1 — reconciliation summary with the conflict table", async ({ page }) => {
  await installReconcileRoutes(page);
  await page.goto(`/app/runs/${RECONCILE_RUN_ID}/reconcile`);
  await expect(page.getByTestId("reconciliation-summary")).toBeVisible();
  await expect(page.getByTestId("conflict-table")).toBeVisible();
  await expect(page.locator("tbody tr")).toHaveCount(PAGE_SIZE_E2E);
  await screenshot(
    page,
    page.getByTestId("reconciliation-summary"),
    "p18-reconciliation-summary.png",
  );
});

test("2 — large conflict table with the selected inspector", async ({ page }) => {
  await installReconcileRoutes(page);
  await page.goto(`/app/runs/${RECONCILE_RUN_ID}/reconcile`);
  await expect(page.getByTestId("conflict-table")).toBeVisible();

  await page.getByTestId("conflict-load-more").click();
  await expect(page.locator("tbody tr")).toHaveCount(2 * PAGE_SIZE_E2E);
  await page.getByTestId("conflict-filter").selectOption("field_mismatch");

  const firstRow = page.locator("tbody tr").first();
  await firstRow.focus();
  await page.keyboard.press("Enter");
  await expect(page.getByTestId("conflict-inspector")).toBeVisible();
  await expect(page.getByText("Value mismatch").first()).toBeVisible();
  await screenshot(
    page,
    page.getByTestId("reconciliation-summary"),
    "p18-conflict-table-inspector.png",
  );
});

test("3 — empty reconciliation display", async ({ page }) => {
  await installReconcileRoutes(page, {
    summary: emptyReconciliationE2E(),
  });
  await page.goto(`/app/runs/${RECONCILE_RUN_ID}/reconcile`);
  await expect(page.getByTestId("reconciliation-empty")).toBeVisible();
  await screenshot(
    page,
    page.getByTestId("reconciliation-summary"),
    "p18-reconciliation-empty.png",
  );
});

test("4 — proposed repair plan review", async ({ page }) => {
  await installRepairRoutes(page, {
    plan: repairPlanE2E(),
  });
  await page.goto(`/app/repairs/${REPAIR_PLAN_ID}`);
  await expect(page.getByTestId("repair-review")).toBeVisible();
  await expect(page.getByTestId("plan-status")).toHaveText("Proposed");
  await expect(page.getByTestId("before-sentinel-rac_e2e-001")).toHaveText(
    "none (create)",
  );
  await screenshot(
    page,
    page.getByTestId("repair-review"),
    "p18-repair-proposed-review.png",
  );
});

test("5 — stale repair plan blocked from approval and application", async ({
  page,
}) => {
  await installRepairRoutes(page, {
    plan: repairPlanE2E({ status: "approved", approval: {} }),
    fenceFingerprint: "ff".repeat(32),
  });
  await page.goto(`/app/repairs/${REPAIR_PLAN_ID}`);
  await expect(page.getByTestId("repair-review")).toBeVisible();
  await expect(page.getByTestId("apply-blocked")).toBeVisible();
  await screenshot(
    page,
    page.getByTestId("repair-review"),
    "p18-repair-stale-blocked.png",
  );
});

test("6 — repair application in progress with durable notifications", async ({
  page,
}) => {
  await installRepairRoutes(page, {
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
    applyStatus: 503,
  });
  await page.goto(`/app/repairs/${REPAIR_PLAN_ID}`);
  await expect(page.getByTestId("repair-review")).toBeVisible();
  await confirmApplication(page);
  await expect(page.getByTestId("progress-notifications")).toContainText(
    "repair_application_started",
  );
  await expect(page.getByText(/Stream connection: live/)).toBeVisible();
  // The 503-class recoverable outcome panel is visible with the retry note.
  await expect(page.getByText(/Recoverable failure/).first()).toBeVisible();
  await screenshot(
    page,
    page.getByTestId("repair-review"),
    "p18-repair-applying-progress.png",
    [page.getByText(/Stream connection:/)],
  );
});

test("7 — repair completed state", async ({ page }) => {
  await installRepairRoutes(page, {
    plan: repairPlanE2E({ status: "approved", approval: {} }),
    sseFrames: [
      [
        repairFrameE2E(1, "repair_application_started", REPAIR_STARTED_PAYLOAD),
        repairFrameE2E(2, "repair_application_completed", REPAIR_COMPLETED_PAYLOAD),
      ],
    ],
  });
  await page.goto(`/app/repairs/${REPAIR_PLAN_ID}`);
  await expect(page.getByTestId("repair-review")).toBeVisible();
  await confirmApplication(page);
  await expect(page.getByTestId("outcome-disposition")).toHaveText("Completed");
  await expect(page.getByTestId("apply-done-note")).toBeVisible();
  await expect(page.getByText(/Stream connection: live/)).toBeVisible();
  void FINGERPRINT_CONTENT;
  void FINGERPRINT_RECON;
  await screenshot(
    page,
    page.getByTestId("repair-review"),
    "p18-repair-completed.png",
    [page.getByText(/Stream connection:/)],
  );
});

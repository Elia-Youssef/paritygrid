/**
 * Phase 18 browser evidence for the reconciliation workbench: coherent
 * summary rendering, lazy cursor paging over a 10,000-conflict logical
 * dataset, deterministic resident filtering, keyboard selection and the
 * field inspector, snapshot quarantine, and the stale run state. Every
 * transport is faked at the browser boundary; nothing touches a network.
 */
import { expect, test } from "@playwright/test";

import {
  PAGE_SIZE_E2E,
  RECONCILE_RUN_ID,
  TOTAL_CONFLICTS_E2E,
  conflictsPageE2E,
  emptyReconciliationE2E,
  installReconcileRoutes,
} from "./fixtures-p18";

test.describe("P18.1 reconciliation summary", () => {
  test("1 — renders the coherent identity, all seven counts, and both observation times", async ({
    page,
  }) => {
    await installReconcileRoutes(page);
    await page.goto(`/app/runs/${RECONCILE_RUN_ID}/reconcile`);
    await expect(page.getByTestId("reconciliation-summary")).toBeVisible();

    await expect(page.getByText(RECONCILE_RUN_ID).first()).toBeVisible();
    await expect(page.getByTestId("count-match")).toHaveText("2000");
    await expect(page.getByTestId("count-missing_from_target")).toHaveText("2000");
    await expect(page.getByTestId("count-missing_from_source")).toHaveText("1500");
    await expect(page.getByTestId("count-field_mismatch")).toHaveText("3000");
    await expect(page.getByTestId("count-duplicate_source")).toHaveText("1250");
    await expect(page.getByTestId("count-duplicate_target")).toHaveText("1250");
    await expect(page.getByTestId("count-duplicate_both")).toHaveText("1000");
    await expect(page.getByText("2026-03-01 12:00:05")).toBeVisible();
    await expect(page.getByText("2026-03-01 12:30:00").first()).toBeVisible();
    // Truthful creation limitation is visible on a healthy snapshot.
    await expect(page.getByTestId("repair-creation-notice")).toBeVisible();
    await expect(page.getByTestId("reconciliation-stale-banner")).toHaveCount(0);
  });

  test("2 — covers the coherent empty display", async ({ page }) => {
    await installReconcileRoutes(page, {
      summary: emptyReconciliationE2E(),
    });
    await page.goto(`/app/runs/${RECONCILE_RUN_ID}/reconcile`);
    await expect(page.getByTestId("reconciliation-empty")).toBeVisible();
  });

  test("3 — shows the API failure state with bounded problem details", async ({
    page,
  }) => {
    await installReconcileRoutes(page);
    await page.route("**/reconciliation", (route) =>
      route.fulfill({
        status: 500,
        contentType: "application/problem+json",
        body: JSON.stringify({
          type: "about:blank",
          title: "Reconciliation unavailable",
          status: 500,
          detail: "the durable snapshot could not be read",
        }),
      }),
    );
    await page.goto(`/app/runs/${RECONCILE_RUN_ID}/reconcile`);
    await expect(page.getByRole("alert")).toBeVisible();
    await expect(page.getByText("Reconciliation unavailable").first()).toBeVisible();
    await expect(page.getByTestId("reconciliation-retry")).toBeVisible();
  });
});

test.describe("P18.2 large conflict table", () => {
  test("4 — pages the 10,000-conflict dataset lazily with bounded fetches", async ({
    page,
  }) => {
    const routes = await installReconcileRoutes(page);
    await page.goto(`/app/runs/${RECONCILE_RUN_ID}/reconcile`);
    await expect(page.getByTestId("conflict-table")).toBeVisible();

    await expect(page.getByText("of 10000 total")).toBeVisible();
    const rows = page.locator("tbody tr");
    await expect(rows).toHaveCount(PAGE_SIZE_E2E);
    expect(routes.conflictRequests().length).toBe(1);

    await page.getByTestId("conflict-load-more").click();
    await expect(rows).toHaveCount(2 * PAGE_SIZE_E2E);
    expect(routes.conflictRequests().length).toBe(2);
    expect(routes.conflictRequests()[1]).toContain("cursor=SKU-00099");

    // One click, one page: the client never races ahead of the operator.
    await page.getByTestId("conflict-load-more").click();
    await expect(rows).toHaveCount(3 * PAGE_SIZE_E2E);
    expect(routes.conflictRequests().length).toBe(3);
    // The full logical dataset stays server-side: 300 of 10000 resident.
    await expect(
      page.getByText("3 page(s) loaded; 300 conflicts resident"),
    ).toBeVisible();
  });

  test("5 — filters resident rows deterministically without fetching", async ({
    page,
  }) => {
    const routes = await installReconcileRoutes(page);
    await page.goto(`/app/runs/${RECONCILE_RUN_ID}/reconcile`);
    await expect(page.getByTestId("conflict-table")).toBeVisible();

    await page.getByTestId("conflict-filter").selectOption("field_mismatch");
    await expect(page.getByText(/filtered view of loaded data/)).toBeVisible();
    const rows = page.locator("tbody tr");
    // Field-mismatch rows occupy one slot in the six-class conflict cycle.
    await expect(rows).toHaveCount(17);
    await expect(rows.getByText("Field mismatch")).toHaveCount(17);
    expect(routes.conflictRequests().length).toBe(1);

    await page.getByTestId("conflict-filter").selectOption("");
    await expect(rows).toHaveCount(PAGE_SIZE_E2E);
  });

  test("6 — quarantines a conflict page persisted under another snapshot", async ({
    page,
  }) => {
    const routes = await installReconcileRoutes(page, {
      overridePage: {
        pageIndex: 1,
        body: conflictsPageE2E(1, {
          run_id: RECONCILE_RUN_ID,
          run_version: 9,
          reconciliation_fingerprint: "ff".repeat(32),
        }),
      },
    });
    await page.goto(`/app/runs/${RECONCILE_RUN_ID}/reconcile`);
    await expect(page.getByTestId("conflict-table")).toBeVisible();

    await page.getByTestId("conflict-load-more").click();
    await expect(page.getByTestId("conflict-pages-rejected")).toBeVisible();
    // The mismatched page is counted as rejected and never merged: the
    // resident snapshot keeps exactly its original 100 rows.
    await expect(page.locator("tbody tr")).toHaveCount(PAGE_SIZE_E2E);
    await expect(
      page.getByText("1 page(s) loaded; 100 conflicts resident"),
    ).toBeVisible();
    expect(routes.conflictRequests().length).toBe(2);

    await page.getByTestId("conflict-recovery").click();
    await expect(page.getByTestId("conflict-pages-rejected")).toHaveCount(0);
    await expect(page.locator("tbody tr")).toHaveCount(PAGE_SIZE_E2E);
    expect(routes.conflictRequests().length).toBe(3);
  });

  test("7 — fences the conflict data and repair actions when the run advanced", async ({
    page,
  }) => {
    await installReconcileRoutes(page, { runVersion: 7 });
    await page.goto(`/app/runs/${RECONCILE_RUN_ID}/reconcile`);
    await expect(page.getByTestId("reconciliation-summary")).toBeVisible();

    await expect(
      page.getByText(/Run state advanced past the rendered snapshot/),
    ).toBeVisible();
    await expect(page.getByTestId("reconciliation-refresh-state")).toBeVisible();
    // Repair entry points stay disabled while identity coherence is broken.
    await expect(page.getByTestId("repair-creation-notice")).toHaveCount(0);
  });
});

test.describe("P18.3 keyboard access and inspector", () => {
  test("8 — selects rows with the keyboard and shows field-level evidence", async ({
    page,
  }) => {
    await installReconcileRoutes(page);
    await page.goto(`/app/runs/${RECONCILE_RUN_ID}/reconcile`);
    await expect(page.getByTestId("conflict-table")).toBeVisible();
    await page.getByTestId("conflict-filter").selectOption("field_mismatch");

    const firstRow = page.locator("tbody tr").first();
    await firstRow.focus();
    await expect(firstRow).toBeFocused();

    await page.keyboard.press("ArrowDown");
    const secondRow = page.locator("tbody tr").nth(1);
    await expect(secondRow).toBeFocused();

    await page.keyboard.press("Enter");
    await expect(page.getByTestId("conflict-selected-announce")).toContainText(
      "selected",
    );
    await expect(page.getByTestId("conflict-inspector")).toBeVisible();
    // The kind badge is the authoritative distinction, rendered as text.
    await expect(page.getByText("Value mismatch").first()).toBeVisible();
    // Source and target values render as inert bounded text.
    await expect(page.getByText(/1999\.00-/).first()).toBeVisible();
  });

  test("9 — distinguishes missing sides through the authoritative kind", async ({
    page,
  }) => {
    await installReconcileRoutes(page);
    await page.goto(`/app/runs/${RECONCILE_RUN_ID}/reconcile`);
    await expect(page.getByTestId("conflict-table")).toBeVisible();
    await page.getByTestId("conflict-filter").selectOption("missing_from_target");

    await page.locator("tbody tr").first().focus();
    await page.keyboard.press("Enter");
    await expect(page.getByTestId("conflict-inspector")).toBeVisible();
    const inspector = page.getByTestId("conflict-inspector");
    await expect(inspector.getByText("Missing from target")).toBeVisible();
    await expect(page.getByText(/create the missing target record/)).toBeVisible();
    // The target side has no references at all: the wholesale absence is
    // visible rather than implied.
    await expect(page.getByTestId("conflict-inspector").getByText("none")).toHaveCount(
      1,
    );
  });

  test("10 — reports the logical total through table semantics", async ({ page }) => {
    await installReconcileRoutes(page);
    await page.goto(`/app/runs/${RECONCILE_RUN_ID}/reconcile`);
    await expect(page.getByTestId("conflict-table")).toBeVisible();
    await expect(
      page.getByText(
        `showing 100 of 100 loaded of ${String(TOTAL_CONFLICTS_E2E)} total`,
      ),
    ).toBeVisible();
    await expect(page.locator("table")).toHaveAttribute(
      "aria-rowcount",
      String(TOTAL_CONFLICTS_E2E),
    );
  });
});

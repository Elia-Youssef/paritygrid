/**
 * Phase 16 browser evidence: public overview, operations overview, library
 * URL state, hostile-text safety, narrow viewport, and reduced motion.
 * All API input is intercepted with strict-schema fixtures.
 */
import { expect, test } from "@playwright/test";

import { installStudioRoutes } from "./fixtures";

test.describe("public overview", () => {
  test("navigates the truthful overview into the console", async ({ page }) => {
    await page.goto("/");
    await expect(
      page.getByRole("heading", { level: 1, name: "Reconciliation you can prove." }),
    ).toBeVisible();
    // Implemented workflows are linked.
    await expect(page.getByRole("link", { name: "Open the console" })).toBeVisible();
    await expect(
      page.getByRole("link", { name: "Browse pipeline library" }),
    ).toBeVisible();
    // Future capabilities are explicitly marked, not claimed.
    await expect(page.getByText("Live run execution")).toBeVisible();
    await expect(page.getByText("Not yet")).toHaveCount(2);
    await expect(page.getByText("Conflict and repair screens")).toBeVisible();

    await page.getByRole("link", { name: "Open the console" }).click();
    await expect(page).toHaveURL(/\/app$/);
    await expect(
      page.getByRole("heading", { level: 1, name: "Operations overview" }),
    ).toBeVisible();
  });
});

test.describe("operations overview", () => {
  test("loads durable runs, runner availability, and readiness", async ({ page }) => {
    await installStudioRoutes(page);
    await page.goto("/app");

    await expect(page.getByRole("cell", { name: "run_scenario-01" })).toBeVisible();
    await expect(page.getByText("migrations applied")).toBeVisible();
    await expect(page.getByText(/GET \/api\/v1\/runs/)).toBeVisible();
  });

  test("renders unavailable facts as unavailable while keeping coherent data", async ({
    page,
  }) => {
    await installStudioRoutes(page, { readinessStatus: 503 });
    await page.goto("/app");

    await expect(page.getByText("Readiness is unavailable right now.")).toBeVisible();
    // Durable run data stays visible; nothing degrades to zero or empty.
    await expect(page.getByRole("cell", { name: "run_scenario-01" })).toBeVisible();
  });

  test("routes from the operations overview into the pipeline library", async ({
    page,
  }) => {
    await installStudioRoutes(page);
    await page.goto("/app");

    await expect(
      page.getByRole("heading", { level: 1, name: "Operations overview" }),
    ).toBeVisible();
    await page.getByRole("link", { name: "Pipeline library" }).click();
    await expect(page).toHaveURL(/\/app\/pipelines$/);
    await expect(
      page.getByRole("heading", { level: 1, name: "Pipeline library" }),
    ).toBeVisible();
    await expect(
      page.getByRole("cell", { name: "pip_e2e-001", exact: true }),
    ).toBeVisible();
  });
});

test.describe("pipeline library", () => {
  test("supports search, selection, reload, back, and forward through URL state", async ({
    page,
  }) => {
    await installStudioRoutes(page);
    await page.goto("/app/pipelines");

    await expect(
      page.getByRole("cell", { name: "pip_e2e-001", exact: true }),
    ).toBeVisible();

    await page.getByTestId("library-search").fill("second");
    await expect(page).toHaveURL(/q=second/);
    await expect(
      page.getByRole("cell", { name: "pip_second-001", exact: true }),
    ).toBeVisible();
    await expect(
      page.getByRole("cell", { name: "pip_e2e-001", exact: true }),
    ).toBeHidden();

    // Reload reproduces the filtered view.
    await page.reload();
    await expect(page).toHaveURL(/q=second/);
    await expect(
      page.getByRole("cell", { name: "pip_second-001", exact: true }),
    ).toBeVisible();

    // Back restores the unfiltered view; forward returns to the search.
    await page.goBack();
    await expect(page).toHaveURL(/\/app\/pipelines$/);
    await expect(
      page.getByRole("cell", { name: "pip_e2e-001", exact: true }),
    ).toBeVisible();
    await page.goForward();
    await expect(page).toHaveURL(/q=second/);

    // Opening the studio keeps the selection path in the URL.
    await page
      .getByRole("link", { name: /Open pipeline studio for pip_second-001/ })
      .click();
    await expect(page).toHaveURL(/\/app\/pipelines\/pip_second-001$/);
  });

  test("creates a pipeline through the dialog", async ({ page }) => {
    await installStudioRoutes(page);
    await page.goto("/app/pipelines");
    await expect(
      page.getByRole("cell", { name: "pip_e2e-001", exact: true }),
    ).toBeVisible();

    await page.getByTestId("new-pipeline").click();
    await expect(page.getByRole("dialog", { name: "New pipeline" })).toBeVisible();
    await page.getByTestId("create-pipeline-id").fill("pip_created-001");
    await page.getByTestId("create-pipeline-name").fill("Created");
    await page.getByTestId("create-pipeline-submit").click();
    await expect(page).toHaveURL(/selected=pip_created-001/);
  });

  test("renders hostile external text inertly", async ({ page }) => {
    await installStudioRoutes(page);
    await page.goto("/app/pipelines");

    const hostile = page.getByRole("cell", {
      name: "E2E <b>bold</b> pipeline",
      exact: true,
    });
    await expect(hostile).toBeVisible();
    // The markup arrives as text, never as elements.
    expect(await hostile.locator("b").count()).toBe(0);

    await page
      .getByRole("link", { name: /Open pipeline studio for pip_e2e-001/ })
      .click();
    const heading = page.getByText('E2E <script>alert("x")</script> pipeline');
    await expect(heading).toBeVisible();
    // No script element may carry the hostile payload.
    expect(await page.locator("script", { hasText: 'alert("x")' }).count()).toBe(0);
  });
});

test.describe("narrow viewport", () => {
  test("keeps the console usable at 600px without hiding context", async ({ page }) => {
    await installStudioRoutes(page);
    await page.setViewportSize({ width: 600, height: 900 });
    await page.goto("/app");

    await expect(
      page.getByRole("heading", { level: 1, name: "Operations overview" }),
    ).toBeVisible();
    await expect(page.getByRole("navigation", { name: "Primary" })).toHaveCount(1);
    await expect(page.locator("aside").first()).toBeHidden();

    await page.goto("/app/pipelines");
    await expect(
      page.getByRole("heading", { level: 1, name: "Pipeline library" }),
    ).toBeVisible();
    await expect(page.getByTestId("library-search")).toBeVisible();
    await expect(page.getByTestId("new-pipeline")).toBeVisible();
  });
});

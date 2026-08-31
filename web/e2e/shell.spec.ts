import { expect, test } from "@playwright/test";

test.beforeEach(async ({ page }) => {
  // Shell checks run against the built SPA without a backend. Keep the
  // connection indicator deterministic without exercising Phase 15 clients.
  await page.route("**/healthz", (route) =>
    route.fulfill({
      status: 200,
      contentType: "application/json",
      body: '{"status":"ok"}',
    }),
  );
});

test("serves a direct nested console address from the production SPA", async ({
  page,
}) => {
  const response = await page.goto("/app/runs/run-42/reconcile");

  expect(response?.status()).toBe(200);
  await expect(
    page.getByRole("heading", { level: 1, name: "Reconciliation workbench" }),
  ).toBeVisible();
  await expect(page).toHaveTitle("Reconcile · ParityGrid console");
});

test("keeps one accessible navigation, supports the skip link, and focuses a keyboard route", async ({
  page,
}) => {
  await page.setViewportSize({ width: 1280, height: 900 });
  await page.goto("/app");

  await expect(page.getByRole("banner")).toBeVisible();
  await expect(page.getByRole("main")).toBeVisible();
  await expect(page.getByRole("navigation", { name: "Primary" })).toHaveCount(1);

  await page.keyboard.press("Tab");
  const skipLink = page.getByRole("link", { name: "Skip to content" });
  await expect(skipLink).toBeFocused();
  await page.keyboard.press("Enter");
  await expect(page).toHaveURL(/#main-content$/);
  const mainContent = page.locator("#main-content");
  await expect(mainContent).toBeVisible();
  await expect(mainContent).toBeFocused();

  const runs = page.getByRole("link", { name: "Runs", exact: true });
  await runs.focus();
  await page.keyboard.press("Enter");
  await expect(page).toHaveURL(/\/app\/runs$/);
  await expect(
    page.getByRole("heading", { level: 1, name: "Run history" }),
  ).toBeFocused();
});

test("exposes only the compact primary navigation at a narrow viewport", async ({
  page,
}) => {
  await page.setViewportSize({ width: 600, height: 900 });
  await page.goto("/app");

  await expect(page.locator("aside")).toBeHidden();
  await expect(page.getByRole("navigation", { name: "Primary" })).toHaveCount(1);
  await expect(page.getByRole("link", { name: "Overview", exact: true })).toBeVisible();
});

test.describe("reduced motion", () => {
  test.use({ contextOptions: { reducedMotion: "reduce" } });

  test("applies the shell's reduced-motion override", async ({ page }) => {
    await page.goto("/app");

    await expect
      .poll(() =>
        page.evaluate(
          () => window.matchMedia("(prefers-reduced-motion: reduce)").matches,
        ),
      )
      .toBe(true);
    const transitionDurationMs = await page
      .getByRole("link", { name: "Overview", exact: true })
      .evaluate((element) => {
        const duration = getComputedStyle(element).transitionDuration;
        return duration.endsWith("ms")
          ? Number.parseFloat(duration)
          : Number.parseFloat(duration) * 1_000;
      });
    expect(transitionDurationMs).toBeLessThanOrEqual(1);
  });
});

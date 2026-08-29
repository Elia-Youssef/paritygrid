import { expect, test } from "@playwright/test";

const HEALTH_OK = JSON.stringify({
  status: "ok",
  service: "ParityGrid",
  version: "0.1.0",
});

test.beforeEach(async ({ page }) => {
  // Shell checks run against the built SPA without a backend. The health
  // probe is served by the Phase 15 query layer; keep it deterministic.
  await page.route("**/healthz", (route) =>
    route.fulfill({
      status: 200,
      contentType: "application/json",
      body: HEALTH_OK,
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
  await expect(page.locator("#main-content")).toBeVisible();

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

test("reflects the health query as an online badge and keeps the command palette accessible", async ({
  page,
}) => {
  await page.goto("/app");

  await expect(page.getByText("API online")).toBeVisible();

  const commands = page.getByRole("button", { name: "Commands" });
  await expect(commands).toBeVisible();
  await commands.focus();
  await page.keyboard.press("Enter");
  await expect(page.getByRole("dialog", { name: "Command palette" })).toBeVisible();
  await page.keyboard.press("Escape");
  await expect(commands).toBeFocused();
});

test("surfaces a failed health query and recovers through the retry control", async ({
  page,
}) => {
  let healthy = false;
  await page.route("**/healthz", (route) =>
    healthy
      ? route.fulfill({
          status: 200,
          contentType: "application/json",
          body: HEALTH_OK,
        })
      : route.fulfill({ status: 503, contentType: "text/plain", body: "restarting" }),
  );

  await page.goto("/app");
  await expect(page.getByText("API unavailable")).toBeVisible();

  await page.getByRole("button", { name: "Retry" }).click();
  // The probe keeps failing until the backend actually recovers.
  await expect(page.getByText("API unavailable")).toBeVisible();

  healthy = true;
  await page.getByRole("button", { name: "Retry" }).click();
  await expect(page.getByText("API online")).toBeVisible();
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

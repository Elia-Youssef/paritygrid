import { expect, test } from "@playwright/test";

const HEALTH_OK = JSON.stringify({
  status: "ok",
  service: "ParityGrid",
  version: "0.1.0",
});

test.beforeEach(async ({ page }) => {
  // Shell checks run against the built SPA without a backend. The health
  // probe and the durable operations endpoints are served deterministically.
  await page.route("**/healthz", (route) =>
    route.fulfill({
      status: 200,
      contentType: "application/json",
      body: HEALTH_OK,
    }),
  );
  await page.route("**/readyz", (route) =>
    route.fulfill({
      status: 200,
      contentType: "application/json",
      body: JSON.stringify({
        status: "ready",
        service: "ParityGrid",
        version: "0.1.0",
        detail: "migrations applied",
      }),
    }),
  );
  await page.route("**/api/v1/runs*", (route) =>
    route.fulfill({
      status: 200,
      contentType: "application/json",
      body: JSON.stringify({
        schema_version: 1,
        items: [],
        limit: 10,
        next_cursor: null,
      }),
    }),
  );
  await page.route("**/api/v1/system/capabilities", (route) =>
    route.fulfill({
      status: 200,
      contentType: "application/json",
      body: JSON.stringify({
        schema_version: 1,
        service: "ParityGrid",
        version: "0.1.0",
        runners: [{ schema_version: 1, strategy_id: "sequential", available: true }],
        subordinate_pools: [],
        features: [],
        sqlite: {
          schema_version: 1,
          library_version: "3.45",
          minimum_supported_version: "3.35",
          journal_mode: "wal",
          synchronous_level: 1,
          threadsafety: 1,
          supports_json_sql: true,
          supports_returning: true,
        },
        limits: {
          schema_version: 1,
          artifact_chunk_bytes: 1,
          idempotency_lease_seconds: 1,
          max_concurrent_requests: 1,
          max_json_depth: 1,
          max_page_size: 1,
          max_request_body_bytes: 1,
          request_timeout_seconds: 1,
        },
      }),
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

test("static fallback never exposes files outside the production distribution", async ({
  request,
}) => {
  for (const candidate of [
    "/..%2Fe2e%2Fstatic-server.mjs",
    "/%2e%2e%5ce2e%5cstatic-server.mjs",
  ]) {
    const response = await request.get(candidate);
    expect(response.status()).toBe(200);
    expect(response.headers()["content-type"]).toContain("text/html");
    expect(await response.text()).not.toContain("createServer");
  }
});

test("keeps one accessible navigation, supports the skip link, and focuses a keyboard route", async ({
  page,
  browserName,
}) => {
  await page.setViewportSize({ width: 1280, height: 900 });
  await page.goto("/app");

  await expect(page.getByRole("banner")).toBeVisible();
  await expect(page.getByRole("main")).toBeVisible();
  await expect(page.getByRole("navigation", { name: "Primary" })).toHaveCount(1);

  await page.keyboard.press("Tab");
  const skipLink = page.getByRole("link", { name: "Skip to content" });
  if (browserName === "webkit") {
    // WebKit follows the Safari keyboard model and does not stop Tab on
    // links unless the user enables full keyboard access, so the first-Tab
    // assertion holds only on Chromium and Firefox.  Focusing the skip
    // link directly keeps the activation and focus path identical.
    await skipLink.focus();
  }
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

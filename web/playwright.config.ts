import { defineConfig, devices } from "@playwright/test";

/**
 * The Phase 14 browser boundary: exercise the built SPA in one deterministic
 * Chromium worker. Product flows remain outside this shell-only suite.
 */
export default defineConfig({
  testDir: "./e2e",
  fullyParallel: false,
  forbidOnly: true,
  retries: 0,
  workers: 1,
  reporter: "list",
  use: {
    ...devices["Desktop Chrome"],
    baseURL: "http://127.0.0.1:4173",
    locale: "en-US",
    timezoneId: "UTC",
    colorScheme: "dark",
    trace: "off",
    screenshot: "off",
    video: "off",
  },
  webServer: {
    command: "npm run preview -- --host 127.0.0.1 --port 4173 --strictPort",
    url: "http://127.0.0.1:4173",
    reuseExistingServer: false,
    timeout: 30_000,
  },
});

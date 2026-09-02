import { defineConfig } from "@playwright/test";

import baseConfig from "./playwright.config";

/** Frontend-only CI excludes the real demo, which requires the Python wheel. */
export default defineConfig(baseConfig, {
  testIgnore: /demo\.spec\.ts/,
});

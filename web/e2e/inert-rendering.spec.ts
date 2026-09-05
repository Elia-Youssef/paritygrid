/**
 * Phase 22 inert-rendering proof: untrusted strings that reach the UI —
 * difference values and Problem Details text — render as text only, and
 * a hostile key that violates the strict runtime schema fails closed.
 * The browser never executes, loads, or creates anything from string
 * content, and a static sink guard proves the frontend source contains
 * no HTML or code execution sinks at all.
 */
import { readdirSync, readFileSync, statSync } from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

import { expect, test } from "@playwright/test";

import {
  RECONCILE_RUN_ID,
  conflictsPageE2E,
  conflictE2E,
  installReconcileRoutes,
} from "./fixtures-p18";

const HOSTILE_TEXT = `<img src=x onerror="window.__pwned=1">`;
const HOSTILE_SCRIPT = `<script>window.__xssRan=1</script>`;
const HOSTILE_PROBLEM = `<b>break</b><img src=x onerror="window.__problemRan=1">`;

/**
 * Page 0 with hostile free-text difference values.  The key stays within
 * the strict runtime schema; the hostile strings live in the bounded
 * text fields the inspector renders.
 */
function hostileTextPage(): Record<string, unknown> {
  const page = conflictsPageE2E(0);
  const hostile = {
    ...conflictE2E(0),
    classification: "field_mismatch",
    suggested_resolution: "update_target",
    differences: [
      {
        field: "notes",
        kind: "value_mismatch",
        source_text: HOSTILE_TEXT,
        target_text: HOSTILE_SCRIPT,
      },
    ],
  };
  return { ...(page as Record<string, object>), items: [hostile] };
}

test("untrusted difference text renders inertly as text", async ({ page }) => {
  await installReconcileRoutes(page, {
    overridePage: { pageIndex: 0, body: hostileTextPage() },
  });
  await page.goto(`/app/runs/${RECONCILE_RUN_ID}/reconcile`);
  await expect(page.getByTestId("conflict-table")).toBeVisible();

  await page.locator("tbody tr").first().focus();
  await page.keyboard.press("Enter");
  await expect(page.getByTestId("conflict-inspector")).toBeVisible();
  await expect(page.getByText(HOSTILE_TEXT).first()).toBeVisible();

  expect(
    await page.evaluate(() => (window as unknown as Record<string, unknown>).__pwned),
  ).toBeUndefined();
  expect(
    await page.evaluate(() => (window as unknown as Record<string, unknown>).__xssRan),
  ).toBeUndefined();
  expect(await page.locator("img[src='x']").count()).toBe(0);
  expect(await page.locator("iframe, object, embed").count()).toBe(0);
});

test("a hostile key that violates the strict runtime schema fails closed", async ({
  page,
}) => {
  const pageBody = conflictsPageE2E(0);
  const hostile = {
    ...conflictE2E(0),
    canonical_key: HOSTILE_TEXT,
  };
  const override = { ...(pageBody as Record<string, object>), items: [hostile] };
  await installReconcileRoutes(page, {
    overridePage: { pageIndex: 0, body: override },
  });
  await page.goto(`/app/runs/${RECONCILE_RUN_ID}/reconcile`);
  // The validated boundary refuses the page; nothing hostile renders or runs.
  await expect(page.getByRole("alert").first()).toBeVisible();
  expect(
    await page.evaluate(() => (window as unknown as Record<string, unknown>).__pwned),
  ).toBeUndefined();
  expect(await page.locator("img[src='x']").count()).toBe(0);
});

test("hostile Problem Details text renders inertly in the failure state", async ({
  page,
}) => {
  await installReconcileRoutes(page);
  await page.route("**/reconciliation", (route) =>
    route.fulfill({
      status: 500,
      contentType: "application/problem+json",
      body: JSON.stringify({
        type: "about:blank",
        title: HOSTILE_PROBLEM,
        status: 500,
        detail: HOSTILE_PROBLEM,
      }),
    }),
  );
  await page.goto(`/app/runs/${RECONCILE_RUN_ID}/reconcile`);
  await expect(page.getByRole("alert")).toBeVisible();
  await expect(page.getByText(HOSTILE_PROBLEM).first()).toBeVisible();
  expect(
    await page.evaluate(
      () => (window as unknown as Record<string, unknown>).__problemRan,
    ),
  ).toBeUndefined();
});

test("the frontend source contains no HTML or code-execution sinks", () => {
  const moduleDir =
    typeof __dirname === "string"
      ? __dirname
      : path.dirname(fileURLToPath(import.meta.url));
  const sourceRoot = path.resolve(moduleDir, "..", "src");
  const sources: string[] = [];
  const visit = (directory: string): void => {
    for (const entry of readdirSync(directory)) {
      const full = path.join(directory, entry);
      if (statSync(full).isDirectory()) {
        visit(full);
      } else if (/\.(ts|tsx|css|html)$/.test(entry)) {
        sources.push(readFileSync(full, "utf-8"));
      }
    }
  };
  visit(sourceRoot);
  expect(sources.length).toBeGreaterThan(10);

  const forbidden = [
    "dangerouslySetInnerHTML",
    ".innerHTML",
    "insertAdjacentHTML",
    "document.write",
    "eval(",
    "new Function(",
  ];
  for (const source of sources) {
    for (const sink of forbidden) {
      expect(source, `frontend source must not contain ${sink}`).not.toContain(sink);
    }
  }
});

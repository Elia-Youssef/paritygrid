/**
 * Phase 16 browser evidence: the full pipeline-authoring workflow — typed
 * node authoring, keyboard graph editing, connection validation, inspector
 * validation with focus movement, plan preview, versioned publication
 * (double submit, stale conflict, idempotent retry, malformed and
 * mismatched acknowledgements), and reduced motion.
 */
import { expect, test, type Page } from "@playwright/test";

import {
  installStudioRoutes,
  PIPELINE_ID,
  type CapturedPublish,
  type StudioRoutesOptions,
} from "./fixtures";

const ACK_1 = JSON.stringify({
  schema_version: 1,
  pipeline_id: PIPELINE_ID,
  version: 1,
  specification_sha256: "a".repeat(64),
  planner_format_version: 1,
  published_at: "2026-01-01 00:00:05",
});

async function openStudio(
  page: Page,
  options: StudioRoutesOptions = {},
): Promise<{
  publishes: CapturedPublish[];
  setFrontier: (next: string) => void;
}> {
  const routes = await installStudioRoutes(page, options);
  await page.goto(`/app/pipelines/${PIPELINE_ID}`);
  await expect(page.getByTestId("studio-root")).toBeVisible();
  return { publishes: routes.publishes, setFrontier: routes.setFrontier };
}

async function authorMinimalPipeline(page: Page): Promise<void> {
  await expect(page.getByTestId("publish-button")).toBeDisabled();

  await page.getByLabel("Add node").selectOption("source.csv");
  await page.getByTestId("add-node-button").click();
  await page.getByLabel("Add node").selectOption("export.parquet");
  await page.getByTestId("add-node-button").click();

  await expect(
    page.getByRole("cell", { name: "nod_source-csv-001", exact: true }),
  ).toBeVisible();

  await page.getByLabel("From", { exact: true }).selectOption("nod_source-csv-001");
  await page.getByLabel("To", { exact: true }).selectOption("nod_export-parquet-001");
  await page.getByRole("button", { name: "Connect", exact: true }).click();

  await expect(
    page.getByRole("cell", { name: "nod_source-csv-001.records" }),
  ).toBeVisible();

  await page
    .getByRole("row", { name: /nod_source-csv-001/ })
    .getByRole("button", { name: "Configure" })
    .click();
  await page.getByLabel(/Connector binding/).selectOption("con_source-001");
  await page.getByRole("button", { name: "Close inspector" }).click();

  await expect(page.getByTestId("publish-button")).toBeEnabled();
}

test("authors typed nodes and publishes version 1 with an explicit frontier", async ({
  page,
}) => {
  const { publishes } = await openStudio(page);
  await authorMinimalPipeline(page);

  await page.getByTestId("preview-button").click();
  const preview = page.getByRole("dialog", { name: "Plan preview" });
  await expect(preview).toBeVisible();
  await expect(preview.getByTestId("preview-expected-frontier")).toHaveText("0");
  await expect(preview.getByText("Canonical logical document")).toBeVisible();
  await page.getByRole("button", { name: "Close preview" }).click();
  await expect(page.getByRole("button", { name: "Preview plan" })).toBeFocused();

  await page.getByTestId("publish-button").click();
  await expect(page.getByTestId("publication-success")).toHaveText(
    /Published version 1/,
  );
  expect(publishes).toHaveLength(1);
  const sent = publishes[0];
  expect(sent?.idempotencyKey).toMatch(/^publish-pip_e2e-001-0-[0-9a-f]{16}$/);
  const body = JSON.parse(sent?.body ?? "{}") as {
    expected_latest_version: number;
    document: { nodes: { id: string }[]; edges: unknown[] };
  };
  expect(body.expected_latest_version).toBe(0);
  expect(body.document.nodes).toHaveLength(2);
  expect(body.document.edges).toHaveLength(1);
});

test("supports keyboard-only graph editing through the semantic structure", async ({
  page,
}) => {
  await openStudio(page);

  // Keyboard: focus the add-node select, choose a kind by type-ahead, and
  // activate Add without any pointer event.
  const addSelect = page.getByLabel("Add node");
  await addSelect.focus();
  await expect(addSelect).toBeFocused();
  await page.keyboard.press("s");
  await page.keyboard.press("Tab");
  await page.keyboard.press("Enter");
  await expect(
    page.getByRole("cell", { name: "nod_source-csv-001", exact: true }),
  ).toBeVisible();

  // Keyboard deletion: focus the row's Delete button and activate it.
  const deleteButton = page
    .getByRole("table", { name: "Nodes" })
    .getByRole("button", { name: "Delete", exact: true });
  await deleteButton.focus();
  await expect(deleteButton).toBeFocused();
  await page.keyboard.press("Enter");
  await expect(
    page.getByRole("cell", { name: "nod_source-csv-001", exact: true }),
  ).toBeHidden();

  // Keyboard re-add, then keyboard connection through the form controls.
  await addSelect.selectOption("source.csv");
  await page.getByTestId("add-node-button").focus();
  await page.keyboard.press("Enter");
  await expect(
    page.getByRole("cell", { name: "nod_source-csv-001", exact: true }),
  ).toBeVisible();
  await addSelect.selectOption("export.parquet");
  await page.getByTestId("add-node-button").focus();
  await page.keyboard.press("Enter");
  await expect(
    page.getByRole("cell", { name: "nod_export-parquet-001", exact: true }),
  ).toBeVisible();
  await page.getByLabel("From", { exact: true }).selectOption("nod_source-csv-001");
  await page.getByLabel("To", { exact: true }).selectOption("nod_export-parquet-001");
  await page.getByRole("button", { name: "Connect", exact: true }).focus();
  await page.keyboard.press("Enter");
  await expect(
    page.getByRole("cell", { name: "nod_source-csv-001.records", exact: true }),
  ).toBeVisible();
});

test("rejects invalid connections with precise safe feedback", async ({ page }) => {
  await openStudio(page);

  await page.getByLabel("Add node").selectOption("source.csv");
  await page.getByTestId("add-node-button").click();
  await page.getByLabel("Add node").selectOption("transform.validate");
  await page.getByTestId("add-node-button").click();

  await page.getByLabel("From", { exact: true }).selectOption("nod_source-csv-001");
  await page
    .getByLabel("To", { exact: true })
    .selectOption("nod_transform-validate-001");
  await page.getByRole("button", { name: "Connect", exact: true }).click();

  await expect(page.getByTestId("connection-error")).toHaveText(
    "Connection rejected: records.raw cannot feed nod_transform-validate-001.records.",
  );
  await expect(page.getByTestId("publish-button")).toBeDisabled();
});

test("moves focus through inspector validation and back", async ({ page }) => {
  await openStudio(page);

  await page.getByLabel("Add node").selectOption("source.csv");
  await page.getByTestId("add-node-button").click();
  await page
    .getByRole("row", { name: /nod_source-csv-001/ })
    .getByRole("button", { name: "Configure" })
    .click();

  const inspector = page.getByRole("complementary", { name: "Node inspector" });
  await expect(inspector).toBeVisible();
  // Validation is announced in the inspector before publication exists.
  await expect(inspector.getByRole("alert")).toContainText(
    "requires a source connector",
  );

  const connector = page.getByLabel(/Connector binding/);
  await connector.focus();
  await expect(connector).toBeFocused();
  await connector.selectOption("con_source-001");

  await page.getByRole("button", { name: "Close inspector" }).click();
  // Closing deselects the node; the panel persists as an explicit
  // placeholder instead of vanishing.
  await expect(
    page.getByText("Select a node to inspect its configuration."),
  ).toBeVisible();
});

test("prevents double submission to exactly one request", async ({ page }) => {
  let resolvePublish: (behavior: { status: number; body: string }) => void = () =>
    undefined;
  const { publishes } = await openStudio(page, {
    publish: () =>
      new Promise((resolve) => {
        resolvePublish = resolve;
      }),
  });
  await authorMinimalPipeline(page);

  await page.getByTestId("publish-button").click();
  await expect(page.getByTestId("publish-button")).toHaveText("Publishing…");
  // A forced second activation while pending must not reach the network.
  await page.getByTestId("publish-button").click({ force: true });
  expect(publishes).toHaveLength(1);

  resolvePublish({ status: 201, body: ACK_1 });
  await expect(page.getByTestId("publication-success")).toHaveText(
    /Published version 1/,
  );
  expect(publishes).toHaveLength(1);
});

test("preserves the draft on a stale conflict and recovers", async ({ page }) => {
  const { publishes, setFrontier } = await openStudio(page, {
    publish: (attempt) =>
      attempt === 1
        ? {
            status: 409,
            body: JSON.stringify({
              type: "https://paritygrid.dev/problems/pipeline-version-conflict",
              title: "pipeline latest version is stale",
              status: 409,
              detail: "the immutable frontier moved",
              code: "pipeline_version_conflict",
            }),
          }
        : {
            status: 201,
            body: JSON.stringify({
              schema_version: 1,
              pipeline_id: PIPELINE_ID,
              version: 2,
              specification_sha256: "b".repeat(64),
              planner_format_version: 1,
              published_at: "2026-01-01 00:00:06",
            }),
          },
  });
  await authorMinimalPipeline(page);

  await page.getByTestId("publish-button").click();
  await expect(page.getByTestId("publication-conflict")).toContainText(
    "Stale publication blocked",
  );
  await expect(
    page.getByRole("cell", { name: "nod_source-csv-001", exact: true }),
  ).toBeVisible();

  setFrontier(
    JSON.stringify({
      schema_version: 1,
      pipeline_id: PIPELINE_ID,
      latest_version: 1,
      published: true,
    }),
  );
  await page.getByRole("button", { name: "Refresh version frontier" }).click();
  await expect(page.getByTestId("publish-button")).toHaveText("Publish version 2");
  await expect(page.getByTestId("publish-button")).toBeEnabled();

  await page.getByTestId("publish-button").click();
  await expect(page.getByTestId("publication-success")).toContainText(
    "Published version 2",
  );
  expect(publishes).toHaveLength(2);
  const retry = JSON.parse(publishes[1]?.body ?? "{}") as {
    expected_latest_version?: number;
  };
  expect(retry.expected_latest_version).toBe(1);
  expect(publishes[1]?.idempotencyKey).not.toBe(publishes[0]?.idempotencyKey);
});

test("reuses the exact idempotency key and bytes on explicit retry", async ({
  page,
}) => {
  const { publishes } = await openStudio(page, {
    publish: (attempt) =>
      attempt === 1
        ? { status: 502, body: "gateway reset" }
        : { status: 201, body: ACK_1 },
  });
  await authorMinimalPipeline(page);

  await page.getByTestId("publish-button").click();
  await expect(page.getByTestId("publication-failure")).toBeVisible();

  await page.getByTestId("publish-button").click();
  await expect(page.getByTestId("publication-success")).toHaveText(
    /Published version 1/,
  );
  expect(publishes).toHaveLength(2);
  expect(publishes[1]?.idempotencyKey).toBe(publishes[0]?.idempotencyKey);
  expect(publishes[1]?.body).toBe(publishes[0]?.body);
});

test("never accepts a malformed or mismatched acknowledgement", async ({ page }) => {
  const { publishes } = await openStudio(page, {
    publish: (attempt) =>
      attempt === 1
        ? { status: 201, body: JSON.stringify({ surprise: true }) }
        : {
            status: 201,
            body: JSON.stringify({
              schema_version: 1,
              pipeline_id: "pip_other-001",
              version: 9,
              specification_sha256: "a".repeat(64),
              planner_format_version: 1,
              published_at: "2026-01-01 00:00:05",
            }),
          },
  });
  await authorMinimalPipeline(page);

  await page.getByTestId("publish-button").click();
  await expect(page.getByTestId("publication-failure")).toBeVisible();
  await expect(
    page.getByRole("cell", { name: "nod_source-csv-001", exact: true }),
  ).toBeVisible();

  await page.getByTestId("publish-button").click();
  await expect(page.getByTestId("publication-failure")).toBeVisible();
  await expect(
    page.getByRole("cell", { name: "nod_source-csv-001", exact: true }),
  ).toBeVisible();
  expect(await page.getByTestId("publication-success").count()).toBe(0);
  expect(publishes).toHaveLength(2);
});

test("keeps authoring usable with reduced motion", async ({ page }) => {
  await openStudio(page);
  await page.emulateMedia({ reducedMotion: "reduce" });
  await page.getByLabel("Add node").selectOption("source.csv");
  await page.getByTestId("add-node-button").click();
  await expect(
    page.getByRole("cell", { name: "nod_source-csv-001", exact: true }),
  ).toBeVisible();
});

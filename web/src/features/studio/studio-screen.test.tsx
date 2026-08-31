import { QueryClientProvider } from "@tanstack/react-query";
import { render, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { createBrowserRouter, Link, RouterProvider } from "react-router";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import { StudioScreen } from "./studio-screen";
import { appQueryClient } from "../../api/query-client";
import { ACK, PIPELINE_ID, stubStudioApi } from "../../test/studio-fixtures";

function renderStudio(pipelineId: string = PIPELINE_ID): void {
  window.history.replaceState({}, "", `/app/pipelines/${pipelineId}`);
  const router = createBrowserRouter([
    {
      path: "/app/pipelines/:pipelineId",
      element: <StudioScreen pipelineId={pipelineId} />,
    },
  ]);
  render(
    <QueryClientProvider client={appQueryClient}>
      <RouterProvider router={router} />
    </QueryClientProvider>,
  );
}

beforeEach(() => {
  appQueryClient.clear();
});

afterEach(() => {
  vi.unstubAllGlobals();
});

/** Drive one authoring pass: two nodes, a connection, a connector binding. */
async function authorMinimalPipeline(
  user: ReturnType<typeof userEvent.setup>,
): Promise<void> {
  await screen.findByTestId("studio-root");

  await user.selectOptions(screen.getByLabelText("Add node"), "source.csv");
  await user.click(screen.getByTestId("add-node-button"));
  await user.selectOptions(screen.getByLabelText("Add node"), "export.parquet");
  await user.click(screen.getByTestId("add-node-button"));

  // Connect through the semantic table form.
  await user.selectOptions(screen.getByLabelText("From"), "nod_source-csv-001");
  await user.selectOptions(screen.getByLabelText("To"), "nod_export-parquet-001");
  await user.click(screen.getByRole("button", { name: "Connect" }));

  // Bind the connector in the inspector.
  const structure = screen.getByRole("table", { name: "Nodes" });
  const sourceRow = within(structure)
    .getByRole("row", { name: /nod_source-csv-001/ })
    .closest("tr");
  expect(sourceRow).not.toBeNull();
  await user.click(
    within(sourceRow as HTMLElement).getByRole("button", { name: "Configure" }),
  );
  const inspector = screen.getByRole("complementary", { name: "Node inspector" });
  await user.selectOptions(
    within(inspector).getByLabelText(/Connector binding/),
    "con_source-001",
  );
  await user.click(within(inspector).getByRole("button", { name: "Close inspector" }));

  await waitFor(() => expect(screen.getByTestId("publish-button")).toBeEnabled());
}

describe("pipeline studio", () => {
  it("walks the full authoring workflow to a successful versioned publication", async () => {
    const api = stubStudioApi();
    const user = userEvent.setup();
    renderStudio();
    await authorMinimalPipeline(user);

    expect(screen.getByTestId("known-base")).toHaveTextContent("0 (unpublished)");
    await user.click(screen.getByTestId("publish-button"));

    expect(await screen.findByTestId("publication-success")).toBeVisible();
    expect(api.publishRequests).toHaveLength(1);
    const published = api.publishRequests[0];
    expect(published?.idempotencyKey).toMatch(
      /^publish-pip_demo-alpha-0-[0-9a-f]{16}$/,
    );
    const body = JSON.parse(published?.body ?? "{}") as {
      document: { nodes: { id: string }[]; edges: unknown[] };
      expected_latest_version: number;
    };
    expect(body.expected_latest_version).toBe(0);
    expect(body.document.nodes.map((entry) => entry.id)).toEqual([
      "nod_export-parquet-001",
      "nod_source-csv-001",
    ]);
    expect(body.document.edges).toHaveLength(1);
    expect(screen.getByTestId("publication-success")).toHaveTextContent(
      "Published version 1",
    );
  });

  it("prevents double submission to exactly one request", async () => {
    let releasePublish: ((response: Response) => void) | null = null;
    const api = stubStudioApi({
      onPublish: () =>
        new Promise<Response>((resolve) => {
          releasePublish = resolve;
        }),
    });
    void releasePublish;
    const user = userEvent.setup();
    renderStudio();
    await authorMinimalPipeline(user);

    await user.click(screen.getByTestId("publish-button"));
    expect(screen.getByTestId("publish-button")).toHaveTextContent("Publishing…");
    expect(screen.getByTestId("publish-button")).toBeDisabled();
    // A second activation while pending must not reach the network.
    const pointerless = userEvent.setup({ pointerEventsCheck: 0 });
    await pointerless.click(screen.getByTestId("publish-button"));
    expect(api.publishRequests).toHaveLength(1);
  });

  it("preserves the draft on a stale conflict and offers a recovery path", async () => {
    let attempts = 0;
    const api = stubStudioApi({
      onPublish: () => {
        attempts += 1;
        if (attempts === 1) {
          return new Response(
            JSON.stringify({
              type: "https://paritygrid.dev/problems/pipeline-version-conflict",
              title: "pipeline latest version is stale",
              status: 409,
              detail: "the immutable frontier moved",
              code: "pipeline_version_conflict",
            }),
            {
              status: 409,
              headers: { "Content-Type": "application/problem+json" },
            },
          );
        }
        return new Response(JSON.stringify({ ...ACK, version: 2 }), {
          status: 201,
          headers: { "Content-Type": "application/json" },
        });
      },
    });
    const user = userEvent.setup();
    renderStudio();
    await authorMinimalPipeline(user);

    await user.click(screen.getByTestId("publish-button"));
    const conflict = await screen.findByTestId("publication-conflict");
    expect(conflict).toBeVisible();
    expect(conflict).toHaveTextContent("Stale publication blocked");
    // The draft is preserved: both nodes and the connection remain.
    const nodesTable = screen.getByRole("table", { name: "Nodes" });
    expect(within(nodesTable).getAllByRole("row")).toHaveLength(3);
    expect(api.publishRequests).toHaveLength(1);
    api.setFrontier({
      schema_version: 1,
      pipeline_id: PIPELINE_ID,
      latest_version: 1,
      published: true,
    });
    await user.click(screen.getByRole("button", { name: "Refresh version frontier" }));
    // The refreshed frontier clears the stale frozen command and re-arms
    // publication against the new immutable base.
    await waitFor(() =>
      expect(screen.getByTestId("publish-button")).toHaveTextContent(
        "Publish version 2",
      ),
    );
    await user.click(screen.getByTestId("publish-button"));
    expect(await screen.findByTestId("publication-success")).toHaveTextContent(
      "Published version 2",
    );
    expect(api.publishRequests).toHaveLength(2);
    const retryBody = JSON.parse(api.publishRequests[1]?.body ?? "{}") as {
      expected_latest_version?: number;
    };
    expect(retryBody.expected_latest_version).toBe(1);
    expect(api.publishRequests[1]?.idempotencyKey).not.toBe(
      api.publishRequests[0]?.idempotencyKey,
    );
  });

  it("retries an unchanged draft under the same idempotency key", async () => {
    let attempts = 0;
    const api = stubStudioApi({
      onPublish: () => {
        attempts += 1;
        if (attempts === 1) {
          return new Response("network reset", { status: 502 });
        }
        return new Response(JSON.stringify({ ...ACK, version: 1 }), {
          status: 201,
          headers: { "Content-Type": "application/json" },
        });
      },
    });
    void api;
    const user = userEvent.setup();
    renderStudio();
    await authorMinimalPipeline(user);

    await user.click(screen.getByTestId("publish-button"));
    expect(await screen.findByTestId("publication-failure")).toBeVisible();
    expect(api.publishRequests).toHaveLength(1);

    // Explicit retry without any edit: same key, byte-equal body.
    await user.click(screen.getByTestId("publish-button"));
    expect(await screen.findByTestId("publication-success")).toBeVisible();
    expect(api.publishRequests).toHaveLength(2);
    expect(api.publishRequests[1]?.idempotencyKey).toBe(
      api.publishRequests[0]?.idempotencyKey,
    );
    expect(api.publishRequests[1]?.body).toBe(api.publishRequests[0]?.body);
  });

  it("never accepts a wrong-pipeline acknowledgement and keeps the draft", async () => {
    const api = stubStudioApi({
      onPublish: () =>
        new Response(
          JSON.stringify({ ...ACK, pipeline_id: "pip_other-001", version: 9 }),
          { status: 201, headers: { "Content-Type": "application/json" } },
        ),
    });
    void api;
    const user = userEvent.setup();
    renderStudio();
    await authorMinimalPipeline(user);

    await user.click(screen.getByTestId("publish-button"));
    const failure = await screen.findByTestId("publication-failure");
    expect(failure).toHaveTextContent("does not match this command");
    const nodesTable = screen.getByRole("table", { name: "Nodes" });
    expect(within(nodesTable).getAllByRole("row")).toHaveLength(3);
  });

  it("treats a malformed acknowledgement as a failure and preserves the draft", async () => {
    const api = stubStudioApi({
      onPublish: () =>
        new Response(JSON.stringify({ surprise: true }), {
          status: 201,
          headers: { "Content-Type": "application/json" },
        }),
    });
    void api;
    const user = userEvent.setup();
    renderStudio();
    await authorMinimalPipeline(user);

    await user.click(screen.getByTestId("publish-button"));
    expect(await screen.findByTestId("publication-failure")).toBeVisible();
    expect(screen.getByRole("table", { name: "Nodes" })).toBeInTheDocument();
  });

  it("rejects invalid connections and shows the validation error without publishing", async () => {
    const api = stubStudioApi();
    void api;
    const user = userEvent.setup();
    renderStudio();
    await screen.findByTestId("studio-root");

    await user.selectOptions(screen.getByLabelText("Add node"), "source.csv");
    await user.click(screen.getByTestId("add-node-button"));
    await user.selectOptions(screen.getByLabelText("Add node"), "transform.validate");
    await user.click(screen.getByTestId("add-node-button"));

    // Raw records cannot feed a normalized-only input.
    await user.selectOptions(screen.getByLabelText("From"), "nod_source-csv-001");
    await user.selectOptions(screen.getByLabelText("To"), "nod_transform-validate-001");
    await user.click(screen.getByRole("button", { name: "Connect" }));

    expect(await screen.findByTestId("connection-error")).toHaveTextContent(
      "records.raw cannot feed nod_transform-validate-001.records",
    );
    // No edge entered the draft; the invalid state is advisory.
    expect(screen.getByTestId("publish-button")).toBeDisabled();
  });

  it("blocks leaving with unsaved changes and offers stay-or-leave", async () => {
    const api = stubStudioApi();
    void api;
    const user = userEvent.setup();
    window.history.replaceState({}, "", `/app/pipelines/${PIPELINE_ID}`);
    const router = createBrowserRouter([
      {
        path: "/app/pipelines/:pipelineId",
        element: (
          <div>
            <Link to="/app">Operations overview</Link>
            <StudioScreen pipelineId={PIPELINE_ID} />
          </div>
        ),
      },
      { path: "/app", element: <div>Library</div> },
    ]);
    render(
      <QueryClientProvider client={appQueryClient}>
        <RouterProvider router={router} />
      </QueryClientProvider>,
    );
    await screen.findByTestId("studio-root");
    await waitFor(() => expect(screen.getByTestId("studio-canvas")).toBeVisible());

    await user.selectOptions(screen.getByLabelText("Add node"), "source.csv");
    await user.click(screen.getByTestId("add-node-button"));

    // Navigating away is blocked while the draft has unsaved changes.
    await user.click(screen.getByRole("link", { name: "Operations overview" }));
    expect(screen.getByTestId("draft-guard")).toBeVisible();
    await user.click(screen.getByRole("button", { name: "Stay and keep editing" }));
    expect(screen.queryByTestId("draft-guard")).toBeNull();
    expect(screen.getByTestId("studio-root")).toBeVisible();
  });
});

describe("fresh studio surfaces", () => {
  it("shows empty structure tables and disables publishing before authoring", async () => {
    stubStudioApi();
    const user = userEvent.setup();
    renderStudio();

    await screen.findByText("No nodes yet. Add a source node to begin.");
    expect(screen.getByText("No connections yet.")).toBeVisible();
    expect(screen.getByTestId("publish-button")).toBeDisabled();
    expect(screen.getByText("content saved")).toBeVisible();

    // Authoring flips the durable dirty indicator immediately.
    await user.selectOptions(screen.getByLabelText("Add node"), "source.csv");
    await user.click(screen.getByTestId("add-node-button"));
    expect(screen.getByText("● unsaved content changes")).toBeVisible();

    // The toolbar delete control works against the current selection.
    await user.click(screen.getByRole("button", { name: "Delete selected" }));
    expect(
      await screen.findByText("No nodes yet. Add a source node to begin."),
    ).toBeVisible();
  });

  it("drives boolean and text inspector fields and shows field errors", async () => {
    stubStudioApi();
    const user = userEvent.setup();
    renderStudio();
    await screen.findByTestId("studio-root");

    await user.selectOptions(screen.getByLabelText("Add node"), "source.csv");
    await user.click(screen.getByTestId("add-node-button"));
    await user.click(
      within(screen.getByRole("table", { name: "Nodes" })).getByRole("button", {
        name: "Configure",
      }),
    );
    const inspector = screen.getByRole("complementary", { name: "Node inspector" });
    const header = within(inspector).getByLabelText("header (optional)");
    await user.selectOptions(header, "true");
    expect(header).toHaveValue("true");

    // The missing connector binding is announced inside the inspector.
    expect(within(inspector).getByRole("alert").textContent).toContain(
      "requires a source connector",
    );
    const connector = within(inspector).getByLabelText(/Connector binding/);
    expect(connector).toHaveAttribute("aria-invalid", "true");
    expect(connector.getAttribute("aria-describedby")).toContain(
      "inspector-connector-error",
    );
  });
});

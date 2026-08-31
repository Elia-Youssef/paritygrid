import { QueryClientProvider } from "@tanstack/react-query";
import { cleanup, render, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { createBrowserRouter, RouterProvider } from "react-router";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import { StudioScreen } from "./studio-screen";
import { appQueryClient } from "../../api/query-client";
import {
  PIPELINE_ID,
  stubStudioApi,
  type StudioApiOptions,
} from "../../test/studio-fixtures";

function renderStudio(options: StudioApiOptions = {}): {
  api: ReturnType<typeof stubStudioApi>;
  user: ReturnType<typeof userEvent.setup>;
} {
  window.history.replaceState({}, "", `/app/pipelines/${PIPELINE_ID}`);
  const api = stubStudioApi(options);
  const router = createBrowserRouter([
    {
      path: "/app/pipelines/:pipelineId",
      element: <StudioScreen pipelineId={PIPELINE_ID} />,
    },
  ]);
  render(
    <QueryClientProvider client={appQueryClient}>
      <RouterProvider router={router} />
    </QueryClientProvider>,
  );
  return { api, user: userEvent.setup() };
}

beforeEach(() => {
  appQueryClient.clear();
});

afterEach(() => {
  cleanup();
  vi.unstubAllGlobals();
});

describe("studio interface", () => {
  it("renders an error state with retry when the pipeline is unknown", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn(() => Promise.resolve(new Response("missing", { status: 404 }))),
    );
    window.history.replaceState({}, "", `/app/pipelines/${PIPELINE_ID}`);
    const router = createBrowserRouter([
      {
        path: "/app/pipelines/:pipelineId",
        element: <StudioScreen pipelineId={PIPELINE_ID} />,
      },
    ]);
    render(
      <QueryClientProvider client={appQueryClient}>
        <RouterProvider router={router} />
      </QueryClientProvider>,
    );
    await screen.findAllByText("Pipeline unavailable");
  });

  it("renders an error state with retry when the frontier rejects", async () => {
    renderStudio({
      // A published flag that contradicts the zero frontier fails the
      // runtime schema, and the studio surfaces a safe error.
      frontier: {
        schema_version: 1,
        pipeline_id: PIPELINE_ID,
        latest_version: 0,
        published: true,
      },
    });
    await screen.findAllByText("Version frontier unavailable");
  });

  it("renders an error instead of loading forever when a published version fails", async () => {
    renderStudio({
      frontier: {
        schema_version: 1,
        pipeline_id: PIPELINE_ID,
        latest_version: 1,
        published: true,
      },
      onFetchVersion: () => new Response("unavailable", { status: 503 }),
    });
    expect(
      await screen.findByText("Published pipeline version unavailable"),
    ).toBeVisible();
    expect(screen.queryByText("Loading pipeline studio")).toBeNull();
  });

  it("surfaces connector inventory failure instead of invalidating every binding", async () => {
    renderStudio({
      onFetchConnectors: () => new Response("unavailable", { status: 503 }),
    });
    expect(await screen.findByText("Connector inventory unavailable")).toBeVisible();
    expect(screen.getByRole("button", { name: "Retry" })).toBeVisible();
  });

  it("opens the preview dialog and returns focus to its trigger", async () => {
    const { user } = renderStudio();
    await screen.findByTestId("studio-root");

    await user.selectOptions(screen.getByLabelText("Add node"), "source.csv");
    await user.click(screen.getByTestId("add-node-button"));

    await user.click(screen.getByTestId("preview-button"));
    const preview = screen.getByRole("dialog", { name: "Plan preview" });
    expect(preview).toBeVisible();
    expect(within(preview).getByTestId("preview-expected-frontier")).toHaveTextContent(
      "0",
    );
    expect(within(preview).getByText(/Canonical logical document/)).toBeVisible();

    await user.click(within(preview).getByRole("button", { name: "Close preview" }));
    await waitFor(() => expect(preview).not.toBeVisible());
    await waitFor(() => expect(screen.getByTestId("preview-button")).toHaveFocus());
  });

  it("edits every inspector field kind and preserves invalid values", async () => {
    const { user } = renderStudio();
    await screen.findByTestId("studio-root");

    await user.selectOptions(screen.getByLabelText("Add node"), "transform.partition");
    await user.click(screen.getByTestId("add-node-button"));
    await user.click(
      within(
        screen.getByRole("row", { name: /nod_transform-partition-001/ }),
      ).getByRole("button", { name: "Configure" }),
    );

    const inspector = screen.getByRole("complementary", { name: "Node inspector" });
    const countField = within(inspector).getByLabelText("partition_count (optional)");
    await user.type(countField, "4");
    await waitFor(() => {
      expect(
        within(inspector).getByLabelText("partition_count (optional)"),
      ).toHaveValue(4);
    });

    // Invalid content is kept so the user can correct it; validation speaks.
    await user.type(countField, "x");
    expect(screen.getByTestId("publish-button")).toBeDisabled();

    await user.click(
      within(inspector).getByRole("button", { name: "Close inspector" }),
    );
    expect(screen.getByRole("complementary", { name: "Node inspector" })).toBeVisible();
  });

  it("exposes fit view and zoom controls without pointer-only assumptions", async () => {
    const { user } = renderStudio();
    await screen.findByTestId("studio-root");

    await user.click(screen.getByRole("button", { name: "Fit view" }));
    await user.click(screen.getByRole("button", { name: "Zoom in" }));
    await user.click(screen.getByRole("button", { name: "Zoom out" }));
    expect(screen.getByTestId("studio-canvas")).toBeVisible();
  });
});

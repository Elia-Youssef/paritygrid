import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { cleanup, render, screen, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, describe, expect, it, vi } from "vitest";

import { PublicationPanel } from "./publication-panel";
import {
  emptyDocument,
  materializeResourcePolicy,
  parsePipelineDocument,
  toPublicationDocument,
  type PipelineDocumentValue,
} from "../../pipelines/document";

function published(): PipelineDocumentValue {
  const parsed = parsePipelineDocument(
    toPublicationDocument({
      schemaVersion: 1,
      canonicalFormatVersion: 1,
      nodes: [
        {
          id: "nod_source-csv-001",
          kind: "source.csv",
          configuration_version: 1,
          configuration: {},
          connector_id: "con_source-001",
        },
      ],
      edges: [],
      resourcePolicy: materializeResourcePolicy({}),
      layout: [{ node_id: "nod_source-csv-001", x: 0, y: 0 }],
    }),
  );
  if (!parsed.ok) {
    throw new Error("fixture rejected");
  }
  return parsed.value;
}

const FRONTIER_ZERO = {
  schema_version: 1 as const,
  pipeline_id: "pip_demo-alpha",
  latest_version: 0,
  published: false,
};

function renderPanel(
  props: Partial<Parameters<typeof PublicationPanel>[0]> = {},
): void {
  const document = props.document ?? published();
  render(
    <QueryClientProvider client={new QueryClient()}>
      <PublicationPanel
        pipelineId="pip_demo-alpha"
        pipelineName="Demo pipeline"
        document={document}
        diagnostics={props.diagnostics ?? []}
        frontier={props.frontier === undefined ? FRONTIER_ZERO : props.frontier}
        command={props.command ?? null}
        publication={props.publication ?? { kind: "idle" }}
        beginCommand={
          props.beginCommand ??
          (() => ({
            idempotencyKey: "publish-pip_demo-alpha-0-0123456789abcdef",
            expectedLatestVersion: 0,
            bodyJson: "{}",
            document: toPublicationDocument(document),
          }))
        }
        onPublished={props.onPublished ?? (() => undefined)}
        onConflict={props.onConflict ?? (() => undefined)}
        onFailed={props.onFailed ?? (() => undefined)}
        onRefreshFrontier={props.onRefreshFrontier ?? (() => Promise.resolve())}
      />
    </QueryClientProvider>,
  );
}

afterEach(() => {
  cleanup();
  vi.unstubAllGlobals();
});

describe("publication panel rendering", () => {
  it("labels the first publication for an unpublished pipeline", () => {
    renderPanel();
    expect(screen.getByTestId("known-base")).toHaveTextContent("0 (unpublished)");
    expect(screen.getByTestId("publish-button")).toHaveTextContent("Publish version 1");
    expect(screen.getByTestId("publish-button")).toBeEnabled();
    expect(screen.getByText(/no errors/)).toBeVisible();
  });

  it("labels the next version for a published pipeline", () => {
    renderPanel({
      frontier: { ...FRONTIER_ZERO, latest_version: 3, published: true },
    });
    expect(screen.getByTestId("known-base")).toHaveTextContent("3");
    expect(screen.getByTestId("publish-button")).toHaveTextContent("Publish version 4");
  });

  it("disables publication while the frontier is unknown", () => {
    renderPanel({ frontier: null });
    expect(screen.getByTestId("publish-button")).toBeDisabled();
    expect(screen.getByTestId("known-base")).toHaveTextContent("unavailable");
  });

  it("disables publication while validation errors remain", () => {
    renderPanel({
      diagnostics: [
        {
          code: "missing-connector",
          message: "CSV source requires a source connector binding.",
          nodeId: "nod_source-csv-001",
        },
      ],
    });
    expect(screen.getByTestId("publish-button")).toBeDisabled();
    expect(screen.getByText(/1 error\(s\) block publication/)).toBeVisible();
  });

  it("disables publication for an empty draft", () => {
    renderPanel({ document: emptyDocument(), frontier: null });
    expect(screen.getByTestId("publish-button")).toBeDisabled();
  });

  it("shows pending publication state", () => {
    renderPanel({ publication: { kind: "pending" } });
    expect(screen.getByTestId("publish-button")).toHaveTextContent("Publishing…");
    expect(screen.getByTestId("publish-button")).toBeDisabled();
  });

  it("opens the preview for a frozen command and shows the fingerprint", async () => {
    const user = userEvent.setup();
    const command = {
      idempotencyKey: "publish-pip_demo-alpha-0-0123456789abcdef",
      expectedLatestVersion: 0,
      bodyJson: '{"expected_latest_version":0}',
      document: toPublicationDocument(published()),
    };
    renderPanel({ command });
    await user.click(screen.getByTestId("preview-button"));
    const preview = within(screen.getByRole("dialog", { name: "Plan preview" }));
    const close = preview.getByRole("button", { name: "Close preview" });
    expect(close).toHaveFocus();
    await user.tab();
    expect(close).toHaveFocus();
    await user.tab({ shift: true });
    expect(close).toHaveFocus();
    expect(preview.getByTestId("command-fingerprint")).not.toHaveTextContent(
      "will bind on publish",
    );
  });

  it("blocks stale replay until the frontier refresh completes", async () => {
    let releaseRefresh!: () => void;
    const refresh = new Promise<void>((resolve) => {
      releaseRefresh = resolve;
    });
    const user = userEvent.setup();
    renderPanel({
      publication: {
        kind: "conflict",
        problem: { title: "stale", extensions: [] },
      },
      onRefreshFrontier: () => refresh,
    });

    expect(screen.getByTestId("publish-button")).toBeDisabled();
    await user.click(screen.getByRole("button", { name: "Refresh version frontier" }));
    expect(
      screen.getByRole("button", { name: "Refreshing version frontier…" }),
    ).toBeDisabled();
    releaseRefresh();
    await refresh;
  });
});

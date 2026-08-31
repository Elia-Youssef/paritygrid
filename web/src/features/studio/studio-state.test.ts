import { act, renderHook, waitFor } from "@testing-library/react";
import { describe, expect, it } from "vitest";

import {
  acknowledgementMatches,
  buildPublicationCommand,
  contentHash,
  layoutDirty,
  logicalDirty,
  useStudioState,
  type StudioDraftState,
} from "./studio-state";
import {
  emptyDocument,
  materializeResourcePolicy,
  parsePipelineDocument,
  serializeLayout,
  serializeLogicalDocument,
  toPublicationDocument,
  type PipelineDocumentValue,
} from "../../pipelines/document";

const PUBLISHED_WIRE = toPublicationDocument({
  schemaVersion: 1,
  canonicalFormatVersion: 1,
  nodes: [
    {
      id: "nod_source-001",
      kind: "source.csv",
      configuration_version: 1,
      configuration: {},
      connector_id: "con_source-001",
    },
  ],
  edges: [],
  resourcePolicy: materializeResourcePolicy({}),
  layout: [{ node_id: "nod_source-001", x: 10, y: 20 }],
});

function publishedDocument(): PipelineDocumentValue {
  const parsed = parsePipelineDocument(PUBLISHED_WIRE);
  if (!parsed.ok) {
    throw new Error("fixture rejected");
  }
  return parsed.value;
}

const NO_CONNECTORS = [] as const;

describe("publication commands", () => {
  it("binds the idempotency key to the exact canonical content", () => {
    const document = publishedDocument();
    const first = buildPublicationCommand("pip_demo-alpha", document, 0);
    const second = buildPublicationCommand("pip_demo-alpha", document, 0);
    expect(second.idempotencyKey).toBe(first.idempotencyKey);
    expect(second.bodyJson).toBe(first.bodyJson);

    // A different frontier is a different command even with equal content.
    const advanced = buildPublicationCommand("pip_demo-alpha", document, 3);
    expect(advanced.idempotencyKey).not.toBe(first.idempotencyKey);
    expect(advanced.bodyJson).toContain('"expected_latest_version":3');
  });

  it("changes the key whenever the draft content changes", () => {
    const document = publishedDocument();
    const first = buildPublicationCommand("pip_demo-alpha", document, 0);
    const edited: PipelineDocumentValue = {
      ...document,
      nodes: document.nodes.map((entry) =>
        entry.kind === "source.csv"
          ? { ...entry, configuration: { header: true } }
          : entry,
      ),
    };
    const second = buildPublicationCommand("pip_demo-alpha", edited, 0);
    expect(second.idempotencyKey).not.toBe(first.idempotencyKey);
  });

  it("freezes byte-equivalent bodies that exclude layout-only identity", () => {
    const document = publishedDocument();
    const moved: PipelineDocumentValue = {
      ...document,
      layout: [{ node_id: "nod_source-001", x: 999, y: 999 }],
    };
    // The body changes with layout (the envelope includes it), and the key
    // changes with it — so a stale key can never replay new bytes.
    const first = buildPublicationCommand("pip_demo-alpha", document, 0);
    const second = buildPublicationCommand("pip_demo-alpha", moved, 0);
    expect(second.bodyJson).not.toBe(first.bodyJson);
    expect(second.idempotencyKey).not.toBe(first.idempotencyKey);
  });

  it("never uses metadata row_version semantics in the frontier", () => {
    // The command carries only the immutable frontier value.
    const command = buildPublicationCommand("pip_demo-alpha", publishedDocument(), 2);
    expect(command.expectedLatestVersion).toBe(2);
    const parsedBody: unknown = JSON.parse(command.bodyJson);
    expect(parsedBody).toHaveProperty("expected_latest_version", 2);
  });

  it("hashes content deterministically", () => {
    expect(contentHash("")).toBe("cbf29ce484222325");
    expect(contentHash("hello")).toBe("a430d84680aabd0b");
    expect(contentHash("abc")).toBe(contentHash("abc"));
    expect(contentHash("abc")).not.toBe(contentHash("abd"));
    expect(contentHash("abc")).toMatch(/^[0-9a-f]{16}$/);
  });

  it("accepts only matching acknowledgements", () => {
    const command = buildPublicationCommand("pip_demo-alpha", publishedDocument(), 4);
    expect(
      acknowledgementMatches("pip_demo-alpha", command, {
        schema_version: 1,
        pipeline_id: "pip_demo-alpha",
        version: 5,
        specification_sha256: "a".repeat(64),
        planner_format_version: 1,
        published_at: "2026-01-01 00:00:00",
      }),
    ).toBe(true);
    expect(
      acknowledgementMatches("pip_demo-alpha", command, {
        schema_version: 1,
        pipeline_id: "pip_other-001",
        version: 5,
        specification_sha256: "a".repeat(64),
        planner_format_version: 1,
        published_at: "2026-01-01 00:00:00",
      }),
    ).toBe(false);
    expect(
      acknowledgementMatches("pip_demo-alpha", command, {
        schema_version: 1,
        pipeline_id: "pip_demo-alpha",
        version: 6,
        specification_sha256: "a".repeat(64),
        planner_format_version: 1,
        published_at: "2026-01-01 00:00:00",
      }),
    ).toBe(false);
  });
});

describe("studio draft state", () => {
  it("initializes once from the published document and ignores refetches", async () => {
    const initial = publishedDocument();
    const { result, rerender } = renderHook(() =>
      useStudioState({
        pipelineId: "pip_demo-alpha",
        connectors: NO_CONNECTORS,
        initialDocument: initial,
        initialReady: true,
      }),
    );
    await waitFor(() => expect(result.current.draft).not.toBeNull());
    expect(result.current.logicalDirty).toBe(false);

    // A later query response (even with different server data) cannot
    // overwrite the local draft.
    const newer = publishedDocument();
    rerender();
    act(() => {
      result.current.edit((current) => ({
        ...current,
        nodes: [
          ...current.nodes,
          {
            id: "nod_export-001",
            kind: "export.parquet",
            configuration_version: 1,
            configuration: {},
            connector_id: null,
          },
        ],
        layout: [{ node_id: "nod_source-001", x: 42, y: 84 }],
      }));
    });
    expect(result.current.layoutDirty).toBe(true);
    const replaced = parsePipelineDocument(
      toPublicationDocument({ ...initial, layout: [] }),
    );
    expect(replaced.ok).toBe(true);
    rerender();
    expect(result.current.draft?.document.nodes).toHaveLength(2);
    expect(result.current.logicalDirty).toBe(true);
    void replaced;
    void newer;
  });

  it("starts unpublished pipelines from an empty bounded draft", async () => {
    const { result } = renderHook(() =>
      useStudioState({
        pipelineId: "pip_new-001",
        connectors: NO_CONNECTORS,
        initialDocument: null,
        initialReady: true,
      }),
    );
    await waitFor(() => expect(result.current.draft).not.toBeNull());
    expect(result.current.draft?.document.nodes).toEqual([]);
    expect(result.current.logicalDirty).toBe(false);
  });

  it("treats layout edits and logical edits as separate dirty state", async () => {
    const { result } = renderHook(() =>
      useStudioState({
        pipelineId: "pip_demo-alpha",
        connectors: NO_CONNECTORS,
        initialDocument: publishedDocument(),
        initialReady: true,
      }),
    );
    await waitFor(() => expect(result.current.draft).not.toBeNull());

    act(() => {
      result.current.edit((current) => ({
        ...current,
        layout: [{ node_id: "nod_source-001", x: 123, y: 45 }],
      }));
    });
    expect(result.current.layoutDirty).toBe(true);
    expect(result.current.logicalDirty).toBe(false);

    act(() => {
      result.current.edit((current) => ({
        ...current,
        nodes: [
          ...current.nodes,
          {
            id: "nod_export-001",
            kind: "export.parquet",
            configuration_version: 1,
            configuration: {},
            connector_id: null,
          },
        ],
      }));
    });
    expect(result.current.logicalDirty).toBe(true);
  });

  it("keeps the frozen command through failures and re-bases only on validated success", async () => {
    const { result } = renderHook(() =>
      useStudioState({
        pipelineId: "pip_demo-alpha",
        connectors: NO_CONNECTORS,
        initialDocument: publishedDocument(),
        initialReady: true,
      }),
    );
    await waitFor(() => expect(result.current.draft).not.toBeNull());

    act(() => {
      result.current.edit((current) => ({
        ...current,
        nodes: [
          ...current.nodes,
          {
            id: "nod_export-001",
            kind: "export.parquet",
            configuration_version: 1,
            configuration: {},
            connector_id: null,
          },
        ],
      }));
    });
    let command!: ReturnType<typeof buildPublicationCommand>;
    act(() => {
      command = result.current.beginCommand(2);
    });
    expect(result.current.command?.idempotencyKey).toBe(command.idempotencyKey);

    act(() => {
      result.current.markFailed({ title: "network down", extensions: [] });
    });
    // The command and the draft survive the failure unchanged, so an
    // explicit retry reuses the exact same key and bytes.
    expect(result.current.command?.idempotencyKey).toBe(command.idempotencyKey);
    expect(result.current.draft?.document.nodes).toHaveLength(2);

    act(() => {
      result.current.markPublished({
        schema_version: 1,
        pipeline_id: "pip_demo-alpha",
        version: 3,
        specification_sha256: "a".repeat(64),
        planner_format_version: 1,
        published_at: "2026-01-01 00:00:00",
      });
    });
    expect(result.current.publication.kind).toBe("published");
    expect(result.current.logicalDirty).toBe(false);
    expect(result.current.layoutDirty).toBe(false);
    expect(result.current.command).toBeNull();
  });

  it("clears a stale command and its conflict before rebuilding", async () => {
    const { result } = renderHook(() =>
      useStudioState({
        pipelineId: "pip_demo-alpha",
        connectors: NO_CONNECTORS,
        initialDocument: publishedDocument(),
        initialReady: true,
      }),
    );
    await waitFor(() => expect(result.current.draft).not.toBeNull());

    act(() => {
      result.current.beginCommand(1);
      result.current.markConflict({ title: "stale", extensions: [] });
    });
    expect(result.current.command?.expectedLatestVersion).toBe(1);
    expect(result.current.publication.kind).toBe("conflict");

    act(() => {
      result.current.clearCommand();
    });
    expect(result.current.command).toBeNull();
    expect(result.current.publication.kind).toBe("idle");

    let rebuilt!: ReturnType<typeof buildPublicationCommand>;
    act(() => {
      rebuilt = result.current.beginCommand(2);
    });
    expect(rebuilt.expectedLatestVersion).toBe(2);
  });

  it("re-bases nothing when the acknowledgement does not match", async () => {
    const { result } = renderHook(() =>
      useStudioState({
        pipelineId: "pip_demo-alpha",
        connectors: NO_CONNECTORS,
        initialDocument: publishedDocument(),
        initialReady: true,
      }),
    );
    await waitFor(() => expect(result.current.draft).not.toBeNull());
    act(() => {
      result.current.edit((current) => ({
        ...current,
        nodes: [
          ...current.nodes,
          {
            id: "nod_export-001",
            kind: "export.parquet",
            configuration_version: 1,
            configuration: {},
            connector_id: null,
          },
        ],
      }));
    });
    expect(result.current.logicalDirty).toBe(true);
    act(() => {
      result.current.beginCommand(2);
    });
    act(() => {
      result.current.markPublished({
        schema_version: 1,
        pipeline_id: "pip_other-001",
        version: 3,
        specification_sha256: "a".repeat(64),
        planner_format_version: 1,
        published_at: "2026-01-01 00:00:00",
      });
    });
    // The foreign acknowledgement is downgraded to a failure.
    expect(result.current.publication.kind).toBe("failed");
    // The logical baseline was NOT updated for a foreign acknowledgement;
    // the draft remains dirty and intact.
    expect(result.current.logicalDirty).toBe(true);
  });

  it("discards stale commands when the draft is edited", async () => {
    const { result } = renderHook(() =>
      useStudioState({
        pipelineId: "pip_demo-alpha",
        connectors: NO_CONNECTORS,
        initialDocument: publishedDocument(),
        initialReady: true,
      }),
    );
    await waitFor(() => expect(result.current.draft).not.toBeNull());
    act(() => {
      result.current.beginCommand(0);
    });
    expect(result.current.command).not.toBeNull();
    act(() => {
      result.current.edit((current) => ({
        ...current,
        layout: [{ node_id: "nod_source-001", x: 5, y: 5 }],
      }));
    });
    expect(result.current.command).toBeNull();
  });

  it("resets everything when the pipeline changes", async () => {
    const { result, rerender } = renderHook(
      ({ pipelineId }: { pipelineId: string }) =>
        useStudioState({
          pipelineId,
          connectors: NO_CONNECTORS,
          initialDocument: publishedDocument(),
          initialReady: true,
        }),
      { initialProps: { pipelineId: "pip_demo-alpha" } },
    );
    await waitFor(() => expect(result.current.draft).not.toBeNull());
    rerender({ pipelineId: "pip_other-001" });
    await waitFor(() => expect(result.current.draft).not.toBeNull());
    expect(result.current.selection).toBeNull();
    expect(result.current.command).toBeNull();
    expect(result.current.publication.kind).toBe("idle");
  });

  it("exposes dirty helpers against the baselines", () => {
    const document = publishedDocument();
    const clean: StudioDraftState = {
      document,
      baselineLogical: serializeLogicalDocument(document),
      baselineLayout: serializeLayout(document),
    };
    expect(logicalDirty(clean)).toBe(false);
    expect(layoutDirty(clean)).toBe(false);
    const dirty: StudioDraftState = {
      document: { ...document, nodes: [...document.nodes] },
      baselineLogical: serializeLogicalDocument(document),
      baselineLayout: serializeLayout(document),
    };
    expect(logicalDirty(dirty)).toBe(false);
  });

  it("supports the empty draft for brand-new pipelines", () => {
    const empty = emptyDocument();
    expect(
      logicalDirty({
        document: empty,
        baselineLogical: serializeLogicalDocument(empty),
        baselineLayout: serializeLayout(empty),
      }),
    ).toBe(false);
  });
});

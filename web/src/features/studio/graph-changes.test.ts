import { describe, expect, it } from "vitest";

import {
  addNode,
  applyEdgeChanges,
  applyLayout,
  applyNodeChanges,
  checkConnection,
  gridPosition,
  nextNodeId,
  removeEdgeByIndex,
} from "./graph-changes";
import {
  emptyDocument,
  materializeResourcePolicy,
  parsePipelineDocument,
  toPublicationDocument,
  type PipelineDocumentValue,
  type PipelineEdgeValue,
} from "../../pipelines/document";

function seeded(): PipelineDocumentValue {
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
        {
          id: "nod_export-parquet-001",
          kind: "export.parquet",
          configuration_version: 1,
          configuration: {},
          connector_id: null,
        },
      ],
      edges: [
        {
          source_node_id: "nod_source-csv-001",
          source_port: "records",
          target_node_id: "nod_export-parquet-001",
          target_port: "records",
        },
      ],
      resourcePolicy: materializeResourcePolicy({}),
      layout: [
        { node_id: "nod_source-csv-001", x: 10, y: 20 },
        { node_id: "nod_export-parquet-001", x: 300, y: 20 },
      ],
    }),
  );
  if (!parsed.ok) {
    throw new Error("fixture rejected");
  }
  return parsed.value;
}

describe("node identity and placement", () => {
  it("generates unique deterministic ids per kind", () => {
    const document = seeded();
    expect(nextNodeId(document, "transform.validate")).toBe(
      "nod_transform-validate-001",
    );
    const withOne = addNode(document, "source.csv");
    expect(withOne.nodeId).toBe("nod_source-csv-002");
    expect(nextNodeId(withOne.document, "source.csv")).toBe("nod_source-csv-003");
  });

  it("places new nodes on a deterministic grid", () => {
    expect(gridPosition(emptyDocument())).toEqual({ x: 24, y: 24 });
    expect(gridPosition(seeded())).toEqual({ x: 464, y: 24 });
    const many: PipelineDocumentValue = {
      ...seeded(),
      nodes: [
        ...seeded().nodes,
        ...seeded().nodes,
        ...seeded().nodes,
        ...seeded().nodes,
      ],
    };
    expect(gridPosition(many).y).toBeGreaterThan(24);
  });

  it("appends nodes with layout entries in one step", () => {
    const { document, nodeId } = addNode(emptyDocument(), "export.parquet");
    expect(nodeId).toBe("nod_export-parquet-001");
    expect(document.nodes).toHaveLength(1);
    expect(document.layout).toEqual([{ node_id: nodeId, x: 24, y: 24 }]);
  });
});

describe("react-flow change application", () => {
  it("applies position changes to layout only, never logical content", () => {
    const document = seeded();
    const before = JSON.stringify(document.nodes);
    const result = applyNodeChanges(document, [
      { type: "position", id: "nod_source-csv-001", position: { x: 55.6, y: -12.2 } },
    ]);
    expect(result.layoutTouched).toBe(true);
    expect(result.removedNodeIds).toEqual([]);
    expect(JSON.stringify(result.document.nodes)).toBe(before);
    const source = result.document.layout.find(
      (entry) => entry.node_id === "nod_source-csv-001",
    );
    expect(source).toEqual({ node_id: "nod_source-csv-001", x: 56, y: -12 });
  });

  it("creates a layout entry for a moved node that had none", () => {
    const document = seeded();
    const stripped: PipelineDocumentValue = {
      ...document,
      layout: document.layout.filter((entry) => entry.node_id !== "nod_source-csv-001"),
    };
    const result = applyLayout(stripped, "nod_source-csv-001", { x: 5, y: 6 });
    expect(result.layout).toContainEqual({ node_id: "nod_source-csv-001", x: 5, y: 6 });
  });

  it("removes nodes together with their edges and layout", () => {
    const document = seeded();
    const result = applyNodeChanges(document, [
      { type: "remove", id: "nod_source-csv-001" },
    ]);
    expect(result.removedNodeIds).toEqual(["nod_source-csv-001"]);
    expect(result.document.nodes).toHaveLength(1);
    expect(result.document.edges).toHaveLength(0);
    expect(result.document.layout).toHaveLength(1);
  });

  it("tracks selection changes", () => {
    const document = seeded();
    const result = applyNodeChanges(document, [
      { type: "select", id: "nod_export-parquet-001", selected: true },
    ]);
    expect(result.selectedNodeId).toBe("nod_export-parquet-001");
    expect(result.document).toBe(document);
  });

  it("decodes and removes edges by change id and by index", () => {
    const document = seeded();
    const edgeId = "nod_source-csv-001:records->nod_export-parquet-001:records";
    const afterChange = applyEdgeChanges(document, [{ type: "remove", id: edgeId }]);
    expect(afterChange.edges).toHaveLength(0);

    const afterIndex = removeEdgeByIndex(document, 0);
    expect(afterIndex.edges).toHaveLength(0);
    // Out-of-range indexes leave the document untouched.
    expect(removeEdgeByIndex(document, 9)).toBe(document);
  });

  it("ignores malformed edge ids", () => {
    const document = seeded();
    const malformed: PipelineEdgeValue[] | PipelineDocumentValue = applyEdgeChanges(
      document,
      [{ type: "remove", id: "garbage" }],
    );
    expect(malformed).toBe(document);
  });
});

describe("connection checks", () => {
  const document = seeded();

  it("accepts a compatible connection on a free input", () => {
    const withoutEdge: PipelineDocumentValue = { ...document, edges: [] };
    const check = checkConnection(withoutEdge, {
      source: "nod_source-csv-001",
      sourceHandle: "records",
      target: "nod_export-parquet-001",
      targetHandle: "records",
    });
    expect(check).toEqual({ ok: true });
  });

  it("rejects unknown nodes, self-connections, unknown ports, wrong types, and occupied inputs", () => {
    expect(
      checkConnection(document, {
        source: "nod_ghost-001",
        sourceHandle: "records",
        target: "nod_export-parquet-001",
        targetHandle: "records",
      }),
    ).toEqual({
      ok: false,
      reason: "Connection references a node that does not exist.",
    });

    expect(
      checkConnection(document, {
        source: "nod_source-csv-001",
        sourceHandle: "records",
        target: "nod_source-csv-001",
        targetHandle: "records",
      }),
    ).toEqual({ ok: false, reason: "A node cannot connect to itself." });

    expect(
      checkConnection(document, {
        source: "nod_source-csv-001",
        sourceHandle: "payload",
        target: "nod_export-parquet-001",
        targetHandle: "records",
      }),
    ).toEqual({ ok: false, reason: "Connection uses an undeclared port." });

    expect(
      checkConnection(document, {
        source: "nod_export-parquet-001",
        sourceHandle: "records",
        target: "nod_source-csv-001",
        targetHandle: "records",
      }),
    ).toEqual({ ok: false, reason: "Connection uses an undeclared port." });

    expect(
      checkConnection(document, {
        source: "nod_source-csv-001",
        sourceHandle: "records",
        target: "nod_export-parquet-001",
        targetHandle: "records",
      }),
    ).toEqual({ ok: false, reason: "That input already has a connection." });
  });
});

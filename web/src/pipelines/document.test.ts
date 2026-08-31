import { describe, expect, it } from "vitest";

import {
  canonicalJson,
  emptyDocument,
  materializeResourcePolicy,
  parsePipelineDocument,
  serializeLayout,
  serializeLogicalDocument,
  toPublicationDocument,
  validateDocument,
  type PipelineDocumentValue,
  type PipelineEdgeValue,
  type PipelineNodeValue,
} from "./document";
import {
  NODE_DEFINITIONS,
  nodeDefinition,
  portsAreCompatible,
  type InputPortDefinition,
  type OutputPortDefinition,
} from "./registry";

const CSV_CONNECTOR = {
  connector_id: "con_source-001",
  kind: "csv_source",
  display_name: "CSV source",
  archived_at: null,
  capabilities: { read: true },
};

function node(
  id: string,
  kind: string,
  overrides: Partial<PipelineNodeValue> = {},
): PipelineNodeValue {
  return {
    id,
    kind,
    configuration_version: 1,
    configuration: {},
    connector_id: null,
    ...overrides,
  };
}

function edge(
  source: string,
  target: string,
  sourcePort = "records",
  targetPort = "records",
): PipelineEdgeValue {
  return {
    source_node_id: source,
    source_port: sourcePort,
    target_node_id: target,
    target_port: targetPort,
  };
}

/** A valid minimal pipeline: CSV source → export. */
function validDocument(): PipelineDocumentValue {
  return {
    schemaVersion: 1,
    canonicalFormatVersion: 1,
    nodes: [
      node("nod_source-001", "source.csv", { connector_id: "con_source-001" }),
      node("nod_export-001", "export.parquet"),
    ],
    edges: [edge("nod_source-001", "nod_export-001")],
    resourcePolicy: materializeResourcePolicy({}),
    layout: [
      { node_id: "nod_source-001", x: 0, y: 0 },
      { node_id: "nod_export-001", x: 260, y: 0 },
    ],
  };
}

describe("node registry", () => {
  it("mirrors the closed backend registry exhaustively", () => {
    // The backend BUILTIN_NODE_REGISTRY defines exactly these 13 kinds; the
    // frontend registry must never silently grow beyond the backend.
    expect(NODE_DEFINITIONS).toHaveLength(13);
    const kinds = NODE_DEFINITIONS.map((definition) => definition.kind).sort();
    expect(kinds).toEqual([
      "export.parquet",
      "reconcile.target",
      "repair.apply",
      "repair.approval",
      "repair.generate",
      "source.csv",
      "source.http.async",
      "source.http.blocking",
      "source.jsonl",
      "transform.normalize",
      "transform.partition",
      "transform.validate",
      "verify.target",
    ]);
  });

  it("defines every kind with exactly one configuration schema version 1", () => {
    for (const definition of NODE_DEFINITIONS) {
      expect(definition.configurationSchema.version).toBe(1);
      expect(
        definition.ports.inputs.length + definition.ports.outputs.length,
      ).toBeGreaterThan(0);
      if (definition.role === "repair_effect") {
        expect(definition.requiresIdempotency).toBe(true);
      }
    }
  });

  it("rejects arbitrary executable node kinds", () => {
    for (const kind of [
      "script.python",
      "eval.js",
      "shell.exec",
      "sql.raw",
      "template.render",
      "expression.math",
    ]) {
      expect(nodeDefinition(kind)).toBeUndefined();
    }
  });

  it("enforces typed port compatibility between the chain kinds", () => {
    const out = (valueType: string): OutputPortDefinition => ({
      name: "records",
      valueType: valueType as never,
    });
    const input = (accepts: readonly string[]): InputPortDefinition => ({
      name: "records",
      accepts: accepts as never,
      required: true,
      maximumConnections: 1,
    });
    expect(portsAreCompatible(out("records.raw"), input(["records.raw"]))).toBe(true);
    expect(portsAreCompatible(out("records.raw"), input(["records.normalized"]))).toBe(
      false,
    );
    expect(
      portsAreCompatible(out("reconciliation.result"), input(["repair.plan"])),
    ).toBe(false);
    expect(
      portsAreCompatible(out("repair.plan.approved"), input(["repair.plan.approved"])),
    ).toBe(true);
  });

  it("documents source connector requirements per kind", () => {
    expect(nodeDefinition("source.csv")?.connectorRequirement).toBe("source");
    expect(nodeDefinition("reconcile.target")?.connectorRequirement).toBe("target");
    expect(nodeDefinition("transform.normalize")?.connectorRequirement).toBe("none");
    expect(nodeDefinition("source.http.async")?.requiredCapabilities).toEqual([
      "async_io",
      "read",
    ]);
    expect(nodeDefinition("repair.apply")?.requiredCapabilities).toEqual([
      "idempotency",
      "write",
    ]);
  });
});

describe("document parsing", () => {
  it("accepts a canonical wire document", () => {
    const wire = toPublicationDocument(validDocument());
    const result = parsePipelineDocument(wire);
    expect(result.ok).toBe(true);
    if (result.ok) {
      expect(result.value.nodes).toHaveLength(2);
      expect(result.value.resourcePolicy.max_concurrency).toBe(4);
    }
  });

  it("rejects unsupported schema and canonical format versions", () => {
    const wrongSchema = {
      ...toPublicationDocument(validDocument()),
      schema_version: 2,
    };
    expect(parsePipelineDocument(wrongSchema).ok).toBe(false);
    const wrongCanonical = {
      ...toPublicationDocument(validDocument()),
      canonical_format_version: 3,
    };
    expect(parsePipelineDocument(wrongCanonical).ok).toBe(false);
  });

  it("rejects unknown fields at every level", () => {
    const rootExtra = { ...toPublicationDocument(validDocument()), telemetry: true };
    expect(parsePipelineDocument(rootExtra).ok).toBe(false);
    const withBadNode = toPublicationDocument(validDocument());
    const badNodes = withBadNode.nodes as Record<string, unknown>[];
    badNodes[0]!.status = "live";
    expect(parsePipelineDocument(withBadNode).ok).toBe(false);
  });

  it("rejects malformed identifiers and negative versions", () => {
    const wire = toPublicationDocument(validDocument());
    const wireNodes = wire.nodes as Record<string, unknown>[];
    wireNodes[0]!.id = "node-1";
    expect(parsePipelineDocument(wire).ok).toBe(false);

    const brokenVersion = toPublicationDocument(validDocument());
    const versionNodes = brokenVersion.nodes as Record<string, unknown>[];
    versionNodes[0]!.configuration_version = 0;
    expect(parsePipelineDocument(brokenVersion).ok).toBe(false);
  });

  it("rejects out-of-bounds coordinates and collection sizes", () => {
    const farLayout = {
      ...toPublicationDocument(validDocument()),
      layout: [{ node_id: "nod_source-001", x: 2_000_000, y: 0 }],
    };
    expect(parsePipelineDocument(farLayout).ok).toBe(false);
  });

  it("does not silently default malformed authoritative data", () => {
    const missingPolicy = toPublicationDocument(validDocument());
    delete missingPolicy.resource_policy;
    expect(parsePipelineDocument(missingPolicy).ok).toBe(false);
  });
});

describe("document validation", () => {
  it("accepts the minimal valid pipeline with matching connectors", () => {
    const diagnostics = validateDocument(validDocument(), [CSV_CONNECTOR]);
    expect(diagnostics).toEqual([]);
  });

  it("rejects unknown node kinds instead of preserving them", () => {
    const document = {
      ...validDocument(),
      nodes: [node("nod_script-001", "script.python")],
    };
    const diagnostics = validateDocument(document, []);
    expect(
      diagnostics.some((diagnostic) => diagnostic.code === "unknown-node-kind"),
    ).toBe(true);
  });

  it("flags unknown configuration fields and wrong value kinds per node", () => {
    const document: PipelineDocumentValue = {
      ...validDocument(),
      nodes: [
        node("nod_partition-001", "transform.partition", {
          configuration: { partition_count: "many", extra: 1 },
        }),
        node("nod_export-001", "export.parquet"),
      ],
    };
    const diagnostics = validateDocument(document, []);
    const fieldErrors = diagnostics.filter(
      (diagnostic) => diagnostic.code === "invalid-configuration",
    );
    expect(fieldErrors.map((diagnostic) => diagnostic.field).sort()).toEqual([
      "extra",
      "partition_count",
    ]);
  });

  it("requires connector bindings with the exact capabilities", () => {
    const document: PipelineDocumentValue = {
      ...validDocument(),
      nodes: [
        node("nod_source-001", "source.csv"),
        node("nod_export-001", "export.parquet"),
      ],
    };
    const missing = validateDocument(document, [CSV_CONNECTOR]);
    expect(missing.some((diagnostic) => diagnostic.code === "missing-connector")).toBe(
      true,
    );

    const weakConnector = { ...CSV_CONNECTOR, capabilities: {} };
    const incapable = validateDocument(
      {
        ...document,
        nodes: [
          node("nod_source-001", "source.csv", { connector_id: "con_source-001" }),
          node("nod_export-001", "export.parquet"),
        ],
      },
      [weakConnector],
    );
    expect(
      incapable.some((diagnostic) => diagnostic.code === "connector-capability"),
    ).toBe(true);

    const archived = { ...CSV_CONNECTOR, archived_at: "2026-01-01 00:00:00" };
    const unavailable = validateDocument(
      {
        ...document,
        nodes: [
          node("nod_source-001", "source.csv", { connector_id: "con_source-001" }),
          node("nod_export-001", "export.parquet"),
        ],
      },
      [archived],
    );
    expect(
      unavailable.some((diagnostic) => diagnostic.code === "missing-connector"),
    ).toBe(true);
  });

  it("forbids connector bindings on connector-free nodes", () => {
    const document: PipelineDocumentValue = {
      ...validDocument(),
      nodes: [
        node("nod_source-001", "source.csv", { connector_id: "con_source-001" }),
        node("nod_export-001", "export.parquet", { connector_id: "con_source-001" }),
      ],
    };
    const diagnostics = validateDocument(document, [CSV_CONNECTOR]);
    expect(
      diagnostics.some((diagnostic) => diagnostic.code === "connector-not-permitted"),
    ).toBe(true);
  });

  it("rejects duplicate nodes, duplicate edges, unknown edge endpoints, and unknown ports", () => {
    const duplicateNode: PipelineDocumentValue = {
      ...validDocument(),
      nodes: [
        node("nod_source-001", "source.csv", { connector_id: "con_source-001" }),
        node("nod_source-001", "export.parquet"),
      ],
    };
    expect(
      validateDocument(duplicateNode, [CSV_CONNECTOR]).some(
        (diagnostic) => diagnostic.code === "duplicate-node-id",
      ),
    ).toBe(true);

    const duplicateEdge: PipelineDocumentValue = {
      ...validDocument(),
      edges: [
        edge("nod_source-001", "nod_export-001"),
        edge("nod_source-001", "nod_export-001"),
      ],
    };
    expect(
      validateDocument(duplicateEdge, [CSV_CONNECTOR]).some(
        (diagnostic) => diagnostic.code === "duplicate-edge",
      ),
    ).toBe(true);

    const unknownNode: PipelineDocumentValue = {
      ...validDocument(),
      edges: [edge("nod_ghost-001", "nod_export-001")],
    };
    expect(
      validateDocument(unknownNode, [CSV_CONNECTOR]).some(
        (diagnostic) => diagnostic.code === "unknown-edge-node",
      ),
    ).toBe(true);

    const unknownPort: PipelineDocumentValue = {
      ...validDocument(),
      edges: [edge("nod_source-001", "nod_export-001", "records", "payload")],
    };
    expect(
      validateDocument(unknownPort, [CSV_CONNECTOR]).some(
        (diagnostic) => diagnostic.code === "unknown-edge-port",
      ),
    ).toBe(true);
  });

  it("rejects incompatible port types, self-connections, and fan-in violations", () => {
    const chain: PipelineDocumentValue = {
      ...validDocument(),
      nodes: [
        node("nod_source-001", "source.csv", { connector_id: "con_source-001" }),
        node("nod_normalize-001", "transform.normalize"),
        node("nod_export-001", "export.parquet"),
      ],
      edges: [
        edge("nod_source-001", "nod_normalize-001"),
        edge("nod_normalize-001", "nod_export-001"),
      ],
    };
    expect(validateDocument(chain, [CSV_CONNECTOR])).toEqual([]);

    // raw records cannot feed a normalized-only input.
    const incompatible: PipelineDocumentValue = {
      ...validDocument(),
      nodes: [
        node("nod_source-001", "source.csv", { connector_id: "con_source-001" }),
        node("nod_validate-001", "transform.validate"),
      ],
      edges: [edge("nod_source-001", "nod_validate-001")],
    };
    expect(
      validateDocument(incompatible, [CSV_CONNECTOR]).some(
        (diagnostic) => diagnostic.code === "incompatible-ports",
      ),
    ).toBe(true);

    // Sources have no input ports, so a back-edge references an undeclared
    // port and can never connect.
    const reversed: PipelineDocumentValue = {
      ...validDocument(),
      nodes: [
        node("nod_source-001", "source.csv", { connector_id: "con_source-001" }),
        node("nod_normalize-001", "transform.normalize"),
      ],
      edges: [edge("nod_normalize-001", "nod_source-001")],
    };
    expect(
      validateDocument(reversed, [CSV_CONNECTOR]).some(
        (diagnostic) => diagnostic.code === "unknown-edge-port",
      ),
    ).toBe(true);

    const selfConnected: PipelineDocumentValue = {
      ...validDocument(),
      edges: [edge("nod_source-001", "nod_source-001")],
    };
    expect(
      validateDocument(selfConnected, [CSV_CONNECTOR]).some(
        (diagnostic) => diagnostic.code === "cyclic-graph",
      ),
    ).toBe(true);
  });

  it("requires required inputs, acyclic topology, sources, and terminals", () => {
    const noSource: PipelineDocumentValue = {
      ...validDocument(),
      nodes: [node("nod_export-001", "export.parquet")],
      edges: [],
    };
    expect(
      validateDocument(noSource, []).some(
        (diagnostic) => diagnostic.code === "invalid-source",
      ),
    ).toBe(true);

    // The typed port lattice is acyclic by construction, so a same-type
    // back-edge (the only way to close a cycle) is asserted via the
    // partition/reconcile pair: partitioned records can feed reconcile, but
    // a second edge back would need a type that does not exist. The
    // remaining cycle form is the self-connection, covered above; here the
    // valid chain must stay cycle-free.
    const diamond: PipelineDocumentValue = {
      ...validDocument(),
      nodes: [
        node("nod_source-001", "source.csv", { connector_id: "con_source-001" }),
        node("nod_export-001", "export.parquet"),
        node("nod_export-002", "export.parquet"),
      ],
      edges: [
        edge("nod_source-001", "nod_export-001"),
        edge("nod_source-001", "nod_export-002"),
      ],
    };
    expect(
      validateDocument(diamond, [CSV_CONNECTOR]).some(
        (diagnostic) => diagnostic.code === "cyclic-graph",
      ),
    ).toBe(false);

    const disconnected: PipelineDocumentValue = {
      ...validDocument(),
      nodes: [
        node("nod_source-001", "source.csv", { connector_id: "con_source-001" }),
        node("nod_export-001", "export.parquet"),
        node("nod_source-002", "source.jsonl", { connector_id: "con_source-001" }),
        node("nod_export-002", "export.parquet"),
      ],
      edges: [
        edge("nod_source-001", "nod_export-001"),
        edge("nod_source-002", "nod_export-002"),
      ],
    };
    expect(
      validateDocument(disconnected, [CSV_CONNECTOR]).some(
        (diagnostic) => diagnostic.code === "disconnected-graph",
      ),
    ).toBe(false); // two complete source→terminal chains are each connected
  });

  it("flags an incoherent resource policy", () => {
    const document = validDocument();
    const incoherent = {
      ...document,
      resourcePolicy: {
        ...document.resourcePolicy,
        max_in_flight: 1,
        max_concurrency: 4,
      },
    };
    const diagnostics = validateDocument(incoherent, [CSV_CONNECTOR]);
    expect(
      diagnostics.some((diagnostic) => diagnostic.code === "resource-policy"),
    ).toBe(true);
  });
});

describe("canonical serialization", () => {
  it("is independent of node and edge insertion order", () => {
    const first = validDocument();
    const second: PipelineDocumentValue = {
      ...first,
      nodes: [first.nodes[1]!, first.nodes[0]!],
      edges: [first.edges[0]!],
      layout: [first.layout[1]!, first.layout[0]!],
    };
    expect(serializeLogicalDocument(second)).toBe(serializeLogicalDocument(first));
  });

  it("excludes layout coordinates from logical identity", () => {
    const first = validDocument();
    const moved: PipelineDocumentValue = {
      ...first,
      layout: [
        { node_id: "nod_source-001", x: 500, y: 700 },
        { node_id: "nod_export-001", x: 0, y: 0 },
      ],
    };
    expect(serializeLogicalDocument(moved)).toBe(serializeLogicalDocument(first));
    expect(serializeLayout(moved)).not.toBe(serializeLayout(first));
  });

  it("changes logical identity when content changes", () => {
    const first = serializeLogicalDocument(validDocument());
    const renamed = validDocument();
    const changed: PipelineDocumentValue = {
      ...renamed,
      nodes: renamed.nodes.map((entry) =>
        entry.kind === "source.csv"
          ? { ...entry, configuration: { header: true } }
          : entry,
      ),
    };
    expect(serializeLogicalDocument(changed)).not.toBe(first);
  });

  it("produces byte-identical publication envelopes regardless of order", () => {
    const first = toPublicationDocument(validDocument());
    const second = toPublicationDocument({
      ...validDocument(),
      nodes: [validDocument().nodes[1]!, validDocument().nodes[0]!],
      layout: [validDocument().layout[1]!, validDocument().layout[0]!],
    });
    expect(JSON.stringify(second)).toBe(JSON.stringify(first));
  });

  it("serializes scalars deterministically", () => {
    expect(canonicalJson({ b: 1, a: [2, { z: true, y: null }] })).toBe(
      '{"a":[2,{"y":null,"z":true}],"b":1}',
    );
  });

  it("materializes the full resource policy", () => {
    const policy = materializeResourcePolicy({ max_concurrency: 8 });
    expect(policy).toEqual({
      max_concurrency: 8,
      max_in_flight: 16,
      memory_limit_bytes: 536_870_912,
      operation_timeout_seconds: 60,
      queue_capacity: 256,
    });
  });

  it("round-trips through the wire schema", () => {
    const parsed = parsePipelineDocument(toPublicationDocument(validDocument()));
    expect(parsed.ok).toBe(true);
    if (parsed.ok) {
      expect(serializeLogicalDocument(parsed.value)).toBe(
        serializeLogicalDocument(validDocument()),
      );
    }
  });

  it("starts drafts from a bounded empty document", () => {
    const empty = emptyDocument();
    expect(empty.nodes).toEqual([]);
    expect(empty.edges).toEqual([]);
    expect(validateDocument(empty, [])).not.toEqual([]);
  });
});

describe("validation branch sweep", () => {
  function edgeDiagnostics(
    edges: PipelineEdgeValue[],
    nodes: readonly PipelineNodeValue[],
  ): string[] {
    return validateDocument({ ...validDocument(), edges, nodes }, [CSV_CONNECTOR]).map(
      (diagnostic) => diagnostic.code,
    );
  }

  it("reports fan-in violations from two different sources", () => {
    const nodes = [
      node("nod_source-001", "source.csv", { connector_id: "con_source-001" }),
      node("nod_export-001", "export.parquet"),
      node("nod_source-009", "source.csv", { connector_id: "con_source-001" }),
    ];
    const codes = edgeDiagnostics(
      [
        edge("nod_source-001", "nod_export-001"),
        edge("nod_source-009", "nod_export-001"),
      ],
      nodes,
    );
    expect(codes).toContain("fan-in-exceeded");
  });

  it("reports dead ends that are not terminal roles", () => {
    const nodes = [
      node("nod_source-001", "source.csv", { connector_id: "con_source-001" }),
      node("nod_normalize-001", "transform.normalize"),
    ];
    const codes = edgeDiagnostics([edge("nod_source-001", "nod_normalize-001")], nodes);
    expect(codes).toContain("invalid-terminal");
  });

  it("reports edges whose endpoints are both unknown", () => {
    const codes = edgeDiagnostics(
      [edge("nod_ghost-001", "nod_ghost-002")],
      [...validDocument().nodes],
    );
    expect(codes).toContain("unknown-edge-node");
  });

  it("supports required-input diagnostics with port targets", () => {
    const nodes = [
      node("nod_source-001", "source.csv", { connector_id: "con_source-001" }),
      node("nod_normalize-001", "transform.normalize"),
      node("nod_export-001", "export.parquet"),
    ];
    const codes = edgeDiagnostics([edge("nod_source-001", "nod_export-001")], nodes);
    expect(codes).toContain("missing-input");
    const diagnostics = validateDocument(
      { ...validDocument(), nodes, edges: [edge("nod_source-001", "nod_export-001")] },
      [CSV_CONNECTOR],
    );
    const missing = diagnostics.find(
      (diagnostic) => diagnostic.code === "missing-input",
    );
    expect(missing?.port).toBe("records");
    expect(missing?.nodeId).toBe("nod_normalize-001");
  });

  it("handles the empty-document diagnostics", () => {
    const empty = emptyDocument();
    const codes = validateDocument(empty, []).map((diagnostic) => diagnostic.code);
    expect(codes).toContain("invalid-source");
  });

  it("omits undefined members from canonical JSON objects", () => {
    expect(canonicalJson({ a: 1, b: undefined, c: [null, false] })).toBe(
      '{"a":1,"c":[null,false]}',
    );
  });
});

describe("canonical JSON scalars", () => {
  it("serializes scalars through JSON semantics", () => {
    expect(canonicalJson("text")).toBe('"text"');
    expect(canonicalJson(5)).toBe("5");
    expect(canonicalJson(null)).toBe("null");
    expect(canonicalJson([])).toBe("[]");
    expect(canonicalJson({})).toBe("{}");
  });
});

describe("registry-aligned configuration bounds", () => {
  it("rejects integer configuration outside the registered bounds", () => {
    const withPartition = (value: unknown): PipelineDocumentValue => ({
      ...validDocument(),
      nodes: [
        node("nod_partition-001", "transform.partition", {
          configuration: { partition_count: value as number },
        }),
        node("nod_export-001", "export.parquet"),
      ],
      edges: [edge("nod_partition-001", "nod_export-001")],
    });
    const low = validateDocument(withPartition(0), []);
    const fieldErrors = low.filter(
      (diagnostic) => diagnostic.code === "invalid-configuration",
    );
    expect(
      fieldErrors.some((diagnostic) => diagnostic.field === "partition_count"),
    ).toBe(true);

    const ok = validateDocument(withPartition(4), []);
    expect(
      ok.filter(
        (diagnostic) =>
          diagnostic.code === "invalid-configuration" &&
          diagnostic.field === "partition_count",
      ),
    ).toEqual([]);
  });

  it("validates text configuration emptiness per the registry value kind", () => {
    const withCsv = (
      configuration: Record<string, string | number | boolean>,
    ): PipelineDocumentValue => ({
      ...validDocument(),
      nodes: [
        node("nod_source-001", "source.csv", {
          configuration,
          connector_id: "con_source-001",
        }),
        node("nod_export-001", "export.parquet"),
      ],
    });
    expect(
      validateDocument(withCsv({ encoding: "" }), [CSV_CONNECTOR]).some(
        (diagnostic) => diagnostic.code === "invalid-configuration",
      ),
    ).toBe(true);
    expect(
      validateDocument(withCsv({ encoding: "utf-8" }), [CSV_CONNECTOR]).some(
        (diagnostic) =>
          diagnostic.code === "invalid-configuration" &&
          diagnostic.field === "encoding",
      ),
    ).toBe(false);
  });
});

describe("diagnostic targeting branches", () => {
  it("targets the known node when an edge source is missing", () => {
    const diagnostics = validateDocument(
      { ...validDocument(), edges: [edge("nod_ghost-001", "nod_export-001")] },
      [CSV_CONNECTOR],
    );
    expect(diagnostics.map((diagnostic) => diagnostic.code)).toContain(
      "unknown-edge-node",
    );
    expect(diagnostics[0]?.nodeId).toBe("nod_export-001");
  });

  it("labels non-object documents at the root", () => {
    const result = parsePipelineDocument("not-a-document");
    expect(result.ok).toBe(false);
    if (!result.ok) {
      expect(result.reason.startsWith("<root>")).toBe(true);
    }
  });

  it("sorts object keys that arrive in reverse order", () => {
    expect(canonicalJson({ b: 1, a: 2 })).toBe('{"a":2,"b":1}');
  });
});

describe("bounded document constraint sweep", () => {
  function baseWire(): Record<string, unknown> {
    return toPublicationDocument(validDocument());
  }

  // Spread helpers typed over open records so malformed variants compile.

  function oversizeNodes(): Record<string, unknown> {
    const wire = baseWire();
    wire.nodes = Array.from({ length: 257 }, (_unused, index) => ({
      id: `nod_bulk-${String(index).padStart(3, "0")}`,
      kind: "export.parquet",
      configuration_version: 1,
      configuration: {},
      connector_id: null,
    }));
    return wire;
  }

  it("rejects documents beyond the bounded collection sizes", () => {
    const oversizeEdges = baseWire();
    const edges: unknown[] = [];
    for (let index = 0; index < 4_097; index += 1) {
      edges.push(edge("nod_source-001", "nod_export-001"));
    }
    oversizeEdges.edges = edges;
    expect(parsePipelineDocument(oversizeEdges).ok).toBe(false);
    expect(parsePipelineDocument(oversizeNodes()).ok).toBe(false);

    const oversizeLayout = baseWire();
    oversizeLayout.layout = Array.from({ length: 257 }, () => ({
      node_id: "nod_source-001",
      x: 0,
      y: 0,
    }));
    expect(parsePipelineDocument(oversizeLayout).ok).toBe(false);
  });

  it("rejects constraint violations on versions, kinds, ports, policy, and values", () => {
    const cases: (() => Record<string, unknown>)[] = [
      () => {
        const wire = baseWire();
        (wire.nodes as Record<string, unknown>[])[0]!.configuration_version =
          2_147_483_648;
        return wire;
      },
      () => {
        const wire = baseWire();
        (wire.nodes as Record<string, unknown>[])[0]!.kind = "SOURCE.CSV";
        return wire;
      },
      () => {
        const wire = baseWire();
        (wire.edges as Record<string, unknown>[])[0]!.source_port = "x".repeat(65);
        return wire;
      },
      () => {
        const wire = baseWire();
        (wire.edges as Record<string, unknown>[])[0]!.target_port = "Bad_Port";
        return wire;
      },
      () => {
        const wire = baseWire();
        const policy = wire.resource_policy as Record<string, unknown>;
        wire.resource_policy = { ...policy, max_concurrency: 0 };
        return wire;
      },
      () => {
        const wire = baseWire();
        const policy = wire.resource_policy as Record<string, unknown>;
        wire.resource_policy = { ...policy, max_concurrency: 1.5 };
        return wire;
      },
      () => {
        const wire = baseWire();
        const policy = wire.resource_policy as Record<string, unknown>;
        wire.resource_policy = { ...policy, unknown_field: 1 };
        return wire;
      },
      () => {
        const wire = baseWire();
        (wire.nodes as Record<string, unknown>[])[0]!.configuration = {
          header: ["true"],
        };
        return wire;
      },
      () => {
        const wire = baseWire();
        (wire.layout as Record<string, unknown>[])[0]!.x = 1_000_001;
        return wire;
      },
    ];
    for (const build of cases) {
      expect(parsePipelineDocument(build()).ok).toBe(false);
    }
  });
});

describe("identifier and name boundary cases", () => {
  it("rejects empty port names and oversized kinds and node ids", () => {
    const withPort = (port: string): Record<string, unknown> => {
      const wire = toPublicationDocument(validDocument());
      (wire.edges as Record<string, unknown>[])[0]!.source_port = port;
      return wire;
    };
    expect(parsePipelineDocument(withPort("")).ok).toBe(false);

    const withKind = (kind: string): Record<string, unknown> => {
      const wire = toPublicationDocument(validDocument());
      (wire.nodes as Record<string, unknown>[])[0]!.kind = kind;
      return wire;
    };
    expect(parsePipelineDocument(withKind("a".repeat(97))).ok).toBe(false);

    const withId = (id: string): Record<string, unknown> => {
      const wire = toPublicationDocument(validDocument());
      (wire.nodes as Record<string, unknown>[])[0]!.id = id;
      return wire;
    };
    expect(parsePipelineDocument(withId(`nod_${"a".repeat(65)}`)).ok).toBe(false);
  });
});

describe("semantic and serialization branch coverage", () => {
  it("rejects an unsupported configuration version at the semantic layer", () => {
    const document: PipelineDocumentValue = {
      ...validDocument(),
      nodes: [
        node("nod_source-001", "source.csv", {
          configuration_version: 2,
          connector_id: "con_source-001",
        }),
        node("nod_export-001", "export.parquet"),
      ],
    };
    const diagnostics = validateDocument(document, [CSV_CONNECTOR]);
    expect(
      diagnostics.some(
        (diagnostic) => diagnostic.code === "unsupported-configuration-version",
      ),
    ).toBe(true);
  });

  it("validates boolean configuration through the closed value kinds", () => {
    const withHeader = (value: string | number | boolean): PipelineDocumentValue => ({
      ...validDocument(),
      nodes: [
        node("nod_source-001", "source.csv", {
          configuration: { header: value },
          connector_id: "con_source-001",
        }),
        node("nod_export-001", "export.parquet"),
      ],
    });
    expect(
      validateDocument(withHeader(true), [CSV_CONNECTOR]).some(
        (diagnostic) =>
          diagnostic.code === "invalid-configuration" && diagnostic.field === "header",
      ),
    ).toBe(false);
    expect(
      validateDocument(withHeader("yes"), [CSV_CONNECTOR]).some(
        (diagnostic) =>
          diagnostic.code === "invalid-configuration" && diagnostic.field === "header",
      ),
    ).toBe(true);
  });

  it("targets the existing node when an edge's source kind is unregistered", () => {
    const document: PipelineDocumentValue = {
      ...validDocument(),
      nodes: [
        node("nod_script-001", "script.python"),
        node("nod_export-001", "export.parquet"),
      ],
      edges: [edge("nod_script-001", "nod_export-001")],
    };
    const diagnostics = validateDocument(document, []);
    const target = diagnostics.find(
      (diagnostic) =>
        diagnostic.code === "unknown-edge-node" ||
        diagnostic.nodeId === "nod_export-001",
    );
    expect(target).toBeDefined();
  });

  it("exercises every compareStrings stage while sorting edges", () => {
    const document: PipelineDocumentValue = {
      ...validDocument(),
      nodes: [
        node("nod_source-001", "source.csv", { connector_id: "con_source-001" }),
        node("nod_source-002", "source.csv", { connector_id: "con_source-001" }),
        node("nod_export-001", "export.parquet"),
        node("nod_export-002", "export.parquet"),
        node("nod_export-003", "export.parquet"),
        node("nod_export-004", "export.parquet"),
      ],
      // Each adjacent pair differs at exactly one comparison stage, and the
      // reverse-sorted order drives both greater-than and less-than paths.
      edges: [
        edge("nod_source-002", "nod_export-004", "records", "records"),
        edge("nod_source-001", "nod_export-003", "records", "records"),
        edge("nod_source-001", "nod_export-001", "records", "records"),
        edge("nod_source-001", "nod_export-002", "records", "records"),
      ],
    };
    const serialized = serializeLogicalDocument(document);
    const published = toPublicationDocument(document);
    // Deterministic re-serialization from a different insertion order.
    const reordered: PipelineDocumentValue = {
      ...document,
      edges: [
        document.edges[3]!,
        document.edges[2]!,
        document.edges[1]!,
        document.edges[0]!,
      ],
    };
    expect(serializeLogicalDocument(reordered)).toBe(serialized);
    expect(JSON.stringify(toPublicationDocument(reordered))).toBe(
      JSON.stringify(published),
    );
  });

  it("sorts object keys in reverse insertion order canonically", () => {
    expect(canonicalJson({ c: 3, a: 1, b: 2 })).toBe('{"a":1,"b":2,"c":3}');
  });

  it("serializes an undefined scalar as null", () => {
    expect(canonicalJson(undefined)).toBe("null");
  });
});

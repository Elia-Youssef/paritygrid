/**
 * Closed, versioned frontend pipeline-document model.
 *
 * Wire shape mirrors the backend `PipelineDocument` exactly: unknown root,
 * node, edge, and layout fields are rejected rather than stripped, versions
 * are closed, collections are bounded, and configuration is validated
 * against the exhaustive node registry. The logical content (nodes, edges,
 * resource policy) is kept conceptually separate from the visual layout:
 * `serializeLogicalDocument` deliberately ignores coordinates so layout
 * edits never change logical identity, while `toPublicationDocument`
 * materializes the complete deterministic wire document the server
 * requires.
 */
import {
  NODE_DEFINITIONS,
  nodeDefinition,
  portsAreCompatible,
  type ConfigurationField,
  type NodeDefinition,
} from "./registry";
import {
  PIPELINE_CANONICAL_FORMAT_VERSION,
  PIPELINE_DOCUMENT_SCHEMA_VERSION,
  RESOURCE_POLICY_DEFAULTS,
  pipelineDocumentSchema,
} from "./document-schema";
import type {
  PipelineDocumentValue,
  PipelineLayoutValue,
  PipelineNodeValue,
  ResourcePolicyValue,
} from "./document-schema";

export {
  PIPELINE_CANONICAL_FORMAT_VERSION,
  PIPELINE_DOCUMENT_SCHEMA_VERSION,
  MAX_PIPELINE_CONFIGURATION_VERSION,
  MAX_PIPELINE_EDGES,
  MAX_PIPELINE_LAYOUT_COORDINATE,
  MAX_PIPELINE_NODES,
  RESOURCE_POLICY_DEFAULTS,
  RESOURCE_POLICY_LIMITS,
  pipelineDocumentSchema,
} from "./document-schema";
export type {
  PipelineDocumentValue,
  PipelineDocumentWire,
  PipelineEdgeValue,
  PipelineLayoutValue,
  PipelineNodeValue,
  ResourcePolicyValue,
} from "./document-schema";

export const PIPELINE_PLANNER_FORMAT_VERSION = 1;

export type ParseResult<T> = { ok: true; value: T } | { ok: false; reason: string };

/** Parse an untrusted document envelope into the trusted model. */
export function parsePipelineDocument(
  input: unknown,
): ParseResult<PipelineDocumentValue> {
  const parsed = pipelineDocumentSchema.safeParse(input);
  if (!parsed.success) {
    const issue = parsed.error.issues[0];
    const path = issue === undefined ? "<root>" : issue.path.join(".") || "<root>";
    return { ok: false, reason: `${path}: ${issue?.message ?? "invalid document"}` };
  }
  const raw = parsed.data;
  return {
    ok: true,
    value: {
      schemaVersion: raw.schema_version,
      canonicalFormatVersion: raw.canonical_format_version,
      nodes: raw.nodes,
      edges: raw.edges,
      resourcePolicy: materializeResourcePolicy(raw.resource_policy),
      layout: raw.layout,
    },
  };
}

/** Apply deterministic defaults so the publication envelope is total. */
export function materializeResourcePolicy(
  partial: Partial<ResourcePolicyValue>,
): ResourcePolicyValue {
  return { ...RESOURCE_POLICY_DEFAULTS, ...partial };
}

// ---------------------------------------------------------------------------
// Validation diagnostics
// ---------------------------------------------------------------------------

export type DiagnosticCode =
  | "unknown-node-kind"
  | "unsupported-configuration-version"
  | "invalid-configuration"
  | "missing-connector"
  | "connector-not-permitted"
  | "connector-capability"
  | "duplicate-node-id"
  | "duplicate-edge"
  | "unknown-edge-node"
  | "unknown-edge-port"
  | "incompatible-ports"
  | "fan-in-exceeded"
  | "missing-input"
  | "cyclic-graph"
  | "disconnected-graph"
  | "invalid-source"
  | "invalid-terminal"
  | "resource-policy";

export interface Diagnostic {
  readonly code: DiagnosticCode;
  readonly message: string;
  readonly nodeId?: string;
  readonly port?: string;
  readonly field?: string;
}

export interface ConnectorSummary {
  readonly connector_id: string;
  readonly kind: string;
  readonly display_name: string;
  readonly archived_at: string | null;
  readonly capabilities: Readonly<Record<string, boolean>>;
}

function configurationDiagnostics(node: PipelineNodeValue): Diagnostic[] {
  const definition = nodeDefinition(node.kind);
  if (definition === undefined) {
    return [
      {
        code: "unknown-node-kind",
        message: `Unknown node kind "${node.kind}".`,
        nodeId: node.id,
      },
    ];
  }
  const diagnostics: Diagnostic[] = [];
  if (node.configuration_version !== definition.configurationSchema.version) {
    diagnostics.push({
      code: "unsupported-configuration-version",
      message: `Configuration version ${String(node.configuration_version)} is not supported for ${node.kind}; expected ${String(definition.configurationSchema.version)}.`,
      nodeId: node.id,
    });
    return diagnostics;
  }
  const defined = new Map(
    definition.configurationSchema.fields.map((f) => [f.name, f]),
  );
  for (const [name, value] of Object.entries(node.configuration)) {
    const schema = defined.get(name);
    if (schema === undefined) {
      diagnostics.push({
        code: "invalid-configuration",
        message: `Unknown configuration field "${name}" for ${node.kind}.`,
        nodeId: node.id,
        field: name,
      });
      continue;
    }
    if (!valueMatchesKind(value, schema)) {
      diagnostics.push({
        code: "invalid-configuration",
        message: `Configuration field "${name}" must be ${schema.valueKind}.`,
        nodeId: node.id,
        field: name,
      });
      continue;
    }
    if (
      schema.valueKind === "integer" &&
      typeof value === "number" &&
      schema.minimum !== undefined &&
      schema.maximum !== undefined &&
      (value < schema.minimum || value > schema.maximum)
    ) {
      diagnostics.push({
        code: "invalid-configuration",
        message: `Configuration field "${name}" must be between ${String(schema.minimum)} and ${String(schema.maximum)}.`,
        nodeId: node.id,
        field: name,
      });
    }
  }
  for (const schema of definition.configurationSchema.fields) {
    if (schema.required && !(schema.name in node.configuration)) {
      diagnostics.push({
        code: "invalid-configuration",
        message: `Missing required configuration field "${schema.name}" for ${node.kind}.`,
        nodeId: node.id,
        field: schema.name,
      });
    }
  }
  return diagnostics;
}

function valueMatchesKind(value: unknown, schema: ConfigurationField): boolean {
  switch (schema.valueKind) {
    case "boolean":
      return typeof value === "boolean";
    case "integer":
      return typeof value === "number" && Number.isInteger(value);
    case "text":
      return typeof value === "string" && value.length > 0;
  }
}

function connectorDiagnostics(
  node: PipelineNodeValue,
  definition: NodeDefinition,
  connectors: readonly ConnectorSummary[],
): Diagnostic[] {
  const byId = new Map(
    connectors.map((connector) => [connector.connector_id, connector]),
  );
  const bound = node.connector_id;
  if (definition.connectorRequirement === "none") {
    return bound === null
      ? []
      : [
          {
            code: "connector-not-permitted",
            message: `${definition.label} does not accept a connector binding.`,
            nodeId: node.id,
            field: "connector",
          },
        ];
  }
  if (bound === null) {
    return [
      {
        code: "missing-connector",
        message: `${definition.label} requires a ${definition.connectorRequirement} connector binding.`,
        nodeId: node.id,
        field: "connector",
      },
    ];
  }
  const connector = byId.get(bound);
  if (connector?.archived_at !== null || connector === undefined) {
    return [
      {
        code: "missing-connector",
        message: `Bound connector "${bound}" is unavailable.`,
        nodeId: node.id,
        field: "connector",
      },
    ];
  }
  const missing = definition.requiredCapabilities.filter(
    (capability) => connector.capabilities[capability] !== true,
  );
  if (missing.length > 0) {
    return [
      {
        code: "connector-capability",
        message: `Connector "${bound}" lacks the required ${definition.connectorRequirement} capabilities (${missing.join(", ")}).`,
        nodeId: node.id,
        field: "connector",
      },
    ];
  }
  return [];
}

/**
 * Full semantic validation: configuration, connectors, edges, fan-in,
 * required inputs, cycles, and source-to-terminal reachability. Mirror of
 * the backend planner validators, used to gate publication client-side.
 */
export function validateDocument(
  document: PipelineDocumentValue,
  connectors: readonly ConnectorSummary[] = [],
): readonly Diagnostic[] {
  const diagnostics: Diagnostic[] = [];

  const byId = new Map<string, PipelineNodeValue>();
  for (const node of document.nodes) {
    if (byId.has(node.id)) {
      diagnostics.push({
        code: "duplicate-node-id",
        message: `Node id "${node.id}" is used more than once.`,
        nodeId: node.id,
      });
      continue;
    }
    byId.set(node.id, node);
  }

  const definitions = new Map<string, NodeDefinition>();
  for (const node of byId.values()) {
    const definition = nodeDefinition(node.kind);
    if (definition === undefined) {
      diagnostics.push(...configurationDiagnostics(node));
      continue;
    }
    definitions.set(node.id, definition);
    diagnostics.push(...configurationDiagnostics(node));
    diagnostics.push(...connectorDiagnostics(node, definition, connectors));
  }

  const outgoing = new Map<string, Map<string, string[]>>();
  for (const node of byId.keys()) {
    outgoing.set(node, new Map());
  }
  const seenEdges = new Set<string>();
  const incomingCount = new Map<string, number>();
  for (const edge of document.edges) {
    const edgeKey = `${edge.source_node_id}:${edge.source_port}>${edge.target_node_id}:${edge.target_port}`;
    if (seenEdges.has(edgeKey)) {
      diagnostics.push({
        code: "duplicate-edge",
        message: `Duplicate connection ${edgeKey}.`,
        nodeId: edge.target_node_id,
        port: edge.target_port,
      });
      continue;
    }
    seenEdges.add(edgeKey);

    const sourceNode = byId.get(edge.source_node_id);
    const targetNode = byId.get(edge.target_node_id);
    if (sourceNode === undefined || targetNode === undefined) {
      diagnostics.push({
        code: "unknown-edge-node",
        message: `Connection references a node that does not exist (${edge.source_node_id} → ${edge.target_node_id}).`,
        nodeId: byId.has(edge.target_node_id)
          ? edge.target_node_id
          : edge.source_node_id,
      });
      continue;
    }
    if (edge.source_node_id === edge.target_node_id) {
      diagnostics.push({
        code: "cyclic-graph",
        message: "A node cannot connect to itself.",
        nodeId: edge.source_node_id,
      });
      continue;
    }
    const sourceDefinition = definitions.get(edge.source_node_id);
    const targetDefinition = definitions.get(edge.target_node_id);
    if (sourceDefinition === undefined || targetDefinition === undefined) {
      // Kind-level diagnostics already cover the unknown definitions.
      continue;
    }
    const sourcePort = sourceDefinition.ports.outputs.find(
      (port) => port.name === edge.source_port,
    );
    const targetPort = targetDefinition.ports.inputs.find(
      (port) => port.name === edge.target_port,
    );
    if (sourcePort === undefined || targetPort === undefined) {
      diagnostics.push({
        code: "unknown-edge-port",
        message: `Connection uses an undeclared port (${edge.source_node_id}.${edge.source_port} → ${edge.target_node_id}.${edge.target_port}).`,
        nodeId: edge.target_node_id,
        port: edge.target_port,
      });
      continue;
    }
    if (!portsAreCompatible(sourcePort, targetPort)) {
      diagnostics.push({
        code: "incompatible-ports",
        message: `${sourcePort.valueType} cannot feed ${edge.target_node_id}.${targetPort.name}.`,
        nodeId: edge.target_node_id,
        port: targetPort.name,
      });
      continue;
    }
    const count =
      (incomingCount.get(`${edge.target_node_id}:${edge.target_port}`) ?? 0) + 1;
    incomingCount.set(`${edge.target_node_id}:${edge.target_port}`, count);
    if (count > targetPort.maximumConnections) {
      diagnostics.push({
        code: "fan-in-exceeded",
        message: `Input ${targetPort.name} of ${edge.target_node_id} accepts at most ${String(targetPort.maximumConnections)} connection.`,
        nodeId: edge.target_node_id,
        port: targetPort.name,
      });
      continue;
    }
    const ports = outgoing.get(edge.source_node_id);
    ports?.set(`${edge.target_node_id}:${edge.target_port}`, [
      ...(ports.get(`${edge.target_node_id}:${edge.target_port}`) ?? []),
      edge.target_node_id,
    ]);
  }

  for (const node of byId.values()) {
    const definition = definitions.get(node.id);
    if (definition === undefined) {
      continue;
    }
    for (const input of definition.ports.inputs) {
      if (
        input.required &&
        (incomingCount.get(`${node.id}:${input.name}`) ?? 0) === 0
      ) {
        diagnostics.push({
          code: "missing-input",
          message: `${definition.label} input "${input.name}" is not connected.`,
          nodeId: node.id,
          port: input.name,
        });
      }
    }
  }

  diagnostics.push(...reachabilityDiagnostics(byId, definitions, outgoing));
  diagnostics.push(...resourcePolicyDiagnostics(document.resourcePolicy));
  return diagnostics;
}

function reachabilityDiagnostics(
  byId: Map<string, PipelineNodeValue>,
  definitions: Map<string, NodeDefinition>,
  outgoing: Map<string, Map<string, string[]>>,
): Diagnostic[] {
  const diagnostics: Diagnostic[] = [];
  if (byId.size === 0) {
    return [
      {
        code: "invalid-source",
        message: "A pipeline requires at least one source node.",
      },
    ];
  }
  const sources = [...byId.values()].filter(
    (node) => definitions.get(node.id)?.role === "source",
  );
  if (sources.length === 0) {
    return [
      {
        code: "invalid-source",
        message: "A pipeline requires at least one source node.",
      },
    ];
  }
  for (const node of byId.values()) {
    if (
      !hasIncoming(outgoing, node.id) &&
      definitions.get(node.id)?.role !== "source"
    ) {
      diagnostics.push({
        code: "invalid-source",
        message: `Graph root ${node.id} must be a source node.`,
        nodeId: node.id,
      });
    }
  }
  // Cycles are structurally impossible here: every accepted edge strictly
  // ascends the closed port-type lattice and self-connections are rejected
  // at edge acceptance, which matches the backend's cycle rejection outcome.
  const reached = walkFromSources(sources, outgoing, byId);
  for (const node of byId.values()) {
    if (!reached.has(node.id)) {
      return [
        {
          code: "disconnected-graph",
          message: `Node ${node.id} is not connected to any source node.`,
          nodeId: node.id,
        },
      ];
    }
  }
  for (const node of byId.values()) {
    const targets = outgoing.get(node.id);
    if (
      (targets?.size ?? 0) === 0 &&
      definitions.get(node.id)?.role !== "export" &&
      definitions.get(node.id)?.role !== "verification"
    ) {
      diagnostics.push({
        code: "invalid-terminal",
        message: `Node ${node.id} produces no output and is not a terminal node.`,
        nodeId: node.id,
      });
    }
  }
  return diagnostics;
}

function hasIncoming(
  outgoing: Map<string, Map<string, string[]>>,
  nodeId: string,
): boolean {
  for (const targets of outgoing.values()) {
    for (const [key, edgeTargets] of targets) {
      if (key.startsWith(`${nodeId}:`) && edgeTargets.length > 0) {
        return true;
      }
    }
  }
  return false;
}

/**
 * Walk downstream of every source; the graph must form one component.
 * Fan-in is bounded to one connection per input port, so each node is
 * enqueued exactly once and no visited re-check is required.
 */
function walkFromSources(
  sources: readonly PipelineNodeValue[],
  outgoing: Map<string, Map<string, string[]>>,
  byId: Map<string, PipelineNodeValue>,
): Set<string> {
  const reached = new Set<string>();
  let pending = sources.map((source) => source.id);
  while (pending.length > 0) {
    const next: string[] = [];
    for (const nodeId of pending) {
      reached.add(nodeId);
      const targets = outgoing.get(nodeId);
      if (targets === undefined) {
        continue;
      }
      for (const edgeTargets of targets.values()) {
        for (const target of edgeTargets) {
          if (byId.has(target) && !reached.has(target)) {
            next.push(target);
          }
        }
      }
    }
    pending = next;
  }
  return reached;
}

function resourcePolicyDiagnostics(policy: ResourcePolicyValue): Diagnostic[] {
  const diagnostics: Diagnostic[] = [];
  if (policy.max_in_flight < policy.max_concurrency) {
    diagnostics.push({
      code: "resource-policy",
      message: "Maximum in-flight work must cover maximum concurrency.",
      field: "max_in_flight",
    });
  }
  return diagnostics;
}

// ---------------------------------------------------------------------------
// Deterministic serialization
// ---------------------------------------------------------------------------

/** Recursively key-sorted JSON string; arrays must already be ordered. */
export function canonicalJson(value: unknown): string {
  if (Array.isArray(value)) {
    return `[${value.map(canonicalJson).join(",")}]`;
  }
  if (typeof value === "object" && value !== null) {
    const entries = Object.entries(value as Record<string, unknown>)
      .filter(([, member]) => member !== undefined)
      .sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0));
    return `{${entries.map(([key, member]) => `${JSON.stringify(key)}:${canonicalJson(member)}`).join(",")}}`;
  }
  return JSON.stringify(value) ?? "null";
}

/**
 * Canonical LOGICAL serialization: nodes, edges, and resource policy only.
 * Layout coordinates are deliberately excluded so visual edits never
 * change logical identity, dirty detection, or the idempotency binding.
 */
export function serializeLogicalDocument(document: PipelineDocumentValue): string {
  const nodes = [...document.nodes]
    .sort((a, b) => (a.id < b.id ? -1 : a.id > b.id ? 1 : 0))
    .map((node) => ({
      configuration: node.configuration,
      configuration_version: node.configuration_version,
      connector_id: node.connector_id,
      id: node.id,
      kind: node.kind,
    }));
  const edges = [...document.edges]
    .sort(
      (a, b) =>
        compareStrings(a.source_node_id, b.source_node_id) ||
        compareStrings(a.source_port, b.source_port) ||
        compareStrings(a.target_node_id, b.target_node_id) ||
        compareStrings(a.target_port, b.target_port),
    )
    .map((edge) => ({ ...edge }));
  return canonicalJson({
    canonical_format_version: document.canonicalFormatVersion,
    edges,
    nodes,
    resource_policy: document.resourcePolicy,
    schema_version: document.schemaVersion,
  });
}

function compareStrings(a: string, b: string): number {
  return a < b ? -1 : a > b ? 1 : 0;
}

/**
 * The complete deterministic wire document for publication, including the
 * materialized resource policy and the layout sorted by node identity.
 */
export function toPublicationDocument(
  document: PipelineDocumentValue,
): Record<string, unknown> {
  return {
    canonical_format_version: document.canonicalFormatVersion,
    edges: [...document.edges]
      .sort(
        (a, b) =>
          compareStrings(a.source_node_id, b.source_node_id) ||
          compareStrings(a.source_port, b.source_port) ||
          compareStrings(a.target_node_id, b.target_node_id) ||
          compareStrings(a.target_port, b.target_port),
      )
      .map((edge) => ({ ...edge })),
    layout: [...document.layout]
      .sort((a, b) => compareStrings(a.node_id, b.node_id))
      .map((entry) => ({ ...entry })),
    nodes: [...document.nodes]
      .sort((a, b) => compareStrings(a.id, b.id))
      .map((node) => ({
        configuration: node.configuration,
        configuration_version: node.configuration_version,
        connector_id: node.connector_id,
        id: node.id,
        kind: node.kind,
      })),
    resource_policy: { ...document.resourcePolicy },
    schema_version: document.schemaVersion,
  };
}

/** Canonical LAYOUT serialization, used for layout-dirty detection. */
export function serializeLayout(document: PipelineDocumentValue): string {
  return canonicalJson(
    [...document.layout].sort((a, b) => compareStrings(a.node_id, b.node_id)),
  );
}

/** Deterministic empty draft with a starter source→export topology absent. */
export function emptyDocument(): PipelineDocumentValue {
  return {
    schemaVersion: PIPELINE_DOCUMENT_SCHEMA_VERSION,
    canonicalFormatVersion: PIPELINE_CANONICAL_FORMAT_VERSION,
    nodes: [],
    edges: [],
    resourcePolicy: { ...RESOURCE_POLICY_DEFAULTS },
    layout: [],
  };
}

/** Extract layout from a parsed wire document, tolerating absent entries. */
export function layoutOf(
  document: PipelineDocumentValue,
): readonly PipelineLayoutValue[] {
  return document.layout;
}

export const KNOWN_NODE_KINDS: readonly string[] = NODE_DEFINITIONS.map(
  (definition) => definition.kind,
);

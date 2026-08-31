/**
 * Pure graph-editing operations for the studio: deterministic node identity
 * generation, React Flow change application over the document model, and
 * connection validation with precise, safe rejection reasons. Keeping these
 * pure lets the exact graph rules be tested without driving canvas
 * internals.
 */
import type { Connection, EdgeChange, NodeChange } from "@xyflow/react";

import type {
  PipelineDocumentValue,
  PipelineEdgeValue,
  PipelineNodeValue,
} from "../../pipelines/document";
import { nodeDefinition } from "../../pipelines/registry";

export function nextNodeId(document: PipelineDocumentValue, kind: string): string {
  const stem = `nod_${kind.replace(/\./g, "-")}`;
  const used = new Set(document.nodes.map((node) => node.id));
  for (let sequence = 1; sequence <= 999; sequence += 1) {
    const candidate = `${stem}-${String(sequence).padStart(3, "0")}`;
    if (!used.has(candidate)) {
      return candidate;
    }
  }
  return `${stem}-${String(document.nodes.length + 1).padStart(4, "0")}`;
}

export function gridPosition(document: PipelineDocumentValue): {
  x: number;
  y: number;
} {
  const columns = 4;
  const index = document.nodes.length;
  return { x: 24 + (index % columns) * 220, y: 24 + Math.floor(index / columns) * 150 };
}

export function addNode(
  document: PipelineDocumentValue,
  kind: string,
): { document: PipelineDocumentValue; nodeId: string } {
  const nodeId = nextNodeId(document, kind);
  const position = gridPosition(document);
  const node: PipelineNodeValue = {
    id: nodeId,
    kind,
    configuration_version: 1,
    configuration: {},
    connector_id: null,
  };
  return {
    document: {
      ...document,
      nodes: [...document.nodes, node],
      layout: [...document.layout, { node_id: nodeId, x: position.x, y: position.y }],
    },
    nodeId,
  };
}

export interface NodeChangeResult {
  readonly document: PipelineDocumentValue;
  /** Layout-only positions were updated; the logical document is unchanged. */
  readonly layoutTouched: boolean;
  readonly removedNodeIds: readonly string[];
  readonly selectedNodeId: string | null;
}

export function applyNodeChanges(
  document: PipelineDocumentValue,
  changes: readonly NodeChange[],
): NodeChangeResult {
  let next = document;
  let layoutTouched = false;
  const removedNodeIds: string[] = [];
  let selectedNodeId: string | null = null;

  for (const change of changes) {
    if (change.type === "position") {
      if (change.position !== undefined) {
        next = applyLayout(next, change.id, change.position);
        layoutTouched = true;
      }
    } else if (change.type === "remove") {
      next = {
        ...next,
        nodes: next.nodes.filter((node) => node.id !== change.id),
        edges: next.edges.filter(
          (edge) =>
            edge.source_node_id !== change.id && edge.target_node_id !== change.id,
        ),
        layout: next.layout.filter((entry) => entry.node_id !== change.id),
      };
      removedNodeIds.push(change.id);
    } else if (change.type === "select" && change.selected) {
      selectedNodeId = change.id;
    }
  }

  return { document: next, layoutTouched, removedNodeIds, selectedNodeId };
}

export function applyLayout(
  document: PipelineDocumentValue,
  nodeId: string,
  position: { x: number; y: number },
): PipelineDocumentValue {
  const layout = document.layout.some((entry) => entry.node_id === nodeId)
    ? document.layout.map((entry) =>
        entry.node_id === nodeId
          ? { node_id: nodeId, x: Math.round(position.x), y: Math.round(position.y) }
          : entry,
      )
    : [
        ...document.layout,
        { node_id: nodeId, x: Math.round(position.x), y: Math.round(position.y) },
      ];
  return { ...document, layout };
}

export function applyEdgeChanges(
  document: PipelineDocumentValue,
  changes: readonly EdgeChange[],
): PipelineDocumentValue {
  let next = document;
  for (const change of changes) {
    if (change.type === "remove") {
      const edge = decodeEdgeId(change.id);
      if (edge !== null) {
        next = { ...next, edges: next.edges.filter((entry) => !sameEdge(entry, edge)) };
      }
    }
  }
  return next;
}

export function removeEdgeByIndex(
  document: PipelineDocumentValue,
  index: number,
): PipelineDocumentValue {
  const edge = document.edges[index];
  if (edge === undefined) {
    return document;
  }
  return {
    ...document,
    edges: document.edges.filter((entry) => !sameEdge(entry, edge)),
  };
}

function sameEdge(a: PipelineEdgeValue, b: PipelineEdgeValue): boolean {
  return (
    a.source_node_id === b.source_node_id &&
    a.source_port === b.source_port &&
    a.target_node_id === b.target_node_id &&
    a.target_port === b.target_port
  );
}

function decodeEdgeId(id: string): PipelineEdgeValue | null {
  const [sourcePart, targetPart] = id.split("->");
  const [sourceNode, sourcePort] = (sourcePart ?? ":").split(":");
  const [targetNode, targetPort] = (targetPart ?? ":").split(":");
  if (
    sourceNode === undefined ||
    sourceNode === "" ||
    targetNode === undefined ||
    targetNode === ""
  ) {
    return null;
  }
  return {
    source_node_id: sourceNode,
    source_port: sourcePort ?? "",
    target_node_id: targetNode,
    target_port: targetPort ?? "",
  };
}

export type ConnectionCheck = { ok: true } | { ok: false; reason: string };

/**
 * Exact typed-port validation for a prospective connection, including the
 * bounded fan-in rule. The rejection reason is bounded, safe text.
 */
export function checkConnection(
  document: PipelineDocumentValue,
  connection: Connection,
): ConnectionCheck {
  const sourceNode = document.nodes.find((node) => node.id === connection.source);
  const targetNode = document.nodes.find((node) => node.id === connection.target);
  if (sourceNode === undefined || targetNode === undefined) {
    return { ok: false, reason: "Connection references a node that does not exist." };
  }
  if (sourceNode.id === targetNode.id) {
    return { ok: false, reason: "A node cannot connect to itself." };
  }
  const sourcePort = nodeDefinition(sourceNode.kind)?.ports.outputs.find(
    (port) => port.name === connection.sourceHandle,
  );
  const targetPort = nodeDefinition(targetNode.kind)?.ports.inputs.find(
    (port) => port.name === connection.targetHandle,
  );
  if (sourcePort === undefined || targetPort === undefined) {
    return { ok: false, reason: "Connection uses an undeclared port." };
  }
  if (!targetPort.accepts.includes(sourcePort.valueType)) {
    return {
      ok: false,
      reason: `${sourcePort.valueType} cannot feed ${targetNode.id}.${targetPort.name}.`,
    };
  }
  const occupied = document.edges.some(
    (edge) =>
      edge.target_node_id === connection.target &&
      edge.target_port === connection.targetHandle,
  );
  if (occupied) {
    return { ok: false, reason: "That input already has a connection." };
  }
  return { ok: true };
}

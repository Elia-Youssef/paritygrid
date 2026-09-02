/**
 * Flow wiring for the live run graph, kept out of component files so fast
 * refresh stays component-only (same pattern as the studio canvas).
 */
import type { Edge, Node } from "@xyflow/react";

import type { PipelineDocumentValue } from "../../pipelines/document";
import type { NodeOverlayResult } from "./run-derivations";
import { RunNodeCard } from "./run-graph";

export interface RunNodeFlowData {
  readonly nodeId: string;
  readonly kind: string;
  /** Durable observation rendered verbatim, or a stable not-observed label. */
  readonly stateLabel: string;
  readonly attempts: number | null;
  readonly lastOccurredAt: string | null;
  readonly [key: string]: unknown;
}

export type RunFlowNode = Node<RunNodeFlowData, "run-node">;

export type RunFlowEdge = Edge;

export const runNodeTypes = { "run-node": RunNodeCard };

/** Build read-only flow nodes from the document plus the durable overlay. */
export function runFlowNodes(
  document: PipelineDocumentValue,
  overlay: NodeOverlayResult,
  selectedNodeId: string | null,
): RunFlowNode[] {
  return document.nodes.map((node) => {
    const state = overlay.states.get(node.id);
    const layout = document.layout.find((entry) => entry.node_id === node.id);
    return {
      id: node.id,
      type: "run-node" as const,
      position: { x: layout?.x ?? 0, y: layout?.y ?? 0 },
      selected: node.id === selectedNodeId,
      data: {
        nodeId: node.id,
        kind: node.kind,
        stateLabel: state !== undefined ? state.state : "not observed yet",
        attempts: state?.attempts ?? null,
        lastOccurredAt: state?.lastOccurredAt ?? null,
      },
    };
  });
}

/** Build read-only flow edges from the document's typed edges. */
export function runFlowEdges(document: PipelineDocumentValue): RunFlowEdge[] {
  return document.edges.map((edge, index) => ({
    id: `edge-${String(index)}-${edge.source_node_id}-${edge.target_node_id}`,
    source: edge.source_node_id,
    target: edge.target_node_id,
    sourceHandle: edge.source_port,
    targetHandle: edge.target_port,
    selectable: false,
  }));
}

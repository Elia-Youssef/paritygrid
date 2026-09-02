/**
 * Live run graph components (P17.2). The accepted Phase 16 typed graph
 * model (registry, document, layout) is re-rendered read-only; durable
 * per-node state is overlaid only when the run and pipeline identities and
 * versions agree. A semantic table twin carries the same information for
 * keyboard and screen-reader users — the canvas is never the only evidence.
 */
import { Handle, Position } from "@xyflow/react";
import type { NodeProps } from "@xyflow/react";
import { useMemo } from "react";

import type { PipelineDocumentValue } from "../../pipelines/document";
import { nodeDefinition } from "../../pipelines/registry";
import type { NodeOverlayResult } from "./run-derivations";
import type { RunFlowNode } from "./run-flow";

/** Read-only card for one pipeline node under the durable overlay. */
export function RunNodeCard({ data, selected }: NodeProps<RunFlowNode>) {
  const definition = nodeDefinition(data.kind);
  const inputs = definition?.ports.inputs ?? [];
  const outputs = definition?.ports.outputs ?? [];
  return (
    <div
      className={`w-48 rounded-lg border bg-surface px-3 py-2 shadow-elevated ${
        selected ? "border-active" : "border-border"
      }`}
      aria-label={`Node ${data.nodeId}, ${data.kind}, durable state ${data.stateLabel}`}
    >
      {inputs.map((port) => (
        <Handle
          key={`in-${port.name}`}
          id={port.name}
          type="target"
          position={Position.Left}
          isConnectable={false}
          aria-label={`Input port ${port.name}`}
        />
      ))}
      <p className="text-2xs uppercase tracking-wide text-muted">{data.kind}</p>
      <p className="truncate font-mono text-xs text-foreground">{data.nodeId}</p>
      <p
        className="mt-1 text-2xs text-foreground"
        data-testid={`run-node-state-${data.nodeId}`}
      >
        <span aria-hidden="true" className="mr-1 font-mono">
          ◉
        </span>
        {data.stateLabel}
        {data.attempts !== null ? `, attempt ${String(data.attempts)}` : ""}
      </p>
      {data.lastOccurredAt !== null && (
        <p className="font-mono text-2xs text-muted">{data.lastOccurredAt}</p>
      )}
      {outputs.map((port) => (
        <Handle
          key={`out-${port.name}`}
          id={port.name}
          type="source"
          position={Position.Right}
          isConnectable={false}
          aria-label={`Output port ${port.name}`}
        />
      ))}
    </div>
  );
}

export interface RunStructureTableProps {
  readonly document: PipelineDocumentValue;
  readonly overlay: NodeOverlayResult;
  readonly selectedNodeId: string | null;
  readonly onSelect: (nodeId: string) => void;
}

/**
 * The accessible twin of the live graph: the same nodes, kinds, durable
 * states, attempts, and last durable sequence as a plain table. Selection
 * is keyboard-operable through its buttons.
 */
export function RunStructureTable({
  document,
  overlay,
  selectedNodeId,
  onSelect,
}: RunStructureTableProps) {
  const edges = useMemo(() => document.edges, [document]);
  return (
    <div className="overflow-x-auto">
      <table
        className="w-full text-left text-xs"
        aria-label="Pipeline structure with durable state"
      >
        <caption className="sr-only">
          Nodes with their durable execution state; the same facts the graph canvas
          renders.
        </caption>
        <thead>
          <tr className="border-b border-border text-muted">
            <th scope="col" className="px-4 py-1.5 font-medium">
              Node
            </th>
            <th scope="col" className="px-4 py-1.5 font-medium">
              Kind
            </th>
            <th scope="col" className="px-4 py-1.5 font-medium">
              Durable state
            </th>
            <th scope="col" className="px-4 py-1.5 font-medium">
              Attempt
            </th>
            <th scope="col" className="px-4 py-1.5 font-medium">
              Last event
            </th>
            <th scope="col" className="px-4 py-1.5 font-medium">
              Observed
            </th>
            <th scope="col" className="px-4 py-1.5 font-medium">
              <span className="sr-only">Select</span>
            </th>
          </tr>
        </thead>
        <tbody>
          {document.nodes.map((node) => {
            const state = overlay.states.get(node.id);
            const selected = node.id === selectedNodeId;
            return (
              <tr
                key={node.id}
                aria-selected={selected}
                data-testid={`structure-row-${node.id}`}
                className={`border-b border-border/60 last:border-b-0 ${
                  selected ? "bg-active/10" : ""
                }`}
              >
                <td className="px-4 py-1.5 font-mono text-foreground">{node.id}</td>
                <td className="px-4 py-1.5 font-mono text-muted">{node.kind}</td>
                <td className="px-4 py-1.5 text-foreground">
                  {state !== undefined ? state.state : "not observed yet"}
                </td>
                <td className="px-4 py-1.5 font-mono text-muted">
                  {state?.attempts !== undefined && state.attempts !== null
                    ? String(state.attempts)
                    : "unavailable"}
                </td>
                <td className="px-4 py-1.5 font-mono text-muted">
                  {state !== undefined ? String(state.lastSequence) : "unavailable"}
                </td>
                <td className="px-4 py-1.5 font-mono text-muted">
                  {state !== undefined ? state.lastOccurredAt : "unavailable"}
                </td>
                <td className="px-4 py-1.5 text-right">
                  <button
                    type="button"
                    aria-pressed={selected}
                    className="rounded border border-border px-2 py-1 text-2xs text-foreground"
                    onClick={() => {
                      onSelect(node.id);
                    }}
                  >
                    {selected ? "Deselect" : "Select"}
                  </button>
                </td>
              </tr>
            );
          })}
        </tbody>
      </table>
      <table className="mt-3 w-full text-left text-xs" aria-label="Pipeline edges">
        <caption className="sr-only">Typed connections between nodes.</caption>
        <thead>
          <tr className="border-b border-border text-muted">
            <th scope="col" className="px-4 py-1.5 font-medium">
              From
            </th>
            <th scope="col" className="px-4 py-1.5 font-medium">
              Port
            </th>
            <th scope="col" className="px-4 py-1.5 font-medium">
              To
            </th>
            <th scope="col" className="px-4 py-1.5 font-medium">
              Port
            </th>
          </tr>
        </thead>
        <tbody>
          {edges.map((edge, index) => (
            <tr key={index} className="border-b border-border/60 last:border-b-0">
              <td className="px-4 py-1.5 font-mono text-foreground">
                {edge.source_node_id}
              </td>
              <td className="px-4 py-1.5 font-mono text-muted">{edge.source_port}</td>
              <td className="px-4 py-1.5 font-mono text-foreground">
                {edge.target_node_id}
              </td>
              <td className="px-4 py-1.5 font-mono text-muted">{edge.target_port}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

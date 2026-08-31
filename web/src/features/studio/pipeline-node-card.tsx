/**
 * Owned typed React Flow node set. Each canvas card communicates the node
 * kind, its stable identity, configuration validity, connector binding,
 * its declared ports, and its runner/retry constraints. No live execution
 * state exists here: live observability belongs to Phase 17.
 */
import { Handle, Position, type NodeProps } from "@xyflow/react";

import { nodeDefinition } from "../../pipelines/registry";
import type { PipelineFlowNode } from "./studio-flow";

function portHandles(kind: string): {
  inputs: readonly string[];
  outputs: readonly string[];
} {
  const definition = nodeDefinition(kind);
  return {
    inputs: (definition?.ports.inputs ?? []).map((port) => port.name),
    outputs: (definition?.ports.outputs ?? []).map((port) => port.name),
  };
}

export function PipelineNodeCard({ data, selected }: NodeProps<PipelineFlowNode>) {
  const { inputs, outputs } = portHandles(data.kind);
  const invalid = !data.configurationValid || data.errorCount > 0;
  return (
    <div
      className={`w-48 rounded-lg border bg-surface px-3 py-2 shadow-elevated ${
        selected ? "border-active" : invalid ? "border-failure" : "border-border"
      }`}
      aria-label={`Node ${data.nodeId}, ${data.kind}${invalid ? ", has validation errors" : ""}`}
    >
      {inputs.map((port) => (
        <Handle
          key={`in-${port}`}
          id={port}
          type="target"
          position={Position.Left}
          aria-label={`Input port ${port}`}
        />
      ))}
      <p className="text-2xs uppercase tracking-wide text-muted">{data.kind}</p>
      <p className="truncate font-mono text-xs text-foreground">{data.nodeId}</p>
      <p className="mt-1 truncate text-2xs text-muted">
        {data.connectorId === null
          ? "No connector"
          : `Connector: ${data.connectorLabel ?? data.connectorId}`}
      </p>
      <p className="mt-0.5 flex items-center gap-1 text-2xs">
        {invalid ? (
          <>
            <span aria-hidden="true" className="text-failure">
              ✗
            </span>
            <span className="text-failure">
              {String(data.errorCount)} validation{" "}
              {data.errorCount === 1 ? "error" : "errors"}
            </span>
          </>
        ) : (
          <>
            <span aria-hidden="true" className="text-verified">
              ✓
            </span>
            <span className="text-muted">Valid</span>
          </>
        )}
        <span className="ml-auto font-mono text-2xs text-muted">
          {data.runnerLabel}
        </span>
      </p>
      {outputs.map((port) => (
        <Handle
          key={`out-${port}`}
          id={port}
          type="source"
          position={Position.Right}
          aria-label={`Output port ${port}`}
        />
      ))}
    </div>
  );
}

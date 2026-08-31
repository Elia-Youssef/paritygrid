/**
 * Semantic, keyboard-operable alternative to the canvas graph: a table of
 * nodes (with ports, validation state, and actions) and a table of
 * connections (with delete actions). Every pointer interaction available
 * on the canvas — add, select, configure, delete, connect — has a
 * keyboard-first equivalent here, and validation errors navigate to the
 * affected node and field.
 */
import { Fragment, useState } from "react";

import { nodeDefinition } from "../../pipelines/registry";
import type { Diagnostic, PipelineDocumentValue } from "../../pipelines/document";

export interface GraphTableProps {
  document: PipelineDocumentValue;
  diagnostics: readonly Diagnostic[];
  selection: string | null;
  onSelect: (nodeId: string | null) => void;
  onConfigure: (nodeId: string) => void;
  onDelete: (nodeId: string) => void;
  onConnect: (
    sourceNodeId: string,
    sourcePort: string,
    targetNodeId: string,
    targetPort: string,
  ) => void;
  onDeleteEdge: (index: number) => void;
  /** Focus request from the validation summary navigation. */
  errorTarget: { nodeId: string; field?: string } | null;
}

function errorsFor(
  diagnostics: readonly Diagnostic[],
  nodeId: string,
): readonly Diagnostic[] {
  return diagnostics.filter((diagnostic) => diagnostic.nodeId === nodeId);
}

export function GraphStructureTable(props: GraphTableProps) {
  const {
    document,
    diagnostics,
    selection,
    onSelect,
    onConfigure,
    onDelete,
    onConnect,
    onDeleteEdge,
    errorTarget,
  } = props;

  return (
    <section aria-label="Pipeline structure" className="border-t border-border">
      <h3 className="px-4 pt-3 text-sm font-semibold text-foreground">
        Pipeline structure
      </h3>
      <div className="grid gap-4 p-4 lg:grid-cols-2">
        <table className="w-full border-collapse text-left text-xs" aria-label="Nodes">
          <caption className="sr-only">
            Pipeline nodes with ports, validation state, and actions
          </caption>
          <thead>
            <tr className="border-b border-border text-muted">
              <th scope="col" className="py-1.5 pr-2 font-medium">
                Node
              </th>
              <th scope="col" className="py-1.5 pr-2 font-medium">
                Kind
              </th>
              <th scope="col" className="py-1.5 pr-2 font-medium">
                Ports
              </th>
              <th scope="col" className="py-1.5 pr-2 font-medium">
                Validation
              </th>
              <th scope="col" className="py-1.5 font-medium">
                Actions
              </th>
            </tr>
          </thead>
          <tbody>
            {document.nodes.length === 0 && (
              <tr>
                <td colSpan={5} className="py-3 text-muted">
                  No nodes yet. Add a source node to begin.
                </td>
              </tr>
            )}
            {document.nodes.map((node) => {
              const definition = nodeDefinition(node.kind);
              const nodeErrors = errorsFor(diagnostics, node.id);
              const selectedNode = selection === node.id;
              return (
                <Fragment key={node.id}>
                  <tr
                    className={`border-b border-border/60 ${selectedNode ? "bg-active/10" : ""}`}
                    aria-selected={selectedNode}
                  >
                    <td className="py-1.5 pr-2 font-mono text-foreground">
                      {node.id}
                      {errorTarget?.nodeId === node.id && (
                        <span className="sr-only"> (has validation errors)</span>
                      )}
                    </td>
                    <td className="py-1.5 pr-2 text-muted">{node.kind}</td>
                    <td className="py-1.5 pr-2 text-muted">
                      <span className="block">
                        in:{" "}
                        {(definition?.ports.inputs ?? [])
                          .map((port) => port.name)
                          .join(", ") || "—"}
                      </span>
                      <span className="block">
                        out:{" "}
                        {(definition?.ports.outputs ?? [])
                          .map((port) => port.name)
                          .join(", ") || "—"}
                      </span>
                    </td>
                    <td className="py-1.5 pr-2">
                      {nodeErrors.length === 0 ? (
                        <span className="text-verified">✓ valid</span>
                      ) : (
                        <span className="text-failure">
                          ✗ {String(nodeErrors.length)} error(s)
                        </span>
                      )}
                    </td>
                    <td className="py-1.5">
                      <div className="flex gap-1">
                        <button
                          type="button"
                          className="rounded border border-border px-2 py-0.5 text-2xs text-foreground hover:bg-surface-elevated"
                          onClick={() => {
                            onSelect(node.id);
                            onConfigure(node.id);
                          }}
                          ref={(element) => {
                            if (
                              element !== null &&
                              errorTarget?.nodeId === node.id &&
                              selectedNode
                            ) {
                              element.focus();
                            }
                          }}
                        >
                          Configure
                        </button>
                        <button
                          type="button"
                          className="rounded border border-border px-2 py-0.5 text-2xs text-foreground hover:bg-surface-elevated"
                          onClick={() => {
                            onSelect(node.id);
                          }}
                        >
                          Select
                        </button>
                        <button
                          type="button"
                          className="rounded border border-border px-2 py-0.5 text-2xs text-failure hover:bg-surface-elevated"
                          onClick={() => {
                            onDelete(node.id);
                          }}
                        >
                          Delete
                        </button>
                      </div>
                    </td>
                  </tr>
                  {nodeErrors.map((diagnostic, index) => (
                    <tr
                      key={`${node.id}-error-${String(index)}`}
                      className="border-b border-border/60"
                    >
                      <td colSpan={5} className="py-1 pl-6 text-2xs text-failure">
                        {diagnostic.message}
                        {diagnostic.field !== undefined && (
                          <>
                            {" "}
                            <button
                              type="button"
                              className="underline hover:text-foreground"
                              onClick={() => {
                                onSelect(node.id);
                                onConfigure(node.id);
                              }}
                            >
                              Fix field {diagnostic.field}
                            </button>
                          </>
                        )}
                      </td>
                    </tr>
                  ))}
                </Fragment>
              );
            })}
          </tbody>
        </table>

        <div>
          <table
            className="w-full border-collapse text-left text-xs"
            aria-label="Connections"
          >
            <caption className="sr-only">Pipeline connections</caption>
            <thead>
              <tr className="border-b border-border text-muted">
                <th scope="col" className="py-1.5 pr-2 font-medium">
                  Source
                </th>
                <th scope="col" className="py-1.5 pr-2 font-medium">
                  Target
                </th>
                <th scope="col" className="py-1.5 font-medium">
                  Actions
                </th>
              </tr>
            </thead>
            <tbody>
              {document.edges.length === 0 && (
                <tr>
                  <td colSpan={3} className="py-3 text-muted">
                    No connections yet.
                  </td>
                </tr>
              )}
              {document.edges.map((edge, index) => (
                <tr
                  key={`${edge.source_node_id}:${edge.source_port}>${edge.target_node_id}:${edge.target_port}`}
                  className="border-b border-border/60"
                >
                  <td className="py-1.5 pr-2 font-mono text-foreground">
                    {edge.source_node_id}.{edge.source_port}
                  </td>
                  <td className="py-1.5 pr-2 font-mono text-foreground">
                    {edge.target_node_id}.{edge.target_port}
                  </td>
                  <td className="py-1.5">
                    <button
                      type="button"
                      className="rounded border border-border px-2 py-0.5 text-2xs text-failure hover:bg-surface-elevated"
                      onClick={() => {
                        onDeleteEdge(index);
                      }}
                    >
                      Delete connection
                    </button>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>

          <ConnectionCreator document={document} onConnect={onConnect} />
        </div>
      </div>
    </section>
  );
}

function ConnectionCreator({
  document,
  onConnect,
}: {
  document: PipelineDocumentValue;
  onConnect: GraphTableProps["onConnect"];
}): React.JSX.Element {
  const [source, setSource] = useState("");
  const [target, setTarget] = useState("");

  const sourceNode = document.nodes.find((node) => node.id === source);
  const targetNode = document.nodes.find((node) => node.id === target);
  const sourceDefinition =
    sourceNode === undefined ? undefined : nodeDefinition(sourceNode.kind);
  const targetDefinition =
    targetNode === undefined ? undefined : nodeDefinition(targetNode.kind);

  return (
    <form
      className="mt-3 rounded-md border border-border p-3"
      aria-label="Create connection"
      onSubmit={(event) => {
        event.preventDefault();
        const sourcePort = sourceDefinition?.ports.outputs[0]?.name;
        const targetPort = targetDefinition?.ports.inputs[0]?.name;
        if (sourcePort === undefined || targetPort === undefined) {
          return;
        }
        onConnect(source, sourcePort, target, targetPort);
      }}
    >
      <p className="text-2xs font-medium uppercase tracking-wide text-muted">
        Connect nodes
      </p>
      <div className="mt-2 flex flex-wrap items-center gap-2">
        <label className="text-2xs text-muted" htmlFor="connect-source">
          From
        </label>
        <select
          id="connect-source"
          className="rounded border border-border bg-surface px-2 py-1 text-xs text-foreground"
          value={source}
          onChange={(event) => {
            setSource(event.target.value);
          }}
        >
          <option value="">Select node…</option>
          {document.nodes.map((node) => (
            <option key={node.id} value={node.id}>
              {node.id}
            </option>
          ))}
        </select>
        <label className="text-2xs text-muted" htmlFor="connect-target">
          To
        </label>
        <select
          id="connect-target"
          className="rounded border border-border bg-surface px-2 py-1 text-xs text-foreground"
          value={target}
          onChange={(event) => {
            setTarget(event.target.value);
          }}
        >
          <option value="">Select node…</option>
          {document.nodes.map((node) => (
            <option key={node.id} value={node.id}>
              {node.id}
            </option>
          ))}
        </select>
        <button
          type="submit"
          className="rounded bg-primary px-2 py-1 text-2xs font-medium text-primary-foreground disabled:cursor-not-allowed disabled:opacity-50"
          disabled={source === "" || target === ""}
        >
          Connect
        </button>
      </div>
    </form>
  );
}

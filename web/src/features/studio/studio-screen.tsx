/**
 * Pipeline studio: typed canvas, semantic structure table, registry-driven
 * inspector, plan preview, and versioned publication.
 *
 * Server state flows through TanStack Query only; the draft is locally
 * owned and never overwritten by later responses. Route-blocking prevents
 * accidental draft loss, and every canvas action has a keyboard equivalent.
 */
import {
  Background,
  MiniMap,
  ReactFlow,
  ReactFlowProvider,
  useReactFlow,
  type Connection,
  type EdgeChange,
  type NodeChange,
} from "@xyflow/react";
import { useBlocker } from "react-router";
import { useQuery, useQueryClient } from "@tanstack/react-query";
import { useCallback, useMemo, useState } from "react";

import {
  fetchConnectors,
  fetchPipeline,
  fetchPipelineVersion,
  fetchPipelineVersionFrontier,
} from "../../api/client";
import { queryKeys } from "../../api/query-keys";
import {
  parsePipelineDocument,
  type ConnectorSummary,
  type PipelineDocumentValue,
} from "../../pipelines/document";
import { nodeDefinition } from "../../pipelines/registry";
import { ErrorState } from "../../components/states/error-state";
import { Loading } from "../../components/states/loading";
import { pipelineNodeTypes, type PipelineFlowNode } from "./studio-flow";
import {
  addNode as addNodeToDocument,
  applyEdgeChanges,
  applyNodeChanges,
  checkConnection,
  removeEdgeByIndex,
} from "./graph-changes";
import { GraphStructureTable } from "./graph-table";
import { Inspector } from "./inspector";
import { PublicationPanel } from "./publication-panel";
import { acknowledgementMatches, useStudioState } from "./studio-state";

export function StudioScreen({ pipelineId }: { pipelineId: string }) {
  return (
    <ReactFlowProvider>
      <Studio pipelineId={pipelineId} />
    </ReactFlowProvider>
  );
}

function Studio({ pipelineId }: { pipelineId: string }) {
  const queryClient = useQueryClient();
  const flow = useReactFlow();
  const [connectionError, setConnectionError] = useState<string | null>(null);

  const detail = useQuery({
    queryKey: queryKeys.pipelineDetail(pipelineId),
    queryFn: ({ signal }) => fetchPipeline(pipelineId, { signal }),
    retry: 0,
  });
  const frontier = useQuery({
    queryKey: queryKeys.pipelineFrontier(pipelineId),
    queryFn: ({ signal }) => fetchPipelineVersionFrontier(pipelineId, { signal }),
    retry: 0,
  });
  const publishedVersion = useQuery({
    queryKey: queryKeys.pipelineVersion(pipelineId, frontier.data?.latest_version ?? 0),
    queryFn: ({ signal }) =>
      fetchPipelineVersion(pipelineId, frontier.data?.latest_version ?? 0, { signal }),
    enabled: frontier.data?.published === true,
    retry: 0,
  });
  const connectors = useQuery({
    queryKey: queryKeys.connectors({ limit: 100 }),
    queryFn: ({ signal }) => fetchConnectors({ limit: 100 }, { signal }),
    staleTime: 60_000,
    retry: 0,
  });

  const initialDocument = useMemo<PipelineDocumentValue | null>(() => {
    if (publishedVersion.data === undefined) {
      return null;
    }
    const parsed = parsePipelineDocument(publishedVersion.data.specification.pipeline);
    return parsed.ok ? parsed.value : null;
  }, [publishedVersion.data]);

  const initialReady =
    !detail.isPending &&
    !frontier.isPending &&
    (frontier.data?.published !== true || publishedVersion.isSuccess);

  const connectorSummaries = useMemo<readonly ConnectorSummary[]>(
    () =>
      (connectors.data?.items ?? []).map((connector) => ({
        connector_id: connector.connector_id,
        kind: connector.kind,
        display_name: connector.display_name,
        archived_at: connector.archived_at,
        capabilities: connector.capabilities,
      })),
    [connectors.data],
  );

  const studio = useStudioState({
    pipelineId,
    connectors: connectorSummaries,
    initialDocument,
    initialReady,
  });

  const blocked = useBlocker(
    studio.draft !== null && (studio.logicalDirty || studio.layoutDirty),
  );

  const documentValue = studio.draft?.document ?? null;

  const connectAllowed = useCallback(
    (connection: Connection): boolean =>
      documentValue === null ? false : checkConnection(documentValue, connection).ok,
    [documentValue],
  );

  const onConnect = (connection: Connection): void => {
    if (documentValue === null) {
      return;
    }
    const check = checkConnection(documentValue, connection);
    if (!check.ok) {
      setConnectionError(check.reason);
      return;
    }
    setConnectionError(null);
    studio.edit((current) => ({
      ...current,
      edges: [
        ...current.edges,
        {
          source_node_id: connection.source,
          source_port: connection.sourceHandle ?? "",
          target_node_id: connection.target,
          target_port: connection.targetHandle ?? "",
        },
      ],
    }));
  };

  const removeNode = (nodeId: string): void => {
    studio.edit((current) => ({
      ...current,
      nodes: current.nodes.filter((node) => node.id !== nodeId),
      edges: current.edges.filter(
        (edge) => edge.source_node_id !== nodeId && edge.target_node_id !== nodeId,
      ),
      layout: current.layout.filter((entry) => entry.node_id !== nodeId),
    }));
    if (studio.selection === nodeId) {
      studio.select(null);
    }
  };

  const onNodesChange = (changes: readonly NodeChange[]): void => {
    if (studio.draft === null) {
      return;
    }
    const result = applyNodeChanges(studio.draft.document, changes);
    if (result.layoutTouched) {
      studio.edit((current) => ({ ...current, layout: result.document.layout }));
    }
    for (const removed of result.removedNodeIds) {
      removeNode(removed);
    }
    if (result.selectedNodeId !== null) {
      studio.select(result.selectedNodeId);
    }
  };

  const onEdgesChange = (changes: readonly EdgeChange[]): void => {
    studio.edit((current) => applyEdgeChanges(current, changes));
  };

  const addNode = (kind: string): void => {
    let createdNodeId: string | null = null;
    studio.edit((current) => {
      const result = addNodeToDocument(current, kind);
      createdNodeId = result.nodeId;
      return result.document;
    });
    if (createdNodeId !== null) {
      studio.select(createdNodeId);
    }
  };

  if (detail.isError) {
    return (
      <div className="p-6">
        <ErrorState
          title="Pipeline unavailable"
          problem={{ title: "Pipeline unavailable", extensions: [] }}
          action={
            <button
              type="button"
              className="rounded border border-border px-3 py-1.5 text-xs text-foreground"
              onClick={() => {
                void detail.refetch();
              }}
            >
              Retry
            </button>
          }
        />
      </div>
    );
  }

  if (frontier.isError) {
    return (
      <div className="p-6">
        <ErrorState
          title="Version frontier unavailable"
          problem={{ title: "Version frontier unavailable", extensions: [] }}
          action={
            <button
              type="button"
              className="rounded border border-border px-3 py-1.5 text-xs text-foreground"
              onClick={() => {
                void frontier.refetch();
              }}
            >
              Retry
            </button>
          }
        />
      </div>
    );
  }

  if (
    frontier.data?.published === true &&
    publishedVersion.isError &&
    studio.draft === null
  ) {
    return (
      <div className="p-6">
        <ErrorState
          title="Published pipeline version unavailable"
          description="The latest immutable pipeline document could not be loaded."
          action={
            <button
              type="button"
              className="rounded border border-border px-3 py-1.5 text-xs text-foreground"
              onClick={() => {
                void publishedVersion.refetch();
              }}
            >
              Retry
            </button>
          }
        />
      </div>
    );
  }

  if (
    frontier.data?.published === true &&
    publishedVersion.isSuccess &&
    initialDocument === null &&
    studio.draft === null
  ) {
    return (
      <div className="p-6">
        <ErrorState
          title="Published pipeline document is invalid"
          description="The immutable document did not satisfy the supported pipeline schema."
        />
      </div>
    );
  }

  if (connectors.isError && connectors.data === undefined) {
    return (
      <div className="p-6">
        <ErrorState
          title="Connector inventory unavailable"
          description="Connector bindings and capabilities could not be loaded safely."
          action={
            <button
              type="button"
              className="rounded border border-border px-3 py-1.5 text-xs text-foreground"
              onClick={() => {
                void connectors.refetch();
              }}
            >
              Retry
            </button>
          }
        />
      </div>
    );
  }

  if (
    detail.isPending ||
    frontier.isPending ||
    connectors.isPending ||
    documentValue === null
  ) {
    return (
      <div className="p-6">
        <Loading label="Loading pipeline studio" />
      </div>
    );
  }

  const flowNodes: PipelineFlowNode[] = documentValue.nodes.map((node) => {
    const definition = nodeDefinition(node.kind);
    const layoutEntry = documentValue.layout.find((entry) => entry.node_id === node.id);
    const errors = studio.diagnostics.filter(
      (diagnostic) => diagnostic.nodeId === node.id,
    );
    const connector = connectorSummaries.find(
      (entry) => entry.connector_id === node.connector_id,
    );
    return {
      id: node.id,
      type: "pipeline-node" as const,
      position: { x: layoutEntry?.x ?? 24, y: layoutEntry?.y ?? 24 },
      selected: studio.selection === node.id,
      data: {
        nodeId: node.id,
        kind: node.kind,
        configurationValid: errors.length === 0,
        errorCount: errors.length,
        connectorId: node.connector_id,
        connectorLabel: connector?.display_name ?? null,
        runnerLabel: (definition?.supportedRunners ?? []).join("+"),
        retryLabel: definition?.retryBehavior ?? "never",
      },
    };
  });

  const flowEdges = documentValue.edges.map((edge) => ({
    id: `${edge.source_node_id}:${edge.source_port}->${edge.target_node_id}:${edge.target_port}`,
    source: edge.source_node_id,
    sourceHandle: edge.source_port,
    target: edge.target_node_id,
    targetHandle: edge.target_port,
    label: edge.target_port,
  }));

  const selectedNode =
    documentValue.nodes.find((node) => node.id === studio.selection) ?? null;

  return (
    <div className="flex min-h-0 flex-1 flex-col" data-testid="studio-root">
      <div className="flex flex-wrap items-center gap-2 border-b border-border px-4 py-2">
        <h2 className="mr-2 text-sm font-semibold text-foreground">
          {detail.data.display_name}
          <span className="ml-2 font-mono text-2xs text-muted">{pipelineId}</span>
        </h2>
        <label htmlFor="add-node-kind" className="text-2xs text-muted">
          Add node
        </label>
        <AddNodeMenu onAdd={addNode} />
        <button
          type="button"
          className="rounded border border-border px-2 py-1 text-2xs text-foreground hover:bg-surface-elevated"
          onClick={() => {
            void flow.fitView({ duration: 0 });
          }}
        >
          Fit view
        </button>
        <button
          type="button"
          className="rounded border border-border px-2 py-1 text-2xs text-foreground hover:bg-surface-elevated"
          onClick={() => {
            void flow.zoomIn({ duration: 0 });
          }}
        >
          Zoom in
        </button>
        <button
          type="button"
          className="rounded border border-border px-2 py-1 text-2xs text-foreground hover:bg-surface-elevated"
          onClick={() => {
            void flow.zoomOut({ duration: 0 });
          }}
        >
          Zoom out
        </button>
        <button
          type="button"
          className="rounded border border-border px-2 py-1 text-2xs text-failure hover:bg-surface-elevated"
          disabled={studio.selection === null}
          onClick={() => {
            if (studio.selection !== null) {
              removeNode(studio.selection);
            }
          }}
        >
          Delete selected
        </button>
        <span className="ml-auto flex items-center gap-2 text-2xs" aria-live="polite">
          <span className={studio.logicalDirty ? "text-warning" : "text-muted"}>
            {studio.logicalDirty ? "● unsaved content changes" : "content saved"}
          </span>
          <span className={studio.layoutDirty ? "text-warning" : "text-muted"}>
            {studio.layoutDirty ? "● unsaved layout" : "layout saved"}
          </span>
        </span>
      </div>

      {blocked.state === "blocked" && (
        <div
          role="alertdialog"
          aria-label="Unsaved changes"
          className="border-b border-warning/40 bg-warning/10 p-3 text-xs text-warning"
          data-testid="draft-guard"
        >
          <p className="font-medium">
            This pipeline has unsaved changes. Leaving now discards the draft.
          </p>
          <div className="mt-2 flex gap-2">
            <button
              type="button"
              className="rounded border border-border px-2 py-1 text-2xs text-foreground"
              onClick={() => {
                blocked.reset();
              }}
            >
              Stay and keep editing
            </button>
            <button
              type="button"
              className="rounded border border-border px-2 py-1 text-2xs text-failure"
              onClick={() => {
                blocked.proceed();
              }}
            >
              Discard draft and leave
            </button>
          </div>
        </div>
      )}

      {connectionError !== null && (
        <p
          role="alert"
          className="border-b border-failure/40 bg-failure/10 px-4 py-1.5 text-2xs text-failure"
          data-testid="connection-error"
        >
          Connection rejected: {connectionError}
        </p>
      )}

      <div className="flex min-h-0 flex-1 flex-col xl:flex-row">
        <div className="flex min-h-0 flex-col">
          <div className="h-[420px] border-b border-border" data-testid="studio-canvas">
            <ReactFlow
              nodes={flowNodes}
              edges={flowEdges}
              nodeTypes={pipelineNodeTypes}
              onNodesChange={onNodesChange}
              onEdgesChange={onEdgesChange}
              onConnect={onConnect}
              isValidConnection={connectAllowed}
              deleteKeyCode={["Backspace", "Delete"]}
              fitView
            >
              <Background />
              <MiniMap pannable zoomable />
            </ReactFlow>
          </div>
          <GraphStructureTable
            document={documentValue}
            diagnostics={studio.diagnostics}
            selection={studio.selection}
            errorTarget={null}
            onSelect={studio.select}
            onConfigure={studio.select}
            onDelete={removeNode}
            onConnect={(sourceNodeId, sourcePort, targetNodeId, targetPort) => {
              onConnect({
                source: sourceNodeId,
                sourceHandle: sourcePort,
                target: targetNodeId,
                targetHandle: targetPort,
              });
            }}
            onDeleteEdge={(index) => {
              studio.edit((current) => removeEdgeByIndex(current, index));
            }}
          />
        </div>
        <Inspector
          node={selectedNode}
          diagnostics={studio.diagnostics}
          connectors={connectorSummaries}
          focusField={null}
          onClose={() => {
            studio.select(null);
          }}
          onConfigure={(nodeId, field, value) => {
            studio.edit((current) => ({
              ...current,
              nodes: current.nodes.map((node) => {
                if (node.id !== nodeId) {
                  return node;
                }
                const configuration = { ...node.configuration };
                if (field !== null && (value === "" || value === null)) {
                  delete configuration[field];
                } else if (field !== null) {
                  configuration[field] = value;
                }
                return { ...node, configuration };
              }),
            }));
          }}
          onSetConnector={(nodeId, connectorId) => {
            studio.edit((current) => ({
              ...current,
              nodes: current.nodes.map((node) =>
                node.id === nodeId ? { ...node, connector_id: connectorId } : node,
              ),
            }));
          }}
        />
      </div>

      <PublicationPanel
        pipelineId={pipelineId}
        pipelineName={detail.data.display_name}
        document={documentValue}
        diagnostics={studio.diagnostics}
        frontier={frontier.data ?? null}
        command={studio.command}
        publication={studio.publication}
        beginCommand={studio.beginCommand}
        onPublished={(ack) => {
          const accepted =
            studio.command !== null &&
            acknowledgementMatches(pipelineId, studio.command, ack);
          studio.markPublished(ack);
          if (!accepted) {
            return;
          }
          queryClient.setQueryData(queryKeys.pipelineFrontier(pipelineId), {
            schema_version: 1,
            pipeline_id: pipelineId,
            latest_version: ack.version,
            published: true,
          });
          void queryClient.invalidateQueries({
            queryKey: queryKeys.pipelinesRoot(),
          });
        }}
        onConflict={studio.markConflict}
        onFailed={studio.markFailed}
        onRefreshFrontier={async () => {
          const result = await frontier.refetch();
          if (!result.isSuccess) {
            throw result.error instanceof Error
              ? result.error
              : new Error("version frontier refresh failed");
          }
          studio.clearCommand();
        }}
      />
    </div>
  );
}

function AddNodeMenu({ onAdd }: { onAdd: (kind: string) => void }): React.JSX.Element {
  const [kind, setKind] = useState("source.csv");
  return (
    <span className="flex items-center gap-1">
      <select
        id="add-node-kind"
        className="rounded border border-border bg-surface px-2 py-1 text-xs text-foreground"
        value={kind}
        onChange={(event) => {
          setKind(event.target.value);
        }}
      >
        {NODE_KIND_OPTIONS.map((option) => (
          <option key={option.kind} value={option.kind}>
            {option.label}
          </option>
        ))}
      </select>
      <button
        type="button"
        className="rounded bg-primary px-2 py-1 text-2xs font-medium text-primary-foreground"
        data-testid="add-node-button"
        onClick={() => {
          onAdd(kind);
        }}
      >
        Add
      </button>
    </span>
  );
}

const NODE_KIND_OPTIONS = [
  { kind: "source.csv", label: "CSV source" },
  { kind: "source.http.async", label: "Async HTTP source" },
  { kind: "source.http.blocking", label: "Blocking HTTP source" },
  { kind: "source.jsonl", label: "JSON Lines source" },
  { kind: "transform.normalize", label: "Normalize records" },
  { kind: "transform.validate", label: "Validate records" },
  { kind: "transform.partition", label: "Partition records" },
  { kind: "reconcile.target", label: "Reconcile with target" },
  { kind: "repair.generate", label: "Generate repair plan" },
  { kind: "repair.approval", label: "Approval gate" },
  { kind: "repair.apply", label: "Apply repairs" },
  { kind: "verify.target", label: "Verify target state" },
  { kind: "export.parquet", label: "Export Parquet" },
];

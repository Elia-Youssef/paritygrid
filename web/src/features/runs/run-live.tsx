/**
 * Live run view (P17.2, P17.3, P17.4, P17.5). Durable REST/SSE state is the
 * only authority for run facts and the graph overlay; the advisory
 * WebSocket telemetry enriches the queue panels and is labeled, bounded,
 * and never allowed to advance durable state. The last coherent durable
 * view stays on screen during recovery under an explicit banner.
 */
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { useMemo, useState } from "react";
import { Background, MiniMap, ReactFlow } from "@xyflow/react";

import {
  controlRun,
  fetchPipelineVersion,
  isApiRequestError,
  type RunControlDirection,
} from "../../api/client";
import { queryKeys } from "../../api/query-keys";
import { ErrorState } from "../../components/states/error-state";
import { Loading } from "../../components/states/loading";
import { DisconnectedNotice } from "../../components/states/disconnected-notice";
import { UnavailableNotice } from "../../components/states/unavailable-notice";
import { StaleNotice } from "../../components/states/stale-notice";
import { StatusBadge } from "../../components/ui/status-badge";
import { parsePipelineDocument } from "../../pipelines/document";
import { useLiveRun, type UseLiveRunOptions } from "../../live/use-live-run";
import { EventTimeline } from "./event-timeline";
import { QueuePanels } from "./queue-panels";
import { RunStructureTable } from "./run-graph";
import { runFlowEdges, runFlowNodes, runNodeTypes } from "./run-flow";
import {
  deriveNodeOverlay,
  deriveQueuePanels,
  deriveSwimlane,
  formatSeconds,
  runDurationSeconds,
  runStateTone,
} from "./run-derivations";
import { SwimlaneTimeline, type SwimlaneZoom } from "./swimlane";

export function RunLive({
  runId,
  liveOptions,
}: {
  runId: string;
  liveOptions?: UseLiveRunOptions;
}) {
  const { snapshot, retry } = useLiveRun(runId, liveOptions);
  const { view } = snapshot;
  const { durable, telemetry } = view;
  const run = durable.run;
  const [zoom, setZoom] = useState<SwimlaneZoom>("full");
  const [selectedNodeId, setSelectedNodeId] = useState<string | null>(null);
  const queryClient = useQueryClient();

  const lifecycle = useMutation({
    mutationFn: (direction: RunControlDirection) => {
      if (run === null) {
        throw new Error("The authoritative run is not loaded.");
      }
      return controlRun(runId, direction, {
        idempotencyKey: `run-control-${direction}-${runId}-${String(run.run_version)}`,
      });
    },
    onSuccess: (updated) => {
      queryClient.setQueryData(queryKeys.runDetail(runId), updated);
    },
    retry: 0,
  });

  const pipelineVersion = useQuery({
    queryKey:
      run !== null
        ? queryKeys.pipelineVersion(run.pipeline_id, run.pipeline_version)
        : queryKeys.pipelineVersion("unknown", 0),
    queryFn: ({ signal }) =>
      fetchPipelineVersion(run?.pipeline_id ?? "unknown", run?.pipeline_version ?? 0, {
        signal,
      }),
    enabled: run !== null,
    staleTime: Number.POSITIVE_INFINITY,
    retry: 0,
  });

  const parsedDocument = useMemo(() => {
    const data = pipelineVersion.data;
    if (data === undefined) {
      return null;
    }
    const parsed = parsePipelineDocument(data.specification.pipeline);
    return parsed.ok ? parsed.value : null;
  }, [pipelineVersion.data]);

  const overlay = useMemo(() => {
    return deriveNodeOverlay(
      durable.nodeEvents,
      parsedDocument !== null ? parsedDocument.nodes.map((node) => node.id) : [],
    );
  }, [durable.nodeEvents, parsedDocument]);

  // The overlay is applied only under the full identity gate: the document
  // must belong to the exact pipeline identity and immutable version the
  // durable run references. Anything else is rendered as incompatible.
  const document =
    run !== null &&
    pipelineVersion.data?.pipeline_id === run.pipeline_id &&
    pipelineVersion.data?.version === run.pipeline_version &&
    parsedDocument !== null
      ? parsedDocument
      : null;

  const swimlane = useMemo(() => deriveSwimlane(durable.events), [durable.events]);
  const queueView = useMemo(
    () => deriveQueuePanels(telemetry, runId),
    [telemetry, runId],
  );
  const sourceQuarantinedRecords = useMemo(
    () =>
      durable.events.reduce((total, event) => {
        const value = event.payload.source_quarantined_count;
        return typeof value === "number" && Number.isInteger(value) && value > 0
          ? total + value
          : total;
      }, 0),
    [durable.events],
  );

  if (durable.status === "error") {
    return (
      <div className="space-y-4">
        <ErrorState
          title="Run unavailable"
          description={
            durable.lastError ?? "The authoritative run could not be loaded."
          }
          action={
            <button
              type="button"
              data-testid="run-retry"
              className="rounded border border-border px-3 py-1.5 text-xs text-foreground"
              onClick={retry}
            >
              Retry
            </button>
          }
        />
      </div>
    );
  }

  return (
    <div className="space-y-4">
      {durable.status === "recovering" && (
        <StaleNotice
          label="Durable stream recovering"
          detail={`${describeRecovery(durable)} The last coherent durable state stays on screen; authoritative state is being refetched.`}
        />
      )}
      {durable.status === "stale" && (
        <StaleNotice
          label="Durable stream stale"
          detail={`${describeRecovery(durable)} Reconnect attempts were exhausted without hiding a gap; the coherent view below is retained. Reload to start a fresh recovery.`}
        />
      )}
      {(telemetry.health === "disconnected" || telemetry.health === "recovering") && (
        <DisconnectedNotice
          detail={
            telemetry.health === "disconnected"
              ? "Advisory telemetry disconnected; queue panels keep their last observation explicitly marked, and durable state is unaffected."
              : "Advisory telemetry is reconnecting; last observations remain marked as stale."
          }
        />
      )}
      {telemetry.health === "unavailable" && (
        <UnavailableNotice detail="Advisory telemetry is unavailable for this run; queue panels show unavailable instead of zeros." />
      )}
      {telemetry.health === "stale" && (
        <StaleNotice
          label="Telemetry stale"
          detail="No fresh telemetry sample arrived within the freshness window; advisory values are marked stale."
        />
      )}

      <section
        aria-label="Run identity"
        className="rounded-lg border border-border p-4"
      >
        <div className="flex flex-wrap items-center gap-3">
          <h2 className="font-mono text-sm font-semibold text-foreground">{runId}</h2>
          {run === null ? (
            <Loading label="Loading durable run" />
          ) : (
            <>
              <StatusBadge state={runStateTone(run.state)}>{run.state}</StatusBadge>
              <span className="font-mono text-2xs text-muted">
                run_version {String(run.run_version)}
              </span>
              <span className="font-mono text-2xs text-muted">
                {run.pipeline_id} v{String(run.pipeline_version)}
              </span>
              <span className="text-2xs text-muted">runner {run.runner_kind}</span>
              <span className="text-2xs text-muted">
                duration {formatSeconds(runDurationSeconds(run))}
              </span>
              <span className="font-mono text-2xs text-muted">
                evidence{" "}
                {run.execution_evidence_fingerprint ?? "unavailable until finalization"}
              </span>
            </>
          )}
        </div>
        <p className="mt-2 text-2xs text-muted">
          Source: <span className="font-mono">GET /api/v1/runs/{runId}</span>, durable
          stream <span className="font-mono">GET /api/v1/stream/runs/{runId}</span>,
          advisory telemetry{" "}
          <span className="font-mono">WS /api/v1/live/runs/{runId}</span>. Durable facts
          outrank telemetry everywhere on this page.
        </p>
        {run !== null && (
          <RunLifecycleControls
            state={run.state}
            unavailable={
              durable.status === "recovering" ||
              durable.status === "stale" ||
              lifecycle.isPending
            }
            pending={lifecycle.variables}
            error={lifecycle.error}
            onCommand={(direction) => lifecycle.mutate(direction)}
          />
        )}
        {sourceQuarantinedRecords > 0 && (
          <p className="mt-2 text-2xs text-muted" data-testid="run-quarantine">
            Durable reconciliation reports {String(sourceQuarantinedRecords)} source
            records quarantined during normalization.
          </p>
        )}
        {snapshot.stream.kind === "recovering" && (
          <p
            className="mt-1 text-2xs text-warning"
            role="status"
            data-testid="stream-status"
          >
            Durable stream status: recovering from{" "}
            {snapshot.stream.reason?.kind ?? "unknown"}.
          </p>
        )}
        {snapshot.stream.kind === "live" && (
          <p
            className="mt-1 text-2xs text-muted"
            role="status"
            data-testid="stream-status"
          >
            Durable stream live at sequence {String(view.durable.acceptedSequence)}.
          </p>
        )}
      </section>

      <section aria-label="Pipeline graph" className="rounded-lg border border-border">
        <div className="border-b border-border px-4 py-2">
          <h2 className="text-sm font-semibold text-foreground">
            Pipeline graph with durable state overlay
          </h2>
          <p className="text-2xs text-muted">
            Source: immutable pipeline version{" "}
            <span className="font-mono">GET /api/v1/pipelines/…/versions/…</span> plus
            durable events. State is overlaid only when run and pipeline identities and
            versions all agree.
          </p>
        </div>
        {run === null ? (
          <div className="p-4">
            <Loading label="Loading pipeline identity" />
          </div>
        ) : pipelineVersion.isError ? (
          <div className="p-4">
            <UnavailableNotice detail="The pipeline version document is unavailable, so no graph overlay is shown. Durable facts below remain authoritative." />
          </div>
        ) : document === null ? (
          <div className="p-4">
            <UnavailableNotice detail="The fetched pipeline version does not match the run's durable identity, so the overlay is blocked rather than merged. This is a coherence failure worth reporting." />
          </div>
        ) : (
          <>
            <div className="h-[380px] border-b border-border" data-testid="run-canvas">
              <ReactFlow
                nodes={runFlowNodes(document, overlay, selectedNodeId)}
                edges={runFlowEdges(document)}
                nodeTypes={runNodeTypes}
                nodesDraggable={false}
                nodesConnectable={false}
                elementsSelectable={false}
                edgesFocusable={false}
                deleteKeyCode={null}
                fitView
              >
                <Background />
                <MiniMap pannable zoomable />
              </ReactFlow>
            </div>
            <RunStructureTable
              document={document}
              overlay={overlay}
              selectedNodeId={selectedNodeId}
              onSelect={(nodeId) => {
                setSelectedNodeId((current) => (current === nodeId ? null : nodeId));
              }}
            />
          </>
        )}
      </section>

      <section
        aria-label="Worker swimlane timeline"
        className="rounded-lg border border-border"
      >
        <div className="border-b border-border px-4 py-2">
          <h2 className="text-sm font-semibold text-foreground">
            Worker swimlane timeline
          </h2>
          <p className="text-2xs text-muted">
            Source: durable events only. Service time is computed between durable claim
            and completion timestamps; queue time is not durable and is never invented.
          </p>
        </div>
        <SwimlaneTimeline
          result={swimlane}
          zoom={zoom}
          onZoomChange={setZoom}
          durableUncertain={
            durable.status === "recovering" || durable.status === "stale"
          }
        />
      </section>

      <section
        aria-label="Queue and saturation"
        className="rounded-lg border border-border"
      >
        <div className="border-b border-border px-4 py-2">
          <h2 className="text-sm font-semibold text-foreground">
            Queue and saturation
          </h2>
          <p className="text-2xs text-muted">
            Source: advisory telemetry only. Every metric names its definition, unit,
            source, and observation timestamp; incomplete or stale samples support no
            performance conclusion.
          </p>
        </div>
        <QueuePanels view={queueView} />
      </section>

      <section aria-label="Durable events" className="rounded-lg border border-border">
        <div className="border-b border-border px-4 py-2">
          <h2 className="text-sm font-semibold text-foreground">
            Durable event timeline
          </h2>
          <p className="text-2xs text-muted">
            Source: durable SSE stream, resumed from the last accepted sequence{" "}
            {String(durable.acceptedSequence)}. Telemetry messages are never listed
            here.
          </p>
        </div>
        <EventTimeline durable={durable} />
      </section>
    </div>
  );
}

function RunLifecycleControls({
  state,
  unavailable,
  pending,
  error,
  onCommand,
}: {
  state: string;
  unavailable: boolean;
  pending: RunControlDirection | undefined;
  error: unknown;
  onCommand: (direction: RunControlDirection) => void;
}) {
  const actions: RunControlDirection[] =
    state === "running"
      ? ["pause", "cancel"]
      : state === "paused"
        ? ["resume", "cancel"]
        : state === "queued"
          ? ["cancel"]
          : [];
  if (actions.length === 0) {
    return null;
  }
  return (
    <div className="mt-3 border-t border-border pt-3" aria-label="Run controls">
      <div className="flex flex-wrap gap-2">
        {actions.map((action) => (
          <button
            key={action}
            type="button"
            data-testid={`run-${action}`}
            className="rounded border border-border px-3 py-1.5 text-xs capitalize text-foreground disabled:cursor-not-allowed disabled:opacity-50"
            disabled={unavailable}
            onClick={() => onCommand(action)}
          >
            {pending === action ? `${action} pending` : action}
          </button>
        ))}
      </div>
      {unavailable && pending === undefined && (
        <p className="mt-2 text-2xs text-warning" role="status">
          Run controls are unavailable until durable state is coherent.
        </p>
      )}
      {error !== null && (
        <p className="mt-2 text-2xs text-danger" role="alert">
          {describeControlError(error)}
        </p>
      )}
    </div>
  );
}

function describeControlError(error: unknown): string {
  if (isApiRequestError(error)) {
    return error.problem.detail ?? error.problem.title;
  }
  return error instanceof Error && error.message !== ""
    ? error.message
    : "The run command failed safely. Retry the same action.";
}

function describeRecovery(durable: { lastError: string | null }): string {
  return durable.lastError !== null ? `Reason: ${durable.lastError}.` : "";
}

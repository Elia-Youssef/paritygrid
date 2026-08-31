/**
 * Plan preview and publication panel.
 *
 * The preview renders the exact canonical logical document intended for
 * publication — identity, frontier, versions, node/edge summary, connector
 * bindings, resource limits, and validation results — and distinguishes
 * logical content from visual layout. Publication is an explicit versioned
 * command: double-click issues exactly one request, retries reuse the same
 * idempotency key and byte-equivalent body, every failure preserves the
 * draft, and only a fully validated matching acknowledgement re-bases it.
 */
import { useMutation } from "@tanstack/react-query";
import { useCallback, useEffect, useRef, useState } from "react";

import { publishPipelineVersion } from "../../api/client";
import type {
  PipelineVersionAckValue,
  PipelineVersionFrontierValue,
} from "../../api/schemas";
import {
  serializeLogicalDocument,
  type Diagnostic,
  type PipelineDocumentValue,
} from "../../pipelines/document";
import {
  contentHash,
  type PublicationCommand,
  type PublicationStatus,
} from "./studio-state";

export interface PublicationPanelProps {
  pipelineId: string;
  pipelineName: string;
  document: PipelineDocumentValue;
  diagnostics: readonly Diagnostic[];
  frontier: PipelineVersionFrontierValue | null;
  command: PublicationCommand | null;
  publication: PublicationStatus;
  /** Freezes (or returns the existing) publication command for this draft. */
  beginCommand: (expectedLatestVersion: number) => PublicationCommand;
  onPublished: (ack: PipelineVersionAckValue) => void;
  onConflict: (problem: {
    title: string;
    detail?: string;
    extensions: readonly { name: string; value: string }[];
  }) => void;
  onFailed: (problem: {
    title: string;
    detail?: string;
    extensions: readonly { name: string; value: string }[];
  }) => void;
  onRefreshFrontier: () => Promise<void>;
}

export function PublicationPanel(props: PublicationPanelProps) {
  const {
    pipelineId,
    pipelineName,
    document,
    diagnostics,
    frontier,
    command,
    publication,
    beginCommand,
    onPublished,
    onConflict,
    onFailed,
    onRefreshFrontier,
  } = props;
  const [previewOpen, setPreviewOpen] = useState(false);
  const [refreshingFrontier, setRefreshingFrontier] = useState(false);
  const previewTriggerRef = useRef<HTMLButtonElement | null>(null);
  const closeButtonRef = useRef<HTMLButtonElement | null>(null);

  // Deterministic focus management: focusing the trigger while the dialog
  // is still mounted keeps focus stable across the unmount.
  const closePreview = useCallback(() => {
    setPreviewOpen(false);
    previewTriggerRef.current?.focus();
  }, []);

  const blocking = diagnostics.length > 0;
  const expectedLatestVersion = frontier === null ? null : frontier.latest_version;
  const fencingKnown = expectedLatestVersion !== null;

  const mutation = useMutation({
    mutationFn: (commandToRun: PublicationCommand) =>
      publishPipelineVersion(
        pipelineId,
        {
          document: commandToRun.document,
          expectedLatestVersion: commandToRun.expectedLatestVersion,
        },
        { idempotencyKey: commandToRun.idempotencyKey },
      ),
    retry: 0,
    onSuccess: (ack) => {
      // The studio state validates the acknowledgement against the frozen
      // command; a mismatched response never clears or replaces the draft.
      onPublished(ack);
    },
    onError: (error: unknown) => {
      const status = (error as { status?: number }).status;
      if (status === 409) {
        onConflict({
          title: "Publication is stale",
          detail:
            (error as { problem?: { detail?: string } }).problem?.detail ??
            "The immutable version frontier moved; refresh it and publish again.",
          extensions: [{ name: "code", value: "pipeline_version_conflict" }],
        });
      } else {
        onFailed({
          title: "Publication failed",
          detail:
            (error as { message?: string }).message ?? "The command did not complete.",
          extensions: [],
        });
      }
    },
  });

  const pending = publication.kind === "pending" || mutation.isPending;
  const staleConflict = publication.kind === "conflict";

  const refreshFrontier = async (): Promise<void> => {
    if (refreshingFrontier) {
      return;
    }
    setRefreshingFrontier(true);
    try {
      await onRefreshFrontier();
    } catch {
      onFailed({
        title: "Version frontier refresh failed",
        detail: "The latest immutable pipeline version could not be refreshed.",
        extensions: [],
      });
    } finally {
      setRefreshingFrontier(false);
    }
  };

  const publish = (): void => {
    if (
      pending ||
      staleConflict ||
      refreshingFrontier ||
      blocking ||
      !fencingKnown ||
      document.nodes.length === 0
    ) {
      return;
    }
    // Freezes the command for this exact draft (idempotent per draft); a
    // double click observes `pending` and never reaches a second request.
    const active = beginCommand(expectedLatestVersion);
    mutation.mutate(active);
  };

  return (
    <section aria-label="Publication" className="border-t border-border p-4">
      <h3 className="text-sm font-semibold text-foreground">Publication</h3>
      <dl
        className="mt-2 grid grid-cols-2 gap-x-4 gap-y-1 text-2xs"
        data-testid="publication-summary"
      >
        <dt className="text-muted">Pipeline</dt>
        <dd className="font-mono text-foreground">{pipelineId}</dd>
        <dt className="text-muted">Known base (latest version)</dt>
        <dd className="font-mono text-foreground" data-testid="known-base">
          {frontier === null
            ? "unavailable"
            : frontier.published
              ? String(frontier.latest_version)
              : "0 (unpublished)"}
        </dd>
        <dt className="text-muted">Document schema</dt>
        <dd className="font-mono text-foreground">{String(document.schemaVersion)}</dd>
        <dt className="text-muted">Planner format version</dt>
        <dd className="font-mono text-foreground">1</dd>
        <dt className="text-muted">Nodes / connections</dt>
        <dd className="font-mono text-foreground">
          {String(document.nodes.length)} / {String(document.edges.length)}
        </dd>
        <dt className="text-muted">Logical content</dt>
        <dd className="text-foreground">
          {String(serializeLogicalDocument(document).length)} bytes, deterministic
        </dd>
        <dt className="text-muted">Visual layout</dt>
        <dd className="text-foreground">tracked separately from content</dd>
        <dt className="text-muted">Validation</dt>
        <dd className={blocking ? "text-failure" : "text-verified"}>
          {blocking
            ? `${String(diagnostics.length)} error(s) block publication`
            : "no errors"}
        </dd>
      </dl>

      <div className="mt-3 flex flex-wrap gap-2">
        <button
          type="button"
          className="rounded border border-border px-3 py-1.5 text-xs text-foreground hover:bg-surface-elevated"
          onClick={() => {
            setPreviewOpen(true);
          }}
          ref={previewTriggerRef}
          data-testid="preview-button"
        >
          Preview plan
        </button>
        <button
          type="button"
          className="rounded bg-primary px-3 py-1.5 text-xs font-medium text-primary-foreground disabled:cursor-not-allowed disabled:opacity-50"
          disabled={
            pending ||
            staleConflict ||
            refreshingFrontier ||
            blocking ||
            !fencingKnown ||
            document.nodes.length === 0
          }
          data-testid="publish-button"
          onClick={() => {
            publish();
          }}
        >
          {pending
            ? "Publishing…"
            : frontier?.published
              ? `Publish version ${String(frontier.latest_version + 1)}`
              : "Publish version 1"}
        </button>
        {publication.kind === "conflict" && (
          <button
            type="button"
            className="rounded border border-border px-3 py-1.5 text-xs text-foreground hover:bg-surface-elevated"
            disabled={refreshingFrontier}
            onClick={() => {
              void refreshFrontier();
            }}
          >
            {refreshingFrontier
              ? "Refreshing version frontier…"
              : "Refresh version frontier"}
          </button>
        )}
      </div>

      <div aria-live="polite" role="status">
        {publication.kind === "published" && (
          <p className="mt-2 text-xs text-verified" data-testid="publication-success">
            Published version {String(publication.ack.version)} · hash{" "}
            <span className="font-mono">
              {publication.ack.specification_sha256.slice(0, 12)}…
            </span>
          </p>
        )}
      </div>
      <div aria-live="assertive" role="alert">
        {publication.kind === "conflict" && (
          <div
            className="mt-2 rounded border border-warning/40 bg-warning/10 p-2 text-xs text-warning"
            data-testid="publication-conflict"
          >
            <p className="font-medium">
              Stale publication blocked: {publication.problem.title}
            </p>
            {publication.problem.detail !== undefined && (
              <p className="mt-1">{publication.problem.detail}</p>
            )}
            <p className="mt-1">
              Your draft is preserved. Refresh the frontier and publish again.
            </p>
          </div>
        )}
        {publication.kind === "failed" && (
          <div
            className="mt-2 rounded border border-failure/40 bg-failure/10 p-2 text-xs text-failure"
            data-testid="publication-failure"
          >
            <p className="font-medium">{publication.problem.title}</p>
            {publication.problem.detail !== undefined && (
              <p className="mt-1">{publication.problem.detail}</p>
            )}
            <p className="mt-1">
              Your draft and the original command are preserved for retry.
            </p>
          </div>
        )}
      </div>

      {previewOpen && (
        <PreviewDialog
          pipelineName={pipelineName}
          document={document}
          diagnostics={diagnostics}
          expectedLatestVersion={expectedLatestVersion}
          command={command}
          closeButtonRef={closeButtonRef}
          onClose={closePreview}
        />
      )}
    </section>
  );
}

function PreviewDialog(props: {
  pipelineName: string;
  document: PipelineDocumentValue;
  diagnostics: readonly Diagnostic[];
  expectedLatestVersion: number | null;
  command: PublicationCommand | null;
  closeButtonRef: React.RefObject<HTMLButtonElement | null>;
  onClose: () => void;
}) {
  const {
    pipelineName,
    document,
    diagnostics,
    expectedLatestVersion,
    command,
    closeButtonRef,
    onClose,
  } = props;
  const dialogRef = useRef<HTMLDivElement | null>(null);

  useEffect(() => {
    closeButtonRef.current?.focus();
  }, [closeButtonRef]);

  const keepFocusInside = (event: React.KeyboardEvent<HTMLDivElement>): void => {
    if (event.key === "Escape") {
      onClose();
      return;
    }
    if (event.key !== "Tab" || dialogRef.current === null) {
      return;
    }
    const focusable = [
      ...dialogRef.current.querySelectorAll<HTMLElement>(
        'button:not([disabled]), [href], input:not([disabled]), select:not([disabled]), textarea:not([disabled]), [tabindex]:not([tabindex="-1"])',
      ),
    ];
    if (focusable.length === 0) {
      event.preventDefault();
      return;
    }
    const first = focusable[0];
    const last = focusable[focusable.length - 1];
    if (event.shiftKey && globalThis.document.activeElement === first) {
      event.preventDefault();
      last?.focus();
    } else if (!event.shiftKey && globalThis.document.activeElement === last) {
      event.preventDefault();
      first?.focus();
    }
  };

  return (
    <div
      className="fixed inset-0 z-50 flex items-start justify-center bg-background/70"
      role="presentation"
      onKeyDown={keepFocusInside}
      onMouseDown={(event) => {
        if (event.target === event.currentTarget) {
          onClose();
        }
      }}
    >
      <div
        ref={dialogRef}
        role="dialog"
        aria-modal="true"
        aria-label="Plan preview"
        className="mt-[8vh] max-h-[80vh] w-[calc(100%-2rem)] max-w-2xl overflow-y-auto rounded-lg border border-border-strong bg-surface p-4 shadow-overlay"
      >
        <div className="flex items-start justify-between gap-2">
          <h2 className="text-base font-semibold text-foreground">Plan preview</h2>
          <button
            type="button"
            ref={closeButtonRef}
            className="rounded border border-border px-2 py-1 text-2xs text-foreground hover:bg-surface-elevated"
            onClick={onClose}
          >
            Close preview
          </button>
        </div>

        <dl
          className="mt-3 grid grid-cols-2 gap-x-4 gap-y-1 text-2xs"
          data-testid="preview-summary"
        >
          <dt className="text-muted">Pipeline</dt>
          <dd className="text-foreground">{pipelineName}</dd>
          <dt className="text-muted">Known base / expected frontier</dt>
          <dd
            className="font-mono text-foreground"
            data-testid="preview-expected-frontier"
          >
            {expectedLatestVersion === null
              ? "unavailable"
              : String(expectedLatestVersion)}
          </dd>
          <dt className="text-muted">Document + planner format</dt>
          <dd className="font-mono text-foreground">
            v{String(document.schemaVersion)} / v1
          </dd>
          <dt className="text-muted">Nodes</dt>
          <dd className="font-mono text-foreground">{String(document.nodes.length)}</dd>
          <dt className="text-muted">Connections</dt>
          <dd className="font-mono text-foreground">{String(document.edges.length)}</dd>
          <dt className="text-muted">Resource limits</dt>
          <dd className="font-mono text-foreground">{`concurrency ${String(document.resourcePolicy.max_concurrency)}, in-flight ${String(document.resourcePolicy.max_in_flight)}, timeout ${String(document.resourcePolicy.operation_timeout_seconds)}s`}</dd>
          <dt className="text-muted">Connector bindings</dt>
          <dd className="text-foreground">
            {[
              ...new Set(
                document.nodes
                  .map((node) => node.connector_id)
                  .filter((id): id is string => id !== null),
              ),
            ].map((id) => (
              <span key={id} className="mr-2 font-mono">
                {id}
              </span>
            ))}
            {document.nodes.every((node) => node.connector_id === null) ? "none" : ""}
          </dd>
          <dt className="text-muted">Command fingerprint</dt>
          <dd className="font-mono text-foreground" data-testid="command-fingerprint">
            {command === null ? "will bind on publish" : contentHash(command.bodyJson)}
          </dd>
        </dl>

        <h3 className="mt-4 text-sm font-semibold text-foreground">
          Validation results
        </h3>
        {diagnostics.length === 0 ? (
          <p className="text-2xs text-verified">
            No errors; the document satisfies the closed graph contract.
          </p>
        ) : (
          <ul className="list-inside list-disc text-2xs text-failure">
            {diagnostics.map((diagnostic, index) => (
              <li key={String(index)}>{diagnostic.message}</li>
            ))}
          </ul>
        )}

        <h3 className="mt-4 text-sm font-semibold text-foreground">
          Canonical logical document
        </h3>
        <pre className="mt-1 max-h-40 overflow-auto rounded border border-border bg-surface-quiet p-2 font-mono text-2xs text-muted">
          {serializeLogicalDocument(document)}
        </pre>
        <p className="mt-1 text-2xs text-muted">
          Visual layout is serialized separately and never changes logical identity.
        </p>
      </div>
    </div>
  );
}

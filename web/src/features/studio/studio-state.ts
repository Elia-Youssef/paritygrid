/**
 * Draft-state and publication-command machine for the pipeline studio.
 *
 * Separation of concerns enforced here:
 * - The local draft (logical document + layout) is the ONLY mutable editor
 *   state; server data initializes it once and can never overwrite newer
 *   local edits.
 * - Logical identity and visual layout are compared separately, so moving
 *   nodes never dirties the logical document and never invalidates an
 *   immutable published version.
 * - One publication command owns one stable idempotency key bound to the
 *   exact canonical request bytes. Any draft edit invalidates the command;
 *   an unchanged draft retries byte-identically under the same key.
 * - A draft is marked published (re-based) only after a fully validated
 *   acknowledgement that matches pipeline identity and expected frontier.
 *   Every failure mode preserves the draft.
 */
import { useCallback, useMemo, useState } from "react";

import type { ProblemDetailsView } from "../../lib/problem-details";
import {
  emptyDocument,
  serializeLayout,
  serializeLogicalDocument,
  toPublicationDocument,
  validateDocument,
  type ConnectorSummary,
  type Diagnostic,
  type PipelineDocumentValue,
} from "../../pipelines/document";
import type { PipelineVersionAckValue } from "../../api/schemas";

const FNV_PRIME = 0x1b3;
const FNV_PRIME_HIGH = 0x100;
const FNV_OFFSET_HIGH = 0xcbf2_9ce4;
const FNV_OFFSET_LOW = 0x8422_2325;

/** Stable 64-bit FNV-1a over UTF-8 bytes, rendered as 16 hex chars. */
export function contentHash(value: string): string {
  const bytes = new TextEncoder().encode(value);
  let high = FNV_OFFSET_HIGH;
  let low = FNV_OFFSET_LOW;
  for (const octet of bytes) {
    low = (low ^ octet) >>> 0;
    // (high:low) * prime in 32-bit halves; low*prime is exact as a double.
    const carry = Math.floor((low * FNV_PRIME) / 0x1_0000_0000);
    const nextLow = Math.imul(low, FNV_PRIME) >>> 0;
    high = (Math.imul(high, FNV_PRIME) + Math.imul(low, FNV_PRIME_HIGH) + carry) >>> 0;
    low = nextLow;
  }
  return `${high.toString(16).padStart(8, "0")}${low.toString(16).padStart(8, "0")}`;
}

/** One frozen publication command. */
export interface PublicationCommand {
  readonly idempotencyKey: string;
  readonly expectedLatestVersion: number;
  /** Byte-equivalent canonical request body for every replay. */
  readonly bodyJson: string;
  readonly document: Record<string, unknown>;
}

export type PublicationStatus =
  | { kind: "idle" }
  | { kind: "pending" }
  | { kind: "published"; ack: PipelineVersionAckValue }
  | { kind: "conflict"; problem: ProblemDetailsView }
  | { kind: "failed"; problem: ProblemDetailsView };

export interface StudioDraftState {
  readonly document: PipelineDocumentValue;
  readonly baselineLogical: string;
  readonly baselineLayout: string;
}

export function logicalDirty(state: StudioDraftState): boolean {
  return serializeLogicalDocument(state.document) !== state.baselineLogical;
}

export function layoutDirty(state: StudioDraftState): boolean {
  return serializeLayout(state.document) !== state.baselineLayout;
}

/** Build (or rebuild) the frozen publication command for a draft. */
export function buildPublicationCommand(
  pipelineId: string,
  document: PipelineDocumentValue,
  expectedLatestVersion: number,
): PublicationCommand {
  const publicationDocument = toPublicationDocument(document);
  const bodyJson = JSON.stringify({
    document: publicationDocument,
    expected_latest_version: expectedLatestVersion,
  });
  // The key binds the pipeline, the frontier it is based on, and the exact
  // canonical content, so a changed draft can never replay under the old key.
  const key = `publish-${pipelineId}-${String(expectedLatestVersion)}-${contentHash(bodyJson)}`;
  return {
    idempotencyKey: key,
    expectedLatestVersion,
    bodyJson,
    document: publicationDocument,
  };
}

/** Validate an acknowledgement against the command it must answer. */
export function acknowledgementMatches(
  pipelineId: string,
  command: PublicationCommand,
  ack: PipelineVersionAckValue,
): boolean {
  return (
    ack.pipeline_id === pipelineId && ack.version === command.expectedLatestVersion + 1
  );
}

export type DraftEdit = (
  mutate: (document: PipelineDocumentValue) => PipelineDocumentValue,
) => void;

export interface UseStudioStateOptions {
  pipelineId: string;
  connectors: readonly ConnectorSummary[];
  /**
   * Authoritative initial document (the latest published version) or null
   * while it is loading. Applied exactly once per pipeline.
   */
  initialDocument: PipelineDocumentValue | null;
  initialReady: boolean;
}

export interface StudioState {
  readonly draft: StudioDraftState | null;
  readonly logicalDirty: boolean;
  readonly layoutDirty: boolean;
  readonly diagnostics: readonly Diagnostic[];
  readonly commandInvalid: boolean;
  readonly selection: string | null;
  readonly command: PublicationCommand | null;
  readonly publication: PublicationStatus;
  select: (nodeId: string | null) => void;
  edit: (mutate: (document: PipelineDocumentValue) => PipelineDocumentValue) => void;
  beginCommand: (expectedLatestVersion: number) => PublicationCommand;
  markPending: () => void;
  markPublished: (ack: PipelineVersionAckValue) => void;
  markConflict: (problem: ProblemDetailsView) => void;
  markFailed: (problem: ProblemDetailsView) => void;
  clearCommand: () => void;
}

interface DraftCell {
  readonly forPipeline: string;
  readonly draft: StudioDraftState | null;
}

/**
 * Owns the studio draft lifecycle. `initialDocument` is applied only once
 * per pipeline id; later query refetches (even with newer server data)
 * never overwrite local edits.
 */
export function useStudioState(options: UseStudioStateOptions): StudioState {
  const { pipelineId, connectors, initialDocument, initialReady } = options;
  const [cell, setCell] = useState<DraftCell>(() => ({
    forPipeline: pipelineId,
    draft: null,
  }));
  const [selection, setSelected] = useState<string | null>(null);
  const [command, setCommand] = useState<PublicationCommand | null>(null);
  const [publication, setPublication] = useState<PublicationStatus>({ kind: "idle" });

  if (cell.forPipeline !== pipelineId) {
    // Render-time reset when the route changes pipeline: everything the
    // previous draft owned is discarded together.
    setCell({ forPipeline: pipelineId, draft: null });
    setSelected(null);
    setCommand(null);
    setPublication({ kind: "idle" });
  } else if (cell.draft === null && initialReady) {
    // One-time initialization from the authoritative published document, or
    // from a bounded empty draft for unpublished and brand-new pipelines.
    const source = initialDocument ?? emptyDocument();
    setCell({
      forPipeline: pipelineId,
      draft: {
        document: source,
        baselineLogical: serializeLogicalDocument(source),
        baselineLayout: serializeLayout(source),
      },
    });
  }
  const draft = cell.forPipeline === pipelineId ? cell.draft : null;

  const edit = useCallback(
    (mutate: (document: PipelineDocumentValue) => PipelineDocumentValue) => {
      setCell((current) => {
        if (current.draft === null) {
          return current;
        }
        return {
          ...current,
          draft: { ...current.draft, document: mutate(current.draft.document) },
        };
      });
      // Any edit — logical or layout — invalidates a pending publication
      // command: the frozen bytes would no longer match the draft.
      setCommand(null);
      setPublication((current) =>
        current.kind === "published" ? { kind: "idle" } : current,
      );
    },
    [],
  );

  const select = useCallback((nodeId: string | null) => {
    setSelected(nodeId);
  }, []);

  const documentValue = draft?.document ?? null;
  const diagnostics = useMemo(
    () => (documentValue === null ? [] : validateDocument(documentValue, connectors)),
    [documentValue, connectors],
  );

  const beginCommand = useCallback(
    (expectedLatestVersion: number): PublicationCommand => {
      if (draft === null) {
        throw new RangeError("cannot publish before the draft is initialized");
      }
      if (command === null) {
        const built = buildPublicationCommand(
          pipelineId,
          draft.document,
          expectedLatestVersion,
        );
        setCommand(built);
        return built;
      }
      return command;
    },
    [command, draft, pipelineId],
  );

  const clearCommand = useCallback(() => {
    setCommand(null);
    setPublication({ kind: "idle" });
  }, []);

  const markPending = useCallback(() => {
    setPublication({ kind: "pending" });
  }, []);

  const markPublished = useCallback(
    (ack: PipelineVersionAckValue) => {
      // Defense in depth: only an acknowledgement naming THIS pipeline and
      // advancing THIS command's frontier can re-base the draft. A stale,
      // foreign, or mismatched acknowledgement is downgraded to a failure;
      // the draft and the frozen command survive untouched.
      if (
        command === null ||
        ack.pipeline_id !== pipelineId ||
        ack.version !== command.expectedLatestVersion + 1
      ) {
        setPublication({
          kind: "failed",
          problem: {
            title: "Publication acknowledgement does not match this command",
            detail: "The response identifies a different pipeline or version.",
            extensions: [],
          },
        });
        return;
      }
      setPublication({ kind: "published", ack });
      setCell((current) => {
        if (current.draft === null) {
          return current;
        }
        // Re-base the draft on the published logical content; layout stays
        // as drafted.
        return {
          ...current,
          draft: {
            ...current.draft,
            baselineLogical: serializeLogicalDocument(current.draft.document),
            baselineLayout: serializeLayout(current.draft.document),
          },
        };
      });
      setCommand(null);
    },
    [command, pipelineId],
  );

  const markConflict = useCallback((problem: ProblemDetailsView) => {
    setPublication({ kind: "conflict", problem });
  }, []);

  const markFailed = useCallback((problem: ProblemDetailsView) => {
    setPublication({ kind: "failed", problem });
  }, []);

  return {
    draft,
    logicalDirty: draft === null ? false : logicalDirty(draft),
    layoutDirty: draft === null ? false : layoutDirty(draft),
    diagnostics,
    commandInvalid: false,
    selection,
    command,
    publication,
    select,
    edit,
    beginCommand,
    markPending,
    markPublished,
    markConflict,
    markFailed,
    clearCommand,
  };
}

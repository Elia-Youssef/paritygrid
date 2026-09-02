/**
 * Reconciliation query with snapshot quarantine. The rendered snapshot is
 * held in local state and only replaced when the newest server snapshot is
 * fully coherent with it (same run identity, input identities,
 * reconciliation fingerprint, and analytical query version). An
 * incompatible newer snapshot — typically a reconciliation re-run — is
 * never merged: it is held in `quarantined` behind an explicit
 * refresh-required decision so a screen cannot mix facts computed under
 * different inputs.
 */
import { useQuery } from "@tanstack/react-query";
import { useState } from "react";

import type { ReconciliationResponse } from "../../api/generated/schema";
import { fetchReconciliation } from "../../api/client";
import { queryKeys } from "../../api/query-keys";
import { summarizeIncompatibleSnapshot } from "./coherence";

export interface ReconciliationView {
  /** The rendered coherent snapshot — never a quarantined one. */
  summary: ReconciliationResponse | null;
  /** An incompatible newer snapshot, held but NOT merged. */
  quarantined: ReconciliationResponse | null;
  /** True exactly when a quarantined snapshot is being held. */
  incompatible: boolean;
  isPending: boolean;
  error: unknown;
  refetch: () => void;
  /** Adopt the quarantined snapshot explicitly (operator refresh). */
  adoptQuarantined: () => void;
}

interface RenderedSnapshotState {
  /** The run the rendered snapshot belongs to; a run switch resets both. */
  runId: string;
  rendered: ReconciliationResponse | null;
  quarantined: ReconciliationResponse | null;
}

const EMPTY_STATE: RenderedSnapshotState = {
  runId: "",
  rendered: null,
  quarantined: null,
};

export function useReconciliation(runId: string): ReconciliationView {
  const query = useQuery({
    queryKey: queryKeys.reconciliation(runId),
    queryFn: ({ signal }) => fetchReconciliation(runId, { signal }),
    retry: false,
  });

  const [state, setState] = useState<RenderedSnapshotState>(EMPTY_STATE);
  const [seenData, setSeenData] = useState(query.data);

  // Derive the rendered snapshot from the newest query data during render
  // (the documented state-adjustment pattern): an incompatible newer
  // snapshot is held unmerged instead of replacing the coherent view.
  if (query.data !== undefined && query.data !== seenData) {
    setSeenData(query.data);
    const next = query.data;
    setState((current) => {
      if (current.runId !== runId || current.rendered === null) {
        // First snapshot for this run: render it directly.
        return { runId, rendered: next, quarantined: null };
      }
      if (summarizeIncompatibleSnapshot(current.rendered, next) === null) {
        // Compatible replacement: the newest observation describes the same
        // durable facts, so it can safely take over the rendered view.
        return { runId, rendered: next, quarantined: current.quarantined };
      }
      if (current.quarantined === next) {
        return current;
      }
      // Incompatible newer snapshot: keep rendering the coherent view and
      // hold the newcomer unmerged until an operator adopts it.
      return { runId, rendered: current.rendered, quarantined: next };
    });
  }

  return {
    summary: state.runId === runId ? state.rendered : null,
    quarantined: state.runId === runId ? state.quarantined : null,
    incompatible: state.runId === runId && state.quarantined !== null,
    isPending: query.isPending,
    error: query.error,
    refetch: () => {
      void query.refetch();
    },
    adoptQuarantined: () => {
      setState((current) =>
        current.quarantined !== null
          ? { runId, rendered: current.quarantined, quarantined: null }
          : current,
      );
    },
  };
}

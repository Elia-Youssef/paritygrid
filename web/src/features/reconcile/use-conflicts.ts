/**
 * React binding for the bounded conflict-page cache.
 *
 * One TanStack Query entry is active at a time, keyed by run id, limit,
 * cursor, reconciliation fingerprint, and run version. The first page
 * loads automatically once an identity is known; every further page loads
 * only through an explicit loadMore call, so the client never races ahead
 * of the user. The query key carries the fingerprint and run version, so
 * pages persisted under different snapshots live in different cache entries
 * and can never be concatenated by the cache itself. Identity changes fence
 * the resident state (data and selection are dropped) and restart from a
 * fresh first page. Responses whose identity does not match the resident
 * snapshot are quarantined by the reducer and counted, never merged.
 */
import { useQuery, useQueryClient } from "@tanstack/react-query";
import { useCallback, useEffect, useReducer, useRef, useState } from "react";

import { fetchConflicts } from "../../api/client";
import { queryKeys } from "../../api/query-keys";
import { sameReconciliationIdentity, type ReconciliationIdentity } from "./coherence";
import {
  DEFAULT_CONFLICT_PAGE_SIZE,
  initialConflictPagesState,
  reduceConflictPages,
  type ConflictPagesState,
} from "./conflict-data";

export interface ConflictPagesView {
  state: ConflictPagesState;
  isFetchingPage: boolean;
  pageError: unknown;
  /** Fetch the next page; a no-op while exhausted or already loading. */
  loadMore: () => void;
  select: (conflictId: string) => void;
  clearSelection: () => void;
  reset: () => void;
}

export function useConflicts(
  runId: string,
  identity: ReconciliationIdentity | null,
): ConflictPagesView {
  const queryClient = useQueryClient();
  const [state, dispatch] = useReducer(
    reduceConflictPages,
    undefined,
    initialConflictPagesState,
  );
  // The cursor the active query is fetching (null = first page).
  const [targetCursor, setTargetCursor] = useState<string | null>(null);
  const identityRef = useRef<ReconciliationIdentity | null>(null);

  useEffect(() => {
    const previous = identityRef.current;
    const changed =
      previous === null
        ? identity !== null
        : identity === null || !sameReconciliationIdentity(previous, identity);
    identityRef.current = identity;
    if (changed) {
      if (identity === null) {
        // Without an identity there is no valid snapshot to keep.
        dispatch({ type: "reset" });
      } else {
        dispatch({ type: "identity-changed", identity });
      }
      setTargetCursor(null);
    }
  }, [identity]);

  // The first page is requested with the empty cursor; later pages with the
  // continuation token they extend. A cursor that is already part of the
  // loaded chain never triggers a second fetch.
  const cursorLoaded =
    targetCursor === null
      ? state.cursors.includes("")
      : state.cursors.includes(targetCursor);

  const conflictsQuery = useQuery({
    queryKey: queryKeys.conflicts(runId, {
      limit: DEFAULT_CONFLICT_PAGE_SIZE,
      cursor: targetCursor ?? undefined,
      reconciliationFingerprint: identity?.reconciliation_fingerprint,
      runVersion: identity?.run_version,
    }),
    queryFn: ({ signal }) =>
      fetchConflicts(
        runId,
        { limit: DEFAULT_CONFLICT_PAGE_SIZE, cursor: targetCursor ?? undefined },
        { signal },
      ),
    enabled: identity !== null && !cursorLoaded,
    retry: 0,
  });

  useEffect(() => {
    if (conflictsQuery.data !== undefined) {
      // The reducer quarantines (and counts) pages whose identity does not
      // match the resident snapshot, so a mismatched response is never merged.
      dispatch({ type: "page-loaded", page: conflictsQuery.data });
    }
  }, [conflictsQuery.data]);

  useEffect(() => {
    if (identity === null) {
      return;
    }
    const retainedCursors = new Set<string | null>([
      ...state.cursors.map((cursor) => (cursor === "" ? null : cursor)),
      targetCursor,
    ]);
    queryClient.removeQueries({
      predicate: (query) => {
        const key = query.queryKey;
        if (
          key[0] !== "runs" ||
          key[1] !== runId ||
          key[2] !== "conflicts" ||
          typeof key[3] !== "object" ||
          key[3] === null
        ) {
          return false;
        }
        const params = key[3] as {
          cursor?: unknown;
          reconciliationFingerprint?: unknown;
          runVersion?: unknown;
        };
        return (
          params.reconciliationFingerprint !== identity.reconciliation_fingerprint ||
          params.runVersion !== identity.run_version ||
          !retainedCursors.has(typeof params.cursor === "string" ? params.cursor : null)
        );
      },
    });
  }, [identity, queryClient, runId, state.cursors, targetCursor]);

  const loadMore = useCallback(() => {
    if (identity === null || state.exhausted || state.nextCursor === null) {
      return;
    }
    if (state.cursors.includes(state.nextCursor)) {
      return;
    }
    // Pass the server's opaque continuation token through untouched.
    setTargetCursor(state.nextCursor);
  }, [identity, state.exhausted, state.nextCursor, state.cursors]);

  const select = useCallback((conflictId: string) => {
    dispatch({ type: "select", conflictId });
  }, []);

  const clearSelection = useCallback(() => {
    dispatch({ type: "clear-selection" });
  }, []);

  const reset = useCallback(() => {
    dispatch({ type: "reset" });
    setTargetCursor(null);
    // A reset means the resident view is discarded on purpose; the cached
    // pages for this run are dropped too, so the next render reloads the
    // first page instead of silently replaying the previous snapshot.
    queryClient.removeQueries({ queryKey: ["runs", runId, "conflicts"] });
  }, [queryClient, runId]);

  return {
    state,
    isFetchingPage: conflictsQuery.isFetching,
    pageError: conflictsQuery.error,
    loadMore,
    select,
    clearSelection,
    reset,
  };
}

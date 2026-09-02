/**
 * Pure pagination and selection state for reconciliation conflict pages.
 *
 * Pages are merged only when the run identity and the reconciliation
 * fingerprint of the incoming response match the resident snapshot exactly;
 * anything else is quarantined and counted so a re-indexed run can never
 * mix rows from two snapshots in one view. The resident cache is bounded:
 * at most MAX_RETAINED_PAGES pages stay in memory, and the oldest page is
 * evicted first once the bound is exceeded. Cursor strings received from
 * the server are opaque: they are passed through verbatim and never
 * derived, rewritten, or decoded.
 */
import type {
  ConflictPageResponse,
  ConflictResponse,
} from "../../api/generated/schema";
import { sameReconciliationIdentity, type ReconciliationIdentity } from "./coherence";

/** Server-side page bound; requested page size must never exceed it. */
export const MAX_CONFLICT_PAGE_SIZE = 100;

/** Requested page size for the conflicts collection. */
export const DEFAULT_CONFLICT_PAGE_SIZE = 100;

/**
 * Bounded client cache: 20 pages x 100 records = at most 2000 conflict
 * records resident at any moment, regardless of how large the run is.
 */
export const MAX_RETAINED_PAGES = 20;

/**
 * A selection is bound to both the conflict id and the reconciliation
 * fingerprint it was selected under, so a stale id can never be replayed
 * against a different snapshot.
 */
export interface ConflictSelection {
  conflictId: string;
  reconciliationFingerprint: string;
}

export type ConflictPagesEvent =
  | { type: "page-loaded"; page: ConflictPageResponse }
  | { type: "identity-changed"; identity: ReconciliationIdentity }
  | { type: "select"; conflictId: string }
  | { type: "clear-selection" }
  | { type: "reset" };

export interface ConflictPagesState {
  identity: ReconciliationIdentity | null;
  /** Request cursors of loaded pages in load order; the first page is "". */
  cursors: string[];
  /** Resident pages, bounded to MAX_RETAINED_PAGES (oldest evicted first). */
  pages: ConflictPageResponse[];
  /** Flattened conflicts of the resident pages. */
  conflicts: ConflictResponse[];
  residentCount: number;
  /** Total pages ever accepted (may exceed the resident bound). */
  loadedPages: number;
  /** Pages quarantined because their identity did not match the snapshot. */
  rejectedPages: number;
  /** Opaque continuation token at the end of the resident chain. */
  nextCursor: string | null;
  /** True when the server ended the loaded chain with a null cursor. */
  exhausted: boolean;
  selection: ConflictSelection | null;
}

export function initialConflictPagesState(): ConflictPagesState {
  return {
    identity: null,
    cursors: [],
    pages: [],
    conflicts: [],
    residentCount: 0,
    loadedPages: 0,
    rejectedPages: 0,
    nextCursor: null,
    exhausted: false,
    selection: null,
  };
}

/** The reconciliation snapshot a conflict page was persisted under. */
function pageIdentity(page: ConflictPageResponse): ReconciliationIdentity {
  return {
    run_id: page.run_id,
    run_version: page.run_version,
    reconciliation_fingerprint: page.reconciliation_fingerprint,
  };
}

/**
 * Append an identity-compatible page to the resident chain, evicting the
 * oldest page when the cache bound is exceeded. The request cursor recorded
 * for the incoming page is the continuation token the chain ended with
 * before the page arrived (the empty string for a first page).
 */
function appendResidentPage(
  state: ConflictPagesState,
  page: ConflictPageResponse,
): ConflictPagesState {
  const pages = [...state.pages, page];
  if (pages.length > MAX_RETAINED_PAGES) {
    pages.shift();
  }

  const requestCursor = state.pages.length === 0 ? "" : (state.nextCursor ?? "");
  const cursors = [...state.cursors, requestCursor];
  const boundedCursors =
    cursors.length > MAX_RETAINED_PAGES
      ? cursors.slice(cursors.length - MAX_RETAINED_PAGES)
      : cursors;

  const conflicts = pages.flatMap((resident) => resident.items);
  const lastPage = pages[pages.length - 1];
  const nextCursor = lastPage === undefined ? null : lastPage.next_cursor;

  return {
    ...state,
    pages,
    cursors: boundedCursors,
    conflicts,
    residentCount: conflicts.length,
    loadedPages: state.loadedPages + 1,
    nextCursor,
    exhausted: pages.length > 0 && nextCursor === null,
  };
}

export function reduceConflictPages(
  state: ConflictPagesState,
  event: ConflictPagesEvent,
): ConflictPagesState {
  switch (event.type) {
    case "page-loaded": {
      if (state.identity === null) {
        // The first accepted response defines the snapshot every later
        // page must match exactly.
        return appendResidentPage(
          { ...state, identity: pageIdentity(event.page) },
          event.page,
        );
      }
      if (!sameReconciliationIdentity(state.identity, pageIdentity(event.page))) {
        // Quarantine: a page from a different run version or reconciliation
        // snapshot is counted but never merged, so resident data stays one
        // coherent snapshot.
        return { ...state, rejectedPages: state.rejectedPages + 1 };
      }
      if (state.pages.length > 0 && state.nextCursor === event.page.next_cursor) {
        // The incoming page carries the continuation token already stored at
        // the end of the resident chain, so it re-describes a chain position
        // that is loaded; ignore it instead of double-counting the page.
        return state;
      }
      return appendResidentPage(state, event.page);
    }
    case "identity-changed":
      // Hard fence: data, page counts, and the selection are dropped so no
      // row or selection can survive a fingerprint or run version change.
      return { ...initialConflictPagesState(), identity: event.identity };
    case "select": {
      if (state.identity === null) {
        return state;
      }
      const resident = state.conflicts.some(
        (conflict) => conflict.conflict_id === event.conflictId,
      );
      if (!resident) {
        // Rows that are not resident have never been seen; selecting them
        // would promise data the client does not hold.
        return state;
      }
      return {
        ...state,
        selection: {
          conflictId: event.conflictId,
          reconciliationFingerprint: state.identity.reconciliation_fingerprint,
        },
      };
    }
    case "clear-selection":
      return state.selection === null ? state : { ...state, selection: null };
    case "reset":
      return initialConflictPagesState();
  }
}

/**
 * Selection keys bind the conflict id to the reconciliation fingerprint of
 * the snapshot it came from, so keys from different snapshots never collide.
 */
export function conflictKey(conflict: ConflictResponse, fingerprint: string): string {
  return `${fingerprint}:${conflict.conflict_id}`;
}

/**
 * Deterministic client-side filter over resident rows only. The filter
 * never triggers a fetch; it narrows what is already loaded, preserving
 * load order.
 */
export function filterConflicts(
  conflicts: ConflictResponse[],
  classification: string | null,
): ConflictResponse[] {
  if (classification === null) {
    return conflicts;
  }
  return conflicts.filter((conflict) => conflict.classification === classification);
}

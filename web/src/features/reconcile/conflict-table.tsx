/**
 * Semantic, keyboard-accessible table over the resident conflict rows.
 *
 * The table renders exactly what is loaded: the classification filter is a
 * deterministic client-side view over resident data and never triggers a
 * fetch, and pagination advances only through the explicit "Load more"
 * control. Rows carry roving tabindex and full arrow-key navigation, and
 * every selection change is announced through a polite live region.
 */
import { useMemo, useRef, useState } from "react";
import type { KeyboardEvent as ReactKeyboardEvent } from "react";

import type { ConflictResponse } from "../../api/generated/schema";
import { EmptyState } from "../../components/states/empty-state";
import { StatusBadge } from "../../components/ui/status-badge";
import { cn } from "../../lib/cn";
import {
  RECONCILIATION_CLASSIFICATIONS,
  classificationBadgeState,
  classificationLabel,
  classificationTone,
  differenceKindLabel,
  toClassificationValue,
  type ReconciliationClassificationValue,
} from "./classifications";
import { conflictKey, filterConflicts, type ConflictPagesState } from "./conflict-data";
import { BoundedValue } from "./value-display";
import { boundDisplayValue, toFieldDifferenceKind } from "./value-bounds";

/** The classification filter value carried by the "All" option. */
const ALL_CLASSIFICATIONS = "";

export interface ConflictTableProps {
  state: ConflictPagesState;
  /** Snapshot total from the reconciliation summary, when known. */
  totalLogicalCount: number | null;
  classificationFilter: ReconciliationClassificationValue | null;
  onClassificationFilterChange: (
    value: ReconciliationClassificationValue | null,
  ) => void;
  onLoadMore: () => void;
  isFetchingPage: boolean;
  onSelect: (conflict: ConflictResponse) => void;
  selectedConflictId: string | null;
  /** Coherent snapshot with zero conflicts. */
  empty?: boolean;
}

function ClassificationBadge(props: { classification: string }): React.JSX.Element {
  const known = toClassificationValue(props.classification);
  if (known === null) {
    // The runtime contract rejects unknown classifications before they reach
    // this view; if one still arrives it is shown verbatim and unstyled
    // instead of being presented under a wrong label.
    return (
      <StatusBadge state="neutral">
        {boundDisplayValue(props.classification, 48)}
      </StatusBadge>
    );
  }
  return (
    <StatusBadge state={classificationBadgeState(classificationTone(known))}>
      {classificationLabel(known)}
    </StatusBadge>
  );
}

function differenceSummary(differences: ConflictResponse["differences"]): string {
  if (differences.length === 0) {
    return "0";
  }
  const labels = differences.map((difference) => {
    const kind = toFieldDifferenceKind(difference.kind);
    return kind === null ? "unrecognized kind" : differenceKindLabel(kind);
  });
  return `${String(differences.length)} — ${labels.join(", ")}`;
}

function ReferenceList(props: {
  references: ConflictResponse["source_references"];
}): React.JSX.Element {
  if (props.references.length === 0) {
    return <span className="text-2xs text-muted">none</span>;
  }
  return (
    <ul className="space-y-0.5">
      {props.references.map((reference) => (
        <li
          key={`${String(reference.position)}-${reference.record_key}`}
          className="text-2xs text-muted"
        >
          <BoundedValue
            text={`#${String(reference.position)} ${reference.record_key}`}
            maxLength={80}
          />
        </li>
      ))}
    </ul>
  );
}

export function ConflictTable(props: ConflictTableProps): React.JSX.Element {
  const {
    state,
    totalLogicalCount,
    classificationFilter,
    onClassificationFilterChange,
    onLoadMore,
    isFetchingPage,
    onSelect,
    selectedConflictId,
    empty,
  } = props;

  const [activeIndex, setActiveIndex] = useState(0);
  const rowRefs = useRef<(HTMLTableRowElement | null)[]>([]);

  const fingerprint = state.identity?.reconciliation_fingerprint ?? null;
  const rows = useMemo(
    () => filterConflicts(state.conflicts, classificationFilter),
    [state.conflicts, classificationFilter],
  );

  if (empty === true) {
    return (
      <div data-testid="conflict-table" className="space-y-3">
        <EmptyState
          title="No conflicts recorded"
          description="The reconciliation snapshot found no differences for this run."
        />
      </div>
    );
  }

  const selectedIndex = rows.findIndex(
    (conflict) => conflict.conflict_id === selectedConflictId,
  );
  const lastRowIndex = rows.length - 1;
  const rovingIndex =
    selectedIndex >= 0
      ? selectedIndex
      : Math.min(Math.max(activeIndex, 0), Math.max(lastRowIndex, 0));

  const focusRow = (index: number): void => {
    const clamped = Math.min(Math.max(index, 0), lastRowIndex);
    setActiveIndex(clamped);
    rowRefs.current[clamped]?.focus();
  };

  const handleRowKeyDown = (
    event: ReactKeyboardEvent<HTMLTableRowElement>,
    index: number,
    conflict: ConflictResponse,
  ): void => {
    if (event.key === "ArrowDown") {
      event.preventDefault();
      focusRow(index + 1);
    } else if (event.key === "ArrowUp") {
      event.preventDefault();
      focusRow(index - 1);
    } else if (event.key === "Home") {
      event.preventDefault();
      focusRow(0);
    } else if (event.key === "End") {
      event.preventDefault();
      focusRow(lastRowIndex);
    } else if (event.key === "Enter" || event.key === " ") {
      event.preventDefault();
      onSelect(conflict);
    }
  };

  const ariaRowCount = totalLogicalCount ?? state.residentCount;
  const filterActive = classificationFilter !== null;
  const showLoadMore = !state.exhausted && state.nextCursor !== null;

  return (
    <div data-testid="conflict-table" className="space-y-3">
      {state.rejectedPages > 0 && (
        <p
          role="alert"
          data-testid="conflict-pages-rejected"
          className="rounded-md border border-warning/30 bg-warning/10 p-2 text-xs font-semibold text-warning"
        >
          {String(state.rejectedPages)} conflict pages were received for a different
          reconciliation snapshot and were discarded (refresh required)
        </p>
      )}

      <div className="flex flex-wrap items-end justify-between gap-2">
        <div className="flex items-end gap-2">
          <label
            htmlFor="conflict-classification-filter"
            className="text-xs font-medium text-foreground"
          >
            Classification
          </label>
          <select
            id="conflict-classification-filter"
            data-testid="conflict-filter"
            className="rounded border border-border bg-surface px-2 py-1.5 text-xs text-foreground"
            value={classificationFilter ?? ALL_CLASSIFICATIONS}
            onChange={(event) => {
              const value = event.target.value;
              onClassificationFilterChange(
                value === ALL_CLASSIFICATIONS ? null : toClassificationValue(value),
              );
            }}
          >
            <option value={ALL_CLASSIFICATIONS}>All</option>
            {RECONCILIATION_CLASSIFICATIONS.map((value) => (
              <option key={value} value={value}>
                {classificationLabel(value)}
              </option>
            ))}
          </select>
        </div>
        <p className="text-2xs text-muted" role="status">
          {filterActive
            ? `${String(rows.length)} of ${String(state.residentCount)} loaded — filtered view of loaded data; no additional pages are fetched`
            : `showing ${String(rows.length)} of ${String(state.residentCount)} loaded${
                totalLogicalCount === null
                  ? ""
                  : ` of ${String(totalLogicalCount)} total`
              }`}
        </p>
      </div>

      {rows.length === 0 ? (
        <p className="text-xs text-muted">
          No loaded conflicts match this filter. Clear the filter to see resident rows.
        </p>
      ) : (
        <table className="w-full text-left text-xs" aria-rowcount={ariaRowCount}>
          <caption className="sr-only">Reconciliation conflicts (loaded pages)</caption>
          <thead>
            <tr className="border-b border-border text-muted">
              <th scope="col" className="py-1.5 pr-3 font-medium">
                Record key
              </th>
              <th scope="col" className="py-1.5 pr-3 font-medium">
                Classification
              </th>
              <th scope="col" className="py-1.5 pr-3 font-medium">
                Differences
              </th>
              <th scope="col" className="py-1.5 pr-3 font-medium">
                Source refs
              </th>
              <th scope="col" className="py-1.5 font-medium">
                Target refs
              </th>
            </tr>
          </thead>
          <tbody>
            {rows.map((conflict, index) => {
              const isSelected = conflict.conflict_id === selectedConflictId;
              return (
                <tr
                  key={
                    fingerprint === null
                      ? conflict.conflict_id
                      : conflictKey(conflict, fingerprint)
                  }
                  ref={(node) => {
                    rowRefs.current[index] = node;
                  }}
                  data-testid={`conflict-row-${conflict.conflict_id}`}
                  tabIndex={index === rovingIndex ? 0 : -1}
                  aria-selected={isSelected}
                  aria-rowindex={index + 2}
                  onClick={() => {
                    onSelect(conflict);
                  }}
                  onKeyDown={(event) => {
                    handleRowKeyDown(event, index, conflict);
                  }}
                  className={cn(
                    "cursor-pointer border-b border-border/60 focus-visible:outline-2 focus-visible:-outline-offset-2 focus-visible:outline-[var(--focus-ring)]",
                    isSelected && "bg-active/10",
                  )}
                >
                  <td className="py-1.5 pr-3">
                    <BoundedValue text={conflict.canonical_key} maxLength={96} />
                  </td>
                  <td className="py-1.5 pr-3">
                    <ClassificationBadge classification={conflict.classification} />
                  </td>
                  <td className="py-1.5 pr-3">
                    <BoundedValue
                      text={differenceSummary(conflict.differences)}
                      maxLength={120}
                    />
                  </td>
                  <td className="py-1.5 pr-3">
                    <ReferenceList references={conflict.source_references} />
                  </td>
                  <td className="py-1.5">
                    <ReferenceList references={conflict.target_references} />
                  </td>
                </tr>
              );
            })}
          </tbody>
        </table>
      )}

      <div className="flex flex-wrap items-center gap-3">
        {showLoadMore && (
          <button
            type="button"
            data-testid="conflict-load-more"
            onClick={onLoadMore}
            disabled={isFetchingPage}
            className="inline-flex items-center justify-center rounded-md bg-primary px-4 py-2 text-sm font-semibold text-primary-foreground shadow-sm transition-colors hover:bg-primary/90 disabled:pointer-events-none disabled:opacity-50 focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-[var(--focus-ring)]"
          >
            {isFetchingPage ? "Loading…" : "Load more conflicts"}
          </button>
        )}
        <p className="text-2xs text-muted">
          {String(state.loadedPages)} page(s) loaded; {String(state.residentCount)}{" "}
          conflicts resident
        </p>
      </div>

      <p
        aria-live="polite"
        role="status"
        data-testid="conflict-selected-announce"
        className="text-2xs text-muted"
      >
        {selectedConflictId === null ? "" : `Conflict ${selectedConflictId} selected`}
      </p>
    </div>
  );
}

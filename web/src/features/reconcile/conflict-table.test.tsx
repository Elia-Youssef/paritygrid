import { fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { useState } from "react";
import { describe, expect, it, vi } from "vitest";

import type {
  ConflictPageResponse,
  ConflictResponse,
} from "../../api/generated/schema";
import { expectNoAccessibilityViolations } from "../../test/axe";
import {
  classificationLabel,
  type ReconciliationClassificationValue,
} from "./classifications";
import {
  initialConflictPagesState,
  reduceConflictPages,
  type ConflictPagesState,
} from "./conflict-data";
import { ConflictTable } from "./conflict-table";

function fireEventKeyDown(element: Element, key: string): void {
  fireEvent.keyDown(element, { key });
}

const IDENTITY = {
  run_id: "run-001",
  run_version: 3,
  reconciliation_fingerprint: "fp-001",
};

function makeConflict(id: string, classification: string): ConflictResponse {
  return {
    schema_version: 1,
    conflict_id: id,
    canonical_key: `key-${id}`,
    classification,
    source_references: [
      { position: 0, record_key: `src-${id}` },
      { position: 1, record_key: `src2-${id}` },
    ],
    target_references: [{ position: 0, record_key: `tgt-${id}` }],
    differences: [
      {
        field: "amount",
        kind: "value_mismatch",
        source_text: "100",
        target_text: "200",
      },
      {
        field: "address.city",
        kind: "missing_on_target",
        source_text: "Berlin",
        target_text: "",
      },
    ],
    suggested_resolution: "update_target",
    created_at: "2026-01-01T00:00:00Z",
  };
}

function makePage(
  items: ConflictResponse[],
  nextCursor: string | null,
): ConflictPageResponse {
  return {
    schema_version: 1,
    run_id: IDENTITY.run_id,
    run_version: IDENTITY.run_version,
    state: "complete",
    observed_at: "2026-01-01T00:00:00Z",
    reconciliation_fingerprint: IDENTITY.reconciliation_fingerprint,
    limit: 100,
    next_cursor: nextCursor,
    items,
  };
}

function stateWithThreeConflicts(nextCursor: string | null): ConflictPagesState {
  const page = makePage(
    [
      makeConflict("c-1", "field_mismatch"),
      makeConflict("c-2", "missing_from_target"),
      makeConflict("c-3", "duplicate_source"),
    ],
    nextCursor,
  );
  return reduceConflictPages(initialConflictPagesState(), {
    type: "page-loaded",
    page,
  });
}

function TableHarness(props: {
  state: ConflictPagesState;
  totalLogicalCount?: number | null;
  onLoadMore?: () => void;
  isFetchingPage?: boolean;
  empty?: boolean;
}): React.JSX.Element {
  const [filter, setFilter] = useState<ReconciliationClassificationValue | null>(null);
  const [selectedId, setSelectedId] = useState<string | null>(null);
  return (
    <ConflictTable
      state={props.state}
      totalLogicalCount={props.totalLogicalCount ?? null}
      classificationFilter={filter}
      onClassificationFilterChange={(value) => {
        setFilter(value);
      }}
      onLoadMore={props.onLoadMore ?? (() => undefined)}
      isFetchingPage={props.isFetchingPage ?? false}
      onSelect={(conflict) => {
        setSelectedId(conflict.conflict_id);
      }}
      selectedConflictId={selectedId}
      empty={props.empty}
    />
  );
}

describe("ConflictTable", () => {
  it("renders resident rows with keys, classification labels, caption, and row count", () => {
    const { container } = render(
      <TableHarness
        state={stateWithThreeConflicts("CNF-000003")}
        totalLogicalCount={7}
      />,
    );

    const table = screen.getByRole("table", {
      name: "Reconciliation conflicts (loaded pages)",
    });
    expect(table).toHaveAttribute("aria-rowcount", "7");

    for (const id of ["c-1", "c-2", "c-3"]) {
      const row = screen.getByTestId(`conflict-row-${id}`);
      expect(within(row).getByText(`key-${id}`)).toBeInTheDocument();
    }
    expect(
      within(screen.getByTestId("conflict-row-c-1")).getByText(
        classificationLabel("field_mismatch"),
      ),
    ).toBeInTheDocument();
    expect(
      within(screen.getByTestId("conflict-row-c-2")).getByText(
        classificationLabel("missing_from_target"),
      ),
    ).toBeInTheDocument();
    // Difference kinds are summarized with their human labels.
    expect(
      within(screen.getByTestId("conflict-row-c-1")).getByText(/missing on target/i),
    ).toBeInTheDocument();
    // References render as position + record key.
    expect(
      within(screen.getByTestId("conflict-row-c-1")).getByText(/#1 src2-c-1/),
    ).toBeInTheDocument();
    expect(container).toBeInTheDocument();
  });

  it("navigates rows by keyboard and announces the selection", async () => {
    const onSelect = vi.fn();
    function SelectingHarness(): React.JSX.Element {
      const [selectedId, setSelectedId] = useState<string | null>(null);
      return (
        <ConflictTable
          state={stateWithThreeConflicts(null)}
          totalLogicalCount={null}
          classificationFilter={null}
          onClassificationFilterChange={() => undefined}
          onLoadMore={() => undefined}
          isFetchingPage={false}
          onSelect={(conflict) => {
            onSelect(conflict);
            setSelectedId(conflict.conflict_id);
          }}
          selectedConflictId={selectedId}
        />
      );
    }
    const { container } = render(<SelectingHarness />);

    const firstRow = screen.getByTestId("conflict-row-c-1");
    const secondRow = screen.getByTestId("conflict-row-c-2");
    const lastRow = screen.getByTestId("conflict-row-c-3");

    firstRow.focus();
    expect(firstRow).toHaveFocus();
    expect(firstRow).toHaveAttribute("tabindex", "0");
    // The visible focus ring is carried by a focus-visible outline class.
    expect(firstRow).toHaveClass("focus-visible:outline-2");

    fireEventKeyDown(firstRow, "ArrowDown");
    expect(secondRow).toHaveFocus();

    fireEventKeyDown(secondRow, "Enter");
    expect(onSelect).toHaveBeenCalledTimes(1);
    expect(onSelect.mock.calls[0]?.[0]).toMatchObject({ conflict_id: "c-2" });
    expect(secondRow).toHaveAttribute("aria-selected", "true");
    await waitFor(() => {
      expect(screen.getByTestId("conflict-selected-announce")).toHaveTextContent(
        "Conflict c-2 selected",
      );
    });

    fireEventKeyDown(secondRow, "End");
    expect(lastRow).toHaveFocus();
    fireEventKeyDown(lastRow, "Home");
    expect(firstRow).toHaveFocus();
    // Arrow navigation clamps at the edges instead of wrapping around.
    fireEventKeyDown(firstRow, "ArrowUp");
    expect(firstRow).toHaveFocus();
    fireEventKeyDown(firstRow, "End");
    expect(lastRow).toHaveFocus();

    fireEventKeyDown(lastRow, " ");
    expect(onSelect).toHaveBeenCalledTimes(2);
    expect(onSelect.mock.calls[1]?.[0]).toMatchObject({ conflict_id: "c-3" });

    await expectNoAccessibilityViolations(container);
  });

  it("filters resident rows without refetching and restores on reset", async () => {
    const user = userEvent.setup();
    const onLoadMore = vi.fn();
    function FilteringHarness(): React.JSX.Element {
      const [filter, setFilter] = useState<ReconciliationClassificationValue | null>(
        null,
      );
      return (
        <ConflictTable
          state={stateWithThreeConflicts("CNF-000003")}
          totalLogicalCount={null}
          classificationFilter={filter}
          onClassificationFilterChange={setFilter}
          onLoadMore={onLoadMore}
          isFetchingPage={false}
          onSelect={() => undefined}
          selectedConflictId={null}
        />
      );
    }
    const { container } = render(<FilteringHarness />);

    await user.selectOptions(screen.getByTestId("conflict-filter"), "field_mismatch");
    expect(screen.getAllByTestId(/^conflict-row-/)).toHaveLength(1);
    expect(screen.getByTestId("conflict-row-c-1")).toBeInTheDocument();
    expect(screen.getByText(/filtered view of loaded data/i)).toBeInTheDocument();
    expect(onLoadMore).not.toHaveBeenCalled();

    await user.selectOptions(screen.getByTestId("conflict-filter"), "");
    expect(screen.getAllByTestId(/^conflict-row-/)).toHaveLength(3);
    expect(screen.queryByText(/filtered view of loaded data/i)).not.toBeInTheDocument();

    await expectNoAccessibilityViolations(container);
  });

  it("drives pagination only through the visible load-more control", async () => {
    const user = userEvent.setup();
    const onLoadMore = vi.fn();
    const { rerender } = render(
      <ConflictTable
        state={stateWithThreeConflicts("CNF-000003")}
        totalLogicalCount={30}
        classificationFilter={null}
        onClassificationFilterChange={() => undefined}
        onLoadMore={onLoadMore}
        isFetchingPage={false}
        onSelect={() => undefined}
        selectedConflictId={null}
      />,
    );

    const button = screen.getByTestId("conflict-load-more");
    await user.click(button);
    expect(onLoadMore).toHaveBeenCalledTimes(1);
    expect(screen.getByText(/3 of 3 loaded/)).toBeInTheDocument();
    expect(screen.getByText(/of 30 total/)).toBeInTheDocument();

    rerender(
      <ConflictTable
        state={stateWithThreeConflicts("CNF-000003")}
        totalLogicalCount={30}
        classificationFilter={null}
        onClassificationFilterChange={() => undefined}
        onLoadMore={onLoadMore}
        isFetchingPage={true}
        onSelect={() => undefined}
        selectedConflictId={null}
      />,
    );
    expect(screen.getByTestId("conflict-load-more")).toBeDisabled();

    rerender(
      <ConflictTable
        state={stateWithThreeConflicts(null)}
        totalLogicalCount={30}
        classificationFilter={null}
        onClassificationFilterChange={() => undefined}
        onLoadMore={onLoadMore}
        isFetchingPage={false}
        onSelect={() => undefined}
        selectedConflictId={null}
      />,
    );
    expect(screen.queryByTestId("conflict-load-more")).not.toBeInTheDocument();
  });

  it("warns visibly when pages from another snapshot were discarded", () => {
    let state = stateWithThreeConflicts(null);
    state = reduceConflictPages(state, {
      type: "page-loaded",
      page: { ...makePage([], null), run_id: "run-other" },
    });

    render(
      <ConflictTable
        state={state}
        totalLogicalCount={null}
        classificationFilter={null}
        onClassificationFilterChange={() => undefined}
        onLoadMore={() => undefined}
        isFetchingPage={false}
        onSelect={() => undefined}
        selectedConflictId={null}
      />,
    );

    expect(screen.getByTestId("conflict-pages-rejected")).toBeVisible();
    expect(screen.getByTestId("conflict-pages-rejected")).toHaveTextContent(
      /1 conflict pages were received for a different reconciliation snapshot and were discarded/,
    );
    // The table carries no repair actions of its own.
    expect(screen.queryByText(/repair/i)).not.toBeInTheDocument();
  });

  it("renders the empty state for a coherent snapshot without conflicts", async () => {
    const { container } = render(
      <TableHarness state={initialConflictPagesState()} empty />,
    );
    expect(screen.getByText("No conflicts recorded")).toBeVisible();
    expect(screen.queryByRole("table")).not.toBeInTheDocument();
    await expectNoAccessibilityViolations(container);
  });

  it("passes axe on the populated table", async () => {
    const { container } = render(
      <TableHarness
        state={stateWithThreeConflicts("CNF-000003")}
        totalLogicalCount={7}
      />,
    );
    await expectNoAccessibilityViolations(container);
  });
});

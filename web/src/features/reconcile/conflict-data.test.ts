import { describe, expect, it } from "vitest";

import type {
  ConflictPageResponse,
  ConflictResponse,
} from "../../api/generated/schema";
import {
  conflictKey,
  filterConflicts,
  initialConflictPagesState,
  MAX_RETAINED_PAGES,
  reduceConflictPages,
  type ConflictPagesState,
} from "./conflict-data";

const BASE_IDENTITY = {
  run_id: "run-001",
  run_version: 3,
  reconciliation_fingerprint: "fp-001",
};

function makeConflict(id: string, classification = "field_mismatch"): ConflictResponse {
  return {
    schema_version: 1,
    conflict_id: id,
    canonical_key: `key-${id}`,
    classification,
    source_references: [{ position: 0, record_key: `src-${id}` }],
    target_references: [{ position: 0, record_key: `tgt-${id}` }],
    differences: [
      {
        field: "amount",
        kind: "value_mismatch",
        source_text: "100",
        target_text: "200",
      },
    ],
    suggested_resolution: "update_target",
    created_at: "2026-01-01T00:00:00Z",
  };
}

function makePage(
  overrides: Partial<ConflictPageResponse> & { items?: ConflictResponse[] },
): ConflictPageResponse {
  return {
    schema_version: 1,
    run_id: BASE_IDENTITY.run_id,
    run_version: BASE_IDENTITY.run_version,
    state: "complete",
    observed_at: "2026-01-01T00:00:00Z",
    reconciliation_fingerprint: BASE_IDENTITY.reconciliation_fingerprint,
    limit: 100,
    next_cursor: null,
    items: [],
    ...overrides,
  };
}

function loadPage(state: ConflictPagesState, page: ConflictPageResponse) {
  return reduceConflictPages(state, { type: "page-loaded", page });
}

function identityOf(page: ConflictPageResponse) {
  return {
    run_id: page.run_id,
    run_version: page.run_version,
    reconciliation_fingerprint: page.reconciliation_fingerprint,
  };
}

describe("conflict page state", () => {
  it("adopts the identity from the first page and flattens same-identity pages in order", () => {
    let state = initialConflictPagesState();

    const first = makePage({
      items: [makeConflict("c-1"), makeConflict("c-2")],
      next_cursor: "CNF-000002",
    });
    state = loadPage(state, first);

    expect(state.identity).toEqual(BASE_IDENTITY);
    expect(state.cursors).toEqual([""]);
    expect(state.conflicts.map((conflict) => conflict.conflict_id)).toEqual([
      "c-1",
      "c-2",
    ]);
    expect(state.residentCount).toBe(2);
    expect(state.loadedPages).toBe(1);
    expect(state.nextCursor).toBe("CNF-000002");
    expect(state.exhausted).toBe(false);

    const second = makePage({
      items: [makeConflict("c-3")],
      next_cursor: null,
    });
    state = loadPage(state, second);

    expect(state.cursors).toEqual(["", "CNF-000002"]);
    expect(state.conflicts.map((conflict) => conflict.conflict_id)).toEqual([
      "c-1",
      "c-2",
      "c-3",
    ]);
    expect(state.residentCount).toBe(3);
    expect(state.loadedPages).toBe(2);
    expect(state.nextCursor).toBeNull();
    expect(state.exhausted).toBe(true);
  });

  it("quarantines pages whose run_id, run_version, or fingerprint differ", () => {
    const resident = loadPage(
      initialConflictPagesState(),
      makePage({ items: [makeConflict("c-1")], next_cursor: "CNF-000002" }),
    );

    const mismatchedRun = loadPage(
      resident,
      makePage({ run_id: "run-other", items: [makeConflict("c-x")] }),
    );
    const mismatchedVersion = loadPage(
      resident,
      makePage({
        run_version: BASE_IDENTITY.run_version + 99,
        items: [makeConflict("c-x")],
      }),
    );
    const mismatchedFingerprint = loadPage(
      resident,
      makePage({
        reconciliation_fingerprint: "fp-other",
        items: [makeConflict("c-x")],
      }),
    );

    for (const quarantined of [
      mismatchedRun,
      mismatchedVersion,
      mismatchedFingerprint,
    ]) {
      expect(quarantined.rejectedPages).toBe(1);
      expect(quarantined.pages).toHaveLength(1);
      expect(quarantined.conflicts.map((c) => c.conflict_id)).toEqual(["c-1"]);
      expect(quarantined.cursors).toEqual(resident.cursors);
      expect(quarantined.loadedPages).toBe(1);
      expect(quarantined.nextCursor).toBe("CNF-000002");
      expect(quarantined.selection).toBeNull();
    }
    expect(mismatchedRun.identity).toEqual(resident.identity);
  });

  it("binds selection keys to the fingerprint and fences them on identity changes", () => {
    let state = loadPage(
      initialConflictPagesState(),
      makePage({ items: [makeConflict("c-1")], next_cursor: "CNF-000002" }),
    );

    expect(conflictKey(makeConflict("c-1"), "fp-001")).toBe("fp-001:c-1");

    state = reduceConflictPages(state, { type: "select", conflictId: "c-1" });
    expect(state.selection).toEqual({
      conflictId: "c-1",
      reconciliationFingerprint: "fp-001",
    });

    // Selecting an id that is not resident changes nothing.
    const unchanged = reduceConflictPages(state, {
      type: "select",
      conflictId: "c-unknown",
    });
    expect(unchanged.selection).toEqual(state.selection);

    // A fingerprint change fences the selection and drops the data.
    const fingerprintChanged = reduceConflictPages(state, {
      type: "identity-changed",
      identity: { ...BASE_IDENTITY, reconciliation_fingerprint: "fp-002" },
    });
    expect(fingerprintChanged.selection).toBeNull();
    expect(fingerprintChanged.conflicts).toHaveLength(0);
    expect(fingerprintChanged.identity).toEqual({
      ...BASE_IDENTITY,
      reconciliation_fingerprint: "fp-002",
    });

    // A run version change fences the selection as well.
    const versionChanged = reduceConflictPages(state, {
      type: "identity-changed",
      identity: { ...BASE_IDENTITY, run_version: BASE_IDENTITY.run_version + 1 },
    });
    expect(versionChanged.selection).toBeNull();
    expect(versionChanged.pages).toHaveLength(0);
    expect(versionChanged.loadedPages).toBe(0);
  });

  it("keeps the resident cache bounded, evicting oldest pages first", () => {
    let state = initialConflictPagesState();
    const totalPageLoads = MAX_RETAINED_PAGES + 2;

    for (let index = 0; index < totalPageLoads; index += 1) {
      state = loadPage(
        state,
        makePage({
          items: [makeConflict(`c-${String(index)}`)],
          next_cursor:
            index + 1 === totalPageLoads
              ? null
              : `CNF-${String(index + 1).padStart(6, "0")}`,
        }),
      );
    }

    expect(state.loadedPages).toBe(totalPageLoads);
    expect(state.pages).toHaveLength(MAX_RETAINED_PAGES);
    expect(state.cursors).toHaveLength(MAX_RETAINED_PAGES);
    // The two oldest pages were evicted; the resident window starts at page 2.
    expect(state.conflicts[0]?.conflict_id).toBe("c-2");
    expect(state.conflicts).toHaveLength(MAX_RETAINED_PAGES);
    expect(state.residentCount).toBeLessThanOrEqual(MAX_RETAINED_PAGES * 100);
    expect(state.cursors[0]).toBe("CNF-000002");
    expect(state.exhausted).toBe(true);
  });

  it("filters resident conflicts by classification deterministically", () => {
    const conflicts = [
      makeConflict("c-1", "field_mismatch"),
      makeConflict("c-2", "missing_from_target"),
      makeConflict("c-3", "field_mismatch"),
      makeConflict("c-4", "duplicate_source"),
    ];

    expect(filterConflicts(conflicts, null)).toEqual(conflicts);
    expect(
      filterConflicts(conflicts, "field_mismatch").map((c) => c.conflict_id),
    ).toEqual(["c-1", "c-3"]);
    expect(
      filterConflicts(conflicts, "missing_from_target").map((c) => c.conflict_id),
    ).toEqual(["c-2"]);
    expect(
      filterConflicts(conflicts, "duplicate_source").map((c) => c.conflict_id),
    ).toEqual(["c-4"]);
    expect(filterConflicts(conflicts, "missing_from_source")).toEqual([]);
    // The unfiltered result preserves the original order and reference.
    expect(filterConflicts(conflicts, null).map((c) => c.conflict_id)).toEqual([
      "c-1",
      "c-2",
      "c-3",
      "c-4",
    ]);
  });

  it("treats a duplicate page delivery as idempotent", () => {
    const page = makePage({
      items: [makeConflict("c-1")],
      next_cursor: "CNF-000002",
    });

    const loaded = loadPage(initialConflictPagesState(), page);
    const duplicate = loadPage(loaded, page);

    expect(duplicate).toBe(loaded);
    expect(duplicate.loadedPages).toBe(1);
    expect(duplicate.pages).toHaveLength(1);
    expect(duplicate.cursors).toEqual([""]);
    expect(duplicate.residentCount).toBe(1);

    // The final page of a chain is also idempotent when re-delivered.
    const finalPage = makePage({ items: [makeConflict("c-2")], next_cursor: null });
    const exhausted = loadPage(loaded, finalPage);
    const duplicateFinal = loadPage(exhausted, finalPage);
    expect(duplicateFinal).toBe(exhausted);
    expect(duplicateFinal.loadedPages).toBe(2);
  });

  it("supports clear-selection and reset", () => {
    let state = loadPage(
      initialConflictPagesState(),
      makePage({ items: [makeConflict("c-1")], next_cursor: "CNF-000002" }),
    );
    state = reduceConflictPages(state, { type: "select", conflictId: "c-1" });
    expect(state.selection).not.toBeNull();

    const cleared = reduceConflictPages(state, { type: "clear-selection" });
    expect(cleared.selection).toBeNull();
    expect(cleared.conflicts).toEqual(state.conflicts);

    const reset = reduceConflictPages(cleared, { type: "reset" });
    expect(reset).toEqual(initialConflictPagesState());

    // An identity-changed event adopts the new identity on an empty cache.
    const adopted = reduceConflictPages(initialConflictPagesState(), {
      type: "identity-changed",
      identity: identityOf(makePage({})),
    });
    expect(adopted.identity).toEqual(BASE_IDENTITY);
    expect(adopted.conflicts).toHaveLength(0);
  });
});

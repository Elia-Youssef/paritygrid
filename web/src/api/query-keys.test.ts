import { describe, expect, it } from "vitest";

import { canMergeConflictPages, queryKeys } from "./query-keys";
import { createQueryClient } from "./query-client";

describe("query key factories", () => {
  it("produces stable keys for identical inputs", () => {
    expect(queryKeys.runDetail("run-1")).toEqual(queryKeys.runDetail("run-1"));
    expect(queryKeys.runList({ limit: 50, cursor: "abc" })).toEqual(
      queryKeys.runList({ limit: 50, cursor: "abc" }),
    );
    expect(queryKeys.runList({})).toEqual(
      queryKeys.runList({ limit: undefined, cursor: undefined }),
    );
  });

  it("changes the key for every result-changing parameter", () => {
    expect(queryKeys.runList({ limit: 50, cursor: "a" })).not.toEqual(
      queryKeys.runList({ limit: 50, cursor: "b" }),
    );
    expect(queryKeys.runList({ limit: 10, cursor: "a" })).not.toEqual(
      queryKeys.runList({ limit: 50, cursor: "a" }),
    );
    expect(queryKeys.runDetail("run-1")).not.toEqual(queryKeys.runDetail("run-2"));
    expect(queryKeys.repairPlan("plan-1")).not.toEqual(queryKeys.repairPlan("plan-2"));
  });

  it("keys conflict pages with reconciliation coherence parameters", () => {
    const base = { limit: 50, cursor: undefined };
    expect(queryKeys.conflicts("run-1", base)).not.toEqual(
      queryKeys.conflicts("run-1", { ...base, reconciliationFingerprint: "fp-1" }),
    );
    expect(
      queryKeys.conflicts("run-1", { ...base, reconciliationFingerprint: "fp-1" }),
    ).not.toEqual(
      queryKeys.conflicts("run-1", { ...base, reconciliationFingerprint: "fp-2" }),
    );
    expect(queryKeys.conflicts("run-1", { ...base, runVersion: 1 })).not.toEqual(
      queryKeys.conflicts("run-1", { ...base, runVersion: 2 }),
    );
    expect(
      queryKeys.conflicts("run-2", { ...base, reconciliationFingerprint: "fp-1" }),
    ).not.toEqual(
      queryKeys.conflicts("run-1", { ...base, reconciliationFingerprint: "fp-1" }),
    );
  });

  it("invalidates run-scoped entries hierarchically", async () => {
    const queryClient = createQueryClient();
    await queryClient.fetchQuery({
      queryKey: queryKeys.runDetail("run-1"),
      queryFn: () => Promise.resolve({ version: 1 }),
    });
    await queryClient.fetchQuery({
      queryKey: queryKeys.conflicts("run-1", {}),
      queryFn: () => Promise.resolve({ page: 1 }),
    });
    await queryClient.fetchQuery({
      queryKey: queryKeys.capabilities(),
      queryFn: () => Promise.resolve({ ok: true }),
    });

    await queryClient.invalidateQueries({ queryKey: queryKeys.runsRoot() });

    expect(queryClient.getQueryState(queryKeys.runDetail("run-1"))?.isInvalidated).toBe(
      true,
    );
    expect(
      queryClient.getQueryState(queryKeys.conflicts("run-1", {}))?.isInvalidated,
    ).toBe(true);
    expect(queryClient.getQueryState(queryKeys.capabilities())?.isInvalidated).toBe(
      false,
    );
  });

  it("never merges conflict pages across identities or fingerprints", () => {
    const base = {
      run_id: "run-1",
      run_version: 3,
      reconciliation_fingerprint: "fp-1",
    };
    expect(canMergeConflictPages(base, base)).toBe(true);
    expect(
      canMergeConflictPages(base, { ...base, reconciliation_fingerprint: "fp-2" }),
    ).toBe(false);
    expect(canMergeConflictPages(base, { ...base, run_version: 4 })).toBe(false);
    expect(canMergeConflictPages(base, { ...base, run_id: "run-2" })).toBe(false);
  });
});

describe("remaining key factories", () => {
  it("keys the live channel and health entries stably", () => {
    expect(queryKeys.runLive("run-1")).toEqual(queryKeys.runLive("run-1"));
    expect(queryKeys.runLive("run-1")).not.toEqual(queryKeys.runLive("run-2"));
    expect(queryKeys.health()).toEqual(["health"]);
    expect(queryKeys.runsRoot()).toEqual(["runs"]);
    expect(queryKeys.reconciliation("run-1")).toEqual([
      "runs",
      "run-1",
      "reconciliation",
    ]);
    expect(queryKeys.capabilities()).toEqual(["system", "capabilities"]);
  });
});

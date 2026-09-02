import { describe, expect, it } from "vitest";

import type { ReconciliationResponse } from "../../api/generated/schema";
import {
  checkIdentityCoherence,
  sameReconciliationIdentity,
  summarizeIncompatibleSnapshot,
  type ReconciliationIdentity,
} from "./coherence";

const IDENTITY: ReconciliationIdentity = {
  run_id: "run_recon-001",
  run_version: 4,
  reconciliation_fingerprint: "a".repeat(64),
};

function snapshot(overrides: Partial<ReconciliationResponse>): ReconciliationResponse {
  return {
    schema_version: 1,
    run_id: "run_recon-001",
    run_version: 4,
    state: "succeeded",
    observed_at: "2026-01-02 03:04:05",
    reconciliation_fingerprint: "a".repeat(64),
    source_input_identity: "b".repeat(64),
    target_input_identity: "c".repeat(64),
    total_count: 2,
    counts: {
      match: 1,
      missing_from_target: 0,
      missing_from_source: 0,
      field_mismatch: 1,
      duplicate_source: 0,
      duplicate_target: 0,
      duplicate_both: 0,
    },
    analytical_query_version: 2,
    reconciliation_observed_at: "2026-01-02 03:04:06",
    ...overrides,
  };
}

describe("checkIdentityCoherence", () => {
  it("accepts an identical identity", () => {
    expect(checkIdentityCoherence(IDENTITY, IDENTITY)).toEqual({ ok: true });
    expect(
      checkIdentityCoherence(IDENTITY, {
        run_id: "run_recon-001",
        run_version: 4,
        reconciliation_fingerprint: "a".repeat(64),
      }),
    ).toEqual({ ok: true });
  });

  it("reports a reason for each differing identity field", () => {
    const runIdChanged = checkIdentityCoherence(IDENTITY, {
      ...IDENTITY,
      run_id: "run_other-002",
    });
    expect(runIdChanged.ok).toBe(false);
    if (!runIdChanged.ok) {
      expect(runIdChanged.reasons).toHaveLength(1);
      expect(runIdChanged.reasons[0]).toContain("run id");
      expect(runIdChanged.reasons[0]).toContain("run_other-002");
    }

    const versionChanged = checkIdentityCoherence(IDENTITY, {
      ...IDENTITY,
      run_version: 5,
    });
    expect(versionChanged.ok).toBe(false);
    if (!versionChanged.ok) {
      expect(versionChanged.reasons).toHaveLength(1);
      expect(versionChanged.reasons[0]).toContain("run version");
      expect(versionChanged.reasons[0]).toContain("5");
    }

    const fingerprintChanged = checkIdentityCoherence(IDENTITY, {
      ...IDENTITY,
      reconciliation_fingerprint: "f".repeat(64),
    });
    expect(fingerprintChanged.ok).toBe(false);
    if (!fingerprintChanged.ok) {
      expect(fingerprintChanged.reasons).toHaveLength(1);
      expect(fingerprintChanged.reasons[0]).toContain("reconciliation fingerprint");
    }

    const everythingChanged = checkIdentityCoherence(IDENTITY, {
      run_id: "run_other-002",
      run_version: 5,
      reconciliation_fingerprint: "f".repeat(64),
    });
    expect(everythingChanged.ok).toBe(false);
    if (!everythingChanged.ok) {
      expect(everythingChanged.reasons).toHaveLength(3);
    }
  });

  it("reports reasons deterministically across repeated calls", () => {
    const observed = { ...IDENTITY, run_version: 9 };
    expect(checkIdentityCoherence(IDENTITY, observed)).toEqual(
      checkIdentityCoherence(IDENTITY, observed),
    );
  });
});

describe("sameReconciliationIdentity", () => {
  it("is true only for exactly equal identities", () => {
    expect(sameReconciliationIdentity(IDENTITY, { ...IDENTITY })).toBe(true);
    expect(sameReconciliationIdentity(IDENTITY, { ...IDENTITY, run_version: 6 })).toBe(
      false,
    );
    expect(
      sameReconciliationIdentity(IDENTITY, {
        ...IDENTITY,
        reconciliation_fingerprint: "e".repeat(64),
      }),
    ).toBe(false);
  });
});

describe("summarizeIncompatibleSnapshot", () => {
  it("returns null for fully compatible snapshots", () => {
    expect(summarizeIncompatibleSnapshot(snapshot({}), snapshot({}))).toBeNull();
  });

  it("detects a reconciliation fingerprint change", () => {
    const reasons = summarizeIncompatibleSnapshot(
      snapshot({}),
      snapshot({ reconciliation_fingerprint: "f".repeat(64) }),
    );
    expect(reasons).not.toBeNull();
    expect(reasons).toHaveLength(1);
    expect(reasons?.[0]).toContain("reconciliation fingerprint");
  });

  it("detects source and target input identity changes", () => {
    const source = summarizeIncompatibleSnapshot(
      snapshot({}),
      snapshot({ source_input_identity: "1".repeat(64) }),
    );
    expect(source).not.toBeNull();
    expect(source?.[0]).toContain("source input identity");

    const target = summarizeIncompatibleSnapshot(
      snapshot({}),
      snapshot({ target_input_identity: "2".repeat(64) }),
    );
    expect(target).not.toBeNull();
    expect(target?.[0]).toContain("target input identity");
  });

  it("detects analytical query version and run version changes", () => {
    const query = summarizeIncompatibleSnapshot(
      snapshot({}),
      snapshot({ analytical_query_version: 3 }),
    );
    expect(query).not.toBeNull();
    expect(query?.[0]).toContain("analytical query version");

    const version = summarizeIncompatibleSnapshot(
      snapshot({}),
      snapshot({ run_version: 5 }),
    );
    expect(version).not.toBeNull();
    expect(version?.[0]).toContain("run version");
  });

  it("detects a run id change and aggregates all reasons", () => {
    const reasons = summarizeIncompatibleSnapshot(
      snapshot({}),
      snapshot({
        run_id: "run_other-002",
        run_version: 5,
        analytical_query_version: 3,
      }),
    );
    expect(reasons).not.toBeNull();
    expect(reasons).toHaveLength(3);
  });
});

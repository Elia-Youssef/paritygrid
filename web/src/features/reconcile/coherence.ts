/**
 * Snapshot coherence for the reconciliation view. A reconciliation response
 * is only mergeable with a previously rendered snapshot when the durable
 * identity it was computed under matches exactly: run identity, both input
 * identities, the reconciliation fingerprint, and the analytical query
 * version. Any difference means reconciliation re-ran under different
 * facts, so the newer snapshot is held apart rather than merged.
 */
import type { ReconciliationResponse } from "../../api/generated/schema";

export interface ReconciliationIdentity {
  run_id: string;
  run_version: number;
  reconciliation_fingerprint: string;
}

export type CoherenceVerdict = { ok: true } | { ok: false; reasons: string[] };

/**
 * Compares the observed identity against the expected identity field by
 * field. Reasons are human-readable, stable, and deterministic so screens
 * and tests can assert on them.
 */
export function checkIdentityCoherence(
  expected: ReconciliationIdentity,
  observed: ReconciliationIdentity,
): CoherenceVerdict {
  const reasons: string[] = [];
  if (observed.run_id !== expected.run_id) {
    reasons.push(
      `run id changed: expected ${expected.run_id}, observed ${observed.run_id}`,
    );
  }
  if (observed.run_version !== expected.run_version) {
    reasons.push(
      `run version changed: expected ${String(expected.run_version)}, observed ${String(observed.run_version)}`,
    );
  }
  if (observed.reconciliation_fingerprint !== expected.reconciliation_fingerprint) {
    reasons.push(
      `reconciliation fingerprint changed: expected ${expected.reconciliation_fingerprint}, observed ${observed.reconciliation_fingerprint}`,
    );
  }
  return reasons.length === 0 ? { ok: true } : { ok: false, reasons };
}

/** True only when both identities describe the exact same durable facts. */
export function sameReconciliationIdentity(
  a: ReconciliationIdentity,
  b: ReconciliationIdentity,
): boolean {
  return (
    a.run_id === b.run_id &&
    a.run_version === b.run_version &&
    a.reconciliation_fingerprint === b.reconciliation_fingerprint
  );
}

/**
 * Full snapshot coherence used by the summary: returns null while the next
 * snapshot may safely replace the rendered one (same run identity, input
 * identities, reconciliation fingerprint, and analytical query version),
 * otherwise returns the reasons the snapshots are incompatible.
 */
export function summarizeIncompatibleSnapshot(
  current: ReconciliationResponse,
  next: ReconciliationResponse,
): string[] | null {
  const reasons: string[] = [];
  if (next.run_id !== current.run_id) {
    reasons.push(`run id changed: expected ${current.run_id}, observed ${next.run_id}`);
  }
  if (next.run_version !== current.run_version) {
    reasons.push(
      `run version changed: expected ${String(current.run_version)}, observed ${String(next.run_version)}`,
    );
  }
  if (next.reconciliation_fingerprint !== current.reconciliation_fingerprint) {
    reasons.push(
      `reconciliation fingerprint changed: expected ${current.reconciliation_fingerprint}, observed ${next.reconciliation_fingerprint}`,
    );
  }
  if (next.source_input_identity !== current.source_input_identity) {
    reasons.push(
      `source input identity changed: expected ${current.source_input_identity}, observed ${next.source_input_identity}`,
    );
  }
  if (next.target_input_identity !== current.target_input_identity) {
    reasons.push(
      `target input identity changed: expected ${current.target_input_identity}, observed ${next.target_input_identity}`,
    );
  }
  if (next.analytical_query_version !== current.analytical_query_version) {
    reasons.push(
      `analytical query version changed: expected ${String(current.analytical_query_version)}, observed ${String(next.analytical_query_version)}`,
    );
  }
  return reasons.length === 0 ? null : reasons;
}

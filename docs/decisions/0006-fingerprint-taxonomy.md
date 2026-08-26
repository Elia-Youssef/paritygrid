# Decision: fingerprint taxonomy

**Status:** accepted
**Date:** 2026-08-20
**Decision scope:** Plan, execution-evidence, reconciliation, and final target-state identity
**Supersedes:** Uses of “final fingerprint” that do not identify a fingerprint kind and version

## Context

ParityGrid has independent facts to identify: a compiled plan, durable execution history, a reconciliation snapshot, and verified target state. Phase 6 finalization hashes execution evidence, not record contents. Calling that value a final reconciliation fingerprint would overstate what it proves and make later schema evolution ambiguous.

## Decision

Fingerprints use explicit kind and version fields that are independent of package, database, canonical-format, pipeline-document, planner, and wire-contract versions.

| Kind | Owner and earliest phase | Required inputs | Meaning |
|---|---|---|---|
| Plan fingerprint | Planner, Phase 5 | Logical compiled plan without visual layout | Identifies scheduling intent |
| Execution-evidence fingerprint | Execution finalizer, Phase 6 | Plan fingerprint, pipeline identity and version, scenario seed, and sorted durable node, work, checkpoint, and logical-metric evidence | Identifies the durable evidence used to finalize one execution |
| Reconciliation fingerprint | Reconciliation, Phase 10 | Versioned query and input-manifest identities plus canonical reconciliation state and summaries | Identifies one analytical reconciliation snapshot |
| Target-state fingerprint | Verification, Phase 11 | Canonical observed target inventory and verification inputs | Identifies the independently observed target state after effects |

The existing Phase 6 finalization document remains execution-evidence version 2. Phase 7 migrates the `runs` persistence name to `execution_evidence_fingerprint` and adds an explicit `execution_evidence_fingerprint_version`. Existing non-null values are preserved and backfilled as version 2 through a forward migration and frozen-schema upgrade test. Compatibility reads may recognize the former storage name only at the repository boundary during the migration window; new writes and public contracts use the new name.

Cross-strategy comparison in Phase 7 is execution-evidence comparison. It compares the versioned execution-evidence fingerprint together with sorted durable work, checkpoint, count, causal-event, and artifact-manifest evidence. It does not claim reconciliation equivalence or target-state equality. Exact global event order is a sequential trace property, not a cross-strategy equality requirement; concurrent comparisons use normalized causal events.

Reconciliation and target-state fingerprints are separate facts. A successful repair may cite both, but neither is renamed or derived from the Phase 6 execution-evidence fingerprint.

## Consequences

- Stored values state exactly what they prove.
- The Phase 6 digest remains compatible while its column and public meaning become accurate.
- Phase 7 runner comparison cannot overclaim Phase 10 reconciliation or Phase 11 target-state behavior.
- Every fingerprint change requires a new kind-specific version, golden vectors, and migration or compatibility rules.

## Alternatives considered

- One universal final fingerprint was rejected because execution history, analytical classification, and observed target state have different inputs and owners.
- Recomputing existing Phase 6 values during rename was rejected because the current version 2 evidence must remain stable.
- Comparing exact concurrent event order was rejected because valid completion schedules may differ.

## Verification

- Golden vectors lock every fingerprint kind and version independently.
- Migration tests preserve existing non-null Phase 6 values and backfill version 2 without reinterpretation.
- Repository, API, and comparison tests reject missing, unknown, or mismatched fingerprint kinds and versions.
- Cross-strategy tests compare normalized execution evidence and include a negative case where equal execution evidence does not imply equal reconciliation state.

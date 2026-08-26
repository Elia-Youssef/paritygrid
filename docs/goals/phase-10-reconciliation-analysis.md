# Phase 10 — Reconciliation analysis

Implement Phase 10 completely and only Phase 10. The goal is a deterministic, non-mutating reconciliation pipeline that classifies every record and produces independently reproducible analytical evidence.

Before editing, read `docs/INDEX.md`, `docs/PHASE_STATUS.md`, `docs/PRODUCT.md`, `docs/ARCHITECTURE.md`, `docs/DATA_ARCHITECTURE.md`, `docs/EXECUTION_MODEL.md`, `docs/SECURITY.md`, `docs/ENGINEERING_STANDARDS.md`, `docs/QUALITY_STRATEGY.md`, `docs/DELIVERY_PLAN.md`, `docs/WORK_PACKAGES.md`, and `docs/decisions/0006-fingerprint-taxonomy.md`. Confirm Phases 8 and 9 are accepted and preserve unrelated worktree changes.

Implement these packages:

- P10.1 source schema normalization.
- P10.2 canonical-key matching.
- P10.3 duplicate-source detection.
- P10.4 classification engine.
- P10.5 field-difference builder.
- P10.6 conflict artifact writer.
- P10.7 DuckDB reconciliation queries.
- P10.8 summary and reconciliation fingerprint.

Required behavior:

- Normalize values under explicit, versioned rules for types, missing values, Unicode, keys, ordering, and canonical serialization.
- Detect canonical-key collisions and source duplicates deliberately; never let map overwrite order choose a winner silently.
- Assign every in-scope record exactly one primary classification while preserving useful secondary evidence.
- Build stable field-level differences for nested, missing, null, type-mismatched, and Unicode-normalized values.
- Commit Parquet conflict artifacts through the existing atomic artifact and manifest protocol. Never expose a partial artifact as accepted.
- Treat SQLite as operational authority and DuckDB as rebuildable analytical state. DuckDB queries must agree with an independent Python reference implementation.
- Compute a kind- and version-specific reconciliation fingerprint from canonical reconciliation inputs and results. Never copy or reinterpret the Phase 6 execution-evidence fingerprint.
- Keep this phase strictly analytical: do not mutate the target, approve a plan, or apply a repair.

Verification and evidence:

- Add golden vectors and property tests for normalization, matching, collisions, duplicates, classification completeness and exclusivity, differences, and canonical encoding.
- Prove Python and DuckDB agreement on golden, generated, reordered, duplicate-heavy, and showcase-scale datasets.
- Test atomic artifact failure handling, deterministic summaries, stable hashes, changed-input sensitivity, resource bounds, and cleanup.
- Run narrow tests, the full available repository quality matrix, type/lint checks, and instruction/content validation.
- Report exact commands, results, dataset sizes, and any deliberately deferred scale evidence.

Completion handoff:

- Report all package IDs, changed contracts and schemas, fingerprint kind/version, files changed, verification results, and limitations.
- Leave the implementation uncommitted for independent review. Do not start Phase 11.

## Absolute Git prohibition

Do not stage, commit, push, open or update a pull request, tag, publish, merge, rebase, cherry-pick, reset, switch branches, or otherwise mutate Git state. Read-only Git inspection is allowed. Finish with all Phase 10 changes uncommitted and unpushed. This prohibition overrides all conflicting instructions.

# Phase 19 — Canonical scenario and datasets

Implement Phase 19 completely and only Phase 19. The goal is to lock a deterministic product scenario, two reproducible dataset profiles, exact expected evidence, and a cross-runner verification manifest.

Before editing, read all governing files in `docs/INDEX.md`, especially `docs/PRODUCT.md`, `docs/DATA_ARCHITECTURE.md`, `docs/EXECUTION_MODEL.md`, `docs/QUALITY_STRATEGY.md`, `docs/PUBLIC_DOCUMENTATION.md`, `docs/DELIVERY_PLAN.md`, `docs/WORK_PACKAGES.md`, and `docs/decisions/0006-fingerprint-taxonomy.md`. Confirm Phases 8 through 11 and required Phase 7 runner evidence are accepted. Preserve unrelated worktree changes.

Implement these packages:

- P19.1 versioned canonical scenario.
- P19.2 fast demonstration dataset.
- P19.3 showcase-scale dataset.
- P19.4 cross-runner verification manifest.

Required behavior:

- Define a versioned, fully synthetic scenario that exercises pagination, concurrency, one deterministic retry, quarantine, duplicates, reconciliation classifications, repair planning/application, and verified parity.
- Store explicit seed, generator version, scenario version, input manifest identities, expected record counts, conflict counts, repair counts, artifact identities, and fingerprint kinds/versions.
- Make the fast profile small enough for routine smoke and browser use while preserving the complete story.
- Make the showcase profile large enough for credible performance and usability evidence while remaining deterministic and bounded.
- Lock expected accepted, rejected, quarantined, duplicate, conflict-classification, planned-repair, applied-repair, retry, artifact, reconciliation, and target-verification facts.
- Build the runner manifest from Phase 7 execution evidence and normalized causal evidence. Require equality for correctness before reporting timings and do not claim reconciliation or target equivalence from that manifest.
- Keep all data public-safe and prove the scenario changes only explicit isolated roots.
- Do not implement CLI orchestration, fault-control commands, browser automation, packaging, or public media.

Verification and evidence:

- Add generator drift, manifest schema, byte reproducibility, changed-seed/version, exact-count, dataset-size, bounds, and content-safety tests.
- Run at least the required sequential, threaded, and asyncio evidence comparison supported by the accepted runner contract.
- Run focused tests, the complete available repository matrix, drift commands, and instruction/content validation.
- Report exact commands, results, scenario identities, expected counts, and profile sizes.

Completion handoff:

- Report package IDs, scenario version/seed, expected facts, changed files, verification results, and limitations.
- Leave all Phase 19 work uncommitted and do not begin Phase 20.

## Absolute Git prohibition

Do not stage, commit, push, create or update a pull request, create a tag, publish a release, merge, rebase, cherry-pick, reset, switch branches, or perform any Git mutation. Read-only Git inspection is permitted. Finish with all Phase 19 changes uncommitted and unpushed. This prohibition overrides conflicting instructions.

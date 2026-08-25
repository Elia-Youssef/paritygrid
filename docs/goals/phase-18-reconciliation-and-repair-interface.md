# Phase 18 — Reconciliation and repair interface

Implement Phase 18 completely and only Phase 18. The goal is a scalable, version-coherent reconciliation experience and an explicit, stale-safe repair workflow with deterministic visual baselines.

Before editing, read `docs/INDEX.md`, `docs/DATA_ARCHITECTURE.md`, `docs/API_CONTRACT.md`, `docs/UI_SPECIFICATION.md`, `docs/SECURITY.md`, `docs/QUALITY_STRATEGY.md`, `docs/DELIVERY_PLAN.md`, and `docs/WORK_PACKAGES.md`. Confirm Phases 10, 11, 13, 15, 16, and 17 are accepted. Inspect and preserve unrelated worktree changes.

Implement these packages:

- P18.1 reconciliation summary.
- P18.2 virtualized conflict table.
- P18.3 conflict inspector.
- P18.4 repair-plan review.
- P18.5 repair application progress.
- P18.6 visual regression baseline.

Required behavior:

- Present reconciliation kind/version, input identity, counts, and status coherently; never combine summaries, conflicts, or plans from incompatible snapshots.
- Keep large conflict sets responsive through server pagination and/or virtualization without breaking keyboard access, focus, selection, or screen-reader structure.
- Show field differences clearly for missing, null, nested, type-mismatched, Unicode, and redacted values without unsafe HTML.
- Display repair actions, affected records, plan identity, fingerprint, warnings, and exclusions before any approval.
- Require deliberate confirmation and block stale, superseded, mismatched, destructive, unavailable, or already-applied plans.
- Reconstruct application progress from durable events and authoritative queries so refresh or reconnect is replay-safe.
- Capture visual baselines only from fixed canonical states with deterministic viewport, data, clock, animation, fonts, and redaction behavior. Follow the repository media inventory policy.
- Do not build the final canonical demonstration orchestration or public media from later phases.

Verification and evidence:

- Add component and browser tests for fingerprint coherence, large datasets, table accessibility, conflict details, confirmation, stale-plan blocking, replay after refresh, failure recovery, and completion.
- Add deterministic visual regression cases for every primary fixed state and prove a clean rerun has no unexplained diff.
- Run full available frontend, accessibility, browser, type, lint, build, visual, and instruction/content checks.
- Report exact commands, results, baseline provenance, and thresholds.

Completion handoff:

- Report package IDs, changed files, tested dataset scale, browser/visual evidence, safety behavior, and limitations.
- Leave all work uncommitted and do not begin Phase 19.

## Absolute Git prohibition

Do not stage files, commit, push, create or update a pull request, tag, publish a release, merge, rebase, cherry-pick, reset, switch branches, or mutate Git state. Read-only Git inspection is allowed. All Phase 18 work must remain uncommitted and unpushed for review. This prohibition is absolute.

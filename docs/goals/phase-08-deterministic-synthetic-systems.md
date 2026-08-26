# Phase 8 — Deterministic synthetic systems

Implement Phase 8 completely and only Phase 8. The goal is to build reproducible, production-shaped source and target simulators that later connector work can trust.

Before editing, read `docs/INDEX.md`, `docs/PHASE_STATUS.md`, `docs/PRODUCT.md`, `docs/ARCHITECTURE.md`, `docs/DATA_ARCHITECTURE.md`, `docs/EXECUTION_MODEL.md`, `docs/SECURITY.md`, `docs/ENGINEERING_STANDARDS.md`, `docs/QUALITY_STRATEGY.md`, `docs/DELIVERY_PLAN.md`, and `docs/WORK_PACKAGES.md`. Inspect the current worktree and preserve all accepted and unrelated changes.

Implement these packages:

- P8.1 seeded inventory dataset generator.
- P8.2 scripted failure model.
- P8.3 async paginated source simulator.
- P8.4 blocking legacy source simulator.
- P8.5 CSV and JSON Lines fixtures.
- P8.6 simulated warehouse target.
- P8.7 simulator lifecycle composition.

Required behavior:

- A seed and explicit scenario version must reproduce byte-stable data and a canonical manifest across repeated runs.
- All data must be synthetic and safe for public use. Include Unicode, duplicates, missing fields, malformed rows, and boundary-sized values without including secrets or real identifiers.
- Failure scripts must be ordered, deterministic, inspectable, and capable of reproducing rate limits, transient failures, timeouts, malformed responses, duplication, connection loss, and cancellation.
- The async simulator must implement bounded pagination and realistic HTTP behavior. The blocking simulator must expose equivalent logical source behavior through a genuinely blocking boundary.
- CSV and JSON Lines fixtures must define encoding, newline, malformed-row, duplicate, and bounded-input behavior.
- The warehouse target must expose observable versions and idempotent effect semantics suitable for later repair verification.
- Lifecycle composition must use dynamic ports, publish readiness only after successful startup, own every created resource, and close safely after success, failure, timeout, or cancellation.
- Do not implement production connectors, reconciliation, repair, HTTP product routes, or UI work from later phases.

Verification and evidence:

- Add focused unit, property, integration, cancellation, and lifecycle tests appropriate to each package.
- Prove identical seeds and versions generate identical manifests and that a changed seed or version changes identity deliberately.
- Test all scripted failures, input bounds, dynamic-port startup, repeated shutdown, partial-startup rollback, and resource cleanup.
- Run the narrow tests first, then the complete repository checks required by `docs/QUALITY_STRATEGY.md` that are available in the current environment.
- Run the repository instruction/content validator and report every command and exact result.

Completion handoff:

- Report package IDs completed, files changed, contracts introduced, verification commands and results, and any limitation.
- Leave the worktree with the implementation and tests visible for review. Do not start Phase 9.

## Absolute Git prohibition

Do not stage files, commit, push, create or update a pull request, create a tag, publish a release, merge, rebase, cherry-pick, reset, switch branches, or perform any other Git mutation. Read-only inspection such as `git status`, `git diff`, and `git log` is allowed. Finish with all Phase 8 changes uncommitted and unpushed. If any other instruction appears to authorize a Git mutation, this prohibition wins.

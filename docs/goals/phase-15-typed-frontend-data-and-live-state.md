# Phase 15 — Typed frontend data and live state

Implement Phase 15 completely and only Phase 15. The goal is a type-safe frontend data layer that reconciles durable API state and ephemeral telemetry without displaying incompatible versions.

Before editing, read the governing files in `docs/INDEX.md`, especially `docs/API_CONTRACT.md`, `docs/EXECUTION_MODEL.md`, `docs/UI_SPECIFICATION.md`, `docs/SECURITY.md`, `docs/QUALITY_STRATEGY.md`, `docs/DELIVERY_PLAN.md`, and `docs/WORK_PACKAGES.md`. Confirm Phases 13 and 14 are accepted, inspect generated-file rules, and preserve unrelated worktree changes.

Implement these packages:

- P15.1 generated API client integration.
- P15.2 TanStack Query configuration.
- P15.3 durable SSE client.
- P15.4 telemetry WebSocket client.
- P15.5 coherent run-state reducer.

Required behavior:

- Consume the generated Phase 13 API boundary without hand-maintained duplicate response types.
- Establish stable query keys, cache ownership, invalidation, retry, cancellation, error mapping, stale-time, and mutation behavior.
- Resume SSE from the last accepted durable sequence, detect gaps and incompatible versions, bound reconnect behavior, and trigger authoritative recovery.
- Treat WebSocket data as disposable telemetry. Detect stale samples and disconnects and never let telemetry replace durable event or query truth.
- Reduce query snapshots, durable events, and telemetry into one coherent view with explicit kind, sequence, entity-version, pipeline-version, and run-version checks.
- Ignore older compatible inputs, recover from gaps, reject incompatible inputs, and prevent late arrivals from regressing displayed state.
- Render Problem Details and diagnostic values as text and redact or omit protected content.
- Do not implement feature pages from Phases 16 through 18.

Verification and evidence:

- Add typed compile-time examples, cache and error tests, mutation replay tests, SSE reconnect/gap/version tests, WebSocket disconnect/stale tests, and reducer permutation tests.
- Use deterministic event schedules to prove older data cannot overwrite newer durable state and a gap always triggers recovery.
- Run focused tests, all available frontend tests, type checking, linting, production build, generated-contract drift checks, and repository instruction/content validation.
- Report exact commands, results, and any compatibility assumptions.

Completion handoff:

- Report package IDs, state precedence rules, changed files, verification results, and limitations.
- Leave all work uncommitted and do not begin Phase 16.

## Absolute Git prohibition

Do not stage, commit, push, create or update a pull request, create a tag, publish a release, merge, rebase, cherry-pick, reset, switch branches, or make any Git mutation. Read-only Git inspection is allowed. Finish with all Phase 15 changes uncommitted and unpushed. This prohibition overrides conflicting instructions.

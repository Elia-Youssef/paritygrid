# Phase 12 — Core HTTP API

Implement Phase 12 completely and only Phase 12. The goal is a stable, versioned operational HTTP boundary with correct lifespan ownership, errors, idempotency, confinement, headers, and request limits.

Before editing, read `docs/INDEX.md`, `docs/PHASE_STATUS.md`, `docs/ARCHITECTURE.md`, `docs/DATA_ARCHITECTURE.md`, `docs/EXECUTION_MODEL.md`, `docs/API_CONTRACT.md`, `docs/SECURITY.md`, `docs/ENGINEERING_STANDARDS.md`, `docs/QUALITY_STRATEGY.md`, `docs/DELIVERY_PLAN.md`, and `docs/WORK_PACKAGES.md`. Confirm all dependencies through Phase 11 are accepted and preserve unrelated worktree changes.

Implement these packages:

- P12.1 API composition and lifespan.
- P12.2 Problem Details mapping.
- P12.3 correlation middleware.
- P12.4 command idempotency boundary.
- P12.5 pipeline routes.
- P12.6 connector routes.
- P12.7 run lifecycle routes.
- P12.8 artifact routes.
- P12.9 security headers and limits.

Required behavior:

- Compose services through the established dependency direction and own startup/shutdown resources through FastAPI lifespan with safe partial-startup rollback.
- Return the versioned Problem Details contract consistently without stack traces, raw payloads, credentials, or unsafe HTML.
- Validate or create correlation identities at the boundary and propagate them through durable operations and diagnostics.
- Implement the durable command-idempotency state machine, including replay, payload mismatch, concurrency, expiry, and stranded in-progress reservation behavior.
- Expose pipeline, connector, run lifecycle, and artifact operations exactly as specified in `docs/API_CONTRACT.md`; do not invent undocumented response shapes.
- Enforce pagination, filtering, input-size, body-size, timeout, and concurrency limits before expensive work.
- Confine artifact identifiers and ranges to committed manifests and authorized roots; reject traversal, ambiguous encoding, symlink escape, invalid ranges, and incomplete artifacts.
- Apply required security headers and safe local-first binding defaults.
- Do not add SSE, WebSocket, reconciliation/repair routes, generated clients, or frontend features from Phase 13 onward.

Verification and evidence:

- Add lifespan, dependency failure, Problem Details matrix, correlation, idempotency race/replay, route contract, transition, limit, header, range, and path-safety tests.
- Include adversarial malformed inputs and prove slow or disconnected HTTP clients do not corrupt execution state.
- Run focused tests, full available backend checks, schema/migration checks, type/lint checks, and repository instruction/content validation.
- Report exact commands, results, public contract changes, and any deferred platform evidence.

Completion handoff:

- Report package IDs, routes and schemas added, files changed, verification evidence, and limitations.
- Leave all Phase 12 work uncommitted and do not begin Phase 13.

## Absolute Git prohibition

Do not stage, commit, push, create or update a pull request, tag, publish a release, merge, rebase, cherry-pick, reset, switch branches, or mutate Git state in any other way. Read-only Git inspection is allowed. The final worktree must contain the uncommitted, unpushed Phase 12 implementation. This prohibition overrides conflicting instructions.

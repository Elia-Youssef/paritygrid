# Optional Phase 27 — Go protocol and simulator

Implement optional Phase 27 completely and only after an explicit post-core decision authorizes it. The goal is to define a narrow cross-language boundary and build a deterministic Go source simulator without moving product or domain ownership out of Python.

Before editing, read all governing files in `docs/INDEX.md`, especially `docs/PRODUCT.md`, `docs/ARCHITECTURE.md`, `docs/EXECUTION_MODEL.md`, `docs/API_CONTRACT.md`, `docs/SECURITY.md`, `docs/ENGINEERING_STANDARDS.md`, `docs/QUALITY_STRATEGY.md`, `docs/DELIVERY_PLAN.md`, and `docs/WORK_PACKAGES.md`. Confirm Phase 26 or the approved post-core prerequisite is accepted. If the required scope decision is not authorized, stop and report that blocker without implementation. Preserve unrelated changes.

Implement these packages:

- P27.1 Go protocol and scope decision.
- P27.2 deterministic Go source simulator.
- P27.3 trace-context propagation.

Required behavior:

- Record an accepted narrow decision that defines why Go is useful, exact protocol ownership, build/runtime isolation, security boundaries, optional status, and exit criteria.
- Keep pipeline planning, scheduling, reconciliation, repair, persistence authority, fingerprint semantics, and product decisions in Python.
- Define versioned language-neutral request, response, pagination, error, retry, seed, scenario, trace-context, readiness, shutdown, and bounds fixtures.
- Implement the Go source simulator to reproduce the authorized subset of Phase 8 deterministic behavior with the same seed and protocol fixtures.
- Use dynamic ports, bounded concurrency and payloads, explicit timeouts, safe local binding, redaction, readiness, and graceful plus forced shutdown handling.
- Propagate standard trace context end to end without treating trace identifiers as idempotency, correlation, or security identities.
- Keep the normal demo, tests, installation, and product runtime fully functional when Go tooling or binaries are absent.
- Do not implement the Phase 28 load generator or performance comparison.

Verification and evidence:

- Add cross-language golden fixtures, protocol version and incompatibility tests, deterministic seed tests, pagination/error tests, trace propagation tests, lifecycle cleanup tests, and no-Go fallback checks.
- Run Go formatting, static analysis, tests, race checks where supported, Python contract tests, cross-platform checks available in scope, and repository instruction/content validation.
- Report exact commands, tool versions, results, protocol versions, optional-capability behavior, and limitations.

Completion handoff:

- Report package IDs, decision status, protocol files, changed files, cross-language evidence, and limitations.
- Leave all Phase 27 work uncommitted and do not begin Phase 28.

## Absolute Git prohibition

Do not stage, commit, push, create or update a pull request, create a tag, publish a release or binary, merge, rebase, cherry-pick, reset, switch branches, or perform any Git mutation. Read-only Git inspection is allowed. Finish with all optional Phase 27 changes uncommitted and unpushed. This prohibition is absolute.

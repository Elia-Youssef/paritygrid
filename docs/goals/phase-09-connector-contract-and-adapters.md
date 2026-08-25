# Phase 9 — Connector contract and adapters

Implement Phase 9 completely and only Phase 9. The goal is to define one production-shaped connector boundary and implement conforming adapters over the accepted Phase 8 simulators.

Before editing, read `docs/INDEX.md`, `docs/PHASE_STATUS.md`, `docs/PRODUCT.md`, `docs/ARCHITECTURE.md`, `docs/DATA_ARCHITECTURE.md`, `docs/EXECUTION_MODEL.md`, `docs/SECURITY.md`, `docs/ENGINEERING_STANDARDS.md`, `docs/QUALITY_STRATEGY.md`, `docs/DELIVERY_PLAN.md`, and `docs/WORK_PACKAGES.md`. Confirm Phase 8 is accepted, inspect the current worktree, and preserve unrelated changes.

Implement these packages:

- P9.1 connector base contract and conformance skeleton.
- P9.2 async HTTP source connector.
- P9.3 blocking HTTP source connector.
- P9.4 CSV connector.
- P9.5 JSON Lines connector.
- P9.6 warehouse target connector.
- P9.7 secret and error redaction.

Required behavior:

- Define one runner-neutral connector contract with explicit configuration, capability, lifecycle, cancellation, timeout, pagination, bounds, error, and observability semantics.
- Keep domain and scheduler ownership out of adapters. Do not add runner-specific branches to connector behavior.
- Run the async HTTP connector without blocking the event loop and isolate the blocking connector through the established blocking boundary.
- Make file adapters stream or page within configured bounds; specify encoding and malformed-record behavior and never read an unbounded source into memory.
- Make target effects idempotent under retries, replay, ambiguous response handling, and repeated calls with the same identity.
- Classify retryable, permanent, validation, cancellation, and unknown outcomes consistently with existing execution contracts.
- Redact credentials, tokens, sensitive headers, connection details, payload fragments, exceptions, logs, events, and public error details through adversarial fixtures.
- Close clients, files, streams, and target sessions after success, failure, cancellation, and partial initialization.
- Do not implement reconciliation, repair planning, product API routes, or UI work.

Verification and evidence:

- Build a shared conformance suite and run every applicable adapter through it.
- Add integration tests against Phase 8 simulators for pagination, rate limiting, malformed input, retry classification, timeout, cancellation, target idempotency, and shutdown.
- Add adversarial redaction tests that prove raw secret values never reach exceptions, structured logs, events, artifacts, or returned details.
- Run narrow tests, the complete available quality matrix, type and lint checks, and the repository instruction/content validator.
- Report exact commands and results, including skipped platform checks with reasons.

Completion handoff:

- Report package IDs, changed files, contract decisions, conformance coverage, verification results, and limitations.
- Leave all work visible and uncommitted for review. Do not start Phase 10.

## Absolute Git prohibition

Do not stage files, commit, push, create or update a pull request, create a tag, publish a release, merge, rebase, cherry-pick, reset, switch branches, or perform any other Git mutation. Read-only `git status`, `git diff`, and `git log` are allowed. Finish with all Phase 9 work uncommitted and unpushed. This prohibition overrides any conflicting instruction.

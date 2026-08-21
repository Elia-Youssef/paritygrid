# Phase status

This document is the durable index of accepted phases, their integration commits, public evidence, known limitations, and the active delivery boundary. It reports repository state; it does not override the governing requirements in the [documentation index](INDEX.md), the acceptance rubric in the [delivery plan](DELIVERY_PLAN.md), or the workflow in the [Git workflow](GIT_WORKFLOW.md).

## Accepted phases

| Phase | Status | Accepted integration commit | Public evidence | Delivered scope | Known limitation at acceptance |
|---|---|---|---|---|---|
| 0 — Repository and product charter | Accepted | [`9eab12b`](https://github.com/Elia-Youssef/paritygrid/commit/9eab12bc3dce06211dfeb81bd692c3f2aca3c03f) | [Baseline instruction audit](INSTRUCTION_AUDIT.md) | Product charter, architecture, delivery plan, repository policy, and documentation baseline | The initial history predates a dedicated phase pull request and this status index. |
| 1 — Toolchain and modular skeleton | Accepted | [`617e051`](https://github.com/Elia-Youssef/paritygrid/commit/617e0510d72631d589385aec7067c0e0d22284d3) | [Phase 1 command set](CI_RELEASE.md#phase-1-command-set) | Locked Python and frontend toolchains, package skeleton, CI, API and UI shells, and smoke path | Later phases retain the underlying commands while the planned top-level verification profiles remain incomplete. |
| 2 — Pure domain foundation | Accepted | [`9646164`](https://github.com/Elia-Youssef/paritygrid/commit/9646164d2312acb387e6e8b873c64b0b454fa04e) | [Phase pull request #5](https://github.com/Elia-Youssef/paritygrid/pull/5) | Trusted values, lifecycle states, reconciliation and repair rules, canonical encoding, fingerprints, typed failures, and domain import purity | Infrastructure integration was deliberately deferred. |
| 3 — SQLite persistence and migrations | Accepted | [`8d63ee1`](https://github.com/Elia-Youssef/paritygrid/commit/8d63ee19e495df9e4b3be62b379737022edb6cce) | [Phase pull request #20](https://github.com/Elia-Youssef/paritygrid/pull/20) | Authoritative SQLite schema, migration, repositories, transactional writer, WAL stress, and crash-reopen harness | Higher-level execution lifecycle coordination was deferred to Phase 6. |
| 4 — Artifact, Parquet, and DuckDB foundation | Accepted | [`4338409`](https://github.com/Elia-Youssef/paritygrid/commit/4338409d882bf3af52caad69a4572f80092d0e9a) | [Phase pull request #35](https://github.com/Elia-Youssef/paritygrid/pull/35) | Safe artifact paths, atomic manifests, versioned Parquet schemas, DuckDB views, analytical queries, and integrity scanning | Product reconciliation workflows and final-state verification were deferred. |
| 5 — Pipeline specification and planner | Accepted | [`dfd89ed`](https://github.com/Elia-Youssef/paritygrid/commit/dfd89ed6714300d831273df6bbd43e175fc4e693) | [Phase pull request #48](https://github.com/Elia-Youssef/paritygrid/pull/48) | Versioned pipeline documents, closed node registry, graph and resource validation, repair-path safety, immutable publication, deterministic plan compilation, partitioning, and plan fingerprints | The planner validates approval-before-effect graph structure; it does not apply repairs. |
| 6 — Sequential execution and recovery | Accepted | [`28d3540`](https://github.com/Elia-Youssef/paritygrid/commit/28d35406636f8b7b556ef813d31736b037624340) | [Phase pull request #62](https://github.com/Elia-Youssef/paritygrid/pull/62) with [post-merge verification run 32458747051](https://github.com/Elia-Youssef/paritygrid/actions/runs/32458747051) | Reference sequential scheduler and runner, work leasing, normalized attempt events, retry classification, result submission, checkpoint commit coordination, pause and resume, cancellation, run finalization, startup recovery, and the synthetic Phase 2–6 integration fixture | Three carried findings moved to Phase 7 P7.0: lease-event correlation bounds, pre-admission invalid-result handling, and the pause acknowledgement/abort race. Threaded, asynchronous, process, and interpreter runners are not implemented. |

## Active boundary

Phase 7 is active at package P7.0 on the integration branch `phase/07-concurrent-execution`.

- Accepted `main`: [`28d3540`](https://github.com/Elia-Youssef/paritygrid/commit/28d35406636f8b7b556ef813d31736b037624340).
- Phase 6 acceptance evidence: [pull request #62](https://github.com/Elia-Youssef/paritygrid/pull/62) and its exact-head Windows and Ubuntu checks, plus the successful [post-merge run 32458747051](https://github.com/Elia-Youssef/paritygrid/actions/runs/32458747051) on that merge commit.
- Phase 7 scope begins with the P7.0 carried-correctness corrections; no concurrent strategy is implemented yet.

## Phase 6 acceptance record

Phase 6 was accepted at merge commit [`28d3540`](https://github.com/Elia-Youssef/paritygrid/commit/28d35406636f8b7b556ef813d31736b037624340) through [pull request #62](https://github.com/Elia-Youssef/paritygrid/pull/62). It delivered the reference sequential scheduler and runner, work leasing, normalized attempt events, retry classification, result submission, checkpoint commit coordination, pause and resume, cancellation, run finalization, startup recovery, and a deterministic synthetic Phase 2–6 integration fixture. The fixture does not contain production connectors or the Phase 9 reconciliation workflow.

### Exact-head evidence

The accepted [pull-request acceptance record](https://github.com/Elia-Youssef/paritygrid/pull/62) is the public source for the accepted integration head, exact-head workflow links, and same-head review. The required workflow matrix was:

| Gate | Result |
|---|---|
| Repository policy | Public instructions, tracked content, generated-file inventory, ignored unsafe files, and whitespace are validated. |
| Python on Ubuntu | Locked dependency audit, formatting, lint, strict types, direct import boundaries, complete tests and coverage, scoped branch coverage, isolated wheel/spawn verification, and backend smoke pass. |
| Python on Windows | The same locked Python matrix passes on Windows. |
| Frontend | Locked install, formatting, linting, type checking, coverage, production build, and dependency audit pass. |
| Frontend/API smoke | The Vite development proxy reaches the FastAPI application factory and returns the expected service identity. |

The pull request left draft status only after every required job passed on its accepted head, the worktree was clean, and the independent acceptance review recorded that same head.

### Local release evidence

On 2026-08-21, the complete local release matrix passed on the committed Phase 6 content that was accepted:

| Gate | Local result |
|---|---|
| Locked Python environment | `uv lock --check` and `uv sync --locked --all-groups` passed for 65 packages. |
| Repository policy | Instruction validation passed for 28 Markdown files, 6 ADRs, 6 generated files, and an explicit empty media inventory; `git diff --check` passed. |
| Python static checks | Ruff formatting covered 320 files; Ruff lint, strict Pyright, and domain/process-worker import boundaries passed. |
| Python tests and coverage | 3,556 tests passed and 4 filesystem-link tests skipped on Windows; aggregate branch-enabled coverage was 99.54%. Application-execution branch coverage was 99.87%, and the sequential runner was 100%. |
| Python dependency audit | The strict hash-pinned all-groups lock export reported no known vulnerabilities. |
| Package isolation | A fresh Python 3.14.4 environment installed locked runtime dependencies and the built wheel, then passed an isolated Unicode-working-directory import, installed CLI smoke, and multiprocessing-spawn child import outside the checkout. |
| Generated fixtures and backend smoke | Frozen schema hashes and the sequential expected manifest reproduced exactly; `paritygrid smoke` passed. |
| Frontend | 33 tests passed; statements, lines, and functions were 100% and branches were 92.3%; formatting, linting, type checking, production build, and the dependency audit passed with zero reported vulnerabilities. |
| Frontend/API smoke | The Vite development proxy reached the FastAPI application factory and returned the expected service identity. |

This local evidence complemented the accepted-head Windows and Ubuntu workflow results and same-head review recorded in the pull request.

### Rubric

| Criterion | Score | Evidence and deduction |
|---|---:|---|
| Reference-runner correctness | 24/25 | The scheduler, sequential runner, deterministic ordering, result boundary, and integration fixture are covered on both required operating systems. One point remains reserved for the shared runner-neutral contract work that precedes Phase 7 strategies. |
| Lifecycle semantics | 24/25 | Pause/resume, cancellation, terminal replay, cleanup, and exact durable transitions are covered. One point is deducted for the carried `abort_pause`/runner race normalization. |
| Recovery and idempotency | 24/25 | Crash-reopen, checkpoint, artifact integrity, replay, expired-lease recovery, and fail-closed ambiguity paths are covered. One point is deducted for carried lease-event and pre-admission result-boundary hardening. |
| Automated verification | 20/20 | The complete locked matrix, dependency audits, scoped coverage, wheel isolation, spawned-process probe, and smoke checks pass locally and in both required operating-system jobs on the recorded exact head. |
| Diagnostics | 4/5 | Typed failures, redaction, durable event evidence, and bounded representations are covered. Concurrency telemetry schemas and stress diagnostics belong to Phase 7. |
| **Total** | **96/100** | Meets the numerical threshold; the same-head governance requirements remain mandatory and are invalidated by any new candidate commit. |

### Hard gates

| Phase 6 hard gate | Status | Evidence |
|---|---|---|
| Duplicate committed effect | Pass on candidate | Controlled interruption, recovery, and replay assertions in the [sequential integration suite](../tests/execution/test_sequential_end_to_end.py). |
| Missing committed checkpoint | Pass on candidate | Transactional receipt and failpoint coverage in the [checkpoint suite](../tests/execution/test_checkpoint_commit.py) and finalization checks. |
| Deadlock | Pass on candidate | Closed scheduler-state and transition coverage in the [scheduler contract](../tests/execution/test_scheduler_contract.py) and [tracker tests](../tests/execution/test_scheduler_tracker.py). |
| Ambiguous recovery accepted | Pass on candidate | Fail-closed classification and persistence evidence in the [recovery contract](../tests/execution/test_recovery_contract.py), [evidence tests](../tests/execution/test_recovery_evidence.py), and [integration suite](../tests/persistence/test_recovery_integration.py). |

No Phase 6 technical hard gate is open at the accepted merge commit.

### Independent review status

Independent governance and acceptance reviews identified and drove the documentation, decision-record, ownership, repository-content, recovery-safety, and acceptance-record corrections before acceptance. The governing final review is the one recorded against the accepted pull-request head after all corrections and exact-head checks passed.

### Known limitations and assigned follow-up

- Phase 7 comparison and stress work: prove concurrent pause, recovery, backpressure, cleanup, and execution-evidence equivalence across every required runner.
- Phase 9: compute and persist the reconciliation/final-state fingerprint; the Phase 6 fingerprint covers execution evidence only.
- Phase 10: decide the durable handling of stranded in-progress idempotency reservations.
- Threaded, asynchronous, process, and optional interpreter runners are not implemented.

## Phase 7 progress

Phase 7 implements bounded concurrent execution behind application-owned scheduling. Work lands package by package on `phase/07-concurrent-execution` and is recorded here as each package is accepted.

### Accepted Phase 7 packages

| Package | Status | Evidence |
|---|---|---|
| P7.0 — carried Phase 6 correctness corrections | Accepted on the phase branch | Lease-event correlation accepts `None` or 1–96 ASCII characters matching `[A-Za-z0-9][A-Za-z0-9._:-]*` and is rejected before clock access, reservation, in-flight mutation, or writer admission on both acquire and renew paths (`MAX_LEASE_EVENT_CORRELATION_ID_LENGTH`). `ResultSinkPreAdmissionError` distinguishes proven pre-writer-admission validation failures, which retain the active lease and permit one corrected bounded resubmission, from generic invalid-result failures, which keep the outcome-unknown disposition; `CheckpointCommitInvalidRequestError` now uses the pre-admission base while the admission classification keeps catch precedence. The pause acknowledgement/abort race is a compare-and-set over the same unacknowledged pause generation (`PauseToken.abort_for_coordinator`): an abort that wins releases the admission gate and the runner continues or reports its own redacted `RunnerProtocolError` without leaking a pause-coordinator exception; an acknowledgement that wins completes pause and requires an explicit resume. Barrier-controlled regressions in `tests/execution/test_p7_0_carried_correctness.py` cover both boundaries and both race winners with exact executor-call counts, stable frontiers, and released gates. |

### Next package

P7.1 — execution-evidence fingerprint compatibility and persistence migration.

### Remaining Phase 7 limitations

- P7.1 through P7.19 are not implemented.
- No threaded, asyncio, process, or interpreter strategy exists yet.
- Concurrency settings, clock/rate policy, capacity limiters, bounded channels, concurrency telemetry, and the concurrent result coordinator are not implemented.

## Advancement rule

Phase 7 began from the accepted Phase 6 merge commit with all required checks passing, no hard-gate failure, an independent acceptance review recorded for that head, a current pull-request record, and a clean worktree. The Phase 7 integration branch `phase/07-concurrent-execution` carries Phase 7 work and may merge into `main` only after the complete phase gate passes.

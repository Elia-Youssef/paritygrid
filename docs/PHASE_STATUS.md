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

Phase 7 is active at the complete P7.0–P7.19 candidate on the integration branch `phase/07-concurrent-execution`.

- Accepted `main`: [`28d3540`](https://github.com/Elia-Youssef/paritygrid/commit/28d35406636f8b7b556ef813d31736b037624340).
- Phase 6 acceptance evidence: [pull request #62](https://github.com/Elia-Youssef/paritygrid/pull/62) and its exact-head Windows and Ubuntu checks, plus the successful [post-merge run 32458747051](https://github.com/Elia-Youssef/paritygrid/actions/runs/32458747051) on that merge commit.
- P7.0–P7.19 and the independent audit corrections are committed on the phase branch at [`3473e2f`](https://github.com/Elia-Youssef/paritygrid/commit/3473e2f0c5623a902517125262d0f74382787de9). The complete candidate passed exact-head Windows/Linux CI in [run 32827063567](https://github.com/Elia-Youssef/paritygrid/actions/runs/32827063567); phase acceptance and merge remain pending.

## Phase 6 acceptance record

Phase 6 was accepted at merge commit [`28d3540`](https://github.com/Elia-Youssef/paritygrid/commit/28d35406636f8b7b556ef813d31736b037624340) through [pull request #62](https://github.com/Elia-Youssef/paritygrid/pull/62). It delivered the reference sequential scheduler and runner, work leasing, normalized attempt events, retry classification, result submission, checkpoint commit coordination, pause and resume, cancellation, run finalization, startup recovery, and a deterministic synthetic Phase 2–6 integration fixture. The fixture does not contain production connectors or the Phase 10 and 11 reconciliation and repair workflow.

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
- Phases 10 and 11: compute and persist the reconciliation and target-state fingerprints; the Phase 6 fingerprint covers execution evidence only.
- Phase 12: decide the durable handling of stranded in-progress idempotency reservations.
- Threaded, asynchronous, process, and optional interpreter runners are not implemented.

## Phase 7 progress

Phase 7 implements bounded concurrent execution behind application-owned scheduling. Work lands package by package on `phase/07-concurrent-execution` and is recorded here as each package is accepted.

### Phase 7 package status

| Package | Status | Evidence |
|---|---|---|
| P7.19 — stress, packaging, and cross-platform proof | Candidate passes local and cross-platform audit; phase acceptance pending | Fifty seeded shuffled executions in `tests/execution/test_stress_shuffled.py` reproduce by seed, vary ready order, retry placement, completion order, and result arrival, and compare equal normalized durable execution evidence across sequential, threaded, and asyncio; every run asserts bounds, contiguous event sequences, and zero owned worker leaks. Installed-wheel and Phase 7 spawn verification passed on Windows and Ubuntu in [run 32827063567](https://github.com/Elia-Youssef/paritygrid/actions/runs/32827063567). |
| P7.18 — full lifecycle, recovery, backpressure, and cleanup matrix | Candidate passes local audit | `tests/execution/test_lifecycle_matrix.py` runs every full-plan strategy through normal completion, retry, pause/resume, cancellation, worker failure, unknown-writer-outcome (forged fence), structural blocked-writer backpressure, repeated cleanup, and restart with expired and non-expired leases under pooled strategies. Every cell asserts contiguous per-run event sequences, durable aggregates, bounded channels, and zero owned worker leaks. |
| P7.17 — cross-strategy execution-evidence comparison harness | Candidate passes local audit | `evidence_comparison` freezes versioned `ExecutionEvidenceSnapshot` projections (kind/version, sorted work states, attempt outcomes, node aggregates, artifact identities, normalized causal events, fingerprint) and `compare_execution_evidence` returns exactly the execution-evidence verdict — its closed field set structurally cannot claim reconciliation, repair, or target-state equivalence, and negative fixtures lock that distinction. The same seeded run compares equal across sequential, threaded, and asyncio. |
| P7.16 — optional subordinate interpreter CPU pool | Candidate passes local audit | `adapters/runners/interpreter_pool.py` provides the interpreter-backed subordinate pool over the same versioned codec, registered CPU operations, and P7.6 CPU permits; capability detection probes the actual `InterpreterPoolExecutor` runtime and reports a structured reason when absent without falling back to the process pool. |
| P7.15 — runtime capability detection and strategy registration | Candidate passes local audit | `runtime_capabilities` detects threaded, asyncio, spawn-process, and interpreter capabilities in the actual runtime and registers sequential/threaded/asyncio as full-plan strategies plus the process and interpreter pools as subordinate capabilities through the lifecycle coordinator. No subordinate pool is exposed as a full-plan runner and no unavailable capability is silently substituted. |
| P7.14 — subordinate process CPU pool, versioned codec, and import isolation | Candidate passes local audit | `adapters/runners/` provides the spawn-context subordinate process pool, versioned bounded primitive codec over registered connector-free operations, and isolated worker entry. Transitive import and spawned-canary gates prohibit persistence, database, connector, filesystem, network, and dynamic-import access from the worker boundary. |
| P7.13 — structured asyncio full-plan strategy and safe sync adaptation | Candidate passes local audit | `asyncio_strategy` runs structured async workers through bounded cooperative channels and an owned blocking-adaptation pool. The async entry works in an active loop; the sync facade refuses a running loop; bounded shutdown cancels and joins every owned resource. It passes the shared full-plan conformance suite. |
| P7.12 — bounded threaded full-plan strategy | Candidate passes local audit | `threaded_strategy` owns a bounded named worker pool over the shared assignment/result discipline, preserves structural backpressure, converts worker failures into bounded result evidence, and joins every worker within the captured bound. It passes shared conformance, overlap, saturation, deadlock, bound, and cleanup tests. |
| P7.11 — shared full-plan and subordinate-pool conformance suites | Candidate passes local audit | `tests/execution/full_plan_conformance.py` applies the same durable-evidence, barrier, retry, pause, cancellation, bounds, and cleanup assertions to sequential, threaded, and asyncio over real SQLite evidence. Deliberately defective fixtures prove the suite rejects each targeted contract violation. |
| P7.10 — concurrent pause, cancellation, lifecycle, and durable recovery | Candidate passes local audit | The concurrent lifecycle, cleanup, recovery, and engine components own pause/cancel coordination, rebuild `SchedulerFrontierV2` only from durable SQLite evidence, resolve expired claims through fenced commands, fail closed on ambiguity, and compose the parent-owned execution loop. The result commit factory translates only validated rebased intents into the closed Phase 6 writer command set. |
| P7.9 — concurrent result coordinator and serialized SQLite boundary | Accepted on the phase branch | `result_coordinator` implements the parent-side `ConcurrentResultCoordinator`: results arrive through the bounded result channel; pre-admission validation checks the contract and plan fingerprint, run/node/partition/work identity, attempt, lease fence and owner, control generation, artifact references against registered admission facts, metrics, failure detail, and cleanup evidence — a rejected request submits zero writer commands and retains the lease for one corrected bounded resubmission. Writer admission is serialized under one lock and rebases the CURRENT run, node-aggregate, work-claim, lease-expiry, and event-frontier evidence through `SQLiteResultCoordinatorReader`, never trusting assignment-time row versions. `TransactionalResultCoordinatorWriter` compiles the validated intent through a parent-owned command factory, proves the resulting `CommitWorkAttempt` or `CommitWorkWithCheckpoint` matches every fence and rebased version, submits only that closed Phase 6 command to the SQLite writer, and translates a validated real `WriterReceipt` into the coordinator acknowledgement. A real SQLite integration covers coordinator → durable reader → command adapter → transactional writer → receipt and verifies work, attempt, checkpoint, aggregate, event, and run updates. Only durable acknowledgement unregisters the assignment, releases dependencies and capacity, and emits telemetry. Known rollbacks are retryable; unknown outcomes stop admission and require recovery. Stale, duplicate, expired, forged, plan-mismatched, and lease-mismatched results fail closed. |
| P7.8 — versioned concurrency telemetry schemas | Accepted on the phase branch | `telemetry` defines schema version 1 records for the eight observation kinds — queue depth, active capacity, capacity wait, service duration, blocked writer, dropped telemetry, cleanup state, and unresolved resources — with bounded metric names, labels, values, series counts, and a 4,096-byte wire bound, strict redaction of secret markers, credentials, and path shapes, canonical deterministic wire encodings whose strict inverse fails closed on unknown schemas, and helper constructors over P7.6 capacity and P7.7 channel facts. `TelemetryCollector` is a bounded run-isolated drop-oldest sink: it rejects foreign-run records, never blocks or grows, and reports counted drops. Non-authority is proven structurally and behaviorally: the module imports no scheduler, persistence, lease, or checkpoint authority, records expose no mutating surface, and a regression holds a real `ConcurrentScheduler` frontier byte-identical across telemetry bursts, drops, and drains. |
| P7.7 — bounded closeable assignment, result, telemetry, and writer-facing channels | Accepted on the phase branch | `channels` provides the four bounded closeable channel kinds with explicit finite capacity, FIFO delivery, idempotent close releasing every blocked peer with typed errors, accepted-message preservation through drain before or after close, an unknown-writer-outcome recovery close that fails producers closed while keeping accepted items drainable, and cooperative async wrappers that never nest or block a loop while the sync surface refuses to run inside one. `ChannelSet` closes in the deadlock-free writer-first dependency order with bounded observability snapshots. Deterministic backpressure regressions prove a deliberately blocked consumer stops upstream progress at the configured bound with memory bounded by capacity, cancellation and close release every waiter, and message conservation holds under concurrent producers. 161 deterministic regressions hold the module at 100% branch coverage with zero flakiness across repeated runs. |
| P7.6 — global, strategy, node, connector, and CPU-pool capacity/rate limiters | Accepted on the phase branch | `capacity` implements bounded, cancellation-safe capacity policy for the five categories. `ScheduledWorkLimiters` acquires the global→strategy→node triple all-or-nothing in a stable order (inversion, skipping, and repetition raise typed order errors), waits boundedly against absolute injected-clock deadlines with FIFO arrival-order fairness, releases exactly once, refuses parent release while subordinate permits are outstanding, and closes idempotently while waking waiters. `SubordinateCallLimiter` covers connector and CPU-pool calls only, verifies the parent triple and current holds before admission, applies the P7.5 token bucket with typed deterministic deferral carrying the exact retry instant, and can never replace or release a scheduled-work permit. High-contention deterministic tests prove saturation exactly at every captured limit, rollback on failed admission, double-release prevention, cancellation at every boundary, and maximum observed counts; the CPU-pool category is a ledger only — no process pool exists. 160 deterministic regressions hold the module at 100% branch coverage with zero flakiness across repeated runs. |
| P7.5 — injected clock, delay parser, and rate-policy values | Accepted on the phase branch | `clock_policy` provides the `PolicyClock` protocol and lock-guarded monotonic `ManualClock`, the `DelayValue` canonical delay object with unit-explicit and canonical-text parsers that reject negative, non-finite, fractional-microsecond, overflow, malformed, and ambiguous-unit values before any sleep or state mutation, policy-ceiling validation against the captured settings, `RateLimitPolicy` and the deterministic injected-timestamp `TokenBucket` with pure state, non-blocking acquisition, earliest-eligibility computation, and non-monotonic rejection, and `SeededJitterSchedule` whose SHA-256-derived, call-order-independent jitter reproduces identical eligibility times for repeated seeded schedules. No ambient clock is read anywhere; a source-scan regression locks that invariant. 263 deterministic regressions hold the module at 100% branch coverage. |
| P7.4 — captured concurrency settings and strategy capabilities | Accepted on the phase branch | `CapturedConcurrencySettings` freezes one immutable snapshot of every run bound — global, per-strategy, and per-node concurrent work, connector requests, subordinate CPU-pool operation slots, assignment/result/telemetry/writer channel capacities, in-flight records, response/file/artifact bytes, and cleanup/shutdown timeouts — validated before runtime ownership, derivable from a plan `ResourcePolicy`, and serializable with fail-closed version handling. `StrategyAvailability` records structured bounded unavailability; `resolve_strategy` never substitutes another strategy and reports typed errors for unknown ids; threaded and asyncio remain structurally unavailable until P7.12/P7.13. `StrategyLifecycleCoordinator` owns startup and shutdown exactly once with reverse-order partial-startup rollback, idempotent shutdown, aggregated bounded listener failures, and lock-guarded concurrent-startup rejection. 109 deterministic regressions hold the module at 100% branch coverage. |
| P7.3 — concurrent scheduler and SchedulerFrontierV2 | Accepted on the phase branch | `SchedulerFrontierV2` (schema version 2) durably records the plan fingerprint, control generation, per-node aggregates, deterministic ready work identities, admitted work with lease fences, results awaiting durable commit, retry waits with eligibility times, control state, and recovery-required reason with full cross-field fail-closed validation and exact serialization round trips. Frontier construction and restore reject self-dependencies, directed cycles, successors that bypass unsatisfied barriers, successors left blocked after all predecessors succeed, and descendants exposed after a blocking predecessor outcome. `ConcurrentScheduler` admits `(run_id, node_id, partition_key)` identities in deterministic plan-node-then-partition order across independent ready nodes and partitions, holds capacity through `mark_result_received` until `commit_result`, releases successor barriers only when every predecessor work item is durably succeeded, permanently blocks successors of blocked-terminal aggregates, moves retry waits through injected eligibility times without reading a clock, and enforces the running/quiescing/paused/cancelling/recovery-required control machine with generation bumps. |
| P7.2 — versioned runner-neutral contract | Accepted on the phase branch | Contract version 1 freezes `StrategyCapabilitiesV1`, `WorkAssignmentV1`, `WorkResultV1`, and `RecoveryFrontierV1` as immutable exact-type envelopes with stable protocol identifiers, closed primitives, explicit plan fingerprints on assignments and results, string/list/map/nesting/byte/metric/failure-detail bounds, canonical deterministic wire encoding with strict fail-closed decoding, and `work_identity()` returning `(run_id, node_id, partition_key)`. Decoding rejects noncanonical map-key order at every nesting level. Live-object leak rejection refuses sessions, writers, connections, clients, callbacks, futures, tasks, queues, coroutine objects, paths, and exceptions; public-text validation rejects secret markers and unrestricted paths. Sync entry points refuse to run inside an active event loop and async facades never nest a loop runner. Golden encodings, round trips, bound edges, adversarial vectors, and loop-safety tests live in `tests/execution/test_runner_contract.py`. No concurrent strategy is implemented. |
| P7.1 — execution-evidence fingerprint migration | Accepted on the phase branch | Forward migration `0002_execution_evidence` rebuilds `runs` with `execution_evidence_fingerprint` plus `execution_evidence_fingerprint_version`, preserving every non-null Phase 6 digest byte-for-byte, backfilling preserved digests as version 2, leaving null digests null, and never recomputing a stored value. Pairing and terminal-state constraints are enforced in storage and in `RunRecord`; compatibility reads of the former storage name are confined to the repository boundary and infer version 2 for preserved digests; unknown or mismatched versions fail closed. Frozen v0001 fixture upgrade, mixed null/non-null, idempotency, new writes, and the documented irreversible downgrade policy are covered in `tests/persistence/test_execution_evidence_migration.py` and the upgraded frozen-fixture preservation test. |
| P7.0 — carried Phase 6 correctness corrections | Accepted on the phase branch | Lease-event correlation accepts `None` or 1–96 ASCII characters matching `[A-Za-z0-9][A-Za-z0-9._:-]*` and is rejected before clock access, reservation, in-flight mutation, or writer admission on both acquire and renew paths (`MAX_LEASE_EVENT_CORRELATION_ID_LENGTH`). `ResultSinkPreAdmissionError` distinguishes proven pre-writer-admission validation failures, which retain the active lease and permit one corrected bounded resubmission, from generic invalid-result failures, which keep the outcome-unknown disposition; `CheckpointCommitInvalidRequestError` now uses the pre-admission base while the admission classification keeps catch precedence. The pause acknowledgement/abort race is a compare-and-set over the same unacknowledged pause generation (`PauseToken.abort_for_coordinator`): an abort that wins releases the admission gate and the runner continues or reports its own redacted `RunnerProtocolError` without leaking a pause-coordinator exception; an acknowledgement that wins completes pause and requires an explicit resume. Barrier-controlled regressions in `tests/execution/test_p7_0_carried_correctness.py` cover both boundaries and both race winners with exact executor-call counts, stable frontiers, and released gates. |

### Current candidate

P7.0 through P7.19 are implemented. The complete candidate is committed and
pushed at [`3473e2f`](https://github.com/Elia-Youssef/paritygrid/commit/3473e2f0c5623a902517125262d0f74382787de9).
The full local audit and exact-head cross-platform CI passed on 2026-08-25;
phase acceptance and merge remain pending.

Complete-candidate local evidence on 2026-08-25:

- The complete Python matrix passed with 5,433 tests and four environment-only
  Windows filesystem-link skips at 97.54% aggregate coverage. Required scoped
  branch coverage passed for application execution (92.89%), the sequential
  runner (100%), and runner adapters (90.62%).
- Ruff formatting and lint, strict Pyright, import boundaries, instruction
  validation, whitespace validation, dependency and lock checks, isolated-wheel
  execution including the spawned subordinate-process operation, backend smoke,
  frozen-fixture and fixture-drift checks, and every frontend gate passed.
- Three independent audit tracks covered the runner-neutral contracts and
  telemetry, scheduler and runner integrity, and the real coordinator-to-SQLite
  commit and recovery path. All reported correctness findings were corrected
  and revalidated before this candidate boundary.
- [GitHub Actions run 32827063567](https://github.com/Elia-Youssef/paritygrid/actions/runs/32827063567)
  passed the repository policy, complete frontend, Ubuntu Python, Windows
  Python, and frontend API smoke jobs on exact head `3473e2f`.

### First-half integration history

P7.0 through P7.9 were integrated on `phase/07-concurrent-execution` at merge commit [`6ed6cc9`](https://github.com/Elia-Youssef/paritygrid/commit/6ed6cc9aad411eb43d7e358252f5e00424b34182). This is historical evidence for the first half; it does not accept the current P7.10–P7.19 candidate.

Exact-head integration evidence on `6ed6cc9`:

- The complete local matrix passed on 2026-08-24: locked environment check and sync, strict hash-pinned dependency audit with no known vulnerabilities, instruction validation, whitespace check, Ruff formatting and lint, strict Pyright, import boundaries, the focused P7 suites (1,676 tests), the complete test matrix (5,232 passed, 4 Windows filesystem skips), scoped coverage (application execution 99.90%, sequential runner 100%), frozen v0001 fixture verification with the 0002 upgrade and migration suites (103 tests), sequential fixture drift check, isolated wheel install with spawn probe, backend smoke, every frontend gate (install, format, lint, types, coverage, build, audit at high severity with zero vulnerabilities), and the frontend API path smoke.
- GitHub checks passed on the exact head on both required operating systems in [run 32727274769](https://github.com/Elia-Youssef/paritygrid/actions/runs/32727274769), alongside every package pull request's own exact-head Windows and Ubuntu runs and recorded independent reviews.

### Remaining Phase 7 limitations

- The complete P7.0–P7.19 candidate is committed and pushed with exact-head CI evidence, but it has not yet been accepted or merged into `main`.
- The P6 sequential runner remains the Phase 6 reference; the P7 sequential
  full-plan strategy is the concurrent engine's inline mode, and the
  cross-strategy equivalence proof compares the three P7 strategies.
- Forced-termination failpoints around artifact rename and result admission are
  exercised through forged-envelope and recovery seams rather than subprocess
  kill points; a dedicated kill-point harness remains future hardening.

## Advancement rule

Phase 7 began from the accepted Phase 6 merge commit with all required checks passing, no hard-gate failure, an independent acceptance review recorded for that head, a current pull-request record, and a clean worktree. The Phase 7 integration branch `phase/07-concurrent-execution` carries Phase 7 work and may merge into `main` only after the complete phase gate passes.

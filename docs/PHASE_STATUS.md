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
| 7 — Concurrent execution | Accepted | [`6ad5fce`](https://github.com/Elia-Youssef/paritygrid/commit/6ad5fce2627642cdb1f8ff43517a4dde5a318d9d) | [Phase pull request #96](https://github.com/Elia-Youssef/paritygrid/pull/96) | Bounded concurrent scheduler and lifecycle, threaded and asyncio full-plan strategies, subordinate process and interpreter pools, capacity and channel controls, durable recovery, telemetry, and cross-strategy execution-evidence proof | Reconciliation, repair, and target-state equivalence remain outside the execution-evidence proof. |
| 8 — Deterministic synthetic systems | Accepted | [`07c997b`](https://github.com/Elia-Youssef/paritygrid/commit/07c997b24c48460c6efade8ea55e1c0e872ecbf6) | [Phase pull request #97](https://github.com/Elia-Youssef/paritygrid/pull/97) | Seeded inventory datasets, scripted failure models, async and blocking source simulators, bounded files, idempotent warehouse simulation, and composed lifecycle control | The systems are loopback-only synthetic fixtures; production transport and authentication remain deferred. |
| 9 — Connector contract and adapters | Accepted | [`38dac4e`](https://github.com/Elia-Youssef/paritygrid/commit/38dac4e134f8d90a97a428baf93c360ee99987e1) | [Phase pull request #98](https://github.com/Elia-Youssef/paritygrid/pull/98) | Application-owned connector contracts, async and blocking HTTP sources, CSV and JSON Lines sources, warehouse target integration, cooperative cancellation, deadlines, and bounded redaction | HTTP support intentionally targets the accepted loopback simulator protocol; product routes and broader transport features remain deferred. |
| 10 — Reconciliation analysis | Accepted | [`724116a`](https://github.com/Elia-Youssef/paritygrid/commit/724116acc97678a2932848a99cdb87785d81487d) | [Phase pull request #99](https://github.com/Elia-Youssef/paritygrid/pull/99) | Deterministic schema normalization, matching, duplicate detection, classification, field differences, conflict artifacts, DuckDB agreement, summaries, and reconciliation fingerprints | Operational persistence and repair remain deferred; agreement testing is bounded by the accepted 5,000-row synthetic ceiling, and quarantine evidence is not yet a separate artifact. |
| 11 — Repair and target verification | Accepted | [`169b538`](https://github.com/Elia-Youssef/paritygrid/commit/169b538eac347f014a1142552bfce9305f27cda7) | [Phase pull request #104](https://github.com/Elia-Youssef/paritygrid/pull/104) | Deterministic safety-limited repair planning, explicit approval and fencing, idempotent application with ambiguous-outcome recovery, independent target verification, and durable audit evidence | HTTP routes and UI workflows remain deferred; target behavior remains bounded to the accepted synthetic loopback warehouse contract, and target-state, reconciliation, and execution-evidence fingerprints remain distinct. |
| 12 — Core HTTP API | Accepted | [`3ca6125`](https://github.com/Elia-Youssef/paritygrid/commit/3ca61251bc03b71d74de5e28b71c2202469ac70e) | [Phase pull request #107](https://github.com/Elia-Youssef/paritygrid/pull/107) | Versioned operational HTTP composition, Problem Details, correlation, durable command idempotency, pipeline, connector, run, artifact, capability, health, and readiness routes, plus request limits and security headers | Reconciliation and repair routes, live transports, generated API clients, and packaged frontend serving remain Phase 13 work. |
| 13 — Live and generated API boundary | Accepted | [`c0011ed`](https://github.com/Elia-Youssef/paritygrid/commit/c0011edbec409ba132c4323a09a7b4a15d655441) | [Phase pull request #109](https://github.com/Elia-Youssef/paritygrid/pull/109) | Reconciliation and repair routes, durable SSE replay, advisory WebSocket telemetry, deterministic OpenAPI and TypeScript generation, and confined packaged frontend serving | Typed frontend data ownership, durable/live state precedence, application routing, and the accessible product shell remain Phase 14 and 15 work. |
| 14 — UI design system and shell | Accepted | [`05d1724`](https://github.com/Elia-Youssef/paritygrid/commit/05d1724f64c049022457fe2d3012f1b0640dcbe8) | [Phase pull request #111](https://github.com/Elia-Youssef/paritygrid/pull/111) | Semantic design tokens, owned accessible primitives, stable routing, keyboard navigation shell, shared async states, responsive and reduced-motion behavior, and safe render-error recovery | Routes intentionally contain shell-level placeholders; typed API ownership, caching, durable/live reducers, and reconnecting browser transports remain Phase 15 work. |
| 15 — Typed frontend data and live state | Accepted | [`1263101`](https://github.com/Elia-Youssef/paritygrid/commit/1263101fbb4bfe79108b8308c726b7c5a25e2979) | [Phase pull request #114](https://github.com/Elia-Youssef/paritygrid/pull/114) | Generated API client integration, deterministic query/cache ownership, bounded durable SSE recovery, advisory WebSocket telemetry, and sequence/version-aware coherent run state | Product screens and workflows remain Phase 16 through 18 work; the accepted layer supplies state and transport ownership without claiming those interfaces. |
| 16 — Pipeline and overview interface | Accepted | [`0d2f8c7`](https://github.com/Elia-Youssef/paritygrid/commit/0d2f8c7aefcf3cd7ed92cba9abdfca0ea52ca312) | [Phase pull request #116](https://github.com/Elia-Youssef/paritygrid/pull/116) | Truthful public and operations overviews, searchable pipeline library with URL-backed state, typed React Flow node set, studio canvas with keyboard authoring, node inspector, and frontier-fenced versioned plan publication | Reconciliation, repair, live execution observability, and comparison screens remain Phase 17 and 18 work; their routes stay explicit placeholders. |
| 17 — Live execution observability | Accepted | [`28fc5f4`](https://github.com/Elia-Youssef/paritygrid/commit/28fc5f43a56f16e5a11803216905b40a3c35cda3) | [Phase pull request #118](https://github.com/Elia-Youssef/paritygrid/pull/118) | URL-backed run history, coherent durable/live state, accessible graph and table evidence, queue and timeline views, runner comparison, and truthful capability reporting | The required browser target is Chromium; release-wide multi-browser verification remains deferred. |
| 18 — Reconciliation and repair interface | Accepted | [`16f705a`](https://github.com/Elia-Youssef/paritygrid/commit/16f705a4b8dddceb33cf1e2d2fcb100f336e19c4) | [Phase pull request #119](https://github.com/Elia-Youssef/paritygrid/pull/119) | Reconciliation summary and conflict inspection, stale-safe repair review and application, durable progress, strict runtime schemas, and deterministic Windows and Linux visual baselines | Visual baselines cover the required Chromium target on Windows and Linux, not a release-wide browser matrix. |
| 19 — Canonical scenario and datasets | Accepted | [`6573f3e`](https://github.com/Elia-Youssef/paritygrid/commit/6573f3e8db16d41a6875841bb122119dce5189d7) | [Phase pull request #120](https://github.com/Elia-Youssef/paritygrid/pull/120) | Canonical deterministic showcase datasets, scenario catalog and runner, exact expected evidence, artifact safety fencing, and execution-manifest verification | The showcase is bounded synthetic evidence; it is not a production-data or external-service integration. |
| 20 — Demo orchestration and smoke proof | Accepted | [`7c4827d`](https://github.com/Elia-Youssef/paritygrid/commit/7c4827dd11b241071171d0a0841de28bdbfe1fc0) | [Phase pull request #121](https://github.com/Elia-Youssef/paritygrid/pull/121) | Isolated demo CLI and public-run launcher, controlled faults and interruption recovery, safe ownership and reset, headless and browser smoke proof, and installed-package execution without Node.js | Run-control browser proof uses the product API; dedicated pause, resume, and cancel buttons are not claimed by this phase. The broader cross-platform performance and stress matrix remains Phase 21. |

## Active boundary

Phase 21 is the next delivery boundary. No Phase 21 implementation has been accepted.

- Accepted Phase 17 integration: [`28fc5f4`](https://github.com/Elia-Youssef/paritygrid/commit/28fc5f43a56f16e5a11803216905b40a3c35cda3) through [pull request #118](https://github.com/Elia-Youssef/paritygrid/pull/118).
- Accepted Phase 18 integration: [`16f705a`](https://github.com/Elia-Youssef/paritygrid/commit/16f705a4b8dddceb33cf1e2d2fcb100f336e19c4) through [pull request #119](https://github.com/Elia-Youssef/paritygrid/pull/119).
- Accepted Phase 19 integration: [`6573f3e`](https://github.com/Elia-Youssef/paritygrid/commit/6573f3e8db16d41a6875841bb122119dce5189d7) through [pull request #120](https://github.com/Elia-Youssef/paritygrid/pull/120).
- Accepted Phase 20 integration: [`7c4827d`](https://github.com/Elia-Youssef/paritygrid/commit/7c4827dd11b241071171d0a0841de28bdbfe1fc0) through [pull request #121](https://github.com/Elia-Youssef/paritygrid/pull/121).
- Phase 21 must begin from accepted Phase 20 and satisfy its own complete gate before acceptance.

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

Phase 7 implements bounded concurrent execution behind application-owned scheduling. It was accepted through [pull request #96](https://github.com/Elia-Youssef/paritygrid/pull/96).

### Phase 7 package status

| Package | Status | Evidence |
|---|---|---|
| P7.19 — stress, packaging, and cross-platform proof | Accepted | Fifty seeded shuffled executions in `tests/execution/test_stress_shuffled.py` reproduce by seed, vary ready order, retry placement, completion order, and result arrival, and compare equal normalized durable execution evidence across sequential, threaded, and asyncio; every run asserts bounds, contiguous event sequences, and zero owned worker leaks. Installed-wheel and Phase 7 spawn verification passed on Windows and Ubuntu in [run 32827063567](https://github.com/Elia-Youssef/paritygrid/actions/runs/32827063567). |
| P7.18 — full lifecycle, recovery, backpressure, and cleanup matrix | Accepted | `tests/execution/test_lifecycle_matrix.py` runs every full-plan strategy through normal completion, retry, pause/resume, cancellation, worker failure, unknown-writer-outcome (forged fence), structural blocked-writer backpressure, repeated cleanup, and restart with expired and non-expired leases under pooled strategies. Every cell asserts contiguous per-run event sequences, durable aggregates, bounded channels, and zero owned worker leaks. |
| P7.17 — cross-strategy execution-evidence comparison harness | Accepted | `evidence_comparison` freezes versioned `ExecutionEvidenceSnapshot` projections (kind/version, sorted work states, attempt outcomes, node aggregates, artifact identities, normalized causal events, fingerprint) and `compare_execution_evidence` returns exactly the execution-evidence verdict — its closed field set structurally cannot claim reconciliation, repair, or target-state equivalence, and negative fixtures lock that distinction. The same seeded run compares equal across sequential, threaded, and asyncio. |
| P7.16 — optional subordinate interpreter CPU pool | Accepted | `adapters/runners/interpreter_pool.py` provides the interpreter-backed subordinate pool over the same versioned codec, registered CPU operations, and P7.6 CPU permits; capability detection probes the actual `InterpreterPoolExecutor` runtime and reports a structured reason when absent without falling back to the process pool. |
| P7.15 — runtime capability detection and strategy registration | Accepted | `runtime_capabilities` detects threaded, asyncio, spawn-process, and interpreter capabilities in the actual runtime and registers sequential/threaded/asyncio as full-plan strategies plus the process and interpreter pools as subordinate capabilities through the lifecycle coordinator. No subordinate pool is exposed as a full-plan runner and no unavailable capability is silently substituted. |
| P7.14 — subordinate process CPU pool, versioned codec, and import isolation | Accepted | `adapters/runners/` provides the spawn-context subordinate process pool, versioned bounded primitive codec over registered connector-free operations, and isolated worker entry. Transitive import and spawned-canary gates prohibit persistence, database, connector, filesystem, network, and dynamic-import access from the worker boundary. |
| P7.13 — structured asyncio full-plan strategy and safe sync adaptation | Accepted | `asyncio_strategy` runs structured async workers through bounded cooperative channels and an owned blocking-adaptation pool. The async entry works in an active loop; the sync facade refuses a running loop; bounded shutdown cancels and joins every owned resource. It passes the shared full-plan conformance suite. |
| P7.12 — bounded threaded full-plan strategy | Accepted | `threaded_strategy` owns a bounded named worker pool over the shared assignment/result discipline, preserves structural backpressure, converts worker failures into bounded result evidence, and joins every worker within the captured bound. It passes shared conformance, overlap, saturation, deadlock, bound, and cleanup tests. |
| P7.11 — shared full-plan and subordinate-pool conformance suites | Accepted | `tests/execution/full_plan_conformance.py` applies the same durable-evidence, barrier, retry, pause, cancellation, bounds, and cleanup assertions to sequential, threaded, and asyncio over real SQLite evidence. Deliberately defective fixtures prove the suite rejects each targeted contract violation. |
| P7.10 — concurrent pause, cancellation, lifecycle, and durable recovery | Accepted | The concurrent lifecycle, cleanup, recovery, and engine components own pause/cancel coordination, rebuild `SchedulerFrontierV2` only from durable SQLite evidence, resolve expired claims through fenced commands, fail closed on ambiguity, and compose the parent-owned execution loop. The result commit factory translates only validated rebased intents into the closed Phase 6 writer command set. |
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

### Acceptance

P7.0 through P7.19 were accepted at merge commit [`6ad5fce`](https://github.com/Elia-Youssef/paritygrid/commit/6ad5fce2627642cdb1f8ff43517a4dde5a318d9d) through [pull request #96](https://github.com/Elia-Youssef/paritygrid/pull/96). The full local audit and exact-head cross-platform checks passed before the merge.

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

- The P6 sequential runner remains the Phase 6 reference; the P7 sequential
  full-plan strategy is the concurrent engine's inline mode, and the
  cross-strategy equivalence proof compares the three P7 strategies.
- Forced-termination failpoints around artifact rename and result admission are
  exercised through forged-envelope and recovery seams rather than subprocess
  kill points; a dedicated kill-point harness remains future hardening.

## Phase 8 progress

Phase 8 provides deterministic, production-shaped synthetic source and target
systems for later connector and reconciliation work. The phase was accepted
through [pull request #97](https://github.com/Elia-Youssef/paritygrid/pull/97)
and completes all seven packages without production connector or product-route
behavior.

### Phase 8 package status

| Package | Status | Evidence |
|---|---|---|
| P8.1 — seeded inventory dataset generator | Accepted | Versioned seeds produce canonical synthetic datasets, byte-stable manifests, Unicode and boundary values, duplicates, missing fields, and malformed members. |
| P8.2 — scripted failure model | Accepted | Ordered failure scripts cover rate limiting, transient errors, timeouts, malformed responses, bounded duplicate pages, connection loss, and cancellation with inspectable applied-failure evidence. |
| P8.3 — async paginated source simulator | Accepted | Dynamic-port HTTP pagination, cursor bounds, failure injection, active-handler tracking, and prompt shutdown are covered in `tests/demo/test_async_source.py`. |
| P8.4 — blocking legacy source simulator | Accepted | The genuinely blocking page service preserves the shared logical dataset and interrupts delayed or held connections during bounded shutdown. |
| P8.5 — CSV and JSON Lines fixtures | Accepted | Canonical UTF-8/LF fixtures preserve malformed and duplicate variants under explicit file and row bounds. |
| P8.6 — simulated warehouse target | Accepted | Observable versions and strict idempotency record one logical effect before timeout or connection-loss responses become ambiguous; same-key replay returns the recorded outcome. |
| P8.7 — simulator lifecycle composition | Accepted | Dynamic ports, readiness after startup, partial-start rollback, repeated close, and active-resource cleanup are exercised across composed services. |

### Phase 8 local evidence

The complete simulator matrix passed after independent audit and repair on
2026-08-26: 184 tests passed with no skips or failures. Ruff, strict Pyright,
import-boundary validation, instruction validation, and `git diff --check` also
passed.

### Phase 8 known limitations

- The systems intentionally expose loopback-only synthetic HTTP and bounded
  fixture contracts; production transport, authentication, and persistence
  belong to later phases.
- Simulator hold and delay behavior is capped and interruptible so teardown is
  deterministic even when a failure script models a hung peer.

## Phase 9 progress

Phase 9 defines the application-owned, runner-neutral connector contract and
five adapters over the accepted Phase 8 simulators. It was accepted through
[pull request #98](https://github.com/Elia-Youssef/paritygrid/pull/98) and
completes all seven packages without reconciliation or repair logic.

### Phase 9 package status

| Package | Status | Evidence |
|---|---|---|
| P9.1 — connector base contract and conformance skeleton | Accepted | `application/ports/connectors.py` freezes contract version 1 with capabilities, lifecycle, cancellation, deadlines, pagination, bounds, errors, observability, and adapter protocols. Adapter-layer compatibility modules contain re-exports only. |
| P9.2 — async HTTP source connector | Accepted | The asyncio adapter pages the cursor simulator without blocking the event loop and propagates native cancellation. |
| P9.3 — blocking HTTP source connector | Accepted | The blocking adapter refuses active event loops and uses cooperative cancellation to interrupt in-flight socket I/O promptly. |
| P9.4 — CSV connector | Accepted | The CSV adapter streams allowlisted files with header validation, bounded row cursors, per-chunk deadline and cancellation checks, and exact-final-page exhaustion. |
| P9.5 — JSON Lines connector | Accepted | The JSON Lines adapter streams byte-offset pages with bounded malformed-line evidence, per-chunk checkpoints, and exact-final-page exhaustion. |
| P9.6 — warehouse target connector | Accepted | Idempotent writes replay ambiguous post-commit timeout and connection-loss outcomes without a second effect; reads and state snapshots share the closed error mapping. |
| P9.7 — secret and error redaction | Accepted | `application/ports/connector_redaction.py` combines connector and per-call secret material and applies bounded fail-closed redaction to response fragments, errors, and events. |

### Phase 9 local evidence

The complete connector matrix passed after independent audit and repair on
2026-08-26: 357 tests passed, with one environment-only symbolic-link skip and
no failures. The shared conformance suite covers all five adapters. Ruff,
strict Pyright, import-boundary validation, instruction validation, and
`git diff --check` also passed.

### Phase 9 known limitations

- The HTTP engines implement the accepted loopback simulator wire contract:
  plain HTTP/1.1 with `Content-Length`, no redirects, chunking, or TLS.
- File cancellation is cooperative at bounded line/chunk boundaries; it cannot
  interrupt an operating-system file read that is already executing.
- HTTP routes remain the accepted Phase 8 synthetic routes; product API routes
  belong to Phase 12 and later.

## Phase 10 acceptance record

Phase 10 implements deterministic, non-mutating reconciliation analysis over
the accepted Phase 8 and 9 boundaries. All eight packages were accepted through
[pull request #99](https://github.com/Elia-Youssef/paritygrid/pull/99) at merge
commit [`724116a`](https://github.com/Elia-Youssef/paritygrid/commit/724116acc97678a2932848a99cdb87785d81487d).
Target mutation, approval, and repair remain outside the phase.

### Phase 10 package status

| Package | Status | Evidence |
|---|---|---|
| P10.1 — source schema normalization | Accepted | Versioned rules cover closed wire types, missing and null values, NFC Unicode, canonical attribute keys, deterministic order, and bounded quarantine evidence. |
| P10.2 — canonical-key matching | Accepted | Explicit per-key member lists prevent overwrite-selected winners; collision evidence preserves repeated record keys and enforces member bounds. |
| P10.3 — duplicate-source detection | Accepted | Duplicate groups retain sorted member provenance, distinct-content analysis, and fail-closed per-side bounds. |
| P10.4 — classification engine | Accepted | Every normalized record receives exactly one primary classification, bounded secondary evidence, and the sole allowed suggested resolution for that classification. |
| P10.5 — field-difference builder | Accepted | Stable nested-path differences cover missing, null, type mismatch, Unicode normalization, and bounded canonical renderings. |
| P10.6 — conflict artifact writer | Accepted | Conflict Parquet schema v1 round-trips through atomic artifact and manifest publication; parallel position/key provenance retains one shared order and rejects inconsistent resolution evidence. |
| P10.7 — DuckDB reconciliation queries | Accepted | DuckDB and the independent Python reference agree on per-key classifications, summaries, and fingerprints across golden, reordered, duplicate-heavy, empty, generated, and 5,000-row datasets. |
| P10.8 — summary and reconciliation fingerprint | Accepted | Kind `reconciliation`, version 1 fingerprints bind versioned inputs, counts, and order-independent outcome state without reinterpreting execution-evidence fingerprints. |

### Phase 10 local evidence

The complete reconciliation matrix passed after independent audit and repair
on 2026-08-26: 129 tests passed with no skips or failures. The final local full
suite passed 6,104 tests with 5 expected skips and 96.85% aggregate branch
coverage; every scoped coverage gate passed, including 96.08% for the
reconciliation domain and 100% for reconciliation analysis. Pull request #99
then passed duplicate exact-head Windows and Ubuntu jobs plus repository policy,
frontend, and frontend API smoke checks before merge. Ruff, strict Pyright,
import-boundary validation, instruction validation, and `git diff --check` also
passed.

### Phase 10 known limitations

- Operational services that persist reconciliation rows behind run nodes belong
  to later application-composition phases.
- DuckDB agreement currently covers the Phase 8 generator ceiling of 5,000
  rows per dataset; later performance phases own larger showcase scale.
- Quarantine evidence participates in counts, fingerprints, and analysis
  results but is not yet published as its own artifact.

## Phase 11 acceptance record

Phase 11 implements the safety-limited, approval-gated repair path and proves
the resulting target state through a separate observation boundary. All five
packages were accepted through [pull request #104](https://github.com/Elia-Youssef/paritygrid/pull/104)
at merge commit [`169b538`](https://github.com/Elia-Youssef/paritygrid/commit/169b538eac347f014a1142552bfce9305f27cda7).

### Phase 11 package status

| Package | Status | Evidence |
|---|---|---|
| P11.1 — repair planning | Accepted | Canonical plans bind the run, reconciliation fingerprint, source and target identities, policy generation, rules, analysis, and query versions. The closed action matrix permits only target creation and update; review-only outcomes cannot produce an action. |
| P11.2 — approval and fencing | Accepted | Durable approval records bind exact plan content and reconciliation state. Stale, foreign, concurrent, and replayed approvals fail closed. |
| P11.3 — idempotent application | Accepted | Per-action reservations, pre-dispatch ambiguity evidence, stable idempotency keys, target preconditions, replay and resume, and competing-application fences prove one logical effect and prevent false acceptance after interruption. |
| P11.4 — target verification | Accepted | Independently paged target observation is bracketed by matching snapshots, produces its own versioned target-state fingerprint, persists immutable verification facts, and cannot self-certify parity from repair output. |
| P11.5 — showcase integration | Accepted | The reconciliation-to-plan-to-approval-to-application-to-verification scenario executes more than 50 repairs, records durable evidence, produces exactly one target request per action, proves reapplication is a no-op, and reproduces the verification fingerprint. |

### Phase 11 acceptance evidence

The accepted candidate `353dc5be14370bc86e83361014ebf4e1f4808c6f`
passed the complete local lane and both exact-head GitHub workflows recorded in
the pull request. The local Windows run passed 6,311 tests with five
environment-only filesystem-link skips and 96.28% aggregate coverage. Scoped
coverage passed for application execution, reconciliation analysis, the
sequential runner, runner adapters, connector adapters, reconciliation, the
repair workflow, and the repair domain. Locked dependency audits, Ruff,
Pyright, import boundaries, isolated wheel and spawned-process verification,
backend smoke, frontend formatting, lint, types, 33 component tests, production
build, frontend dependency audit, API proxy smoke, instruction validation, and
the repository-content check all passed.

An independent exact-head review assessed the phase at 100/100 and found no
open hard gate. Deletion is not representable, stale plans and approvals are
fenced, ambiguous outcomes cannot become accepted, logical repair effects are
idempotent, and target parity is established only by independent observation.

### Phase 11 known limitations

- HTTP routes and UI workflows remain assigned to later phases.
- Target behavior remains bounded to the accepted synthetic loopback warehouse
  contract and does not claim broader production transport support.
- Target-state, reconciliation, and execution-evidence fingerprints remain
  deliberately distinct and are not interchangeable.

## Phase 12 acceptance record

Phase 12 implements the stable operational HTTP boundary over the accepted
Phase 11 services. All nine packages were accepted through
[pull request #107](https://github.com/Elia-Youssef/paritygrid/pull/107) at merge
commit [`3ca6125`](https://github.com/Elia-Youssef/paritygrid/commit/3ca61251bc03b71d74de5e28b71c2202469ac70e).

### Phase 12 package status

| Package | Status | Evidence |
|---|---|---|
| P12.1 — API composition and lifespan | Accepted | The application factory owns startup, rollback, shutdown, migration, storage, analytics, execution-owner, and background-task lifecycles with explicit ordering and failure tests. |
| P12.2 — Problem Details mapping | Accepted | Typed service and domain failures map to bounded RFC 9457 responses without secret, credential, path, or exception leakage. |
| P12.3 — correlation middleware | Accepted | Valid incoming identifiers propagate; malformed or oversized values are rejected and generated identifiers remain stable across response and error paths. |
| P12.4 — command idempotency boundary | Accepted | Durable reservations cover replay, conflict, concurrent owners, expiry, restart, and stranded-owner recovery under accepted decision record 0007. |
| P12.5 — pipeline routes | Accepted | Versioned create, publish, list, and detail contracts preserve immutable publication, validation, and bounded pagination semantics. |
| P12.6 — connector routes | Accepted | Connector inventory and detail contracts expose bounded redacted capabilities and limits without leaking configuration secrets. |
| P12.7 — run lifecycle routes | Accepted | Run creation, list, detail, pause, resume, and cancellation preserve lifecycle and idempotency fences through application-owned services. |
| P12.8 — artifact routes | Accepted | Metadata and ranged streaming retain safe-path confinement, integrity checks, strict range parsing, bounded reads, and no-store behavior. |
| P12.9 — security headers and limits | Accepted | CORS is disabled by default, request and response bounds fail closed, ordinary API responses retain a strict CSP, and Swagger documentation receives only the narrowly required documentation policy. |

### Phase 12 acceptance evidence

The accepted candidate `82ce2d0a644517161371279be108ed36d33dd92c`
passed the complete local lane and the exact-head push and pull-request workflow
matrices linked from pull request #107. The local Windows run passed 6,448
tests with five environment-only filesystem-link skips and 95.72% aggregate
coverage. Every scoped coverage gate passed, including 89.92% for the API.
Locked dependency audits, Ruff, strict Pyright, import boundaries, isolated
wheel and spawned-process verification, backend smoke, frontend formatting,
lint, types, 33 tests, production build, dependency audit, API proxy smoke,
instruction validation, and whitespace validation all passed.

One duplicate pull-request Windows job encountered a 30-second hosted-runner
stall in a Phase 7 parked-thread timing assertion. The same exact-head push job
passed, the isolated assertion passed five consecutive local repetitions, and
the unchanged required-job rerun passed the complete Windows lane before merge.

An independent exact-head review assessed Phase 12 at 100/100 and found no open
hard gate. Artifact paths and ranges remain confined, body and response sizes
are bounded, secrets are redacted, idempotency ownership fails closed, and the
public contract matches the implemented route boundary.

### Phase 12 known limitations

- Reconciliation, repair, and comparison routes remain assigned to Phase 13.
- Durable SSE, advisory WebSocket telemetry, deterministic OpenAPI and
  TypeScript generation, and packaged SPA serving remain assigned to Phase 13.
- Typed frontend query, cache, reducer, and live-state ownership remains
  assigned to Phase 15.

## Phase 13 acceptance record

Phase 13 completes the domain-facing API, separates durable replay from
advisory telemetry, reproduces generated contracts, and serves the packaged
frontend without a Node.js runtime dependency. All seven packages were
accepted through [pull request #109](https://github.com/Elia-Youssef/paritygrid/pull/109)
at merge commit
[`c0011ed`](https://github.com/Elia-Youssef/paritygrid/commit/c0011edbec409ba132c4323a09a7b4a15d655441).

### Phase 13 package status

| Package | Status | Evidence |
|---|---|---|
| P13.1 — reconciliation routes | Accepted | Versioned list, summary, detail, and comparison contracts expose bounded persisted reconciliation evidence through application-owned services and typed Problem Details. |
| P13.2 — repair routes | Accepted | Planning, approval, application, and verification routes retain Phase 11 identities, safety limits, fencing, idempotency, and independent target observation. |
| P13.3 — durable SSE stream | Accepted | Run events replay from durable sequence cursors, preserve ordering and reconnect semantics, emit bounded heartbeats, and stop cleanly under slow or disconnected clients. |
| P13.4 — live WebSocket telemetry | Accepted | Advisory telemetry is bounded, redacted, capability-gated, and explicitly non-authoritative; durable replay remains the source of truth. |
| P13.5 — deterministic OpenAPI export | Accepted | The committed OpenAPI document reproduces byte-for-byte from the application factory and fails CI on drift. |
| P13.6 — TypeScript type generation | Accepted | The committed generated declaration reproduces from the OpenAPI boundary, compiles independently, and fails CI on drift. |
| P13.7 — production frontend serving | Accepted | Packaged assets use confinement, immutable hashed-asset caching, no-store HTML, SPA fallback outside `/api`, strict API 404 behavior, and installed-wheel verification without Node.js. |

### Phase 13 acceptance evidence

The accepted candidate `dae394a5328ece623cd954713945b5a616735cf4`
passed the complete local lane and the exact-head
[pull-request workflow](https://github.com/Elia-Youssef/paritygrid/actions/runs/33386157838).
The local Windows run passed 6,507 tests with seven environment-only
filesystem-link skips and 95.58% aggregate coverage. Every scoped coverage
gate passed, including 90.53% for the API. Locked dependency audits, Ruff,
strict Pyright, import boundaries, deterministic OpenAPI and TypeScript drift
checks, declaration compilation, isolated wheel and spawned-process checks,
packaged frontend probing, backend smoke, frontend formatting, lint, types,
33 tests, coverage, production build, exact three-file distribution
reproduction, dependency audit, API proxy smoke, instruction validation, and
whitespace validation all passed.

The duplicate exact-head
[push workflow](https://github.com/Elia-Youssef/paritygrid/actions/runs/33386121894)
initially exposed an inherited Windows WAL-stress scheduling race after 6,513
tests passed: the mocked terminal reader failure became visible after the
progress gate. Phase 13 does not modify that code, the isolated regression
passed five consecutive local runs, the required pull-request Windows lane
passed unchanged, and the unchanged push-workflow rerun passed the complete
matrix before merge.

An independent exact-head review assessed Phase 13 at 100/100 and found no
open hard gate. Durable events remain authoritative, telemetry cannot mutate
execution state, generated artifacts reproduce exactly, static paths remain
confined, and the packaged application serves its frontend without Node.js.

### Phase 13 known limitations

- The accepted frontend remains a packaged boundary rather than the Phase 14
  accessible design system, routing shell, and shared UI state layer.
- Typed query ownership, cache policy, durable/live reducers, and reconnecting
  browser clients remain assigned to Phase 15.
- Product-specific pipeline, overview, live execution, reconciliation, and
  repair interfaces remain assigned to later phases.

## Phase 14 acceptance record

Phase 14 establishes the owned accessible visual system and application shell
that later product workflows reuse. All seven packages were accepted through
[pull request #111](https://github.com/Elia-Youssef/paritygrid/pull/111) at
merge commit
[`05d1724`](https://github.com/Elia-Youssef/paritygrid/commit/05d1724f64c049022457fe2d3012f1b0640dcbe8).

### Phase 14 package status

| Package | Status | Evidence |
|---|---|---|
| P14.1 — semantic design tokens | Accepted | Named color, typography, spacing, elevation, focus, motion, status, and layout tokens are centralized; contrast and raw-color guards prevent feature-level palette bypass. |
| P14.2 — owned primitive components | Accepted | Buttons, badges, breadcrumbs, skip links, and the command palette use owned typed components, bounded variants, keyboard behavior, and text-only rendering. |
| P14.3 — application routing | Accepted | Stable public and console routes include nested direct-entry handling, not-found behavior, document titles, breadcrumbs, and route-focus ownership. |
| P14.4 — accessible navigation shell | Accepted | Landmarks, active-route semantics, wide and compact navigation, skip-link focus, keyboard route changes, and command-palette navigation have unit, axe, and Chromium evidence. |
| P14.5 — shared async state components | Accepted | Loading, empty, error, stale, disconnected, and unavailable states render hostile strings and bounded Problem Details without unsafe HTML. |
| P14.6 — responsive layout rules | Accepted | The shell supports 320-pixel layouts, zoom-safe wrapping, touch targets, overflow control, narrow navigation, and reduced-motion overrides. |
| P14.7 — frontend error boundary | Accepted | Root and route boundaries catch render failures, retain bounded diagnostics, expose safe recovery, and restore useful focus without leaking protected details. |

### Phase 14 acceptance evidence

The accepted candidate `e3e766dab154b4b270675ef6381db79912e8e22e`
passed the complete local lane and both exact-head
[push](https://github.com/Elia-Youssef/paritygrid/actions/runs/33391061455)
and [pull-request](https://github.com/Elia-Youssef/paritygrid/actions/runs/33391102299)
workflow matrices. The local Windows run passed 6,507 Python tests with seven
environment-only filesystem-link skips and 95.58% aggregate coverage; every
scoped coverage gate passed, including 90.53% for the API. Locked dependency
audits, Ruff, strict Pyright, import boundaries, generated-contract drift,
isolated wheel and spawned-process verification, packaged frontend probing,
backend smoke, API proxy smoke, instruction validation, and whitespace
validation passed.

The frontend lane passed formatting, lint, types, generated declaration
compilation, 211 unit and component tests, 95.95% statement coverage, 93.68%
branch coverage, the production build, exact three-file distribution
reproduction, and the high-severity dependency audit with zero reported
vulnerabilities. All four Chromium shell tests passed: nested SPA entry,
wide and narrow navigation, reduced motion, and keyboard skip-link focus on
the main landmark.

An independent exact-head review assessed Phase 14 at 100/100 and found no
open hard gate. It confirmed P14.1–P14.7, token confinement, safe text-only
rendering, shell-only placeholder scope, clean current history, and the real
browser focus proof that closed the final reserved accessibility point.

### Phase 14 known limitations

- Console destinations are deliberate shell placeholders and do not claim
  the product workflows assigned to Phases 16 through 18.
- Typed API ownership, normalized query keys, cache invalidation, durable/live
  reducers, reconnecting SSE, and advisory telemetry clients remain Phase 15.
- The required browser acceptance lane is Chromium; the broader release
  browser matrix remains assigned to later release verification.

## Phase 15 acceptance record

Phase 15 provides the typed query, durable event, advisory telemetry, and
coherent run-state layer consumed by later product interfaces. All five
packages were accepted through
[pull request #114](https://github.com/Elia-Youssef/paritygrid/pull/114) at
merge commit
[`1263101`](https://github.com/Elia-Youssef/paritygrid/commit/1263101fbb4bfe79108b8308c726b7c5a25e2979).

### Phase 15 package status

| Package | Status | Evidence |
|---|---|---|
| P15.1 — generated API client integration | Accepted | Request and response ownership derives from the Phase 13 generated declaration; runtime schemas, compile-time identity examples, bounded Problem Details, cancellation, and server-aligned idempotency validation prevent hand-maintained contract drift. |
| P15.2 — TanStack Query configuration | Accepted | One query client owns stable keys, stale and cache policy, bounded retries, cancellation, error mapping, mutation replay, and explicit invalidation behavior. |
| P15.3 — durable SSE client | Accepted | The client resumes from the last accepted sequence, rejects incompatible versions, detects every gap, bounds reconnects, disposes completely, and triggers authoritative query recovery. |
| P15.4 — telemetry WebSocket client | Accepted | Telemetry is a separate disposable slice with snapshot-first validation, stale and disconnect states, bounded reconnects, complete cleanup, and absolute same-origin `ws:`/`wss:` browser endpoints. |
| P15.5 — coherent run-state reducer | Accepted | The pure reducer checks sequence, entity, pipeline, and run versions, ignores older compatible inputs, preserves state while recovering gaps, rejects incompatible inputs, and never lets advisory telemetry replace durable truth. |

### Phase 15 acceptance evidence

The accepted candidate `9e524a1093501f47f9100e5faf3a516646b4b3b9`
passed the complete local lane and the exact-head
[pull-request workflow](https://github.com/Elia-Youssef/paritygrid/actions/runs/33395145435).
The local Windows run passed 6,508 Python tests with seven environment-only
filesystem-link skips and 95.58% aggregate coverage. Every scoped coverage
gate passed, including 90.53% for the API. Locked dependency audits, Ruff,
strict Pyright, import boundaries, generated-contract drift, isolated wheel
and spawned-process verification, packaged frontend probing, backend smoke,
API proxy smoke, instruction validation, and whitespace validation passed.

The frontend lane passed formatting, lint, types, generated declaration
compilation, 363 tests, 94.81% statement coverage, 90.44% branch coverage,
the production build, exact three-file distribution reproduction, six
Chromium shell tests, and the dependency audit with zero reported
vulnerabilities. Focused blocker coverage passed 34 tests: HTTP and HTTPS page
origins produce absolute `ws:` and `wss:` telemetry endpoints, while malformed
idempotency keys are rejected before any request using the exact server
alphabet and 1–128 character bound.

An independent exact-head review assessed Phase 15 at 100/100 and found no
open hard gate. It confirmed generated type ownership, deterministic query
policy, gap and incompatibility recovery, telemetry non-authority, reducer
permutation safety, bounded diagnostics, the native browser URL repair, and
exact client/server idempotency parity.

### Phase 15 known limitations

- Public overview, pipeline library and studio, live execution, comparison,
  reconciliation, and repair screens remain Phase 16 through 18 work.
- The accepted live layer supplies browser transports and coherent state but
  does not claim product-specific visualizations or operator workflows.
- The required browser acceptance lane is Chromium; the broader release
  browser matrix remains assigned to later release verification.

## Phase 16 acceptance record

Phase 16 delivers the public and operations overviews, the pipeline library,
and the complete accessible pipeline-authoring workflow. All seven packages
were accepted through
[pull request #116](https://github.com/Elia-Youssef/paritygrid/pull/116) at
merge commit
[`0d2f8c7`](https://github.com/Elia-Youssef/paritygrid/commit/0d2f8c7aefcf3cd7ed92cba9abdfca0ea52ca312).

### Phase 16 package status

| Package | Status | Evidence |
|---|---|---|
| P16.1 — public project overview | Accepted | The overview claims only shipped capabilities, marks future work explicitly unavailable, and links into the console; hostile external text renders inertly. |
| P16.2 — operations overview | Accepted | Durable runs, runner availability, and readiness render from strict validated responses; unavailable facts render as unavailable and never as zero, with coherent stale and error states. |
| P16.3 — pipeline library | Accepted | Search, selection, creation, and pagination keep URL-backed state across reload, back, forward, and direct entry; keyboard-accessible rows and dialogs throughout. |
| P16.4 — typed React Flow node set | Accepted | A closed node registry binds typed ports and kinds; the graph always has a semantic table alternative so pointer interaction is never the only path. |
| P16.5 — pipeline studio canvas | Accepted | Typed graph authoring with port-validated connections, precise rejection feedback, keyboard-only editing, and a route-bound unsaved-draft navigation blocker. |
| P16.6 — node configuration inspector | Accepted | Typed field editors validate before publication, announce precise safe errors, and move focus correctly on open and close. |
| P16.7 — plan preview and publication | Accepted | Canonical plan preview, explicit `expected_latest_version` frontier fencing against the new durable version-frontier route, idempotency-keyed publication that blocks double submission, preserves the draft on stale conflict, replays exact bytes under the same key, and fails closed on malformed or mismatched acknowledgements. |

### Phase 16 acceptance evidence

The accepted candidate `00f2845`
(`866c7f4` is content-identical; the merge updates ancestry only) passed the
complete local Windows lane and the exact-head
[pull-request workflow](https://github.com/Elia-Youssef/paritygrid/actions/runs/33413597134)
with Ubuntu and Windows Python, frontend, and repository-policy checks. The
local run passed 6,512 Python tests with seven environment-only
filesystem-link skips and 95.59% aggregate coverage. Every scoped coverage
gate passed, including 90.53% for the API. Locked dependency audits, Ruff,
strict Pyright, import boundaries, generated OpenAPI and TypeScript drift,
isolated wheel and spawned-process verification, packaged frontend
reproduction, backend smoke, API proxy smoke, instruction validation, and
whitespace validation passed.

The frontend lane passed formatting, lint, types, generated declaration
compilation, 490 tests at required coverage, the production build, exact
three-file distribution reproduction, 23 Chromium browser workflows with
deterministic locale, timezone, viewport, and reduced-motion settings, and
the dependency audit with zero reported vulnerabilities. Browser evidence
covers overview navigation, durable operations data with coherent unavailable
rendering, library URL-state restoration, creation, hostile-text inertness,
600px narrow viewport, and the full canonical authoring path including
keyboard-only graph editing, invalid-connection rejection, inspector
validation focus, plan preview, first publication, double-submit blocking,
stale-conflict recovery with frontier refresh, exact idempotency replay, and
malformed and mismatched acknowledgement rejection.

An independent exact-head review assessed Phase 16 at 100/100 across workflow
completeness, graph correctness, accessibility, error states, and browser
evidence, with no open hard gate.

### Phase 16 known limitations

- Reconciliation, repair, live execution observability, and runner
  comparison screens remain Phase 17 and 18 work; their routes stay explicit
  placeholders.
- The graph canvas keeps pointer and keyboard parity through semantic
  alternatives; free-form spatial editing gestures remain bounded by the
  owned component set.
- The required browser acceptance lane is Chromium; the broader release
  browser matrix remains assigned to later release verification.

## Phase 17 acceptance record

Phase 17 was accepted through [pull request #118](https://github.com/Elia-Youssef/paritygrid/pull/118) at merge commit [`28fc5f4`](https://github.com/Elia-Youssef/paritygrid/commit/28fc5f43a56f16e5a11803216905b40a3c35cda3). Its single candidate commit, `acff030dd873af23dfccbc488ddb556accec534f`, passed both the exact-head [push workflow](https://github.com/Elia-Youssef/paritygrid/actions/runs/33648960239) and [pull-request workflow](https://github.com/Elia-Youssef/paritygrid/actions/runs/33648966526). The accepted scope covers P17.1–P17.7: run history, coherent durable/live state, accessible execution evidence, queue and timeline views, runner comparison, and capability reporting. Durable events remain authoritative, telemetry remains advisory, and gaps or incompatible versions fail closed to authoritative recovery.

## Phase 18 acceptance record

Phase 18 was accepted through [pull request #119](https://github.com/Elia-Youssef/paritygrid/pull/119) at merge commit [`16f705a`](https://github.com/Elia-Youssef/paritygrid/commit/16f705a4b8dddceb33cf1e2d2fcb100f336e19c4). Its single candidate commit, `b08ed5f5acd0c19c9e733ed98943f0c6a2eff2b9`, passed both the exact-head [push workflow](https://github.com/Elia-Youssef/paritygrid/actions/runs/33654338472) and [pull-request workflow](https://github.com/Elia-Youssef/paritygrid/actions/runs/33654339427). The accepted scope covers P18.1–P18.6: scalable conflict inspection, stale-safe repair approval and application, durable progress, strict runtime schemas, and deterministic Chromium visual baselines on Windows and Linux. The visual suite passed twice unchanged on both platforms after pinned local fonts removed host-font dependence.

## Phase 19 acceptance record

Phase 19 was accepted through [pull request #120](https://github.com/Elia-Youssef/paritygrid/pull/120) at merge commit [`6573f3e`](https://github.com/Elia-Youssef/paritygrid/commit/6573f3e8db16d41a6875841bb122119dce5189d7). Its single candidate commit, `a05070427488d279e424ee895302c8639551d2ba`, passed both the exact-head [push workflow](https://github.com/Elia-Youssef/paritygrid/actions/runs/33656960804) and [pull-request workflow](https://github.com/Elia-Youssef/paritygrid/actions/runs/33656965419). The accepted scope locks the deterministic scenario, fast and showcase datasets, exact expected counts, safe artifact verification, and a cross-runner execution-evidence manifest without claiming target-state equivalence.

## Phase 20 acceptance record

Phase 20 was accepted through [pull request #121](https://github.com/Elia-Youssef/paritygrid/pull/121) at merge commit [`7c4827d`](https://github.com/Elia-Youssef/paritygrid/commit/7c4827dd11b241071171d0a0841de28bdbfe1fc0). Its single candidate commit, `8a17d291df05148c6d8e6da1bd4a596e6033120a`, passed both the exact-head [push workflow](https://github.com/Elia-Youssef/paritygrid/actions/runs/33666193344) and [pull-request workflow](https://github.com/Elia-Youssef/paritygrid/actions/runs/33666200815). Both matrices passed repository policy, Ubuntu and Windows Python, frontend, and the dependent Frontend API smoke job; the latter exercised the real packaged demo through Chromium. The accepted implementation provides isolated orchestration, controlled faults, forced interruption and recovery, safe reset ownership, the canonical browser story, and installed-wheel proof with Node.js unavailable at runtime. Audit fixes closed the durable-envelope and strict pipeline-schema drifts reported by the initial browser proof while preserving fail-closed behavior.

## Advancement rule

Phase 20 is accepted on top of accepted Phase 19. Phase 21 may begin from the
accepted Phase 20 integration commit and may merge only after its own
complete gate and remote CI pass; acceptance of a later phase does not bypass
acceptance of its prerequisites.

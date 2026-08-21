# Work packages

## Package protocol

Each package is intentionally narrow enough to implement and review independently. Before starting a package:

1. Confirm all listed dependencies are accepted.
2. Read the governing architecture and quality documents.
3. Identify the exact modules and contracts owned by the package.
4. Avoid editing unrelated modules.
5. Add or update tests in the same change.

Each package handoff must state:

- Package ID.
- Files changed.
- Public contracts added or changed.
- Verification commands executed.
- Results and any remaining limitations.
- Follow-up package IDs, if applicable.

Parallel work may proceed only when packages do not edit the same contracts or files. Contract-defining packages land before their implementations.

## Phase 0 packages

| ID | Deliverable | Dependencies | Minimum verification |
|---|---|---|---|
| P0.1 | Repository initialization on `main` | None | Git status and metadata check |
| P0.2 | Approved identity and description | P0.1 | Text consistency audit |
| P0.3 | Confidentiality and synthetic-data statement | P0.2 | Confidentiality review |
| P0.4 | Ignore, attributes, and editor policies | P0.1 | Tracked-file and line-ending check |
| P0.5 | Documentation index and precedence | P0.2 | Local-link check |
| P0.6 | Architecture instruction set | P0.5 | Cross-document consistency audit |
| P0.7 | Repository-content validation script specification | P0.4 | Forbidden-content test fixtures |

## Phase 1 packages

| ID | Deliverable | Dependencies | Minimum verification |
|---|---|---|---|
| P1.1 | Python `src` package and `pyproject.toml` | P0 | Package import test |
| P1.2 | Locked Python dependency environment | P1.1 | Clean `uv sync` |
| P1.3 | FastAPI application factory | P1.1 | Factory unit test |
| P1.4 | Health and readiness skeleton | P1.3 | HTTP success and failure tests |
| P1.5 | CLI group and version command | P1.1 | CLI invocation test |
| P1.6 | Runtime settings model | P1.1 | Environment and validation tests |
| P1.7 | React, TypeScript, and Vite project | P0 | Production build |
| P1.8 | Tailwind and semantic token scaffold | P1.7 | CSS build and token test |
| P1.9 | Frontend application shell placeholder | P1.7 | Component render test |
| P1.10 | Python lint, format, and strict type config | P1.1 | Static checks pass |
| P1.11 | Frontend lint, format, and test config | P1.7 | Static checks and unit sample |
| P1.12 | Import-boundary verifier | P1.1 | Positive and negative fixture tests |
| P1.13 | Initial pull-request CI | P1.2, P1.11 | Clean workflow run |
| P1.14 | Initial smoke command | P1.3, P1.5 | Process startup and shutdown test |

## Phase 2 packages

| ID | Deliverable | Dependencies | Minimum verification |
|---|---|---|---|
| P2.1 | Identifier value objects | P1.10 | Valid, invalid, and property tests |
| P2.2 | UTC timestamp and duration values | P2.1 | Boundary and round-trip tests |
| P2.3 | Money and currency values | P2.1 | Overflow, exponent, and serialization tests |
| P2.4 | Canonical inventory record | P2.1–P2.3 | Construction and validation tests |
| P2.5 | Pipeline node and edge values | P2.1 | Equality and encoding tests |
| P2.6 | Run lifecycle state machine | P2.1 | Transition matrix tests |
| P2.7 | Work-item lifecycle state machine | P2.1 | Transition matrix tests |
| P2.8 | Failure classification model | P2.1 | Exhaustive variant tests |
| P2.9 | Reconciliation classification model | P2.4 | Completeness tests |
| P2.10 | Repair plan and action values | P2.4, P2.9 | Stale and invalid action tests |
| P2.11 | Versioned canonical encoder | P2.1–P2.10 | Golden bytes and property tests |
| P2.12 | State fingerprint service | P2.11 | Order-independence tests |
| P2.13 | Typed domain failures | P2.6–P2.10 | Exhaustive mapping tests |
| P2.14 | Domain import-purity gate | P2.1–P2.13 | Boundary verifier passes |

## Phase 3 packages

| ID | Deliverable | Dependencies | Minimum verification |
|---|---|---|---|
| P3.1 | SQLAlchemy engine and session factories | P1.6, P2 | File database lifecycle tests |
| P3.2 | SQLite pragma and version validation | P3.1 | Capability and failure tests |
| P3.3 | Initial relational metadata | P2, P3.1 | Metadata constraint inspection |
| P3.4 | Alembic environment and initial migration | P3.3 | Empty migration test |
| P3.5 | Pipeline repository | P3.4 | Immutable version integration tests |
| P3.6 | Connector repository | P3.4 | Secret-reference persistence tests |
| P3.7 | Run repository | P3.4 | State and row-version tests |
| P3.8 | Work-item and attempt repositories | P3.4 | Lease and uniqueness tests |
| P3.9 | Checkpoint repository | P3.4 | Monotonic-version tests |
| P3.10 | Durable event repository | P3.4 | Sequence and append-only tests |
| P3.11 | Idempotency repository | P3.4 | Replay and conflict tests |
| P3.12 | Repair and audit repositories | P3.4 | Immutability and audit tests |
| P3.13 | Transactional writer queue | P3.7–P3.12 | Atomic batch and backpressure tests |
| P3.14 | WAL concurrency stress harness | P3.13 | Readers, writer, and busy tests |
| P3.15 | Crash reopen harness | P3.13 | Subprocess termination test |
| P3.16 | Frozen-schema migration fixture | P3.4 | Upgrade and data-preservation test |

## Phase 4 packages

| ID | Deliverable | Dependencies | Minimum verification |
|---|---|---|---|
| P4.1 | Safe relative artifact path | P2.1 | Traversal and platform path tests |
| P4.2 | Atomic artifact writer | P4.1 | Step failpoint tests |
| P4.3 | SHA-256 artifact manifest | P4.2, P3 | File-manifest consistency tests |
| P4.4 | Versioned raw Parquet schema | P2.4 | Round-trip and boundary tests |
| P4.5 | Versioned normalized Parquet schema | P2.4 | Round-trip and canonical tests |
| P4.6 | Parquet partition writer | P4.2, P4.4, P4.5 | Multi-partition integration tests |
| P4.7 | DuckDB lifecycle coordinator | P3.2 | Open, rebuild, and close tests |
| P4.8 | Analytical view registry | P4.7 | Version and schema tests |
| P4.9 | Reconciliation query foundation | P4.6–P4.8 | Golden reference comparison |
| P4.10 | Run-statistics queries | P4.7, P3 | Golden metrics comparison |
| P4.11 | Orphan integrity scanner | P4.3 | Missing file and missing manifest tests |
| P4.12 | Artifact streaming port | P4.3 | Range, size, and confinement tests |

## Phase 5 packages

| ID | Deliverable | Dependencies | Minimum verification |
|---|---|---|---|
| P5.1 | Versioned pipeline document schema | P2.5 | Schema examples and invalid cases |
| P5.2 | Closed node registry | P5.1 | Unknown-kind rejection |
| P5.3 | Typed input and output ports | P5.2 | Compatibility matrix tests |
| P5.4 | DAG cycle validator | P5.1 | Generated graph property tests |
| P5.5 | Reachability and terminal validator | P5.4 | Disconnected graph tests |
| P5.6 | Connector capability validator | P5.2, P3.6 | Missing capability tests |
| P5.7 | Resource policy validator | P5.2 | Bounds and default tests |
| P5.8 | Repair safety validator | P5.2 | Approval-before-effect tests |
| P5.9 | Immutable publication service | P5.1–P5.8, P3.5 | Replay and mutation tests |
| P5.10 | Execution-plan compiler | P5.3–P5.8 | Golden plan tests |
| P5.11 | Partition strategy contract | P5.10 | Stable partition-key tests |
| P5.12 | Plan fingerprint | P5.10 | Layout and JSON-order independence |
| P5.13 | Human-readable validation errors | P5.4–P5.8 | Error-code snapshot tests |

## Phase 6 packages

| ID | Deliverable | Dependencies | Minimum verification |
|---|---|---|---|
| P6.1 | Scheduler state and dependency tracker | P5.10 | Ordering unit tests |
| P6.2 | Sequential runner contract | P6.1 | Runner conformance baseline |
| P6.3 | Work-item leasing service | P3.8, P6.1 | Expiry and ownership tests |
| P6.4 | Normalized attempt-event variants | P2.8, P6.2 | Exhaustive event tests |
| P6.5 | Named retry policy | P2.8 | Classification and delay tests |
| P6.6 | Result-sink contract | P3.13, P4.3 | Accepted and rejected result tests |
| P6.7 | Checkpoint commit sequence | P3.13, P6.6 | Before/after commit failpoints |
| P6.8 | Pause coordinator | P6.1, P6.7 | Stable-boundary tests |
| P6.9 | Cancellation coordinator | P6.1, P6.2 | Prompt cleanup tests |
| P6.10 | Run finalization and execution-evidence verification | P2.12, P4.10, P6.7 | Versioned execution-evidence fingerprint tests |
| P6.11 | Startup recovery scanner | P3.15, P4.11, P6.7 | Interrupted state matrix |
| P6.12 | Synthetic Phase 2–6 sequential integration fixture | P6.1–P6.11 | Five-node durable-boundary scenario |

## Phase 7 packages

| ID | Deliverable | Dependencies | Minimum verification |
|---|---|---|---|
| P7.0 | Lease event-correlation validation, invalid-result lease poisoning before writer admission, and abort-pause/runner TOCTOU normalization | Accepted P6.1–P6.12 | Three deterministic regression suites |
| P7.1 | Execution-evidence fingerprint compatibility and persistence migration | P7.0, P3.16, P6.10 | Frozen-schema preservation and version backfill |
| P7.2 | Versioned runner-neutral contract and bounded wire envelopes | P7.1 | Golden contract and rejection vectors |
| P7.3 | Concurrent scheduler and durable `SchedulerFrontierV2` | P7.2, P5.11 | Readiness, admission, and frontier recovery tests |
| P7.4 | Captured concurrency settings and strategy capabilities | P7.2, P1.6, P5.7 | Capture, support, and unavailability tests |
| P7.5 | Injected clock, validated delay parser, and rate-policy values | P7.4, P6.5 | Clock-driven boundary tests |
| P7.6 | Global, strategy, node, connector, and CPU-pool capacity/rate limiters | P7.3, P7.5 | Saturation and cancellation-safety tests |
| P7.7 | Bounded closeable assignment, result, and telemetry channels | P7.2, P7.3, P6.9 | Backpressure, close-order, and blocked-peer tests |
| P7.8 | Versioned concurrency telemetry schemas | P7.3, P7.4, P7.7 | Schema, bound, and non-authority tests |
| P7.9 | Concurrent result coordinator and serialized SQLite boundary | P7.1, P7.3, P7.7, P7.8, P3.13, P6.6, P6.7 | Out-of-order commit and writer-failure tests |
| P7.10 | Concurrent pause, cancellation, lifecycle, and durable recovery | P7.3, P7.7, P7.9, P6.8, P6.9, P6.11 | Race, fencing, recovery, and cleanup tests |
| P7.11 | Shared full-plan and subordinate-pool conformance suites | Accepted P6.1–P6.12, P7.0–P7.10 | Sequential and controlled-double baselines pass |
| P7.12 | Bounded threaded full-plan strategy | P7.6–P7.11 | Conformance, deadlock, bound, and join tests |
| P7.13 | Structured asyncio full-plan strategy and safe sync adaptation | P7.6–P7.11 | Conformance, loop-safety, bound, and cleanup tests |
| P7.14 | Subordinate process CPU pool, versioned codec, and import isolation | P7.2, P7.6, P7.7, P7.9–P7.11 | Spawn, serialization, isolation, and orphan tests |
| P7.15 | Runtime capability detection and strategy registration | P7.4, P7.12–P7.14 | Exact registration and lifecycle tests |
| P7.16 | Optional subordinate interpreter CPU pool | P7.11, P7.14, P7.15 | Pool conformance or structured unavailability |
| P7.17 | Cross-strategy execution-evidence comparison harness | P6.12, P7.1, P7.8–P7.16 | Versioned evidence and causal-event comparison |
| P7.18 | Full lifecycle, recovery, backpressure, and cleanup matrix | P7.10, P7.12–P7.16 | All strategy and failpoint cells pass |
| P7.19 | Repeated stress, installed-wheel, and cross-platform spawn proof | P7.17, P7.18 | Shuffled runs and Windows/Linux package tests |

### Phase 7 acceptance details

- **P7.0:** Lease event correlation accepts `None` or 1–96 ASCII characters matching `[A-Za-z0-9][A-Za-z0-9._:-]*` and rejects empty, whitespace, non-ASCII, and overlong values before clock access, reservation, or writer admission. A public typed pre-admission validation failure retains the active lease and permits one corrected bounded resubmission; a generic invalid-result failure remains outcome-unknown because it may occur after admission. Abort-pause/runner TOCTOU behavior is normalized with compare-and-set on the same unacknowledged generation; if acknowledgement wins, pause completes and an explicit resume is required. Barrier-controlled tests prove exactly one pause-race winner, a stable scheduler frontier with no active node, released gates, exact executor-call counts, and no leaked pause-coordinator exception.
- **P7.1:** The forward migration replaces the former run-column meaning with `execution_evidence_fingerprint` plus its explicit version. Every existing non-null digest is preserved byte-for-byte and backfilled as version 2; null values remain null. Empty and frozen prior schemas, mixed values, repository compatibility reads, new writes, and the downgrade policy are tested before P7.2 starts.
- **P7.2:** Contract version 1 freezes capabilities, immutable assignment/result envelopes, control generations, lifecycle states, idempotent cleanup evidence, and durable recovery-frontier shape. Golden encodings and adversarial bounds reject sessions, writers, callbacks, clients, resolved secrets, unrestricted paths, lease credentials, unsupported primitives, and unknown versions. Sync and async facades cannot block an active event loop or invoke a nested loop runner.
- **P7.3:** `(run_id, node_id, partition_key)` is the sole admitted work unit. Independent ready nodes and partitions overlap, successors wait for every predecessor work item to be durably terminal, retry waits use the durable frontier, and no streaming edge is introduced. `SchedulerFrontierV2` round-trips plan identity, control generation, ready, admitted, awaiting-commit, retry-wait, node aggregate, and recovery-required state.
- **P7.4:** One immutable settings snapshot records every configured bound and capability fact used for a run. Unsupported strategies return structured unavailability and are never silently substituted. Startup and shutdown own each registered strategy exactly once.
- **P7.5:** All delay and rate behavior uses an injected clock. Negative, non-finite, overflowed, malformed, and policy-exceeding delays are rejected without sleeping; seeded schedules reproduce exact eligibility times.
- **P7.6:** Global, strategy, node, connector, and subordinate CPU-pool limits never exceed their captured values under saturation. Acquisition order is stable, reservations are released exactly once on cancellation or failed admission, and a connector call cannot bypass its connector permit.
- **P7.7:** Assignment, result, telemetry, and writer-facing flow are bounded and closeable. With the writer deliberately blocked, producer progress stops at the configured bound, memory does not grow with offered work, cancellation unblocks every waiter, and close order neither loses an accepted result nor deadlocks.
- **P7.8:** Telemetry uses versioned bounded names and values for queue depth, capacity, wait, service, and cleanup state. It may be sampled or dropped and cannot release dependencies, advance a checkpoint, reconstruct recovery, or publish authoritative progress.
- **P7.9:** The parent coordinator validates the work-local lease fence and rebases run/node aggregates and the event frontier at serialized writer admission. Deliberately reversed completions commit correct aggregates and contiguous per-run event sequences without stale global row-version failures. No thread, task, process, or interpreter worker can access the SQLite writer. A pre-admission rejection retains the lease; a known rollback is retryable within policy; an unknown outcome stops admission and requires recovery.
- **P7.10:** Pause quiesces admission at a stable durable boundary, cancellation closes producers and owned resources within a bound, and late results from an earlier control generation or coordinator owner are fenced. Exclusive startup recovery rebuilds only from durable evidence, handles non-expired leases explicitly, and never serializes futures, tasks, queues, or worker state. Cleanup is idempotent and returns structured unresolved-resource evidence.
- **P7.11:** Sequential, threaded, and asyncio full-plan strategies share scheduling, retry, result, pause, cancellation, recovery, bounds, and cleanup assertions. Process and optional interpreter pools use a separate subordinate-operation suite that forbids plan scheduling, connectors, artifact ownership, and persistence. Controlled doubles prove every required assertion fails for the intended defect.
- **P7.12:** The threaded strategy passes the full-plan suite, shares no session or connection across threads, prevents nested-pool starvation, respects every bound, and joins all owned threads after success, failure, pause, cancellation, and shutdown.
- **P7.13:** The asyncio strategy passes the full-plan suite with structured task ownership, bounded queues and semaphores, propagated cancellation, and closed clients and streams. Tests cover calls from no loop, an active loop, task failure, cancellation, and shutdown without nested `asyncio.run` or event-loop blocking.
- **P7.14:** The process pool accepts only registered connector-free CPU operations and bounded versioned primitive envelopes. The parent owns artifacts, results, leases, and SQLite. Spawn, worker crash, cancellation, pool failure, and shutdown leave no orphan process. A transitive scan of `src/paritygrid/adapters/runners/process_workers/` rejects persistence, runtime composition, connector, and database-library imports, and a canary proves a worker cannot open the operational database.
- **P7.15:** Runtime registers sequential, threaded, and asyncio strategies only when their exact capabilities are present and registers the subordinate process pool separately. Captured capabilities match exposed choices; partial startup rolls back in reverse order and shutdown is idempotent.
- **P7.16:** An enabled interpreter pool passes the subordinate-operation, isolation, bound, cancellation, and cleanup suite. An unavailable runtime returns a stable reason and leaves the default demo and required strategy gate unaffected.
- **P7.17:** Comparison uses the explicit execution-evidence kind and version plus sorted durable work states, checkpoints, counts, artifact-manifest identities, and normalized causal events. It ignores timing and valid concurrent global event order. It never labels the result reconciliation equivalence, repair equivalence, or target-state parity, and negative fixtures lock that distinction.
- **P7.18:** A reviewed matrix covers every full-plan strategy and every enabled subordinate pool across normal completion, retry, pause at each frontier, pause-abort race, cancellation under saturation, blocked writer, worker failure, unknown writer outcome, forced termination around every durable boundary, restart, and repeated cleanup. Every cell asserts bounds, no duplicate committed effect, no missing checkpoint, fenced stale results, contiguous events, and zero owned-resource leaks.
- **P7.19:** At least fifty seeded shuffled executions preserve versioned execution evidence across required strategies. Windows and Linux install the built wheel into an isolated environment, start process workers with spawn semantics, run the process import/isolation gate, complete the lifecycle matrix, and prove all threads, tasks, processes, queues, files, and database owners close within configured bounds.

## Phase 8 packages

| ID | Deliverable | Dependencies | Minimum verification |
|---|---|---|---|
| P8.1 | Seeded inventory dataset generator | P2.4 | Manifest reproducibility |
| P8.2 | Scripted failure model | P2.8 | Seed and sequence tests |
| P8.3 | Async paginated source simulator | P8.1, P8.2 | HTTP behavior tests |
| P8.4 | Blocking legacy source simulator | P8.1, P8.2 | Blocking client behavior tests |
| P8.5 | CSV and JSON Lines fixtures | P8.1 | Encoding and malformed-row tests |
| P8.6 | Simulated warehouse target | P8.1, P8.2 | Version and idempotency tests |
| P8.7 | Connector base contract | P5.6 | Conformance skeleton |
| P8.8 | Async HTTP source connector | P8.3, P8.7 | Full connector suite |
| P8.9 | Blocking HTTP source connector | P8.4, P8.7 | Full connector suite |
| P8.10 | CSV connector | P8.5, P8.7 | Full connector suite |
| P8.11 | JSON Lines connector | P8.5, P8.7 | Full connector suite |
| P8.12 | Warehouse target connector | P8.6, P8.7 | Idempotent effect suite |
| P8.13 | Secret and error redaction | P8.7 | Adversarial redaction fixtures |
| P8.14 | Simulator lifecycle composition | P8.3, P8.4, P8.6 | Dynamic-port start and stop tests |

## Phase 9 packages

| ID | Deliverable | Dependencies | Minimum verification |
|---|---|---|---|
| P9.1 | Source schema normalization | P2.4, P8 | Golden and property tests |
| P9.2 | Canonical-key matching | P9.1 | Unicode and collision tests |
| P9.3 | Duplicate-source detection | P9.2 | Duplicate group tests |
| P9.4 | Classification engine | P9.2, P9.3 | Completeness and exclusivity |
| P9.5 | Field-difference builder | P9.4 | Nested and missing field tests |
| P9.6 | Conflict artifact writer | P9.5, P4.6 | Golden Parquet test |
| P9.7 | DuckDB reconciliation queries | P9.4, P4.9 | Python reference agreement |
| P9.8 | Summary and fingerprint | P9.4, P2.12 | Golden count and hash test |
| P9.9 | Repair-plan generator | P9.5, P2.10 | Safe action matrix |
| P9.10 | Approval service | P9.9, P3.12 | Stale and concurrent tests |
| P9.11 | Idempotent repair application | P9.10, P8.12 | Replay and partial-failure tests |
| P9.12 | Target parity verifier | P9.11, P2.12 | Expected fingerprint test |
| P9.13 | Showcase-scale reconciliation test | P9.1–P9.12 | Bounded resource integration run |

## Phase 10 packages

| ID | Deliverable | Dependencies | Minimum verification |
|---|---|---|---|
| P10.1 | API composition and lifespan | P3–P9 | Startup and shutdown tests |
| P10.2 | Problem Details mapping | P2.13, P10.1 | Error matrix tests |
| P10.3 | Correlation middleware | P10.1 | Validation and propagation tests |
| P10.4 | Command idempotency boundary | P3.11, P10.1 | Replay and conflict tests |
| P10.5 | Pipeline routes | P5.9, P10.2 | Contract tests |
| P10.6 | Connector routes | P8, P10.2 | Contract and limit tests |
| P10.7 | Run lifecycle routes | P6, P10.2, P10.4 | Transition contract tests |
| P10.8 | Reconciliation routes | P9, P10.2 | Pagination and version tests |
| P10.9 | Repair routes | P9.9–P9.12, P10.4 | Approval and application tests |
| P10.10 | Artifact routes | P4.12 | Range and path-safety tests |
| P10.11 | Durable SSE stream | P3.10 | Replay, gap, and slow-client tests |
| P10.12 | Live WebSocket telemetry | P7.8 | Connect and disconnect tests |
| P10.13 | OpenAPI export | P10.1–P10.12 | Deterministic snapshot |
| P10.14 | TypeScript type generation | P10.13 | Clean regeneration diff |
| P10.15 | Production frontend serving | P1.7, P10.1 | SPA and API 404 tests |
| P10.16 | Security headers and limits | P10.1 | Header and oversized-body tests |

## Phase 11 packages

| ID | Deliverable | Dependencies | Minimum verification |
|---|---|---|---|
| P11.1 | Semantic design tokens | P1.8 | Token snapshot and contrast check |
| P11.2 | Owned primitive components | P11.1 | Browser component tests |
| P11.3 | Application routing | P1.9 | Route render tests |
| P11.4 | Accessible navigation shell | P11.2, P11.3 | Keyboard and accessibility tests |
| P11.5 | Generated API client integration | P10.14 | Typed sample queries |
| P11.6 | TanStack Query configuration | P11.5 | Cache and error tests |
| P11.7 | Durable SSE client | P10.11, P11.6 | Reconnect and sequence tests |
| P11.8 | Telemetry WebSocket client | P10.12, P11.6 | Disconnect and stale tests |
| P11.9 | Coherent run-state reducer | P11.7, P11.8 | Gap and older-event tests |
| P11.10 | Shared async state components | P11.2, P11.6 | Loading through stale tests |
| P11.11 | Responsive layout rules | P11.4 | Viewport component tests |
| P11.12 | Frontend error boundary | P11.10 | Recovery and safe-detail tests |

## Phase 12 packages

| ID | Deliverable | Dependencies | Minimum verification |
|---|---|---|---|
| P12.1 | Public project overview | P11 | Browser smoke and accessibility |
| P12.2 | Operations overview | P11, P10.7 | Component and browser tests |
| P12.3 | Pipeline library | P11, P10.5 | Search and empty-state tests |
| P12.4 | Typed React Flow node set | P5.2, P11 | Component interaction tests |
| P12.5 | Pipeline studio canvas | P12.4, P10.5 | Graph editing browser tests |
| P12.6 | Node configuration inspector | P12.4 | Validation and keyboard tests |
| P12.7 | Plan preview and publication | P12.5, P12.6 | Publication browser test |
| P12.8 | Run list and filters | P11, P10.7 | Pagination and state tests |
| P12.9 | Live run graph overlay | P11.9, P12.4 | Version-coherence tests |
| P12.10 | Worker swimlane timeline | P11.8 | Chart and table alternative tests |
| P12.11 | Queue and saturation panels | P11.8 | Stale and reduced-motion tests |
| P12.12 | Durable event timeline | P11.7 | Gap recovery browser test |
| P12.13 | Reconciliation summary | P10.8, P11 | Fingerprint-coherence tests |
| P12.14 | Virtualized conflict table | P10.8 | Large dataset interaction tests |
| P12.15 | Conflict inspector | P12.14 | Field-difference tests |
| P12.16 | Repair-plan review | P10.9, P12.15 | Stale and confirmation tests |
| P12.17 | Repair application progress | P12.16, P11.7 | Replay-safe browser test |
| P12.18 | Runner comparison dashboard | P7.17, P10.8 | Correctness-first display tests |
| P12.19 | System capabilities page | P7.15, P10.1 | Available/unavailable tests |
| P12.20 | Visual regression baseline | P12.1–P12.19 | Canonical screenshots |

## Phase 13 packages

| ID | Deliverable | Dependencies | Minimum verification |
|---|---|---|---|
| P13.1 | Versioned canonical scenario | P8, P9 | Locked scenario manifest |
| P13.2 | Fast demonstration dataset | P13.1 | Exact expected counts |
| P13.3 | Showcase-scale dataset | P13.1 | Determinism and size checks |
| P13.4 | Demo CLI orchestration | P10.15, P13.2 | Start, URL, and shutdown test |
| P13.5 | Controlled fault controls | P8.2, P13.4 | Reproducible fault test |
| P13.6 | Controlled process interruption | P6.11, P13.4 | Resume test |
| P13.7 | Cross-runner verification manifest | P7.17, P13.2 | Required execution-evidence match |
| P13.8 | Headless smoke command | P13.4 | Sequential clean run |
| P13.9 | Required runner smoke profiles | P13.8 | Threaded and async clean runs |
| P13.10 | Canonical browser scenario | P12, P13.4 | Chromium end-to-end test |
| P13.11 | Demo data isolation and reset | P13.4 | Absolute target safety tests |
| P13.12 | Packaged no-Node runtime check | P13.4 | Isolated environment run |

## Phase 14 packages

| ID | Deliverable | Dependencies | Minimum verification |
|---|---|---|---|
| P14.1 | Windows verification matrix | P13 | Clean Windows run |
| P14.2 | Linux verification matrix | P13 | Clean Linux run |
| P14.3 | Runtime capability matrix | P7.15, P13 | Supported profile tests |
| P14.4 | Performance harness | P13.3 | Reproducible JSON result |
| P14.5 | Memory and queue profiling | P14.4 | Bounded growth assertions |
| P14.6 | Threat-model verification | Security packages | Threat test suite |
| P14.7 | Dependency and license audits | P1 | No unresolved critical finding |
| P14.8 | Content Security Policy | P10.15, P12 | Browser policy test |
| P14.9 | Python package build | P13.4 | Install and run built package |
| P14.10 | Frontend distribution verification | P12, P10.15 | Rebuild produces no diff |
| P14.11 | Nightly stress workflow | P7.19, P13 | Successful repeated run |
| P14.12 | Release verification command | P14.1–P14.11 | One-command complete gate |
| P14.13 | Third-party notices | P14.7 | License inventory review |

## Phase 15 packages

| ID | Deliverable | Dependencies | Minimum verification |
|---|---|---|---|
| P15.1 | Final README structure and narrative | P13, P14 | Claim-to-evidence review |
| P15.2 | System architecture diagram | P14 | Implementation-name audit |
| P15.3 | Data commit and recovery diagrams | P14 | Sequence audit |
| P15.4 | Execution-runner comparison diagram | P14.4 | Metric-definition audit |
| P15.5 | Guided code tour | P14 | Every link resolves |
| P15.6 | Verification report | P14.12 | Report generated from evidence |
| P15.7 | Threat-model summary | P14.6 | Security review |
| P15.8 | Reproducible UI screenshots | P12.20, P13 | Playwright capture passes |
| P15.9 | Animated canonical demonstration | P13.10 | Scenario and alt text review |
| P15.10 | Documentation link and command checks | P15.1–P15.9 | Clean-clone documentation test |
| P15.11 | Final confidentiality and content audit | P15.1–P15.10 | Repository audit passes |
| P15.12 | Release tag and artifact hashes | P14.12, P15.11 | Release checklist passes |

## Optional Phase 16 packages

| ID | Deliverable | Dependencies | Minimum verification |
|---|---|---|---|
| P16.1 | Go protocol and scope decision | P15 | Accepted decision record |
| P16.2 | Deterministic Go source simulator | P16.1 | Cross-language contract tests |
| P16.3 | Go load generator | P16.2 | Repeatable load profile |
| P16.4 | Trace-context propagation | P16.2 | End-to-end trace test |
| P16.5 | Python and Go comparison report | P16.3 | Disclosed environment and metrics |
| P16.6 | Cross-platform binary build | P16.2 | Windows and Linux runs |

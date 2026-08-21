# Delivery plan

## Phase grading

Each phase is scored out of 100:

- **A, 90–100:** accepted; dependent work may proceed.
- **B, 80–89:** remediation required before dependent work.
- **C, 70–79:** incomplete or insufficiently verified.
- **F, below 70:** rejected.

A phase cannot receive an A when a hard gate fails. Rubric points are earned from reproducible evidence, not from planned behavior.

## Phase 0 — Repository and product charter

**Objective:** Establish ParityGrid identity, confidentiality language, repository hygiene, and an authoritative planning baseline.

**Inputs:** Approved name and description.
**Outputs:** Initialized repository, product charter, architecture instruction set, repository metadata, and documentation index.
**Dependencies:** None.

**Acceptance evidence:**

- Repository is on `main` with the approved description.
- Documentation links resolve.
- NDA language is present and discloses no protected detail.
- Repository-content audit passes.

**Rubric:**

| Criterion | Points |
|---|---:|
| Identity and positioning | 20 |
| Confidentiality and deliberate boundaries | 25 |
| Instruction completeness and consistency | 30 |
| Repository hygiene | 15 |
| Reproducible validation | 10 |

**Hard gates:** Incorrect confidentiality language, missing authoritative index, or repository-content violation.

## Phase 1 — Toolchain and modular skeleton

**Objective:** Create reproducible Python and frontend packages with a minimal working FastAPI-to-browser path.

**Outputs:** `pyproject.toml`, `uv.lock`, package skeleton, Vite application, Tailwind tokens, application factory, CLI, health route, static checks, and initial CI.

**Dependencies:** Phase 0.

**Acceptance evidence:**

- `uv sync` succeeds on Windows and Linux.
- Python package imports without starting resources.
- Health route passes an HTTP test.
- Frontend development and production builds succeed.
- Strict type, lint, format, and import-boundary checks pass.

**Rubric:**

| Criterion | Points |
|---|---:|
| Reproducible toolchain | 25 |
| Modular package boundaries | 25 |
| Static verification and CI | 30 |
| Minimal smoke path | 20 |

**Hard gates:** Clean-clone failure, outer dependency in the domain, or missing lockfiles.

## Phase 2 — Pure domain foundation

**Objective:** Define trusted values, state machines, canonicalization, and core reconciliation semantics without infrastructure dependencies.

**Outputs:** Domain identifiers, canonical records, pipeline definitions, run and work state, reconciliation classifications, repair rules, canonical serializer, and fingerprinting.

**Dependencies:** Phase 1.

**Acceptance evidence:**

- Every valid and invalid lifecycle transition is tested.
- Canonical serialization is versioned and stable.
- Fingerprints are independent of arrival order.
- Domain import-boundary check passes.
- Property tests cover Unicode, numeric, date, and collection boundaries.

**Rubric:**

| Criterion | Points |
|---|---:|
| Domain correctness | 35 |
| Explicit invariants | 25 |
| Unit and property evidence | 25 |
| Clarity and dependency purity | 15 |

**Hard gates:** Unstable canonical fingerprint, invalid transition accepted, or domain infrastructure dependency.

## Phase 3 — SQLite persistence and migrations

**Objective:** Implement the authoritative operational store and serialized durable write boundary.

**Outputs:** SQLAlchemy models, Alembic migration, repositories, transactional writer, SQLite capability validation, and persistence tests.

**Dependencies:** Phases 1 and 2.

**Acceptance evidence:**

- Empty and prior-schema migrations pass.
- Constraints are exercised by failure tests.
- Attempt result, checkpoint, events, and aggregates commit atomically.
- Concurrent readers and queued writes survive stress.
- Abrupt termination reopens cleanly.

**Rubric:**

| Criterion | Points |
|---|---:|
| Schema and migrations | 25 |
| Transactional correctness | 30 |
| Concurrency behavior | 20 |
| Recovery tests | 20 |
| Schema clarity | 5 |

**Hard gates:** Partial committed transition, migration data loss, session sharing, or unsupported SQLite concurrency profile.

## Phase 4 — Artifact, Parquet, and DuckDB foundation

**Objective:** Add safe content-addressed artifacts and rebuildable analytical projections.

**Outputs:** Artifact store, commit protocol, manifests, Parquet schemas, DuckDB coordinator, analytical views, and integrity scanner.

**Dependencies:** Phases 2 and 3.

**Acceptance evidence:**

- Failpoints cover every artifact commit stage.
- Manifest hashes match files.
- DuckDB is rebuilt from committed inputs.
- Golden Python and DuckDB outputs agree.
- Unsafe paths are rejected.

**Rubric:**

| Criterion | Points |
|---|---:|
| Artifact integrity | 25 |
| Analytical correctness | 25 |
| Recovery and rebuildability | 20 |
| Integration coverage | 20 |
| Bounded memory and query behavior | 10 |

**Hard gates:** Manifest-file ambiguity silently accepted, path escape, or authoritative state stored only in DuckDB.

## Phase 5 — Pipeline specification and planner

**Objective:** Validate, version, and compile a typed pipeline into a deterministic execution plan.

**Outputs:** Pipeline schema, node registry, DAG validation, port types, resource policy, immutable publication, plan compiler, and plan fingerprint.

**Dependencies:** Phases 2 and 3.

**Acceptance evidence:**

- Generated invalid DAGs are rejected.
- Unknown nodes and incompatible ports are rejected.
- Published versions cannot change.
- Visual layout does not affect logical plan fingerprints.
- Every path to a `repair.apply` node is rejected unless it first crosses a `repair.approval` node.

**Rubric:**

| Criterion | Points |
|---|---:|
| Planner correctness | 30 |
| Versioning and immutability | 20 |
| Validation quality | 20 |
| Unit and property evidence | 20 |
| Extension design | 10 |

**Hard gates:** Cyclic plan accepted, mutable published version, or arbitrary executable node.

## Phase 6 — Sequential execution and recovery

**Objective:** Implement the reference runner, scheduler, checkpoint commit sequence, lifecycle controls, and recovery.

**Outputs:** Scheduler, sequential runner, leasing, retry policy, normalized events, checkpoint commits, pause, resume, cancellation, run finalization, and startup recovery.

**Dependencies:** Phases 3, 4, and 5.

**Acceptance evidence:**

- A published and compiled synthetic Phase 2–6 fixture executes a five-node plan through the reference sequential runner and durable boundaries without claiming production connectors.
- The deterministic fixture locks bounded retry and quarantine classification.
- Pause reaches a stable checkpoint and resume continues from the captured scheduler frontier.
- Cancellation reaches its durable terminal state and cleans up owned resources.
- Controlled interruption and reopen resume from durable evidence without a duplicate committed effect.
- Expected durable event ordering and the versioned execution-evidence finalization fingerprint are locked.

**Rubric:**

| Criterion | Points |
|---|---:|
| Reference-runner correctness | 25 |
| Lifecycle semantics | 25 |
| Recovery and idempotency | 25 |
| Automated verification | 20 |
| Diagnostics | 5 |

**Hard gates:** Duplicate committed effect, missing committed checkpoint, deadlock, or ambiguous recovery accepted.

## Phase 7 — Concurrent execution strategies

**Objective:** Add bounded threaded and asyncio full-plan strategies behind application-owned scheduling, plus isolated subordinate CPU pools, without changing durable execution semantics.

**Outputs:** Carried Phase 6 race corrections, execution-evidence fingerprint migration, versioned runner-neutral contract, concurrent scheduler frontier, captured settings and capabilities, clock-driven rate policy, bounded capacity and channels, concurrency telemetry, parent-side result coordination, lifecycle and recovery coordination, shared conformance suites, threaded and asyncio strategies, subordinate process CPU pool, capability registration, optional interpreter pool, execution-evidence comparison, lifecycle matrix, and cross-platform stress and packaging proof.

**Dependencies:** Phase 6.

**Acceptance evidence:**

- P7.0 closes the lease-correlation validation, invalid-result resubmission, and pause acknowledgement/abort race findings with deterministic barriers.
- Existing Phase 6 execution-evidence digests are preserved byte-for-byte, named accurately, and backfilled with explicit version 2 before the runner contract lands.
- `(run_id, node_id, partition_key)` is the sole scheduled work unit; independent ready work overlaps and successors wait for durable predecessor completion.
- Sequential, threaded, and asyncio strategies pass the same versioned full-plan contract, lifecycle, recovery, bounds, and cleanup suite.
- Every configured capacity holds under saturation, and blocked persistence propagates bounded backpressure without memory growth or deadlock.
- Pause, cancellation, unknown writer outcomes, forced termination, restart, and stale-result fencing pass the complete lifecycle matrix.
- Out-of-order results commit through the parent coordinator with correct rebased aggregates, contiguous per-run durable events, and no direct worker SQLite access.
- The process pool executes only registered connector-free CPU operations through bounded versioned envelopes. `src/paritygrid/adapters/runners/process_workers/` passes transitive import isolation, spawn, crash, cancellation, shutdown, and orphan-process tests.
- An interpreter pool is either unavailable with a structured reason or passes the subordinate-pool suite; it is not required for core acceptance.
- At least fifty seeded shuffled runs preserve the versioned execution-evidence fingerprint and normalized causal evidence across required strategies. The comparison makes no Phase 9 reconciliation, repair, or target-state equivalence claim.
- Windows and Linux install the built wheel in isolation, exercise process spawn semantics, and close all owned threads, tasks, processes, queues, clients, files, and database owners within configured bounds.

**Rubric:**

| Criterion | Points |
|---|---:|
| Runner-neutral contract and execution-evidence equivalence | 25 |
| Bounded concurrency | 20 |
| Lifecycle, recovery, and SQLite coordination | 25 |
| Backpressure, isolation, and cleanup | 20 |
| Stress and packaging evidence | 10 |
| Implementation clarity | 10 |

**Hard gates:** Required-strategy execution-evidence mismatch, exceeded configured bound, premature dependency release, non-contiguous durable events, accepted stale result, unresolved writer ambiguity, deadlock, unreleased owned resource, failed process isolation, direct worker SQLite access, or missing Windows/Linux installed-wheel spawn evidence.

## Phase 8 — Synthetic systems and connectors

**Objective:** Build deterministic source and target simulators and production-shaped connectors.

**Outputs:** Dataset generator, async paginated API, blocking legacy API, file fixtures, warehouse target, fault scripts, connectors, and shared conformance tests.

**Dependencies:** Phases 5 through 7.

**Acceptance evidence:**

- Same seed creates identical fixture manifests.
- Fault scripts are reproducible.
- Pagination, rate limiting, duplication, malformed records, timeouts, and cancellation pass.
- Secret redaction is verified.
- Resources close after failure and cancellation.

**Rubric:**

| Criterion | Points |
|---|---:|
| Connector contract | 25 |
| Deterministic simulation | 20 |
| Fault coverage | 25 |
| Integration evidence | 20 |
| Connector documentation | 10 |

**Hard gates:** Non-deterministic canonical fixture, leaked secret, duplicate logical effect, or unbounded input.

## Phase 9 — Reconciliation and repair

**Objective:** Produce accurate conflict classifications, safe repair plans, idempotent application, and final verification.

**Outputs:** Normalization, matching, duplicate detection, conflict materialization, summaries, repair planning, approval, application, and target verification.

**Dependencies:** Phases 4 and 8.

**Acceptance evidence:**

- Every record receives one primary classification.
- Golden and generated datasets are correct.
- Python reference and DuckDB analytical results agree.
- Stale plans are blocked.
- Repeated application is idempotent.
- Failed application does not expose a partial accepted state.

**Rubric:**

| Criterion | Points |
|---|---:|
| Reconciliation accuracy | 30 |
| Repair safety | 25 |
| Canonical verification | 15 |
| Golden and property evidence | 20 |
| Dataset-scale behavior | 10 |

**Hard gates:** Misclassified golden record, stale plan applied, destructive deletion supported, or duplicate repair effect.

## Phase 10 — FastAPI and live transport

**Objective:** Expose versioned HTTP contracts, durable event streaming, telemetry, and packaged frontend delivery.

**Outputs:** API composition, route groups, Problem Details, idempotency, SSE, WebSocket, artifact streaming, OpenAPI export, generated TypeScript types, and static delivery.

**Dependencies:** Phases 3 through 9.

**Acceptance evidence:**

- Contract and failure tests pass.
- OpenAPI and generated types reproduce exactly.
- SSE resumes from the last durable event.
- Slow clients do not block execution.
- WebSocket disconnect does not affect run state.
- Artifact paths remain confined.

**Rubric:**

| Criterion | Points |
|---|---:|
| API contract | 25 |
| Validation and errors | 20 |
| Live transport | 20 |
| Security boundaries | 15 |
| API verification | 20 |

**Hard gates:** Secret exposure, unbounded body, lost durable event replay, API contract drift, or unsafe artifact access.

## Phase 11 — UI foundation

**Objective:** Establish the custom design system, application shell, typed API access, coherent event handling, and accessible shared states.

**Outputs:** Tailwind tokens, owned components, routing, API client, query configuration, event clients, state coherence reducers, application shell, and component tests.

**Dependencies:** Phases 1 and 10.

**Acceptance evidence:**

- Shared loading, empty, error, stale, and disconnected states are tested.
- Keyboard navigation and automated accessibility checks pass.
- Event sequence gaps trigger recovery.
- No hardcoded feature-state colors bypass tokens.
- API Problem Details are displayed safely.

**Rubric:**

| Criterion | Points |
|---|---:|
| Design-system consistency | 25 |
| API and event reliability | 20 |
| Accessibility | 20 |
| Component verification | 20 |
| Responsive quality | 15 |

**Hard gates:** Inaccessible primary workflow, mixed incompatible state, unsafe HTML, or broken production build.

## Phase 12 — Product interface

**Objective:** Complete the operations overview, pipeline studio, live execution, reconciliation, repair, and comparison experiences.

**Outputs:** All product routes and their browser workflows.

**Dependencies:** Phase 11 and the corresponding backend capability.

**Acceptance evidence:**

- Each feature arrives with component and browser tests.
- Refresh during active execution restores coherent state.
- Large conflict sets remain usable.
- Stale plans are visibly blocked.
- Visual baselines are captured from fixed scenario states.

**Rubric:**

| Criterion | Points |
|---|---:|
| Complete workflows | 30 |
| Real-time state coherence | 20 |
| Error and stale-state handling | 15 |
| End-to-end evidence | 20 |
| Visual polish | 15 |

**Hard gates:** Incomplete canonical workflow, incoherent version display, unconfirmed repair, or failing primary browser scenario.

## Phase 13 — Canonical demonstration

**Objective:** Deliver the deterministic showcase, controlled failures, recovery story, headless smoke commands, and runner proof.

**Outputs:** Versioned scenario, fast and showcase datasets, demo CLI, failure controls, verification manifest, smoke suite, and canonical browser scenario.

**Dependencies:** Phases 6 through 12.

**Acceptance evidence:**

- Exact accepted, rejected, quarantined, conflict, repair, and final counts are locked.
- Required runners share one fingerprint.
- Forced termination resumes correctly.
- Demo changes only its isolated data directory.
- Packaged demo requires neither Node.js nor a database service.

**Rubric:**

| Criterion | Points |
|---|---:|
| Canonical story | 25 |
| Crash and recovery proof | 25 |
| Smoke and browser reliability | 25 |
| Deterministic evidence | 15 |
| Demo usability | 10 |

**Hard gates:** Runner mismatch, non-isolated cleanup, external-service requirement, or unreproducible scenario.

## Phase 14 — Hardening and release engineering

**Objective:** Prove cross-platform reproducibility, bounded performance, security posture, packaging, and release automation.

**Outputs:** Windows/Linux matrix, runtime capability matrix, performance harness, threat review, audits, packaged build, nightly stress, and release verification command.

**Dependencies:** Phase 13.

**Acceptance evidence:**

- Clean Windows and Linux verification passes.
- No unbounded queue, thread, task, process, or memory growth is observed.
- No unresolved critical dependency finding remains.
- Frontend rebuild has no unexplained difference.
- Installed package contains and serves production assets.

**Rubric:**

| Criterion | Points |
|---|---:|
| Cross-platform reproducibility | 20 |
| Performance evidence | 20 |
| Security posture | 20 |
| Stress reliability | 20 |
| Release automation | 20 |

**Hard gates:** Critical vulnerability, packaging failure, broad unsafe bind, unbounded resource growth, or clean-clone failure.

## Phase 15 — Public documentation and release

**Objective:** Publish a compelling, accurate, evidence-backed repository presentation.

**Outputs:** Final README, diagrams, code tour, verification report, screenshots, animated demonstration, third-party notices, and release tag.

**Dependencies:** Phase 14.

**Acceptance evidence:**

- Every major README claim links to implementation or test evidence.
- Commands run from a clean clone.
- Diagrams use real implementation names.
- Images come from canonical synthetic data.
- Links and alt text pass review.
- Final repository-content and confidentiality audits pass.

**Rubric:**

| Criterion | Points |
|---|---:|
| README impact and accuracy | 25 |
| Architecture diagrams | 20 |
| UI imagery and demonstration | 20 |
| Code tour and verification evidence | 20 |
| Clean release quality | 15 |

**Hard gates:** Unsupported claim, broken command, confidential content, missing attribution, or repository-content violation.

## Optional Phase 16 — Go load generator

**Objective:** Add a deterministic high-concurrency source and load generator without moving domain ownership out of Python.

**Dependencies:** Phase 15 or an explicit post-core decision.

**Rubric:**

| Criterion | Points |
|---|---:|
| Protocol compliance | 25 |
| Useful comparison evidence | 20 |
| Isolation from Python domain decisions | 20 |
| Automated verification | 20 |
| Public documentation | 15 |

**Hard gates:** Required for normal demo, inconsistent protocol behavior, or domain logic duplicated in Go.

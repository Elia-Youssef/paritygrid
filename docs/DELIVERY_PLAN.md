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
- At least fifty seeded shuffled runs preserve the versioned execution-evidence fingerprint and normalized causal evidence across required strategies. The comparison makes no Phase 10 reconciliation or Phase 11 repair and target-state equivalence claim.
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

## Remaining-phase split map

The former long Phase 8–16 sprints are replaced by the smaller goals below. Every former phase is split into at least two independently accepted phases.

| Former phase | Replacement phases |
|---|---|
| 8 — Synthetic systems and connectors | 8 — Deterministic synthetic systems; 9 — Connector contract and adapters |
| 9 — Reconciliation and repair | 10 — Reconciliation analysis; 11 — Repair and target verification |
| 10 — FastAPI and live transport | 12 — Core HTTP API; 13 — Live and generated API boundary |
| 11 — UI foundation | 14 — UI design system and shell; 15 — Typed frontend data and live state |
| 12 — Product interface | 16 — Pipeline and overview interface; 17 — Live execution observability; 18 — Reconciliation and repair interface |
| 13 — Canonical demonstration | 19 — Canonical scenario and datasets; 20 — Demo orchestration and smoke proof |
| 14 — Hardening and release engineering | 21 — Cross-platform performance and stress; 22 — Security, dependencies, and licensing; 23 — Packaging and release verification |
| 15 — Public documentation and release | 24 — Public narrative and architecture documentation; 25 — Reproducible evidence and content audit; 26 — Release-candidate handoff |
| Optional 16 — Go load generator | Optional 27 — Go protocol and simulator; Optional 28 — Go load and comparison |

Implementation phases stop at a verified handoff. Staging, commits, pushes, tags, pull requests, and release publication are separate integration actions outside these goals.

## Phase 8 — Deterministic synthetic systems

**Objective:** Build reproducible source and target simulators before connector code depends on them.

**Outputs:** Seeded inventory data, scripted failures, async and blocking source simulators, CSV and JSON Lines fixtures, a warehouse target, and lifecycle composition.

**Dependencies:** Phases 2, 5, 6, and 7.

**Acceptance evidence:** Identical seeds reproduce byte-stable manifests; pagination, malformed data, rate limits, timeouts, duplication, cancellation, target versions, and dynamic-port cleanup pass deterministic tests.

**Rubric:** Determinism 25; simulator behavior 25; fault coverage 20; lifecycle cleanup 20; documentation 10.

**Hard gates:** Non-deterministic canonical fixture, unbounded input, duplicate logical effect, fixed-port collision, or leaked owned resource.

## Phase 9 — Connector contract and adapters

**Objective:** Define one production-shaped connector contract and implement conforming source and target adapters over Phase 8 systems.

**Outputs:** Connector base contract, async and blocking HTTP sources, CSV and JSON Lines sources, warehouse target connector, shared conformance suite, and redaction behavior.

**Dependencies:** Phases 5 and 8.

**Acceptance evidence:** Every adapter passes the same lifecycle, bounds, cancellation, error, and redaction suite; repeated target effects remain idempotent.

**Rubric:** Contract clarity 25; adapter conformance 25; failure behavior 20; redaction 20; documentation 10.

**Hard gates:** Secret exposure, contract-specific scheduler branching, unbounded reads, duplicate target effect, or resource leak.

## Phase 10 — Reconciliation analysis

**Objective:** Produce complete, deterministic reconciliation classifications and analytical artifacts without mutating the target.

**Outputs:** Normalization, canonical matching, duplicate detection, classification, field differences, conflict artifacts, DuckDB queries, summaries, and reconciliation fingerprint.

**Dependencies:** Phases 4 and 9.

**Acceptance evidence:** Every record has exactly one primary classification; golden and generated cases pass; Python reference and DuckDB results agree; summaries and fingerprints reproduce.

**Rubric:** Classification accuracy 30; analytical agreement 20; artifact integrity 20; deterministic fingerprinting 20; scale behavior 10.

**Hard gates:** Misclassified golden record, incomplete classification, authoritative DuckDB state, non-deterministic fingerprint, or target mutation.

## Phase 11 — Repair and target verification

**Objective:** Convert reconciliation differences into safe approved repairs, apply them idempotently, and independently verify target parity.

**Outputs:** Repair plans, approval service, stale-plan fencing, idempotent target application, target-state fingerprint, and showcase-scale integration proof.

**Dependencies:** Phases 3, 9, and 10.

**Acceptance evidence:** Unsafe actions are absent; stale plans are rejected; replay does not duplicate effects; ambiguous failures recover safely; final target observation matches the expected fingerprint.

**Rubric:** Repair safety 30; approval and fencing 20; idempotency 20; target verification 20; integration evidence 10.

**Hard gates:** Destructive deletion, stale plan applied, duplicate effect, partial accepted state, or self-certified target parity.

## Phase 12 — Core HTTP API

**Objective:** Expose stable operational HTTP contracts and cross-cutting request protections before adding live transports.

**Outputs:** Application composition, lifespan ownership, Problem Details, correlation, command idempotency, pipeline, connector, run, and artifact routes, plus headers and request limits.

**Dependencies:** Phases 3 through 11.

**Acceptance evidence:** Startup and shutdown, route contracts, transition failures, replay conflicts, range requests, path confinement, headers, and oversized bodies pass.

**Rubric:** API composition 20; route contracts 25; error semantics 20; idempotency 20; security boundaries 15.

**Hard gates:** Secret exposure, unsafe artifact access, unbounded body, stranded ownership, or undocumented contract drift.

## Phase 13 — Live and generated API boundary

**Objective:** Add reconciliation and repair endpoints, durable and ephemeral live channels, reproducible API generation, and packaged frontend serving.

**Outputs:** Reconciliation and repair routes, resumable SSE, WebSocket telemetry, OpenAPI export, TypeScript generation, and SPA delivery.

**Dependencies:** Phases 7, 10, 11, and 12.

**Acceptance evidence:** SSE replay resumes without gaps, slow clients cannot block execution, WebSocket loss cannot alter run state, endpoint versions remain coherent, and generated files reproduce exactly.

**Rubric:** Domain routes 20; durable streaming 20; telemetry isolation 15; generated contracts 25; packaged serving 20.

**Hard gates:** Lost durable event, telemetry treated as authoritative, API/type drift, slow-client execution blockage, or broken production serving.

## Phase 14 — UI design system and shell

**Objective:** Establish an accessible owned design system and responsive application shell independent of product workflow complexity.

**Outputs:** Semantic tokens, primitive components, routing, navigation shell, async-state components, responsive rules, and error boundary.

**Dependencies:** Phases 1 and 13.

**Acceptance evidence:** Token and contrast checks, component interactions, keyboard navigation, route rendering, viewport behavior, safe failure detail, and recovery tests pass.

**Rubric:** Design consistency 25; accessibility 25; shell behavior 20; shared states 15; responsive quality 15.

**Hard gates:** Inaccessible navigation, unsafe HTML, hardcoded feature-state colors, unrecoverable shell failure, or broken production build.

## Phase 15 — Typed frontend data and live state

**Objective:** Create the typed query and event layer that keeps durable API state and ephemeral telemetry coherent.

**Outputs:** Generated API client integration, query configuration, SSE and WebSocket clients, and a sequence-aware run-state reducer.

**Dependencies:** Phases 13 and 14.

**Acceptance evidence:** Typed queries compile; cache/error behavior is tested; reconnect, disconnect, stale telemetry, older events, and sequence gaps trigger the specified recovery behavior.

**Rubric:** Type safety 20; query reliability 20; stream recovery 25; state coherence 25; diagnostics 10.

**Hard gates:** Mixed incompatible versions, silent sequence gap, telemetry overriding durable state, unsafe Problem Details rendering, or unrecoverable reconnect loop.

## Phase 16 — Pipeline and overview interface

**Objective:** Deliver the public overview, operations overview, pipeline library, and complete pipeline-authoring workflow.

**Outputs:** Overview routes, typed graph nodes, studio canvas, node inspector, plan preview, and publication flow.

**Dependencies:** Phases 5, 12, 14, and 15.

**Acceptance evidence:** Search, empty states, graph editing, validation, keyboard control, preview, publication, accessibility, and primary browser workflows pass.

**Rubric:** Workflow completeness 30; graph correctness 20; accessibility 20; error states 15; browser evidence 15.

**Hard gates:** Invalid graph publication, inaccessible primary workflow, incoherent plan version, unsafe rendered data, or failing publication scenario.

## Phase 17 — Live execution observability

**Objective:** Make active and historical execution understandable without confusing telemetry with durable truth.

**Outputs:** Run list, live graph overlay, worker swimlanes, saturation panels, durable timeline, runner comparison, and capability page.

**Dependencies:** Phases 7, 12, 13, 15, and 16.

**Acceptance evidence:** Refresh and reconnect restore coherent state; gaps recover; stale telemetry is visible; tables provide accessible chart alternatives; unavailable capabilities are explicit.

**Rubric:** State coherence 25; observability usefulness 20; accessibility 20; recovery behavior 20; comparison accuracy 15.

**Hard gates:** Telemetry presented as durable fact, hidden sequence gap, inaccessible chart-only evidence, misleading runner claim, or refresh corruption.

## Phase 18 — Reconciliation and repair interface

**Objective:** Deliver scalable conflict inspection and an explicit, stale-safe repair workflow, then lock visual baselines.

**Outputs:** Reconciliation summary, virtualized conflict table, conflict inspector, repair review, application progress, and deterministic visual regression baseline.

**Dependencies:** Phases 10, 11, 13 through 17.

**Acceptance evidence:** Large conflict sets remain usable; fingerprints stay coherent; destructive or stale actions are visibly blocked; confirmation and replay-safe progress pass browser tests; fixed states reproduce screenshots.

**Rubric:** Conflict usability 25; repair safety 25; version coherence 20; browser evidence 15; visual quality 15.

**Hard gates:** Unconfirmed repair, stale plan enabled, incoherent fingerprint display, unusable canonical dataset, or unstable visual baseline.

## Phase 19 — Canonical scenario and datasets

**Objective:** Lock the deterministic product story and its expected evidence before building orchestration around it.

**Outputs:** Versioned scenario, fast and showcase datasets, exact expected counts, and cross-runner verification manifest.

**Dependencies:** Phases 7 through 11.

**Acceptance evidence:** Repeated generation is identical; accepted, rejected, quarantined, conflict, repair, and final counts are exact; required runners share execution evidence without overclaiming target equivalence.

**Rubric:** Scenario coverage 25; determinism 25; expected evidence 25; runner proof 15; scale suitability 10.

**Hard gates:** Unreproducible scenario, runner mismatch, unlocked expected count, real or confidential data, or conflated fingerprint semantics.

## Phase 20 — Demo orchestration and smoke proof

**Objective:** Turn the canonical scenario into an isolated, recoverable, packaged demonstration with headless and browser proof.

**Outputs:** Demo CLI, controlled faults and process interruption, headless and runner smoke profiles, canonical browser scenario, safe reset, and no-Node runtime check.

**Dependencies:** Phases 6, 8, 13, and 16 through 19.

**Acceptance evidence:** Startup, URL reporting, controlled failure, forced termination and resume, all required runner profiles, browser flow, absolute-path reset safety, and isolated installed-package execution pass.

**Rubric:** Orchestration 20; recovery proof 25; smoke reliability 20; browser proof 20; isolation and packaging 15.

**Hard gates:** Non-isolated cleanup, external service required, Node.js required at runtime, runner mismatch, unsafe reset, or unreproducible fault.

## Phase 21 — Cross-platform performance and stress

**Objective:** Prove supported runtime profiles, bounded resources, and repeatable performance on Windows and Linux.

**Outputs:** Platform matrices, capability matrix, performance harness, memory and queue profiling, and nightly stress workflow.

**Dependencies:** Phases 7, 19, and 20.

**Acceptance evidence:** Clean installed-wheel runs pass on Windows and Linux; profiles disclose capabilities; results are reproducible JSON; memory, queues, tasks, threads, and processes remain bounded under repeated stress.

**Rubric:** Platform reproducibility 25; capability accuracy 15; performance method 20; resource bounds 25; stress reliability 15.

**Hard gates:** Unsupported profile advertised, unbounded growth, orphaned resource, undisclosed environment, or clean-platform failure.

## Phase 22 — Security, dependencies, and licensing

**Objective:** Close the threat model, dependency, browser-policy, license, and attribution gates independently of packaging mechanics.

**Outputs:** Threat verification, dependency and license audits, Content Security Policy, and third-party notices.

**Dependencies:** Phases 1, 13, 16 through 18, and 20.

**Acceptance evidence:** Threat tests pass; no unresolved critical finding remains; CSP blocks prohibited behavior without breaking the canonical UI; notices match the dependency inventory.

**Rubric:** Threat coverage 30; dependency posture 25; browser policy 20; license accuracy 15; remediation evidence 10.

**Hard gates:** Critical vulnerability, missing attribution, unsafe broad bind, CSP bypass, credential exposure, or unresolved prohibited license.

## Phase 23 — Packaging and release verification

**Objective:** Produce reproducible Python and frontend distributions and one complete release-readiness command.

**Outputs:** Built Python package, verified frontend distribution, installed-package checks, and aggregate release verification command.

**Dependencies:** Phases 13, 16 through 18, 20, 21, and 22.

**Acceptance evidence:** Clean builds reproduce; the installed package contains and serves production assets; the aggregate command runs every required platform-independent gate and reports failures precisely.

**Rubric:** Python packaging 25; frontend reproducibility 25; installed behavior 20; verification automation 20; diagnostics 10.

**Hard gates:** Packaging failure, unexplained rebuild diff, missing production asset, clean-clone failure, or incomplete verification command.

## Phase 24 — Public narrative and architecture documentation

**Objective:** Write an accurate public repository narrative tied directly to implementation and evidence.

**Outputs:** Final README, system architecture, data commit and recovery diagrams, runner comparison diagram, guided code tour, and threat-model summary.

**Dependencies:** Phases 19 through 23.

**Acceptance evidence:** Every major claim links to evidence; diagrams use actual implementation names and correct sequencing; code-tour links resolve; security claims match verified scope.

**Rubric:** Narrative accuracy 25; architecture clarity 20; diagram correctness 20; code tour 20; security communication 15.

**Hard gates:** Unsupported claim, broken implementation link, inaccurate diagram, confidential content, or misleading security statement.

## Phase 25 — Reproducible evidence and content audit

**Objective:** Generate the public verification report and canonical media, then audit all documentation and repository content.

**Outputs:** Verification report, reproducible screenshots, animated demonstration, link and command checks, confidentiality review, and repository-content audit.

**Dependencies:** Phases 18, 20, 23, and 24.

**Acceptance evidence:** Evidence is regenerated from canonical data; links, commands, alt text, and media inventory pass; clean-clone documentation checks and final confidentiality/content audits pass.

**Rubric:** Evidence traceability 25; reproducible media 20; documentation verification 20; accessibility 15; content safety 20.

**Hard gates:** Broken command, unstable canonical media, missing alt text, confidential content, untracked media provenance, or repository-content violation.

## Phase 26 — Release-candidate handoff

**Objective:** Produce a reviewable, immutable release-candidate evidence bundle without performing any Git or hosting mutation.

**Outputs:** Reproducible artifact hash manifest and completed release-candidate handoff checklist.

**Dependencies:** Phases 23 and 25.

**Acceptance evidence:** A second build reproduces every recorded hash; the checklist records exact verification commands, versions, supported platforms, limitations, and proposed next integration action.

**Rubric:** Hash reproducibility 30; checklist completeness 25; evidence traceability 25; limitation accuracy 10; handoff clarity 10.

**Hard gates:** Non-reproducible artifact, missing verification evidence, concealed limitation, staged file, commit, push, tag, pull request, or release publication.

## Optional Phase 27 — Go protocol and simulator

**Objective:** Define the optional Go boundary and implement a deterministic source simulator without moving domain ownership out of Python.

**Outputs:** Accepted scope decision, cross-language protocol contract, deterministic Go source simulator, and trace-context propagation.

**Dependencies:** Phase 26 or an explicit post-core decision.

**Acceptance evidence:** Protocol fixtures agree across languages; the simulator reproduces seeded behavior; traces propagate; normal Python demonstration remains independent of Go.

**Rubric:** Scope discipline 25; protocol compliance 25; determinism 20; trace behavior 15; automated verification 15.

**Hard gates:** Required for normal demo, domain logic duplicated in Go, inconsistent protocol behavior, or unreproducible fixture.

## Optional Phase 28 — Go load and comparison

**Objective:** Add a repeatable Go load generator and disclose a fair cross-language comparison with cross-platform binaries.

**Outputs:** Load generator, repeatable profiles, Python and Go comparison report, and Windows/Linux binary builds.

**Dependencies:** Phase 27.

**Acceptance evidence:** Profiles reproduce within documented tolerances; environment and metric definitions are disclosed; comparison never substitutes throughput for correctness; binaries pass supported-platform tests.

**Rubric:** Load reproducibility 25; comparison validity 25; correctness safeguards 20; cross-platform builds 20; documentation 10.

**Hard gates:** Misleading comparison, undisclosed environment, correctness regression, platform build failure, or Go becoming required for the normal product.

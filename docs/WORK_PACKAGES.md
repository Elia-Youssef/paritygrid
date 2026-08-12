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
| P6.10 | Run finalization and verification | P2.12, P4.9 | Final fingerprint tests |
| P6.11 | Startup recovery scanner | P3.15, P4.11, P6.7 | Interrupted state matrix |
| P6.12 | Sequential end-to-end integration fixture | P6.1–P6.11 | Complete pipeline test |

## Phase 7 packages

| ID | Deliverable | Dependencies | Minimum verification |
|---|---|---|---|
| P7.1 | Shared runner conformance suite | P6.2 | Sequential suite passes |
| P7.2 | Bounded thread runner | P7.1 | Bound and equivalence tests |
| P7.3 | Structured async runner | P7.1 | Bound and equivalence tests |
| P7.4 | Per-connector capacity limiter | P7.2, P7.3 | Saturation tests |
| P7.5 | Rate limiter and delay parser | P6.5 | Clock-driven tests |
| P7.6 | Bounded result channel | P6.6 | Backpressure and cancellation tests |
| P7.7 | Process runner | P7.1, P7.6 | Serialization and isolation tests |
| P7.8 | Runtime capability detection | P1.6, P7.2, P7.3, P7.7 | Supported and unavailable tests |
| P7.9 | Optional interpreter runner | P7.1, P7.8 | Conformance or unavailable result |
| P7.10 | Cross-runner comparison harness | P6.12, P7.2, P7.3 | Fingerprint equivalence |
| P7.11 | Concurrency telemetry | P7.2–P7.7 | Metric name and bound tests |
| P7.12 | Repeated scheduling stress suite | P7.10 | Shuffled repeated runs |

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
| P10.12 | Live WebSocket telemetry | P7.11 | Connect and disconnect tests |
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
| P12.18 | Runner comparison dashboard | P7.10, P10.8 | Correctness-first display tests |
| P12.19 | System capabilities page | P7.8, P10.1 | Available/unavailable tests |
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
| P13.7 | Cross-runner verification manifest | P7.10, P13.2 | Required fingerprint match |
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
| P14.3 | Runtime capability matrix | P7.8, P13 | Supported profile tests |
| P14.4 | Performance harness | P13.3 | Reproducible JSON result |
| P14.5 | Memory and queue profiling | P14.4 | Bounded growth assertions |
| P14.6 | Threat-model verification | Security packages | Threat test suite |
| P14.7 | Dependency and license audits | P1 | No unresolved critical finding |
| P14.8 | Content Security Policy | P10.15, P12 | Browser policy test |
| P14.9 | Python package build | P13.4 | Install and run built package |
| P14.10 | Frontend distribution verification | P12, P10.15 | Rebuild produces no diff |
| P14.11 | Nightly stress workflow | P7.12, P13 | Successful repeated run |
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

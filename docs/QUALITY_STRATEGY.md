# Quality strategy

## Quality objectives

Verification must prove the claims that make ParityGrid valuable:

- Equivalent versioned execution evidence across required full-plan strategies.
- No partial durable transition.
- Idempotent recovery and repair effects.
- Explicit quarantine of invalid data.
- Coherent browser state.
- Bounded concurrency and memory.
- Reproducible local setup and demonstration.

## Verification layers

### Static verification

Python:

- Ruff lint and format check.
- Pyright strict checking.
- Import-boundary verification.
- A direct static isolation guard for the reserved process-worker package; P7.14 adds
  reachable-import verification before worker code is accepted.
- Packaging metadata validation.
- Isolated built-wheel installation and Windows/Linux spawn verification.
- Dependency audit.

Frontend:

- TypeScript strict checking.
- ESLint.
- Formatting verification.
- Production build.
- Generated API contract drift check.
- Dependency audit.

### Unit tests

Unit tests cover:

- Value objects and identifiers.
- State transitions.
- Canonical serialization.
- Fingerprinting.
- Fingerprint-kind and version separation.
- DAG validation.
- Reconciliation rules.
- Retry and rate-limit policies.
- Repair-plan rules.
- Artifact path validation.
- Frontend event reducers, formatting, and validation.

### Property-based tests

Hypothesis covers:

- Canonicalization independent of input order.
- Stable round trips for versioned encodings.
- Required-strategy execution-evidence equivalence on generated datasets.
- DAG cycle and reachability detection.
- Checkpoint monotonicity.
- Idempotent application.
- Unicode, numeric, date, decimal, and collection boundaries.
- Classification completeness and exclusivity.

Failed examples must record a reproducible seed or minimized case.

### Persistence integration tests

Use temporary file-based SQLite databases. Cover:

- Empty and upgrade migrations.
- Foreign keys and uniqueness.
- Rollback on failure.
- WAL readers during writes.
- Busy handling.
- Writer queue backpressure.
- Concurrent command conflicts.
- Out-of-order result commits with rebased aggregates and contiguous event sequences.
- Invalid pre-admission result correction without lease loss.
- Unknown writer outcome stopping admission and requiring recovery.
- Abrupt process termination and reopening.
- Event sequence and checkpoint atomicity.

### Analytical tests

- Golden Parquet inputs.
- Python reference calculation versus DuckDB result.
- Empty and malformed datasets.
- Schema evolution.
- Duplicate detection.
- Fingerprint stability.
- Rebuild from artifacts.
- Query behavior under configured thread limits.

### Contract tests

Every connector runs the same conformance suite:

- Success and empty result.
- Pagination and cursor continuation.
- Timeout and cancellation.
- Rate limiting.
- Malformed content.
- Duplicate data.
- Connection reset.
- Size limit.
- Redaction.
- Resource cleanup.

Sequential, threaded, and asyncio full-plan strategies run the same conformance suite:

- Dependency ordering.
- Independent-node and partition overlap.
- Attempt event shape.
- Pause generations and pause-abort races.
- Cancellation under saturation.
- Retry.
- Bounded result submission and durable acknowledgement.
- Restart from the durable scheduler frontier.
- Idempotent cleanup with no owned-resource leaks.
- Resource bounds.
- Versioned execution-evidence equivalence.

The process CPU pool and any enabled interpreter CPU pool run a separate subordinate-operation suite:

- Only registered connector-free CPU operations.
- Bounded versioned primitive envelopes.
- No plan scheduling, connectors, artifact ownership, or persistence access.
- Spawn startup, worker failure, cancellation, shutdown, and orphan prevention.
- Transitive import isolation for `src/paritygrid/adapters/runners/process_workers/`.

### API tests

- Media types and status codes.
- Request and response validation.
- Problem Details.
- Idempotency.
- Pagination.
- Content limits.
- Correlation IDs.
- OpenAPI snapshot.
- SSE replay and reconnect.
- WebSocket lifecycle.
- Static frontend and API fallback behavior.

### Concurrency tests

- Maximum threads, tasks, processes, and requests are respected.
- Independent ready nodes and partitions overlap without releasing successors before durable predecessor completion.
- Slow or blocked persistence propagates backpressure through every bounded channel.
- Cancellation under saturation completes within a bound.
- Pause reaches a stable checkpoint and its acknowledgement/abort race has exactly one durable winner.
- Repeated shuffled completion order preserves the versioned execution-evidence fingerprint and normalized causal evidence.
- Concurrent repair application produces one logical winner.
- Out-of-order results rebase aggregates and preserve a contiguous per-run event sequence.
- An unknown writer outcome stops new admission and enters recovery-required state.
- Process and interpreter workers cannot bypass the parent result coordinator or open SQLite.

### Recovery tests

Run the application in a subprocess and terminate at controlled failpoints:

- Before artifact rename.
- After artifact rename and before manifest commit.
- Before checkpoint commit.
- After checkpoint commit and before acknowledgement.
- While a result waits behind a blocked writer.
- After worker completion with an unknown writer outcome.
- During pause acknowledgement and abort coordination.
- During repair application.
- During final verification.
- During shutdown.

After restart, assert:

- No duplicate effect.
- No missing committed checkpoint.
- No stale result from prior ownership is accepted.
- Durable scheduler state reconstructs without serialized in-memory runner objects.
- Run and node aggregates and per-run event sequences remain consistent.
- Orphan files are reported.
- Ambiguous integrity failures are not silently accepted.
- The versioned execution-evidence fingerprint remains correct after recovery.
- Every owned thread, task, process, interpreter, queue, client, file, and database owner closes within the bound.

### Smoke tests

The current Phase 6 gate uses the implemented application smoke path and the synthetic
five-node sequential durable-boundary fixture:

```powershell
uv run paritygrid smoke
uv run pytest tests/execution/test_sequential_end_to_end.py
```

These commands verify the capability currently present; the synthetic fixture is not a
production-connector or complete reconciliation workflow.

The following Phase 20 runner smoke commands are planned and unavailable until their owning
packages and canonical scenario are accepted:

```powershell
uv run paritygrid demo --headless --runner sequential
uv run paritygrid demo --headless --runner threaded
uv run paritygrid demo --headless --runner asyncio
```

At Phase 20, each command must verify startup, migrations, simulator health, pipeline
publication, run completion, artifact integrity, reconciliation summary, the expected
kind-and-version fingerprints, and clean shutdown. A process CPU pool is a subordinate
capability and is not exposed as a full-plan `--runner` value.

### Frontend component tests

- Shared state components.
- Pipeline node behavior.
- Table filters and selections.
- Approval confirmation.
- Version coherence and stale state.
- Event gaps and reconnect.
- Keyboard navigation.
- Accessible names and roles.

### End-to-end tests

Canonical browser scenarios cover:

- Start and complete a run.
- Observe scripted retry and quarantine.
- Refresh during execution.
- Pause and resume.
- Cancel a run.
- Review conflicts.
- Generate and approve a repair plan.
- Apply repairs and verify parity.
- Compare required runners.
- Recover from a controlled interruption.

Chromium runs on each change. Chromium, Firefox, and WebKit run nightly and for releases.

### Performance verification

Record:

- Hardware, operating system, and runtime.
- Runner and configured concurrency.
- Dataset size.
- Total duration and records per second.
- p50, p95, and p99 latency.
- Queue wait and service time.
- Retry count.
- Peak in-flight work and memory.
- SQLite transaction latency.
- DuckDB query duration.

Correctness gates are absolute. Performance gates compare against a recorded baseline with disclosed tolerance and environment.

### Mutation verification

Run mutation testing nightly or before release for:

- Domain state transitions.
- Reconciliation classifications.
- Idempotency rules.
- Repair approval.
- Canonical hashing.

## Coverage thresholds

| Area | Minimum branch coverage |
|---|---:|
| Pure domain | 95% |
| Reconciliation | 95% |
| Application execution and recovery | 90% |
| Runner adapters and worker mechanics | 90% |
| Persistence invariants | 90% |
| API | 85% |
| Frontend state and utilities | 85% |
| UI components | 75% |

Coverage excludes generated types and committed production assets. A threshold exception requires a documented reason and compensating behavioral evidence.

## Flake policy

- A flaky test is a defect.
- Do not add unconditional retries to conceal it.
- Quarantine is temporary, time-bounded, and visible.
- Concurrency tests use deterministic coordination rather than arbitrary sleeps.
- Browser tests use observable conditions rather than fixed delays.
- Every test isolates ports, files, databases, clock, and random seed.

## Test data policy

- All fixtures are synthetic.
- Canonical datasets are versioned and reviewed.
- No production export or derived identifier is allowed.
- Large generated data is created deterministically during tests unless a compact reviewed artifact is necessary.
- Golden updates require a human-readable explanation of the expected behavior change.

## Phase acceptance

A phase is accepted only when:

- Its required tests pass.
- Static checks pass.
- Its rubric reaches at least 90.
- No hard gate fails.
- Verification evidence is reproducible from documented commands.

Hard-gate failures include data loss, required-strategy execution-evidence mismatch, exceeded concurrency bounds, unreleased owned resources, process-worker isolation failure, direct worker SQLite access, unresolved critical security issues, broken clean-clone setup, failing migrations, failing required tests, or repository-content violations.

## Phase 7 concurrent execution gates

- Shared full-plan conformance (`tests/execution/full_plan_conformance.py`) runs
  identical durable-evidence, barrier, retry, pause, cancellation, bounds, and
  cleanup assertions against the sequential, threaded, and asyncio strategies
  over the real SQLite scenario, plus pooled overlap and out-of-order completion
  proofs; defective doubles in `tests/execution/conformance_defects.py` must fail
  the assertions they target.
- Subordinate-pool conformance (`tests/execution/test_process_pool.py`) covers
  codec rejection vectors, spawn execution, worker crash and timeout
  classification, P7.6 CPU-permit gating, orphan-process checks, and a spawned
  canary proving workers cannot import the database driver or open the
  operational database.
- The lifecycle matrix (`tests/execution/test_lifecycle_matrix.py`) covers every
  full-plan strategy across completion, retry, pause/resume, cancellation,
  worker failure, unknown writer outcome, structural blocked-writer
  backpressure, repeated cleanup, and restart with expired and non-expired
  leases.
- The stress gate (`tests/execution/test_stress_shuffled.py`) runs fifty seeded
  shuffled executions plus a triple-strategy reproduction proof comparing
  normalized durable execution evidence across sequential, threaded, and
  asyncio.
- Import isolation for `src/paritygrid/adapters/runners/process_workers/` is
  enforced by the existing `paritygrid-check-boundaries` transitive scan.
- Cross-platform Windows/Linux CI execution of these gates passed on the Phase 7
  exact head in [run 32827063567](https://github.com/Elia-Youssef/paritygrid/actions/runs/32827063567).

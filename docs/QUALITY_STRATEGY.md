# Quality strategy

## Quality objectives

Verification must prove the claims that make ParityGrid valuable:

- Logical equivalence across execution strategies.
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
- Packaging metadata validation.
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
- Runner equivalence on generated datasets.
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

Every runner runs the same conformance suite:

- Dependency ordering.
- Attempt event shape.
- Cancellation.
- Retry.
- Result submission.
- Cleanup.
- Resource bounds.
- Logical equivalence.

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
- Slow persistence propagates backpressure.
- Cancellation under saturation completes within a bound.
- Pause reaches a stable checkpoint.
- Repeated shuffled completion order preserves the fingerprint.
- Concurrent repair application produces one logical winner.
- Process workers cannot bypass the result sink.

### Recovery tests

Run the application in a subprocess and terminate at controlled failpoints:

- Before artifact rename.
- After artifact rename and before manifest commit.
- Before checkpoint commit.
- After checkpoint commit and before acknowledgement.
- During repair application.
- During final verification.
- During shutdown.

After restart, assert:

- No duplicate effect.
- No missing committed checkpoint.
- Orphan files are reported.
- Ambiguous integrity failures are not silently accepted.
- The final fingerprint remains correct after recovery.

### Smoke tests

Required commands:

```powershell
uv run paritygrid smoke
uv run paritygrid demo --headless --runner sequential
uv run paritygrid demo --headless --runner threaded
uv run paritygrid demo --headless --runner asyncio
```

Each verifies startup, migrations, simulator health, pipeline publication, run completion, artifact integrity, reconciliation summary, expected fingerprint, and clean shutdown.

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
| Execution engine | 90% |
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

Hard-gate failures include data loss, runner fingerprint mismatch, unresolved critical security issues, broken clean-clone setup, failing migrations, failing required tests, or repository-content violations.

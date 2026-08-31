# Data architecture

## Storage roles

ParityGrid uses three complementary local storage forms:

| Storage | Role | Authoritative |
|---|---|---|
| SQLite | Operational transactions and control state | Yes |
| Parquet files | Immutable or append-finalized run datasets | Yes, with committed manifest |
| DuckDB | Rebuildable analytical projections and query cache | No |

## SQLite configuration

The application must enable and verify:

```sql
PRAGMA foreign_keys = ON;
PRAGMA journal_mode = WAL;
PRAGMA synchronous = FULL;
PRAGMA busy_timeout = 5000;
```

The runtime must report the SQLite library version and reject the concurrency profiles when the installed library lacks required fixes or capabilities.

ParityGrid supports SQLite 3.35.0 or newer. This baseline includes native `RETURNING`
support for the operational repositories while retaining the required WAL, foreign-key,
full-synchronous, and busy-timeout behavior. Startup rejects a library older than this
baseline or one compiled without thread-safe connection support before opening the
operational database. The configured database path must be absolute and must not traverse
symbolic links or junctions. Startup also performs a no-change `user_version` write inside
a rolled-back transaction so read-only storage is rejected before the application reports
ready. Explicit startup probes verify the JSON SQL functions used by schema constraints and
native `RETURNING`; version text alone is not treated as proof that an optional build feature
is available. Python DB-API thread-safety levels 1 and 3 are supported: connection checkout
ownership ensures level 1 connections are never used simultaneously by multiple threads,
while level 0 is rejected.

### Connection ownership

- A SQLAlchemy `Session` or `AsyncSession` belongs to one task and one transaction scope.
- Sessions must never be shared between concurrent tasks or threads.
- Process workers must not write directly to SQLite.
- `src/paritygrid/adapters/runners/process_workers/` must not import persistence, runtime
  composition, connectors, or database libraries directly or transitively.
- Read sessions must remain short-lived to prevent checkpoint starvation.
- Migrations run before the API becomes ready.

### Transactional writer

Worker outputs enter a bounded result channel and then a parent-side result coordinator.
The coordinator validates the work-local lease fence and payload before writer admission. It
must not rely on run, node, or event row versions captured when concurrent work was admitted;
it rebases current aggregate and event frontiers at the serialized write boundary. The
transactional writer then:

1. Revalidates the result, lease, and checkpoint versions.
2. Opens a short database transaction.
3. Writes the attempt result.
4. Advances the work item.
5. Writes or advances the checkpoint.
6. Appends durable execution events.
7. Updates run and node aggregates.
8. Commits.
9. Publishes committed notifications.

If commit fails, no notification may describe the result as durable. A known rollback may use
a bounded contention policy. An unknown commit outcome stops admission and marks the run
recovery-required. Scheduled-work capacity and dependency readiness are released only after a
known durable commit. A payload rejected before writer admission retains its active lease so a
corrected bounded result may be submitted.

## Operational schema

### `system_metadata`

- `key` primary key.
- `value` text.
- Holds canonical-format version, schema metadata, and instance identity.

### `pipelines`

- Stable pipeline identity and display metadata.
- Soft archival is allowed; deletion is not required.

### `pipeline_versions`

- Immutable specification JSON.
- Monotonic version number within a pipeline.
- Specification hash and planner-format version.
- Publication timestamp.
- Unique `(pipeline_id, version_number)`.

### `connectors`

- Connector kind, display name, and validated configuration JSON.
- Capability and schema-discovery metadata.
- Secret values are never stored in connector configuration.

### `connector_secret_references`

- Immutable mapping from a connector-local reference name to an environment-variable name.
- Values contain references only; resolved secret values are prohibited.

### `runs`

- Pipeline-version reference.
- Captured runner configuration.
- Lifecycle state and monotonic row version.
- Canonical scenario seed where applicable.
- Start, finish, cancellation, and recovery timestamps.
- Execution-evidence fingerprint and its explicit fingerprint version.

### `run_event_counters`

- Next durable event sequence number for one run.
- A contiguous event range is advanced and populated in the same transaction.

### `run_nodes`

- Per-run, per-node aggregate state.
- Work counts, records, bytes, retries, and duration.
- Unique `(run_id, node_id)`.

### `work_items`

- Unique `(run_id, node_id, partition_key)`.
- State, lease owner, lease expiry, attempt count, and input reference.
- Checkpoint version expected by the item.
- Active attempt identity, runner, worker, and lease data while the durable state is `running`.
- `leased` remains a transient claim-validation state and is never persisted.

### `work_attempts`

- Immutable attempt history.
- Start and finish times, runner kind, worker identity, failure classification, and redacted detail.
- Unique `(work_item_id, attempt_number)`.
- Rows represent complete results only. Active attempt data remains on the work item until completion
  or lease-expiry recovery commits the immutable result.

### `checkpoint_heads`

- Current version and optimistic row version for one run, node, and partition.
- Created with the work item at version zero.

### `checkpoints`

- Append-only history of source cursors, output positions, and versions.
- Unique version per `(run_id, node_id, partition_key)`.
- Checkpoint payload has a versioned schema.

### `execution_events`

- Append-only durable event history.
- Monotonic sequence per run.
- Event kind, timestamp, subject, structured payload, and schema version.
- Unique `(run_id, sequence_number)`.

### `idempotency_records`

- Scope, key, canonical request hash, status, and stored logical response.
- Reusing a key with a different request hash is a conflict.
- An in-progress reservation reuses mutable `updated_at` as its durable lease
  generation. After the bounded lease expires, exactly one identical retry
  atomically advances that generation and becomes the exclusive reclaimer;
  concurrent losers fail closed. Terminalization compares the exact claimed
  generation, and terminal rows remain immutable and replay byte-for-byte.

### `artifact_manifests`

- Run, node, partition, relative path, media type, schema version, byte size, row count, and SHA-256.
- A unique content identity prevents ambiguous duplicate registration.

### `reconciliation_summaries`

- Counts by classification.
- Source and target fingerprints.
- Reconciliation fingerprint.
- Analytical query version.

### `reconciliation_conflicts`

- Canonical key, classification, source references, target reference, field-level differences, and suggested resolution.
- Large raw payloads remain in artifacts and are referenced rather than duplicated.

### `repair_plans`

- Exact reconciliation fingerprint, immutable contents, and controlled status.
- Immutable source and target input identities plus policy, generation, normalization,
  analysis, and query-version identities; these fields are part of canonical plan content
  and cannot be changed after creation.
- Proposed, approved, applying, applied, rejected, or failed.

### `repair_approvals`

- Immutable approval fact for one exact plan and reconciliation fingerprint.
- Approver identity, timestamp, correlation ID, schema version, and redacted detail.
- Plan approval and status advancement commit in one transaction.

### `repair_actions`

- One action per canonical key and action kind.
- External idempotency key.
- Before and proposed after hashes.
- Application result and target version.

### `audit_entries`

- Append-only administrative and security events.
- Actor, operation, object, correlation ID, timestamp, and redacted structured detail.

### `target_state_verifications`

- One immutable independently observed target-state verification fact per observation.
- Verification identity, run, optional repair plan, reconciliation fingerprint, and optional plan content fingerprint.
- Observed fingerprint with its own explicit kind and version, expected fingerprint, verdict, observed and expected record counts, observed target version, timestamp, and redacted divergence evidence.
- Verdicts are parity-holding, parity-divergent, or observation-failed; rows never update or delete.

## Required database constraints

- All references use foreign keys.
- Terminal run states cannot be returned to active states by repository methods.
- Pipeline versions are immutable after insert.
- Approved repair plans cannot be edited.
- Checkpoint versions only increase.
- Event sequences are gap-free within a committed transaction series.
- Terminal idempotency records, repair plans, and repair actions cannot be mutated.
- Every checkpoint-head or event-counter update advances its authoritative sequence value.
- Artifact paths are relative, normalized, and confined to the configured root.
- Monetary values use signed integer minor units and explicit currency.
- Timestamps are stored in UTC with explicit parsing and formatting rules.
- Operational rows cannot be deleted through normal SQL paths. Demo reset removes only an explicitly
  validated database file.

### Physical storage contract

- UTC timestamps use fixed 27-character text: `YYYY-MM-DDTHH:MM:SS.ffffffZ`.
- Durations use integer microseconds.
- SHA-256 digests use 64 lowercase hexadecimal characters.
- Monetary values use integer minor units, a three-letter currency, and an exponent from zero to six.
- Structured values use deterministic JSON text with sorted keys, compact separators, NFC strings,
  and no floating-point values or non-finite numbers.
- Database checks enforce storage class, basic shape, ranges, JSON validity, and JSON top-level shape.
  Repositories apply exact domain parsing, normalization, size limits, and lifecycle rules.

Checkpoint-head equality with the maximum checkpoint history version and event-counter equality with
the maximum event sequence plus one are verified by repository transactions and startup integrity
checks. SQLite metadata cannot express these cross-row aggregate invariants directly.

## Fingerprint persistence

Fingerprint fields name the fact they identify and store an explicit kind-specific version. The plan
fingerprint belongs to immutable planning metadata. `runs.execution_evidence_fingerprint` and
`runs.execution_evidence_fingerprint_version` identify execution finalization evidence. Reconciliation
summaries retain the independently computed reconciliation fingerprint and analytical query version.
Target-state verification stores its own canonical fingerprint and input identity in
`target_state_verifications` with an explicit kind, fingerprint version, and observation version.
These values are not aliases and must not be copied between kinds. Repair fencing requires that a run
finalized its execution evidence, but it never compares or equates the execution-evidence and
reconciliation fingerprints.

The current Phase 6 finalization document is execution-evidence version 2. The first Phase 7 migration
renames the former `runs.final_reconciliation_fingerprint` storage meaning, adds the explicit version,
preserves every existing non-null digest byte-for-byte, and backfills its version as 2. Compatibility
reads of the former name are confined to the repository migration boundary; all new writes and public
contracts use the execution-evidence name. Empty-database, frozen-schema upgrade, downgrade-policy,
and mixed null/non-null fixtures must prove the migration before runner-contract work begins.

## Migration policy

Each schema change must include:

- An Alembic forward migration.
- A test that migrates an empty database.
- A test that migrates the previous released schema fixture.
- Assertions that required data and constraints survive.
- An updated schema reference.
- Clear handling for intentional irreversibility.

Migrations must not depend on network access or developer-specific state.

A compatibility rename must also document the old and new semantic names, the version assigned to
existing values, the repository compatibility window, and a byte-for-byte preservation assertion.

## Artifact store

### Layout

```text
data/
├── paritygrid.db
├── analytics.duckdb
└── artifacts/
    └── runs/
        └── <run-id>/
            ├── raw/
            ├── normalized/
            ├── target/
            ├── reconciliation/
            ├── repairs/
            └── manifest.json
```

### Commit protocol

1. Resolve and validate the target beneath the artifact root.
2. Write a uniquely named temporary file in the destination filesystem.
3. Flush and close the file.
4. Calculate SHA-256 and metadata.
5. Atomically rename the file to its final relative path.
6. Commit its SQLite manifest.
7. Publish the artifact-created event after commit.

An artifact file without a manifest is an orphan. A manifest without its file is an integrity failure. Startup verification reports both conditions without silently deleting evidence.

## Parquet rules

- Schemas are versioned.
- Monetary values remain integers.
- Timestamp timezone semantics are explicit.
- Output ordering is canonical when it contributes to a fingerprint.
- Partition filenames are deterministic only when safe; temporary names remain unique.
- Row counts, schema version, and content hash are stored in the manifest.
- Raw and normalized datasets are separated.

## DuckDB rules

- One analytics coordinator owns writable DuckDB access.
- Analytical database state is disposable and rebuildable.
- Query definitions are versioned with their output schema.
- Reconciliation queries operate only on committed artifacts.
- Query results include source manifest hashes.
- Large result sets are paginated or exported rather than materialized fully in the API process.
- DuckDB conflicts or corruption cannot invalidate SQLite operational history.

## Backup and reset

The demo reset command may remove only an explicitly created demo directory after resolving and validating the absolute target. It must never operate on the repository root or a broad user directory.

The export command should produce:

- A consistent SQLite backup.
- Committed artifacts and their manifests.
- Version and capability metadata.
- A verification manifest containing hashes.

## Data QA

Required tests include:

- Constraint and transaction tests against file-based SQLite.
- WAL concurrency and busy handling.
- Out-of-order concurrent result commits with rebased run/node aggregates and contiguous event sequences.
- Rejected pre-admission results retaining their lease and accepting one corrected resubmission.
- Unknown writer outcomes stopping admission and requiring durable recovery.
- Crash reopening.
- Migration from frozen fixtures.
- Execution-evidence fingerprint rename, version backfill, and byte-for-byte preservation.
- Failpoints at each artifact commit step.
- Manifest and file cross-checking.
- DuckDB rebuild from committed inputs.
- Golden analytical comparisons.
- Path traversal and unsafe filename rejection.
- Empty, malformed, Unicode, large-value, and schema-evolution fixtures.

## Concurrent execution readers (Phase 7)

`SQLiteAdmissionStateReader` and `SQLiteConcurrentRecoveryReader`
(`adapters/persistence/concurrent_execution.py`) extend the read-only
boundary for the concurrent engine: admission reads current work-item,
node, run, and event-frontier row versions in one short transaction,
and recovery assembles one coherent run/node/work evidence snapshot.
All durable mutations still flow exclusively through the single
transactional writer; no reader ever mutates or holds a session.

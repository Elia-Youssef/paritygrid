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
- Read sessions must remain short-lived to prevent checkpoint starvation.
- Migrations run before the API becomes ready.

### Transactional writer

Worker outputs enter a bounded result queue. A transactional writer:

1. Validates result and checkpoint versions.
2. Opens a short database transaction.
3. Writes the attempt result.
4. Advances the work item.
5. Writes or advances the checkpoint.
6. Appends durable execution events.
7. Updates run and node aggregates.
8. Commits.
9. Publishes committed notifications.

If commit fails, no notification may describe the result as durable. Retriable database contention uses a bounded policy. Permanent persistence failure stops new scheduling and marks the run for recovery.

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
- Secret references contain environment-variable names only.
- Capability and schema-discovery metadata.

### `runs`

- Pipeline-version reference.
- Captured runner configuration.
- Lifecycle state and monotonic row version.
- Canonical scenario seed where applicable.
- Start, finish, cancellation, and recovery timestamps.
- Final reconciliation fingerprint.

### `run_nodes`

- Per-run, per-node aggregate state.
- Work counts, records, bytes, retries, and duration.
- Unique `(run_id, node_id)`.

### `work_items`

- Unique `(run_id, node_id, partition_key)`.
- State, lease owner, lease expiry, attempt count, and input reference.
- Checkpoint version expected by the item.

### `work_attempts`

- Immutable attempt history.
- Start and finish times, runner kind, worker identity, failure classification, and redacted detail.
- Unique `(work_item_id, attempt_number)`.

### `checkpoints`

- Source cursor, output position, and version.
- Monotonic version per `(run_id, node_id, partition_key)`.
- Checkpoint payload has a versioned schema.

### `execution_events`

- Append-only durable event history.
- Monotonic sequence per run.
- Event kind, timestamp, subject, structured payload, and schema version.
- Unique `(run_id, sequence_number)`.

### `idempotency_records`

- Scope, key, canonical request hash, status, and stored logical response.
- Reusing a key with a different request hash is a conflict.

### `artifact_manifests`

- Run, node, partition, relative path, media type, schema version, byte size, row count, and SHA-256.
- A unique content identity prevents ambiguous duplicate registration.

### `reconciliation_summaries`

- Counts by classification.
- Source and target fingerprints.
- Final reconciliation fingerprint.
- Analytical query version.

### `reconciliation_conflicts`

- Canonical key, classification, source references, target reference, field-level differences, and suggested resolution.
- Large raw payloads remain in artifacts and are referenced rather than duplicated.

### `repair_plans`

- Exact reconciliation fingerprint and immutable status.
- Proposed, approved, applying, applied, rejected, or failed.
- Approval identity and timestamp when applicable.

### `repair_actions`

- One action per canonical key and action kind.
- External idempotency key.
- Before and proposed after hashes.
- Application result and target version.

### `audit_entries`

- Append-only administrative and security events.
- Actor, operation, object, correlation ID, timestamp, and redacted structured detail.

## Required database constraints

- All references use foreign keys.
- Terminal run states cannot be returned to active states by repository methods.
- Pipeline versions are immutable after insert.
- Approved repair plans cannot be edited.
- Checkpoint versions only increase.
- Event sequences are gap-free within a committed transaction series.
- Artifact paths are relative, normalized, and confined to the configured root.
- Monetary values use signed integer minor units and explicit currency.
- Timestamps are stored in UTC with explicit parsing and formatting rules.

## Migration policy

Each schema change must include:

- An Alembic forward migration.
- A test that migrates an empty database.
- A test that migrates the previous released schema fixture.
- Assertions that required data and constraints survive.
- An updated schema reference.
- Clear handling for intentional irreversibility.

Migrations must not depend on network access or developer-specific state.

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
- Crash reopening.
- Migration from frozen fixtures.
- Failpoints at each artifact commit step.
- Manifest and file cross-checking.
- DuckDB rebuild from committed inputs.
- Golden analytical comparisons.
- Path traversal and unsafe filename rejection.
- Empty, malformed, Unicode, large-value, and schema-evolution fixtures.

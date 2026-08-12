# Decision: embedded operational and analytical data stack

**Status:** accepted
**Date:** 2026-08-12

## Context

ParityGrid requires durable transactional application state, large reconciliation queries, local reproducibility, and no mandatory database server. One database model does not optimally cover both high-integrity control-plane writes and analytical scans over run artifacts.

## Decision

Use SQLite with SQLAlchemy and Alembic as the authoritative operational database. Use DuckDB as a rebuildable analytical engine over Parquet artifacts. Keep artifacts on the local filesystem behind a safe artifact-store interface.

SQLite owns pipeline metadata, runs, work items, attempts, checkpoints, durable events, idempotency records, repair approvals, and audit entries. DuckDB owns no irreplaceable state. It derives reconciliation and performance results from SQLite metadata and committed artifacts.

## Consequences

- The demonstration remains self-contained.
- Operational transactions retain relational constraints and migration history.
- Analytical queries can scan columnar data efficiently.
- A bounded transactional writer must coordinate SQLite writes.
- DuckDB access must be coordinated within the owning process.
- Multi-host execution is outside the initial scope.
- Database and artifact recovery require explicit verification.

## Alternatives considered

- PostgreSQL provides stronger multi-writer server behavior but violates the no-server requirement.
- MongoDB requires a service and does not improve the relational control-plane model.
- SQLite alone would make large analytical reconciliation less expressive and efficient.
- DuckDB alone is not the correct authority for multi-process scheduling and durable event coordination.

## Verification

- Migration and constraint tests for SQLite.
- WAL reader/writer stress tests.
- Artifact commit failpoint tests.
- DuckDB rebuild tests from Parquet and SQLite metadata.
- Golden comparisons between analytical queries and reference Python calculations.

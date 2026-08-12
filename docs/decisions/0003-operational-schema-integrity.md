# Decision: operational schema integrity boundaries

**Status:** accepted
**Date:** 2026-08-12

## Context

SQLite is the authoritative operational store. The first relational metadata must preserve exact UTC
and digest values, deterministic migration output, append-only recovery evidence, secret-reference
safety, and repair approval facts without relying on a database service or SQLite's permissive type
coercion.

## Decision

Use SQLAlchemy 2 declarative metadata with 21 explicitly named operational tables, constraints, and
indexes. Naming conventions remain a backstop, while every schema object receives a deliberate stable
name for migration review.

Persist timestamps as canonical UTC text rather than SQLite datetime values. Persist durations and
money as integers. Persist SHA-256 values as lowercase hexadecimal text. Store structured data through
a deterministic JSON codec independent of the canonical fingerprint protocol.

Normalize connector secret references into immutable rows containing environment-variable names only.
Split checkpoints into a mutable current head and append-only history. Allocate durable event sequences
through a per-run counter advanced in the same transaction as event insertion. Store repair approvals as
immutable facts bound by foreign key to the plan's exact reconciliation fingerprint.

Durable work claims end in `running`. The `leased` domain state validates the claim in memory but is not
observable in storage. Active attempt and lease fields remain on the work item until completion or
expiry writes the immutable attempt result.

Operational deletion is prohibited. Immutable history tables reject updates, while named mutable tables
protect identity and content columns and permit only lifecycle, optimistic-version, and aggregate fields.
Checkpoint-head and event-counter updates must advance their authoritative sequence value; changing only
the optimistic row version is invalid. Completed or failed idempotency records, terminal repair plans,
and applied or failed repair actions reject every later update, including no-op updates.
The initial migration owns trigger installation. Authoritative downgrade is intentionally irreversible.

The migration installs the complete 47-trigger declaration catalog: 21 deletion guards, 10 whole-table
update guards, 11 immutable-column guards, two monotonic-value guards, and three terminal-state guards.
These category counts and the exact stable names are inspection-tested so migration drift is visible.

## Consequences

- SQLite storage remains portable and self-contained.
- Recovery can distinguish active claims from complete attempt history.
- Checkpoint and event allocation can be tested for atomic rollback without aggregate sequence scans.
- Approval identity cannot be reconstructed from mutable plan state.
- Repositories must validate exact domain values and maintain cross-row head and counter consistency.
- Migrations must recreate and inspect named triggers when table-rebuild operations occur.

## Alternatives considered

SQLite timezone datetime columns were rejected because they do not preserve the required timezone
contract. JSON secret-reference maps were rejected because normalization makes secret rules inspectable.
Updating a single checkpoint row was rejected because recovery benefits from immutable history. Computing
event sequences with `MAX` was rejected because an explicit counter gives a clearer atomic allocation
boundary. Approval fields on repair plans were rejected because a mutable row is weaker evidence than an
immutable approval fact.

## Verification

- Compile SQLite metadata twice and compare exact output.
- Inspect every table, key, constraint, index, and trigger declaration.
- Exercise storage-class, timestamp, hash, JSON-shape, enumeration, and hybrid-parent failures with raw SQL.
- Prove immutable histories and identity columns reject mutation once migration triggers are installed.
- Prove the persistence package exports no ORM rows and creates no database during import.
- Migrate empty and frozen prior schemas on Windows and Linux in the migration work package.

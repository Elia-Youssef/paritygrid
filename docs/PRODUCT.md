# Product charter

## Identity

**Name:** ParityGrid
**Tagline:** Verifiable data reconciliation and observable I/O execution.
**Repository description:** Local-first data reconciliation and I/O execution showcase built with Python, FastAPI, SQLite, DuckDB, React and TypeScript. Inspired by a real-world system that remains confidential; this is an independent implementation using synthetic data.

## Purpose

ParityGrid demonstrates a trustworthy workflow for bringing inconsistent sources into a verified target state. It emphasizes data integrity, deterministic results, bounded concurrency, explicit recovery, and a polished operational interface.

The initial domain is synthetic inventory reconciliation. Sources include a paginated asynchronous API, a blocking legacy API, CSV files, and JSON Lines files. The target is a simulated warehouse. The system detects mismatches, creates a reviewable repair plan, applies approved non-destructive repairs, and verifies the final state.

## Primary user story

An operator loads a versioned pipeline, chooses an execution strategy, starts a reconciliation run, observes live progress and retries, inspects field-level conflicts, approves a repair plan, applies repairs, and receives a final canonical fingerprint proving the target reached the expected logical state.

## Canonical demonstration

The canonical scenario must:

1. Start from a clean isolated directory.
2. Initialize and migrate SQLite.
3. Start deterministic synthetic source systems.
4. Load a versioned pipeline through the normal public boundary.
5. Read data from asynchronous HTTP, blocking HTTP, and files.
6. Encounter scripted latency, rate limiting, malformed data, and a transient connection failure.
7. Quarantine invalid records without losing valid work.
8. Persist checkpoints before acknowledging completed partitions.
9. Support controlled interruption and recovery.
10. Produce a reviewable repair plan.
11. Require explicit approval before applying repairs.
12. Verify the final target state.
13. Produce the same logical fingerprint with sequential, threaded, and asynchronous runners.

## Required capabilities

- Versioned immutable pipeline definitions.
- Typed connectors without arbitrary executable nodes.
- Sequential reference execution.
- Bounded thread-pool execution for blocking I/O.
- Structured asynchronous execution for async I/O.
- Optional process execution for CPU-heavy transformations.
- Durable run, work-item, attempt, event, checkpoint, repair, and audit state.
- Embedded operational and analytical databases.
- Content-addressed Parquet artifacts.
- Live execution telemetry and durable event replay.
- Reconciliation summaries and record-level conflict inspection.
- Idempotent repair application.
- Headless smoke and verification commands.
- One-command local packaged demonstration.

## Deliberate boundaries

The initial public release must not include:

- Arbitrary Python or shell execution.
- Destructive target deletions.
- Real production credentials or data.
- A generic automation marketplace.
- Multi-tenant hosting.
- Internet-facing deployment claims.
- Distributed coordination requiring Redis or a database server.
- Claims of exactly-once delivery.
- Claims that this code reproduces a confidential production system.

The correct delivery statement is at-least-once work execution with idempotent effects and verified logical convergence.

## Local-first requirement

The normal demonstration must not require Docker, Redis, PostgreSQL, MongoDB, or another database service. Node.js is required only when rebuilding or testing the frontend. The packaged Python application must serve the committed production frontend.

## Confidentiality statement

The following text must appear prominently in the final public README:

> This public repository is an independently designed technical demonstration inspired by classes of data reconciliation and automation problems encountered in prior professional work. The underlying production system, client details, data, business rules, implementation, and operational metrics remain confidential under NDA and are not reproduced here. All code, scenarios, names, and datasets in this repository are synthetic.

## Definition of done

ParityGrid is ready for its first public release only when:

- A clean clone completes the documented setup.
- The canonical demonstration runs without an external service.
- Required runners produce the same final fingerprint.
- Controlled interruption resumes without duplicate effects.
- All release verification lanes pass on Windows and Linux.
- The operations console completes the entire user story.
- The README contains verified diagrams, screenshots, commands, boundaries, and test evidence.
- No confidential data or production-derived fixture is present.

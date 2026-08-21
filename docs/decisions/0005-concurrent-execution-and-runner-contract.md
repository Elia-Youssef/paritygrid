# Decision: concurrent execution and runner contract

**Status:** accepted
**Date:** 2026-08-20
**Decision scope:** Phase 7 scheduling, runner neutrality, lifecycle, recovery, and process isolation
**Supersedes:** The runner-contract and process-runner descriptions in `docs/EXECUTION_MODEL.md` before this decision

## Context

Sequential execution established durable leasing, checkpoint, result, lifecycle, and recovery boundaries. Concurrent execution must preserve those semantics while allowing independent work to overlap. A runner that owns DAG decisions or persistence would duplicate application policy and make recovery strategy-specific. Out-of-order results also make captured run-wide row versions unsafe unless one parent-side coordinator rebases and serializes their durable effects.

## Decision

The application layer owns orchestration. DAG nodes are dependency barriers; `(run_id, node_id, partition_key)` is the only scheduled and capacity-counted work unit. Attempts are immutable executions of that identity. A successor becomes ready only after every work item of every predecessor is durably terminal with an allowed aggregate outcome. Phase 7 does not introduce streaming edges.

The application contract is independently versioned. Version 1 defines immutable work assignments and results, strategy capabilities and limits, pause generations, cancellation, cleanup, and durable recovery frontiers. Wire values use bounded closed envelopes of primitives and stable protocol identifiers. They never carry sessions, writers, clients, callbacks, resolved secrets, lease tokens, or unrestricted filesystem paths.

Admission acquires applicable capacity, obtains a durable lease, and then submits work. Completion enters a bounded channel and is validated and committed by the parent-side result coordinator. Only durable commit releases dependencies or reports authoritative progress. Unknown writer outcomes stop admission and require recovery. Recovery reconstructs state only from durable evidence, fences late results from earlier ownership, and never serializes in-memory runner state.

Sequential execution remains the application semantic reference. Threaded and asyncio strategies are required full-plan mechanics behind the application scheduler. Adapter runners own only thread, task, and queue mechanics. Process execution is a subordinate CPU pool for connector-free, process-safe, serializable functions; it is not a full-plan runner. The parent owns artifacts, results, leases, and every SQLite write. `src/paritygrid/adapters/runners/process_workers/` is the process isolation boundary and must pass a transitive import rule that excludes persistence, runtime composition, connectors, and database libraries. An interpreter pool is optional, capability-detected, and subject to the same subordinate-worker restrictions.

The durable scheduler frontier records its schema version, plan fingerprint, control generation, per-node state, deterministic ready work, admitted work, results awaiting durable commit, retry waits, control state, and whether recovery is required. Lifecycle states distinguish new, open, running, quiescing, paused, cancelling, closed, and recovery-required. Sync and async entry points preserve one semantic contract without blocking an active event loop or nesting an event-loop runner.

## Consequences

- Scheduling and recovery policy remain runner-neutral.
- Concurrent completion order may differ while durable per-run event sequences remain contiguous.
- Connector calls consume connector permits but are subordinate operations, not separate scheduled work items.
- Capacity is not released early when a worker finishes but its result is still waiting for durable commit.
- Process speedups are limited to explicitly registered pure CPU operations.
- Contract or envelope changes require an explicit compatibility version and conformance evidence.

## Alternatives considered

- Node-level runner ownership was rejected because it hides partition concurrency and duplicates DAG policy.
- Full-plan process runners were rejected because they weaken isolation and persistence ownership.
- Persisting in-memory queues or futures was rejected because those objects are not durable recovery facts.
- Direct worker writes were rejected because SQLite has one coordinated transactional write boundary.

## Verification

- Shared sequential, threaded, and asyncio conformance tests cover scheduling, durable results, pause, cancellation, recovery, cleanup, and bounds.
- Barrier-controlled tests exercise out-of-order completion, a blocked writer, and an unknown commit outcome without exceeding capacity or releasing a successor early.
- Subprocess recovery tests terminate before and after lease, artifact, result, checkpoint, and acknowledgement boundaries.
- Process isolation tests scan `src/paritygrid/adapters/runners/process_workers/` transitively, reject forbidden imports and payloads, and prove workers cannot open a SQLite write path.
- Windows and Linux packaging tests exercise spawn startup, shutdown, and resource cleanup.

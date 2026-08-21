# Execution model

## Objective

The current implementation contains only the sequential semantic reference. This document defines the accepted Phase 7 target for bounded concurrency without changing durable execution meaning. Threaded and asyncio strategies are required full-plan strategies. A process pool is a subordinate CPU executor, and an interpreter pool is optional. All strategies preserve application-owned scheduling, persistence, lifecycle, and recovery policy.

## Pipeline node registry

The initial registry contains:

- Async HTTP source.
- Blocking HTTP source.
- CSV source.
- JSON Lines source.
- Normalize records.
- Validate records.
- Partition records.
- Reconcile with target.
- Generate repair plan.
- Approval gate.
- Apply simulated repairs.
- Verify target state.
- Export Parquet.

Each node definition declares a stable node-kind identifier, versioned configuration, typed ports, supported execution mechanics, resource requirements, retry and idempotency behavior, deterministic encoding, and conformance evidence. Arbitrary executable code nodes are prohibited.

## Planning and work decomposition

Publication validates graph shape, reachability, port compatibility, connector capabilities, required configuration, resources, repair safety, and canonical-format compatibility. The planner produces an immutable execution plan and plan fingerprint. JSON declaration order and visual layout do not change the logical plan fingerprint.

A node is a dependency barrier. The only scheduled, leased, and capacity-counted work unit is:

```text
(run_id, node_id, partition_key)
```

An attempt is an immutable execution of that work-item identity; retry increments the attempt number without changing the identity. Connector calls and registered CPU operations are subordinate calls made by a work item. They consume their applicable connector or CPU-pool permit but are not separate scheduler work items.

Independent ready nodes and their partitions may overlap. A successor becomes ready only after every work item of every predecessor is durably terminal and the predecessor aggregate permits continuation. Phase 7 uses barriered edges; it does not stream work across an unfinished predecessor.

## Runner-neutral contract

The application owns an independently versioned contract. Contract version 1 defines:

- `StrategyCapabilitiesV1`: strategy identifier, supported operation classes, lifecycle features, platform requirements, and hard limits.
- `WorkAssignmentV1`: contract version, plan and work identity, attempt, lease fencing identity, operation descriptor, bounded input references, captured settings, control generation, and deadline.
- `WorkResultV1`: contract version, matching identities, outcome, bounded metrics, artifact-manifest references, checkpoint proposal, redacted failure, and cleanup status.
- `RecoveryFrontierV1`: durable ownership, completed and unresolved work identities, control generation, and recovery-required reason.

Assignments and results are immutable. Envelopes use a bounded closed set of primitive values, normalized strings, lists, and maps with stable protocol names and explicit size limits. They never contain ORM sessions, database writers, connector clients, callbacks, resolved secrets, unrestricted filesystem paths, in-memory queues or futures, or bearer-style lease credentials.

Lifecycle states are `new`, `open`, `running`, `quiescing`, `paused`, `cancelling`, `closed`, and `recovery_required`. Cleanup is idempotent and returns structured evidence. Pause and cancellation carry a control generation so stale signals and results can be fenced. Sync and async entry points preserve identical semantics: a sync facade must not block an already running event loop, and an async facade must not call a nested event-loop runner.

## Strategy roles

### Sequential reference

- Lives with application execution policy.
- Runs one work item at a time through the same lease, result, checkpoint, event, and lifecycle boundaries.
- Defines semantic conformance, not a special persistence path.

### Threaded strategy

- Uses a bounded thread pool for blocking work.
- Receives only application-admitted assignments.
- Applies connector capacity without sharing sessions or database connections between threads.
- Structurally prevents nested-pool starvation and joins every owned thread during cleanup.

### Asyncio strategy

- Uses structured task ownership, bounded queues, semaphores, and explicit timeouts.
- Receives only application-admitted assignments.
- Propagates cancellation and closes clients, streams, tasks, and channels.
- Provides a safe sync boundary without blocking the active event-loop thread.

### Process CPU pool

- Is subordinate to a work item and never schedules a plan.
- Accepts only registered connector-free, process-safe CPU operations with versioned serializable envelopes.
- Returns serializable results to the parent; the parent owns artifacts, leases, result submission, and SQLite.
- Uses `src/paritygrid/adapters/runners/process_workers/` as its isolation boundary. A transitive import gate excludes persistence, runtime composition, connectors, and database libraries.
- Must pass spawn startup, cancellation, worker-failure, shutdown, and orphan-process tests on supported platforms before capability registration.

### Interpreter CPU pool

- Is optional, experimental, and subordinate under the same operation and isolation rules as the process pool.
- Reports structured unavailability when the runtime capability is absent.
- Is not required for the default demo or core correctness gate.

## Scheduler frontier and admission

The durable concurrency model is `SchedulerFrontierV2`. It records:

- Frontier version, plan fingerprint, and control generation.
- Per-node aggregate state and deterministic ready work identities.
- Admitted work and its lease fencing identity.
- Results received but awaiting durable commit.
- Retry-wait entries and eligible times from the injected clock.
- Running, quiescing, paused, cancelling, or recovery-required control state.
- A recovery-required reason when the writer outcome or integrity state is unknown.

Admission proceeds in this order:

1. Re-evaluate durable dependency readiness and control state.
2. Acquire global, strategy, and node scheduled-work capacity in a stable order.
3. Obtain the durable work lease and attempt identity.
4. Submit the immutable assignment.
5. Retain capacity until the result is durably accepted or the lease is resolved by recovery.

The executing work item acquires connector or subordinate CPU-pool capacity immediately before the applicable call and releases that subordinate permit when the call ends. These permits cannot replace or release the scheduled-work permit. All queues and channels are bounded and closeable. A blocked downstream writer must slow admission rather than grow memory.

## Result and SQLite coordination

Workers return results through a bounded channel to the parent-side result coordinator. The coordinator validates contract version, work and attempt identity, lease owner and expiry, control generation, artifact references, and bounded payloads before writer admission.

Captured global run, node, or event row versions are not authoritative across out-of-order completions. At serialized writer admission, the coordinator uses the work-local lease fence and rebases current run/node aggregates and the event frontier. One short SQLite transaction writes the attempt result, work state, checkpoint, aggregates, and a contiguous durable event range. Only commit acknowledgment releases dependencies, reports authoritative progress, and frees the scheduled-work permit.

A result rejected before writer admission leaves the active lease available for a corrected bounded resubmission. A known rollback may follow bounded persistence retry. An unknown commit outcome stops new admission, closes producer flow, and marks the run recovery-required; it is never guessed from an in-memory response.

## Bounded resources and rate policy

Immutable run settings capture limits for concurrent work, connector requests, thread and process workers, in-flight records, assignment and result channels, SQLite writer capacity, response and file bytes, and artifact bytes per run. Runtime capability facts are captured with the settings used to select a strategy.

Rate limits and retry delays use an injected clock. Delay parsing rejects negative, non-finite, overflowed, malformed, or policy-exceeding values. Capacity and token reservations are cancellation-safe and expose bounded telemetry. Ephemeral telemetry may report queue depth, capacity use, wait and service time, and cleanup state, but it never reconstructs authoritative execution.

## Retry classification

| Failure | Default action |
|---|---|
| Connection failure | Bounded retry |
| Timeout | Bounded retry |
| HTTP 429 | Retry after a validated policy-bounded delay |
| HTTP 5xx | Bounded retry |
| HTTP 4xx | Permanent unless explicitly classified |
| Validation failure | Quarantine |
| Idempotency conflict | Terminal conflict |
| SQLite contention | Short bounded persistence retry |
| User cancellation | Do not retry |

Retries use exponential backoff with bounded jitter. The canonical scenario uses a seeded jitter source.

## Pause, cancellation, and recovery

Pause increments the control generation, stops admission, and waits until active work has either reached a durable checkpoint boundary or has an explicitly recoverable lease. A pause-abort compare-and-set may clear only the same unacknowledged generation. If acknowledgement wins the race, the run completes pause and requires an explicit resume; it cannot expose a paused strategy while durable state says running.

Cancellation stops admission, signals owned work, closes channels in dependency order, and waits for bounded cleanup. Cleanup joins or closes every owned thread, task, process, interpreter, client, and queue and may be repeated safely.

Startup recovery runs under exclusive coordinator ownership and reconstructs `SchedulerFrontierV2` from SQLite, committed checkpoints, attempt history, idempotency facts, and artifact integrity. It fences late results from previous ownership, waits for or resolves non-expired leases according to the durable lease policy, and resubmits only work that durable evidence permits. In-memory runner state is never a recovery source. Integrity ambiguity or unknown writer outcome remains recovery-required until resolved explicitly.

## Fingerprints and strategy comparison

The fingerprint kinds are independent:

- A plan fingerprint identifies logical scheduling intent.
- An execution-evidence fingerprint identifies versioned durable execution evidence.
- A reconciliation fingerprint identifies a Phase 9 analytical snapshot.
- A target-state fingerprint identifies a Phase 9 observed target state.

The Phase 6 finalization digest is execution-evidence version 2. Phase 7 comparisons use the same fingerprint kind and version plus sorted durable work states, checkpoints, counts, artifact-manifest identities, and normalized causal events. Exact global event order may differ under concurrency and is not compared. Equal Phase 7 execution evidence does not claim equal reconciliation classifications, repair contents, effects, or target state.

## Execution QA

Required evidence includes:

- Common full-plan conformance for sequential, threaded, and asyncio strategies.
- Subordinate CPU-pool conformance for process and any enabled interpreter pool.
- Deterministic barrier tests for independent-node overlap, partition overlap, dependency release, pause races, and cancellation.
- Backpressure tests with a blocked writer and bounded assignment, result, telemetry, and writer queues.
- Subprocess termination before and after lease, artifact, result, checkpoint, event, and acknowledgement boundaries.
- Recovery from durable evidence without duplicate committed effects or acceptance of stale results.
- Exact cleanup assertions for threads, tasks, processes, interpreters, clients, queues, files, and database ownership.
- Windows and Linux spawn and installed-wheel verification for process isolation.
- Fifty or more shuffled repeated executions comparing versioned execution evidence across required strategies.

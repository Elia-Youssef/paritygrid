# Demo commands

ParityGrid ships one canonical demonstration: a bounded lifecycle that starts the
packaged application, its embedded SQLite and DuckDB databases, and three loopback
simulators; publishes the canonical pipeline; runs a deterministic story with two
scripted faults; executes the canonical engine plan under one selected runner; and
verifies every durable fact before reporting success. Success is never inferred from
a clean shutdown alone — the proof re-derives its facts from committed state and
exits 0 only when the complete proof holds.

All commands below run from the repository root with `uv run`. The demo binds only
`127.0.0.1` on dynamic port zero, uses embedded SQLite and DuckDB files inside its
owned demo root, and requires no Node.js and no external service.

## Commands at a glance

| Command | Purpose |
| --- | --- |
| `paritygrid demo --headless` | Full canonical proof, no browser, deterministic exit |
| `paritygrid demo` | Serve the packaged application until interrupted |
| `paritygrid demo-reset` | Safely delete one explicitly owned demo root |
| `paritygrid demo-faults` | Print the closed fault-control catalog |
| `paritygrid demo-compare` | Compare cross-runner execution evidence |
| `paritygrid demo-interruption` | Prove controlled interruption and durable recovery |
| `paritygrid stress performance` | Correctness-gated showcase performance harness |
| `paritygrid stress resources` | Memory, queue, cleanup, and orphan bounds exercise |
| `paritygrid stress capabilities` | Closed runtime capability matrix |
| `paritygrid stress wal` | Bounded SQLite WAL stress workload |

The `stress` commands are bounded verification workloads, not demo flows: each
writes a versioned JSON evidence report and is documented with the Phase 21
verification commands in [CI and release](CI_RELEASE.md).

## Stable exit codes

Every demo command uses the same exit-code contract:

| Code | Meaning |
| --- | --- |
| 0 | Success: the requested proof completed and held |
| 1 | Failure: a proof fact did not hold or an operation failed |
| 2 | Usage: invalid options or a refused demo-root path |
| 3 | Readiness timeout: a bounded startup step did not become ready |
| 4 | Cancellation: the run was interrupted before completion |

## Headless proof

```text
uv run paritygrid demo --headless --runner sequential --root <ABS> [--json] [--reset]
```

- `--runner` accepts the closed set `sequential`, `threaded`, `asyncio`; the default
  is `sequential`.
- `--root` takes an explicit absolute demo-root directory. Without it, a
  deterministic, isolated default root under the system temporary directory is used.
- `--reset` safely resets the owned demo root before starting; a refused reset is a
  usage error (exit code 2).
- Headless mode never opens a browser. It verifies startup, simulator health,
  pipeline publication, run completion, artifacts, reconciliation, repair, target
  verification, and clean shutdown, then exits 0.

The human summary names the story run, its reconciliation and target-state
fingerprints, the engine runner and run identity, and the engine execution-evidence
fingerprint. Wall-clock durations are printed as diagnostics only and never enter
the canonical result.

With `--json`, the command appends the deterministic result document
(`"format": "paritygrid.demo.result"`, result version 1) as one compact JSON line
with sorted keys.

```text
uv run paritygrid demo --headless --runner threaded --root "D:\demo\threaded" --json
```

## Serve mode

```text
uv run paritygrid demo [--root ABS] [--open-browser]
```

Without `--headless`, the same lifecycle keeps the packaged application serving and
reports the URL (for example `http://127.0.0.1:<port>/`) after readiness. Pass
`--open-browser` to open the reported URL in a browser on request. Pressing
Ctrl+C (or Ctrl+Break) shuts the lifecycle down gracefully; every owned process,
thread, task, client, file, and port is released in reverse startup order.

## Reset

```text
uv run paritygrid demo-reset --root <ABS>
```

Deletes one exact, explicitly owned demo root. Anything else is refused with exit
code 2 and the tree is left byte-identical. See the reset safety contract below.

## Fault-control catalog

```text
uv run paritygrid demo-faults [--json]
```

Prints the closed, versioned catalog of the two canonical fault controls. With
`--json` it prints the byte-stable catalog document
(`"format": "paritygrid.demo.fault-control"`, version 1). There is deliberately no
way to inject arbitrary code, URLs, SQL, paths, or scripts: every control binds to
a named scripted failure owned by the simulator model, and the canonical story
activates exactly the canonical set every run.

### The two fault controls

**1. Canonical asynchronous-source rate limit**
(`paritygrid.demo.fault-control/canonical.rate_limit/v1`)

- Activation point: the asynchronous source simulator rejects request 2 of the
  canonical story's first read attempt with an HTTP 429 response.
- Expected consequence: exactly 1 bounded retry attempt is recorded durably for the
  asynchronous source work item before it succeeds; no record is lost or quarantined
  by the fault.
- Recovery: the accepted named retry policy schedules one retry after the
  server-advised delay on the injected clock; the retry is the second attempt of the
  same work-item identity.
- Observable evidence: the run's durable attempt history carries one `http_429`
  classified attempt, the run-node retry count is 1, and the scenario manifest locks
  `rate_limit_retries`.
- Reset behavior: a fresh demo run recreates the simulator from the canonical
  failure script, so the fault re-fires deterministically at the same request
  sequence.

**2. Canonical transient warehouse connection loss**
(`paritygrid.demo.fault-control/canonical.warehouse_transient_failure/v1`)

- Activation point: the simulated warehouse drops the connection of mutating write
  3 of the canonical repair plan, after the effect was durably recorded in the
  simulator.
- Expected consequence: exactly 1 ambiguous outcome is raised for that repair action
  while the logical effect exists exactly once in the target.
- Recovery: the idempotent applier replays the same external idempotency key; the
  warehouse answers with the recorded outcome and the action completes without a
  second logical effect.
- Observable evidence: the scenario manifest locks `transient_connection_failures`
  and `ambiguous_replays_resolved`, and the simulator's applied-failure evidence
  shows exactly the one connection loss.
- Reset behavior: a fresh demo run recreates the warehouse from the canonical
  failure script, so the transient loss re-fires on the same repair action.

The definitions live in
[`src/paritygrid/demo/fault_controls.py`](../src/paritygrid/demo/fault_controls.py);
`paritygrid demo-faults` prints them without reading the source.

## Cross-runner comparison

```text
uv run paritygrid demo-compare --root <ABS> [--json]
```

Compares the durable execution evidence of the sequential, threaded, and asyncio
engine runs recorded in one demo root. Correctness comes first: the comparison
covers versioned execution evidence only and never claims reconciliation, repair,
or target-state equivalence. It exits 1 when any evidence differs, and 2 when the
demo root is refused. With `--json` it prints the byte-stable cross-runner
manifest.

Run all three runners against the same root first:

```text
uv run paritygrid demo --headless --runner sequential --root "D:\demo\canonical"
uv run paritygrid demo --headless --runner threaded  --root "D:\demo\canonical"
uv run paritygrid demo --headless --runner asyncio   --root "D:\demo\canonical"
uv run paritygrid demo-compare --root "D:\demo\canonical"
```

## Interruption and recovery

```text
uv run paritygrid demo-interruption --root <ABS> [--failpoint NAME] [--json]
```

Proves controlled process interruption and durable recovery without duplicate
effects. The harness runs the headless demo in an explicitly owned child process,
waits for a durable failpoint handshake written only after the named boundary's
commits returned durably, and terminates exactly that child process handle. The
same owned demo root then restarts without the failpoint and must recover from
committed state alone. The interrupted child must not have published the scenario
manifest; partial state is never presented as success. The simulated warehouse's
idempotency receipts and target rows are persisted independently under the scenario
root, so recovery verifies the observed target and proves every repair key produced
exactly one logical external effect across the process boundary.

The closed failpoint set:

| Failpoint | Durable boundary |
| --- | --- |
| `attempts.recorded` | after the retry attempt history is committed |
| `reconciliation.persisted` | after the reconciliation summary is committed |
| `repair.approved` | after the repair approval is committed (default) |
| `repair.applied` | after the repair application is committed |

Unknown failpoints fail closed. With `--json` the command prints the machine-readable
proof document (`"format": "paritygrid.demo.interruption-proof"`, version 1) after
the recovery checks pass.

These are canonical story boundaries. They do not substitute for the engine-level
forced-termination matrix around lease acquisition, result admission, checkpoint
commit, event append, and acknowledgement described in `QUALITY_STRATEGY.md`.

## Owned demo roots and the reset safety contract

A demo root is an outer directory whose only children are the ownership marker
(`paritygrid-demo-ownership.json`, format `paritygrid.demo.ownership`, version 1)
and the `scenario/` directory. The marker contains a fresh 256-bit ownership id.
Creation also writes a path-bound proof outside the deletion target in a private,
per-user temporary registry; both records must agree before deletion is possible.

```text
<demo-root>/
  paritygrid-demo-ownership.json
  scenario/
    canonical.db            operational SQLite database
    scenario-manifest.json  canonical manifest published after verification
    artifacts/              manifested artifacts with integrity digests
    analytics.duckdb        rebuildable analytics projection
    engine-analytics.duckdb engine-run analytics projection
```

The scenario directory stays a plain scenario root and data root of the composed
runtime; creation requires it to be nonexistent or empty.

`demo-reset` (and `demo --reset`) refuses, with exit code 2, before deleting
anything when the target is: relative or empty; a filesystem or drive root; the
home directory, one of its ancestors, or the Desktop, Documents, or Downloads
folders; the working directory, the repository, or any ancestor; the temporary
directory root itself; missing the ownership marker or external path-bound proof;
carrying a divergent schema or ownership id; holding unexpected entries beside the
marker and the scenario directory; or traversing — or containing — any symbolic
link, junction, or reparse point. Marker and proof files that are themselves links
are refused. Ownership is checked twice immediately before deletion, and a
successful reset removes the external proof. Every rejected reset leaves the tree
byte-identical, so unrelated files always survive.

## Fingerprints

The demo result distinguishes four fingerprint kinds, each with its own kind
identity and version. They are distinct kinds and are never equated with one
another:

| Kind | Version | Meaning |
| --- | --- | --- |
| `plan` | 1 | the published canonical plan |
| `execution-evidence` | 2 | the engine run's durable execution evidence |
| `reconciliation` | 1 | the reconciliation outcome derived for the story run |
| `target_state` | 1 | the independently verified observed target state |

Two runs with equal input facts produce equal fingerprints. Wall-clock durations
are diagnostics only and never enter canonical correctness bytes.

## Installed no-Node verification

The packaged product is proven, not assumed: the stable command

```text
uv run python scripts/verify_demo_no_node.py
```

builds the wheel, installs it with the locked runtime dependencies into a fresh
virtual environment outside the checkout, and proves that the installed
`paritygrid demo --headless --runner sequential --json` command completes its full
canonical proof with a `PATH` that cannot resolve `node`, `npm`, `npx`, or `vite`
and with no external service. It also verifies that the packaged frontend ships
inside the wheel. A live installed serve process must announce a `127.0.0.1`
address, return healthy state and the packaged index, stop answering after bounded
termination, and permit safe reset of its exact owned root. The wheel's SHA-256 is
printed with the probe results; the command exits 0 on success and 1 with a precise
bounded error otherwise.

## Related documents

- [PRODUCT.md](PRODUCT.md) — product story and audience.
- [ARCHITECTURE.md](ARCHITECTURE.md) — system structure and composition root.
- [EXECUTION_MODEL.md](EXECUTION_MODEL.md) — durable execution, attempts, and recovery.
- [QUALITY_STRATEGY.md](QUALITY_STRATEGY.md) — where the demo proof sits in the quality gates.
- [SECURITY.md](SECURITY.md) — local-first binding and data ownership boundaries.
- [`src/paritygrid/cli.py`](../src/paritygrid/cli.py) — the command definitions.

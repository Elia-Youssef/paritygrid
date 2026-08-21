# Phase status

This document is the durable index of accepted phases, their integration commits, public evidence, known limitations, and the active delivery boundary. It reports repository state; it does not override the governing requirements in the [documentation index](INDEX.md), the acceptance rubric in the [delivery plan](DELIVERY_PLAN.md), or the workflow in the [Git workflow](GIT_WORKFLOW.md).

## Accepted phases

| Phase | Status | Accepted integration commit | Public evidence | Delivered scope | Known limitation at acceptance |
|---|---|---|---|---|---|
| 0 — Repository and product charter | Accepted | [`9eab12b`](https://github.com/Elia-Youssef/paritygrid/commit/9eab12bc3dce06211dfeb81bd692c3f2aca3c03f) | [Baseline instruction audit](INSTRUCTION_AUDIT.md) | Product charter, architecture, delivery plan, repository policy, and documentation baseline | The initial history predates a dedicated phase pull request and this status index. |
| 1 — Toolchain and modular skeleton | Accepted | [`617e051`](https://github.com/Elia-Youssef/paritygrid/commit/617e0510d72631d589385aec7067c0e0d22284d3) | [Phase 1 command set](CI_RELEASE.md#phase-1-command-set) | Locked Python and frontend toolchains, package skeleton, CI, API and UI shells, and smoke path | Later phases retain the underlying commands while the planned top-level verification profiles remain incomplete. |
| 2 — Pure domain foundation | Accepted | [`9646164`](https://github.com/Elia-Youssef/paritygrid/commit/9646164d2312acb387e6e8b873c64b0b454fa04e) | [Phase pull request #5](https://github.com/Elia-Youssef/paritygrid/pull/5) | Trusted values, lifecycle states, reconciliation and repair rules, canonical encoding, fingerprints, typed failures, and domain import purity | Infrastructure integration was deliberately deferred. |
| 3 — SQLite persistence and migrations | Accepted | [`8d63ee1`](https://github.com/Elia-Youssef/paritygrid/commit/8d63ee19e495df9e4b3be62b379737022edb6cce) | [Phase pull request #20](https://github.com/Elia-Youssef/paritygrid/pull/20) | Authoritative SQLite schema, migration, repositories, transactional writer, WAL stress, and crash-reopen harness | Higher-level execution lifecycle coordination was deferred to Phase 6. |
| 4 — Artifact, Parquet, and DuckDB foundation | Accepted | [`4338409`](https://github.com/Elia-Youssef/paritygrid/commit/4338409d882bf3af52caad69a4572f80092d0e9a) | [Phase pull request #35](https://github.com/Elia-Youssef/paritygrid/pull/35) | Safe artifact paths, atomic manifests, versioned Parquet schemas, DuckDB views, analytical queries, and integrity scanning | Product reconciliation workflows and final-state verification were deferred. |
| 5 — Pipeline specification and planner | Accepted | [`dfd89ed`](https://github.com/Elia-Youssef/paritygrid/commit/dfd89ed6714300d831273df6bbd43e175fc4e693) | [Phase pull request #48](https://github.com/Elia-Youssef/paritygrid/pull/48) | Versioned pipeline documents, closed node registry, graph and resource validation, repair-path safety, immutable publication, deterministic plan compilation, partitioning, and plan fingerprints | The planner validates approval-before-effect graph structure; it does not apply repairs. |

## Active boundary

Pre-Phase-7 remediation is active. Phase 6 is implemented but remains an acceptance candidate; Phase 7 concurrent runner implementation has not begun.

- Current accepted `main`: [`ff0ec58`](https://github.com/Elia-Youssef/paritygrid/commit/ff0ec5894f37f867586c673a57ba752b58288525).
- Phase 6 candidate head: [`2431283`](https://github.com/Elia-Youssef/paritygrid/commit/2431283f0c842675f28821d54d475db831a11a8c).
- Phase pull request: [#62](https://github.com/Elia-Youssef/paritygrid/pull/62), currently draft.
- Candidate relationship: `ff0ec58` is the merge base and ancestor of `2431283`; the candidate is 49 commits ahead with no missing `main` commit at the recorded review point.
- Pull-request acceptance record at inspection: the body still identified `7ea9fd1` as its final integration head and no submitted review was recorded. It must be refreshed only after an authorized remediation commit exists.

## Phase 6 acceptance candidate

Phase 6 provides the reference sequential scheduler and runner, work leasing, normalized attempt events, retry classification, result submission, checkpoint commit coordination, pause and resume, cancellation, run finalization, startup recovery, and a deterministic synthetic Phase 2–6 integration fixture. The fixture does not contain production connectors or the Phase 9 reconciliation workflow.

### Exact-head evidence

The pull-request workflow for candidate `2431283` completed successfully on 2026-08-20 in [run 32350130390](https://github.com/Elia-Youssef/paritygrid/actions/runs/32350130390):

| Gate | Result |
|---|---|
| Repository policy | Passed; instruction validation covered 25 Markdown files and the whitespace check passed. |
| Python on Ubuntu | 3,541 tests passed; 99.54% statement/branch coverage; Ruff, Pyright, import boundaries, and smoke passed. |
| Python on Windows | 3,541 tests passed; 99.55% statement/branch coverage; Ruff, Pyright, import boundaries, and smoke passed. |
| Frontend | 33 tests passed; formatting, linting, type checking, coverage, production build, and high-severity dependency audit passed with zero reported vulnerabilities. |
| Frontend/API smoke | Passed through the Vite development proxy to the FastAPI application factory. |

The exact candidate relationship and clean worktree were also inspected locally before this remediation. A complete local acceptance matrix has not been rerun on a commit containing the documentation and architecture remediation; that rerun remains required before acceptance.

### Remediation worktree evidence

On 2026-08-20, the complete available local matrix passed on the uncommitted remediation worktree layered on `2431283`:

| Gate | Local result |
|---|---|
| Locked Python environment | `uv lock --check` and `uv sync --locked --all-groups` passed for 65 packages. |
| Repository policy | Instruction validation passed for 28 Markdown files, 6 ADRs, 6 generated files, and an explicit empty media inventory; `git diff --check` passed. |
| Python static checks | Ruff formatting covered 320 files; Ruff lint, strict Pyright, and domain/process-worker import boundaries passed. |
| Python tests and coverage | 3,556 tests passed and 4 filesystem-link tests skipped on Windows; aggregate branch-enabled coverage was 99.54%. Application-execution branch coverage was 99.87%, and the sequential runner was 100%. |
| Python dependency audit | The strict hash-pinned all-groups lock export reported no known vulnerabilities. |
| Package isolation | A fresh Python 3.14.4 environment installed locked runtime dependencies and the built wheel, then passed an isolated Unicode-working-directory import, installed CLI smoke, and multiprocessing-spawn child import outside the checkout. |
| Generated fixtures and backend smoke | Frozen schema hashes and the sequential expected manifest reproduced exactly; `paritygrid smoke` passed. |
| Frontend | 33 tests passed; statements, lines, and functions were 100% and branches were 92.3%; formatting, linting, type checking, production build, and the dependency audit passed with zero reported vulnerabilities. |
| Frontend/API smoke | The Vite development proxy reached the FastAPI application factory and returned the expected service identity. |

This evidence validates the proposed files but is not exact-head evidence: the remediation remains uncommitted by operating rule. The same matrix must pass on the authorized commit in Windows and Ubuntu CI, and the pull-request acceptance record and independent review must identify that exact commit before Phase 6 acceptance.

### Rubric

| Criterion | Score | Evidence and deduction |
|---|---:|---|
| Reference-runner correctness | 24/25 | The scheduler, sequential runner, deterministic ordering, result boundary, and integration fixture are covered on both required operating systems. One point remains reserved for the shared runner-neutral contract work that precedes Phase 7 strategies. |
| Lifecycle semantics | 24/25 | Pause/resume, cancellation, terminal replay, cleanup, and exact durable transitions are covered. One point is deducted for the carried `abort_pause`/runner race normalization. |
| Recovery and idempotency | 24/25 | Crash-reopen, checkpoint, artifact integrity, replay, expired-lease recovery, and fail-closed ambiguity paths are covered. One point is deducted for carried lease-event and pre-admission result-boundary hardening. |
| Automated verification | 19/20 | The complete locked candidate CI matrix passes on Windows and Ubuntu with more than 99.5% aggregate coverage, and the remediation worktree passes the new local wheel-isolation, scoped-coverage, and dependency-audit gates. One point is reserved until those gates pass in both operating-system jobs on the exact committed remediation head. |
| Diagnostics | 4/5 | Typed failures, redaction, durable event evidence, and bounded representations are covered. Concurrency telemetry schemas and stress diagnostics belong to Phase 7. |
| **Total** | **95/100** | Meets the numerical threshold, subject to the remaining governance and exact-final-head gates below. |

### Hard gates

| Phase 6 hard gate | Status | Evidence |
|---|---|---|
| Duplicate committed effect | Pass on candidate | Controlled interruption, recovery, and replay assertions in the [sequential integration suite](../tests/execution/test_sequential_end_to_end.py). |
| Missing committed checkpoint | Pass on candidate | Transactional receipt and failpoint coverage in the [checkpoint suite](../tests/execution/test_checkpoint_commit.py) and finalization checks. |
| Deadlock | Pass on candidate | Closed scheduler-state and transition coverage in the [scheduler contract](../tests/execution/test_scheduler_contract.py) and [tracker tests](../tests/execution/test_scheduler_tracker.py). |
| Ambiguous recovery accepted | Pass on candidate | Fail-closed classification and persistence evidence in the [recovery contract](../tests/execution/test_recovery_contract.py), [evidence tests](../tests/execution/test_recovery_evidence.py), and [integration suite](../tests/persistence/test_recovery_integration.py). |

No Phase 6 technical hard gate is open on `2431283`. Formal acceptance remains blocked until the remediation is committed, the final exact-head command matrix and CI pass, the independent acceptance review is recorded for that same head, and the phase pull-request record is refreshed with those results.

### Independent review status

An independent read-only governance review of `2431283` was completed on 2026-08-20. It confirmed the exact candidate and CI evidence and identified the documentation, decision-record, ownership, repository-content, and acceptance-record corrections assigned to this remediation. Because these corrections change the acceptance candidate, the final independent acceptance review must be repeated and recorded against the resulting exact head before Phase 6 is marked accepted.

A separate independent read-only acceptance review of all 30 final remediation files completed on 2026-08-20 with no remaining P0–P2 finding. It verified the fingerprint inputs against the Phase 6 implementation, the runner-neutral architecture, the acyclic P7.0–P7.19 graph, and the new repository, import, dependency, coverage, wheel, and spawn gates. This is review evidence for the uncommitted worktree, not a substitute for recording the same review against the authorized exact commit and pull request.

### Known limitations and assigned follow-up

- Phase 7 P7.0: bound and validate lease-event correlation identifiers.
- Phase 7 P7.0: prevent invalid-result lease poisoning before writer admission.
- Phase 7 P7.0: normalize the `abort_pause`/runner race and its failure type.
- Phase 7 comparison and stress work: prove concurrent pause, recovery, backpressure, cleanup, and execution-evidence equivalence across every required runner.
- Phase 9: compute and persist the reconciliation/final-state fingerprint; the Phase 6 fingerprint covers execution evidence only.
- Phase 10: decide the durable handling of stranded in-progress idempotency reservations.
- Threaded, asynchronous, process, and optional interpreter runners are not implemented.

## Advancement rule

Phase 7 may formally begin only after Phase 6 is accepted on one exact commit with all required checks passing, no hard-gate failure, an independent acceptance review, a current pull-request record, and a clean worktree. Planning and contract decisions may be prepared during this remediation, but concurrent runner implementation must wait for that boundary.

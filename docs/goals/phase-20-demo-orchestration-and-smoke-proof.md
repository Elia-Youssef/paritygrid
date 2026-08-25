# Phase 20 — Demo orchestration and smoke proof

Implement Phase 20 completely and only Phase 20. The goal is an isolated, recoverable, packaged demonstration with controlled failures and repeatable headless and browser proof.

Before editing, read `docs/INDEX.md`, `docs/PRODUCT.md`, `docs/ARCHITECTURE.md`, `docs/EXECUTION_MODEL.md`, `docs/API_CONTRACT.md`, `docs/UI_SPECIFICATION.md`, `docs/SECURITY.md`, `docs/QUALITY_STRATEGY.md`, `docs/CI_RELEASE.md`, `docs/DELIVERY_PLAN.md`, and `docs/WORK_PACKAGES.md`. Confirm Phase 19 and all interface dependencies are accepted. Preserve unrelated changes.

Implement these packages:

- P20.1 demo CLI orchestration.
- P20.2 controlled fault controls.
- P20.3 controlled process interruption.
- P20.4 headless smoke command.
- P20.5 required runner smoke profiles.
- P20.6 canonical browser scenario.
- P20.7 demo data isolation and reset.
- P20.8 packaged no-Node runtime check.

Required behavior:

- Start the packaged application, migrations, Phase 8 simulators, isolated data roots, dynamic ports, health checks, and browser URL through one bounded CLI lifecycle.
- Expose only explicit deterministic fault controls tied to the Phase 19 scenario; make activation, expected consequence, and reset observable.
- Prove forced process interruption and restart resume from durable evidence without duplicate effects or accepting partial state.
- Provide headless sequential, threaded, and asyncio smoke profiles that verify startup, simulator health, pipeline publication, run completion, artifacts, reconciliation, repair, target verification, expected fingerprints, and clean shutdown.
- Implement the canonical browser workflow through verified parity and runner comparison using stable selectors and deterministic data.
- Confine creation and deletion to an explicitly resolved demo root. Reject empty, broad, root, workspace, or unsafe reset targets.
- Prove the installed production package serves its frontend and runs the normal demo without Node.js or an external database service.
- Close all owned processes, threads, tasks, clients, files, queues, and ports after success, failure, cancellation, and interruption.

Verification and evidence:

- Add CLI lifecycle, readiness, partial-startup, shutdown, fault, interruption/resume, isolation, unsafe-reset, runner-smoke, browser, and isolated-install tests.
- Run every available Phase 20 smoke command and the full repository quality and instruction/content checks.
- Report exact commands, results, timings as non-acceptance diagnostics, installed artifact identity, and any unavailable platform check.

Completion handoff:

- Report package IDs, commands delivered, changed files, recovery and browser evidence, isolation guarantees, and limitations.
- Leave all work uncommitted and do not begin Phase 21.

## Absolute Git prohibition

Do not stage files, commit, push, create or update a pull request, create a tag, publish a release, merge, rebase, cherry-pick, reset, switch branches, or otherwise mutate Git state. Read-only inspection is allowed. Finish with all Phase 20 changes uncommitted and unpushed. This prohibition is absolute.

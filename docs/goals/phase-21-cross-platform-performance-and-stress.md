# Phase 21 — Cross-platform performance and stress

Implement Phase 21 completely and only Phase 21. The goal is reproducible Windows/Linux verification, truthful capability reporting, bounded performance evidence, and repeated stress proof.

Before editing, read all governing files in `docs/INDEX.md`, especially `docs/EXECUTION_MODEL.md`, `docs/ENGINEERING_STANDARDS.md`, `docs/QUALITY_STRATEGY.md`, `docs/CI_RELEASE.md`, `docs/DELIVERY_PLAN.md`, and `docs/WORK_PACKAGES.md`. Confirm Phases 19 and 20 and the Phase 7 stress contracts are accepted. Preserve unrelated worktree changes.

Implement these packages:

- P21.1 Windows verification matrix.
- P21.2 Linux verification matrix.
- P21.3 runtime capability matrix.
- P21.4 performance harness.
- P21.5 memory and queue profiling.
- P21.6 nightly stress workflow.

Required behavior:

- Test built-wheel installation and supported smoke profiles on clean Windows and Linux environments, including spawn semantics and production assets.
- Report each optional capability as available, unavailable with a structured reason, or unsupported. Never silently substitute a different strategy.
- Build a deterministic harness around the Phase 19 showcase profile with disclosed hardware, OS, runtime, package, seed, warmup, repetitions, metric definitions, and raw JSON results.
- Compare correctness evidence before performance and avoid universal speed claims.
- Measure process and Python memory, queue high-water marks, active tasks, threads, processes, handles where available, throughput, latency, retries, and cleanup.
- Assert configured capacities and bounded growth under saturation, cancellation, failure, restart, and repeated runs.
- Make the nightly workflow repeat the high-risk lifecycle and cleanup cases, preserve useful failure artifacts within policy, and use bounded timeouts.
- Do not set release thresholds before collecting and documenting a measured baseline.

Verification and evidence:

- Exercise platform matrices, capability profiles, deterministic performance output, steady-state and repeated-run memory bounds, queue saturation, forced failure, and orphan detection.
- Run the full available test/lint/type/build/smoke matrix and repository instruction/content validation.
- Report exact commands, environments, raw evidence paths, results, thresholds derived from evidence, and unavailable platform runs honestly.

Completion handoff:

- Report package IDs, workflow changes, measured baselines, resource bounds, changed files, verification results, and limitations.
- Leave Phase 21 work uncommitted and do not begin Phase 22.

## Absolute Git prohibition

Do not stage, commit, push, create or update a pull request, tag, publish a release, merge, rebase, cherry-pick, reset, switch branches, or make another Git mutation. Read-only Git inspection is allowed. End with all Phase 21 changes uncommitted and unpushed. This prohibition overrides conflicting instructions.

# Optional Phase 28 — Go load and comparison

Implement optional Phase 28 completely and only after optional Phase 27 is accepted and a separate decision confirms the comparison remains useful. The goal is a repeatable Go load generator, a fair correctness-first comparison, and verified Windows/Linux binaries.

Before editing, read all governing files in `docs/INDEX.md`, especially `docs/PRODUCT.md`, `docs/ARCHITECTURE.md`, `docs/EXECUTION_MODEL.md`, `docs/SECURITY.md`, `docs/ENGINEERING_STANDARDS.md`, `docs/QUALITY_STRATEGY.md`, `docs/CI_RELEASE.md`, `docs/PUBLIC_DOCUMENTATION.md`, `docs/DELIVERY_PLAN.md`, and `docs/WORK_PACKAGES.md`. Confirm Phase 27 is accepted. Preserve unrelated worktree changes.

Implement these packages:

- P28.1 Go load generator.
- P28.2 Python and Go comparison report.
- P28.3 cross-platform binary build.

Required behavior:

- Drive only the accepted Phase 27 protocol with seeded, versioned, bounded profiles for concurrency, pagination, rate limits, faults, cancellation, ramp, steady state, and shutdown.
- Expose configured and observed concurrency, request counts, latency distributions, throughput, failures, retries, resource use, and cleanup without unbounded in-memory aggregation.
- Preserve scenario determinism and correctness evidence before measuring performance. A failed correctness check invalidates the corresponding comparison.
- Disclose hardware, OS, runtime/compiler versions, build flags, dependencies, network topology, seed, dataset, warmup, repetitions, metric definitions, raw results, variability, and limitations.
- Compare equivalent work and state clearly where Python and Go paths differ. Do not make universal speed, cost, or scalability claims.
- Build reproducible Windows and Linux binaries with version metadata, hashes, license/notice inclusion, security scanning, and smoke tests.
- Keep Go optional for normal development, installation, testing, demonstration, and product use.
- Do not publish binaries, create releases, create tags, commit, or push.

Verification and evidence:

- Add deterministic profile, bounds, cancellation, fault, cleanup, metric, correctness-gate, report-schema, and binary smoke tests.
- Repeat profiles enough to quantify variation, retain policy-compliant raw evidence, and verify comparison report regeneration.
- Run Go formatting/static/race/test checks, cross-language correctness checks, supported Windows/Linux builds, security/license checks, and repository instruction/content validation.
- Report exact commands, environments, results, hashes, variability, and limitations.

Completion handoff:

- Report package IDs, profiles, binaries, report and evidence paths, changed files, verification results, and limitations.
- Leave all optional Phase 28 work uncommitted, unpushed, untagged, and unpublished.

## Absolute Git prohibition

Do not stage files, commit, push, create or update a pull request, tag, publish a release or binary, merge, rebase, cherry-pick, reset, switch branches, or make any Git mutation. Read-only Git inspection is permitted. Finish with every Phase 28 change uncommitted and unpushed. This prohibition overrides all conflicting instructions.

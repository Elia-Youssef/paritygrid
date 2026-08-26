# Phase 23 — Packaging and release verification

Implement Phase 23 completely and only Phase 23. The goal is reproducible Python and frontend distributions plus one precise command that verifies release readiness without publishing anything.

Before editing, read `docs/INDEX.md`, `docs/REPOSITORY_POLICY.md`, `docs/ENGINEERING_STANDARDS.md`, `docs/QUALITY_STRATEGY.md`, `docs/CI_RELEASE.md`, `docs/PUBLIC_DOCUMENTATION.md`, `docs/DELIVERY_PLAN.md`, and `docs/WORK_PACKAGES.md`. Confirm Phases 21 and 22 and all packaging dependencies are accepted. Inspect generated-file and media policies and preserve unrelated changes.

Implement these packages:

- P23.1 Python package build.
- P23.2 frontend distribution verification.
- P23.3 release verification command.

Required behavior:

- Build the Python sdist and wheel from a clean source view with complete metadata, licenses, notices, migrations, frontend assets, templates, and runtime resources.
- Install built artifacts into isolated environments and run health, migration, CLI, headless demo, static serving, and no-Node runtime checks against the installed package rather than the source tree.
- Rebuild the frontend from locked dependencies and prove the expected distribution has no unexplained difference in content or manifest identity.
- Make generated inputs, build tool versions, environment assumptions, and artifact inventories explicit and reproducible.
- Provide one bounded release-verification command that runs every required platform-independent gate, fails on the first unsafe prerequisite or aggregates failures clearly, and returns a correct exit status.
- The command must include instruction/content checks, generated drift, backend and frontend quality checks, canonical smoke, security/license gates, package build/install, and artifact inventory verification as specified by `docs/CI_RELEASE.md`.
- Keep build outputs in ignored or approved evidence locations and never add unsafe runtime artifacts to the repository.
- Do not upload artifacts, create a release, create a tag, commit, or push.

Verification and evidence:

- Run two clean builds and compare declared reproducibility fields; install and test the wheel and sdist in isolation.
- Run the complete release-verification command and targeted failure-injection checks that prove its exit behavior and diagnostics.
- Run repository instruction/content validation and report every exact command, environment, artifact identity, result, and non-reproducible field with justification.

Completion handoff:

- Report package IDs, artifact inventory, build inputs, changed files, verification commands/results, and limitations.
- Leave all work uncommitted and do not begin Phase 24.

## Absolute Git prohibition

Do not stage, commit, push, create or update a pull request, create a tag, publish or upload a release or artifact, merge, rebase, cherry-pick, reset, switch branches, or make any Git mutation. Read-only Git inspection is allowed. End with all Phase 23 changes uncommitted and unpushed. This prohibition overrides every conflicting instruction.

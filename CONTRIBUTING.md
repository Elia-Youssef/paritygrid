# Contributing to ParityGrid

Thank you for improving ParityGrid. Changes are accepted when they preserve deterministic behavior, local-first operation, clear package boundaries, and reproducible verification.

## Before starting

Read the [documentation index](docs/INDEX.md), then the governing product, architecture, engineering, repository, and quality documents for the area you plan to change. Check the [phase status](docs/PHASE_STATUS.md), select one or more work packages from the [work package catalog](docs/WORK_PACKAGES.md), and confirm their dependencies are accepted.

Use the [Git workflow](docs/GIT_WORKFLOW.md). Topic branches target the current phase branch; phase branches target `main` only after their acceptance gate passes.

## Development expectations

- Keep domain code independent of web frameworks, databases, filesystems, and process orchestration.
- Keep I/O explicit, bounded, cancellable, and covered by failure-path tests.
- Add or update tests in the same change as behavior.
- Use synthetic data and fictional identifiers only.
- Do not commit credentials, local databases, generated run artifacts, coverage output, or confidential material.
- Write comments and documentation for maintainers and users, focusing on contracts, invariants, and non-obvious tradeoffs.
- Update public contracts and architecture documents when behavior changes deliberately.

## Local verification

Phase 1 establishes the stable commands used by continuous integration. Until a package adds its final command, run every available check for the files you changed and record the exact commands in the pull request.

At minimum, a proposed change must demonstrate:

- locked dependency installation;
- formatting and lint checks;
- strict type checks;
- focused tests for the changed behavior;
- the relevant smoke or production build path;
- repository-content validation.

## Pull requests

Open a draft pull request early when contract review would reduce rework. Before requesting review:

1. Rebase or merge the latest target branch as the repository setting requires.
2. Complete the pull-request template.
3. Confirm the full relevant check set passes on the current commit.
4. Review the changed-file list for accidental output or unrelated edits.
5. State limitations honestly and assign deferred work to a later package.

A pull request is not complete while a required check is failing, a review conversation is unresolved, a hard gate is open, or its verification evidence refers to another commit.

## Reporting security issues

Do not open a public issue for a suspected vulnerability. Use GitHub private vulnerability reporting after it is enabled for the repository. Until then, contact the repository owner privately without including live secrets or production data.

# Git workflow

## Goals

The repository uses short-lived topic branches, required pull-request checks, and explicit phase acceptance. The workflow keeps `main` releasable while allowing independent packages to progress without sharing an unreviewed working tree.

## Protected branches

`main` is the protected integration history for accepted phases. Direct pushes are disabled. A change reaches `main` only through a pull request that:

- is current with its target branch;
- passes all required checks;
- has no unresolved review conversation;
- has approval from a maintainer who did not author the change when another maintainer is available;
- receives a fresh approval when the reviewed commit changes and approval is required;
- uses a merge commit so the accepted phase remains visible as one historical unit.

History rewriting and force pushes are disabled on protected branches. Branch deletion is enabled after a successful merge.

### Solo-maintainer operation

GitHub does not allow a pull-request author to approve their own change. While the repository has one maintainer, rulesets therefore require zero approvals but still require the pull request, current-commit status checks, conversation resolution, and the accepted merge method. Independent acceptance review is recorded in the pull-request evidence before a phase merges. Once a second maintainer participates, rulesets require at least one approval and dismiss it after a new reviewable push.

## Branch hierarchy

Each phase starts from the accepted `main` commit and receives one integration branch:

```text
main
  phase/NN-short-name
    feat/pN-topic
    fix/pN-topic
    test/pN-topic
    docs/pN-topic
```

Topic branches target the current phase branch. Their names identify the change type and phase package. Examples include `feat/p1-backend-foundation` and `test/p6-recovery-failpoints`.

The phase branch may merge into `main` only after all phase packages are accepted, the phase rubric scores at least 90, and every hard gate passes. Urgent fixes start from `main` as `fix/short-name`, receive the same checks, and are merged back into any active phase branch.

### Implementation-goal boundary

The standalone Phase 8–28 implementation prompts operate only inside a branch and working tree prepared beforehand. They must not stage, commit, push, switch branches, create tags, open pull requests, merge, or publish releases. After the implementation is audited and accepted, a separately authorized integration task performs the applicable steps below. Keeping integration outside the implementation goal does not waive any branch, review, exact-commit, or required-check rule.

## Pull-request sequence

1. Create a topic branch from the current phase branch.
2. Keep the change limited to its declared work packages.
3. Add tests and public contract updates in the same change.
4. Run the relevant local verification commands.
5. Push the branch and open a draft pull request against the phase branch.
6. Mark it ready only when its checklist and handoff evidence are complete.
7. Resolve review findings and obtain a green required-check set.
8. Merge with a merge commit and delete the topic branch.
9. Re-run the complete phase lane after all topic changes are integrated.
10. Open the phase pull request against `main` with rubric evidence.

Draft pull requests may run reduced checks while work is changing rapidly. The complete required lane must pass on the final commit before merge.

## Commit policy

Commits use a concise conventional form:

```text
type(optional-scope): imperative summary
```

Accepted types are `feat`, `fix`, `test`, `docs`, `refactor`, `perf`, `build`, `ci`, and `chore`. A commit should represent one reviewable reason for change. Formatting-only churn and unrelated refactors do not share a functional commit.

Merge commits use an explicit summary, such as `merge: complete Phase 1 toolchain skeleton`. Public history must not contain secrets, confidential material, temporary coordination notes, or local runtime output.

## Required checks

The phase pull request requires these stable check groups when their implementation exists:

- repository policy and documentation validation;
- Python formatting, linting, strict typing, and import boundaries;
- Python unit, API, integration, and smoke tests;
- frontend formatting, linting, strict typing, component tests, and production build;
- generated-contract and packaged-asset drift checks;
- dependency and credential scanning appropriate to the phase.

Checks use locked dependencies and clean, non-interactive commands. A skipped required check is a failure unless the phase explicitly documents why the capability is not yet present.

## Phase acceptance record

The phase pull request description records:

- included work package IDs;
- source and public contract changes;
- exact verification commands and results;
- rubric score with point-by-point evidence;
- hard-gate status;
- known limitations assigned to later work packages.

Acceptance evidence describes the exact commit being merged. Evidence from an older commit must be rerun.

## Repository settings

When the GitHub repository is created, enable repository rulesets for `main` and active `phase/**` branches. Rulesets are preferred because their active constraints are visible to contributors and multiple rulesets combine without silently replacing one another. Configure them to provide:

- pull requests and approvals when more than one maintainer is available;
- dismissal of stale approvals after new commits;
- required conversation resolution;
- required status checks from the committed workflows;
- a requirement that the proposed commit is current with its target;
- blocked force pushes and branch deletion;
- automatic deletion of merged branches;
- secret scanning, dependency alerts, and private vulnerability reporting;
- merge commits, with squash and rebase merging disabled for phase acceptance.

Enable a merge queue if simultaneous contribution volume makes repeated target-branch updates expensive. The queue must rerun the required checks for its merge group before it lands.

Rulesets are reviewed when workflow job names change so a renamed check cannot silently remove a merge gate.

# Phase 26 — Release-candidate handoff

Implement Phase 26 completely and only Phase 26. The goal is a reviewable release-candidate evidence bundle and handoff with reproducible hashes, without performing any Git, hosting, or publication action.

Before editing, read every governing file in `docs/INDEX.md`, especially `docs/REPOSITORY_POLICY.md`, `docs/GIT_WORKFLOW.md`, `docs/QUALITY_STRATEGY.md`, `docs/CI_RELEASE.md`, `docs/PUBLIC_DOCUMENTATION.md`, `docs/INSTRUCTION_AUDIT.md`, `docs/DELIVERY_PLAN.md`, and `docs/WORK_PACKAGES.md`. Confirm Phases 23 and 25 are accepted. Inspect the complete worktree and preserve unrelated changes.

Implement these packages:

- P26.1 release-candidate artifact hash manifest.
- P26.2 release-candidate handoff checklist.

Required behavior:

- Rebuild every intended release artifact through the accepted Phase 23 process from the exact candidate source state.
- Create a deterministic manifest containing artifact names, sizes, cryptographic hashes, package and scenario versions, build inputs, tool versions, supported platforms, and verification evidence references.
- Exclude timestamps, paths, machine identifiers, or other volatile fields unless normalized or explicitly documented outside the reproducibility identity.
- Rebuild independently and require every identity field and artifact hash to match.
- Complete a handoff checklist that records accepted phases, hard gates, exact verification commands/results, dependency and license status, notices, media inventory, confidentiality audit, expected canonical facts, limitations, and rollback/recovery notes.
- State the proposed manual integration sequence as a recommendation only. Do not execute it and do not imply that a commit, push, tag, release, or publication already exists.
- Do not modify implementation behavior except for a narrowly scoped reproducibility or documentation defect discovered during verification; report any larger defect as a blocker for its owning phase.

Verification and evidence:

- Run two candidate builds, compare the manifest and hashes, run the accepted aggregate release-verification command, and run instruction/content validation.
- Verify the Git index has not been changed by this work and report the working-tree state using read-only commands.
- Report exact commands, results, artifact hashes, checklist status, blockers, and limitations.

Completion handoff:

- Report package IDs, manifest and checklist paths, verification evidence, unresolved blockers, and the proposed next manual integration action.
- Stop with all Phase 26 changes visible, uncommitted, unpushed, untagged, and unpublished.

## Absolute Git prohibition

Do not run `git add`, `git commit`, `git push`, `git tag`, `git merge`, `git rebase`, `git cherry-pick`, `git reset`, `git checkout`, `git switch`, or any command that mutates Git state. Do not create or update a pull request, release, hosted artifact, or publication. Read-only `git status`, `git diff`, and `git log` are allowed. No other instruction can override this prohibition.

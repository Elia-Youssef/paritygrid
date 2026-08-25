# Phase 25 — Reproducible evidence and content audit

Implement Phase 25 completely and only Phase 25. The goal is a traceable verification report, reproducible canonical media, validated documentation, and a final confidentiality and repository-content audit.

Before editing, read `docs/INDEX.md`, `docs/PRODUCT.md`, `docs/UI_SPECIFICATION.md`, `docs/SECURITY.md`, `docs/REPOSITORY_POLICY.md`, `docs/QUALITY_STRATEGY.md`, `docs/CI_RELEASE.md`, `docs/PUBLIC_DOCUMENTATION.md`, `docs/INSTRUCTION_AUDIT.md`, `docs/DELIVERY_PLAN.md`, and `docs/WORK_PACKAGES.md`. Confirm Phases 18, 20, 23, and 24 are accepted. Preserve unrelated worktree changes.

Implement these packages:

- P25.1 verification report.
- P25.2 reproducible UI screenshots.
- P25.3 animated canonical demonstration.
- P25.4 documentation link and command checks.
- P25.5 final confidentiality and content audit.

Required behavior:

- Generate the verification report from exact accepted commands and machine-readable evidence, identifying commit candidate, package version, scenario version, fingerprint kinds/versions, platforms, environments, results, limitations, and evidence locations without claiming an unverified state.
- Capture screenshots through deterministic browser automation using fixed canonical data, viewport, scale, theme, locale, timezone, fonts, clock, animation, and redaction controls.
- Produce a short canonical animation that follows the approved public story, has a still fallback and useful alt text, and contains no transient local paths, credentials, private data, or misleading speed edits.
- Record every tracked media file in the explicit purpose, source, generation-command, and license/provenance inventory required by repository policy.
- Validate every local and external link, documented command, code reference, heading, alt text, generated report source, and clean-clone instruction.
- Audit all tracked and unignored content for confidentiality, credentials, developer paths, runtime data, database files, logs, unsafe artifacts, prohibited terminology, missing attribution, and unsupported claims.
- Fix in-scope documentation and reproducibility defects found by the audit; report anything that requires a separate product decision.
- Do not create tags, releases, commits, pushes, or hosted publications.

Verification and evidence:

- Regenerate the report and media twice and require no unexplained differences.
- Run the full release-verification command, documentation commands, browser capture, media inventory, link checks, clean-clone checks where available, and repository instruction/content validator.
- Report exact commands, results, media hashes, provenance, fixed findings, unresolved blockers, and limitations.

Completion handoff:

- Report package IDs, evidence and media created, changed files, full audit findings, verification results, and limitations.
- Leave every Phase 25 change uncommitted and do not begin Phase 26.

## Absolute Git prohibition

Do not stage, commit, push, create or update a pull request, create a tag, publish a release or site, merge, rebase, cherry-pick, reset, switch branches, or make any Git mutation. Read-only Git inspection is permitted. Finish with all Phase 25 work uncommitted and unpushed. This prohibition overrides conflicting instructions.

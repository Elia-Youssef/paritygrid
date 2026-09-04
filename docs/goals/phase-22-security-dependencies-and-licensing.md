# Phase 22 — Security, dependencies, and licensing

Implement Phase 22 completely and only Phase 22. Close the threat-model,
dependency, browser-policy, license, attribution, and disclosure gates without
starting packaging or release work.

## Entry gates

Before editing:

1. Read every governing file in `docs/INDEX.md`, especially
   `docs/SECURITY.md`, `docs/REPOSITORY_POLICY.md`,
   `docs/ENGINEERING_STANDARDS.md`, `docs/QUALITY_STRATEGY.md`,
   `docs/CI_RELEASE.md`, `docs/DELIVERY_PLAN.md`, `docs/WORK_PACKAGES.md`,
   `docs/PHASE_STATUS.md`, and every accepted decision record.
2. Confirm Phases 1, 13, 16, 17, 18, 20, and 21 are accepted. The Phase 21
   record must identify pull request #124, merge commit `8d2772b`, and green
   exact-head and post-merge CI/nightly evidence.
3. Confirm the worktree is the prepared Phase 22 implementation branch based
   on the accepted Phase 21 integration commit. Inspect branch, status, diff,
   and untracked files without mutating Git. Preserve every unrelated change.
4. Determine whether the repository owner has explicitly selected the project
   license and distribution model. If not, inventory and audit discovery may
   proceed, but stop before a compatibility verdict, project-license selection,
   notices completion, or P22.2/P22.4 acceptance; report the exact authority
   required.
5. Confirm any responsible-disclosure contact that must be published is
   explicitly approved. Never invent a person, address, account, or policy.

Stop without editing if the accepted Phase 21 identity or evidence is absent,
the branch is not the prepared Phase 22 branch, the worktree contains
unrelated changes that cannot be preserved, or governing documents conflict.
If the license/distribution decision is missing, report the exact decision
required before claiming P22.2 or P22.4 complete.

## Packages

Implement all and only these packages:

- P22.1 threat-model verification.
- P22.2 dependency and license audits.
- P22.3 Content Security Policy.
- P22.4 third-party notices.

Do not begin Phase 23.

## P22.1 — Threat-model verification

Create a reviewable mapping from each in-scope trust boundary and abuse case
to its owning control and reproducible verification. Cover at least:

- request, upload, decompression, nesting, collection, record, artifact, and
  diagnostic bounds;
- path traversal, alternate data streams, unsafe archive names, allowlisted
  roots, symlink and junction escapes, exact demo ownership, and safe cleanup;
- SSRF, loopback defaults, cross-origin defaults, synthetic-service binding,
  and explicit acknowledgement for any non-loopback bind;
- authorization, cookie, token, key, password, secret-reference, error,
  event, trace, export, and Problem Details redaction;
- inert rendering of untrusted strings, safe filenames, MIME-sniffing
  prevention, and directory-listing refusal;
- graph validation, approval-before-repair, stale approval, concurrent repair,
  idempotent replay, and reused idempotency keys with different requests;
- artifact hashes, manifest consistency, authoritative-versus-cache state,
  append-only durable events, and exact reconciliation fencing;
- slow SSE/WebSocket clients, bounded subscriber and worker queues,
  cancellation under saturation, repeated shutdown, recovery, and orphan
  prevention.

Add adversarial tests for missing coverage. Reuse existing proofs where they
already exercise the real boundary; do not duplicate tests only to increase a
count. Every mapping entry must identify the implementation and test evidence,
or remain an explicit blocking finding.

## P22.2 — Dependency and license audits

- Inventory every dependency surface that actually exists: Python direct and
  transitive locks, frontend direct and transitive locks, GitHub Actions, and
  any container or deployment inputs present in the repository. Mark a surface
  not applicable only after proving it is absent.
- Run the existing locked audits, including
  `uv run python scripts/verify_python_dependencies.py` and
  `npm --prefix web audit --audit-level=high`. Add any further scanner only
  when its purpose, version, lock/pin, inputs, and reproducible invocation are
  explicit.
- Record tool version, complete input identity, severity policy, result, and
  evidence for every scan. A critical finding is a hard failure.
- Distinguish a vulnerability finding from scanner or registry unavailability.
  A bounded retry may run only after output proves a transport/service error,
  must use the unchanged dependency input, and must retain the first-attempt
  evidence. Never retry, suppress, downgrade, or relabel a vulnerability
  result into success.
- Every suppression must be narrow and independently tested, with a reason,
  owner, approval authority, expiry, and upstream identifier. Broad package,
  path, scanner, or severity exclusions are prohibited.
- Audit every direct and transitive license against the owner-selected project
  license and distribution model. Resolve unknown, ambiguous, prohibited, or
  incompatible cases explicitly; absence of metadata is not compatibility.
- Keep dependency and license output deterministic, bounded, free of local
  paths and secrets, and checked against the lockfiles for drift.

## P22.3 — Content Security Policy

- Define one restrictive production policy for the packaged UI and required
  local API, SSE, and WebSocket connections.
- Do not add wildcard origins, remote script/style/font/image dependencies,
  `unsafe-eval`, unnecessary inline execution, unrestricted `connect-src`, or
  a fallback that silently weakens a more specific directive.
- Preserve secure response headers, MIME-sniffing protection, loopback and
  cross-origin defaults, API routing, packaged static assets, and deep-link
  fallback behavior.
- Add policy contract tests and negative browser tests proving prohibited
  script, frame, object, style, image, and connection behavior is blocked as
  applicable.
- Run the complete canonical packaged-UI workflow under the production CSP,
  including live transports and repair flow. A passing header snapshot without
  a real browser workflow is insufficient.

## P22.4 — Third-party notices and disclosure

- Generate or maintain a deterministic third-party notice inventory from the
  audited locks. Each distributed package or asset must have the correct name,
  version, copyright information when supplied, license expression and text
  obligations, and source location.
- Add a drift check proving notices match both lockfiles and the selected
  distribution contents. Unknown packages, duplicate conflicting records,
  stale versions, missing license text, and missing attribution fail closed.
- Keep project licensing distinct from third-party licensing. Do not invent a
  project license, relicensing permission, copyright owner, or distribution
  grant.
- Add responsible-disclosure information only when required by the governing
  release policy and only with an owner-approved contact.

## Verification sequence

Use focused verification while implementing, then run every applicable gate
from the final worktree. At minimum:

1. Run the new threat-model, dependency/license, notice-drift, response-header,
   CSP contract, and adversarial browser tests.
2. Run `uv run ruff format --check .` and `uv run ruff check .`; a lint pass
   never substitutes for the formatter check.
3. Run strict Pyright, import-boundary checks, and both explicit platform
   targets when platform-conditional code is touched:
   `uv run pyright --pythonplatform Windows` and
   `uv run pyright --pythonplatform Linux`.
4. Run frontend formatting, linting, strict types, focused coverage, production
   build, packaged-distribution drift, and the browser suites that exercise the
   production CSP.
5. Run all locked dependency and license audits. Exercise the positive finding,
   suppression, expired-suppression, malformed-output, and transport-failure
   paths without contacting unapproved services.
6. Run generated-contract drift checks when API headers or public contracts
   change, and run affected GitHub workflow contract tests when workflows
   change.
7. Run `scripts/validate_instructions.ps1`, `git diff --check`, and the tracked
   content/credential/local-path audit.
8. Run broader tests only where the changed dependency, security boundary, or
   CSP can affect that behavior. Do not substitute an unrelated complete suite
   for the focused security proofs above.

For every command, record the exact invocation, tool version, input identity,
result, skips, and reason. Independently review the final diff and verify the
reported evidence from the final worktree; partial, stale, cancelled, or
different-head evidence is not acceptance.

## Acceptance and hard gates

The Phase 22 rubric is:

- threat coverage: 30;
- dependency posture: 25;
- browser policy: 20;
- license accuracy: 15;
- remediation evidence: 10.

Score at least 90 and leave no hard gate open. Stop and report any critical
vulnerability, missing attribution, unsafe broad bind, CSP bypass, credential
exposure, unresolved prohibited or unknown license, unapproved disclosure
contact, or governing security/data-integrity conflict. Do not hide a finding
through broad exclusions, relaxed assertions, reduced severity, or claims that
an unavailable scan passed.

## Completion handoff

Report:

- P22.1–P22.4 status and changed files;
- the threat/control/test map and remaining gaps;
- scan tools, versions, locked inputs, results, retries, suppressions, and
  remediation;
- the owner-authorized project license and distribution model;
- the notice inventory and lockfile-drift evidence;
- production CSP directives, negative-policy proof, and canonical UI browser
  evidence;
- accepted non-critical residual risks with their authority and expiry;
- unavailable checks and exact reasons;
- final branch, HEAD, worktree, and Git-mutation status.

Do not publish packages, create tags or releases, deploy or host publicly, or
begin out-of-scope external coordination, support, or disclosure. Normal
traffic to the documented dependency-audit endpoints is permitted only for the
required scans and must follow the bounded evidence rules above.

## Absolute Git prohibition

Do not stage files, commit, push, create or update a pull request, create a tag,
publish a release, merge, rebase, cherry-pick, reset, switch branches, or
perform any Git mutation. Read-only Git inspection is permitted. Finish with
every Phase 22 change uncommitted and unpushed. This prohibition is absolute.

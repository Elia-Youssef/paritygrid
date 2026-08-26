# Phase 22 — Security, dependencies, and licensing

Implement Phase 22 completely and only Phase 22. The goal is to close threat-model, dependency, browser-policy, license, attribution, and disclosure gates independently of packaging mechanics.

Before editing, read every governing file in `docs/INDEX.md`, especially `docs/SECURITY.md`, `docs/REPOSITORY_POLICY.md`, `docs/ENGINEERING_STANDARDS.md`, `docs/QUALITY_STRATEGY.md`, `docs/CI_RELEASE.md`, `docs/DELIVERY_PLAN.md`, `docs/WORK_PACKAGES.md`, and existing decision records. Confirm the required application and demonstration phases are accepted. Preserve unrelated worktree changes.

Implement these packages:

- P22.1 threat-model verification.
- P22.2 dependency and license audits.
- P22.3 Content Security Policy.
- P22.4 third-party notices.

Required behavior:

- Turn each in-scope trust boundary and abuse case into a reproducible verification item covering input bounds, path confinement, SSRF and binding restrictions, secret redaction, unsafe rendering, repair approval, idempotency, artifact integrity, and lifecycle denial of service.
- Run authoritative Python, frontend, workflow, and container/deployment dependency scans applicable to the repository; record tool versions, inputs, suppressions, ownership, and expiry.
- Leave no unresolved critical finding. Do not hide findings through broad exclusions or severity downgrades.
- Audit direct and transitive licenses against the selected project license and distribution model. Resolve prohibited or unknown cases explicitly.
- Define a restrictive production Content Security Policy compatible with packaged static assets and required connections, without unsafe wildcard origins or unnecessary inline execution.
- Generate or maintain third-party notices from the audited dependency inventory with correct package, version, copyright, license, and source information.
- Add responsible-disclosure information when the governing release policy requires it, without inventing an unapproved contact.
- Do not perform package publication, release tagging, or public hosting actions.

Verification and evidence:

- Add threat and browser-policy tests, including adversarial inputs and the complete canonical UI workflow under CSP.
- Run dependency and license audit commands, test all suppressions, compare notices to lockfiles, and run repository instruction/content validation.
- Report exact commands, tool versions, results, remediation performed, accepted residual risks with authority, and unavailable checks.

Completion handoff:

- Report package IDs, changed policies and files, scan summaries, notice inventory, CSP evidence, open non-critical risks, and limitations.
- Leave all work uncommitted and do not begin Phase 23.

## Absolute Git prohibition

Do not stage files, commit, push, create or update a pull request, create a tag, publish a release, merge, rebase, cherry-pick, reset, switch branches, or perform any Git mutation. Read-only Git inspection is permitted. Finish with every Phase 22 change uncommitted and unpushed. This prohibition is absolute.

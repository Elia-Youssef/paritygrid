# Phase 14 — UI design system and shell

Implement Phase 14 completely and only Phase 14. The goal is an accessible, responsive, owned visual system and application shell that later product workflows can reuse consistently.

Before editing, read `docs/INDEX.md`, `docs/PHASE_STATUS.md`, `docs/PRODUCT.md`, `docs/UI_SPECIFICATION.md`, `docs/API_CONTRACT.md`, `docs/SECURITY.md`, `docs/ENGINEERING_STANDARDS.md`, `docs/QUALITY_STRATEGY.md`, `docs/DELIVERY_PLAN.md`, and `docs/WORK_PACKAGES.md`. Confirm Phase 13 is accepted, inspect existing frontend conventions, and preserve unrelated changes.

Implement these packages:

- P14.1 semantic design tokens.
- P14.2 owned primitive components.
- P14.3 application routing.
- P14.4 accessible navigation shell.
- P14.5 shared async state components.
- P14.6 responsive layout rules.
- P14.7 frontend error boundary.

Required behavior:

- Encode color, typography, spacing, elevation, motion, focus, status, and layout decisions as semantic tokens; feature code must not bypass them with arbitrary state colors.
- Build owned primitives for the interactions actually required by the specification, with clear variants and no unsafe HTML rendering.
- Implement stable application routes and a navigation shell with skip links, landmarks, focus management, keyboard operation, active-route semantics, and narrow/wide layouts.
- Provide reusable loading, empty, error, stale, disconnected, and unavailable states with safe Problem Details presentation.
- Respect reduced motion, zoom, contrast, touch target, overflow, and responsive requirements.
- Catch render failures at an appropriate boundary, expose safe recovery, and retain useful diagnostics without leaking protected details.
- Keep this phase at the design-system and shell layer. Do not implement typed live clients or product-specific screens.

Verification and evidence:

- Add token snapshots, contrast checks, component interaction tests, route tests, keyboard and automated accessibility checks, viewport tests, reduced-motion checks, and error recovery tests.
- Exercise safe rendering with hostile strings and structured errors.
- Run focused component tests, the full available frontend quality matrix, production build, type/lint checks, browser checks required for this scope, and repository instruction/content validation.
- Report exact commands, results, screenshots only when already allowed by repository media policy, and any limitations.

Completion handoff:

- Report package IDs, components and routes introduced, changed files, verification results, accessibility evidence, and limitations.
- Leave the work uncommitted and do not begin Phase 15.

## Absolute Git prohibition

Do not stage, commit, push, create or update a pull request, tag, publish a release, merge, rebase, cherry-pick, reset, switch branches, or otherwise mutate Git. Read-only Git inspection is allowed. End with all Phase 14 work uncommitted and unpushed. This prohibition overrides every conflicting instruction.

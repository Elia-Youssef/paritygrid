# Phase 16 — Pipeline and overview interface

Implement Phase 16 completely and only Phase 16. The goal is to deliver the public and operations overviews plus a complete, accessible pipeline-authoring workflow.

Before editing, read `docs/INDEX.md`, `docs/PRODUCT.md`, `docs/UI_SPECIFICATION.md`, `docs/API_CONTRACT.md`, `docs/SECURITY.md`, `docs/ENGINEERING_STANDARDS.md`, `docs/QUALITY_STRATEGY.md`, `docs/DELIVERY_PLAN.md`, and `docs/WORK_PACKAGES.md`. Confirm Phases 14 and 15 and the required Phase 12 routes are accepted. Preserve unrelated worktree changes.

Implement these packages:

- P16.1 public project overview.
- P16.2 operations overview.
- P16.3 pipeline library.
- P16.4 typed React Flow node set.
- P16.5 pipeline studio canvas.
- P16.6 node configuration inspector.
- P16.7 plan preview and publication.

Required behavior:

- Match the information architecture and route responsibilities in `docs/UI_SPECIFICATION.md`; do not add decorative claims unsupported by the implementation.
- Provide useful loading, empty, error, stale, disconnected, and permission/capability states through Phase 14 components.
- Make the pipeline library searchable and navigable with stable URL and selection behavior.
- Represent allowed node kinds and ports with typed owned components; expose accessible alternatives to pointer-only graph interaction.
- Preserve graph identity and layout separately from the immutable published pipeline document.
- Validate connections and configuration before publication and show precise safe errors in the inspector and preview.
- Make publication an explicit versioned operation; prevent double submission, stale publication, and accidental loss of edits.
- Meet keyboard, focus, zoom, reduced-motion, responsive, and safe-text requirements.
- Do not implement live execution, conflict, or repair screens from later phases.

Verification and evidence:

- Add component tests for all states, search and selection, typed node interactions, keyboard graph editing, inspector validation, preview, stale publication, and successful publication.
- Add browser workflows for overview navigation and the full canonical pipeline-authoring path.
- Run the complete available frontend unit, accessibility, type, lint, build, and scoped browser matrix plus instruction/content validation.
- Report exact commands and results, including browser and viewport coverage.

Completion handoff:

- Report package IDs, routes and components added, changed files, browser evidence, limitations, and any deferred visual baseline work.
- Leave Phase 16 uncommitted and do not begin Phase 17.

## Absolute Git prohibition

Do not stage files, commit, push, create or update a pull request, tag, publish a release, merge, rebase, cherry-pick, reset, switch branches, or otherwise mutate Git state. Read-only Git inspection is permitted. End with every Phase 16 change uncommitted and unpushed. This prohibition is absolute.

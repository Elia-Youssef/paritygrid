# Phase 17 — Live execution observability

Implement Phase 17 completely and only Phase 17. The goal is to make current and historical execution understandable while preserving the strict distinction between durable truth and ephemeral telemetry.

Before editing, read all governing documents in `docs/INDEX.md`, especially `docs/EXECUTION_MODEL.md`, `docs/API_CONTRACT.md`, `docs/UI_SPECIFICATION.md`, `docs/QUALITY_STRATEGY.md`, `docs/DELIVERY_PLAN.md`, `docs/WORK_PACKAGES.md`, and the runner comparison requirements. Confirm Phases 15 and 16 and the required Phase 7 and 13 contracts are accepted. Preserve unrelated changes.

Implement these packages:

- P17.1 run list and filters.
- P17.2 live run graph overlay.
- P17.3 worker swimlane timeline.
- P17.4 queue and saturation panels.
- P17.5 durable event timeline.
- P17.6 runner comparison dashboard.
- P17.7 system capabilities page.

Required behavior:

- Make run pagination, filtering, refresh, selection, and URL state deterministic and resilient to loading, empty, stale, and error conditions.
- Overlay live node state only when run, pipeline, entity, event, and telemetry versions are compatible.
- Label stale or disconnected telemetry visibly and fall back to durable state without implying continuous observation.
- Provide accessible tables or summaries for every timeline and chart; support keyboard navigation, reduced motion, zoom, and responsive layouts.
- Show bounded queue and saturation metrics with definitions, timestamps, capacity, and unavailable states; avoid conclusions not supported by the measurement.
- Make durable events resumable and gap-aware through the Phase 15 client.
- Compare runners correctness-first using the versioned Phase 7 execution-evidence contract before timing; never claim reconciliation or target equivalence from runner evidence alone.
- Report runtime capabilities as available, unavailable with structured reason, or unsupported. Do not hide capability failure.

Verification and evidence:

- Add component and reducer-integration tests for pagination, reconnect, sequence gaps, stale telemetry, incompatible versions, unavailable capabilities, and runner mismatches.
- Add accessibility tests for chart alternatives and browser workflows covering active-run refresh and recovery.
- Run full available frontend, type, lint, build, accessibility, and scoped browser checks plus repository instruction/content validation.
- Report exact commands, results, viewport coverage, and deliberately unavailable capabilities.

Completion handoff:

- Report package IDs, routes and visualizations added, durable/ephemeral precedence, changed files, evidence, and limitations.
- Leave all work uncommitted and do not begin Phase 18.

## Absolute Git prohibition

Do not stage, commit, push, create or update a pull request, create a tag, publish a release, merge, rebase, cherry-pick, reset, switch branches, or perform any Git mutation. Read-only Git inspection is allowed. Finish with all Phase 17 changes uncommitted and unpushed. This prohibition wins over conflicting instructions.

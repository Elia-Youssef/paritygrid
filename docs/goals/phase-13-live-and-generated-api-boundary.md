# Phase 13 — Live and generated API boundary

Implement Phase 13 completely and only Phase 13. The goal is to complete the domain API, separate durable replay from ephemeral telemetry, reproduce generated contracts exactly, and serve the packaged frontend safely.

Before editing, read all governing documents in `docs/INDEX.md`, especially `docs/API_CONTRACT.md`, `docs/EXECUTION_MODEL.md`, `docs/SECURITY.md`, `docs/QUALITY_STRATEGY.md`, `docs/DELIVERY_PLAN.md`, `docs/WORK_PACKAGES.md`, and the generated-file policy. Confirm Phases 10 through 12 are accepted and preserve unrelated changes.

Implement these packages:

- P13.1 reconciliation routes.
- P13.2 repair routes.
- P13.3 durable SSE stream.
- P13.4 live WebSocket telemetry.
- P13.5 deterministic OpenAPI export.
- P13.6 TypeScript type generation.
- P13.7 production frontend serving.

Required behavior:

- Expose version-aware, paginated reconciliation results and the exact stale-safe repair approval/application workflow.
- Stream durable events through SSE with monotonic per-run identities, resume from the last acknowledged durable event, explicit gap behavior, bounded buffering, heartbeats, and slow-client isolation.
- Stream only ephemeral operational telemetry through WebSocket. Disconnect, reconnect, or slow consumption must never affect run correctness or durable state.
- Keep durable and ephemeral payloads distinguishable and prevent telemetry from being used as recovery truth.
- Export OpenAPI deterministically from the application contract and generate the TypeScript boundary reproducibly with no hand-edited generated output.
- Record generator inputs, versions, and drift commands according to repository policy.
- Serve packaged frontend assets through the production application with correct SPA fallback, API 404 behavior, cache policy, confinement, and no Node.js runtime dependency.
- Do not implement frontend state management or product screens from later phases.

Verification and evidence:

- Add route version/pagination/staleness tests; SSE replay, gap, slow-client, cancellation, and disconnect tests; WebSocket connect/disconnect/stale tests; deterministic generation drift tests; and static-serving confinement tests.
- Prove a disconnected live client cannot block or change execution and a resumed SSE client receives every retained durable event exactly as contracted.
- Run narrow tests, full available backend and frontend generation/build checks, type/lint checks, and instruction/content validation.
- Report exact commands and results, including generated-file diffs.

Completion handoff:

- Report package IDs, wire contracts, generated artifacts, changed files, verification results, and limitations.
- Leave the work uncommitted for review and do not begin Phase 14.

## Acceptance rubric

- Domain routes: 20 points.
- Durable streaming: 20 points.
- Telemetry isolation: 15 points.
- Generated contracts: 25 points.
- Packaged serving: 20 points.

The phase requires at least 90/100 and no hard-gate failure. Hard gates are a lost durable event, authoritative telemetry, API or generated-type drift, slow-client execution blockage, or broken production frontend serving.

## Absolute Git prohibition

Do not stage files, commit, push, create or update a pull request, create a tag, publish a release, merge, rebase, cherry-pick, reset, switch branches, or perform any Git mutation. Read-only status, diff, and log inspection is allowed. Finish with all Phase 13 changes uncommitted and unpushed. This prohibition is absolute.

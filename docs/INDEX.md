# Documentation index

This directory is the authoritative instruction set for ParityGrid. When documents conflict, use the precedence order below and resolve the inconsistency before implementation continues.

Current delivery state, accepted commits, and active limitations are tracked in [Phase status](PHASE_STATUS.md). That status record reports evidence and does not change the precedence of governing requirements.

## Precedence

1. [Product charter](PRODUCT.md)
2. [Architecture](ARCHITECTURE.md)
3. [Data architecture](DATA_ARCHITECTURE.md)
4. [Execution model](EXECUTION_MODEL.md)
5. [API contract](API_CONTRACT.md)
6. [UI specification](UI_SPECIFICATION.md)
7. [Security model](SECURITY.md)
8. [Engineering standards](ENGINEERING_STANDARDS.md)
9. [Repository policy](REPOSITORY_POLICY.md)
10. [Git workflow](GIT_WORKFLOW.md)
11. [Quality strategy](QUALITY_STRATEGY.md)
12. [Delivery plan](DELIVERY_PLAN.md)
13. [Work packages](WORK_PACKAGES.md)
14. [CI and release](CI_RELEASE.md)
15. [Public documentation plan](PUBLIC_DOCUMENTATION.md)
16. [Instruction audit](INSTRUCTION_AUDIT.md)

Product requirements take precedence over implementation convenience. Security and data-integrity requirements are hard gates even when another document does not repeat them.

### Decision-record precedence

[Decision records](decisions/) govern a deliberately narrow architecture or contract question:

- A proposed decision record has no authority.
- An accepted decision record amends the highest-precedence governing document that it explicitly names for the decision's stated scope.
- A decision record cannot silently override the product charter or a security or data-integrity hard gate. Any such conflict blocks implementation until the record and the governing document are reconciled in the same change.
- When accepted decision records cover the same scope, the later record supersedes the earlier one only when it says so explicitly. Otherwise the conflict must be resolved before implementation continues.
- Superseded and rejected records remain historical evidence and do not govern new work.

## Requirement language

- **Must** indicates a release-blocking requirement.
- **Should** indicates the expected design; deviations require a documented technical decision.
- **May** indicates an optional implementation choice.

## How work is accepted

Every work package must provide:

1. The intended source changes.
2. Automated tests appropriate to the risk.
3. Evidence that the package-specific verification commands pass.
4. Updated public contracts or documentation when behavior changes.
5. A repository-content check.

A phase is complete only when all of its packages are complete and its rubric scores at least 90 out of 100 with no hard-gate failure.

## Standalone implementation goals

The remaining roadmap is split into one independently executable prompt per phase. These prompts restate the governing scope for handoff convenience; the precedence list above remains authoritative. Every prompt forbids staging, commits, pushes, tags, pull requests, releases, and other Git mutations because integration is reviewed and performed separately.

- [Phase 8 — Deterministic synthetic systems](goals/phase-08-deterministic-synthetic-systems.md)
- [Phase 9 — Connector contract and adapters](goals/phase-09-connector-contract-and-adapters.md)
- [Phase 10 — Reconciliation analysis](goals/phase-10-reconciliation-analysis.md)
- [Phase 11 — Repair and target verification](goals/phase-11-repair-and-target-verification.md)
- [Phase 12 — Core HTTP API](goals/phase-12-core-http-api.md)
- [Phase 13 — Live and generated API boundary](goals/phase-13-live-and-generated-api-boundary.md)
- [Phase 14 — UI design system and shell](goals/phase-14-ui-design-system-and-shell.md)
- [Phase 15 — Typed frontend data and live state](goals/phase-15-typed-frontend-data-and-live-state.md)
- [Phase 16 — Pipeline and overview interface](goals/phase-16-pipeline-and-overview-interface.md)
- [Phase 17 — Live execution observability](goals/phase-17-live-execution-observability.md)
- [Phase 18 — Reconciliation and repair interface](goals/phase-18-reconciliation-and-repair-interface.md)
- [Phase 19 — Canonical scenario and datasets](goals/phase-19-canonical-scenario-and-datasets.md)
- [Phase 20 — Demo orchestration and smoke proof](goals/phase-20-demo-orchestration-and-smoke-proof.md)
- [Phase 21 — Cross-platform performance and stress](goals/phase-21-cross-platform-performance-and-stress.md)
- [Phase 22 — Security, dependencies, and licensing](goals/phase-22-security-dependencies-and-licensing.md)
- [Phase 23 — Packaging and release verification](goals/phase-23-packaging-and-release-verification.md)
- [Phase 24 — Public narrative and architecture documentation](goals/phase-24-public-narrative-and-architecture-documentation.md)
- [Phase 25 — Reproducible evidence and content audit](goals/phase-25-reproducible-evidence-and-content-audit.md)
- [Phase 26 — Release-candidate handoff](goals/phase-26-release-candidate-handoff.md)
- [Optional Phase 27 — Go protocol and simulator](goals/phase-27-optional-go-protocol-and-simulator.md)
- [Optional Phase 28 — Go load and comparison](goals/phase-28-optional-go-load-and-comparison.md)

## Documentation maintenance

- Update these files in the same change as any deliberate architecture or contract change.
- Do not describe planned behavior as already implemented.
- Keep examples executable or explicitly label them as illustrative.
- Prefer links to tests and stable source entry points once implementation exists.
- Record substantial design changes in `docs/decisions/` using the decision template.

## Generated artifacts

`generated/openapi.json` in this directory is the committed OpenAPI contract exported from the application factory. Together with `web/src/api/generated/schema.d.ts` (the generated TypeScript API boundary) and `web/dist` (the reproducible production frontend), it is a generated artifact governed by the repository policy: regeneration commands and drift checks are documented in the [API contract](API_CONTRACT.md), and the tracked inventory lives in `scripts/validate_instructions.ps1`. Generated files are never edited by hand.

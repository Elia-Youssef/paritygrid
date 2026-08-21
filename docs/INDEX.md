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

## Documentation maintenance

- Update these files in the same change as any deliberate architecture or contract change.
- Do not describe planned behavior as already implemented.
- Keep examples executable or explicitly label them as illustrative.
- Prefer links to tests and stable source entry points once implementation exists.
- Record substantial design changes in `docs/decisions/` using the decision template.

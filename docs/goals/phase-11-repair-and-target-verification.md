# Phase 11 — Repair and target verification

Implement Phase 11 completely and only Phase 11. The goal is a safe, approved, idempotent repair workflow followed by an independent observation of target parity.

Before editing, read all governing documents from `docs/INDEX.md`, especially `docs/DATA_ARCHITECTURE.md`, `docs/EXECUTION_MODEL.md`, `docs/API_CONTRACT.md`, `docs/SECURITY.md`, `docs/QUALITY_STRATEGY.md`, `docs/DELIVERY_PLAN.md`, `docs/WORK_PACKAGES.md`, and `docs/decisions/0006-fingerprint-taxonomy.md`. Confirm Phase 10 is accepted, inspect the worktree, and preserve unrelated changes.

Implement these packages:

- P11.1 repair-plan generator.
- P11.2 approval service.
- P11.3 idempotent repair application.
- P11.4 target parity verifier.
- P11.5 showcase-scale reconciliation and repair test.

Required behavior:

- Generate only the explicitly safe repair actions allowed by governing contracts. Do not introduce destructive deletion.
- Bind every plan to exact source, target, reconciliation-fingerprint, policy, and version identities with a canonical plan representation.
- Require explicit approval of the exact current plan and reject stale, superseded, mismatched, concurrent, or already-consumed approval attempts.
- Apply effects through the Phase 9 target connector using durable idempotency identities and bounded retry semantics.
- Handle failures before dispatch, known rejection, known success, timeout, connection loss, and unknown writer or target outcome without exposing a partial state as accepted.
- Fence late or stale work and make replay after interruption converge without duplicate effects.
- Verify parity by independently reading the observed target after effects and computing a separate Phase 11 target-state fingerprint. Do not derive it from the repair plan or execution evidence.
- Preserve audit evidence sufficient to explain approval, each attempted effect, resolution, and final verification without leaking secrets.

Verification and evidence:

- Add safe-action matrix, stale/concurrent approval, idempotency replay, partial-failure, ambiguous-outcome, interruption/restart, and target-observation tests.
- Prove repeated application has one logical effect, failed application has no false accepted state, and changed target data changes the target-state fingerprint.
- Run a bounded showcase-scale reconciliation-to-verification integration case and assert exact outcomes.
- Run all narrow and complete available quality checks plus repository instruction/content validation; report exact commands and results.

Completion handoff:

- Report package IDs, safety invariants, persisted identities, changed files, verification results, and remaining limitations.
- Leave all work uncommitted for review. Do not begin Phase 12.

## Absolute Git prohibition

Do not stage files, commit, push, create or update a pull request, create a tag, publish a release, merge, rebase, cherry-pick, reset, switch branches, or perform another Git mutation. Read-only inspection is allowed. End with the Phase 11 changes uncommitted and unpushed. This prohibition wins over any conflicting instruction.

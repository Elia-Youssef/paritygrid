# Decision: command idempotency lease policy

**Status:** accepted
**Date:** 2026-08-28
**Decision scope:** Durable command-idempotency reservations behind the Phase 12 HTTP boundary
**Supersedes:** The Phase 6 limitation that deferred durable handling of stranded in-progress idempotency reservations

## Context

Phase 3 delivered durable idempotency reservations with insert-on-conflict
begin, terminal compare-and-set completion, and append-only terminal rows, but
left the recovery of in-progress reservations without an owner policy. Phase 12
exposes mutating HTTP commands whose clients retry, so the boundary needs one
explicit rule for concurrent owners, stranded owners, restarts, and unknown
completion outcomes that never duplicates a logical effect and never strands
ownership indefinitely.

The accepted storage contract constrains the options: `idempotency_records`
rows cannot be deleted, terminal rows cannot change at all, and reservation
identity, request digest, and creation time are immutable. The existing
`updated_at` field remains mutable while a row is in progress and is therefore
the durable lease-generation fence.

## Decision

Ownership of an in-progress reservation is a time-based lease fenced by a
durable compare-and-set:

- A retry first reclaims: no row inserts a fresh reservation; a terminal row
  replays the stored logical response; an in-progress row whose `updated_at`
  is older than the lease window is reclaimed only by atomically advancing
  `updated_at` from the observed value to the new lease generation.
- Exactly one concurrent reclaimer wins that compare-and-set. Every loser
  observes the winner's live generation and returns an in-progress conflict.
- A reclaim within the lease returns a bounded conflict so a live concurrent
  request is never duplicated.
- The lease window must exceed the HTTP request timeout (enforced by settings
  validation), so a live request can never be reclaimed; only terminated
  owners leave reclaimable reservations.
- Terminalization keeps the accepted compare-and-set: the first owner to
  complete or fail wins, and a later owner either replays the winner's stored
  response byte-for-byte or fails closed with a conflict.
- Restart and recovery need no special path: replay reads durable rows, and
  stranding is bounded by the lease generation recorded in `updated_at`.
- Non-terminal (5xx) outcomes leave the reservation in progress so an
  identical retry converges after the lease; the underlying use cases are
  idempotent by identity (duplicate creation, replay-tolerant publication,
  fenced run transitions), so reclamation cannot duplicate a logical effect.

The HTTP timeout itself bounds how long any single owner can hold a lease
segment, and the concurrency limit bounds how many owners exist at once.

## Consequences

- No schema change or migration is required; the policy reuses the accepted
  Phase 3 table and its immutability triggers unchanged.
- Stored logical responses must pass the durable redaction wall, which shapes
  the publication and connector-registration acknowledgement bodies.
- A reclaimed request may re-execute an idempotent use case; effects are
  deduplicated by the use case identity rather than by the reservation row.
- Lease tuning is a settings decision validated against the request budget.

## Alternatives considered

- Separate lease owner-token and expiry columns were rejected because the
  existing mutable `updated_at` generation supports an exclusive CAS without
  a schema migration.
- Deleting expired rows was rejected because the table prohibits deletes.
- Never reclaiming (manual recovery only) was rejected because it strands
  ownership indefinitely after a crash.
- Reclaiming with a different request digest was rejected because it would
  break the key-reuse conflict contract.

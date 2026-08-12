# Instruction audit

**Audit date:** 2026-08-12
**Scope:** Documentation-first repository baseline

## Automated checks

Run:

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File scripts/validate_instructions.ps1
```

The validation checks:

- Required instruction files exist.
- Local Markdown links resolve.
- Prohibited internal terminology is absent from Markdown.
- Prohibited instruction filenames are absent.
- Unsafe runtime and secret extensions are not tracked.
- The approved repository description remains exact in the product charter.

## Manual consistency review

The baseline has been reviewed for:

- Product scope and deliberate boundaries.
- Dependency direction.
- SQLite, DuckDB, and Parquet ownership.
- Transactional commit ordering.
- Runner equivalence and resource bounds.
- Recovery and idempotency semantics.
- API and browser version coherence.
- Security and local-first posture.
- Test-layer completeness.
- Phase dependencies, grading, and hard gates.
- Public NDA wording and synthetic-data requirements.

## Findings resolved in the baseline

- SQLite is the sole authoritative operational database.
- DuckDB is explicitly rebuildable and non-authoritative.
- SQLite writes are coordinated through a bounded transactional writer.
- Parquet artifacts have an atomic commit and manifest protocol.
- Sequential execution is the semantic reference for concurrent runners.
- Durable events and ephemeral telemetry have separate guarantees.
- Repair application requires approval, an exact fingerprint, and idempotency.
- The normal demonstration requires no database service.
- The final public README is deferred until implementation evidence exists.

## Items intentionally deferred

- Exact dependency versions and lockfiles belong to Phase 1.
- Public license selection is required before the first release.
- Canonical record counts and fingerprints belong to Phase 13.
- Performance thresholds require a measured baseline in Phase 14.
- Screenshot and animation assets require the finished interface in Phase 15.
- Responsible-disclosure contact details are added before public release.

## Audit conclusion

The instruction baseline is sufficient to begin Phase 1. Each implementation change must preserve these contracts or update the relevant decision and instruction documents before dependent work proceeds.

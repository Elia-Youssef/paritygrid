# Instruction audit

**Audit date:** 2026-08-20
**Scope:** Pre-Phase-7 governance, architecture, and instruction consistency

## Automated checks

Run:

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File scripts/validate_instructions.ps1
```

The current validation checks:

- Required instruction files exist, including the safe root [environment example](../.env.example), durable [phase-status record](PHASE_STATUS.md), and concurrency and fingerprint decisions.
- Local Markdown links resolve.
- Prohibited internal terminology and instruction filenames are absent.
- Decision identifiers are unique and status, date, scope, and supersession metadata is valid.
- High-confidence credential material and developer-specific absolute paths are absent from every tracked and unignored UTF-8 text file, independent of filename or extension.
- The complete unsafe runtime, database, artifact, environment, credential, log, and temporary-file suffix set is not tracked.
- The generated-file inventory exists, and every tracked media asset appears in the explicit purpose/source inventory; the current media inventory is empty.
- Required ignore rules cover runtime, test, database, environment, credential, artifact, and frontend output.
- The approved repository description remains exact in the product charter.

The generated-file sources and drift commands are:

| Generated file | Source of truth | Drift command |
|---|---|---|
| `tests/fixtures/persistence/v0001/{schema.sql,seed.sql,manifest.json}` | Alembic revision 0001 and the deterministic frozen-schema builder | `uv run paritygrid-freeze-v0001` |
| `tests/fixtures/sequential_e2e/expected.json` | The synthetic sequential scenario and generator | `uv run python tests/fixtures/sequential_e2e/generate.py`, then `git diff --exit-code -- tests/fixtures/sequential_e2e/expected.json` |
| `uv.lock` | `pyproject.toml` dependency constraints | `uv lock --check` |
| `web/package-lock.json` | `web/package.json` dependency constraints | `npm --prefix web ci` |

The validator checks the inventory and policy statically. Phase acceptance also runs the documented drift commands in locked environments.

## Manual consistency review

The instruction set has been reviewed for:

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
- Implemented package ownership through Phase 6.
- The distinction between execution-evidence and reconciliation/final-state fingerprints.
- Contract-first Phase 7 dependencies and carried Phase 6 findings.
- Accepted phase commits, evidence, limitations, and the active phase boundary.

## Findings resolved in the current instructions

- SQLite is the sole authoritative operational database.
- DuckDB is explicitly rebuildable and non-authoritative.
- SQLite writes are coordinated through a bounded transactional writer.
- Parquet artifacts have an atomic commit and manifest protocol.
- Sequential execution is the semantic reference for concurrent runners.
- Durable events and ephemeral telemetry have separate guarantees.
- Repair application requires approval, an exact fingerprint, and idempotency.
- The normal demonstration requires no database service.
- The README distinguishes implemented sequential execution from unimplemented concurrent strategies.
- The [phase-status record](PHASE_STATUS.md) links accepted phases, candidate evidence, limitations, and the active boundary.
- Phase 5 acceptance language describes graph validation rather than claiming repair execution.
- Phase 6 acceptance language describes the synthetic contract fixture and execution-evidence fingerprint without claiming the later product workflow.
- Accepted decision records have explicit scoped precedence, and the pipeline-document decision has complete status and date metadata.
- The repository includes a safe `.env.example` containing only current non-secret runtime settings.

## Items intentionally deferred

- Exact dependency versions and lockfiles belong to Phase 1.
- Public license selection is required before the first release.
- Canonical record counts and fingerprints belong to Phase 13.
- Performance thresholds require a measured baseline in Phase 14.
- Screenshot and animation assets require the finished interface in Phase 15.
- Responsible-disclosure contact details are added before public release.
- Threaded, asynchronous, process, and optional interpreter runners belong to Phase 7 and are not yet implemented.
- Reconciliation/final-state fingerprint computation belongs to Phase 9; the Phase 6 finalization fingerprint covers execution evidence.

## Audit conclusion

The instruction set is sufficient to complete pre-Phase-7 remediation. It does not by itself accept Phase 6 or authorize concurrent runner implementation. Phase 6 must first pass the complete command matrix and independent acceptance review on the exact final commit, record that evidence in the phase pull request, close every hard gate, and leave a clean worktree. Each later implementation change must preserve these contracts or update the relevant decision and instruction documents before dependent work proceeds.

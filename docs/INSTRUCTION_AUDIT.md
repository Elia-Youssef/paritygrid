# Instruction audit

**Audit date:** 2026-08-24
**Scope:** Phase 7 governance and Phase 8–28 roadmap, dependency, numbering, and prompt consistency

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
- The Phase 8–28 split, renumbered package dependencies, and standalone implementation-goal coverage.
- Separation of implementation handoff from staging, commits, pushes, tags, pull requests, releases, and publication.

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
- Every former Phase 8–16 scope is split into at least two smaller phases, with the optional Go scope split into Phases 27 and 28.
- Every remaining phase has one indexed standalone prompt with an explicit prohibition on Git mutations and later-phase scope creep.

## Remaining-roadmap renumbering audit

| Former phase | Former packages | Replacement packages | Scope result |
|---|---:|---|---|
| 8 | 14 | P8.1–P8.7 and P9.1–P9.7 | All synthetic-system and connector work retained and separated at the contract boundary |
| 9 | 13 | P10.1–P10.8 and P11.1–P11.5 | All reconciliation and repair work retained and separated before target mutation |
| 10 | 16 | P12.1–P12.9 and P13.1–P13.7 | All HTTP, live transport, generation, and serving work retained and separated before live transport |
| 11 | 12 | P14.1–P14.7 and P15.1–P15.5 | All UI-foundation work retained and separated between shell/design and typed live state |
| 12 | 20 | P16.1–P16.7, P17.1–P17.7, and P18.1–P18.6 | All product routes retained and separated by authoring, observability, and repair workflows |
| 13 | 12 | P19.1–P19.4 and P20.1–P20.8 | All canonical-scenario work retained and separated from orchestration and smoke proof |
| 14 | 13 | P21.1–P21.6, P22.1–P22.4, and P23.1–P23.3 | All hardening work retained and separated by performance, security, and packaging concerns |
| 15 | 12 | P24.1–P24.6, P25.1–P25.5, and P26.1–P26.2 | Documentation and evidence retained; the former combined release-tag package is split into a hash manifest and handoff while tag creation is excluded |
| Optional 16 | 6 | P27.1–P27.3 and P28.1–P28.3 | All optional Go work retained and separated before load generation |

The former package total is 118. The replacement total is 119 because the former release-tag-and-hashes package is now two implementation packages. No package performs a tag or any other Git integration action.

## Items intentionally deferred

- Exact dependency versions and lockfiles belong to Phase 1.
- Public license selection is required before the first release.
- Canonical record counts and fingerprints belong to Phase 19.
- Performance thresholds require a measured baseline in Phase 21.
- Screenshot and animation assets require the finished interface and canonical demonstration in Phase 25.
- Responsible-disclosure contact details are added before public release.
- Threaded, asynchronous, process, and optional interpreter runners belong to Phase 7 and are not yet implemented.
- Reconciliation and target-state fingerprint computation belongs to Phases 10 and 11 respectively; the Phase 6 finalization fingerprint covers execution evidence.

## Audit conclusion

The instruction set now governs the active Phase 7 boundary and the complete remaining Phase 8–28 roadmap. It does not by itself accept Phase 7 or any later phase. Each phase requires its own exact-state verification and independent acceptance review before dependent work begins. Implementation handoffs deliberately stop before every staging, commit, push, tag, pull-request, release, or publication action.

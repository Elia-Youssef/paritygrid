# Public documentation plan

## Goal

The final public presentation must communicate a compelling product story and provide verifiable engineering evidence. It must not overstate production readiness or reproduce confidential work.

## Final README structure

1. Hero screenshot or short animation.
2. Name, tagline, and one-paragraph outcome.
3. Confidentiality and synthetic-data statement.
4. Quick start.
5. Canonical demonstration story.
6. Key engineering guarantees.
7. System architecture diagram.
8. Data architecture diagram.
9. Execution and commit sequence.
10. Runner comparison.
11. UI gallery.
12. Verification baseline.
13. Repository map.
14. Guided code tour.
15. Security model and deliberate boundaries.
16. Technology stack.
17. Rebuild and test commands.
18. License and third-party notices.

## Required diagrams

### System architecture

Show browser, FastAPI, application services, scheduler, runners, connectors, transactional writer, SQLite, artifacts, DuckDB, SSE, and WebSockets.

### Durable commit sequence

Show work completion, artifact commit, result submission, SQLite transaction, checkpoint, durable events, scheduler acknowledgement, and client publication.

### Recovery sequence

Show process interruption, lease scan, checkpoint and idempotency lookup, artifact integrity check, safe requeue, and final verification.

### Data ownership

Clearly distinguish authoritative SQLite state, authoritative manifested artifacts, and rebuildable DuckDB projections.

### Runner comparison

Show one plan feeding sequential, threaded, and async runners that converge on one verified fingerprint.

Diagrams begin as Mermaid where practical. Exported static versions used in the README must be generated reproducibly and match the source diagram.

## Required UI images

- Project overview with canonical story.
- Pipeline studio.
- Live execution graph.
- Worker timeline and backpressure.
- Reconciliation workbench.
- Field-level conflict inspector.
- Repair-plan approval.
- Runner-comparison dashboard.

Screenshots must:

- Use canonical synthetic data.
- Use a fixed viewport and color theme.
- Hide local absolute paths.
- Include useful alt text.
- Be captured by a reproducible Playwright command.
- Avoid empty decorative screens.

## Animated demonstration

The short animation should show:

1. Start the canonical run.
2. Observe concurrent source nodes.
3. See one rate-limit retry and one quarantine.
4. Inspect reconciliation conflicts.
5. Approve and apply repairs.
6. Reach verified parity.
7. Compare runner fingerprints and timing.

Keep it short enough to load comfortably on GitHub. Provide a still-image fallback.

## Canonical narrative

The README must state exact expected facts from the versioned scenario, including input counts, accepted records, quarantined records, conflict classifications, repair actions, retry count, artifacts, and final fingerprint. These values are populated only after Phase 13 locks them.

## Code tour

The tour follows one partition through:

```text
untrusted source
  -> connector validation
  -> normalized records
  -> manifested Parquet artifact
  -> transactional checkpoint
  -> DuckDB reconciliation
  -> repair plan
  -> idempotent target effect
  -> canonical verification
  -> coherent browser state
```

Each section links to the smallest stable source entry point and its focused tests.

## Verification report

Report:

- Supported operating systems and runtimes.
- Unit, property, persistence, analytical, contract, concurrency, recovery, browser, and smoke results.
- Coverage by architectural area.
- Canonical expected counts and fingerprints.
- Performance environment and measurements.
- Known limitations and excluded claims.

Do not publish a benchmark without hardware, dataset, runner, concurrency, and methodology context.

## Writing style

- Lead with the outcome.
- Use precise claims backed by links.
- Explain domain language once and use it consistently.
- Keep commands copyable.
- Separate verified current behavior from future ideas.
- Avoid inflated production claims.
- Avoid confidential comparisons.

## Documentation QA

- Test every command from a clean clone.
- Check every local and web link.
- Verify image alt text.
- Verify diagrams against current implementation names.
- Recreate screenshots from canonical data.
- Verify confidentiality wording exactly.
- Search for local absolute paths, secrets, stale version numbers, and placeholder values.
- Confirm the README does not claim optional features as required or complete.

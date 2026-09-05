# CI and release

## Command design

The project must expose stable top-level verification commands. Exact command names are finalized during Phase 1, but the required intent is:

```powershell
uv run paritygrid verify quick
uv run paritygrid verify main
uv run paritygrid verify nightly
uv run paritygrid verify release
uv run paritygrid smoke
```

Frontend commands must use the committed lockfile and non-interactive execution.

### Phase 1 command set

The initial required lane uses these reproducible commands:

```powershell
uv sync --locked --all-groups
uv run python scripts/verify_python_dependencies.py
uv run ruff format --check .
uv run ruff check .
uv run pyright
uv run paritygrid-check-boundaries
uv run pytest --cov=paritygrid --cov-report=term-missing
uv run python scripts/verify_python_coverage.py
uv run python scripts/verify_wheel_install.py
uv run paritygrid smoke
npm --prefix web ci
npm --prefix web run format:check
npm --prefix web run lint
npm --prefix web run typecheck
npm --prefix web run test:coverage
npm --prefix web run build
npm --prefix web exec -- playwright install chromium
npm --prefix web run test:browser
uv run python scripts/verify_frontend_api_path.py
```

The pre-push portability checks supplement the host-native `pyright` run:

```powershell
uv run pyright --pythonplatform Windows
uv run pyright --pythonplatform Linux
```

Both target checks are required when platform-specific source or tests change.
Runtime tests on the applicable hosted operating systems remain authoritative;
targeted static analysis does not emulate operating-system behavior. Tests that
use platform-only modules must evaluate their platform skip or capability gate
before importing those modules.

When a GitHub Actions workflow changes, run its repository contract tests before
the first push. For the current workflow set this includes:

```powershell
uv run pytest tests/quality/test_nightly_workflow.py
```

The contract must inspect every changed workflow and reject invalid step
semantics, including command-only keys on action (`uses`) steps. A generic YAML
parse alone is insufficient.

The Chromium command exercises the built production shell with deterministic locale, timezone, viewport, and reduced-motion settings. In CI, the frontend-only job runs the mocked browser suite through `web/playwright.frontend.config.ts`. The dependent Frontend API smoke job, which installs both locked Python and Node environments, verifies final third-party notice and distribution drift (`scripts/verify_third_party_notices.py`), runs the transport-classified frontend dependency audit (`scripts/verify_frontend_dependencies.py`), exercises the Vite development proxy against the FastAPI application factory, and then runs `web/e2e/demo.spec.ts` against the real packaged demo. The Python jobs run the transport-classified locked Python audit on both operating systems; the nightly browser job installs the same locked Python environment before verifying the final third-party notices. This keeps each check in a job that provides every declared input without skipping either suite. Later phases add the stable `paritygrid verify` profiles without removing these underlying checks.

The dependency audit exports every locked group to a temporary hash-pinned requirements file, omits only the unpublished local project, and audits that exact set in strict mode. The scoped coverage command consumes the same `.coverage` data as the aggregate report and independently requires at least 90 percent branch coverage for `paritygrid.application.execution`, 95 percent for the sequential runner, and 90 percent for `paritygrid.adapters.runners`. Every new runner adapter remains inside that registered gate.

The wheel verifier builds into a temporary directory, exports locked runtime dependencies, installs them and the wheel into a fresh virtual environment, and runs import, CLI smoke, and spawned-child import probes outside the checkout. Temporary build, environment, and probe files are removed when verification finishes.

The import-boundary command enforces both domain purity and the reserved `src/paritygrid/adapters/runners/process_workers/` boundary. The reserved directory may be absent; any future Python file below it is scanned for persistence, result-commit, connector, filesystem, database, network, and dynamic-import access.

### Phase 21 command set

Phase 21 adds bounded stress-verification commands under the existing `stress` group. Each writes a versioned, deterministic JSON report and never records hostnames, usernames, local paths, or environment values.

```powershell
uv run paritygrid stress performance --root <TEMP-DIR> --report <EVIDENCE-DIR>/performance.json
uv run paritygrid stress resources --root <TEMP-DIR> --report <EVIDENCE-DIR>/resource-bounds.json
uv run paritygrid stress capabilities [--json] [--report <EVIDENCE-DIR>/capability-matrix.json]
```

- `stress performance` runs the correctness-gated performance harness over the accepted Phase 19 showcase profile and Phase 20 orchestration: the showcase derivation, the executed story manifest, and cross-runner execution-evidence equality must all hold before any timing is accepted, and every measured repetition re-proves its own manifest equality. Repetition counts are bounded options (`--story-warmups`, `--story-repetitions`, `--runner-warmups`, `--runner-repetitions`).
- `stress resources` runs the bounded resource exercise over real runtime owners: steady-state and repeated engine executions with a documented bounded-growth method and an enforced scripted-retry failure, queue saturation with bounded backpressure, cancellation, the scripted retry failure, partial-startup rollback, idempotent repeated shutdown, the interruption-and-restart proof, and final orphan detection.
- `stress capabilities` prints or writes the closed, versioned runtime capability matrix. Every known profile reports exactly one state — `available`, `unavailable` with a bounded structured reason, or `unsupported` — and full-plan strategies remain structurally distinct from subordinate pools.

The Windows and Linux platform matrix lives in `scripts/verify_platform_matrix.py` (logic in `src/paritygrid/quality/platform_matrix.py`): it builds the wheel, installs it with locked runtime dependencies outside the checkout, and verifies imports, CLI help, spawn semantics, packaged frontend assets, backend smoke, all three headless demo profiles, the cross-runner comparison, and bounded cleanup with orphan detection from clean temporary roots. Windows coverage includes 8.3-style path components, paths with spaces and Unicode, and junction safety; Linux coverage includes read-only permission behavior. A failed step fails the matrix; a capability the host cannot provide at all is listed as unavailable in the summary with its reason, and the exit code never reports success when a step failed.

### Nightly workflow

The `.github/workflows/nightly.yml` scheduled workflow implements the nightly lane on Windows and Linux with bounded job timeouts, least-privilege permissions, commit-SHA-pinned actions, a non-cancelling concurrency group, and a gate job that fails when any required nightly job was skipped. It runs the complete test suite under the full `nightly` Hypothesis profile, repeats the scheduling, lifecycle, and cleanup stress cells, verifies WAL stress, runs the performance harness, the resource-bounds exercise, and the capability profiles on both operating systems, and exercises Chromium, Firefox, and WebKit. It uploads only the bounded JSON evidence directory with explicit retention; databases, WAL files, coverage, and unrestricted logs are never uploaded. The workflow keeps `workflow_dispatch` for controlled manual proof, and its scheduled path remains unexecuted until an authorized integration task pushes it.

## Pull-request lane

Runs on each proposed change:

- Repository-content audit.
- Python format, lint, and strict typing.
- Import-boundary verification.
- Frontend format, lint, and strict typing.
- Python unit tests.
- Fast property-test profile.
- SQLite integration tests.
- DuckDB golden tests.
- API contract tests.
- Frontend component tests.
- Production frontend build.
- Generated contract drift check (`scripts/export_openapi.py --check`, `scripts/generate_api_types.py --check`, and `scripts/verify_frontend_dist.py`).
- Chromium canonical browser test.
- Sequential headless smoke test.

The lane should provide useful failure output without uploading sensitive or unbounded artifacts.

## Main-branch lane

Includes the pull-request lane plus:

- Windows and Linux.
- Threaded smoke.
- Async smoke.
- Crash-recovery subset.
- Packaging build and installation test.
- Committed frontend distribution drift check.

## Nightly lane

Includes:

- Full Hypothesis profile.
- Repeated scheduling stress.
- Full crash-recovery matrix.
- Mutation-testing subset.
- Chromium, Firefox, and WebKit browser runs.
- Showcase-scale dataset.
- Performance comparison.
- Memory and queue-bound verification.
- Dependency and license audit.
- Optional runtime capability profiles.

Nightly performance results are retained as bounded structured artifacts with environment metadata.

## Release lane

The release lane starts from a clean checkout and locked dependencies. It performs:

1. Repository-content and confidentiality audit.
2. Complete Windows and Linux test matrix.
3. All required runner smoke profiles.
4. Full recovery and idempotency suite.
5. Python package build and isolated install.
6. Production frontend clean rebuild and drift check.
7. OpenAPI and generated-type clean rebuild.
8. Documentation link and command validation.
9. Reproducible screenshot verification.
10. Third-party license inventory.
11. Release artifact hashing.

## Matrix policy

### Operating systems

- Windows is required because the project is developed and demonstrated there.
- Linux is required for common public-repository and deployment evaluation.
- macOS may be added when resources permit but is not a first-release hard gate.

### Python

- Python 3.14 is the primary target.
- Optional free-threaded or interpreter profiles are isolated from the core matrix.
- Capability tests must report unsupported profiles clearly.

### Browsers

- Chromium is the fast per-change target.
- Chromium, Firefox, and WebKit are required nightly and for release.

## Caching

- Cache dependencies, not runtime databases or canonical output.
- Cache keys include lockfile hashes and relevant runtime versions.
- Verification must not pass only because an old generated file was restored from cache.

## Artifacts

Allowed bounded CI artifacts include:

- Test reports.
- Coverage summary and HTML report where useful.
- Playwright traces for failed browser tests.
- Performance JSON and charts.
- Built distributions for release verification.
- Canonical screenshot diffs.

Artifacts must not include secret environment values, local databases with credentials, or unbounded raw payloads.

## Versioning

Use semantic versioning after the first public tag. Until then, use `0.x` releases. Public contract changes require explicit release notes. Canonical encoding and artifact schema versions are separate from package version and must not be inferred from it.

## Release checklist

Phase 26 produces the reproducible candidate hashes and handoff checklist without staging, committing, pushing, tagging, or publishing. A separately authorized integration task creates the exact candidate commit and performs the Git and hosting actions required to evaluate the final checklist below.

- Phase 26 scores at least 90.
- No hard gate is open.
- Working tree is clean.
- Required workflows pass for the exact commit.
- Package and UI report the intended version.
- Canonical expected counts and fingerprint match.
- Confidentiality statement is present.
- License is selected and present.
- Third-party notices are current.
- README commands pass from a clean clone.
- Release notes state deliberate limitations.
- Tag and artifact hashes are recorded.

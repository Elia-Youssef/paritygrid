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

The Chromium command exercises the built production shell with deterministic locale, timezone, viewport, and reduced-motion settings. In CI, the frontend-only job runs the mocked browser suite through `web/playwright.frontend.config.ts`. The dependent Frontend API smoke job, which installs both locked Python and Node environments, separately runs `web/e2e/demo.spec.ts` against the real packaged demo after exercising the Vite development proxy against the FastAPI application factory. This keeps each test in a job that provides its declared runtime without skipping either suite. Later phases add the stable `paritygrid verify` profiles without removing these underlying checks.

The dependency audit exports every locked group to a temporary hash-pinned requirements file, omits only the unpublished local project, and audits that exact set in strict mode. The scoped coverage command consumes the same `.coverage` data as the aggregate report and independently requires at least 90 percent branch coverage for `paritygrid.application.execution`, 95 percent for the sequential runner, and 90 percent for `paritygrid.adapters.runners`. Every new runner adapter remains inside that registered gate.

The wheel verifier builds into a temporary directory, exports locked runtime dependencies, installs them and the wheel into a fresh virtual environment, and runs import, CLI smoke, and spawned-child import probes outside the checkout. Temporary build, environment, and probe files are removed when verification finishes.

The import-boundary command enforces both domain purity and the reserved `src/paritygrid/adapters/runners/process_workers/` boundary. The reserved directory may be absent; any future Python file below it is scanned for persistence, result-commit, connector, filesystem, database, network, and dynamic-import access.

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

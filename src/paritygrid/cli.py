"""ParityGrid command-line entry point."""

from enum import StrEnum
from pathlib import Path
from typing import Annotated

import typer
import uvicorn

from paritygrid import __version__
from paritygrid.adapters.persistence import (
    SQLiteDatabaseConfig,
    create_sqlite_engine,
    upgrade_to_head,
)
from paritygrid.demo.orchestration import DemoOptions
from paritygrid.demo.scenarios import CANONICAL_PIPELINE_VERSION
from paritygrid.quality.wal_stress import (
    WalStressConfig,
    WalStressError,
    WalStressProfile,
    run_wal_stress,
    validate_report_destination,
    write_report_atomic,
)
from paritygrid.runtime.config import Settings
from paritygrid.runtime.smoke import run_smoke

app = typer.Typer(
    name="paritygrid",
    help="Verifiable data reconciliation and observable I/O execution.",
    no_args_is_help=True,
)
database_app = typer.Typer(help="Manage the authoritative operational database.")
stress_app = typer.Typer(help="Run bounded local verification workloads.")
app.add_typer(database_app, name="database")
app.add_typer(stress_app, name="stress")


class DemoRunner(StrEnum):
    """The closed set of full-plan demo runners."""

    sequential = "sequential"
    threaded = "threaded"
    asyncio = "asyncio"


@app.command()
def demo(
    headless: Annotated[
        bool,
        typer.Option(
            "--headless",
            help="Run the full canonical proof without ever opening a browser.",
        ),
    ] = False,
    runner: Annotated[
        DemoRunner,
        typer.Option("--runner", help="Full-plan runner for the canonical engine run."),
    ] = DemoRunner.sequential,
    root: Annotated[
        Path | None,
        typer.Option(
            "--root",
            dir_okay=True,
            file_okay=False,
            resolve_path=False,
            help="Explicit absolute demo root directory.",
        ),
    ] = None,
    json_output: Annotated[
        bool,
        typer.Option(
            "--json",
            help="Print the deterministic machine-readable result after verification.",
        ),
    ] = False,
    open_browser: Annotated[
        bool,
        typer.Option(
            "--open-browser",
            help="Serve mode only: open the reported URL in a browser on request.",
        ),
    ] = False,
    reset: Annotated[
        bool,
        typer.Option(
            "--reset",
            help="Safely reset the owned demo root before starting.",
        ),
    ] = False,
) -> None:
    """Run the canonical demonstration lifecycle with stable exit codes.

    Headless mode verifies every canonical fact and exits 0 only after the
    full proof holds.  Without ``--headless`` the packaged application serves
    until interrupted.  Exit codes: 0 success, 1 failure, 2 usage, 3 readiness
    timeout, 4 cancellation.
    """
    from paritygrid.demo.demo_app import default_demo_root, run_demo_command

    effective_root = root if root is not None else default_demo_root()
    options = DemoOptions(
        root=effective_root,
        runner=runner.value,
        headless=headless,
        open_browser=open_browser,
    )
    raise typer.Exit(code=run_demo_command(options, json_output=json_output, reset=reset))


@app.command("demo-reset")
def demo_reset(
    root: Annotated[
        Path | None,
        typer.Option(
            "--root",
            dir_okay=True,
            file_okay=False,
            resolve_path=False,
            help="Explicit absolute demo root directory to validate and remove.",
        ),
    ],
) -> None:
    """Safely reset one explicitly owned demo root; refuse everything else."""
    from paritygrid.demo.demo_app import default_demo_root
    from paritygrid.demo.ownership import DemoRootError, reset_demo_root

    effective_root = root if root is not None else default_demo_root()
    try:
        resolved = reset_demo_root(effective_root)
    except DemoRootError as error:
        typer.echo(f"demo reset refused: {error}", err=True)
        raise typer.Exit(code=2) from None
    typer.echo(f"Demo root removed: {resolved}")


@app.command("demo-faults")
def demo_faults(
    json_output: Annotated[
        bool,
        typer.Option("--json", help="Print the byte-stable catalog document."),
    ] = False,
) -> None:
    """Show the closed, versioned canonical fault controls."""
    from paritygrid.demo.fault_controls import fault_control_catalog_bytes, fault_controls

    if json_output:
        typer.echo(fault_control_catalog_bytes().decode("utf-8"))
        return
    for control in fault_controls():
        typer.echo(f"{control.identity}")
        typer.echo(f"  activation: {control.activation_point}")
        typer.echo(f"  consequence: {control.expected_consequence}")
        typer.echo(f"  recovery: {control.recovery_behavior}")
        typer.echo(f"  evidence: {control.observable_evidence}")
        typer.echo(f"  reset: {control.reset_behavior}")


@app.command("demo-compare")
def demo_compare(
    root: Annotated[
        Path | None,
        typer.Option(
            "--root",
            dir_okay=True,
            file_okay=False,
            resolve_path=False,
            help="Explicit absolute demo root holding all three engine runs.",
        ),
    ] = None,
    json_output: Annotated[
        bool,
        typer.Option("--json", help="Print the byte-stable cross-runner manifest."),
    ] = False,
) -> None:
    """Compare sequential, threaded, and asyncio durable execution evidence.

    Correctness comes first: the comparison covers versioned execution
    evidence only and never claims reconciliation, repair, or target-state
    equivalence.  Exit code 1 when any evidence differs.
    """
    from paritygrid.adapters.artifacts.paths import resolve_artifact_root
    from paritygrid.adapters.persistence.sqlite import (
        SQLiteDatabase,
        SQLiteDatabaseConfig,
        create_session_factory,
    )
    from paritygrid.adapters.persistence.writer.core import (
        SQLiteTransactionalWriter,
        WriterSettings,
    )
    from paritygrid.demo.demo_app import default_demo_root
    from paritygrid.demo.engine_runner import DemoEngineError, collect_cross_runner_manifest
    from paritygrid.demo.ownership import DemoRootError, open_or_create_demo_root
    from paritygrid.demo.scenario_runner import DATABASE_FILENAME
    from paritygrid.domain.models import PipelineVersion

    effective_root = root if root is not None else default_demo_root()
    try:
        demo_root, _created = open_or_create_demo_root(effective_root)
    except DemoRootError as error:
        typer.echo(f"demo compare refused: {error}", err=True)
        raise typer.Exit(code=2) from None
    database = SQLiteDatabase.open(
        SQLiteDatabaseConfig((demo_root.scenario_path / DATABASE_FILENAME).resolve())
    )
    writer = SQLiteTransactionalWriter(create_session_factory(database.engine), WriterSettings())
    writer.start()
    try:
        manifest = collect_cross_runner_manifest(
            database,
            writer,
            resolve_artifact_root(demo_root.scenario_path / "artifacts"),
            PipelineVersion(CANONICAL_PIPELINE_VERSION),
        )
    except DemoEngineError as error:
        typer.echo(f"demo compare failed: {error}", err=True)
        raise typer.Exit(code=1) from None
    finally:
        writer.close(timeout_seconds=10.0)
        database.close()
    if not manifest.equal:
        for difference in manifest.differences:
            typer.echo(f"  difference: {difference}", err=True)
        raise typer.Exit(code=1)
    typer.echo("Cross-runner execution evidence is equal: sequential == threaded == asyncio")
    if json_output:
        typer.echo(manifest.canonical_bytes().decode("utf-8"))


@app.command("demo-interruption")
def demo_interruption(
    root: Annotated[
        Path | None,
        typer.Option(
            "--root",
            dir_okay=True,
            file_okay=False,
            resolve_path=False,
            help="Explicit absolute demo root directory.",
        ),
    ] = None,
    failpoint: Annotated[
        str,
        typer.Option(
            "--failpoint",
            help="Named durable boundary for the controlled interruption.",
        ),
    ] = "repair.approved",
    json_output: Annotated[
        bool,
        typer.Option("--json", help="Print the machine-readable proof document."),
    ] = False,
) -> None:
    """Prove controlled interruption and durable recovery without duplicate effects."""
    from paritygrid.demo.demo_app import default_demo_root
    from paritygrid.demo.interruption import InterruptionError, run_interruption_proof

    effective_root = root if root is not None else default_demo_root()
    try:
        outcome = run_interruption_proof(effective_root, failpoint)
    except InterruptionError as error:
        typer.echo(f"interruption proof failed: {error}", err=True)
        raise typer.Exit(code=1) from None
    typer.echo(f"Interruption proof passed at failpoint {outcome.failpoint}:")
    for check in outcome.checks:
        typer.echo(f"  - {check}")
    if json_output:
        typer.echo(outcome.canonical_bytes().decode("utf-8"))


@app.command()
def version() -> None:
    """Print the installed ParityGrid version."""
    typer.echo(f"ParityGrid {__version__}")


@app.command()
def serve() -> None:
    """Serve the local application using environment-based settings."""
    settings = Settings()
    uvicorn.run(
        "paritygrid.runtime.composition:create_runtime_app",
        factory=True,
        host=settings.bind_host,
        port=settings.port,
        log_level=settings.log_level,
    )


@app.command()
def smoke() -> None:
    """Verify local HTTP startup, readiness, and clean shutdown."""
    result = run_smoke()
    typer.echo(
        f"Smoke check passed: health={result.health_status}, readiness={result.readiness_status}"
    )


@database_app.command("upgrade")
def database_upgrade(
    database_path: Annotated[
        Path,
        typer.Option(
            "--database",
            dir_okay=False,
            resolve_path=False,
            help="Absolute path to the SQLite operational database file.",
        ),
    ],
    create_parent: Annotated[
        bool,
        typer.Option(
            "--create-parent",
            help="Create missing database parent directories explicitly.",
        ),
    ] = False,
) -> None:
    """Upgrade an explicit file database to the packaged schema revision."""
    config = SQLiteDatabaseConfig(database_path=database_path, create_parent=create_parent)
    engine = create_sqlite_engine(config)
    try:
        with engine.connect() as connection:
            report = upgrade_to_head(connection)
    finally:
        engine.dispose()
    typer.echo(
        f"Database revision: {report.previous_revision or 'empty'} -> {report.current_revision}"
    )


@stress_app.command("wal")
def stress_wal(
    database_path: Annotated[
        Path,
        typer.Option(
            "--database",
            dir_okay=False,
            resolve_path=False,
            help="Absolute path to a new SQLite stress database file.",
        ),
    ],
    report_path: Annotated[
        Path,
        typer.Option(
            "--report",
            dir_okay=False,
            resolve_path=False,
            help="Absolute path for the canonical JSON evidence report.",
        ),
    ],
    profile: Annotated[
        WalStressProfile,
        typer.Option("--profile", help="Finite stress workload profile."),
    ] = WalStressProfile.CI,
    seed: Annotated[
        int,
        typer.Option("--seed", min=0, max=4_294_967_295, help="Deterministic scenario seed."),
    ] = 1,
    create_parent: Annotated[
        bool,
        typer.Option(
            "--create-parent",
            help="Create missing database parent directories explicitly.",
        ),
    ] = False,
) -> None:
    """Verify SQLite WAL readers, bounded writes, contention, and integrity."""
    try:
        validate_report_destination(report_path, database_path)
        result = run_wal_stress(
            WalStressConfig(database_path, profile, seed, create_parent=create_parent)
        )
        write_report_atomic(result, report_path, database_path=database_path)
    except (TypeError, ValueError, WalStressError) as error:
        typer.echo(f"WAL stress failed: {error}", err=True)
        raise typer.Exit(code=1) from None
    typer.echo(
        "WAL stress passed: "
        f"commands={result.committed}, retries={result.writer.contention_retries}, "
        "report=written"
    )

"""ParityGrid command-line entry point."""

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
from paritygrid.quality.wal_stress import (
    WalStressConfig,
    WalStressError,
    WalStressProfile,
    run_wal_stress,
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
        result = run_wal_stress(
            WalStressConfig(database_path, profile, seed, create_parent=create_parent)
        )
        write_report_atomic(result, report_path)
    except (TypeError, ValueError, WalStressError) as error:
        typer.echo(f"WAL stress failed: {error}", err=True)
        raise typer.Exit(code=1) from None
    typer.echo(
        "WAL stress passed: "
        f"commands={result.committed}, retries={result.writer.contention_retries}, "
        "report=written"
    )

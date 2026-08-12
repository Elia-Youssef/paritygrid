"""ParityGrid command-line entry point."""

import typer
import uvicorn

from paritygrid import __version__
from paritygrid.runtime.config import Settings
from paritygrid.runtime.smoke import run_smoke

app = typer.Typer(
    name="paritygrid",
    help="Verifiable data reconciliation and observable I/O execution.",
    no_args_is_help=True,
)


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

"""Command-line entry point tests."""

from unittest.mock import patch

from typer.testing import CliRunner

from paritygrid.cli import app
from paritygrid.runtime.config import Settings

runner = CliRunner()


def test_version_command_reports_installed_version() -> None:
    result = runner.invoke(app, ["version"])

    assert result.exit_code == 0
    assert result.stdout == "ParityGrid 0.1.0\n"


def test_smoke_command_starts_probes_and_stops_local_server() -> None:
    result = runner.invoke(app, ["smoke"])

    assert result.exit_code == 0
    assert result.stdout == "Smoke check passed: health=ok, readiness=ready\n"


def test_serve_command_uses_validated_runtime_settings() -> None:
    settings = Settings(port=8123)

    with (
        patch("paritygrid.cli.Settings", return_value=settings),
        patch("paritygrid.cli.uvicorn.run") as run_server,
    ):
        result = runner.invoke(app, ["serve"])

    assert result.exit_code == 0
    run_server.assert_called_once_with(
        "paritygrid.runtime.composition:create_runtime_app",
        factory=True,
        host="127.0.0.1",
        port=8123,
        log_level="info",
    )

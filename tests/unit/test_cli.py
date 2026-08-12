"""Command-line entry point tests."""

from pathlib import Path
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


def test_database_upgrade_requires_explicit_path() -> None:
    result = runner.invoke(app, ["database", "upgrade"])

    assert result.exit_code == 2
    assert isinstance(result.exception, SystemExit)
    assert result.exception.code == 2


def test_database_upgrade_help_describes_explicit_path() -> None:
    result = runner.invoke(app, ["database", "upgrade", "--help"])

    assert result.exit_code == 0
    assert "--database" in result.output
    assert "Absolute path" in result.output


def test_database_upgrade_migrates_absolute_file_and_reports_repeat(tmp_path: Path) -> None:
    database = tmp_path / "operational.db"

    first = runner.invoke(app, ["database", "upgrade", "--database", str(database)])
    second = runner.invoke(app, ["database", "upgrade", "--database", str(database)])

    assert first.exit_code == 0
    assert first.stdout == "Database revision: empty -> 0001_operational\n"
    assert second.exit_code == 0
    assert second.stdout == "Database revision: 0001_operational -> 0001_operational\n"


def test_database_upgrade_rejects_relative_and_missing_parent_paths(tmp_path: Path) -> None:
    relative = runner.invoke(app, ["database", "upgrade", "--database", "relative.db"])
    missing = runner.invoke(
        app,
        ["database", "upgrade", "--database", str(tmp_path / "missing" / "runtime.db")],
    )

    assert relative.exit_code == 1
    assert "absolute" in str(relative.exception)
    assert missing.exit_code == 1
    assert "does not exist" in str(missing.exception)


def test_database_upgrade_creates_parent_only_with_explicit_option(tmp_path: Path) -> None:
    database = tmp_path / "created" / "nested" / "operational.db"

    result = runner.invoke(
        app,
        [
            "database",
            "upgrade",
            "--database",
            str(database),
            "--create-parent",
        ],
    )

    assert result.exit_code == 0
    assert database.is_file()

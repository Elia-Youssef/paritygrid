"""Phase 21 CLI help and dispatch tests for the stress verification commands."""

import json
from pathlib import Path

import pytest
from typer.testing import CliRunner

from paritygrid.cli import app

runner = CliRunner()


class TestStressHelpContracts:
    def test_stress_group_lists_the_closed_command_set(self) -> None:
        result = runner.invoke(app, ["stress", "--help"])

        assert result.exit_code == 0
        for command in ("capabilities", "performance", "resources", "wal"):
            assert command in result.stdout

    @pytest.mark.parametrize("command", ["performance", "resources", "capabilities"])
    def test_each_new_command_prints_help_and_exits_cleanly(self, command: str) -> None:
        result = runner.invoke(app, ["stress", command, "--help"])

        assert result.exit_code == 0
        assert "Usage" in result.stdout


class TestCapabilitiesCommand:
    def test_capabilities_reports_every_known_profile(self) -> None:
        result = runner.invoke(app, ["stress", "capabilities"])

        assert result.exit_code == 0
        for profile in (
            "sequential (full_plan): available",
            "subordinate-process-pool (subordinate_pool): available",
        ):
            assert profile in result.stdout

    def test_json_output_is_the_canonical_matrix_document(self) -> None:
        result = runner.invoke(app, ["stress", "capabilities", "--json"])

        assert result.exit_code == 0
        document = json.loads(result.stdout)
        assert document["format"] == "paritygrid-runtime-capability-matrix"
        assert document["version"] == 1
        assert len(document["profiles"]) == 5

    def test_report_writes_canonical_bytes(self, tmp_path: Path) -> None:
        report = tmp_path / "matrix.json"
        result = runner.invoke(app, ["stress", "capabilities", "--report", str(report)])

        assert result.exit_code == 0
        payload = report.read_bytes()
        assert b'"format":"paritygrid-runtime-capability-matrix"' in payload


class TestPerformanceCommand:
    def test_missing_root_is_a_usage_error(self) -> None:
        result = runner.invoke(app, ["stress", "performance"])

        assert result.exit_code == 2

    def test_missing_report_parent_is_reported_as_failure(self, tmp_path: Path) -> None:
        result = runner.invoke(
            app,
            [
                "stress",
                "performance",
                "--root",
                str(tmp_path / "root"),
                "--report",
                str(tmp_path / "absent" / "report.json"),
            ],
        )

        assert result.exit_code == 1
        assert "performance harness failed" in (result.stderr or result.stdout)

    def test_dispatch_runs_the_gated_harness_and_writes_the_report(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        from paritygrid.quality import performance_harness

        calls: dict[str, object] = {}

        def fake_build(root: Path, config: object) -> dict[str, object]:
            calls["root"] = root
            calls["config"] = config
            return {
                "story": {"latency_p50_seconds": 1.25},
                "runners": {},
            }

        monkeypatch.setattr(performance_harness, "build_performance_report", fake_build)
        report = tmp_path / "perf.json"
        result = runner.invoke(
            app,
            [
                "stress",
                "performance",
                "--root",
                str(tmp_path),
                "--report",
                str(report),
                "--story-warmups",
                "0",
                "--story-repetitions",
                "1",
                "--runner-warmups",
                "0",
                "--runner-repetitions",
                "2",
            ],
        )

        assert result.exit_code == 0
        assert "report=written" in result.stdout
        assert report.is_file()
        config = calls["config"]
        assert config is not None
        assert config.story_warmup_runs == 0  # type: ignore[attr-defined]
        assert config.runner_measured_runs == 2  # type: ignore[attr-defined]


class TestResourcesCommand:
    def test_missing_root_is_a_usage_error(self) -> None:
        result = runner.invoke(app, ["stress", "resources"])

        assert result.exit_code == 2

    def test_out_of_bound_repetitions_are_refused(self, tmp_path: Path) -> None:
        result = runner.invoke(
            app,
            [
                "stress",
                "resources",
                "--root",
                str(tmp_path),
                "--report",
                str(tmp_path / "r.json"),
                "--repetitions",
                "99",
            ],
        )

        assert result.exit_code == 2

    def test_dispatch_runs_the_exercise_and_writes_the_report(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        from paritygrid.quality import resource_bounds

        seen: dict[str, int] = {}

        def fake_exercise(root: Path, *, repetitions: int) -> dict[str, object]:
            seen["repetitions"] = repetitions
            return {"format": "paritygrid-resource-bounds-report", "version": 1}

        monkeypatch.setattr(resource_bounds, "run_resource_bounds_exercise", fake_exercise)
        report = tmp_path / "bounds.json"
        result = runner.invoke(
            app,
            [
                "stress",
                "resources",
                "--root",
                str(tmp_path),
                "--report",
                str(report),
                "--repetitions",
                "2",
                "--create-parent",
            ],
        )

        assert result.exit_code == 0
        assert "zero_orphans=true" in result.stdout
        assert json.loads(report.read_bytes())["version"] == 1
        assert seen["repetitions"] == 2

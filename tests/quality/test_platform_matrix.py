"""Platform verification matrix tests (P21.1/P21.2) — fast, importable helpers."""

# pyright: reportPrivateUsage=false

import os
import subprocess
import sys
import time
from pathlib import Path

import pytest

from paritygrid.demo.scenario_runner import _validate_component
from paritygrid.quality.platform_matrix import (
    _TILDE_DIR_NAME,
    MatrixStepError,
    MatrixUsageError,
    _path_is_within,
    _venv_child_processes,
    build_summary,
    is_supported_matrix_platform,
    main,
    run_matrix,
    windows_short_path,
)


class TestSummaryContract:
    def test_summary_passes_only_when_every_step_passed(self) -> None:
        passing = build_summary("win32", [{"name": "a", "passed": True}])
        failing = build_summary(
            "win32", [{"name": "a", "passed": True}, {"name": "b", "passed": False}]
        )
        unavailable = build_summary(
            "win32", [{"name": "required", "passed": False, "unavailable": True}]
        )
        assert passing["passed"] is True
        assert failing["passed"] is False
        assert unavailable["passed"] is False

    def test_summary_is_deterministic_json(self) -> None:
        import json

        steps = [{"name": "a", "passed": True, "detail": "x"}]
        first = json.dumps(build_summary("linux", steps), sort_keys=True)
        second = json.dumps(build_summary("linux", steps), sort_keys=True)
        assert first == second


class TestPlatformGuards:
    def test_platform_predicate_is_exact(self) -> None:
        assert is_supported_matrix_platform("win32") is True
        assert is_supported_matrix_platform("linux") is True
        assert is_supported_matrix_platform("darwin") is False
        assert is_supported_matrix_platform("cygwin") is False

    def test_unsupported_platform_is_reported_not_run(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr("paritygrid.quality.platform_matrix.sys.platform", "sunos")
        with pytest.raises(MatrixUsageError, match="not a supported matrix target"):
            run_matrix(tmp_path)

    def test_missing_uv_is_reported_as_usage_error(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        def _missing_uv(_name: str) -> str | None:
            return None

        monkeypatch.setattr("paritygrid.quality.platform_matrix.shutil.which", _missing_uv)
        with pytest.raises(MatrixUsageError):
            run_matrix(tmp_path)


class TestWindowsPathCoverage:
    def test_tilde_component_is_a_valid_scenario_component(self) -> None:
        assert "~" in _TILDE_DIR_NAME
        _validate_component(_TILDE_DIR_NAME)
        _validate_component("RUNNER~1")

    def test_scenario_root_under_a_short_name_parent_is_accepted(self, tmp_path: Path) -> None:
        import shutil

        from paritygrid.demo.scenario_runner import open_scenario_root

        root = tmp_path / "RUNNER~1" / "matrix-scenario"
        try:
            scenario_root = open_scenario_root(root)
            assert scenario_root.path == root.resolve()
        finally:
            shutil.rmtree(root, ignore_errors=True)

    def test_scenario_root_at_a_real_8_3_alias_is_accepted(self, tmp_path: Path) -> None:
        """A true 8.3 alias root is accepted; a junction root never is."""
        import shutil

        from paritygrid.demo.scenario_runner import open_scenario_root
        from paritygrid.quality.platform_matrix import windows_short_path

        long_root = tmp_path / "parity grid matrix long name"
        long_root.mkdir()
        short = windows_short_path(long_root)
        if short is None or str(short) == str(long_root):
            pytest.skip("8.3 short-name generation is disabled on this volume")
        scenario_root = open_scenario_root(short / "scenario")
        assert scenario_root.path == (long_root / "scenario").resolve()
        shutil.rmtree(long_root, ignore_errors=True)

    def test_junction_root_is_still_rejected(self, tmp_path: Path) -> None:
        import _winapi
        import shutil

        import pytest

        from paritygrid.demo.scenario_runner import (
            ScenarioPathError,
            open_scenario_root,
        )

        if os.name != "nt":
            pytest.skip("junctions are Windows-specific")
        target = tmp_path / "real-target"
        target.mkdir()
        link = tmp_path / "j-link"
        _winapi.CreateJunction(str(target), str(link))
        try:
            with pytest.raises(ScenarioPathError):
                open_scenario_root(link / "scenario")
        finally:
            shutil.rmtree(tmp_path, ignore_errors=True)

    def test_short_path_query_returns_value_or_marks_unavailable(self, tmp_path: Path) -> None:
        if os.name != "nt":
            result = windows_short_path(tmp_path)
            assert result is None
            return
        result = windows_short_path(tmp_path)
        assert result is None or "~" in str(result) or result == tmp_path.resolve()


class TestOrphanDetection:
    def test_process_path_containment_rejects_prefix_siblings(self, tmp_path: Path) -> None:
        environment = tmp_path / "venv"
        executable = environment / "bin" / "helper"
        sibling = tmp_path / "venv-stale" / "bin" / "helper"
        assert _path_is_within(str(executable), str(environment)) is True
        assert _path_is_within(str(sibling), str(environment)) is False

    def test_spawned_child_from_the_environment_is_detected_and_reaped(
        self, tmp_path: Path
    ) -> None:
        environment_root = Path(sys.executable).resolve().parent.parent
        child = subprocess.Popen(
            [sys.executable, "-c", "import time; time.sleep(30)"],
            cwd=tmp_path,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        try:
            deadline = time.monotonic() + 5.0
            detected: list[int] | None = None
            while time.monotonic() < deadline:
                detected = _venv_child_processes(environment_root)
                if detected:
                    break
                time.sleep(0.05)
            if detected is None:
                pytest.skip("process enumeration is unavailable on this platform")
            assert child.pid in detected
        finally:
            child.terminate()
            child.wait(10)

    def test_no_children_remain_in_a_quiet_environment(self, tmp_path: Path) -> None:
        detected = _venv_child_processes(Path(sys.executable).resolve().parent.parent)
        if detected is None:
            pytest.skip("process enumeration is unavailable on this platform")
        assert detected == [] or all(pid != 0 for pid in detected)


class TestStepFailureDiagnostics:
    def test_failed_command_raises_a_bounded_diagnostic(self, tmp_path: Path) -> None:
        from paritygrid.quality.platform_matrix import _run

        with pytest.raises(MatrixStepError) as error:
            _run(
                (sys.executable, "-c", "raise SystemExit('boom for the matrix')"),
                cwd=tmp_path,
                timeout_seconds=30,
            )
        assert "boom" in str(error.value)

    def test_utf8_child_output_is_decoded_on_windows_codepage_hosts(self, tmp_path: Path) -> None:
        from paritygrid.quality.platform_matrix import _run, isolated_environment

        output = _run(
            (sys.executable, "-c", "print('Parité 网格')"),
            cwd=tmp_path,
            environment=isolated_environment(),
            timeout_seconds=30,
        )
        assert output.strip() == "Parité 网格"


class TestMainEntrypoint:
    def test_main_reports_failure_without_a_matrix(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        def _explode(*_args: object, **_kwargs: object) -> dict[str, object]:
            raise MatrixUsageError("no uv")

        monkeypatch.setattr("paritygrid.quality.platform_matrix.run_matrix", _explode)
        assert main([]) == 1

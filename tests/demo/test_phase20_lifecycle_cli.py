"""CLI-level demo lifecycle tests: help, faults, headless proof, resume, reset."""

import json
import re
from collections.abc import Sequence
from pathlib import Path
from typing import Protocol, cast, runtime_checkable

import pytest
from click import unstyle
from typer.testing import CliRunner

from paritygrid.cli import app
from paritygrid.demo.fault_controls import fault_controls
from paritygrid.demo.proof import DEMO_RESULT_FORMAT, DEMO_RESULT_VERSION

runner = CliRunner()

_TIMESTAMP_LIKE = re.compile(rb"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}")
_DRIVE_PATH = re.compile(rb"[A-Za-z]:\\\\")

_RUNNER_CLOSED_SET = ("sequential", "threaded", "asyncio")
_EXPECTED_FINGERPRINT_KINDS = {
    "execution_evidence": ("execution-evidence", 2),
    "plan": ("plan", 1),
    "reconciliation": ("reconciliation", 1),
    "target_state": ("target_state", 1),
}


def _result_line(stdout: str) -> str:
    documents = [line for line in stdout.splitlines() if line.startswith("{")]
    assert len(documents) == 1, stdout
    return documents[0]


def _mapping(document: dict[str, object], key: str) -> dict[str, object]:
    value = document[key]
    assert isinstance(value, dict), key
    return cast("dict[str, object]", value)


@runtime_checkable
class _ChoiceLike(Protocol):
    """The closed-choices shape every typer enum option carries."""

    choices: Sequence[str]


def _headless_arguments(root: Path, runner_name: str) -> list[str]:
    return [
        "demo",
        "--headless",
        "--runner",
        runner_name,
        "--root",
        str(root),
        "--json",
    ]


@pytest.fixture(scope="module")
def demo_root(tmp_path_factory: pytest.TempPathFactory) -> Path:
    return tmp_path_factory.mktemp("phase20-lifecycle") / "demo"


@pytest.fixture(scope="module")
def sequential_documents(demo_root: Path) -> list[bytes]:
    """Run one real headless demo, then replay it on the same root."""
    arguments = _headless_arguments(demo_root, "sequential")
    first = runner.invoke(app, arguments)
    assert first.exit_code == 0, first.stdout
    second = runner.invoke(app, arguments)
    assert second.exit_code == 0, second.stdout
    return [
        _result_line(first.stdout).encode("ascii"),
        _result_line(second.stdout).encode("ascii"),
    ]


class TestDemoCommandSurface:
    def test_help_lists_the_closed_runner_choices(self) -> None:
        from typer.core import TyperGroup, TyperOption
        from typer.main import get_command

        result = runner.invoke(app, ["demo", "--help"])

        assert result.exit_code == 0
        assert "--runner" in unstyle(result.stdout)
        root = get_command(app)
        assert isinstance(root, TyperGroup)
        context = root.make_context("paritygrid", [], resilient_parsing=True)
        demo_command = root.get_command(context, "demo")
        assert demo_command is not None
        runner_parameter = next(
            parameter for parameter in demo_command.params if parameter.name == "runner"
        )
        assert isinstance(runner_parameter, TyperOption)
        choice = runner_parameter.type
        assert isinstance(choice, _ChoiceLike)
        assert tuple(choice.choices) == _RUNNER_CLOSED_SET

    def test_runner_outside_the_closed_set_exits_with_usage_error(self, tmp_path: Path) -> None:
        root = tmp_path / "demo"

        result = runner.invoke(app, _headless_arguments(root, "process"))

        assert result.exit_code == 2
        assert "Invalid value" in result.stderr
        assert "process" in result.stderr
        assert not root.exists()


class TestDemoFaultsCommand:
    def test_human_output_lists_both_control_identities(self) -> None:
        result = runner.invoke(app, ["demo-faults"])

        assert result.exit_code == 0
        for control in fault_controls():
            assert control.identity in result.stdout

    def test_json_output_is_exactly_the_byte_stable_catalog(self) -> None:
        from paritygrid.demo.fault_controls import fault_control_catalog_bytes

        result = runner.invoke(app, ["demo-faults", "--json"])

        assert result.exit_code == 0
        assert result.stdout == fault_control_catalog_bytes().decode("ascii") + "\n"


class TestHeadlessSequentialProof:
    def test_document_carries_the_verified_canonical_facts(
        self, sequential_documents: list[bytes], demo_root: Path
    ) -> None:
        document: object = json.loads(sequential_documents[0].decode("ascii"))
        assert isinstance(document, dict)
        parsed = cast("dict[str, object]", document)

        assert parsed["format"] == DEMO_RESULT_FORMAT
        assert parsed["result_version"] == DEMO_RESULT_VERSION
        assert parsed["runner"] == "sequential"

        engine = _mapping(parsed, "engine")
        assert engine["strategy_id"] == "sequential"
        assert engine["evidence_kind"] == "execution-evidence"
        assert engine["evidence_version"] == 2

        story = _mapping(parsed, "story")
        counts = _mapping(story, "counts")
        assert counts["rate_limit_retries"] == 1
        assert counts["transient_connection_failures"] == 1
        assert counts["applied_repairs"] == counts["planned_repairs"]

        fingerprints = _mapping(parsed, "fingerprints")
        assert set(fingerprints) == set(_EXPECTED_FINGERPRINT_KINDS)
        for key, (kind, version) in _EXPECTED_FINGERPRINT_KINDS.items():
            fingerprint = _mapping(fingerprints, key)
            assert fingerprint["kind"] == kind
            assert fingerprint["version"] == version

        assert parsed["fault_controls"] == [control.identity for control in fault_controls()]
        assert demo_root.exists()

    def test_document_is_canonical_json_without_paths_or_timestamps(
        self, sequential_documents: list[bytes], demo_root: Path
    ) -> None:
        payload = sequential_documents[0]

        parsed: object = json.loads(payload.decode("ascii"))
        canonical = json.dumps(
            parsed, ensure_ascii=True, separators=(",", ":"), sort_keys=True
        ).encode("ascii")
        assert canonical == payload

        assert _TIMESTAMP_LIKE.search(payload) is None
        assert _DRIVE_PATH.search(payload) is None
        for forbidden in (b"/home/", b"/Users/"):
            assert forbidden not in payload
        for leaked in (
            str(demo_root).encode("ascii"),
            str(demo_root).replace("\\", "\\\\").encode("ascii"),
        ):
            assert leaked not in payload

    def test_replayed_run_is_byte_identical(self, sequential_documents: list[bytes]) -> None:
        assert sequential_documents[0] == sequential_documents[1]


class TestRunnerSelection:
    def test_threaded_runner_reports_the_threaded_strategy_on_the_same_root(
        self, demo_root: Path
    ) -> None:
        result = runner.invoke(app, _headless_arguments(demo_root, "threaded"))

        assert result.exit_code == 0, result.stdout
        document: object = json.loads(_result_line(result.stdout))
        assert isinstance(document, dict)
        parsed = cast("dict[str, object]", document)
        assert parsed["runner"] == "threaded"
        assert _mapping(parsed, "engine")["strategy_id"] == "threaded"


class TestDemoResetCommand:
    def test_reset_removes_the_owned_root_then_refuses_again(self, demo_root: Path) -> None:
        assert demo_root.is_dir()

        first = runner.invoke(app, ["demo-reset", "--root", str(demo_root)])
        assert first.exit_code == 0, first.stdout + first.stderr
        assert not demo_root.exists()

        second = runner.invoke(app, ["demo-reset", "--root", str(demo_root)])
        assert second.exit_code == 2
        assert "refused" in second.stderr

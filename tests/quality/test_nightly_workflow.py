"""Nightly stress workflow contract tests (P21.6).

GitHub workflow files are YAML, and the standard library has no YAML
parser and the dev dependency set deliberately has none, so these tests
use a small indentation-based subset parser sufficient for this
repository's two workflow documents.  The assertions lock the safety
properties the phase requires: schedule plus manual dispatch, read-only
permissions, a non-cancelling concurrency group, full-commit-SHA action
pins, bounded job timeouts, bounded artifact retention, and a gate job
that fails when any required nightly job was skipped.
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import cast

import pytest

_WORKFLOWS = Path(__file__).resolve().parents[2] / ".github" / "workflows"
_NIGHTLY = _WORKFLOWS / "nightly.yml"
_CI = _WORKFLOWS / "ci.yml"


def _strip_comment(line: str) -> str:
    index = line.find(" #")
    return line if index < 0 else line[:index]


def _parse_value(raw: str) -> object:
    text = raw.strip()
    if text.startswith("[") and text.endswith("]"):
        items = [item.strip().strip("'\"") for item in text[1:-1].split(",") if item.strip()]
        return items
    if len(text) >= 2 and text[0] == text[-1] and text[0] in "'\"":
        return text[1:-1]
    if re.fullmatch(r"-?\d+", text):
        return int(text)
    if text in ("true", "false"):
        return text == "true"
    if text in ("null", "~"):
        return None
    return text


_BLOCK_SCALARS = ("|", ">", "|-", ">-", "|+", ">+")


def _parse_simple_yaml(lines: list[str]) -> dict[str, object]:
    """Parse the mapping/list subset used by the repository workflows.

    Supports nested mappings, lists of scalars, lists of mappings, quoted
    and flow-sequence scalars, comments, and ``|``/``>`` block scalars;
    anything else fails loudly instead of guessing.
    """
    tokens: list[tuple[int, str]] = []
    for line in lines:
        stripped = _strip_comment(line.rstrip())
        if not stripped.strip() or stripped.strip().startswith("#"):
            continue
        tokens.append((len(stripped) - len(stripped.lstrip()), stripped.strip()))
    position = 0

    def consume_block(key_indent: int) -> str:
        nonlocal position
        consumed: list[str] = []
        while position < len(tokens):
            block_indent, block_line = tokens[position]
            if block_indent <= key_indent:
                break
            consumed.append(block_line.strip())
            position += 1
        return " ".join(consumed)

    def read_value(key_indent: int, remainder: str) -> object:
        text_value = remainder.strip()
        if text_value in _BLOCK_SCALARS:
            return consume_block(key_indent)
        return _parse_value(remainder)

    def parse_node(indent: int) -> object:
        nonlocal position
        if position >= len(tokens):
            return {}
        if tokens[position][1].startswith("- "):
            return parse_list(indent)
        return parse_mapping(indent)

    def parse_list(indent: int) -> list[object]:
        nonlocal position
        items: list[object] = []
        while position < len(tokens):
            item_indent, item_line = tokens[position]
            if item_indent != indent or not item_line.startswith("- "):
                if item_indent < indent:
                    break
                if not item_line.startswith("- "):
                    break
                raise ValueError(f"ragged list indentation near: {item_line!r}")
            item_text = item_line[2:].strip()
            if ":" in item_text and not item_text.startswith(('"', "'")):
                key, _, remainder = item_text.partition(":")
                position += 1
                entry: dict[str, object] = {}
                if remainder.strip():
                    entry[key.strip()] = read_value(indent, remainder)
                elif (
                    position < len(tokens)
                    and tokens[position][0] > indent
                    and not tokens[position][1].startswith("- ")
                ):
                    entry[key.strip()] = parse_node(tokens[position][0])
                else:
                    entry[key.strip()] = {}
                while position < len(tokens):
                    follow_indent, follow_line = tokens[position]
                    if follow_indent <= indent or follow_line.startswith("- "):
                        break
                    if follow_indent != item_indent + 2:
                        raise ValueError(f"ragged mapping in list near: {follow_line!r}")
                    follow_key, _, follow_rest = follow_line.partition(":")
                    position += 1
                    if follow_rest.strip():
                        entry[follow_key.strip()] = read_value(follow_indent, follow_rest)
                    elif position < len(tokens) and tokens[position][0] > follow_indent:
                        entry[follow_key.strip()] = parse_node(tokens[position][0])
                    else:
                        entry[follow_key.strip()] = {}
                items.append(entry)
                continue
            position += 1
            items.append(_parse_value(item_text))
        return items

    def parse_mapping(indent: int) -> dict[str, object]:
        nonlocal position
        mapping: dict[str, object] = {}
        while position < len(tokens):
            key_indent, line = tokens[position]
            if key_indent < indent:
                break
            if key_indent > indent:
                raise ValueError(f"unexpected indentation near: {line!r}")
            if line.startswith("- "):
                break
            key, _, remainder = line.partition(":")
            key_text = key.strip().strip("'\"")
            position += 1
            if remainder.strip():
                mapping[key_text] = read_value(key_indent, remainder)
                continue
            if position < len(tokens):
                next_indent, next_line = tokens[position]
                if next_indent > key_indent:
                    mapping[key_text] = parse_node(next_indent)
                    continue
                if next_indent == key_indent and next_line.startswith("- "):
                    mapping[key_text] = parse_list(key_indent)
                    continue
            mapping[key_text] = {}
        return mapping

    document = parse_mapping(tokens[0][0] if tokens else 0)
    if position != len(tokens):
        raise ValueError(f"trailing content near: {tokens[position][1]!r}")
    return document


def _as_step(value: object) -> dict[str, object]:
    """Narrow one parsed workflow step to a JSON object."""
    assert isinstance(value, dict)
    return cast("dict[str, object]", value)


def _mapping(document: object, key: str) -> dict[str, object]:
    """Narrow one parsed workflow member to a JSON object for assertions."""
    assert isinstance(document, dict)
    member = cast("object", document[key])
    assert isinstance(member, dict)
    return cast("dict[str, object]", member)


def _items(document: object, key: str) -> list[object]:
    """Narrow one parsed workflow member to a JSON array for assertions."""
    assert isinstance(document, dict)
    member = cast("object", document[key])
    assert isinstance(member, list)
    return cast("list[object]", member)


@pytest.fixture(scope="module")
def nightly() -> dict[str, object]:
    return _parse_simple_yaml(_NIGHTLY.read_text(encoding="utf-8").splitlines())


class TestWorkflowSubsetParser:
    def test_parser_round_trips_the_ci_workflow(self) -> None:
        document = _parse_simple_yaml(_CI.read_text(encoding="utf-8").splitlines())
        assert document["name"] == "CI"
        assert _mapping(document, "permissions") == {"contents": "read"}
        jobs = _mapping(document, "jobs")
        assert jobs
        python_job = _mapping(jobs, "python")
        matrix = _mapping(python_job, "strategy")
        assert matrix["fail-fast"] is False

    def test_parser_rejects_garbage(self) -> None:
        with pytest.raises(ValueError, match="unexpected indentation"):
            _parse_simple_yaml(["key: value", "  - stray"])


class TestNightlyTriggers:
    def test_schedule_and_manual_dispatch_are_both_present(
        self, nightly: dict[str, object]
    ) -> None:
        on = _mapping(nightly, "on")
        schedule = _items(on, "schedule")
        assert schedule
        first = cast("dict[str, object]", schedule[0])
        assert re.fullmatch(r"\d+ \d+ \* \* \*", str(first["cron"]))
        assert "workflow_dispatch" in on

    def test_permissions_are_least_privilege(self, nightly: dict[str, object]) -> None:
        assert _mapping(nightly, "permissions") == {"contents": "read"}

    def test_concurrency_never_cancels_a_newer_run(self, nightly: dict[str, object]) -> None:
        concurrency = _mapping(nightly, "concurrency")
        assert concurrency["cancel-in-progress"] is False
        group = str(concurrency["group"])
        assert "${{ github.ref }}" in group


class TestNightlyActionsAndBounds:
    def test_every_third_party_action_is_pinned_to_a_full_sha(self) -> None:
        for workflow in (_NIGHTLY, _CI):
            for match in re.finditer(r"uses:\s*(\S+)@(\S+)", workflow.read_text(encoding="utf-8")):
                reference = match.group(2)
                assert re.fullmatch(r"[0-9a-f]{40}", reference), (workflow.name, reference)

    def test_every_job_declares_a_bounded_timeout(self, nightly: dict[str, object]) -> None:
        jobs = _mapping(nightly, "jobs")
        for name, job_value in jobs.items():
            job = cast("dict[str, object]", job_value)
            timeout = job.get("timeout-minutes")
            assert isinstance(timeout, int)
            assert 0 < timeout <= 180, name

    def test_artifacts_have_explicit_retention(self, nightly: dict[str, object]) -> None:
        jobs = _mapping(nightly, "jobs")
        retention_steps = 0
        for job_value in jobs.values():
            steps = _items(cast("dict[str, object]", job_value), "steps")
            for step_value in steps:
                step = cast("dict[str, object]", step_value)
                used = str(step.get("uses", ""))
                if used.startswith("actions/upload-artifact"):
                    retention_steps += 1
                    assert _mapping(step, "with")["retention-days"] == 14
        assert retention_steps >= 1

    def test_nightly_jobs_exist_with_expected_content(self, nightly: dict[str, object]) -> None:
        jobs = _mapping(nightly, "jobs")
        assert frozenset(jobs) == {"python-nightly", "browser-nightly", "nightly-gate"}
        python_job = _mapping(jobs, "python-nightly")
        matrix = _mapping(python_job, "strategy")
        matrix_os = _mapping(matrix, "matrix")["os"]
        assert matrix_os == ["ubuntu-latest", "windows-latest"]


class TestNightlyContent:
    def test_python_lane_runs_the_required_stress_content(self) -> None:
        text = _NIGHTLY.read_text(encoding="utf-8")
        for required in (
            "HYPOTHESIS_PROFILE: nightly",
            "verify_python_dependencies.py",
            "test_lifecycle_matrix.py",
            "test_stress_shuffled.py",
            "test_process_pool.py",
            "paritygrid stress wal",
            "paritygrid stress performance",
            "paritygrid stress resources",
            "paritygrid stress capabilities",
            "runner.temp",
        ):
            assert required in text, required

    def test_nightly_prepares_empty_roots_for_fail_closed_harnesses(self) -> None:
        text = _NIGHTLY.read_text(encoding="utf-8")
        setup = text.index("Prepare fresh-owned stress roots")
        performance = text.index("Run the correctness-gated performance harness")
        resources = text.index("Prove memory, queue, cleanup, and orphan bounds")
        assert setup < performance < resources
        assert 'mkdir -p "${performance_root}" "${resources_root}"' in text
        assert 'find "${performance_root}" -mindepth 1 -print -quit' in text
        assert 'find "${resources_root}" -mindepth 1 -print -quit' in text

    def test_evidence_uploads_never_include_databases_or_logs(self) -> None:
        text = _NIGHTLY.read_text(encoding="utf-8")
        assert "*.db" not in text
        assert "evidence" in text
        for forbidden in ("data/paritygrid.db", "ignored.sqlite3", "coverage"):
            assert forbidden not in text

    def test_browser_lane_covers_chromium_firefox_and_webkit(self) -> None:
        text = _NIGHTLY.read_text(encoding="utf-8")
        for browser in ("chromium", "firefox", "webkit"):
            assert f"--browser={browser}" in text, browser

    def test_locked_python_and_frontend_dependencies_are_audited(self) -> None:
        text = _NIGHTLY.read_text(encoding="utf-8")
        assert "verify_python_dependencies.py" in text
        assert "npm audit --audit-level=high" in text

    def test_gate_fails_when_a_required_job_was_skipped(self, nightly: dict[str, object]) -> None:
        jobs = _mapping(nightly, "jobs")
        gate = _mapping(jobs, "nightly-gate")
        needs = _items(gate, "needs")
        assert {str(item) for item in needs} == {"python-nightly", "browser-nightly"}
        steps = _items(gate, "steps")
        run_steps = [str(step.get("run", "")) for step in map(_as_step, steps)]
        assert any('!= "success"' in body for body in run_steps)
        assert any("exit 1" in body for body in run_steps)

    def test_evidence_reports_use_the_cli_report_names(self, nightly: dict[str, object]) -> None:
        # The evidence uploads reference JSON report files only; this locks
        # the naming contract used by the CLI commands.
        del nightly
        text = _NIGHTLY.read_text(encoding="utf-8")
        for report in (
            "wal-stress-${{ matrix.os }}.json",
            "performance-${{ matrix.os }}.json",
            "resource-bounds-${{ matrix.os }}.json",
            "capability-matrix-${{ matrix.os }}.json",
            "platform-matrix-${{ matrix.os }}.json",
        ):
            assert report in text, report


class TestProfileRegistration:
    def test_nightly_hypothesis_profile_is_stricter_than_default(self) -> None:
        from hypothesis import settings

        nightly_profile = settings.get_profile("nightly")
        default_profile = settings.get_profile("default")
        nightly_examples = cast(
            "int",
            nightly_profile.max_examples,  # pyright: ignore[reportUnknownMemberType]
        )
        default_examples = cast(
            "int",
            default_profile.max_examples,  # pyright: ignore[reportUnknownMemberType]
        )
        assert nightly_examples > default_examples
        deadline = nightly_profile.deadline  # pyright: ignore[reportUnknownMemberType, reportUnknownVariableType]
        assert cast("object", deadline) is None

"""Enforce coverage thresholds for independently governed Python scopes."""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
import tempfile
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import cast


@dataclass(frozen=True, slots=True)
class CoverageScope:
    """One source-path scope with an independent branch-coverage minimum."""

    name: str
    path_prefix: str
    minimum: float
    exact: bool = False


APPLICATION_COVERAGE_SCOPES = (
    CoverageScope("application-execution", "src/paritygrid/application/execution/", 90.0),
)
RUNNER_COVERAGE_SCOPES = (
    CoverageScope(
        "sequential-runner", "src/paritygrid/application/execution/runner.py", 95.0, True
    ),
    CoverageScope("runner-adapters", "src/paritygrid/adapters/runners/", 90.0),
    CoverageScope("connector-adapters", "src/paritygrid/adapters/connectors/", 90.0),
)


def _mapping(value: object, subject: str) -> Mapping[str, object]:
    if not isinstance(value, dict):
        raise ValueError(f"coverage JSON {subject} is not an object")
    return cast(dict[str, object], value)


def _count(value: object, subject: str) -> int:
    if not isinstance(value, int):
        raise ValueError(f"coverage JSON {subject} is not an integer")
    return value


def _matches(filename: str, scope: CoverageScope) -> bool:
    normalized = filename.replace("\\", "/")
    return (
        normalized == scope.path_prefix if scope.exact else normalized.startswith(scope.path_prefix)
    )


def _scope_percentage(files: Mapping[str, object], scope: CoverageScope) -> float:
    covered = 0
    possible = 0
    matched = 0
    for filename, details_value in files.items():
        if not _matches(filename, scope):
            continue
        matched += 1
        details = _mapping(details_value, f"entry for {filename}")
        summary = _mapping(details.get("summary"), f"summary for {filename}")
        covered += _count(summary.get("covered_branches"), f"covered_branches for {filename}")
        possible += _count(summary.get("num_branches"), f"num_branches for {filename}")
    if matched == 0 or possible == 0:
        raise ValueError(f"coverage scope {scope.name!r} matched no measurable branches")
    return covered * 100.0 / possible


def verify_coverage(data_file: Path) -> bool:
    """Read one coverage data file and report every registered scope."""
    if not data_file.is_file():
        raise ValueError(f"coverage data file does not exist: {data_file}")
    with tempfile.TemporaryDirectory(prefix="paritygrid-coverage-") as temporary_directory:
        report_path = Path(temporary_directory) / "coverage.json"
        subprocess.run(
            (
                sys.executable,
                "-m",
                "coverage",
                "json",
                "--quiet",
                "--data-file",
                str(data_file),
                "-o",
                str(report_path),
            ),
            check=True,
        )
        document = _mapping(json.loads(report_path.read_text(encoding="utf-8")), "document")
    files = _mapping(document.get("files"), "files")
    passed = True
    for scope in (*APPLICATION_COVERAGE_SCOPES, *RUNNER_COVERAGE_SCOPES):
        percentage = _scope_percentage(files, scope)
        scope_passed = percentage + 1e-9 >= scope.minimum
        passed = passed and scope_passed
        result = "PASS" if scope_passed else "FAIL"
        print(f"{result} {scope.name}: {percentage:.2f}% (minimum {scope.minimum:.2f}%)")
    return passed


def main(argv: Sequence[str] | None = None) -> int:
    """Run the scoped coverage gate."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-file", type=Path, default=Path(".coverage"))
    arguments = parser.parse_args(argv)
    try:
        return 0 if verify_coverage(arguments.data_file.resolve()) else 1
    except (OSError, ValueError, subprocess.CalledProcessError) as error:
        print(f"Scoped coverage verification failed: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())

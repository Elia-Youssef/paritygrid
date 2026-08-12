"""Import-boundary verification tests."""

# pyright: reportPrivateUsage=false

import ast
from pathlib import Path

import pytest

from paritygrid.quality.import_boundaries import (
    _find_file_violations,
    _resolve_from_import,
    find_domain_import_violations,
    main,
)

FIXTURES = Path(__file__).parents[1] / "fixtures" / "import_boundaries"


def test_valid_fixture_has_no_domain_boundary_violation() -> None:
    violations = find_domain_import_violations(FIXTURES / "valid" / "src")

    assert violations == ()


def test_invalid_fixture_reports_outer_import_with_stable_location() -> None:
    source_root = FIXTURES / "invalid" / "src"

    violations = find_domain_import_violations(source_root)

    assert len(violations) == 1
    assert violations[0].render(source_root) == (
        "paritygrid/domain/value.py:3: paritygrid.api.create_app"
    )


def test_absolute_from_import_resolution() -> None:
    node = ast.ImportFrom(module="paritygrid.api", names=[], level=0)

    assert _resolve_from_import(node, "paritygrid.domain.value", is_package=False) == (
        "paritygrid.api"
    )


def test_relative_from_import_resolution_for_package() -> None:
    node = ast.ImportFrom(module="models", names=[], level=1)

    assert _resolve_from_import(node, "paritygrid.domain", is_package=True) == (
        "paritygrid.domain.models"
    )


def test_plain_import_reports_outer_dependency(tmp_path: Path) -> None:
    package_root = tmp_path / "paritygrid"
    domain_root = package_root / "domain"
    domain_root.mkdir(parents=True)
    source = domain_root / "value.py"
    source.write_text("import paritygrid.runtime.config\n", encoding="utf-8")

    violations = _find_file_violations(source, package_root)

    assert [(violation.line, violation.imported_module) for violation in violations] == [
        (1, "paritygrid.runtime.config")
    ]


@pytest.mark.parametrize(
    ("source_text", "imported_module"),
    [
        ("import duckdb\n", "duckdb"),
        ("from fastapi import Depends\n", "fastapi.Depends"),
        ("from httpx import AsyncClient\n", "httpx.AsyncClient"),
        ("import logging\n", "logging"),
        ("import os\n", "os"),
        ("from pathlib import Path\n", "pathlib.Path"),
        ("from pydantic_settings import BaseSettings\n", "pydantic_settings.BaseSettings"),
        ("import shutil\n", "shutil"),
        ("from sqlalchemy.orm import Session\n", "sqlalchemy.orm.Session"),
        ("import tempfile\n", "tempfile"),
    ],
)
def test_infrastructure_imports_are_rejected_from_domain(
    tmp_path: Path,
    source_text: str,
    imported_module: str,
) -> None:
    package_root = tmp_path / "paritygrid"
    domain_root = package_root / "domain"
    domain_root.mkdir(parents=True)
    source = domain_root / "value.py"
    source.write_text(source_text, encoding="utf-8")

    violations = _find_file_violations(source, package_root)

    assert [(violation.line, violation.imported_module) for violation in violations] == [
        (1, imported_module)
    ]


def test_domain_safe_standard_library_imports_are_allowed(tmp_path: Path) -> None:
    package_root = tmp_path / "paritygrid"
    domain_root = package_root / "domain"
    domain_root.mkdir(parents=True)
    source = domain_root / "value.py"
    source.write_text("import hashlib\nfrom dataclasses import dataclass\n", encoding="utf-8")

    violations = _find_file_violations(source, package_root)

    assert violations == []


def test_command_passes_for_current_source_tree(capsys: pytest.CaptureFixture[str]) -> None:
    source_root = Path(__file__).parents[2] / "src"

    exit_code = main(["--source-root", str(source_root)])

    assert exit_code == 0
    assert capsys.readouterr().out == "Import boundaries passed.\n"


def test_command_returns_failure_and_prints_stable_diagnostic(
    capsys: pytest.CaptureFixture[str],
) -> None:
    source_root = FIXTURES / "invalid" / "src"

    exit_code = main(["--source-root", str(source_root)])

    captured = capsys.readouterr()
    assert exit_code == 1
    assert captured.out == ""
    assert captured.err == "paritygrid/domain/value.py:3: paritygrid.api.create_app\n"

"""Import-boundary verification tests."""

# pyright: reportPrivateUsage=false

import ast
from pathlib import Path

import pytest

from paritygrid.quality.import_boundaries import (
    ImportViolation,
    _find_file_violations,
    _resolve_from_import,
    find_domain_import_violations,
    find_process_worker_import_violations,
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


def test_relative_import_without_module_resolves_to_current_package() -> None:
    node = ast.ImportFrom(module=None, names=[], level=1)

    assert _resolve_from_import(node, "paritygrid.domain", is_package=True) == ("paritygrid.domain")


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
        ("import pydantic\n", "pydantic"),
        ("import logging\n", "logging"),
        ("import os\n", "os"),
        ("from pathlib import Path\n", "pathlib.Path"),
        ("import requests\n", "requests"),
        ("import importlib.util\n", "importlib.util"),
        ("from pydantic_settings import BaseSettings\n", "pydantic_settings.BaseSettings"),
        ("import shutil\n", "shutil"),
        ("import sqlite3 as database\n", "sqlite3"),
        ("from sqlalchemy.orm import Session\n", "sqlalchemy.orm.Session"),
        ("import tempfile\n", "tempfile"),
        ("import socket\n", "socket"),
        ("from urllib.request import urlopen\n", "urllib.request.urlopen"),
        ("import subprocess\n", "subprocess"),
        ("import asyncio\n", "asyncio"),
        ("import threading\n", "threading"),
        ("import multiprocessing\n", "multiprocessing"),
        (
            "from concurrent.futures import ThreadPoolExecutor\n",
            "concurrent.futures.ThreadPoolExecutor",
        ),
        ("import importlib\n", "importlib"),
        ("import builtins\n", "builtins"),
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
    source.write_text(
        "\n".join(
            (
                "from collections.abc import Mapping",
                "from __future__ import annotations",
                "from dataclasses import dataclass",
                "from datetime import UTC",
                "from decimal import Decimal",
                "from enum import StrEnum",
                "import hashlib",
                "import json",
                "import re",
                "from types import MappingProxyType",
                "from typing import ClassVar",
                "import unicodedata",
                "from paritygrid.domain.models import InventoryRecord",
                "from .models import Money",
                "",
            )
        ),
        encoding="utf-8",
    )

    violations = _find_file_violations(source, package_root)

    assert violations == []


@pytest.mark.parametrize(
    ("source_text", "expected"),
    [
        ("open('record.json')\n", "builtins.open"),
        ("__import__('fastapi')\n", "builtins.__import__"),
        ("import builtins as runtime\nruntime.open('record.json')\n", "builtins.open"),
        (
            "from builtins import __import__ as load\nload('fastapi')\n",
            "builtins.__import__",
        ),
        (
            "import importlib as loader\nloader.import_module('fastapi')\n",
            "importlib.import_module",
        ),
        (
            "from importlib import import_module as load\nload('fastapi')\n",
            "importlib.import_module",
        ),
        ("load = __import__\nload('fastapi')\n", "builtins.__import__"),
        ("opener = open\nopener('record.json')\n", "builtins.open"),
        (
            "runtime = __builtins__\nruntime.open('record.json')\n",
            "builtins.open",
        ),
        (
            "load = __builtins__['__import__']\nload('fastapi')\n",
            "builtins.__import__",
        ),
        (
            "load = getattr(__builtins__, '__import__')\nload('fastapi')\n",
            "builtins.__import__",
        ),
        (
            "first = __import__\nsecond = first\nsecond('fastapi')\n",
            "builtins.__import__",
        ),
        ("globals()['open']('record.json')\n", "builtins.globals"),
        ("eval(\"__import__('fastapi')\")\n", "builtins.eval"),
    ],
)
def test_dynamic_import_and_file_calls_are_rejected(
    tmp_path: Path,
    source_text: str,
    expected: str,
) -> None:
    package_root = tmp_path / "paritygrid"
    domain_root = package_root / "domain"
    domain_root.mkdir(parents=True)
    source = domain_root / "value.py"
    source.write_text(source_text, encoding="utf-8")

    violations = _find_file_violations(source, package_root)

    assert expected in {violation.imported_module for violation in violations}


def test_dangerous_reference_fails_closed_across_nested_scope_shadowing(tmp_path: Path) -> None:
    package_root = tmp_path / "paritygrid"
    domain_root = package_root / "domain"
    domain_root.mkdir(parents=True)
    source = domain_root / "value.py"
    source.write_text(
        "def unsafe():\n"
        "    load = __import__\n"
        "    return load('fastapi')\n"
        "def unrelated():\n"
        "    load = lambda value: value\n"
        "    return load('safe')\n",
        encoding="utf-8",
    )

    violations = _find_file_violations(source, package_root)

    assert ImportViolation(source, 2, "builtins.__import__") in violations


@pytest.mark.parametrize(
    ("source_text", "expected"),
    [
        ("__loader__.load_module('fastapi')\n", "runtime.__loader__"),
        ("__spec__.loader.load_module('fastapi')\n", "runtime.__spec__"),
    ],
)
def test_runtime_import_hooks_are_rejected(
    tmp_path: Path,
    source_text: str,
    expected: str,
) -> None:
    package_root = tmp_path / "paritygrid"
    domain_root = package_root / "domain"
    domain_root.mkdir(parents=True)
    source = domain_root / "value.py"
    source.write_text(source_text, encoding="utf-8")

    violations = _find_file_violations(source, package_root)

    assert expected in {violation.imported_module for violation in violations}


@pytest.mark.parametrize(
    "source_text",
    [
        "def load():\n    import fastapi\n",
        "class Loader:\n    import sqlite3\n",
        "from typing import TYPE_CHECKING\nif TYPE_CHECKING:\n    from paritygrid.api import app\n",
        "from ..api import app\n",
    ],
)
def test_nested_and_relative_outer_imports_are_rejected(tmp_path: Path, source_text: str) -> None:
    package_root = tmp_path / "paritygrid"
    domain_root = package_root / "domain"
    domain_root.mkdir(parents=True)
    source = domain_root / "value.py"
    source.write_text(source_text, encoding="utf-8")

    assert _find_file_violations(source, package_root)


def test_excessive_relative_import_escape_is_rejected(tmp_path: Path) -> None:
    package_root = tmp_path / "paritygrid"
    nested = package_root / "domain" / "canonical"
    nested.mkdir(parents=True)
    source = nested / "encoder.py"
    source.write_text("from ....api import app\n", encoding="utf-8")

    violations = _find_file_violations(source, package_root)

    assert [(violation.line, violation.imported_module) for violation in violations] == [
        (1, "<relative-import-escape>")
    ]


def test_source_read_failure_is_reported_as_invalid_python(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    package_root = tmp_path / "paritygrid"
    domain_root = package_root / "domain"
    domain_root.mkdir(parents=True)
    source = domain_root / "value.py"
    source.write_text("value = 1\n", encoding="utf-8")

    def fail_read_text(_path: Path, *, encoding: str) -> str:
        del encoding
        raise OSError("synthetic read failure")

    monkeypatch.setattr(Path, "read_text", fail_read_text)

    assert _find_file_violations(source, package_root) == [
        ImportViolation(path=source, line=1, imported_module="<invalid-python>")
    ]


def test_recursive_scan_checks_nested_domain_packages(tmp_path: Path) -> None:
    source_root = tmp_path / "src"
    nested = source_root / "paritygrid" / "domain" / "canonical"
    nested.mkdir(parents=True)
    (nested / "encoder.py").write_text("import sqlite3\n", encoding="utf-8")

    violations = find_domain_import_violations(source_root)

    assert len(violations) == 1
    assert violations[0].render(source_root) == (
        "paritygrid/domain/canonical/encoder.py:1: sqlite3"
    )


def test_missing_future_process_worker_root_passes(tmp_path: Path) -> None:
    assert find_process_worker_import_violations(tmp_path / "src") == ()


def test_future_process_worker_allows_pure_contract_imports(tmp_path: Path) -> None:
    source_root = tmp_path / "src"
    worker_root = source_root / "paritygrid" / "adapters" / "runners" / "process_workers"
    worker_root.mkdir(parents=True)
    (worker_root / "probe.py").write_text(
        "from dataclasses import dataclass\n"
        "from paritygrid.application.planner.execution_plan import ExecutionPlan\n",
        encoding="utf-8",
    )

    assert find_process_worker_import_violations(source_root) == ()


@pytest.mark.parametrize(
    ("source_text", "expected"),
    [
        ("import sqlite3\n", "sqlite3"),
        (
            "from paritygrid.adapters.persistence import SQLiteDatabase\n",
            "paritygrid.adapters.persistence.SQLiteDatabase",
        ),
        (
            "from paritygrid.application.ports.writer import TransactionalWriter\n",
            "paritygrid.application.ports.writer.TransactionalWriter",
        ),
        (
            "from paritygrid.application.ports.execution import RunRepository\n",
            "paritygrid.application.ports.execution.RunRepository",
        ),
        (
            "from paritygrid.application import execution\n",
            "paritygrid.application.execution",
        ),
        ("import asyncio\n", "asyncio"),
        ("import http.client as client\n", "http.client"),
        ("from ssl import SSLContext\n", "ssl.SSLContext"),
        (
            "from ....application.writes import StartRun\n",
            "paritygrid.application.writes.StartRun",
        ),
        ("from ..... import escaped\n", "<relative-import-escape>"),
        ("from pathlib import Path\n", "pathlib.Path"),
        (
            "from typing import TYPE_CHECKING\n"
            "if TYPE_CHECKING:\n"
            "    from paritygrid.application.writes import StartRun\n",
            "paritygrid.application.writes.StartRun",
        ),
        ("__import__('sqlite3')\n", "builtins.__import__"),
    ],
)
def test_future_process_worker_rejects_write_and_dynamic_access(
    tmp_path: Path,
    source_text: str,
    expected: str,
) -> None:
    source_root = tmp_path / "src"
    worker_root = source_root / "paritygrid" / "adapters" / "runners" / "process_workers"
    worker_root.mkdir(parents=True)
    source = worker_root / "unsafe.py"
    source.write_text(source_text, encoding="utf-8")

    violations = find_process_worker_import_violations(source_root)

    assert expected in {violation.imported_module for violation in violations}


def test_invalid_future_process_worker_python_fails_closed(tmp_path: Path) -> None:
    source_root = tmp_path / "src"
    worker_root = source_root / "paritygrid" / "adapters" / "runners" / "process_workers"
    worker_root.mkdir(parents=True)
    source = worker_root / "broken.py"
    source.write_text("def broken(:\n", encoding="utf-8")

    assert find_process_worker_import_violations(source_root) == (
        ImportViolation(source, 1, "<invalid-python>"),
    )


def test_missing_domain_root_fails_closed(tmp_path: Path) -> None:
    source_root = tmp_path / "src"

    violations = find_domain_import_violations(source_root)

    assert len(violations) == 1
    assert violations[0].render(source_root) == ("paritygrid/domain:0: <missing-domain-root>")


def test_invalid_python_fails_closed_with_stable_location(tmp_path: Path) -> None:
    source_root = tmp_path / "src"
    domain_root = source_root / "paritygrid" / "domain"
    domain_root.mkdir(parents=True)
    (domain_root / "broken.py").write_text("def broken(:\n", encoding="utf-8")

    violations = find_domain_import_violations(source_root)

    assert len(violations) == 1
    assert violations[0].render(source_root) == ("paritygrid/domain/broken.py:1: <invalid-python>")


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


def test_command_reports_missing_domain_root_as_failure(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    source_root = tmp_path / "src"

    exit_code = main(["--source-root", str(source_root)])

    captured = capsys.readouterr()
    assert exit_code == 1
    assert captured.out == ""
    assert captured.err == "paritygrid/domain:0: <missing-domain-root>\n"


def test_command_reports_invalid_python_as_failure(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    source_root = tmp_path / "src"
    domain_root = source_root / "paritygrid" / "domain"
    domain_root.mkdir(parents=True)
    (domain_root / "broken.py").write_text("def broken(:\n", encoding="utf-8")

    exit_code = main(["--source-root", str(source_root)])

    captured = capsys.readouterr()
    assert exit_code == 1
    assert captured.out == ""
    assert captured.err == "paritygrid/domain/broken.py:1: <invalid-python>\n"


def test_command_reports_process_worker_violation(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    source_root = tmp_path / "src"
    domain_root = source_root / "paritygrid" / "domain"
    domain_root.mkdir(parents=True)
    (domain_root / "__init__.py").write_text("", encoding="utf-8")
    worker_root = source_root / "paritygrid" / "adapters" / "runners" / "process_workers"
    worker_root.mkdir(parents=True)
    (worker_root / "unsafe.py").write_text("import sqlite3\n", encoding="utf-8")

    exit_code = main(["--source-root", str(source_root)])

    captured = capsys.readouterr()
    assert exit_code == 1
    assert captured.out == ""
    assert captured.err == ("paritygrid/adapters/runners/process_workers/unsafe.py:1: sqlite3\n")

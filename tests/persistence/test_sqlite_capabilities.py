"""SQLite capability contract tests."""

import sqlite3
from typing import cast

import pytest
from hypothesis import given
from hypothesis import strategies as st

from paritygrid.adapters.persistence import (
    MINIMUM_SQLITE_VERSION,
    SQLiteCapabilityError,
    SQLiteConfigurationError,
    SQLiteLibraryInfo,
    SQLitePragmaState,
    build_capability_report,
    current_sqlite_library,
    validate_sqlite_library,
    validate_sqlite_pragmas,
)


def _library(version: tuple[int, int, int], *, threadsafety: int = 3) -> SQLiteLibraryInfo:
    return SQLiteLibraryInfo(
        version=".".join(str(component) for component in version),
        version_info=version,
        threadsafety=threadsafety,
    )


def test_current_sqlite_library_reports_python_runtime() -> None:
    library = current_sqlite_library()

    assert library.version == sqlite3.sqlite_version
    assert library.version_info == sqlite3.sqlite_version_info
    assert library.threadsafety == sqlite3.threadsafety


@pytest.mark.parametrize(
    ("version", "version_info", "threadsafety", "message"),
    [
        ("-1.2.3", (-1, 2, 3), 3, "cannot be negative"),
        ("3.40.0", (3, 41, 0), 3, "must match"),
        ("3.40.0", (3, 40, 0), 2, "not recognized"),
    ],
)
def test_library_info_rejects_incoherent_probe_values(
    version: str,
    version_info: tuple[int, int, int],
    threadsafety: int,
    message: str,
) -> None:
    with pytest.raises(SQLiteConfigurationError, match=message):
        SQLiteLibraryInfo(version, version_info, threadsafety)


@pytest.mark.parametrize(
    ("version", "version_info", "threadsafety"),
    [
        (cast(str, 3), (3, 40, 0), 3),
        ("3.40", cast(tuple[int, int, int], (3, 40)), 3),
        ("3.40.0.1", cast(tuple[int, int, int], (3, 40, 0, 1)), 3),
        ("3.true.0", cast(tuple[int, int, int], (3, True, 0)), 3),
        ("3.40.0", (3, 40, 0), cast(int, True)),
    ],
)
def test_library_info_rejects_wrong_probe_types(
    version: str,
    version_info: tuple[int, int, int],
    threadsafety: int,
) -> None:
    with pytest.raises(SQLiteConfigurationError):
        SQLiteLibraryInfo(version, version_info, threadsafety)


@given(
    major=st.integers(min_value=0, max_value=5),
    minor=st.integers(min_value=0, max_value=99),
    patch=st.integers(min_value=0, max_value=99),
)
def test_minimum_version_uses_componentwise_tuple_order(
    major: int,
    minor: int,
    patch: int,
) -> None:
    version = (major, minor, patch)
    library = _library(version)

    if version < MINIMUM_SQLITE_VERSION:
        with pytest.raises(SQLiteCapabilityError, match="or newer is required"):
            validate_sqlite_library(library)
    else:
        validate_sqlite_library(library)


def test_library_rejects_single_thread_compile_mode() -> None:
    with pytest.raises(SQLiteCapabilityError, match="thread-safe"):
        validate_sqlite_library(_library(MINIMUM_SQLITE_VERSION, threadsafety=0))


@pytest.mark.parametrize("threadsafety", [1, 3])
def test_library_accepts_supported_dbapi_thread_modes(threadsafety: int) -> None:
    validate_sqlite_library(_library(MINIMUM_SQLITE_VERSION, threadsafety=threadsafety))


@pytest.mark.parametrize(
    ("state", "message"),
    [
        (
            SQLitePragmaState(False, "wal", 2, 5_000),
            "foreign-key enforcement",
        ),
        (
            SQLitePragmaState(True, "delete", 2, 5_000),
            "WAL journal mode",
        ),
        (
            SQLitePragmaState(True, "wal", 1, 5_000),
            "full synchronous durability",
        ),
        (
            SQLitePragmaState(True, "wal", 2, 1),
            "busy timeout",
        ),
    ],
)
def test_pragma_validation_rejects_each_weakened_setting(
    state: SQLitePragmaState,
    message: str,
) -> None:
    with pytest.raises(SQLiteCapabilityError, match=message):
        validate_sqlite_pragmas(state)


def test_pragma_validation_accepts_case_insensitive_wal_report() -> None:
    state = SQLitePragmaState(True, "WAL", 2, 5_000)

    validate_sqlite_pragmas(state)


@pytest.mark.parametrize(
    "state",
    [
        SQLitePragmaState(cast(bool, 1), "wal", 2, 5_000),
        SQLitePragmaState(True, cast(str, 1), 2, 5_000),
        SQLitePragmaState(True, "wal", cast(int, True), 5_000),
        SQLitePragmaState(True, "wal", 2, cast(int, True)),
    ],
)
def test_pragma_validation_rejects_wrong_report_types(state: SQLitePragmaState) -> None:
    with pytest.raises(SQLiteCapabilityError, match="invalid"):
        validate_sqlite_pragmas(state)


def test_capability_report_is_immutable_and_normalized() -> None:
    library = _library((3, 50, 4), threadsafety=1)
    report = build_capability_report(
        library,
        SQLitePragmaState(True, "WAL", 2, 5_000),
        supports_json_sql=True,
        supports_returning=True,
    )

    assert report.library_version == "3.50.4"
    assert report.library_version_info == (3, 50, 4)
    assert report.minimum_supported_version == MINIMUM_SQLITE_VERSION
    assert report.threadsafety == 1
    assert report.foreign_keys is True
    assert report.journal_mode == "wal"
    assert report.synchronous_level == 2
    assert report.busy_timeout_ms == 5_000
    assert report.supports_json_sql is True
    assert report.supports_returning is True
    with pytest.raises((AttributeError, TypeError)):
        report.busy_timeout_ms = 1  # type: ignore[misc]


@pytest.mark.parametrize(
    ("supports_json_sql", "supports_returning", "message"),
    [
        (False, True, "JSON SQL"),
        (True, False, "RETURNING"),
        (cast(bool, 1), True, "JSON SQL"),
        (True, cast(bool, 1), "RETURNING"),
    ],
)
def test_capability_report_rejects_missing_or_untyped_required_features(
    supports_json_sql: bool,
    supports_returning: bool,
    message: str,
) -> None:
    with pytest.raises(SQLiteCapabilityError, match=message):
        build_capability_report(
            _library(MINIMUM_SQLITE_VERSION),
            SQLitePragmaState(True, "wal", 2, 5_000),
            supports_json_sql=supports_json_sql,
            supports_returning=supports_returning,
        )

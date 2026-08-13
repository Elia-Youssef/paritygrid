"""Dependency-neutral analytical lifecycle contract tests."""

from dataclasses import FrozenInstanceError
from pathlib import Path
from typing import cast

import pytest

from paritygrid.application.ports.analytics import (
    MAX_ANALYTICS_MEMORY_BYTES,
    MAX_ANALYTICS_THREADS,
    MEBIBYTE,
    MIN_ANALYTICS_MEMORY_BYTES,
    AnalyticalDatabaseConfig,
    AnalyticalDatabaseError,
    AnalyticalDatabaseInvalidError,
    AnalyticalDatabaseLifecycle,
    AnalyticalDatabaseOwnershipError,
    AnalyticalDatabaseSnapshot,
    AnalyticalDatabaseState,
    AnalyticalDatabaseStateError,
    AnalyticalDatabaseStorageError,
)


def test_config_accepts_closed_supported_bounds(tmp_path: Path) -> None:
    minimum = AnalyticalDatabaseConfig(
        (tmp_path / "minimum.duckdb").resolve(),
        threads=1,
        memory_limit_bytes=MIN_ANALYTICS_MEMORY_BYTES,
    )
    maximum = AnalyticalDatabaseConfig(
        (tmp_path / "maximum.DUCKDB").resolve(),
        threads=MAX_ANALYTICS_THREADS,
        memory_limit_bytes=MAX_ANALYTICS_MEMORY_BYTES,
    )

    assert minimum.threads == 1
    assert maximum.threads == MAX_ANALYTICS_THREADS


@pytest.mark.parametrize(
    ("path", "expected"),
    [
        (cast(Path, "state.duckdb"), TypeError),
        (cast(Path, 7), TypeError),
        (Path("state.duckdb"), AnalyticalDatabaseInvalidError),
    ],
)
def test_config_rejects_wrong_or_relative_path(path: Path, expected: type[Exception]) -> None:
    with pytest.raises(expected):
        AnalyticalDatabaseConfig(path)


def test_config_rejects_unsupported_suffix(tmp_path: Path) -> None:
    with pytest.raises(AnalyticalDatabaseInvalidError, match="suffix"):
        AnalyticalDatabaseConfig((tmp_path / "analytics.db").resolve())


@pytest.mark.parametrize(
    "threads",
    [cast(int, True), cast(int, 1.0), 0, MAX_ANALYTICS_THREADS + 1],
)
def test_config_rejects_invalid_thread_count(tmp_path: Path, threads: int) -> None:
    expected = TypeError if type(threads) is not int else AnalyticalDatabaseInvalidError
    with pytest.raises(expected):
        AnalyticalDatabaseConfig((tmp_path / "state.duckdb").resolve(), threads=threads)


@pytest.mark.parametrize(
    "memory",
    [
        cast(int, True),
        cast(int, 64.0),
        MIN_ANALYTICS_MEMORY_BYTES - MEBIBYTE,
        MAX_ANALYTICS_MEMORY_BYTES + MEBIBYTE,
        MIN_ANALYTICS_MEMORY_BYTES + 1,
    ],
)
def test_config_rejects_invalid_memory_limit(tmp_path: Path, memory: int) -> None:
    expected = TypeError if type(memory) is not int else AnalyticalDatabaseInvalidError
    with pytest.raises(expected):
        AnalyticalDatabaseConfig(
            (tmp_path / "state.duckdb").resolve(),
            memory_limit_bytes=memory,
        )


def test_snapshot_is_immutable_and_validates_closed_values(tmp_path: Path) -> None:
    snapshot = AnalyticalDatabaseSnapshot(
        AnalyticalDatabaseState.CLOSED,
        (tmp_path / "state.duckdb").resolve(),
        0,
        0,
    )

    with pytest.raises(FrozenInstanceError):
        snapshot.database_size_bytes = 1  # pyright: ignore[reportAttributeAccessIssue]


@pytest.mark.parametrize(
    ("state", "path", "database_size", "wal_size", "expected"),
    [
        (cast(AnalyticalDatabaseState, "open"), Path.cwd() / "x.duckdb", 0, 0, TypeError),
        (AnalyticalDatabaseState.OPEN, cast(Path, "x"), 0, 0, TypeError),
        (AnalyticalDatabaseState.OPEN, Path("x.duckdb"), 0, 0, TypeError),
        (AnalyticalDatabaseState.OPEN, Path.cwd() / "x.duckdb", True, 0, TypeError),
        (AnalyticalDatabaseState.OPEN, Path.cwd() / "x.duckdb", 0, 1.0, TypeError),
        (AnalyticalDatabaseState.OPEN, Path.cwd() / "x.duckdb", -1, 0, ValueError),
        (AnalyticalDatabaseState.OPEN, Path.cwd() / "x.duckdb", 0, -1, ValueError),
    ],
)
def test_snapshot_rejects_invalid_values(
    state: AnalyticalDatabaseState,
    path: Path,
    database_size: int,
    wal_size: int,
    expected: type[Exception],
) -> None:
    with pytest.raises(expected):
        AnalyticalDatabaseSnapshot(state, path, database_size, wal_size)


def test_error_taxonomy_is_dependency_neutral_and_specific() -> None:
    assert issubclass(AnalyticalDatabaseInvalidError, AnalyticalDatabaseError)
    assert issubclass(AnalyticalDatabaseStateError, AnalyticalDatabaseError)
    assert issubclass(AnalyticalDatabaseOwnershipError, AnalyticalDatabaseError)
    assert issubclass(AnalyticalDatabaseStorageError, AnalyticalDatabaseError)
    assert AnalyticalDatabaseState.CLOSED.value == "closed"
    assert AnalyticalDatabaseState.OPEN.value == "open"
    assert AnalyticalDatabaseState.FAILED.value == "failed"


def test_protocol_has_no_adapter_or_duckdb_dependency() -> None:
    module = Path("src/paritygrid/application/ports/analytics.py").read_text(encoding="utf-8")

    assert "import duckdb" not in module
    assert "paritygrid.adapters" not in module
    assert "DuckDBPyConnection" not in module
    assert set(AnalyticalDatabaseLifecycle.__dict__) >= {
        "open",
        "checkpoint",
        "close",
        "recreate",
        "snapshot",
    }

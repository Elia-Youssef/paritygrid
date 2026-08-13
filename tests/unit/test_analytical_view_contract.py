"""Dependency-neutral analytical view value tests."""

from dataclasses import FrozenInstanceError
from pathlib import Path
from typing import cast

import pytest

from paritygrid.application.ports.analytics import (
    MAX_ANALYTICAL_VIEW_NAME_LENGTH,
    MAX_ANALYTICAL_VIEW_VERSION,
    AnalyticalDatabaseError,
    AnalyticalViewCatalogSnapshot,
    AnalyticalViewColumn,
    AnalyticalViewConflictError,
    AnalyticalViewCorruptionError,
    AnalyticalViewError,
    AnalyticalViewInvalidError,
    AnalyticalViewName,
    AnalyticalViewRecord,
    AnalyticalViewSchemaError,
    AnalyticalViewVersion,
)


@pytest.mark.parametrize(
    "value",
    ["pgv_a", "pgv_inventory_v1", "pgv_" + "x" * (MAX_ANALYTICAL_VIEW_NAME_LENGTH - 4)],
)
def test_view_name_accepts_reserved_portable_namespace(value: str) -> None:
    assert str(AnalyticalViewName(value)) == value


@pytest.mark.parametrize(
    "value",
    [
        cast(str, 1),
        "view",
        "pgv_",
        "pgv_1bad",
        "pgv_Bad",
        "pgv_bad-name",
        "pgv_bad\nname",
        "pgv_" + "x" * MAX_ANALYTICAL_VIEW_NAME_LENGTH,
    ],
)
def test_view_name_rejects_untyped_or_noncanonical_values(value: str) -> None:
    expected = TypeError if type(value) is not str else AnalyticalViewInvalidError
    with pytest.raises(expected):
        AnalyticalViewName(value)


@pytest.mark.parametrize("value", [1, MAX_ANALYTICAL_VIEW_VERSION])
def test_view_version_accepts_positive_int32_bounds(value: int) -> None:
    assert AnalyticalViewVersion(value).value == value


@pytest.mark.parametrize(
    "value", [cast(int, True), cast(int, 1.0), 0, MAX_ANALYTICAL_VIEW_VERSION + 1]
)
def test_view_version_rejects_invalid_values(value: int) -> None:
    expected = TypeError if type(value) is not int else AnalyticalViewInvalidError
    with pytest.raises(expected):
        AnalyticalViewVersion(value)


@pytest.mark.parametrize(
    ("name", "type_name", "nullable"),
    [
        ("", "BIGINT", True),
        (cast(str, 1), "BIGINT", True),
        ("x" * 129, "BIGINT", True),
        ("value", "", True),
        ("value", cast(str, 1), True),
        ("value", "X" * 129, True),
        ("value", "BIGINT", cast(bool, 1)),
    ],
)
def test_view_column_rejects_invalid_values(name: str, type_name: str, nullable: bool) -> None:
    with pytest.raises(TypeError):
        AnalyticalViewColumn(name, type_name, nullable)


def _record(name: str = "pgv_a") -> AnalyticalViewRecord:
    return AnalyticalViewRecord(
        AnalyticalViewName(name),
        AnalyticalViewVersion(1),
        "a" * 64,
        "b" * 64,
        (AnalyticalViewColumn("value", "BIGINT", True),),
    )


def test_view_record_and_catalog_are_immutable_sorted_values() -> None:
    record = _record()
    catalog = AnalyticalViewCatalogSnapshot((record,))

    with pytest.raises(FrozenInstanceError):
        record.version = AnalyticalViewVersion(2)  # pyright: ignore[reportAttributeAccessIssue]
    assert catalog.views == (record,)


@pytest.mark.parametrize(
    "kwargs",
    [
        {"name": "pgv_a"},
        {"version": 1},
        {"definition_sha256": "A" * 64},
        {"output_schema_sha256": "x"},
        {"columns": []},
        {"columns": ("value",)},
        {
            "columns": (
                AnalyticalViewColumn("value", "BIGINT", True),
                AnalyticalViewColumn("value", "VARCHAR", True),
            )
        },
    ],
)
def test_view_record_rejects_invalid_identity_hashes_and_columns(
    kwargs: dict[str, object],
) -> None:
    values: dict[str, object] = {
        "name": AnalyticalViewName("pgv_a"),
        "version": AnalyticalViewVersion(1),
        "definition_sha256": "a" * 64,
        "output_schema_sha256": "b" * 64,
        "columns": (AnalyticalViewColumn("value", "BIGINT", True),),
    }
    values.update(kwargs)
    with pytest.raises((TypeError, AnalyticalViewInvalidError)):
        AnalyticalViewRecord(**values)  # pyright: ignore[reportCallIssue,reportArgumentType]


@pytest.mark.parametrize(
    "views",
    [
        cast(tuple[AnalyticalViewRecord, ...], []),
        cast(tuple[AnalyticalViewRecord, ...], ("bad",)),
        (_record("pgv_b"), _record("pgv_a")),
        (_record(), _record()),
    ],
)
def test_catalog_rejects_untyped_unsorted_or_duplicate_views(
    views: tuple[AnalyticalViewRecord, ...],
) -> None:
    with pytest.raises((TypeError, AnalyticalViewInvalidError)):
        AnalyticalViewCatalogSnapshot(views)


def test_view_errors_are_dependency_neutral_and_specific() -> None:
    assert issubclass(AnalyticalViewError, AnalyticalDatabaseError)
    assert issubclass(AnalyticalViewInvalidError, AnalyticalViewError)
    assert issubclass(AnalyticalViewConflictError, AnalyticalViewError)
    assert issubclass(AnalyticalViewSchemaError, AnalyticalViewError)
    assert issubclass(AnalyticalViewCorruptionError, AnalyticalViewError)
    source = Path("src/paritygrid/application/ports/analytics.py").read_text(encoding="utf-8")
    assert "import duckdb" not in source
    assert "DuckDBPyConnection" not in source

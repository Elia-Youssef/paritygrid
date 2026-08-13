"""Versioned analytical view registry integration tests."""

# pyright: reportPrivateUsage=false

from collections.abc import Iterator
from pathlib import Path
from typing import Any, cast

import pytest

from paritygrid.adapters.analytics import (
    DuckDBAnalyticalViewRegistry,
    DuckDBLifecycleCoordinator,
    DuckDBViewColumnDefinition,
    DuckDBViewDefinition,
)
from paritygrid.adapters.analytics import views as view_module
from paritygrid.application.ports.analytics import (
    AnalyticalDatabaseConfig,
    AnalyticalDatabaseStorageError,
    AnalyticalViewConflictError,
    AnalyticalViewCorruptionError,
    AnalyticalViewInvalidError,
    AnalyticalViewName,
    AnalyticalViewSchemaError,
    AnalyticalViewVersion,
)


@pytest.fixture
def coordinator(tmp_path: Path) -> Iterator[DuckDBLifecycleCoordinator]:
    lifecycle = DuckDBLifecycleCoordinator(
        AnalyticalDatabaseConfig((tmp_path / "views.duckdb").resolve())
    )
    lifecycle.open()
    try:
        yield lifecycle
    finally:
        lifecycle.close()


def _definition(
    name: str = "pgv_numbers",
    version: int = 1,
    sql: str = "SELECT CAST(1 AS BIGINT) AS value",
    columns: tuple[DuckDBViewColumnDefinition, ...] = (
        DuckDBViewColumnDefinition("value", "BIGINT", True),
    ),
) -> DuckDBViewDefinition:
    return DuckDBViewDefinition(
        AnalyticalViewName(name), AnalyticalViewVersion(version), sql, columns
    )


def test_synchronize_installs_sorted_catalog_and_queryable_views(
    coordinator: DuckDBLifecycleCoordinator,
) -> None:
    registry = DuckDBAnalyticalViewRegistry(coordinator)
    text = _definition(
        "pgv_text",
        sql="SELECT CAST('Caf\u00e9' AS VARCHAR) AS label",
        columns=(DuckDBViewColumnDefinition("label", "VARCHAR", True),),
    )
    numbers = _definition()

    snapshot = registry.synchronize((text, numbers))

    assert tuple(str(record.name) for record in snapshot.views) == (
        "pgv_numbers",
        "pgv_text",
    )
    assert coordinator._fetch_all("SELECT * FROM pgv_numbers") == ((1,),)
    assert coordinator._fetch_all("SELECT * FROM pgv_text") == (("Caf\u00e9",),)
    assert registry.snapshot() == snapshot


def test_exact_repeat_is_read_only_and_upgrade_replaces_definition(
    coordinator: DuckDBLifecycleCoordinator,
) -> None:
    registry = DuckDBAnalyticalViewRegistry(coordinator)
    first = _definition()
    installed = registry.synchronize((first,))
    sql_before = coordinator._fetch_all(
        "SELECT sql FROM duckdb_views() WHERE view_name='pgv_numbers'"
    )

    assert registry.synchronize((first,)) == installed
    assert (
        coordinator._fetch_all("SELECT sql FROM duckdb_views() WHERE view_name='pgv_numbers'")
        == sql_before
    )

    second = _definition(  # versioned replacement
        version=2,
        sql="SELECT CAST(2 AS BIGINT) AS value",
    )
    upgraded = registry.synchronize((second,))

    assert upgraded.views[0].version == AnalyticalViewVersion(2)
    assert coordinator._fetch_all("SELECT * FROM pgv_numbers") == ((2,),)


def test_downgrade_and_divergent_same_version_are_conflicts(
    coordinator: DuckDBLifecycleCoordinator,
) -> None:
    registry = DuckDBAnalyticalViewRegistry(coordinator)
    second = _definition(version=2, sql="SELECT CAST(2 AS BIGINT) AS value")
    registry.synchronize((second,))

    with pytest.raises(AnalyticalViewConflictError, match="backward"):
        registry.synchronize((_definition(),))
    with pytest.raises(AnalyticalViewConflictError, match="different content"):
        registry.synchronize((_definition(version=2, sql="SELECT CAST(3 AS BIGINT) AS value"),))

    assert coordinator._fetch_all("SELECT * FROM pgv_numbers") == ((2,),)


def test_schema_mismatch_rolls_back_new_and_replacement_views(
    coordinator: DuckDBLifecycleCoordinator,
) -> None:
    registry = DuckDBAnalyticalViewRegistry(coordinator)
    first = _definition()
    registry.synchronize((first,))
    mismatched = _definition(
        version=2,
        sql="SELECT CAST('wrong' AS VARCHAR) AS value",
    )

    with pytest.raises(AnalyticalViewSchemaError, match="differs"):
        registry.synchronize((mismatched,))

    assert registry.snapshot().views[0].version == AnalyticalViewVersion(1)
    assert coordinator._fetch_all("SELECT * FROM pgv_numbers") == ((1,),)


def test_synchronize_removes_stale_managed_views_but_preserves_unmanaged_views(
    coordinator: DuckDBLifecycleCoordinator,
) -> None:
    registry = DuckDBAnalyticalViewRegistry(coordinator)
    registry.synchronize((_definition(), _definition("pgv_other")))
    coordinator._execute("CREATE VIEW external_view AS SELECT 1 AS x")

    snapshot = registry.synchronize((_definition("pgv_other"),))

    assert tuple(str(record.name) for record in snapshot.views) == ("pgv_other",)
    assert coordinator._fetch_all("SELECT * FROM external_view") == ((1,),)
    with pytest.raises(AnalyticalDatabaseStorageError):
        coordinator._fetch_all("SELECT * FROM pgv_numbers")


def test_rebuild_discards_all_disposable_state_and_reinstalls_catalog(
    coordinator: DuckDBLifecycleCoordinator,
) -> None:
    registry = DuckDBAnalyticalViewRegistry(coordinator)
    registry.synchronize((_definition(),))
    coordinator._execute("CREATE TABLE disposable(value INTEGER)")

    rebuilt = registry.rebuild((_definition("pgv_rebuilt"),))

    assert tuple(str(record.name) for record in rebuilt.views) == ("pgv_rebuilt",)
    assert coordinator._fetch_all(
        "SELECT count(*) FROM information_schema.tables WHERE table_name='disposable'"
    ) == ((0,),)


def test_empty_catalog_is_valid_and_removes_all_managed_views(
    coordinator: DuckDBLifecycleCoordinator,
) -> None:
    registry = DuckDBAnalyticalViewRegistry(coordinator)
    assert registry.snapshot().views == ()
    registry.synchronize((_definition(),))

    assert registry.synchronize(()).views == ()


@pytest.mark.parametrize(
    "value",
    [
        cast(tuple[DuckDBViewDefinition, ...], []),
        cast(tuple[DuckDBViewDefinition, ...], ("bad",)),
        (_definition(), _definition()),
    ],
)
def test_catalog_input_requires_tuple_typed_unique_definitions(
    coordinator: DuckDBLifecycleCoordinator,
    value: tuple[DuckDBViewDefinition, ...],
) -> None:
    with pytest.raises((TypeError, AnalyticalViewInvalidError)):
        DuckDBAnalyticalViewRegistry(coordinator).synchronize(value)


@pytest.mark.parametrize(
    "kwargs",
    [
        {"name": "pgv_numbers"},
        {"version": 1},
        {"select_sql": 1},
        {"select_sql": ""},
        {"select_sql": " SELECT 1"},
        {"select_sql": "DELETE FROM x"},
        {"select_sql": "SELECT 1; DROP TABLE x"},
        {"select_sql": "SELECT '\x00'"},
        {"columns": []},
        {"columns": ("value",)},
        {
            "columns": (
                DuckDBViewColumnDefinition("value", "BIGINT", True),
                DuckDBViewColumnDefinition("value", "BIGINT", True),
            )
        },
    ],
)
def test_definition_rejects_untyped_or_unsafe_values(kwargs: dict[str, object]) -> None:
    values: dict[str, object] = {
        "name": AnalyticalViewName("pgv_numbers"),
        "version": AnalyticalViewVersion(1),
        "select_sql": "SELECT CAST(1 AS BIGINT) AS value",
        "columns": (DuckDBViewColumnDefinition("value", "BIGINT", True),),
    }
    values.update(kwargs)
    with pytest.raises((TypeError, AnalyticalViewInvalidError)):
        DuckDBViewDefinition(**cast(Any, values))


@pytest.mark.parametrize(
    ("name", "type_name", "nullable"),
    [
        ("Bad", "BIGINT", True),
        ("bad-name", "BIGINT", True),
        ("x" * 129, "BIGINT", True),
        (cast(str, 1), "BIGINT", True),
        ("value", "bigint", True),
        ("value", "BIGINT;DROP", True),
        ("value", "X" * 129, True),
        ("value", cast(str, 1), True),
        ("value", "BIGINT", cast(bool, 1)),
    ],
)
def test_column_definition_rejects_noncanonical_values(
    name: str, type_name: str, nullable: bool
) -> None:
    with pytest.raises((TypeError, AnalyticalViewInvalidError)):
        DuckDBViewColumnDefinition(name, type_name, nullable)


def test_registry_constructor_requires_exact_lifecycle() -> None:
    with pytest.raises(TypeError, match="lifecycle"):
        DuckDBAnalyticalViewRegistry(cast(DuckDBLifecycleCoordinator, object()))


def test_digests_and_schema_json_are_deterministic_golden_values() -> None:
    definition = _definition()

    assert definition.definition_sha256 == (
        "63b8c2e4820e1968c2a5f9af56ade89508569596f724e1dc8214e01595ec33c5"
    )
    assert definition.output_schema_json == ('[{"name":"value","nullable":true,"type":"BIGINT"}]')
    assert definition.output_schema_sha256 == (
        "776da80d71836115bbe332983d4ed10dfb7fbebfb150bf382f237ec340d85e11"
    )


def test_orphan_managed_view_without_registry_is_corruption(
    coordinator: DuckDBLifecycleCoordinator,
) -> None:
    coordinator._execute("CREATE VIEW pgv_orphan AS SELECT 1 AS value")

    with pytest.raises(AnalyticalViewCorruptionError, match="without a registry"):
        DuckDBAnalyticalViewRegistry(coordinator).snapshot()


def test_missing_registered_view_and_extra_managed_view_are_corruption(
    coordinator: DuckDBLifecycleCoordinator,
) -> None:
    registry = DuckDBAnalyticalViewRegistry(coordinator)
    registry.synchronize((_definition(),))
    coordinator._execute("DROP VIEW pgv_numbers")

    with pytest.raises(AnalyticalViewCorruptionError, match="inventory"):
        registry.snapshot()

    coordinator._execute("CREATE VIEW pgv_numbers AS SELECT 1 AS value")
    coordinator._execute("CREATE VIEW pgv_extra AS SELECT 2 AS value")
    with pytest.raises(AnalyticalViewCorruptionError, match="inventory"):
        registry.snapshot()


def test_tampered_view_sql_or_schema_is_corruption(
    coordinator: DuckDBLifecycleCoordinator,
) -> None:
    registry = DuckDBAnalyticalViewRegistry(coordinator)
    registry.synchronize((_definition(),))
    coordinator._execute("CREATE OR REPLACE VIEW pgv_numbers AS SELECT 2::BIGINT AS value")

    with pytest.raises(AnalyticalViewCorruptionError, match="definition differs"):
        registry.snapshot()

    registry.rebuild((_definition(),))
    coordinator._execute("CREATE OR REPLACE VIEW pgv_numbers AS SELECT 'x'::VARCHAR AS value")
    with pytest.raises(AnalyticalViewCorruptionError, match="schema differs"):
        registry.snapshot()


@pytest.mark.parametrize(
    ("column", "value", "message"),
    [
        ("definition_version", 0, "identity"),
        ("definition_sha256", "BAD", "metadata"),
        ("installed_sql_sha256", "BAD", "metadata"),
        ("output_schema_json", "not-json", "schema"),
        ("output_schema_json", "[]", "schema"),
        ("output_schema_json", '[{"name":"value","nullable":true}]', "schema"),
        (
            "output_schema_json",
            '[{"type":"BIGINT","nullable":true,"name":"value"}]',
            "canonical",
        ),
    ],
)
def test_tampered_registry_rows_are_strict_corruption(
    coordinator: DuckDBLifecycleCoordinator,
    column: str,
    value: object,
    message: str,
) -> None:
    registry = DuckDBAnalyticalViewRegistry(coordinator)
    registry.synchronize((_definition(),))
    coordinator._execute(f"UPDATE paritygrid_analytical_view_registry SET {column} = ?", (value,))

    with pytest.raises(AnalyticalViewCorruptionError, match=message):
        registry.snapshot()


def test_wrong_registry_relation_kind_or_shape_is_corruption(
    coordinator: DuckDBLifecycleCoordinator,
) -> None:
    coordinator._execute("CREATE VIEW paritygrid_analytical_view_registry AS SELECT 1 AS wrong")
    registry = DuckDBAnalyticalViewRegistry(coordinator)
    with pytest.raises(AnalyticalViewCorruptionError, match="wrong kind"):
        registry.snapshot()
    coordinator._execute("DROP VIEW paritygrid_analytical_view_registry")
    coordinator._execute("CREATE TABLE paritygrid_analytical_view_registry(wrong INTEGER)")
    with pytest.raises(AnalyticalViewCorruptionError, match="wrong schema"):
        registry.synchronize(())


def test_failed_second_install_rolls_back_complete_catalog(
    monkeypatch: pytest.MonkeyPatch,
    coordinator: DuckDBLifecycleCoordinator,
) -> None:
    registry = DuckDBAnalyticalViewRegistry(coordinator)
    original = DuckDBAnalyticalViewRegistry._install
    count = 0

    def fail_second(self: DuckDBAnalyticalViewRegistry, definition: DuckDBViewDefinition) -> None:
        nonlocal count
        count += 1
        if count == 2:
            raise RuntimeError("stop")
        original(self, definition)

    monkeypatch.setattr(DuckDBAnalyticalViewRegistry, "_install", fail_second)
    with pytest.raises(RuntimeError, match="stop"):
        registry.synchronize((_definition(), _definition("pgv_other")))

    assert registry.snapshot().views == ()


def test_helper_corruption_defenses_reject_malformed_driver_rows() -> None:
    with pytest.raises(AnalyticalViewCorruptionError, match="schema row"):
        view_module._column_from_describe(("value", "BIGINT"))
    with pytest.raises(AnalyticalViewCorruptionError, match="schema row"):
        view_module._column_from_describe((1, "BIGINT", "YES"))
    with pytest.raises(AnalyticalViewCorruptionError, match="schema row"):
        view_module._column_from_describe(("", "BIGINT", "YES"))
    with pytest.raises(AnalyticalViewCorruptionError, match="schema"):
        view_module._decode_schema('["bad"]')


def test_private_read_defenses_reject_malformed_driver_inventories(
    monkeypatch: pytest.MonkeyPatch,
    coordinator: DuckDBLifecycleCoordinator,
) -> None:
    registry = DuckDBAnalyticalViewRegistry(coordinator)
    registry.synchronize((_definition(),))

    with pytest.raises(AnalyticalViewCorruptionError, match="row is malformed"):
        registry._record_from_row(("short",))

    original_fetch = DuckDBLifecycleCoordinator._fetch_all

    def malformed_registry(
        self: DuckDBLifecycleCoordinator,
        statement: str,
        parameters: object = None,
    ) -> tuple[tuple[object, ...], ...]:
        if statement.startswith("SELECT view_name, definition_version"):
            return ((1, 1, "a" * 64, "b" * 64, "[]"),)
        return original_fetch(self, statement, cast(Any, parameters))

    monkeypatch.setattr(DuckDBLifecycleCoordinator, "_fetch_all", malformed_registry)
    with pytest.raises(AnalyticalViewCorruptionError, match="row is malformed"):
        registry.snapshot()


def test_private_catalog_helpers_reject_malformed_engine_results(
    monkeypatch: pytest.MonkeyPatch,
    coordinator: DuckDBLifecycleCoordinator,
) -> None:
    registry = DuckDBAnalyticalViewRegistry(coordinator)
    original_fetch = DuckDBLifecycleCoordinator._fetch_all
    result: tuple[tuple[object, ...], ...] = ()

    def injected_fetch(
        self: DuckDBLifecycleCoordinator,
        statement: str,
        parameters: object = None,
    ) -> tuple[tuple[object, ...], ...]:
        del self, statement, parameters
        return result

    monkeypatch.setattr(DuckDBLifecycleCoordinator, "_fetch_all", injected_fetch)
    with pytest.raises(AnalyticalViewCorruptionError, match="definition is unavailable"):
        registry._installed_sql(AnalyticalViewName("pgv_a"))
    result = ((1,),)
    with pytest.raises(AnalyticalViewCorruptionError, match="inventory is malformed"):
        registry._managed_view_names()
    with pytest.raises(AnalyticalViewCorruptionError, match="relation inventory"):
        registry._relation_type("anything")

    coordinator._execute("CREATE TABLE paritygrid_analytical_view_registry(x INTEGER)")

    def malformed_shape(
        self: DuckDBLifecycleCoordinator,
        statement: str,
        parameters: object = None,
    ) -> tuple[tuple[object, ...], ...]:
        if "information_schema.tables" in statement:
            return (("BASE TABLE",),)
        if statement.startswith("DESCRIBE"):
            return (("value", "BIGINT"),)
        return original_fetch(self, statement, cast(Any, parameters))

    monkeypatch.setattr(DuckDBLifecycleCoordinator, "_fetch_all", malformed_shape)
    with pytest.raises(AnalyticalViewCorruptionError, match="wrong schema"):
        registry._ensure_registry_table()


def test_describe_translates_query_failure_and_preserves_schema_corruption(
    monkeypatch: pytest.MonkeyPatch,
    coordinator: DuckDBLifecycleCoordinator,
) -> None:
    registry = DuckDBAnalyticalViewRegistry(coordinator)

    def fail_fetch(
        _self: DuckDBLifecycleCoordinator,
        _statement: str,
        _parameters: object = None,
    ) -> tuple[tuple[object, ...], ...]:
        raise AnalyticalDatabaseStorageError("bounded")

    monkeypatch.setattr(DuckDBLifecycleCoordinator, "_fetch_all", fail_fetch)
    with pytest.raises(AnalyticalViewCorruptionError, match="could not be described"):
        registry._describe_view(AnalyticalViewName("pgv_a"))

    def malformed_fetch(
        _self: DuckDBLifecycleCoordinator,
        _statement: str,
        _parameters: object = None,
    ) -> tuple[tuple[object, ...], ...]:
        return (("value", "BIGINT"),)

    monkeypatch.setattr(DuckDBLifecycleCoordinator, "_fetch_all", malformed_fetch)
    with pytest.raises(AnalyticalViewCorruptionError, match="schema row"):
        registry._describe_view(AnalyticalViewName("pgv_a"))


def test_invalid_managed_view_name_is_corruption(
    coordinator: DuckDBLifecycleCoordinator,
) -> None:
    coordinator._execute('CREATE VIEW "pgv_Bad" AS SELECT 1 AS value')

    with pytest.raises(AnalyticalViewCorruptionError, match="name is malformed"):
        DuckDBAnalyticalViewRegistry(coordinator).snapshot()


def test_ensure_registry_rejects_relation_kind_directly(
    coordinator: DuckDBLifecycleCoordinator,
) -> None:
    coordinator._execute("CREATE VIEW paritygrid_analytical_view_registry AS SELECT 1 AS wrong")

    with pytest.raises(AnalyticalViewCorruptionError, match="wrong kind"):
        DuckDBAnalyticalViewRegistry(coordinator)._ensure_registry_table()


def test_registry_source_never_uses_drop_table_or_unbounded_query_results() -> None:
    source = Path("src/paritygrid/adapters/analytics/views.py").read_text(encoding="utf-8")
    assert "DROP TABLE" not in source
    assert "read_parquet" not in source
    assert "fetch_df" not in source

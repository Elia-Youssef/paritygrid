"""Behavioral tests for pipeline and connector configuration repositories."""

from __future__ import annotations

import sqlite3
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
from datetime import UTC, datetime
from pathlib import Path
from threading import Barrier
from types import MethodType
from typing import Any, NoReturn, cast

import pytest
from sqlalchemy import event, func, insert, select, text, update
from sqlalchemy.engine import Connection, Result
from sqlalchemy.exc import IntegrityError, OperationalError, SQLAlchemyError
from sqlalchemy.orm import Session
from sqlalchemy.sql import Executable

from paritygrid.adapters.persistence import (
    SQLiteDatabase,
    SQLiteDatabaseConfig,
    create_session_factory,
)
from paritygrid.adapters.persistence.migration import upgrade_to_head
from paritygrid.adapters.persistence.repositories import (
    SqlAlchemyConnectorRepository,
    SqlAlchemyPipelineRepository,
)
from paritygrid.adapters.persistence.schema import (
    connector_secret_references,
    connectors,
    pipeline_versions,
)
from paritygrid.application.ports import (
    MAX_PAGE_SIZE,
    ConfigurationDocument,
    ConfigurationStorageError,
    ConfigurationStorageUnavailableError,
    ConnectorRecord,
    ConnectorSecretReference,
    CorruptRepositoryRecordError,
    DuplicateRecordError,
    InvalidRepositoryRequestError,
    PipelineRecord,
    PipelineVersionConflictError,
    RecordNotFoundError,
    RecordStateConflictError,
    StaleConnectorRevisionError,
    StaleRowVersionError,
    UnsafeConnectorConfigurationError,
)
from paritygrid.domain.models import ConnectorId, PipelineId, PipelineVersion, UtcTimestamp

_DEFAULT_PIPELINE_ID = PipelineId("pip_configuration")
_DEFAULT_CONNECTOR_ID = ConnectorId("con_inventory")


@pytest.fixture
def database(tmp_path: Path) -> Iterator[SQLiteDatabase]:
    database = SQLiteDatabase.open(
        SQLiteDatabaseConfig(database_path=tmp_path / "config بيانات %25.db")
    )
    with database.engine.connect() as connection:
        upgrade_to_head(connection)
    try:
        yield database
    finally:
        database.close()


def timestamp(second: int) -> UtcTimestamp:
    return UtcTimestamp(datetime(2026, 8, 12, 12, 0, second, tzinfo=UTC))


def document(**values: object) -> ConfigurationDocument:
    return ConfigurationDocument.from_mapping(values)


def create_pipeline(
    repository: SqlAlchemyPipelineRepository,
    pipeline_id: PipelineId = _DEFAULT_PIPELINE_ID,
) -> PipelineRecord:
    return repository.create(
        pipeline_id=pipeline_id,
        display_name="مزامنة inventory",
        description="Canonical configuration",
        created_at=timestamp(0),
    )


def create_connector(
    repository: SqlAlchemyConnectorRepository,
    connector_id: ConnectorId = _DEFAULT_CONNECTOR_ID,
) -> ConnectorRecord:
    return repository.create(
        connector_id=connector_id,
        kind="inventory-http",
        display_name="Inventory source",
        configuration=document(
            api_token_reference="primary.token",
            endpoint="https://inventory.invalid",
            labels=["مستودع", "east"],
        ),
        capabilities=document(read=True, write=False),
        schema_discovery=None,
        secret_references=(ConnectorSecretReference("primary.token", "INVENTORY_API_TOKEN"),),
        created_at=timestamp(0),
    )


def test_document_is_normalized_immutable_detached_and_redacted() -> None:
    source: dict[str, object] = {"z": ["é"], "a": {"b": 1}}
    value = ConfigurationDocument.from_mapping(source)
    source["z"] = ["changed"]

    assert value.to_mapping() == {"a": {"b": 1}, "z": ["é"]}
    assert repr(value) == "ConfigurationDocument(fields=2)"
    with pytest.raises(TypeError):
        ConfigurationDocument.from_mapping({"bad": 1.5})
    with pytest.raises(ValueError, match="normalized Unicode"):
        ConfigurationDocument.from_mapping({"bad": "e\u0301"})
    with pytest.raises(ValueError, match="canonical and sorted"):
        ConfigurationDocument(items=(("z", 1), ("a", 2)))
    nested: object = "leaf"
    for _index in range(34):
        nested = [nested]
    with pytest.raises(ValueError, match="maximum nesting depth"):
        ConfigurationDocument.from_mapping({"nested": nested})
    with pytest.raises(ValueError, match="supported length"):
        ConfigurationDocument.from_mapping({"key": "x" * 65_537})


def test_pipeline_lifecycle_publication_replay_and_restart(database: SQLiteDatabase) -> None:
    pipeline_id = PipelineId("pip_configuration")
    specification = document(nodes=[{"id": "source"}], title="جرد")
    with database.transaction() as session:
        repository = SqlAlchemyPipelineRepository(session)
        created = create_pipeline(repository)
        published = repository.publish_version(
            pipeline_id=pipeline_id,
            expected_latest_version=None,
            specification=specification,
            planner_format_version=1,
            published_at=timestamp(1),
        )
        replay = repository.publish_version(
            pipeline_id=pipeline_id,
            expected_latest_version=None,
            specification=specification,
            planner_format_version=1,
            published_at=timestamp(1),
        )
        assert replay == published
        assert created.row_version == 1
        assert published.specification_sha256 == (
            "a78079ea0acfec841a167331d920e578938fe85b5ac319e93efe271305512d57"
        )
        assert "nodes" not in repr(published)

    with database.transaction() as session:
        repository = SqlAlchemyPipelineRepository(session)
        assert repository.get(pipeline_id) == created
        assert repository.get_version(pipeline_id, PipelineVersion(1)) == published
        second = repository.publish_version(
            pipeline_id=pipeline_id,
            expected_latest_version=PipelineVersion(1),
            specification=document(nodes=[]),
            planner_format_version=1,
            published_at=timestamp(2),
        )
        assert second.version == PipelineVersion(2)


@pytest.mark.parametrize(
    ("change", "value"),
    [
        ("specification", document(nodes=[{"id": "different"}])),
        ("planner_format_version", 2),
        ("published_at", timestamp(2)),
    ],
)
def test_pipeline_replay_requires_every_field(
    database: SQLiteDatabase, change: str, value: object
) -> None:
    specification = document(nodes=[])
    with database.transaction() as session:
        repository = SqlAlchemyPipelineRepository(session)
        create_pipeline(repository)
        repository.publish_version(
            pipeline_id=PipelineId("pip_configuration"),
            expected_latest_version=None,
            specification=specification,
            planner_format_version=1,
            published_at=timestamp(1),
        )
        arguments: dict[str, object] = {
            "pipeline_id": PipelineId("pip_configuration"),
            "expected_latest_version": None,
            "specification": specification,
            "planner_format_version": 1,
            "published_at": timestamp(1),
        }
        arguments[change] = value
        with pytest.raises(PipelineVersionConflictError):
            repository.publish_version(
                pipeline_id=cast(PipelineId, arguments["pipeline_id"]),
                expected_latest_version=cast(
                    PipelineVersion | None, arguments["expected_latest_version"]
                ),
                specification=cast(ConfigurationDocument, arguments["specification"]),
                planner_format_version=cast(int, arguments["planner_format_version"]),
                published_at=cast(UtcTimestamp, arguments["published_at"]),
            )


def test_pipeline_conflicts_pagination_archive_and_stale_cas(database: SQLiteDatabase) -> None:
    with database.transaction() as session:
        repository = SqlAlchemyPipelineRepository(session)
        first = create_pipeline(repository, PipelineId("pip_alpha"))
        create_pipeline(repository, PipelineId("pip_beta"))
        with pytest.raises(DuplicateRecordError):
            create_pipeline(repository, PipelineId("pip_alpha"))
        assert repository.get(PipelineId("pip_beta")) is not None
        page = repository.list(limit=1)
        assert [str(item.pipeline_id) for item in page.items] == ["pip_alpha"]
        assert page.next_cursor == PipelineId("pip_alpha")
        assert repository.list(limit=1, after=page.next_cursor).items[0].pipeline_id == (
            PipelineId("pip_beta")
        )
        with pytest.raises(PipelineVersionConflictError):
            repository.publish_version(
                pipeline_id=first.pipeline_id,
                expected_latest_version=PipelineVersion(1),
                specification=document(nodes=[]),
                planner_format_version=1,
                published_at=timestamp(1),
            )
        unchanged = repository.update_metadata(
            first.pipeline_id,
            expected_row_version=1,
            display_name=first.display_name,
            description=first.description,
        )
        assert unchanged == first
        changed = repository.update_metadata(
            first.pipeline_id,
            expected_row_version=1,
            display_name="Renamed pipeline",
            description=None,
        )
        assert changed.display_name == "Renamed pipeline"
        assert changed.description is None
        assert changed.row_version == 2
    with database.transaction() as session:
        archived = SqlAlchemyPipelineRepository(session).archive(
            PipelineId("pip_alpha"), expected_row_version=2, archived_at=timestamp(2)
        )
        assert archived.row_version == 3
    with database.transaction() as session:
        repository = SqlAlchemyPipelineRepository(session)
        assert [item.pipeline_id for item in repository.list(limit=10).items] == [
            PipelineId("pip_beta")
        ]
        with pytest.raises(RecordStateConflictError):
            repository.archive(
                PipelineId("pip_alpha"), expected_row_version=2, archived_at=timestamp(3)
            )
        with pytest.raises(RecordStateConflictError):
            repository.update_metadata(
                PipelineId("pip_alpha"),
                expected_row_version=3,
                display_name="Archived",
                description=None,
            )
        with pytest.raises(StaleRowVersionError):
            repository.archive(
                PipelineId("pip_beta"), expected_row_version=2, archived_at=timestamp(3)
            )
        with pytest.raises(RecordNotFoundError):
            repository.archive(
                PipelineId("pip_missing"), expected_row_version=1, archived_at=timestamp(3)
            )


def test_pipeline_requires_caller_transaction_and_bounded_pages(
    database: SQLiteDatabase,
) -> None:
    session = create_session_factory(database.engine)()
    try:
        repository = SqlAlchemyPipelineRepository(session)
        with pytest.raises(InvalidRepositoryRequestError):
            repository.get(PipelineId("pip_configuration"))
    finally:
        session.close()
    with database.transaction() as session:
        repository = SqlAlchemyPipelineRepository(session)
        with pytest.raises(InvalidRepositoryRequestError):
            repository.list(limit=0)
        with pytest.raises(InvalidRepositoryRequestError):
            repository.list(limit=True)


def test_pipeline_corrupt_digest_is_rejected(database: SQLiteDatabase) -> None:
    with database.transaction() as session:
        create_pipeline(SqlAlchemyPipelineRepository(session))
        session.execute(
            insert(pipeline_versions).values(
                pipeline_id="pip_configuration",
                version_number=1,
                specification_json='{"nodes":[]}',
                specification_sha256="0" * 64,
                planner_format_version=1,
                published_at=str(timestamp(1)),
            )
        )
    with database.transaction() as session, pytest.raises(CorruptRepositoryRecordError):
        SqlAlchemyPipelineRepository(session).get_version(
            PipelineId("pip_configuration"), PipelineVersion(1)
        )


def test_connector_create_round_trip_canonical_storage_and_redaction(
    database: SQLiteDatabase,
) -> None:
    with database.transaction() as session:
        record = create_connector(SqlAlchemyConnectorRepository(session))
        stored = session.execute(
            select(
                connectors.c.configuration_json,
                connector_secret_references.c.environment_variable_name,
            ).join(connector_secret_references)
        ).one()
        assert stored.configuration_json == (
            r'{"api_token_reference":"primary.token",'
            r'"endpoint":"https://inventory.invalid","labels":["\u0645\u0633\u062a\u0648\u062f\u0639","east"]}'
        )
        assert stored.environment_variable_name == "INVENTORY_API_TOKEN"
        assert "INVENTORY_API_TOKEN" not in repr(record)
        assert "inventory.invalid" not in repr(record)
        assert not hasattr(record, "_sa_instance_state")

    with database.transaction() as session:
        assert SqlAlchemyConnectorRepository(session).get(record.connector_id) == record


def test_connector_metadata_definition_and_archive_versions(database: SQLiteDatabase) -> None:
    with database.transaction() as session:
        repository = SqlAlchemyConnectorRepository(session)
        created = create_connector(repository)
        metadata = repository.update_metadata(
            created.connector_id,
            expected_row_version=1,
            display_name="Renamed inventory",
            updated_at=timestamp(1),
        )
        assert (metadata.revision, metadata.row_version) == (1, 2)
        definition = repository.update_definition(
            created.connector_id,
            expected_row_version=2,
            expected_revision=1,
            kind=created.kind,
            configuration=document(api_token_reference="primary.token", page_size=100),
            capabilities=document(read=True, write=True),
            schema_discovery=document(tables=["inventory"]),
            updated_at=timestamp(2),
        )
        assert (definition.revision, definition.row_version) == (2, 3)
        archived = repository.archive(
            created.connector_id,
            expected_row_version=3,
            archived_at=timestamp(3),
        )
        assert (archived.revision, archived.row_version) == (2, 4)
        assert archived.archived_at == timestamp(3)
        assert repository.list(limit=10).items == ()
        assert repository.list(limit=10, include_archived=True).items == (archived,)


def test_connector_cas_kind_and_state_conflicts(database: SQLiteDatabase) -> None:
    connector_id = ConnectorId("con_inventory")
    with database.transaction() as session:
        create_connector(SqlAlchemyConnectorRepository(session))
    with database.transaction() as session:
        repository = SqlAlchemyConnectorRepository(session)
        with pytest.raises(RecordStateConflictError):
            repository.update_definition(
                connector_id,
                expected_row_version=1,
                expected_revision=1,
                kind="changed-kind",
                configuration=document(api_token_reference="primary.token"),
                capabilities=document(),
                schema_discovery=None,
                updated_at=timestamp(1),
            )
        with pytest.raises(StaleRowVersionError):
            repository.update_metadata(
                connector_id,
                expected_row_version=2,
                display_name="stale",
                updated_at=timestamp(1),
            )
        with pytest.raises(StaleConnectorRevisionError):
            repository.update_definition(
                connector_id,
                expected_row_version=1,
                expected_revision=2,
                kind="inventory-http",
                configuration=document(api_token_reference="primary.token"),
                capabilities=document(),
                schema_discovery=None,
                updated_at=timestamp(1),
            )
    with database.transaction() as session:
        repository = SqlAlchemyConnectorRepository(session)
        repository.archive(connector_id, expected_row_version=1, archived_at=timestamp(2))
        with pytest.raises(RecordStateConflictError):
            repository.update_metadata(
                connector_id,
                expected_row_version=2,
                display_name="too late",
                updated_at=timestamp(3),
            )


@pytest.mark.parametrize(
    "configuration",
    [
        document(password="resolved-value"),
        document(api_token="resolved-value"),
        document(api_token_reference="missing-reference"),
        document(nested={"private_key": "resolved-value"}),
    ],
)
def test_connector_rejects_resolved_or_undeclared_secrets_without_leakage(
    database: SQLiteDatabase, configuration: ConfigurationDocument
) -> None:
    candidate = "resolved-value"
    with database.transaction() as session:
        repository = SqlAlchemyConnectorRepository(session)
        with pytest.raises(UnsafeConnectorConfigurationError) as caught:
            repository.create(
                connector_id=ConnectorId("con_inventory"),
                kind="inventory-http",
                display_name="Inventory source",
                configuration=configuration,
                capabilities=document(),
                schema_discovery=None,
                secret_references=(
                    ConnectorSecretReference("primary.token", "INVENTORY_API_TOKEN"),
                ),
                created_at=timestamp(0),
            )
        with pytest.raises(InvalidRepositoryRequestError, match="canonical"):
            repository.create(
                connector_id=ConnectorId("con_inventory"),
                kind="inventory-http",
                display_name="Inventory",
                configuration=document(),
                capabilities=document(),
                schema_discovery=None,
                secret_references=(ConnectorSecretReference("Bad Name", "TOKEN"),),
                created_at=timestamp(0),
            )
        assert candidate not in str(caught.value)
        assert candidate not in repr(caught.value)
        assert caught.value.__cause__ is None
        assert caught.value.__context__ is None
        assert session.execute(select(func.count()).select_from(connectors)).scalar_one() == 0
    database_path = Path(cast(str, database.engine.url.database))
    for suffix in ("", "-wal", "-shm"):
        candidate_path = Path(f"{database_path}{suffix}")
        if candidate_path.exists():
            assert candidate.encode("utf-8") not in candidate_path.read_bytes()


def test_connector_duplicate_and_transaction_rollback_are_atomic(database: SQLiteDatabase) -> None:
    connector_id = ConnectorId("con_inventory")

    def abort_transaction() -> None:
        with database.transaction() as session:
            create_connector(SqlAlchemyConnectorRepository(session))
            raise RuntimeError("abort caller transaction")

    with pytest.raises(RuntimeError):
        abort_transaction()
    with database.transaction() as session:
        repository = SqlAlchemyConnectorRepository(session)
        assert repository.get(connector_id) is None
        created = create_connector(repository)
        with pytest.raises(DuplicateRecordError):
            create_connector(repository)
        assert repository.get(connector_id) == created


def test_connector_mid_write_failure_rolls_back_all_rows(database: SQLiteDatabase) -> None:
    class ForcedInterruption(BaseException):
        pass

    def fail_reference_insert(
        _connection: Connection,
        _cursor: object,
        statement: str,
        _parameters: object,
        _context: object,
        _executemany: bool,
    ) -> None:
        if "INSERT INTO connector_secret_references" in statement:
            raise ForcedInterruption("simulated persistence interruption")

    event.listen(database.engine, "before_cursor_execute", fail_reference_insert)
    try:
        with (
            pytest.raises(ForcedInterruption, match="persistence interruption"),
            database.transaction() as session,
        ):
            SqlAlchemyConnectorRepository(session).create(
                connector_id=ConnectorId("con_inventory"),
                kind="inventory-http",
                display_name="Inventory",
                configuration=document(
                    api_token_reference="primary.token",
                    backup_token_reference="backup.token",
                ),
                capabilities=document(),
                schema_discovery=None,
                secret_references=(
                    ConnectorSecretReference("primary.token", "PRIMARY_TOKEN"),
                    ConnectorSecretReference("backup.token", "BACKUP_TOKEN"),
                ),
                created_at=timestamp(0),
            )
    finally:
        event.remove(database.engine, "before_cursor_execute", fail_reference_insert)

    with database.transaction() as session:
        assert SqlAlchemyConnectorRepository(session).get(ConnectorId("con_inventory")) is None
        assert session.execute(select(func.count()).select_from(connectors)).scalar_one() == 0
        assert (
            session.execute(
                select(func.count()).select_from(connector_secret_references)
            ).scalar_one()
            == 0
        )


def test_connector_pagination_reference_order_and_direct_secret_mutation_guard(
    database: SQLiteDatabase,
) -> None:
    with database.transaction() as session:
        repository = SqlAlchemyConnectorRepository(session)
        first = create_connector(repository, ConnectorId("con_alpha"))
        create_connector(repository, ConnectorId("con_beta"))
        page = repository.list(limit=1)
        assert page.items[0].connector_id == first.connector_id
        assert page.next_cursor == first.connector_id
        assert repository.list(limit=1, after=page.next_cursor).items[0].connector_id == (
            ConnectorId("con_beta")
        )
        with pytest.raises(IntegrityError, match="does not permit update"):
            session.execute(
                update(connector_secret_references)
                .where(connector_secret_references.c.connector_id == "con_alpha")
                .values(environment_variable_name="CHANGED")
            )


def test_connector_corrupt_canonical_json_is_rejected(database: SQLiteDatabase) -> None:
    with database.transaction() as session:
        create_connector(SqlAlchemyConnectorRepository(session))
        session.execute(text("PRAGMA ignore_check_constraints = ON"))
        session.execute(
            update(connectors)
            .where(connectors.c.connector_id == "con_inventory")
            .values(capabilities_json='{ "read": true }')
        )
        session.execute(text("PRAGMA ignore_check_constraints = OFF"))
    with database.transaction() as session, pytest.raises(CorruptRepositoryRecordError):
        SqlAlchemyConnectorRepository(session).get(ConnectorId("con_inventory"))


def test_repositories_return_only_public_records(database: SQLiteDatabase) -> None:
    with database.transaction() as session:
        pipeline = create_pipeline(SqlAlchemyPipelineRepository(session))
        connector = create_connector(SqlAlchemyConnectorRepository(session))
        assert type(pipeline) is PipelineRecord
        assert type(connector) is ConnectorRecord
        assert not hasattr(pipeline, "_mapping")
        assert not hasattr(connector, "_mapping")


def test_pipeline_version_listing_missing_archive_and_time_guards(
    database: SQLiteDatabase,
) -> None:
    pipeline_id = PipelineId("pip_configuration")
    with database.transaction() as session:
        repository = SqlAlchemyPipelineRepository(session)
        with pytest.raises(RecordNotFoundError):
            repository.publish_version(
                pipeline_id=pipeline_id,
                expected_latest_version=None,
                specification=document(nodes=[]),
                planner_format_version=1,
                published_at=timestamp(1),
            )
        create_pipeline(repository)
        for version in (1, 2):
            repository.publish_version(
                pipeline_id=pipeline_id,
                expected_latest_version=(None if version == 1 else PipelineVersion(1)),
                specification=document(version=version),
                planner_format_version=1,
                published_at=timestamp(version),
            )
        first = repository.list_versions(pipeline_id, limit=1)
        assert first.next_cursor == PipelineVersion(1)
        second = repository.list_versions(pipeline_id, limit=1, after=first.next_cursor)
        assert second.items[0].version == PipelineVersion(2)
        assert second.next_cursor is None
        assert repository.list(limit=MAX_PAGE_SIZE, include_archived=True).items
        with pytest.raises(InvalidRepositoryRequestError, match="later than creation"):
            repository.archive(pipeline_id, expected_row_version=1, archived_at=timestamp(0))
        repository.archive(pipeline_id, expected_row_version=1, archived_at=timestamp(3))
        with pytest.raises(RecordStateConflictError, match="archived pipeline"):
            repository.publish_version(
                pipeline_id=pipeline_id,
                expected_latest_version=PipelineVersion(2),
                specification=document(version=3),
                planner_format_version=1,
                published_at=timestamp(4),
            )


def test_pipeline_publication_cannot_precede_creation(database: SQLiteDatabase) -> None:
    pipeline_id = PipelineId("pip_future")
    with database.transaction() as session:
        repository = SqlAlchemyPipelineRepository(session)
        repository.create(
            pipeline_id=pipeline_id,
            display_name="Future",
            description=None,
            created_at=timestamp(2),
        )
        with pytest.raises(InvalidRepositoryRequestError, match="cannot precede"):
            repository.publish_version(
                pipeline_id=pipeline_id,
                expected_latest_version=None,
                specification=document(),
                planner_format_version=1,
                published_at=timestamp(1),
            )
        first = repository.list_versions(pipeline_id, limit=1)
        assert first.items == ()
        assert first.next_cursor is None


def test_pipeline_stale_expected_latest_conflicts_and_exact_retry_converges(
    database: SQLiteDatabase,
) -> None:
    pipeline_id = PipelineId("pip_configuration")
    specification = document(nodes=[])
    with database.transaction() as session:
        repository = SqlAlchemyPipelineRepository(session)
        create_pipeline(repository)
        installed = repository.publish_version(
            pipeline_id=pipeline_id,
            expected_latest_version=None,
            specification=specification,
            planner_format_version=1,
            published_at=timestamp(1),
        )
    with database.transaction() as session:
        repository = SqlAlchemyPipelineRepository(session)
        assert (
            repository.publish_version(
                pipeline_id=pipeline_id,
                expected_latest_version=None,
                specification=specification,
                planner_format_version=1,
                published_at=timestamp(1),
            )
            == installed
        )
        with pytest.raises(PipelineVersionConflictError, match="does not match"):
            repository.publish_version(
                pipeline_id=pipeline_id,
                expected_latest_version=None,
                specification=document(nodes=[{"id": "different"}]),
                planner_format_version=1,
                published_at=timestamp(1),
            )
        repository.publish_version(
            pipeline_id=pipeline_id,
            expected_latest_version=PipelineVersion(1),
            specification=document(nodes=[{"id": "second"}]),
            planner_format_version=1,
            published_at=timestamp(2),
        )
        with pytest.raises(PipelineVersionConflictError, match="stale"):
            repository.publish_version(
                pipeline_id=pipeline_id,
                expected_latest_version=None,
                specification=specification,
                planner_format_version=1,
                published_at=timestamp(1),
            )


def test_connector_no_op_and_timestamp_guards(database: SQLiteDatabase) -> None:
    connector_id = ConnectorId("con_inventory")
    with database.transaction() as session:
        repository = SqlAlchemyConnectorRepository(session)
        created = create_connector(repository)
        assert (
            repository.update_metadata(
                connector_id,
                expected_row_version=1,
                display_name=created.display_name,
                updated_at=created.updated_at,
            )
            == created
        )
        assert (
            repository.update_definition(
                connector_id,
                expected_row_version=1,
                expected_revision=1,
                kind=created.kind,
                configuration=created.configuration,
                capabilities=created.capabilities,
                schema_discovery=created.schema_discovery,
                updated_at=created.updated_at,
            )
            == created
        )
        with pytest.raises(InvalidRepositoryRequestError, match="later than"):
            repository.update_metadata(
                connector_id,
                expected_row_version=1,
                display_name="changed",
                updated_at=created.updated_at,
            )
        with pytest.raises(InvalidRepositoryRequestError, match="later than"):
            repository.update_definition(
                connector_id,
                expected_row_version=1,
                expected_revision=1,
                kind=created.kind,
                configuration=document(api_token_reference="primary.token", changed=True),
                capabilities=created.capabilities,
                schema_discovery=None,
                updated_at=created.updated_at,
            )
        with pytest.raises(InvalidRepositoryRequestError, match="later than"):
            repository.archive(
                connector_id,
                expected_row_version=1,
                archived_at=created.updated_at,
            )


def test_connector_without_references_and_missing_operations(database: SQLiteDatabase) -> None:
    with database.transaction() as session:
        repository = SqlAlchemyConnectorRepository(session)
        assert repository.list(limit=10).items == ()
        created = repository.create(
            connector_id=ConnectorId("con_plain"),
            kind="plain",
            display_name="Plain",
            configuration=document(endpoint="local"),
            capabilities=document(),
            schema_discovery=document(),
            secret_references=(),
            created_at=timestamp(0),
        )
        assert created.secret_references == ()
        missing_id = ConnectorId("con_missing")
        with pytest.raises(RecordNotFoundError):
            repository.update_metadata(
                missing_id,
                expected_row_version=1,
                display_name="Missing",
                updated_at=timestamp(1),
            )
        with pytest.raises(RecordNotFoundError):
            repository.update_definition(
                missing_id,
                expected_row_version=1,
                expected_revision=1,
                kind="plain",
                configuration=document(),
                capabilities=document(),
                schema_discovery=None,
                updated_at=timestamp(1),
            )
        with pytest.raises(RecordNotFoundError):
            repository.archive(missing_id, expected_row_version=1, archived_at=timestamp(1))


def test_connector_requires_transaction_and_valid_secret_reference_inputs(
    database: SQLiteDatabase,
) -> None:
    session = create_session_factory(database.engine)()
    try:
        with pytest.raises(InvalidRepositoryRequestError, match="caller-owned"):
            SqlAlchemyConnectorRepository(session).get(ConnectorId("con_inventory"))
    finally:
        session.close()
    with database.transaction() as session:
        repository = SqlAlchemyConnectorRepository(session)
        with pytest.raises(InvalidRepositoryRequestError, match="unique"):
            repository.create(
                connector_id=ConnectorId("con_inventory"),
                kind="inventory-http",
                display_name="Inventory",
                configuration=document(),
                capabilities=document(),
                schema_discovery=None,
                secret_references=(
                    ConnectorSecretReference("primary.token", "TOKEN_ONE"),
                    ConnectorSecretReference("primary.token", "TOKEN_TWO"),
                ),
                created_at=timestamp(0),
            )


def test_two_sessions_classify_stale_connector_writes(database: SQLiteDatabase) -> None:
    connector_id = ConnectorId("con_inventory")
    with database.transaction() as session:
        create_connector(SqlAlchemyConnectorRepository(session))
    session_factory = create_session_factory(database.engine)
    first = session_factory()
    second = session_factory()
    try:
        first.begin()
        second.begin()
        first_repository = SqlAlchemyConnectorRepository(first)
        second_repository = SqlAlchemyConnectorRepository(second)
        first_read = first_repository.get(connector_id)
        second_read = second_repository.get(connector_id)
        assert first_read is not None
        assert first_read.row_version == 1
        assert second_read is not None
        assert second_read.row_version == 1
        first_repository.update_metadata(
            connector_id,
            expected_row_version=1,
            display_name="First update",
            updated_at=timestamp(1),
        )
        first.commit()
        with pytest.raises(StaleRowVersionError):
            second_repository.update_metadata(
                connector_id,
                expected_row_version=1,
                display_name="Second update",
                updated_at=timestamp(2),
            )
        second.rollback()
    finally:
        first.close()
        second.close()


def test_two_sessions_publish_one_version_and_converge_or_conflict(
    database: SQLiteDatabase,
) -> None:
    exact_id = PipelineId("pip_race-exact")
    divergent_id = PipelineId("pip_race-divergent")
    with database.transaction() as session:
        repository = SqlAlchemyPipelineRepository(session)
        create_pipeline(repository, exact_id)
        create_pipeline(repository, divergent_id)

    def race(
        pipeline_id: PipelineId,
        specifications: tuple[ConfigurationDocument, ConfigurationDocument],
    ) -> tuple[object, object]:
        barrier = Barrier(2)

        def synchronize_inserts(
            _connection: Connection,
            _cursor: object,
            statement: str,
            _parameters: object,
            _context: object,
            _executemany: bool,
        ) -> None:
            if "INSERT INTO pipeline_versions" in statement:
                barrier.wait(timeout=10)

        def publish(specification: ConfigurationDocument) -> object:
            try:
                with database.transaction() as session:
                    return SqlAlchemyPipelineRepository(session).publish_version(
                        pipeline_id=pipeline_id,
                        expected_latest_version=None,
                        specification=specification,
                        planner_format_version=1,
                        published_at=timestamp(1),
                    )
            except PipelineVersionConflictError as error:
                return error

        event.listen(database.engine, "before_cursor_execute", synchronize_inserts)
        try:
            with ThreadPoolExecutor(max_workers=2) as executor:
                futures = tuple(executor.submit(publish, value) for value in specifications)
                return futures[0].result(timeout=15), futures[1].result(timeout=15)
        finally:
            event.remove(database.engine, "before_cursor_execute", synchronize_inserts)

    exact_specification = document(nodes=[])
    exact_results = race(exact_id, (exact_specification, exact_specification))
    assert exact_results[0] == exact_results[1]
    divergent_results = race(
        divergent_id,
        (document(nodes=[{"id": "left"}]), document(nodes=[{"id": "right"}])),
    )
    assert (
        sum(isinstance(result, PipelineVersionConflictError) for result in divergent_results) == 1
    )
    with database.transaction() as session:
        repository = SqlAlchemyPipelineRepository(session)
        assert len(repository.list_versions(exact_id, limit=10).items) == 1
        assert len(repository.list_versions(divergent_id, limit=10).items) == 1


@pytest.mark.parametrize(
    ("raised", "expected_type", "expected_message"),
    [
        (
            OperationalError(
                "SELECT canary_sql",
                {"secret": "canary_parameter"},
                sqlite3.OperationalError("canary_database"),
            ),
            ConfigurationStorageUnavailableError,
            "Configuration storage is unavailable.",
        ),
        (
            IntegrityError(
                "INSERT canary_sql",
                {"secret": "canary_parameter"},
                sqlite3.IntegrityError("canary_integrity"),
            ),
            ConfigurationStorageError,
            "Configuration storage operation failed.",
        ),
    ],
)
def test_storage_failures_are_redacted_without_raw_exception_chain(
    database: SQLiteDatabase,
    monkeypatch: pytest.MonkeyPatch,
    raised: SQLAlchemyError,
    expected_type: type[ConfigurationStorageError],
    expected_message: str,
) -> None:
    with database.transaction() as session:

        def fail_execute(_self: object, _statement: object) -> NoReturn:
            raise raised

        monkeypatch.setattr(session, "execute", MethodType(fail_execute, session))
        with pytest.raises(expected_type) as caught:
            SqlAlchemyPipelineRepository(session).get(PipelineId("pip_configuration"))
        error = caught.value
        assert str(error) == expected_message
        assert error.args == (expected_message,)
        assert error.__cause__ is None
        assert error.__context__ is None
        exposed = f"{error!s} {error!r} {error.args!r}"
        assert "canary" not in exposed
        assert session.in_transaction()


def test_raw_version_gaps_and_precreation_publication_are_corruption(
    database: SQLiteDatabase,
) -> None:
    pipeline_id = PipelineId("pip_corrupt-history")
    with database.transaction() as session:
        create_pipeline(SqlAlchemyPipelineRepository(session), pipeline_id)
        session.execute(
            insert(pipeline_versions),
            [
                {
                    "pipeline_id": str(pipeline_id),
                    "version_number": version,
                    "specification_json": "{}",
                    "specification_sha256": (
                        "44136fa355b3678a1146ad16f7e8649e94fb4fc21fe77e8310c060f61caaff8a"
                    ),
                    "planner_format_version": 1,
                    "published_at": str(timestamp(1)),
                }
                for version in (1, 3)
            ],
        )
        repository = SqlAlchemyPipelineRepository(session)
        with pytest.raises(CorruptRepositoryRecordError, match="not contiguous"):
            repository.get_version(pipeline_id, PipelineVersion(1))
        with pytest.raises(CorruptRepositoryRecordError, match="not contiguous"):
            repository.list_versions(pipeline_id, limit=10)
        with pytest.raises(CorruptRepositoryRecordError, match="not contiguous"):
            repository.publish_version(
                pipeline_id=pipeline_id,
                expected_latest_version=PipelineVersion(3),
                specification=document(),
                planner_format_version=1,
                published_at=timestamp(2),
            )

    early_id = PipelineId("pip_early-version")
    with database.transaction() as session:
        create_pipeline(SqlAlchemyPipelineRepository(session), early_id)
        session.execute(
            insert(pipeline_versions).values(
                pipeline_id=str(early_id),
                version_number=1,
                specification_json="{}",
                specification_sha256=(
                    "44136fa355b3678a1146ad16f7e8649e94fb4fc21fe77e8310c060f61caaff8a"
                ),
                planner_format_version=1,
                published_at="2026-08-12T11:59:59.000000Z",
            )
        )
        with pytest.raises(CorruptRepositoryRecordError, match="publication time"):
            SqlAlchemyPipelineRepository(session).get_version(early_id, PipelineVersion(1))


def test_connector_reads_revalidate_secret_policy_and_row_coherence(
    database: SQLiteDatabase,
) -> None:
    def insert_connector(
        session: object,
        connector_id: str,
        configuration_json: str,
        *,
        updated_at: str = "2026-08-12T12:00:00.000000Z",
        archived_at: str | None = None,
    ) -> None:
        cast(Session, session).execute(
            insert(connectors).values(
                connector_id=connector_id,
                kind="plain",
                display_name="Corrupt fixture",
                configuration_json=configuration_json,
                capabilities_json="{}",
                schema_discovery_json=None,
                revision=1,
                created_at="2026-08-12T12:00:00.000000Z",
                updated_at=updated_at,
                archived_at=archived_at,
                row_version=1,
            )
        )

    with database.transaction() as session:
        insert_connector(session, "con_raw-secret", '{"api_token":"canary"}')
        insert_connector(
            session,
            "con_unused-reference",
            '{"api_token_reference":"primary.token"}',
        )
        session.execute(
            insert(connector_secret_references),
            [
                {
                    "connector_id": "con_unused-reference",
                    "reference_name": name,
                    "environment_variable_name": environment,
                    "created_at": "2026-08-12T12:00:00.000000Z",
                }
                for name, environment in (
                    ("primary.token", "PRIMARY_TOKEN"),
                    ("unused.token", "UNUSED_TOKEN"),
                )
            ],
        )
        insert_connector(
            session,
            "con_bad-archive-time",
            "{}",
            updated_at="2026-08-12T12:00:01.000000Z",
            archived_at="2026-08-12T12:00:02.000000Z",
        )
        insert_connector(
            session,
            "con_bad-reference-time",
            '{"api_token_reference":"primary.token"}',
        )
        session.execute(
            insert(connector_secret_references).values(
                connector_id="con_bad-reference-time",
                reference_name="primary.token",
                environment_variable_name="PRIMARY_TOKEN",
                created_at="2026-08-12T12:00:01.000000Z",
            )
        )
        repository = SqlAlchemyConnectorRepository(session)
        for identity in (
            "con_raw-secret",
            "con_unused-reference",
            "con_bad-archive-time",
            "con_bad-reference-time",
        ):
            with pytest.raises(CorruptRepositoryRecordError):
                repository.get(ConnectorId(identity))


def test_exact_runtime_inputs_and_description_bound_fail_before_sql(
    database: SQLiteDatabase,
) -> None:
    with database.transaction() as session:
        pipelines_repository = SqlAlchemyPipelineRepository(session)
        connectors_repository = SqlAlchemyConnectorRepository(session)
        with pytest.raises(InvalidRepositoryRequestError, match="pipeline identifier"):
            pipelines_repository.get(cast(PipelineId, "pip_wrong_type"))
        with pytest.raises(InvalidRepositoryRequestError, match="creation time"):
            pipelines_repository.create(
                pipeline_id=PipelineId("pip_wrong-time"),
                display_name="Wrong",
                description=None,
                created_at=cast(UtcTimestamp, "2026-08-12T12:00:00.000000Z"),
            )
        with pytest.raises(InvalidRepositoryRequestError, match="invalid length"):
            pipelines_repository.create(
                pipeline_id=PipelineId("pip_long-description"),
                display_name="Long",
                description="x" * 4_097,
                created_at=timestamp(0),
            )
        with pytest.raises(InvalidRepositoryRequestError, match="include archived"):
            pipelines_repository.list(limit=1, include_archived=cast(bool, 1))
        with pytest.raises(InvalidRepositoryRequestError, match="include archived"):
            connectors_repository.list(limit=1, include_archived=cast(bool, 1))
        with pytest.raises(InvalidRepositoryRequestError, match="connector identifier"):
            connectors_repository.get(cast(ConnectorId, "con_wrong_type"))
        with pytest.raises(InvalidRepositoryRequestError, match="ConfigurationDocument"):
            connectors_repository.create(
                connector_id=ConnectorId("con_wrong-document"),
                kind="plain",
                display_name="Wrong",
                configuration=cast(ConfigurationDocument, {}),
                capabilities=document(),
                schema_discovery=None,
                secret_references=(),
                created_at=timestamp(0),
            )


def test_caller_owns_session_and_wal_visibility_survives_reopen(tmp_path: Path) -> None:
    path = tmp_path / "configuration عربي %25.db"
    database = SQLiteDatabase.open(SQLiteDatabaseConfig(database_path=path))
    with database.engine.connect() as connection:
        upgrade_to_head(connection)

    class OwnershipSpy:
        def __init__(self, session: Session) -> None:
            self._session = session

        def in_transaction(self) -> bool:
            return self._session.in_transaction()

        def execute(self, statement: Executable) -> Result[Any]:
            return self._session.execute(statement)

        def begin(self) -> NoReturn:
            raise AssertionError("repository attempted to begin a transaction")

        def begin_nested(self) -> NoReturn:
            raise AssertionError("repository attempted to create a savepoint")

        def commit(self) -> NoReturn:
            raise AssertionError("repository attempted to commit")

        def rollback(self) -> NoReturn:
            raise AssertionError("repository attempted to roll back")

        def close(self) -> NoReturn:
            raise AssertionError("repository attempted to close the caller session")

    try:
        with database.transaction() as session:
            repository = SqlAlchemyPipelineRepository(cast(Session, OwnershipSpy(session)))
            create_pipeline(repository, PipelineId("pip_wal-visibility"))
            with closing(sqlite3.connect(path)) as reader:
                assert reader.execute(
                    "SELECT count(*) FROM pipelines WHERE pipeline_id = 'pip_wal-visibility'"
                ).fetchone() == (0,)
            assert session.in_transaction()
        with closing(sqlite3.connect(path)) as reader:
            assert reader.execute(
                "SELECT count(*) FROM pipelines WHERE pipeline_id = 'pip_wal-visibility'"
            ).fetchone() == (1,)
    finally:
        database.close()

    reopened = SQLiteDatabase.open(SQLiteDatabaseConfig(database_path=path))
    try:
        with reopened.engine.connect() as connection:
            assert connection.exec_driver_sql("PRAGMA quick_check").scalar_one() == "ok"
            assert connection.exec_driver_sql("PRAGMA foreign_key_check").all() == []
        with reopened.transaction() as session:
            assert (
                SqlAlchemyPipelineRepository(session).get(PipelineId("pip_wal-visibility"))
                is not None
            )
    finally:
        reopened.close()

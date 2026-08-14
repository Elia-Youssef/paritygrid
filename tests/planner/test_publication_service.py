"""Replay, mutation, and composition tests for immutable publication."""

from __future__ import annotations

from collections.abc import Iterator
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, cast

import pytest

from paritygrid.adapters.persistence import SQLiteDatabase, SQLiteDatabaseConfig, upgrade_to_head
from paritygrid.adapters.persistence.repositories import (
    SqlAlchemyConnectorRepository,
    SqlAlchemyPipelineRepository,
)
from paritygrid.application.planner import (
    MissingConnectorError,
    PipelineDocument,
    PipelinePublicationService,
    PublishedPipelineSpecification,
)
from paritygrid.application.ports import (
    ConfigurationDocument,
    ConnectorRepository,
    ConnectorSecretReference,
    PipelineRepository,
    PipelineVersionConflictError,
)
from paritygrid.domain.models import (
    ConnectorId,
    PipelineId,
    PipelineVersion,
    UtcTimestamp,
)

_PIPELINE_ID = PipelineId("pip_publication")
_CONNECTOR_ID = ConnectorId("con_source-001")


@pytest.fixture
def database(tmp_path: Path) -> Iterator[SQLiteDatabase]:
    database = SQLiteDatabase.open(SQLiteDatabaseConfig(tmp_path / "publication Δ.db"))
    with database.engine.connect() as connection:
        upgrade_to_head(connection)
    try:
        yield database
    finally:
        database.close()


def _time(second: int) -> UtcTimestamp:
    return UtcTimestamp(datetime(2026, 8, 14, 9, 0, second, tzinfo=UTC))


def _document(**values: object) -> ConfigurationDocument:
    return ConfigurationDocument.from_mapping(values)


def _draft(*, connector_id: str = "con_source-001") -> PipelineDocument:
    value: dict[str, object] = {
        "canonical_format_version": 1,
        "edges": [
            {
                "source_node_id": "nod_source-001",
                "source_port": "records",
                "target_node_id": "nod_export-001",
                "target_port": "records",
            }
        ],
        "layout": [
            {"node_id": "nod_source-001", "x": 0, "y": 0},
            {"node_id": "nod_export-001", "x": 10, "y": 20},
        ],
        "nodes": [
            {
                "configuration": {"encoding": "utf-8"},
                "configuration_version": 1,
                "connector_id": connector_id,
                "id": "nod_source-001",
                "kind": "source.csv",
            },
            {
                "configuration": {"compression": "zstd"},
                "configuration_version": 1,
                "connector_id": None,
                "id": "nod_export-001",
                "kind": "export.parquet",
            },
        ],
        "resource_policy": {"max_concurrency": 2},
        "schema_version": 1,
    }
    return PipelineDocument.from_mapping(value)


def _create_state(
    pipelines: SqlAlchemyPipelineRepository,
    connectors: SqlAlchemyConnectorRepository,
) -> None:
    pipelines.create(
        pipeline_id=_PIPELINE_ID,
        display_name="Immutable publication",
        description=None,
        created_at=_time(0),
    )
    connectors.create(
        connector_id=_CONNECTOR_ID,
        kind="csv-local",
        display_name="Local source",
        configuration=_document(
            api_token_reference="source.token",
            path="inventory.csv",
        ),
        capabilities=_document(read=True),
        schema_discovery=None,
        secret_references=(ConnectorSecretReference("source.token", "PARITYGRID_SOURCE_TOKEN"),),
        created_at=_time(0),
    )


def test_publication_replays_exactly_and_materializes_immutable_snapshot(
    database: SQLiteDatabase,
) -> None:
    with database.transaction() as session:
        pipelines = SqlAlchemyPipelineRepository(session)
        connectors = SqlAlchemyConnectorRepository(session)
        _create_state(pipelines, connectors)
        service = PipelinePublicationService(pipelines, connectors)
        published = service.publish(
            pipeline_id=_PIPELINE_ID,
            expected_latest_version=None,
            draft=_draft(),
            published_at=_time(1),
        )
        assert (
            service.publish(
                pipeline_id=_PIPELINE_ID,
                expected_latest_version=None,
                draft=_draft(),
                published_at=_time(1),
            )
            == published
        )
        envelope = PublishedPipelineSpecification.from_configuration_document(
            published.specification
        )
        assert envelope.pipeline.resource_policy.to_mapping() == {
            "max_concurrency": 2,
            "max_in_flight": 16,
            "memory_limit_bytes": 536_870_912,
            "operation_timeout_seconds": 60,
            "queue_capacity": 256,
        }
        binding = envelope.connector_bindings[0]
        assert binding.revision == 1
        assert binding.configuration.to_mapping()["path"] == "inventory.csv"
        stored = published.specification.to_mapping()
        assert "display_name" not in str(stored)
        assert "row_version" not in str(stored)
        assert "PARITYGRID_SOURCE_TOKEN" in str(stored)

    with database.transaction() as session:
        pipelines = SqlAlchemyPipelineRepository(session)
        connectors = SqlAlchemyConnectorRepository(session)
        updated = connectors.update_definition(
            _CONNECTOR_ID,
            expected_row_version=1,
            expected_revision=1,
            kind="csv-local",
            configuration=_document(
                api_token_reference="source.token",
                path="mutated.csv",
            ),
            capabilities=_document(read=True),
            schema_discovery=None,
            updated_at=_time(2),
        )
        assert updated.revision == 2
        old = pipelines.get_version(_PIPELINE_ID, PipelineVersion(1))
        assert old is not None
        old_envelope = PublishedPipelineSpecification.from_configuration_document(old.specification)
        assert old_envelope.connector_bindings[0].revision == 1
        assert old_envelope.connector_bindings[0].configuration.to_mapping()["path"] == (
            "inventory.csv"
        )
        service = PipelinePublicationService(pipelines, connectors)
        with pytest.raises(PipelineVersionConflictError):
            service.publish(
                pipeline_id=_PIPELINE_ID,
                expected_latest_version=None,
                draft=_draft(),
                published_at=_time(1),
            )
        second = service.publish(
            pipeline_id=_PIPELINE_ID,
            expected_latest_version=PipelineVersion(1),
            draft=_draft(),
            published_at=_time(3),
        )
        second_envelope = PublishedPipelineSpecification.from_configuration_document(
            second.specification
        )
        assert second.version == PipelineVersion(2)
        assert second_envelope.connector_bindings[0].revision == 2
        assert second_envelope.connector_bindings[0].configuration.to_mapping()["path"] == (
            "mutated.csv"
        )


class _MissingConnectorRepository:
    def get(self, _connector_id: ConnectorId) -> None:
        return None


def test_missing_connector_fails_before_repository_publication() -> None:
    service = PipelinePublicationService(
        cast(PipelineRepository, object()),
        cast(ConnectorRepository, _MissingConnectorRepository()),
    )
    with pytest.raises(MissingConnectorError):
        service.publish(
            pipeline_id=_PIPELINE_ID,
            expected_latest_version=None,
            draft=_draft(),
            published_at=_time(1),
        )


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        ("pipeline_id", "pip_publication", "PipelineId"),
        ("expected_latest_version", 1, "PipelineVersion"),
        ("draft", {}, "PipelineDocument"),
        ("published_at", "2026-08-14", "UtcTimestamp"),
    ],
)
def test_publication_requires_exact_public_inputs(field: str, value: object, message: str) -> None:
    service = PipelinePublicationService(
        cast(PipelineRepository, object()),
        cast(ConnectorRepository, object()),
    )
    arguments: dict[str, object] = {
        "pipeline_id": _PIPELINE_ID,
        "expected_latest_version": None,
        "draft": _draft(),
        "published_at": _time(1),
    }
    arguments[field] = value
    with pytest.raises(TypeError, match=message):
        service.publish(**cast(Any, arguments))

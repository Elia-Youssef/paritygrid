# pyright: reportPrivateUsage=false
"""Adversarial validation tests at the configuration storage boundary."""

from __future__ import annotations

from typing import cast

import pytest
from sqlalchemy.engine import RowMapping
from sqlalchemy.orm import Session

from paritygrid.adapters.persistence import repositories as runtime
from paritygrid.application.ports import (
    ConfigurationDocument,
    ConnectorRecord,
    ConnectorSecretReference,
    CorruptRepositoryRecordError,
    InvalidRepositoryRequestError,
    PipelineRecord,
    PipelineVersionConflictError,
    PipelineVersionRecord,
    RecordNotFoundError,
    RecordStateConflictError,
    StaleConnectorRevisionError,
    StaleRowVersionError,
)
from paritygrid.application.ports.configuration import DocumentObject
from paritygrid.domain.models import ConnectorId, PipelineId, PipelineVersion, UtcTimestamp


def row(**values: object) -> RowMapping:
    return cast(RowMapping, values)


def pipeline_row(**overrides: object) -> RowMapping:
    values: dict[str, object] = {
        "pipeline_id": "pip_valid",
        "display_name": "Valid",
        "description": None,
        "created_at": "2026-08-12T12:00:00.000000Z",
        "archived_at": None,
        "row_version": 1,
    }
    values.update(overrides)
    return row(**values)


def version_row(**overrides: object) -> RowMapping:
    values: dict[str, object] = {
        "pipeline_id": "pip_valid",
        "version_number": 1,
        "specification_json": "{}",
        "specification_sha256": (
            "44136fa355b3678a1146ad16f7e8649e94fb4fc21fe77e8310c060f61caaff8a"
        ),
        "planner_format_version": 1,
        "published_at": "2026-08-12T12:00:00.000000Z",
    }
    values.update(overrides)
    return row(**values)


def connector_row(**overrides: object) -> RowMapping:
    values: dict[str, object] = {
        "connector_id": "con_valid",
        "kind": "plain",
        "display_name": "Valid",
        "configuration_json": "{}",
        "capabilities_json": "{}",
        "schema_discovery_json": None,
        "revision": 1,
        "created_at": "2026-08-12T12:00:00.000000Z",
        "updated_at": "2026-08-12T12:00:00.000000Z",
        "archived_at": None,
        "row_version": 1,
    }
    values.update(overrides)
    return row(**values)


@pytest.mark.parametrize(
    "overrides",
    [
        {"pipeline_id": 1},
        {"pipeline_id": "bad"},
        {"display_name": ""},
        {"display_name": "e\u0301"},
        {"description": 1},
        {"description": "e\u0301"},
        {"created_at": 1},
        {"created_at": "2026-08-12T12:00:00Z"},
        {"archived_at": "invalid"},
        {"archived_at": "2026-08-12T11:59:59.000000Z"},
        {"row_version": 0},
    ],
)
def test_pipeline_read_boundary_rejects_corruption(overrides: dict[str, object]) -> None:
    with pytest.raises(CorruptRepositoryRecordError):
        runtime._pipeline_from_row(pipeline_row(**overrides))


@pytest.mark.parametrize(
    "overrides",
    [
        {"specification_json": 1},
        {"specification_json": "[]"},
        {"specification_json": '{ "a":1}'},
        {"specification_sha256": 1},
        {"specification_sha256": "0" * 64},
        {"version_number": 0},
        {"planner_format_version": False},
        {"published_at": "invalid"},
    ],
)
def test_pipeline_version_read_boundary_rejects_corruption(
    overrides: dict[str, object],
) -> None:
    with pytest.raises(CorruptRepositoryRecordError):
        runtime._pipeline_version_from_row(version_row(**overrides))


@pytest.mark.parametrize(
    "overrides",
    [
        {"connector_id": 1},
        {"connector_id": "bad"},
        {"kind": ""},
        {"display_name": "e\u0301"},
        {"configuration_json": "[]"},
        {"capabilities_json": '{ "a":1}'},
        {"schema_discovery_json": "[]"},
        {"revision": 0},
        {"updated_at": "invalid"},
        {"updated_at": "2026-08-12T11:59:59.000000Z"},
        {"archived_at": "2026-08-12T11:59:59.000000Z"},
        {"row_version": False},
    ],
)
def test_connector_read_boundary_rejects_corruption(overrides: dict[str, object]) -> None:
    with pytest.raises(CorruptRepositoryRecordError):
        runtime._connector_from_row(connector_row(**overrides), ())


@pytest.mark.parametrize(
    "values",
    [
        {"reference_name": 1, "environment_variable_name": "VALID"},
        {"reference_name": "Bad", "environment_variable_name": "VALID"},
        {"reference_name": "valid", "environment_variable_name": "bad"},
        {
            "reference_name": "valid",
            "environment_variable_name": "VALID",
            "created_at": "invalid",
        },
    ],
)
def test_secret_reference_read_boundary_rejects_corruption(
    values: dict[str, object],
) -> None:
    complete = {
        "reference_name": "valid",
        "environment_variable_name": "VALID",
        "created_at": "2026-08-12T12:00:00.000000Z",
        **values,
    }
    with pytest.raises(CorruptRepositoryRecordError):
        runtime._secret_reference_from_row(row(**complete))


def test_closed_input_and_codec_error_paths_do_not_echo_values() -> None:
    with pytest.raises(InvalidRepositoryRequestError):
        runtime._bounded_text(1, "name", 10)
    with pytest.raises(InvalidRepositoryRequestError):
        runtime._bounded_text("", "name", 10)
    with pytest.raises(InvalidRepositoryRequestError):
        runtime._bounded_text("e\u0301", "name", 10)
    with pytest.raises(InvalidRepositoryRequestError):
        runtime._optional_text(1, "description")
    with pytest.raises(InvalidRepositoryRequestError):
        runtime._positive_int(True, "version")
    with pytest.raises(InvalidRepositoryRequestError, match="supported range"):
        runtime._positive_int(2_147_483_648, "version")
    with pytest.raises(RecordStateConflictError, match="supported maximum"):
        runtime._require_incrementable(2_147_483_647, "row version")
    with pytest.raises(InvalidRepositoryRequestError):
        runtime._encode_document(cast(ConfigurationDocument, object()))
    with pytest.raises(InvalidRepositoryRequestError, match="encoded size"):
        runtime._encode_document(ConfigurationDocument.from_mapping({"large": ["x" * 65_536] * 16}))
    with pytest.raises(CorruptRepositoryRecordError):
        runtime._decode_document(1, "document")
    with pytest.raises(CorruptRepositoryRecordError):
        runtime._decode_document("[]", "document")
    assert runtime._decode_optional_document(None, "document") is None


def test_document_constructor_and_bounds_are_closed() -> None:
    with pytest.raises(TypeError, match="keys must be text"):
        ConfigurationDocument.from_mapping(cast(dict[str, object], {1: "value"}))
    with pytest.raises(TypeError, match="immutable tuple"):
        ConfigurationDocument(items=cast(DocumentObject, cast(object, [])))
    with pytest.raises(TypeError, match="key-value pairs"):
        ConfigurationDocument(items=cast(DocumentObject, (1,)))
    with pytest.raises(TypeError, match="key-value pairs"):
        ConfigurationDocument(items=cast(DocumentObject, (("bad",),)))
    with pytest.raises(TypeError, match="key-value pairs"):
        ConfigurationDocument(items=cast(DocumentObject, (("bad", 1, 2),)))
    with pytest.raises(TypeError, match="keys must be text"):
        ConfigurationDocument(items=cast(DocumentObject, ((1, 2),)))
    with pytest.raises(TypeError, match="values must be immutable"):
        ConfigurationDocument(items=cast(DocumentObject, (("bad", []),)))
    with pytest.raises(ValueError, match="too many entries"):
        ConfigurationDocument.from_mapping({"items": [None] * 10_001})
    assert repr(ConnectorSecretReference("primary", "PRIMARY")) == (
        "ConnectorSecretReference(redacted=True)"
    )


def test_secret_reference_input_type_and_nested_policy_paths_are_closed() -> None:
    with pytest.raises(InvalidRepositoryRequestError, match="public value type"):
        runtime._validate_secret_references(cast(tuple[ConnectorSecretReference, ...], (object(),)))
    declared = frozenset({"primary.token"})
    runtime._validate_secret_policy(
        ConfigurationDocument.from_mapping({"items": [{"api_token_reference": "primary.token"}]}),
        declared,
    )


def test_concurrent_cas_failure_classification_is_deterministic() -> None:
    created_at = UtcTimestamp.parse("2026-08-12T12:00:00.000000Z")
    pipeline = PipelineRecord(
        pipeline_id=PipelineId("pip_valid"),
        display_name="Valid",
        description=None,
        created_at=created_at,
        archived_at=None,
        row_version=2,
    )
    with pytest.raises(RecordNotFoundError):
        runtime._raise_cas_failure(None, 1, "pipeline")
    with pytest.raises(StaleRowVersionError):
        runtime._raise_cas_failure(pipeline, 1, "pipeline")
    archived_pipeline = PipelineRecord(
        pipeline_id=pipeline.pipeline_id,
        display_name=pipeline.display_name,
        description=None,
        created_at=created_at,
        archived_at=UtcTimestamp.parse("2026-08-12T12:00:01.000000Z"),
        row_version=2,
    )
    with pytest.raises(RecordStateConflictError):
        runtime._raise_cas_failure(archived_pipeline, 2, "pipeline")
    with pytest.raises(RecordStateConflictError):
        runtime._raise_cas_failure(pipeline, 2, "pipeline")
    connector = ConnectorRecord(
        connector_id=ConnectorId("con_valid"),
        kind="plain",
        display_name="Valid",
        configuration=ConfigurationDocument.from_mapping({}),
        capabilities=ConfigurationDocument.from_mapping({}),
        schema_discovery=None,
        secret_references=(),
        revision=2,
        created_at=created_at,
        updated_at=created_at,
        archived_at=None,
        row_version=2,
    )
    with pytest.raises(RecordNotFoundError):
        runtime._raise_connector_cas_failure(None, 1, None)
    with pytest.raises(StaleRowVersionError):
        runtime._raise_connector_cas_failure(connector, 1, None)
    with pytest.raises(StaleConnectorRevisionError):
        runtime._raise_connector_cas_failure(connector, 2, 1)
    with pytest.raises(RecordStateConflictError):
        runtime._raise_connector_cas_failure(connector, 2, 2)
    archived_connector = ConnectorRecord(
        connector_id=connector.connector_id,
        kind=connector.kind,
        display_name=connector.display_name,
        configuration=connector.configuration,
        capabilities=connector.capabilities,
        schema_discovery=None,
        secret_references=(),
        revision=2,
        created_at=created_at,
        updated_at=created_at,
        archived_at=UtcTimestamp.parse("2026-08-12T12:00:01.000000Z"),
        row_version=2,
    )
    with pytest.raises(RecordStateConflictError):
        runtime._raise_connector_cas_failure(archived_connector, 2, 2)


def test_missing_mapping_keys_and_empty_reference_group_are_closed() -> None:
    with pytest.raises(CorruptRepositoryRecordError):
        runtime._pipeline_from_row(row())
    with pytest.raises(CorruptRepositoryRecordError):
        runtime._pipeline_version_from_row(row())
    with pytest.raises(CorruptRepositoryRecordError):
        runtime._connector_from_row(row(), ())


class _EmptyMappingsResult:
    def __init__(self, scalar: object = None) -> None:
        self._scalar = scalar

    def mappings(self) -> _EmptyMappingsResult:
        return self

    def one_or_none(self) -> None:
        return None

    def scalar_one_or_none(self) -> object:
        return self._scalar

    def scalar_one(self) -> object:
        return self._scalar


class _RacingSession:
    def __init__(self, scalar: object = None) -> None:
        self._scalar = scalar

    def in_transaction(self) -> bool:
        return True

    def execute(self, _statement: object) -> _EmptyMappingsResult:
        return _EmptyMappingsResult(self._scalar)


def test_empty_pipeline_version_sequence_has_zero_latest_version() -> None:
    repository = runtime.SqlAlchemyPipelineRepository(cast(Session, cast(object, _RacingSession())))
    assert repository._latest_version_number(PipelineId("pip_valid")) == 0


def test_pipeline_post_update_race_is_reclassified(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    created_at = UtcTimestamp.parse("2026-08-12T12:00:00.000000Z")
    current = PipelineRecord(
        pipeline_id=PipelineId("pip_valid"),
        display_name="Valid",
        description=None,
        created_at=created_at,
        archived_at=None,
        row_version=1,
    )
    raced = PipelineRecord(
        pipeline_id=current.pipeline_id,
        display_name="Raced",
        description=None,
        created_at=created_at,
        archived_at=None,
        row_version=2,
    )
    repository = runtime.SqlAlchemyPipelineRepository(
        cast(Session, cast(object, _RacingSession(1)))
    )
    reads = iter((current, raced))

    def read_pipeline(_pipeline_id: PipelineId) -> PipelineRecord:
        return next(reads)

    monkeypatch.setattr(repository, "get", read_pipeline)
    with pytest.raises(StaleRowVersionError):
        repository.update_metadata(
            current.pipeline_id,
            expected_row_version=1,
            display_name="Changed",
            description=None,
        )


def test_connector_post_update_race_is_reclassified(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    created_at = UtcTimestamp.parse("2026-08-12T12:00:00.000000Z")
    current = ConnectorRecord(
        connector_id=ConnectorId("con_valid"),
        kind="plain",
        display_name="Valid",
        configuration=ConfigurationDocument.from_mapping({}),
        capabilities=ConfigurationDocument.from_mapping({}),
        schema_discovery=None,
        secret_references=(),
        revision=1,
        created_at=created_at,
        updated_at=created_at,
        archived_at=None,
        row_version=1,
    )
    raced = ConnectorRecord(
        connector_id=current.connector_id,
        kind=current.kind,
        display_name="Raced",
        configuration=current.configuration,
        capabilities=current.capabilities,
        schema_discovery=None,
        secret_references=(),
        revision=1,
        created_at=created_at,
        updated_at=UtcTimestamp.parse("2026-08-12T12:00:01.000000Z"),
        archived_at=None,
        row_version=2,
    )
    repository = runtime.SqlAlchemyConnectorRepository(
        cast(Session, cast(object, _RacingSession()))
    )
    reads = iter((current, raced))

    def read_connector(_connector_id: ConnectorId) -> ConnectorRecord:
        return next(reads)

    monkeypatch.setattr(repository, "get", read_connector)
    with pytest.raises(StaleRowVersionError):
        repository.update_metadata(
            current.connector_id,
            expected_row_version=1,
            display_name="Changed",
            updated_at=UtcTimestamp.parse("2026-08-12T12:00:02.000000Z"),
        )


def test_pipeline_archive_post_update_race_is_reclassified(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    created_at = UtcTimestamp.parse("2026-08-12T12:00:00.000000Z")
    current = PipelineRecord(
        pipeline_id=PipelineId("pip_valid"),
        display_name="Valid",
        description=None,
        created_at=created_at,
        archived_at=None,
        row_version=1,
    )
    raced = PipelineRecord(
        pipeline_id=current.pipeline_id,
        display_name=current.display_name,
        description=None,
        created_at=created_at,
        archived_at=None,
        row_version=2,
    )
    repository = runtime.SqlAlchemyPipelineRepository(
        cast(Session, cast(object, _RacingSession(1)))
    )
    reads = iter((current, raced))

    def read_pipeline(_pipeline_id: PipelineId) -> PipelineRecord:
        return next(reads)

    monkeypatch.setattr(repository, "get", read_pipeline)
    with pytest.raises(StaleRowVersionError):
        repository.archive(
            current.pipeline_id,
            expected_row_version=1,
            archived_at=UtcTimestamp.parse("2026-08-12T12:00:01.000000Z"),
        )


def test_publication_concurrent_exact_install_converges(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pipeline_id = PipelineId("pip_valid")
    published_at = UtcTimestamp.parse("2026-08-12T12:00:01.000000Z")
    specification = ConfigurationDocument.from_mapping({})
    installed = PipelineVersionRecord(
        pipeline_id=pipeline_id,
        version=PipelineVersion(1),
        specification=specification,
        specification_sha256=("44136fa355b3678a1146ad16f7e8649e94fb4fc21fe77e8310c060f61caaff8a"),
        planner_format_version=1,
        published_at=published_at,
    )
    repository = runtime.SqlAlchemyPipelineRepository(
        cast(Session, cast(object, _RacingSession(1)))
    )
    versions = iter((None, installed))

    def read_version(
        _pipeline_id: PipelineId, _version: PipelineVersion
    ) -> PipelineVersionRecord | None:
        return next(versions)

    def read_pipeline(_pipeline_id: PipelineId) -> PipelineRecord:
        return PipelineRecord(
            pipeline_id=pipeline_id,
            display_name="Valid",
            description=None,
            created_at=UtcTimestamp.parse("2026-08-12T12:00:00.000000Z"),
            archived_at=None,
            row_version=1,
        )

    monkeypatch.setattr(repository, "get_version", read_version)
    monkeypatch.setattr(repository, "get", read_pipeline)
    assert (
        repository.publish_version(
            pipeline_id=pipeline_id,
            expected_latest_version=None,
            specification=specification,
            planner_format_version=1,
            published_at=published_at,
        )
        == installed
    )


def test_connector_create_readback_failure_is_typed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    connector_id = ConnectorId("con_valid")
    repository = runtime.SqlAlchemyConnectorRepository(
        cast(Session, cast(object, _RacingSession(str(connector_id))))
    )

    def missing_connector(_connector_id: ConnectorId) -> None:
        return None

    monkeypatch.setattr(repository, "get", missing_connector)
    with pytest.raises(CorruptRepositoryRecordError, match="could not be read"):
        repository.create(
            connector_id=connector_id,
            kind="plain",
            display_name="Valid",
            configuration=ConfigurationDocument.from_mapping({}),
            capabilities=ConfigurationDocument.from_mapping({}),
            schema_discovery=None,
            secret_references=(),
            created_at=UtcTimestamp.parse("2026-08-12T12:00:00.000000Z"),
        )


def test_public_sequence_limits_fail_before_storage(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pipeline_id = PipelineId("pip_valid")
    pipeline_repository = runtime.SqlAlchemyPipelineRepository(
        cast(Session, cast(object, _RacingSession()))
    )
    with pytest.raises(PipelineVersionConflictError, match="supported maximum"):
        pipeline_repository.publish_version(
            pipeline_id=pipeline_id,
            expected_latest_version=PipelineVersion(2_147_483_647),
            specification=ConfigurationDocument.from_mapping({}),
            planner_format_version=1,
            published_at=UtcTimestamp.parse("2026-08-12T12:00:01.000000Z"),
        )

    created_at = UtcTimestamp.parse("2026-08-12T12:00:00.000000Z")
    connector = ConnectorRecord(
        connector_id=ConnectorId("con_valid"),
        kind="plain",
        display_name="Valid",
        configuration=ConfigurationDocument.from_mapping({}),
        capabilities=ConfigurationDocument.from_mapping({}),
        schema_discovery=None,
        secret_references=(),
        revision=2_147_483_647,
        created_at=created_at,
        updated_at=created_at,
        archived_at=None,
        row_version=2_147_483_647,
    )
    connector_repository = runtime.SqlAlchemyConnectorRepository(
        cast(Session, cast(object, _RacingSession()))
    )

    def read_connector(_connector_id: ConnectorId) -> ConnectorRecord:
        return connector

    monkeypatch.setattr(connector_repository, "get", read_connector)
    with pytest.raises(RecordStateConflictError, match="row version"):
        connector_repository.update_metadata(
            connector.connector_id,
            expected_row_version=2_147_483_647,
            display_name="Changed",
            updated_at=UtcTimestamp.parse("2026-08-12T12:00:01.000000Z"),
        )

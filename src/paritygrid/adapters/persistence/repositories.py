"""SQLAlchemy adapters for durable pipeline and connector configuration."""

from __future__ import annotations

import hashlib
import unicodedata
from collections.abc import Callable, Mapping, Sequence
from functools import wraps
from typing import NoReturn, cast

from sqlalchemy import func, insert, literal, select, update
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.engine import RowMapping
from sqlalchemy.exc import InterfaceError, OperationalError, SQLAlchemyError
from sqlalchemy.orm import Session

from paritygrid.adapters.persistence.schema import (
    connector_secret_references,
    connectors,
    pipeline_versions,
    pipelines,
)
from paritygrid.adapters.persistence.values import (
    CanonicalStorageJson,
    EnvironmentVariableName,
    SecretReferenceName,
    Sha256Digest,
    StoragePrimitive,
)
from paritygrid.application.ports import (
    ConfigurationDocument,
    ConfigurationStorageError,
    ConfigurationStorageUnavailableError,
    ConnectorPage,
    ConnectorRecord,
    ConnectorRepository,
    ConnectorSecretReference,
    CorruptRepositoryRecordError,
    DuplicateRecordError,
    InvalidRepositoryRequestError,
    PipelinePage,
    PipelineRecord,
    PipelineRepository,
    PipelineVersionConflictError,
    PipelineVersionPage,
    PipelineVersionRecord,
    RecordNotFoundError,
    RecordStateConflictError,
    StaleConnectorRevisionError,
    StaleRowVersionError,
    UnsafeConnectorConfigurationError,
    validate_page_limit,
)
from paritygrid.domain.models import ConnectorId, PipelineId, PipelineVersion, UtcTimestamp

_SENSITIVE_KEY_PARTS = frozenset(
    {
        "access_key",
        "api_key",
        "authorization",
        "credential",
        "password",
        "private_key",
        "secret",
        "token",
    }
)
MAX_CANONICAL_DOCUMENT_BYTES = 1_000_000
MAX_PERSISTED_INTEGER = 2_147_483_647


def _translate_storage_errors[**P, R](
    operation: Callable[P, R],
) -> Callable[P, R]:
    """Replace database implementation errors with a redacted public failure."""

    @wraps(operation)
    def translated(*args: P.args, **kwargs: P.kwargs) -> R:
        unavailable = False
        try:
            return operation(*args, **kwargs)
        except InterfaceError, OperationalError:
            unavailable = True
        except SQLAlchemyError:
            pass
        if unavailable:
            raise ConfigurationStorageUnavailableError(
                "Configuration storage is unavailable."
            ) from None
        raise ConfigurationStorageError("Configuration storage operation failed.") from None

    return translated


class SqlAlchemyPipelineRepository(PipelineRepository):
    """Persist pipeline configuration within a caller-owned Session transaction."""

    def __init__(self, session: Session) -> None:
        self._session = session

    @_translate_storage_errors
    def create(
        self,
        *,
        pipeline_id: PipelineId,
        display_name: str,
        description: str | None,
        created_at: UtcTimestamp,
    ) -> PipelineRecord:
        self._require_transaction()
        name = _bounded_text(display_name, "pipeline display name", maximum=160)
        detail = _optional_text(description, "pipeline description")
        values = {
            "pipeline_id": str(pipeline_id),
            "display_name": name,
            "description": detail,
            "created_at": str(created_at),
            "archived_at": None,
            "row_version": 1,
        }
        row = (
            self._session.execute(
                sqlite_insert(pipelines)
                .values(**values)
                .on_conflict_do_nothing(index_elements=[pipelines.c.pipeline_id])
                .returning(*pipelines.c)
            )
            .mappings()
            .one_or_none()
        )
        if row is None:
            raise DuplicateRecordError("pipeline already exists")
        return _pipeline_from_row(row)

    @_translate_storage_errors
    def get(self, pipeline_id: PipelineId) -> PipelineRecord | None:
        self._require_transaction()
        row = (
            self._session.execute(
                select(pipelines).where(pipelines.c.pipeline_id == str(pipeline_id))
            )
            .mappings()
            .one_or_none()
        )
        return None if row is None else _pipeline_from_row(row)

    @_translate_storage_errors
    def list(
        self,
        *,
        limit: int,
        after: PipelineId | None = None,
        include_archived: bool = False,
    ) -> PipelinePage:
        self._require_transaction()
        page_size = validate_page_limit(limit)
        query = select(pipelines)
        if after is not None:
            query = query.where(pipelines.c.pipeline_id > str(after))
        if not include_archived:
            query = query.where(pipelines.c.archived_at.is_(None))
        rows = (
            self._session.execute(query.order_by(pipelines.c.pipeline_id).limit(page_size + 1))
            .mappings()
            .all()
        )
        records = tuple(_pipeline_from_row(row) for row in rows[:page_size])
        next_cursor = records[-1].pipeline_id if len(rows) > page_size else None
        return PipelinePage(items=records, next_cursor=next_cursor)

    @_translate_storage_errors
    def archive(
        self,
        pipeline_id: PipelineId,
        *,
        expected_row_version: int,
        archived_at: UtcTimestamp,
    ) -> PipelineRecord:
        self._require_transaction()
        expected = _positive_int(expected_row_version, "expected row version")
        current = _require_pipeline_cas(self.get(pipeline_id), expected)
        _require_incrementable(expected, "pipeline row version")
        if archived_at <= current.created_at:
            raise InvalidRepositoryRequestError(
                "pipeline archive time must be later than creation time"
            )
        row = (
            self._session.execute(
                update(pipelines)
                .where(
                    pipelines.c.pipeline_id == str(pipeline_id),
                    pipelines.c.row_version == expected,
                    pipelines.c.archived_at.is_(None),
                )
                .values(archived_at=str(archived_at), row_version=expected + 1)
                .returning(*pipelines.c)
            )
            .mappings()
            .one_or_none()
        )
        if row is None:
            current = self.get(pipeline_id)
            _raise_cas_failure(current, expected, "pipeline")
        return _pipeline_from_row(row)

    @_translate_storage_errors
    def update_metadata(
        self,
        pipeline_id: PipelineId,
        *,
        expected_row_version: int,
        display_name: str,
        description: str | None,
    ) -> PipelineRecord:
        self._require_transaction()
        expected = _positive_int(expected_row_version, "expected row version")
        current = _require_pipeline_cas(self.get(pipeline_id), expected)
        _require_incrementable(expected, "pipeline row version")
        name = _bounded_text(display_name, "pipeline display name", maximum=160)
        detail = _optional_text(description, "pipeline description")
        if name == current.display_name and detail == current.description:
            return current
        row = (
            self._session.execute(
                update(pipelines)
                .where(
                    pipelines.c.pipeline_id == str(pipeline_id),
                    pipelines.c.row_version == expected,
                    pipelines.c.archived_at.is_(None),
                )
                .values(
                    display_name=name,
                    description=detail,
                    row_version=expected + 1,
                )
                .returning(*pipelines.c)
            )
            .mappings()
            .one_or_none()
        )
        if row is None:
            _raise_cas_failure(self.get(pipeline_id), expected, "pipeline")
        return _pipeline_from_row(row)

    @_translate_storage_errors
    def publish_version(
        self,
        *,
        pipeline_id: PipelineId,
        expected_latest_version: PipelineVersion | None,
        specification: ConfigurationDocument,
        planner_format_version: int,
        published_at: UtcTimestamp,
    ) -> PipelineVersionRecord:
        self._require_transaction()
        planner_format = _positive_int(planner_format_version, "planner format version")
        encoded = _encode_document(specification)
        digest = hashlib.sha256(encoded.text.encode("utf-8")).hexdigest()
        expected_number = 0 if expected_latest_version is None else int(expected_latest_version)
        if expected_number >= MAX_PERSISTED_INTEGER:
            raise PipelineVersionConflictError(
                "pipeline version cannot advance beyond the supported maximum"
            )
        allocated_version = PipelineVersion(expected_number + 1)
        existing = self.get_version(pipeline_id, allocated_version)
        candidate = PipelineVersionRecord(
            pipeline_id=pipeline_id,
            version=allocated_version,
            specification=specification,
            specification_sha256=digest,
            planner_format_version=planner_format,
            published_at=published_at,
        )
        if existing is not None:
            if self._latest_version_number(pipeline_id) != int(allocated_version):
                raise PipelineVersionConflictError("pipeline latest version is stale")
            if existing == candidate:
                return existing
            raise PipelineVersionConflictError("pipeline version replay does not match")

        pipeline = self.get(pipeline_id)
        if pipeline is None:
            raise RecordNotFoundError("pipeline does not exist")
        if pipeline.archived_at is not None:
            raise RecordStateConflictError("archived pipeline cannot publish versions")
        if published_at < pipeline.created_at:
            raise InvalidRepositoryRequestError(
                "pipeline publication time cannot precede creation time"
            )
        current_latest = func.coalesce(func.max(pipeline_versions.c.version_number), 0)
        guarded_values = select(
            literal(str(pipeline_id)),
            literal(int(allocated_version)),
            literal(encoded.text),
            literal(digest),
            literal(planner_format),
            literal(str(published_at)),
        ).where(
            select(current_latest)
            .where(pipeline_versions.c.pipeline_id == str(pipeline_id))
            .scalar_subquery()
            == expected_number
        )
        row = (
            self._session.execute(
                sqlite_insert(pipeline_versions)
                .from_select(
                    (
                        "pipeline_id",
                        "version_number",
                        "specification_json",
                        "specification_sha256",
                        "planner_format_version",
                        "published_at",
                    ),
                    guarded_values,
                )
                .on_conflict_do_nothing(
                    index_elements=[
                        pipeline_versions.c.pipeline_id,
                        pipeline_versions.c.version_number,
                    ]
                )
                .returning(*pipeline_versions.c)
            )
            .mappings()
            .one_or_none()
        )
        if row is None:
            installed = self.get_version(pipeline_id, allocated_version)
            if (
                installed is not None
                and self._latest_version_number(pipeline_id) == int(allocated_version)
                and installed == candidate
            ):
                return installed
            raise PipelineVersionConflictError("pipeline latest version is stale")
        return _pipeline_version_from_row(row)

    @_translate_storage_errors
    def get_version(
        self, pipeline_id: PipelineId, version: PipelineVersion
    ) -> PipelineVersionRecord | None:
        self._require_transaction()
        row = (
            self._session.execute(
                select(pipeline_versions).where(
                    pipeline_versions.c.pipeline_id == str(pipeline_id),
                    pipeline_versions.c.version_number == int(version),
                )
            )
            .mappings()
            .one_or_none()
        )
        return None if row is None else _pipeline_version_from_row(row)

    @_translate_storage_errors
    def list_versions(
        self,
        pipeline_id: PipelineId,
        *,
        limit: int,
        after: PipelineVersion | None = None,
    ) -> PipelineVersionPage:
        self._require_transaction()
        page_size = validate_page_limit(limit)
        query = select(pipeline_versions).where(pipeline_versions.c.pipeline_id == str(pipeline_id))
        if after is not None:
            query = query.where(pipeline_versions.c.version_number > int(after))
        rows = (
            self._session.execute(
                query.order_by(pipeline_versions.c.version_number).limit(page_size + 1)
            )
            .mappings()
            .all()
        )
        records = tuple(_pipeline_version_from_row(row) for row in rows[:page_size])
        next_cursor = records[-1].version if len(rows) > page_size else None
        return PipelineVersionPage(items=records, next_cursor=next_cursor)

    def _latest_version_number(self, pipeline_id: PipelineId) -> int:
        value = self._session.execute(
            select(func.max(pipeline_versions.c.version_number)).where(
                pipeline_versions.c.pipeline_id == str(pipeline_id)
            )
        ).scalar_one()
        if value is None:
            return 0
        return _stored_positive_int(value, "pipeline latest version")

    def _require_transaction(self) -> None:
        if not self._session.in_transaction():
            raise InvalidRepositoryRequestError("repository requires a caller-owned transaction")


class SqlAlchemyConnectorRepository(ConnectorRepository):
    """Persist connector definitions without resolving or storing secret values."""

    def __init__(self, session: Session) -> None:
        self._session = session

    @_translate_storage_errors
    def create(
        self,
        *,
        connector_id: ConnectorId,
        kind: str,
        display_name: str,
        configuration: ConfigurationDocument,
        capabilities: ConfigurationDocument,
        schema_discovery: ConfigurationDocument | None,
        secret_references: Sequence[ConnectorSecretReference],
        created_at: UtcTimestamp,
    ) -> ConnectorRecord:
        self._require_transaction()
        connector_kind = _bounded_text(kind, "connector kind", maximum=96)
        name = _bounded_text(display_name, "connector display name", maximum=160)
        references = _validate_secret_references(secret_references)
        _validate_secret_policy(configuration, frozenset(ref.reference_name for ref in references))
        configuration_json = _encode_document(configuration)
        capabilities_json = _encode_document(capabilities)
        discovery_json = None if schema_discovery is None else _encode_document(schema_discovery)
        inserted = self._session.execute(
            sqlite_insert(connectors)
            .values(
                connector_id=str(connector_id),
                kind=connector_kind,
                display_name=name,
                configuration_json=configuration_json.text,
                capabilities_json=capabilities_json.text,
                schema_discovery_json=(None if discovery_json is None else discovery_json.text),
                revision=1,
                created_at=str(created_at),
                updated_at=str(created_at),
                archived_at=None,
                row_version=1,
            )
            .on_conflict_do_nothing(index_elements=[connectors.c.connector_id])
            .returning(connectors.c.connector_id)
        ).scalar_one_or_none()
        if inserted is None:
            raise DuplicateRecordError("connector already exists")
        if references:
            self._session.execute(
                insert(connector_secret_references),
                [
                    {
                        "connector_id": str(connector_id),
                        "reference_name": reference.reference_name,
                        "environment_variable_name": reference.environment_variable_name,
                        "created_at": str(created_at),
                    }
                    for reference in references
                ],
            )
        created = self.get(connector_id)
        if created is None:
            raise CorruptRepositoryRecordError("created connector could not be read")
        return created

    @_translate_storage_errors
    def get(self, connector_id: ConnectorId) -> ConnectorRecord | None:
        self._require_transaction()
        row = (
            self._session.execute(
                select(connectors).where(connectors.c.connector_id == str(connector_id))
            )
            .mappings()
            .one_or_none()
        )
        if row is None:
            return None
        references = self._read_references((connector_id,)).get(connector_id, ())
        return _connector_from_row(row, references)

    @_translate_storage_errors
    def list(
        self,
        *,
        limit: int,
        after: ConnectorId | None = None,
        include_archived: bool = False,
    ) -> ConnectorPage:
        self._require_transaction()
        page_size = validate_page_limit(limit)
        query = select(connectors)
        if after is not None:
            query = query.where(connectors.c.connector_id > str(after))
        if not include_archived:
            query = query.where(connectors.c.archived_at.is_(None))
        rows = (
            self._session.execute(query.order_by(connectors.c.connector_id).limit(page_size + 1))
            .mappings()
            .all()
        )
        page_rows = rows[:page_size]
        ids = tuple(_stored_connector_id(row["connector_id"]) for row in page_rows)
        reference_map = self._read_references(ids)
        records = tuple(
            _connector_from_row(row, reference_map.get(connector_id, ()))
            for row, connector_id in zip(page_rows, ids, strict=True)
        )
        next_cursor = records[-1].connector_id if len(rows) > page_size else None
        return ConnectorPage(items=records, next_cursor=next_cursor)

    @_translate_storage_errors
    def update_metadata(
        self,
        connector_id: ConnectorId,
        *,
        expected_row_version: int,
        display_name: str,
        updated_at: UtcTimestamp,
    ) -> ConnectorRecord:
        self._require_transaction()
        expected = _positive_int(expected_row_version, "expected row version")
        current = _require_connector_cas(self.get(connector_id), expected, None)
        if display_name == current.display_name:
            return current
        _require_incrementable(expected, "connector row version")
        if updated_at <= current.updated_at:
            raise InvalidRepositoryRequestError(
                "connector update time must be later than its current update time"
            )
        return self._update_connector(
            connector_id,
            expected_row_version=expected,
            expected_revision=None,
            values={
                "display_name": _bounded_text(display_name, "connector display name", maximum=160),
                "updated_at": str(updated_at),
                "row_version": expected + 1,
            },
        )

    @_translate_storage_errors
    def update_definition(
        self,
        connector_id: ConnectorId,
        *,
        expected_row_version: int,
        expected_revision: int,
        kind: str,
        configuration: ConfigurationDocument,
        capabilities: ConfigurationDocument,
        schema_discovery: ConfigurationDocument | None,
        updated_at: UtcTimestamp,
    ) -> ConnectorRecord:
        self._require_transaction()
        current = self.get(connector_id)
        if current is None:
            raise RecordNotFoundError("connector does not exist")
        if kind != current.kind:
            raise RecordStateConflictError("connector kind is immutable")
        reference_names = frozenset(ref.reference_name for ref in current.secret_references)
        _validate_secret_policy(configuration, reference_names)
        expected_row = _positive_int(expected_row_version, "expected row version")
        expected_content = _positive_int(expected_revision, "expected connector revision")
        current = _require_connector_cas(current, expected_row, expected_content)
        if (
            configuration == current.configuration
            and capabilities == current.capabilities
            and schema_discovery == current.schema_discovery
        ):
            return current
        _require_incrementable(expected_row, "connector row version")
        _require_incrementable(expected_content, "connector revision")
        if updated_at <= current.updated_at:
            raise InvalidRepositoryRequestError(
                "connector update time must be later than its current update time"
            )
        configuration_json = _encode_document(configuration)
        capabilities_json = _encode_document(capabilities)
        discovery_json = None if schema_discovery is None else _encode_document(schema_discovery)
        return self._update_connector(
            connector_id,
            expected_row_version=expected_row,
            expected_revision=expected_content,
            values={
                "configuration_json": configuration_json.text,
                "capabilities_json": capabilities_json.text,
                "schema_discovery_json": None if discovery_json is None else discovery_json.text,
                "revision": expected_content + 1,
                "updated_at": str(updated_at),
                "row_version": expected_row + 1,
            },
        )

    @_translate_storage_errors
    def archive(
        self,
        connector_id: ConnectorId,
        *,
        expected_row_version: int,
        archived_at: UtcTimestamp,
    ) -> ConnectorRecord:
        self._require_transaction()
        expected = _positive_int(expected_row_version, "expected row version")
        current = _require_connector_cas(self.get(connector_id), expected, None)
        _require_incrementable(expected, "connector row version")
        if archived_at <= current.updated_at or archived_at <= current.created_at:
            raise InvalidRepositoryRequestError(
                "connector archive time must be later than its stored times"
            )
        return self._update_connector(
            connector_id,
            expected_row_version=expected,
            expected_revision=None,
            values={
                "archived_at": str(archived_at),
                "updated_at": str(archived_at),
                "row_version": expected + 1,
            },
        )

    def _update_connector(
        self,
        connector_id: ConnectorId,
        *,
        expected_row_version: int,
        expected_revision: int | None,
        values: Mapping[str, object],
    ) -> ConnectorRecord:
        self._require_transaction()
        conditions = [
            connectors.c.connector_id == str(connector_id),
            connectors.c.row_version == expected_row_version,
            connectors.c.archived_at.is_(None),
        ]
        if expected_revision is not None:
            conditions.append(connectors.c.revision == expected_revision)
        row = (
            self._session.execute(
                update(connectors).where(*conditions).values(**values).returning(*connectors.c)
            )
            .mappings()
            .one_or_none()
        )
        if row is None:
            current = self.get(connector_id)
            _raise_connector_cas_failure(current, expected_row_version, expected_revision)
        references = self._read_references((connector_id,)).get(connector_id, ())
        return _connector_from_row(row, references)

    def _read_references(
        self, connector_ids: tuple[ConnectorId, ...]
    ) -> dict[ConnectorId, tuple[ConnectorSecretReference, ...]]:
        if not connector_ids:
            return {}
        rows = (
            self._session.execute(
                select(connector_secret_references)
                .where(
                    connector_secret_references.c.connector_id.in_(
                        [str(connector_id) for connector_id in connector_ids]
                    )
                )
                .order_by(
                    connector_secret_references.c.connector_id,
                    connector_secret_references.c.reference_name,
                )
            )
            .mappings()
            .all()
        )
        grouped: dict[ConnectorId, list[ConnectorSecretReference]] = {}
        for row in rows:
            connector_id = _stored_connector_id(row["connector_id"])
            reference = _secret_reference_from_row(row)
            grouped.setdefault(connector_id, []).append(reference)
        return {key: tuple(value) for key, value in grouped.items()}

    def _require_transaction(self) -> None:
        if not self._session.in_transaction():
            raise InvalidRepositoryRequestError("repository requires a caller-owned transaction")


def _pipeline_from_row(row: RowMapping) -> PipelineRecord:
    try:
        record = PipelineRecord(
            pipeline_id=_stored_pipeline_id(row["pipeline_id"]),
            display_name=_stored_text(row["display_name"], "pipeline display name", 160),
            description=_stored_optional_text(row["description"], "pipeline description"),
            created_at=_stored_timestamp(row["created_at"], "pipeline created time"),
            archived_at=_stored_optional_timestamp(row["archived_at"], "pipeline archive time"),
            row_version=_stored_positive_int(row["row_version"], "pipeline row version"),
        )
        if record.archived_at is not None and record.archived_at < record.created_at:
            raise CorruptRepositoryRecordError("pipeline archive time is corrupt")
        return record
    except CorruptRepositoryRecordError:
        raise
    except (KeyError, TypeError, ValueError) as error:
        raise CorruptRepositoryRecordError("pipeline record is corrupt") from error


def _pipeline_version_from_row(row: RowMapping) -> PipelineVersionRecord:
    try:
        specification = _decode_document(row["specification_json"], "pipeline specification")
        digest_value = cast(object, row["specification_sha256"])
        if not isinstance(digest_value, str):
            raise TypeError
        digest = Sha256Digest(digest_value).value
        calculated = hashlib.sha256(
            _encode_document(specification).text.encode("utf-8")
        ).hexdigest()
        if digest != calculated:
            raise CorruptRepositoryRecordError("pipeline specification digest is invalid")
        return PipelineVersionRecord(
            pipeline_id=_stored_pipeline_id(row["pipeline_id"]),
            version=PipelineVersion(
                _stored_positive_int(row["version_number"], "pipeline version")
            ),
            specification=specification,
            specification_sha256=digest,
            planner_format_version=_stored_positive_int(
                row["planner_format_version"], "planner format version"
            ),
            published_at=_stored_timestamp(row["published_at"], "publication time"),
        )
    except CorruptRepositoryRecordError:
        raise
    except (KeyError, TypeError, ValueError) as error:
        raise CorruptRepositoryRecordError("pipeline version record is corrupt") from error


def _connector_from_row(
    row: RowMapping, references: tuple[ConnectorSecretReference, ...]
) -> ConnectorRecord:
    try:
        record = ConnectorRecord(
            connector_id=_stored_connector_id(row["connector_id"]),
            kind=_stored_text(row["kind"], "connector kind", 96),
            display_name=_stored_text(row["display_name"], "connector display name", 160),
            configuration=_decode_document(row["configuration_json"], "connector configuration"),
            capabilities=_decode_document(row["capabilities_json"], "connector capabilities"),
            schema_discovery=_decode_optional_document(
                row["schema_discovery_json"], "connector schema discovery"
            ),
            secret_references=references,
            revision=_stored_positive_int(row["revision"], "connector revision"),
            created_at=_stored_timestamp(row["created_at"], "connector created time"),
            updated_at=_stored_timestamp(row["updated_at"], "connector updated time"),
            archived_at=_stored_optional_timestamp(row["archived_at"], "connector archive time"),
            row_version=_stored_positive_int(row["row_version"], "connector row version"),
        )
        if record.updated_at < record.created_at:
            raise CorruptRepositoryRecordError("connector update time is corrupt")
        if record.archived_at is not None and record.archived_at < record.created_at:
            raise CorruptRepositoryRecordError("connector archive time is corrupt")
        return record
    except CorruptRepositoryRecordError:
        raise
    except (KeyError, TypeError, ValueError) as error:
        raise CorruptRepositoryRecordError("connector record is corrupt") from error


def _secret_reference_from_row(row: RowMapping) -> ConnectorSecretReference:
    try:
        reference_value = cast(object, row["reference_name"])
        environment_value = cast(object, row["environment_variable_name"])
        if not isinstance(reference_value, str) or not isinstance(environment_value, str):
            raise TypeError
        reference = SecretReferenceName(reference_value).value
        environment = EnvironmentVariableName(environment_value).value
        _stored_timestamp(row["created_at"], "secret reference created time")
        return ConnectorSecretReference(
            reference_name=reference,
            environment_variable_name=environment,
        )
    except (KeyError, TypeError, ValueError) as error:
        raise CorruptRepositoryRecordError("connector secret reference is corrupt") from error


def _validate_secret_references(
    references: Sequence[ConnectorSecretReference],
) -> tuple[ConnectorSecretReference, ...]:
    canonical: list[ConnectorSecretReference] = []
    names: set[str] = set()
    for reference in references:
        reference_value = cast(object, reference)
        if not isinstance(reference_value, ConnectorSecretReference):
            raise InvalidRepositoryRequestError("secret references must use the public value type")
        try:
            name = SecretReferenceName(reference.reference_name).value
            environment = EnvironmentVariableName(reference.environment_variable_name).value
        except (TypeError, ValueError) as error:
            raise InvalidRepositoryRequestError("secret reference is not canonical") from error
        if name in names:
            raise InvalidRepositoryRequestError("secret reference names must be unique")
        names.add(name)
        canonical.append(
            ConnectorSecretReference(
                reference_name=name,
                environment_variable_name=environment,
            )
        )
    canonical.sort(key=lambda reference: reference.reference_name)
    return tuple(canonical)


def _validate_secret_policy(
    document: ConfigurationDocument, reference_names: frozenset[str]
) -> None:
    def visit(value: object) -> None:
        if isinstance(value, dict):
            mapping = cast(dict[str, object], value)
            for raw_key, child in mapping.items():
                key = unicodedata.normalize("NFC", raw_key).casefold().replace("-", "_")
                sensitive = any(
                    key == part or f"_{part}_" in f"_{key}_" for part in _SENSITIVE_KEY_PARTS
                )
                if sensitive:
                    if not key.endswith("_reference"):
                        raise UnsafeConnectorConfigurationError(
                            "connector configuration may contain secret references only"
                        )
                    if not isinstance(child, str) or child not in reference_names:
                        raise UnsafeConnectorConfigurationError(
                            "connector secret reference is not declared"
                        )
                visit(child)
        elif isinstance(value, list):
            for child in cast(list[object], value):
                visit(child)

    visit(document.to_mapping())


def _encode_document(document: ConfigurationDocument) -> CanonicalStorageJson:
    document_value = cast(object, document)
    if not isinstance(document_value, ConfigurationDocument):
        raise InvalidRepositoryRequestError("configuration must use the public document type")
    primitive = cast(StoragePrimitive, document.to_mapping())
    encoded = CanonicalStorageJson.encode(primitive)
    if len(encoded.text.encode("utf-8")) > MAX_CANONICAL_DOCUMENT_BYTES:
        raise InvalidRepositoryRequestError(
            "configuration document exceeds the supported encoded size"
        )
    return encoded


def _decode_document(value: object, subject: str) -> ConfigurationDocument:
    if not isinstance(value, str):
        raise CorruptRepositoryRecordError(f"{subject} is corrupt")
    try:
        decoded = CanonicalStorageJson(value).decode()
        if not isinstance(decoded, dict):
            raise ValueError("object required")
        return ConfigurationDocument.from_mapping(cast(dict[str, object], decoded))
    except (TypeError, ValueError) as error:
        raise CorruptRepositoryRecordError(f"{subject} is corrupt") from error


def _decode_optional_document(value: object, subject: str) -> ConfigurationDocument | None:
    return None if value is None else _decode_document(value, subject)


def _bounded_text(value: object, subject: str, maximum: int) -> str:
    if not isinstance(value, str):
        raise InvalidRepositoryRequestError(f"{subject} must be text")
    if not 1 <= len(value) <= maximum:
        raise InvalidRepositoryRequestError(f"{subject} has an invalid length")
    if unicodedata.normalize("NFC", value) != value:
        raise InvalidRepositoryRequestError(f"{subject} must use normalized Unicode")
    return value


def _optional_text(value: object, subject: str) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str):
        raise InvalidRepositoryRequestError(f"{subject} must be text or null")
    if unicodedata.normalize("NFC", value) != value:
        raise InvalidRepositoryRequestError(f"{subject} must use normalized Unicode")
    return value


def _positive_int(value: object, subject: str) -> int:
    if (
        isinstance(value, bool)
        or not isinstance(value, int)
        or not 1 <= value <= MAX_PERSISTED_INTEGER
    ):
        raise InvalidRepositoryRequestError(
            f"{subject} must be an integer within the supported range"
        )
    return value


def _require_incrementable(value: int, subject: str) -> None:
    if value >= MAX_PERSISTED_INTEGER:
        raise RecordStateConflictError(f"{subject} cannot advance beyond the supported maximum")


def _stored_text(value: object, subject: str, maximum: int) -> str:
    try:
        return _bounded_text(value, subject, maximum)
    except InvalidRepositoryRequestError as error:
        raise CorruptRepositoryRecordError(f"{subject} is corrupt") from error


def _stored_optional_text(value: object, subject: str) -> str | None:
    try:
        return _optional_text(value, subject)
    except InvalidRepositoryRequestError as error:
        raise CorruptRepositoryRecordError(f"{subject} is corrupt") from error


def _stored_positive_int(value: object, subject: str) -> int:
    try:
        return _positive_int(value, subject)
    except InvalidRepositoryRequestError as error:
        raise CorruptRepositoryRecordError(f"{subject} is corrupt") from error


def _stored_pipeline_id(value: object) -> PipelineId:
    try:
        if not isinstance(value, str):
            raise TypeError
        return PipelineId.parse(value)
    except (TypeError, ValueError) as error:
        raise CorruptRepositoryRecordError("pipeline identifier is corrupt") from error


def _stored_connector_id(value: object) -> ConnectorId:
    try:
        if not isinstance(value, str):
            raise TypeError
        return ConnectorId.parse(value)
    except (TypeError, ValueError) as error:
        raise CorruptRepositoryRecordError("connector identifier is corrupt") from error


def _stored_timestamp(value: object, subject: str) -> UtcTimestamp:
    try:
        if not isinstance(value, str):
            raise TypeError
        timestamp = UtcTimestamp.parse(value)
        if str(timestamp) != value:
            raise ValueError
        return timestamp
    except (TypeError, ValueError) as error:
        raise CorruptRepositoryRecordError(f"{subject} is corrupt") from error


def _stored_optional_timestamp(value: object, subject: str) -> UtcTimestamp | None:
    return None if value is None else _stored_timestamp(value, subject)


def _raise_cas_failure(current: PipelineRecord | None, expected: int, subject: str) -> NoReturn:
    if current is None:
        raise RecordNotFoundError(f"{subject} does not exist")
    if current.archived_at is not None:
        raise RecordStateConflictError(f"{subject} is already archived")
    if current.row_version != expected:
        raise StaleRowVersionError(f"{subject} row version is stale")
    raise RecordStateConflictError(f"{subject} update was rejected")


def _require_pipeline_cas(current: PipelineRecord | None, expected: int) -> PipelineRecord:
    if current is None:
        raise RecordNotFoundError("pipeline does not exist")
    if current.archived_at is not None:
        raise RecordStateConflictError("pipeline is already archived")
    if current.row_version != expected:
        raise StaleRowVersionError("pipeline row version is stale")
    return current


def _raise_connector_cas_failure(
    current: ConnectorRecord | None,
    expected_row: int,
    expected_revision: int | None,
) -> NoReturn:
    if current is None:
        raise RecordNotFoundError("connector does not exist")
    if current.archived_at is not None:
        raise RecordStateConflictError("connector is already archived")
    if current.row_version != expected_row:
        raise StaleRowVersionError("connector row version is stale")
    if expected_revision is not None and current.revision != expected_revision:
        raise StaleConnectorRevisionError("connector revision is stale")
    raise RecordStateConflictError("connector update was rejected")


def _require_connector_cas(
    current: ConnectorRecord | None, expected_row: int, expected_revision: int | None
) -> ConnectorRecord:
    if current is None:
        raise RecordNotFoundError("connector does not exist")
    if current.archived_at is not None:
        raise RecordStateConflictError("connector is already archived")
    if current.row_version != expected_row:
        raise StaleRowVersionError("connector row version is stale")
    if expected_revision is not None and current.revision != expected_revision:
        raise StaleConnectorRevisionError("connector revision is stale")
    return current


__all__ = [
    "MAX_CANONICAL_DOCUMENT_BYTES",
    "SqlAlchemyConnectorRepository",
    "SqlAlchemyPipelineRepository",
]

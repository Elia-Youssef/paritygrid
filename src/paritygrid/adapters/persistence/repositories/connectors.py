"""SQLAlchemy repository for connector definitions and secret references."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import cast

from sqlalchemy import insert, select, update
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.orm import Session

from paritygrid.adapters.persistence.repositories.common import (
    bounded_text,
    encode_document,
    positive_int,
    require_connector_id,
    require_document,
    require_incrementable,
    require_timestamp,
    translate_storage_errors,
    validate_secret_policy,
)
from paritygrid.adapters.persistence.repositories.mapping import (
    StoredSecretReference,
    connector_from_row,
    raise_connector_cas_failure,
    require_connector_cas,
    stored_connector_id,
    stored_secret_reference_from_row,
)
from paritygrid.adapters.persistence.schema import connector_secret_references, connectors
from paritygrid.adapters.persistence.values import EnvironmentVariableName, SecretReferenceName
from paritygrid.application.ports import (
    ConfigurationDocument,
    ConnectorPage,
    ConnectorRecord,
    ConnectorRepository,
    ConnectorSecretReference,
    CorruptRepositoryRecordError,
    DuplicateRecordError,
    InvalidRepositoryRequestError,
    RecordNotFoundError,
    RecordStateConflictError,
    validate_page_limit,
)
from paritygrid.domain.models import ConnectorId, UtcTimestamp


class SqlAlchemyConnectorRepository(ConnectorRepository):
    """Persist connector definitions without resolving or storing secret values."""

    def __init__(self, session: Session) -> None:
        self._session = session

    @translate_storage_errors
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
        identity = require_connector_id(connector_id)
        timestamp = require_timestamp(created_at, "connector creation time")
        connector_kind = bounded_text(kind, "connector kind", 96)
        name = bounded_text(display_name, "connector display name", 160)
        config = require_document(configuration, "connector configuration")
        capability_doc = require_document(capabilities, "connector capabilities")
        discovery_doc = (
            None
            if schema_discovery is None
            else require_document(schema_discovery, "connector schema discovery")
        )
        references = validate_secret_references(secret_references)
        validate_secret_policy(config, frozenset(ref.reference_name for ref in references))
        configuration_json = encode_document(config)
        capabilities_json = encode_document(capability_doc)
        discovery_json = None if discovery_doc is None else encode_document(discovery_doc)
        inserted = self._session.execute(
            sqlite_insert(connectors)
            .values(
                connector_id=str(identity),
                kind=connector_kind,
                display_name=name,
                configuration_json=configuration_json.text,
                capabilities_json=capabilities_json.text,
                schema_discovery_json=None if discovery_json is None else discovery_json.text,
                revision=1,
                created_at=str(timestamp),
                updated_at=str(timestamp),
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
                        "connector_id": str(identity),
                        "reference_name": reference.reference_name,
                        "environment_variable_name": reference.environment_variable_name,
                        "created_at": str(timestamp),
                    }
                    for reference in references
                ],
            )
        created = self.get(identity)
        if created is None:
            raise CorruptRepositoryRecordError("created connector could not be read")
        return created

    @translate_storage_errors
    def get(self, connector_id: ConnectorId) -> ConnectorRecord | None:
        self._require_transaction()
        identity = require_connector_id(connector_id)
        row = (
            self._session.execute(
                select(connectors).where(connectors.c.connector_id == str(identity))
            )
            .mappings()
            .one_or_none()
        )
        if row is None:
            return None
        references = self._read_references((identity,)).get(identity, ())
        return connector_from_row(row, references)

    @translate_storage_errors
    def list(
        self,
        *,
        limit: int,
        after: ConnectorId | None = None,
        include_archived: bool = False,
    ) -> ConnectorPage:
        self._require_transaction()
        page_size = validate_page_limit(limit)
        if type(include_archived) is not bool:
            raise InvalidRepositoryRequestError("include archived must be boolean")
        cursor = None if after is None else require_connector_id(after)
        query = select(connectors)
        if cursor is not None:
            query = query.where(connectors.c.connector_id > str(cursor))
        if not include_archived:
            query = query.where(connectors.c.archived_at.is_(None))
        rows = (
            self._session.execute(query.order_by(connectors.c.connector_id).limit(page_size + 1))
            .mappings()
            .all()
        )
        page_rows = rows[:page_size]
        ids = tuple(stored_connector_id(row["connector_id"]) for row in page_rows)
        reference_map = self._read_references(ids)
        records = tuple(
            connector_from_row(row, reference_map.get(connector_id, ()))
            for row, connector_id in zip(page_rows, ids, strict=True)
        )
        next_cursor = records[-1].connector_id if len(rows) > page_size else None
        return ConnectorPage(items=records, next_cursor=next_cursor)

    @translate_storage_errors
    def update_metadata(
        self,
        connector_id: ConnectorId,
        *,
        expected_row_version: int,
        display_name: str,
        updated_at: UtcTimestamp,
    ) -> ConnectorRecord:
        self._require_transaction()
        identity = require_connector_id(connector_id)
        expected = positive_int(expected_row_version, "expected row version")
        timestamp = require_timestamp(updated_at, "connector update time")
        name = bounded_text(display_name, "connector display name", 160)
        current = require_connector_cas(self.get(identity), expected, None)
        if name == current.display_name:
            return current
        require_incrementable(expected, "connector row version")
        if timestamp <= current.updated_at:
            raise InvalidRepositoryRequestError(
                "connector update time must be later than its current update time"
            )
        return self._update_connector(
            identity,
            expected_row_version=expected,
            expected_revision=None,
            values={
                "display_name": name,
                "updated_at": str(timestamp),
                "row_version": expected + 1,
            },
        )

    @translate_storage_errors
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
        identity = require_connector_id(connector_id)
        expected_row = positive_int(expected_row_version, "expected row version")
        expected_content = positive_int(expected_revision, "expected connector revision")
        timestamp = require_timestamp(updated_at, "connector update time")
        connector_kind = bounded_text(kind, "connector kind", 96)
        config = require_document(configuration, "connector configuration")
        capability_doc = require_document(capabilities, "connector capabilities")
        discovery_doc = (
            None
            if schema_discovery is None
            else require_document(schema_discovery, "connector schema discovery")
        )
        current = self.get(identity)
        if current is None:
            raise RecordNotFoundError("connector does not exist")
        if connector_kind != current.kind:
            raise RecordStateConflictError("connector kind is immutable")
        reference_names = frozenset(ref.reference_name for ref in current.secret_references)
        validate_secret_policy(config, reference_names)
        current = require_connector_cas(current, expected_row, expected_content)
        if (
            config == current.configuration
            and capability_doc == current.capabilities
            and discovery_doc == current.schema_discovery
        ):
            return current
        require_incrementable(expected_row, "connector row version")
        require_incrementable(expected_content, "connector revision")
        if timestamp <= current.updated_at:
            raise InvalidRepositoryRequestError(
                "connector update time must be later than its current update time"
            )
        configuration_json = encode_document(config)
        capabilities_json = encode_document(capability_doc)
        discovery_json = None if discovery_doc is None else encode_document(discovery_doc)
        return self._update_connector(
            identity,
            expected_row_version=expected_row,
            expected_revision=expected_content,
            values={
                "configuration_json": configuration_json.text,
                "capabilities_json": capabilities_json.text,
                "schema_discovery_json": None if discovery_json is None else discovery_json.text,
                "revision": expected_content + 1,
                "updated_at": str(timestamp),
                "row_version": expected_row + 1,
            },
        )

    @translate_storage_errors
    def archive(
        self,
        connector_id: ConnectorId,
        *,
        expected_row_version: int,
        archived_at: UtcTimestamp,
    ) -> ConnectorRecord:
        self._require_transaction()
        identity = require_connector_id(connector_id)
        expected = positive_int(expected_row_version, "expected row version")
        timestamp = require_timestamp(archived_at, "connector archive time")
        current = require_connector_cas(self.get(identity), expected, None)
        require_incrementable(expected, "connector row version")
        if timestamp <= current.updated_at or timestamp <= current.created_at:
            raise InvalidRepositoryRequestError(
                "connector archive time must be later than its stored times"
            )
        return self._update_connector(
            identity,
            expected_row_version=expected,
            expected_revision=None,
            values={
                "archived_at": str(timestamp),
                "updated_at": str(timestamp),
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
            raise_connector_cas_failure(
                self.get(connector_id), expected_row_version, expected_revision
            )
        references = self._read_references((connector_id,)).get(connector_id, ())
        return connector_from_row(row, references)

    def _read_references(
        self, connector_ids: tuple[ConnectorId, ...]
    ) -> dict[ConnectorId, tuple[StoredSecretReference, ...]]:
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
        grouped: dict[ConnectorId, list[StoredSecretReference]] = {}
        for row in rows:
            connector_id = stored_connector_id(row["connector_id"])
            grouped.setdefault(connector_id, []).append(stored_secret_reference_from_row(row))
        return {key: tuple(value) for key, value in grouped.items()}

    def _require_transaction(self) -> None:
        if not self._session.in_transaction():
            raise InvalidRepositoryRequestError("repository requires a caller-owned transaction")


def validate_secret_references(
    references: Sequence[ConnectorSecretReference],
) -> tuple[ConnectorSecretReference, ...]:
    """Canonicalize create-only references without reading secret values."""
    if isinstance(references, (str, bytes, bytearray)):
        raise InvalidRepositoryRequestError("secret references must be a sequence of public values")
    canonical: list[ConnectorSecretReference] = []
    names: set[str] = set()
    for reference in references:
        reference_value = cast(object, reference)
        if type(reference_value) is not ConnectorSecretReference:
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
            ConnectorSecretReference(reference_name=name, environment_variable_name=environment)
        )
    canonical.sort(key=lambda reference: reference.reference_name)
    return tuple(canonical)

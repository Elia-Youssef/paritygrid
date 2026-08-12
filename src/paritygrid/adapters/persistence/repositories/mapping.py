"""Strict row-to-contract mapping for configuration persistence."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from typing import NoReturn, cast

from sqlalchemy.engine import RowMapping

from paritygrid.adapters.persistence.repositories.common import (
    bounded_text,
    decode_document,
    decode_optional_document,
    encode_document,
    optional_text,
    positive_int,
    validate_stored_secret_policy,
)
from paritygrid.adapters.persistence.values import (
    EnvironmentVariableName,
    SecretReferenceName,
    Sha256Digest,
)
from paritygrid.application.ports import (
    ConnectorRecord,
    ConnectorSecretReference,
    CorruptRepositoryRecordError,
    InvalidRepositoryRequestError,
    PipelineRecord,
    PipelineVersionRecord,
    RecordNotFoundError,
    RecordStateConflictError,
    StaleConnectorRevisionError,
    StaleRowVersionError,
)
from paritygrid.domain.models import ConnectorId, PipelineId, PipelineVersion, UtcTimestamp


@dataclass(frozen=True, slots=True)
class StoredSecretReference:
    """A validated public reference paired with its persisted creation time."""

    value: ConnectorSecretReference
    created_at: UtcTimestamp


def pipeline_from_row(row: RowMapping) -> PipelineRecord:
    """Map a stored pipeline row and enforce cross-column chronology."""
    try:
        record = PipelineRecord(
            pipeline_id=stored_pipeline_id(row["pipeline_id"]),
            display_name=stored_text(row["display_name"], "pipeline display name", 160),
            description=stored_optional_text(row["description"], "pipeline description"),
            created_at=stored_timestamp(row["created_at"], "pipeline created time"),
            archived_at=stored_optional_timestamp(row["archived_at"], "pipeline archive time"),
            row_version=stored_positive_int(row["row_version"], "pipeline row version"),
        )
        if record.archived_at is not None and record.archived_at <= record.created_at:
            raise CorruptRepositoryRecordError("pipeline archive time is corrupt")
        return record
    except CorruptRepositoryRecordError:
        raise
    except (KeyError, TypeError, ValueError) as error:
        raise CorruptRepositoryRecordError("pipeline record is corrupt") from error


def pipeline_version_from_row(
    row: RowMapping, *, pipeline_created_at: UtcTimestamp | None = None
) -> PipelineVersionRecord:
    """Map an immutable version row, rechecking its digest and chronology."""
    try:
        specification = decode_document(row["specification_json"], "pipeline specification")
        digest_value = cast(object, row["specification_sha256"])
        if not isinstance(digest_value, str):
            raise TypeError
        digest = Sha256Digest(digest_value).value
        calculated = hashlib.sha256(encode_document(specification).text.encode("utf-8")).hexdigest()
        if digest != calculated:
            raise CorruptRepositoryRecordError("pipeline specification digest is invalid")
        record = PipelineVersionRecord(
            pipeline_id=stored_pipeline_id(row["pipeline_id"]),
            version=PipelineVersion(stored_positive_int(row["version_number"], "pipeline version")),
            specification=specification,
            specification_sha256=digest,
            planner_format_version=stored_positive_int(
                row["planner_format_version"], "planner format version"
            ),
            published_at=stored_timestamp(row["published_at"], "publication time"),
        )
        if pipeline_created_at is not None and record.published_at < pipeline_created_at:
            raise CorruptRepositoryRecordError("pipeline publication time is corrupt")
        return record
    except CorruptRepositoryRecordError:
        raise
    except (KeyError, TypeError, ValueError) as error:
        raise CorruptRepositoryRecordError("pipeline version record is corrupt") from error


def connector_from_row(
    row: RowMapping, references: tuple[StoredSecretReference, ...]
) -> ConnectorRecord:
    """Map a connector and revalidate chronology and secret-reference coherence."""
    try:
        public_references = tuple(reference.value for reference in references)
        record = ConnectorRecord(
            connector_id=stored_connector_id(row["connector_id"]),
            kind=stored_text(row["kind"], "connector kind", 96),
            display_name=stored_text(row["display_name"], "connector display name", 160),
            configuration=decode_document(row["configuration_json"], "connector configuration"),
            capabilities=decode_document(row["capabilities_json"], "connector capabilities"),
            schema_discovery=decode_optional_document(
                row["schema_discovery_json"], "connector schema discovery"
            ),
            secret_references=public_references,
            revision=stored_positive_int(row["revision"], "connector revision"),
            created_at=stored_timestamp(row["created_at"], "connector created time"),
            updated_at=stored_timestamp(row["updated_at"], "connector updated time"),
            archived_at=stored_optional_timestamp(row["archived_at"], "connector archive time"),
            row_version=stored_positive_int(row["row_version"], "connector row version"),
        )
        if record.updated_at < record.created_at:
            raise CorruptRepositoryRecordError("connector update time is corrupt")
        if record.archived_at is not None and record.archived_at != record.updated_at:
            raise CorruptRepositoryRecordError("connector archive time is corrupt")
        if any(reference.created_at != record.created_at for reference in references):
            raise CorruptRepositoryRecordError("connector secret reference time is corrupt")
        validate_stored_secret_policy(record.configuration, public_references)
        return record
    except CorruptRepositoryRecordError:
        raise
    except (KeyError, TypeError, ValueError) as error:
        raise CorruptRepositoryRecordError("connector record is corrupt") from error


def stored_secret_reference_from_row(row: RowMapping) -> StoredSecretReference:
    """Map a secret-reference row without discarding its coherence timestamp."""
    try:
        reference_value = cast(object, row["reference_name"])
        environment_value = cast(object, row["environment_variable_name"])
        if not isinstance(reference_value, str) or not isinstance(environment_value, str):
            raise TypeError
        return StoredSecretReference(
            value=ConnectorSecretReference(
                reference_name=SecretReferenceName(reference_value).value,
                environment_variable_name=EnvironmentVariableName(environment_value).value,
            ),
            created_at=stored_timestamp(row["created_at"], "secret reference created time"),
        )
    except (KeyError, TypeError, ValueError) as error:
        raise CorruptRepositoryRecordError("connector secret reference is corrupt") from error


def secret_reference_from_row(row: RowMapping) -> ConnectorSecretReference:
    """Compatibility mapper returning only the public reference value."""
    return stored_secret_reference_from_row(row).value


def stored_text(value: object, subject: str, maximum: int) -> str:
    try:
        return bounded_text(value, subject, maximum)
    except InvalidRepositoryRequestError as error:
        raise CorruptRepositoryRecordError(f"{subject} is corrupt") from error


def stored_optional_text(value: object, subject: str) -> str | None:
    try:
        return optional_text(value, subject)
    except InvalidRepositoryRequestError as error:
        raise CorruptRepositoryRecordError(f"{subject} is corrupt") from error


def stored_positive_int(value: object, subject: str) -> int:
    try:
        return positive_int(value, subject)
    except InvalidRepositoryRequestError as error:
        raise CorruptRepositoryRecordError(f"{subject} is corrupt") from error


def stored_pipeline_id(value: object) -> PipelineId:
    try:
        if not isinstance(value, str):
            raise TypeError
        return PipelineId.parse(value)
    except (TypeError, ValueError) as error:
        raise CorruptRepositoryRecordError("pipeline identifier is corrupt") from error


def stored_connector_id(value: object) -> ConnectorId:
    try:
        if not isinstance(value, str):
            raise TypeError
        return ConnectorId.parse(value)
    except (TypeError, ValueError) as error:
        raise CorruptRepositoryRecordError("connector identifier is corrupt") from error


def stored_timestamp(value: object, subject: str) -> UtcTimestamp:
    try:
        if not isinstance(value, str):
            raise TypeError
        timestamp = UtcTimestamp.parse(value)
        if str(timestamp) != value:
            raise ValueError
        return timestamp
    except (TypeError, ValueError) as error:
        raise CorruptRepositoryRecordError(f"{subject} is corrupt") from error


def stored_optional_timestamp(value: object, subject: str) -> UtcTimestamp | None:
    return None if value is None else stored_timestamp(value, subject)


def raise_pipeline_cas_failure(
    current: PipelineRecord | None, expected: int, subject: str
) -> NoReturn:
    if current is None:
        raise RecordNotFoundError(f"{subject} does not exist")
    if current.archived_at is not None:
        raise RecordStateConflictError(f"{subject} is already archived")
    if current.row_version != expected:
        raise StaleRowVersionError(f"{subject} row version is stale")
    raise RecordStateConflictError(f"{subject} update was rejected")


def require_pipeline_cas(current: PipelineRecord | None, expected: int) -> PipelineRecord:
    if current is None:
        raise RecordNotFoundError("pipeline does not exist")
    if current.archived_at is not None:
        raise RecordStateConflictError("pipeline is already archived")
    if current.row_version != expected:
        raise StaleRowVersionError("pipeline row version is stale")
    return current


def raise_connector_cas_failure(
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


def require_connector_cas(
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

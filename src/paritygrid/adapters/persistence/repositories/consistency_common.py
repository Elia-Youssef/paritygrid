"""Validation, canonical encoding, and error translation for consistency storage."""

import hashlib
import re
import unicodedata
from collections.abc import Callable, Sequence
from functools import wraps
from typing import cast

from sqlalchemy.exc import InterfaceError, OperationalError, SQLAlchemyError

from paritygrid.adapters.persistence.repositories.common import MAX_CANONICAL_DOCUMENT_BYTES
from paritygrid.adapters.persistence.values import CanonicalStorageJson, StoragePrimitive
from paritygrid.application.ports.configuration import ConfigurationDocument
from paritygrid.application.ports.consistency import (
    MAX_CONSISTENCY_SEQUENCE,
    MAX_EVENT_BATCH_SIZE,
    CheckpointVersion,
    ConsistencyCorruptionError,
    ConsistencyInvalidRequestError,
    ConsistencyStateConflictError,
    ConsistencyStorageError,
    ConsistencyStorageUnavailableError,
    EventSequence,
    EventSubjectKind,
    IdempotencyCursor,
    PendingExecutionEvent,
    RedactedDocument,
)
from paritygrid.domain.models import ArtifactId, NodeId, RunId, UtcTimestamp, WorkItemId
from paritygrid.domain.pipeline import PartitionKey

_EVENT_KIND_PATTERN = re.compile(r"[a-z][a-z0-9]*(?:_[a-z0-9]+)*", flags=re.ASCII)
_IDEMPOTENCY_SCOPE_PATTERN = re.compile(
    r"[a-z][a-z0-9]*(?:[-_][a-z0-9]+)*(?::[a-z][a-z0-9]*(?:[-_][a-z0-9]+)*)*",
    flags=re.ASCII,
)
_PORTABLE_IDENTITY_PATTERN = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:-]*", flags=re.ASCII)


def translate_consistency_storage_errors[**P, R](
    operation: Callable[P, R],
) -> Callable[P, R]:
    """Replace database implementation failures with a redacted public error."""

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
            raise ConsistencyStorageUnavailableError(
                "Consistency storage is unavailable."
            ) from None
        raise ConsistencyStorageError("Consistency storage operation failed.") from None

    return translated


def require_exact[T](value: object, expected_type: type[T], subject: str) -> T:
    if type(value) is not expected_type:
        raise ConsistencyInvalidRequestError(f"{subject} must use {expected_type.__name__}")
    return cast(T, value)


def require_run_id(value: object) -> RunId:
    return require_exact(value, RunId, "run identifier")


def require_node_id(value: object) -> NodeId:
    return require_exact(value, NodeId, "node identifier")


def require_work_item_id(value: object) -> WorkItemId:
    return require_exact(value, WorkItemId, "work-item identifier")


def require_artifact_id(value: object) -> ArtifactId:
    return require_exact(value, ArtifactId, "artifact identifier")


def require_partition_key(value: object) -> PartitionKey:
    return require_exact(value, PartitionKey, "partition key")


def require_checkpoint_version(value: object) -> CheckpointVersion:
    return require_exact(value, CheckpointVersion, "checkpoint version")


def require_event_sequence(value: object) -> EventSequence:
    return require_exact(value, EventSequence, "event sequence")


def require_timestamp(value: object, subject: str) -> UtcTimestamp:
    return require_exact(value, UtcTimestamp, subject)


def require_document(value: object, subject: str) -> ConfigurationDocument:
    return require_exact(value, ConfigurationDocument, subject)


def require_redacted_document(value: object, subject: str) -> RedactedDocument:
    return require_exact(value, RedactedDocument, subject)


def bounded_text(value: object, subject: str, maximum: int) -> str:
    if type(value) is not str:
        raise ConsistencyInvalidRequestError(f"{subject} must be text")
    if not 1 <= len(value) <= maximum:
        raise ConsistencyInvalidRequestError(f"{subject} has an invalid length")
    if unicodedata.normalize("NFC", value) != value:
        raise ConsistencyInvalidRequestError(f"{subject} must use normalized Unicode")
    return value


def optional_text(value: object, subject: str, maximum: int) -> str | None:
    return None if value is None else bounded_text(value, subject, maximum)


def portable_identity(value: object, subject: str, maximum: int) -> str:
    """Validate one portable header-like identifier without normalization."""
    identity = bounded_text(value, subject, maximum)
    if _PORTABLE_IDENTITY_PATTERN.fullmatch(identity) is None:
        raise ConsistencyInvalidRequestError(f"{subject} must use portable ASCII")
    return identity


def idempotency_scope(value: object) -> str:
    """Validate a canonical command scope."""
    scope = bounded_text(value, "idempotency scope", 96)
    if _IDEMPOTENCY_SCOPE_PATTERN.fullmatch(scope) is None:
        raise ConsistencyInvalidRequestError("idempotency scope must use canonical lowercase ASCII")
    return scope


def event_kind(value: object) -> str:
    kind = bounded_text(value, "event kind", 96)
    if _EVENT_KIND_PATTERN.fullmatch(kind) is None:
        raise ConsistencyInvalidRequestError("event kind must use canonical lowercase snake_case")
    return kind


def positive_int(value: object, subject: str) -> int:
    if type(value) is not int or not 1 <= value <= MAX_CONSISTENCY_SEQUENCE:
        raise ConsistencyInvalidRequestError(
            f"{subject} must be an integer within the supported range"
        )
    return value


def require_incrementable(value: int, subject: str) -> None:
    if value >= MAX_CONSISTENCY_SEQUENCE:
        raise ConsistencyStateConflictError(
            f"{subject} cannot advance beyond the supported maximum"
        )


def encode_document(document: ConfigurationDocument, subject: str) -> CanonicalStorageJson:
    exact = require_document(document, subject)
    return _encode_mapping(exact.to_mapping(), subject)


def encode_redacted_document(document: RedactedDocument, subject: str) -> CanonicalStorageJson:
    exact = require_redacted_document(document, subject)
    return _encode_mapping(exact.to_mapping(), subject)


def _encode_mapping(value: dict[str, object], subject: str) -> CanonicalStorageJson:
    try:
        encoded = CanonicalStorageJson.encode(cast(StoragePrimitive, value))
    except (TypeError, ValueError) as error:
        raise ConsistencyInvalidRequestError(f"{subject} is invalid") from error
    if len(encoded.text.encode("utf-8")) > MAX_CANONICAL_DOCUMENT_BYTES:
        raise ConsistencyInvalidRequestError(f"{subject} exceeds the supported encoded size")
    return encoded


def request_digest(document: ConfigurationDocument) -> str:
    """Hash exact canonical storage JSON UTF-8 bytes for idempotency matching."""
    encoded = encode_document(document, "idempotency request")
    return hashlib.sha256(encoded.text.encode("utf-8")).hexdigest()


def decode_document(value: object, subject: str) -> ConfigurationDocument:
    mapping = _decode_mapping(value, subject)
    try:
        return ConfigurationDocument.from_mapping(mapping)
    except (TypeError, ValueError) as error:
        raise ConsistencyCorruptionError(f"{subject} is corrupt") from error


def decode_optional_document(value: object, subject: str) -> ConfigurationDocument | None:
    return None if value is None else decode_document(value, subject)


def decode_redacted_document(value: object, subject: str) -> RedactedDocument:
    mapping = _decode_mapping(value, subject)
    try:
        return RedactedDocument.from_mapping(mapping)
    except (ConsistencyInvalidRequestError, TypeError, ValueError) as error:
        raise ConsistencyCorruptionError(f"{subject} is corrupt") from error


def _decode_mapping(value: object, subject: str) -> dict[str, object]:
    if type(value) is not str:
        raise ConsistencyCorruptionError(f"{subject} is corrupt")
    try:
        decoded = CanonicalStorageJson(value).decode()
        if not isinstance(decoded, dict):
            raise ValueError
        return cast(dict[str, object], decoded)
    except (TypeError, ValueError) as error:
        raise ConsistencyCorruptionError(f"{subject} is corrupt") from error


def validate_events(value: Sequence[PendingExecutionEvent]) -> tuple[PendingExecutionEvent, ...]:
    if isinstance(value, (str, bytes, bytearray)):
        raise ConsistencyInvalidRequestError("events must be a nonempty bounded sequence")
    events = tuple(value)
    if not 1 <= len(events) <= MAX_EVENT_BATCH_SIZE:
        raise ConsistencyInvalidRequestError("events must be a nonempty bounded sequence")
    return tuple(require_exact(item, PendingExecutionEvent, "execution event") for item in events)


def require_event_subject_kind(value: object) -> EventSubjectKind:
    return require_exact(value, EventSubjectKind, "event subject kind")


def require_idempotency_cursor(value: object) -> IdempotencyCursor:
    return require_exact(value, IdempotencyCursor, "idempotency cursor")


def stored_text(value: object, subject: str, maximum: int) -> str:
    try:
        return bounded_text(value, subject, maximum)
    except ConsistencyInvalidRequestError as error:
        raise ConsistencyCorruptionError(f"{subject} is corrupt") from error


def stored_optional_text(value: object, subject: str, maximum: int) -> str | None:
    return None if value is None else stored_text(value, subject, maximum)


def stored_event_kind(value: object) -> str:
    try:
        return event_kind(value)
    except ConsistencyInvalidRequestError as error:
        raise ConsistencyCorruptionError("event kind is corrupt") from error


def stored_portable_identity(value: object, subject: str, maximum: int) -> str:
    try:
        return portable_identity(value, subject, maximum)
    except ConsistencyInvalidRequestError as error:
        raise ConsistencyCorruptionError(f"{subject} is corrupt") from error


def stored_idempotency_scope(value: object) -> str:
    try:
        return idempotency_scope(value)
    except ConsistencyInvalidRequestError as error:
        raise ConsistencyCorruptionError("idempotency scope is corrupt") from error


def stored_positive_int(value: object, subject: str) -> int:
    try:
        return positive_int(value, subject)
    except ConsistencyInvalidRequestError as error:
        raise ConsistencyCorruptionError(f"{subject} is corrupt") from error


def stored_nonnegative_int(value: object, subject: str) -> int:
    if type(value) is not int or not 0 <= value <= MAX_CONSISTENCY_SEQUENCE:
        raise ConsistencyCorruptionError(f"{subject} is corrupt")
    return value


def stored_timestamp(value: object, subject: str) -> UtcTimestamp:
    if type(value) is not str:
        raise ConsistencyCorruptionError(f"{subject} is corrupt")
    try:
        return UtcTimestamp.parse(value)
    except (TypeError, ValueError) as error:
        raise ConsistencyCorruptionError(f"{subject} is corrupt") from error


def stored_optional_timestamp(value: object, subject: str) -> UtcTimestamp | None:
    return None if value is None else stored_timestamp(value, subject)


def stored_run_id(value: object) -> RunId:
    return _stored_identifier(value, RunId, "run identifier")


def stored_node_id(value: object) -> NodeId:
    return _stored_identifier(value, NodeId, "node identifier")


def stored_work_item_id(value: object) -> WorkItemId:
    return _stored_identifier(value, WorkItemId, "work-item identifier")


def stored_artifact_id(value: object) -> ArtifactId:
    return _stored_identifier(value, ArtifactId, "artifact identifier")


def stored_optional_artifact_id(value: object) -> ArtifactId | None:
    return None if value is None else stored_artifact_id(value)


def stored_partition_key(value: object) -> PartitionKey:
    if type(value) is not str:
        raise ConsistencyCorruptionError("partition key is corrupt")
    try:
        return PartitionKey(value)
    except (TypeError, ValueError) as error:
        raise ConsistencyCorruptionError("partition key is corrupt") from error


def _stored_identifier[T](value: object, expected: type[T], subject: str) -> T:
    if type(value) is not str:
        raise ConsistencyCorruptionError(f"{subject} is corrupt")
    try:
        return expected(value)  # type: ignore[call-arg]
    except (TypeError, ValueError) as error:
        raise ConsistencyCorruptionError(f"{subject} is corrupt") from error

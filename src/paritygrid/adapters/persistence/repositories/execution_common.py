"""Validation and error translation shared by execution repositories."""

import unicodedata
from collections.abc import Callable, Sequence
from functools import wraps
from typing import cast

from sqlalchemy.exc import InterfaceError, OperationalError, SQLAlchemyError

from paritygrid.adapters.persistence.repositories.common import MAX_CANONICAL_DOCUMENT_BYTES
from paritygrid.adapters.persistence.values import CanonicalStorageJson, StoragePrimitive
from paritygrid.adapters.persistence.writer.contention import is_sqlite_contention
from paritygrid.application.ports.configuration import ConfigurationDocument
from paritygrid.application.ports.execution import (
    ExecutionCorruptionError,
    ExecutionInvalidRequestError,
    ExecutionStateConflictError,
    ExecutionStorageError,
    ExecutionStorageUnavailableError,
)
from paritygrid.application.ports.writer import PersistenceContentionError
from paritygrid.domain.models import (
    AttemptNumber,
    NodeId,
    PipelineId,
    PipelineVersion,
    RunId,
    StateFingerprint,
    UtcTimestamp,
    WorkItemId,
)
from paritygrid.domain.pipeline import PartitionKey

MAX_PERSISTED_INTEGER = 2_147_483_647
MAX_SQLITE_INTEGER = 9_223_372_036_854_775_807
MIN_SQLITE_INTEGER = -9_223_372_036_854_775_808


def translate_execution_storage_errors[**P, R](
    operation: Callable[P, R],
) -> Callable[P, R]:
    """Replace database implementation failures with a redacted public error."""

    @wraps(operation)
    def translated(*args: P.args, **kwargs: P.kwargs) -> R:
        contention = False
        unavailable = False
        try:
            return operation(*args, **kwargs)
        except OperationalError as error:
            contention = is_sqlite_contention(error)
            unavailable = not contention
        except InterfaceError:
            unavailable = True
        except SQLAlchemyError:
            pass
        if contention:
            raise PersistenceContentionError("Persistence is temporarily contended.") from None
        if unavailable:
            raise ExecutionStorageUnavailableError("Execution storage is unavailable.") from None
        raise ExecutionStorageError("Execution storage operation failed.") from None

    return translated


def require_exact[T](value: object, expected_type: type[T], subject: str) -> T:
    if type(value) is not expected_type:
        raise ExecutionInvalidRequestError(f"{subject} must use {expected_type.__name__}")
    return cast(T, value)


def require_run_id(value: object) -> RunId:
    return require_exact(value, RunId, "run identifier")


def require_work_item_id(value: object) -> WorkItemId:
    return require_exact(value, WorkItemId, "work-item identifier")


def require_node_id(value: object) -> NodeId:
    return require_exact(value, NodeId, "node identifier")


def require_pipeline_id(value: object) -> PipelineId:
    return require_exact(value, PipelineId, "pipeline identifier")


def require_pipeline_version(value: object) -> PipelineVersion:
    return require_exact(value, PipelineVersion, "pipeline version")


def require_attempt_number(value: object) -> AttemptNumber:
    return require_exact(value, AttemptNumber, "attempt number")


def require_partition_key(value: object) -> PartitionKey:
    return require_exact(value, PartitionKey, "partition key")


def require_timestamp(value: object, subject: str) -> UtcTimestamp:
    return require_exact(value, UtcTimestamp, subject)


def require_fingerprint(value: object) -> StateFingerprint:
    return require_exact(value, StateFingerprint, "final reconciliation fingerprint")


def require_document(value: object, subject: str) -> ConfigurationDocument:
    return require_exact(value, ConfigurationDocument, subject)


def bounded_text(value: object, subject: str, maximum: int) -> str:
    if type(value) is not str:
        raise ExecutionInvalidRequestError(f"{subject} must be text")
    if not 1 <= len(value) <= maximum:
        raise ExecutionInvalidRequestError(f"{subject} has an invalid length")
    if unicodedata.normalize("NFC", value) != value:
        raise ExecutionInvalidRequestError(f"{subject} must use normalized Unicode")
    return value


def optional_text(value: object, subject: str, maximum: int) -> str | None:
    if value is None:
        return None
    return bounded_text(value, subject, maximum)


def positive_int(value: object, subject: str) -> int:
    if type(value) is not int or not 1 <= value <= MAX_PERSISTED_INTEGER:
        raise ExecutionInvalidRequestError(
            f"{subject} must be an integer within the supported range"
        )
    return value


def nonnegative_int(value: object, subject: str, maximum: int = MAX_SQLITE_INTEGER) -> int:
    if type(value) is not int or not 0 <= value <= maximum:
        raise ExecutionInvalidRequestError(
            f"{subject} must be a nonnegative integer within the supported range"
        )
    return value


def optional_sqlite_int(value: object, subject: str) -> int | None:
    if value is None:
        return None
    if type(value) is not int or not MIN_SQLITE_INTEGER <= value <= MAX_SQLITE_INTEGER:
        raise ExecutionInvalidRequestError(
            f"{subject} must be a signed integer within the SQLite range or null"
        )
    return value


def require_incrementable(value: int, subject: str) -> None:
    if value >= MAX_PERSISTED_INTEGER:
        raise ExecutionStateConflictError(f"{subject} cannot advance beyond the supported maximum")


def encode_execution_document(
    document: ConfigurationDocument, subject: str
) -> CanonicalStorageJson:
    exact = require_document(document, subject)
    try:
        encoded = CanonicalStorageJson.encode(cast(StoragePrimitive, exact.to_mapping()))
    except (TypeError, ValueError) as error:
        raise ExecutionInvalidRequestError(f"{subject} is invalid") from error
    if len(encoded.text.encode("utf-8")) > MAX_CANONICAL_DOCUMENT_BYTES:
        raise ExecutionInvalidRequestError(f"{subject} exceeds the supported encoded size")
    return encoded


def decode_execution_document(value: object, subject: str) -> ConfigurationDocument:
    if not isinstance(value, str):
        raise ExecutionCorruptionError(f"{subject} is corrupt")
    try:
        decoded = CanonicalStorageJson(value).decode()
        if not isinstance(decoded, dict):
            raise ValueError
        return ConfigurationDocument.from_mapping(cast(dict[str, object], decoded))
    except (TypeError, ValueError) as error:
        raise ExecutionCorruptionError(f"{subject} is corrupt") from error


def decode_optional_execution_document(value: object, subject: str) -> ConfigurationDocument | None:
    return None if value is None else decode_execution_document(value, subject)


def encode_primitive_document(value: ConfigurationDocument) -> StoragePrimitive:
    return cast(StoragePrimitive, value.to_mapping())


def validate_node_ids(node_ids: Sequence[NodeId]) -> tuple[NodeId, ...]:
    if isinstance(node_ids, (str, bytes, bytearray)) or not node_ids:
        raise ExecutionInvalidRequestError("run nodes must be a nonempty sequence")
    canonical = tuple(require_node_id(node_id) for node_id in node_ids)
    if len(set(canonical)) != len(canonical):
        raise ExecutionInvalidRequestError("run node identifiers must be unique")
    return tuple(sorted(canonical, key=str))

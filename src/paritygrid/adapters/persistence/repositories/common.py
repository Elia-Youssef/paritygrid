"""Shared validation and error translation for configuration repositories."""

from __future__ import annotations

import unicodedata
from collections.abc import Callable
from functools import wraps
from typing import cast

from sqlalchemy.exc import InterfaceError, OperationalError, SQLAlchemyError

from paritygrid.adapters.persistence.values import CanonicalStorageJson, StoragePrimitive
from paritygrid.application.ports import (
    MAX_PIPELINE_DESCRIPTION_LENGTH,
    ConfigurationDocument,
    ConfigurationStorageError,
    ConfigurationStorageUnavailableError,
    ConnectorSecretReference,
    CorruptRepositoryRecordError,
    InvalidRepositoryRequestError,
    RecordStateConflictError,
    UnsafeConnectorConfigurationError,
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


def translate_storage_errors[**P, R](operation: Callable[P, R]) -> Callable[P, R]:
    """Replace database implementation errors with one redacted public failure."""

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


def encode_document(document: ConfigurationDocument) -> CanonicalStorageJson:
    """Encode one exact public document through the deterministic storage codec."""
    if type(document) is not ConfigurationDocument:
        raise InvalidRepositoryRequestError("configuration must use the public document type")
    primitive = cast(StoragePrimitive, document.to_mapping())
    encoded = CanonicalStorageJson.encode(primitive)
    if len(encoded.text.encode("utf-8")) > MAX_CANONICAL_DOCUMENT_BYTES:
        raise InvalidRepositoryRequestError(
            "configuration document exceeds the supported encoded size"
        )
    return encoded


def decode_document(value: object, subject: str) -> ConfigurationDocument:
    """Decode strict canonical object JSON from an untrusted stored row."""
    if not isinstance(value, str):
        raise CorruptRepositoryRecordError(f"{subject} is corrupt")
    try:
        decoded = CanonicalStorageJson(value).decode()
        if not isinstance(decoded, dict):
            raise ValueError("object required")
        return ConfigurationDocument.from_mapping(cast(dict[str, object], decoded))
    except (TypeError, ValueError) as error:
        raise CorruptRepositoryRecordError(f"{subject} is corrupt") from error


def decode_optional_document(value: object, subject: str) -> ConfigurationDocument | None:
    """Decode optional strict canonical object JSON."""
    return None if value is None else decode_document(value, subject)


def validate_secret_policy(
    document: ConfigurationDocument, reference_names: frozenset[str]
) -> None:
    """Require every declared secret reference to be used exactly as a reference."""
    dependencies: set[str] = set()

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
                    dependencies.add(child)
                visit(child)
        elif isinstance(value, list):
            for child in cast(list[object], value):
                visit(child)

    visit(document.to_mapping())
    if dependencies != set(reference_names):
        raise UnsafeConnectorConfigurationError(
            "connector secret references must match configuration dependencies"
        )


def validate_stored_secret_policy(
    document: ConfigurationDocument, references: tuple[ConnectorSecretReference, ...]
) -> None:
    """Translate an invalid persisted secret policy into stored-row corruption."""
    try:
        validate_secret_policy(
            document, frozenset(reference.reference_name for reference in references)
        )
    except UnsafeConnectorConfigurationError as error:
        raise CorruptRepositoryRecordError("connector secret policy is corrupt") from error


def bounded_text(value: object, subject: str, maximum: int) -> str:
    """Validate bounded NFC text supplied to a repository."""
    if not isinstance(value, str):
        raise InvalidRepositoryRequestError(f"{subject} must be text")
    if not 1 <= len(value) <= maximum:
        raise InvalidRepositoryRequestError(f"{subject} has an invalid length")
    if unicodedata.normalize("NFC", value) != value:
        raise InvalidRepositoryRequestError(f"{subject} must use normalized Unicode")
    return value


def optional_text(
    value: object,
    subject: str,
    maximum: int = MAX_PIPELINE_DESCRIPTION_LENGTH,
) -> str | None:
    """Validate optional bounded NFC text supplied to a repository."""
    if value is None:
        return None
    if not isinstance(value, str):
        raise InvalidRepositoryRequestError(f"{subject} must be text or null")
    if len(value) > maximum:
        raise InvalidRepositoryRequestError(f"{subject} has an invalid length")
    if unicodedata.normalize("NFC", value) != value:
        raise InvalidRepositoryRequestError(f"{subject} must use normalized Unicode")
    return value


def positive_int(value: object, subject: str) -> int:
    """Validate one exact persisted positive integer."""
    if type(value) is not int or not 1 <= value <= MAX_PERSISTED_INTEGER:
        raise InvalidRepositoryRequestError(
            f"{subject} must be an integer within the supported range"
        )
    return value


def stored_nonnegative_int(value: object, subject: str) -> int:
    """Parse an untrusted stored nonnegative aggregate integer."""
    if type(value) is not int or not 0 <= value <= MAX_PERSISTED_INTEGER:
        raise CorruptRepositoryRecordError(f"{subject} is corrupt")
    return value


def require_incrementable(value: int, subject: str) -> None:
    """Reject a version counter that cannot be advanced safely."""
    if value >= MAX_PERSISTED_INTEGER:
        raise RecordStateConflictError(f"{subject} cannot advance beyond the supported maximum")


def require_exact[T](value: object, expected_type: type[T], subject: str) -> T:
    """Require the exact public value type at a repository boundary."""
    if type(value) is not expected_type:
        raise InvalidRepositoryRequestError(f"{subject} must use {expected_type.__name__}")
    return cast(T, value)


def require_pipeline_id(value: object) -> PipelineId:
    return require_exact(value, PipelineId, "pipeline identifier")


def require_connector_id(value: object) -> ConnectorId:
    return require_exact(value, ConnectorId, "connector identifier")


def require_pipeline_version(value: object, subject: str) -> PipelineVersion:
    return require_exact(value, PipelineVersion, subject)


def require_timestamp(value: object, subject: str) -> UtcTimestamp:
    return require_exact(value, UtcTimestamp, subject)


def require_document(value: object, subject: str) -> ConfigurationDocument:
    return require_exact(value, ConfigurationDocument, subject)

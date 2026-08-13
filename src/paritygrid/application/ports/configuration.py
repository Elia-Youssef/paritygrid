"""Dependency-neutral contracts for durable pipeline and connector configuration."""

from __future__ import annotations

import unicodedata
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Protocol, Self, cast

from paritygrid.domain.models import ConnectorId, PipelineId, PipelineVersion, UtcTimestamp


@dataclass(frozen=True, slots=True)
class DocumentArray:
    """An immutable JSON array whose shape cannot be confused with an object."""

    values: tuple[DocumentValue, ...]


@dataclass(frozen=True, slots=True)
class NestedDocumentObject:
    """An immutable nested JSON object with an explicit structural tag."""

    items: DocumentObject


type DocumentValue = bool | int | str | DocumentArray | NestedDocumentObject | None
type DocumentObject = tuple[tuple[str, DocumentValue], ...]

MAX_PAGE_SIZE = 100
MAX_DOCUMENT_DEPTH = 32
MAX_DOCUMENT_ENTRIES = 10_000
MAX_DOCUMENT_KEY_LENGTH = 1_024
MAX_DOCUMENT_STRING_LENGTH = 65_536
MAX_PIPELINE_DESCRIPTION_LENGTH = 4_096


class ConfigurationRepositoryError(Exception):
    """Base class for stable configuration repository failures."""


class InvalidRepositoryRequestError(ConfigurationRepositoryError):
    """The requested repository operation violates a public contract."""


class DuplicateRecordError(ConfigurationRepositoryError):
    """A record already exists for the supplied stable identifier."""


class RecordNotFoundError(ConfigurationRepositoryError):
    """No record exists for the supplied stable identifier."""


class StaleRowVersionError(ConfigurationRepositoryError):
    """The supplied optimistic row version no longer matches storage."""


class StaleConnectorRevisionError(ConfigurationRepositoryError):
    """The supplied connector content revision no longer matches storage."""


class RecordStateConflictError(ConfigurationRepositoryError):
    """The current lifecycle state does not permit the requested operation."""


class PipelineVersionConflictError(ConfigurationRepositoryError):
    """A pipeline publication is not the next version or an exact replay."""


class UnsafeConnectorConfigurationError(ConfigurationRepositoryError):
    """Connector configuration does not follow the secret-reference policy."""


class CorruptRepositoryRecordError(ConfigurationRepositoryError):
    """Persisted data failed strict validation at the repository boundary."""


class ConfigurationStorageError(ConfigurationRepositoryError):
    """An unexpected persistence implementation failure prevented the operation."""


class ConfigurationStorageUnavailableError(ConfigurationStorageError):
    """The configuration database was unavailable for the requested operation."""


@dataclass(frozen=True, slots=True, repr=False)
class ConfigurationDocument:
    """An immutable, normalized JSON object independent of any storage codec."""

    items: DocumentObject

    def __post_init__(self) -> None:
        _validate_document_object(cast(object, self.items), depth=0, budget=[0])

    @classmethod
    def from_mapping(cls, value: Mapping[str, object]) -> Self:
        """Copy and normalize a mapping into an immutable document."""
        return cls(items=_freeze_object(cast(Mapping[object, object], value), depth=0, budget=[0]))

    def to_mapping(self) -> dict[str, object]:
        """Return a detached JSON-compatible mapping."""
        return {key: _thaw_value(value) for key, value in self.items}

    def __repr__(self) -> str:
        return f"ConfigurationDocument(fields={len(self.items)})"


@dataclass(frozen=True, slots=True)
class PipelineRecord:
    pipeline_id: PipelineId
    display_name: str
    description: str | None
    created_at: UtcTimestamp
    archived_at: UtcTimestamp | None
    row_version: int


@dataclass(frozen=True, slots=True, repr=False)
class PipelineVersionRecord:
    pipeline_id: PipelineId
    version: PipelineVersion
    specification: ConfigurationDocument
    specification_sha256: str
    planner_format_version: int
    published_at: UtcTimestamp

    def __repr__(self) -> str:
        return (
            "PipelineVersionRecord("
            f"pipeline_id={self.pipeline_id!r}, version={self.version!r}, "
            f"specification_sha256={self.specification_sha256!r}, "
            f"planner_format_version={self.planner_format_version!r}, "
            f"published_at={self.published_at!r})"
        )


@dataclass(frozen=True, slots=True, repr=False)
class ConnectorSecretReference:
    reference_name: str
    environment_variable_name: str

    def __repr__(self) -> str:
        return "ConnectorSecretReference(redacted=True)"


@dataclass(frozen=True, slots=True, repr=False)
class ConnectorRecord:
    connector_id: ConnectorId
    kind: str
    display_name: str
    configuration: ConfigurationDocument
    capabilities: ConfigurationDocument
    schema_discovery: ConfigurationDocument | None
    secret_references: tuple[ConnectorSecretReference, ...]
    revision: int
    created_at: UtcTimestamp
    updated_at: UtcTimestamp
    archived_at: UtcTimestamp | None
    row_version: int

    def __repr__(self) -> str:
        return (
            "ConnectorRecord("
            f"connector_id={self.connector_id!r}, kind={self.kind!r}, "
            f"display_name={self.display_name!r}, revision={self.revision!r}, "
            f"created_at={self.created_at!r}, updated_at={self.updated_at!r}, "
            f"archived_at={self.archived_at!r}, row_version={self.row_version!r}, "
            "configuration=<redacted>, capabilities=<redacted>, "
            "schema_discovery=<redacted>, secret_references=<redacted>)"
        )


@dataclass(frozen=True, slots=True)
class PipelinePage:
    items: tuple[PipelineRecord, ...]
    next_cursor: PipelineId | None


@dataclass(frozen=True, slots=True, repr=False)
class PipelineVersionPage:
    items: tuple[PipelineVersionRecord, ...]
    next_cursor: PipelineVersion | None


@dataclass(frozen=True, slots=True, repr=False)
class ConnectorPage:
    items: tuple[ConnectorRecord, ...]
    next_cursor: ConnectorId | None


class PipelineRepository(Protocol):
    """Persistence operations for pipeline identities and immutable versions."""

    def create(
        self,
        *,
        pipeline_id: PipelineId,
        display_name: str,
        description: str | None,
        created_at: UtcTimestamp,
    ) -> PipelineRecord: ...

    def get(self, pipeline_id: PipelineId) -> PipelineRecord | None: ...

    def list(
        self,
        *,
        limit: int,
        after: PipelineId | None = None,
        include_archived: bool = False,
    ) -> PipelinePage: ...

    def update_metadata(
        self,
        pipeline_id: PipelineId,
        *,
        expected_row_version: int,
        display_name: str,
        description: str | None,
    ) -> PipelineRecord: ...

    def archive(
        self,
        pipeline_id: PipelineId,
        *,
        expected_row_version: int,
        archived_at: UtcTimestamp,
    ) -> PipelineRecord: ...

    def publish_version(
        self,
        *,
        pipeline_id: PipelineId,
        expected_latest_version: PipelineVersion | None,
        specification: ConfigurationDocument,
        planner_format_version: int,
        published_at: UtcTimestamp,
    ) -> PipelineVersionRecord: ...

    def get_version(
        self, pipeline_id: PipelineId, version: PipelineVersion
    ) -> PipelineVersionRecord | None: ...

    def list_versions(
        self,
        pipeline_id: PipelineId,
        *,
        limit: int,
        after: PipelineVersion | None = None,
    ) -> PipelineVersionPage: ...


class ConnectorRepository(Protocol):
    """Persistence operations for connector configuration and secret references."""

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
    ) -> ConnectorRecord: ...

    def get(self, connector_id: ConnectorId) -> ConnectorRecord | None: ...

    def list(
        self,
        *,
        limit: int,
        after: ConnectorId | None = None,
        include_archived: bool = False,
    ) -> ConnectorPage: ...

    def update_metadata(
        self,
        connector_id: ConnectorId,
        *,
        expected_row_version: int,
        display_name: str,
        updated_at: UtcTimestamp,
    ) -> ConnectorRecord: ...

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
    ) -> ConnectorRecord: ...

    def archive(
        self,
        connector_id: ConnectorId,
        *,
        expected_row_version: int,
        archived_at: UtcTimestamp,
    ) -> ConnectorRecord: ...


def validate_page_limit(limit: object) -> int:
    """Validate a bounded page size without silently changing it."""
    if isinstance(limit, bool) or not isinstance(limit, int):
        raise InvalidRepositoryRequestError("page limit must be an integer")
    if not 1 <= limit <= MAX_PAGE_SIZE:
        raise InvalidRepositoryRequestError(f"page limit must be between 1 and {MAX_PAGE_SIZE}")
    return limit


def _freeze_object(
    value: Mapping[object, object], *, depth: int, budget: list[int]
) -> DocumentObject:
    _check_depth(depth)
    frozen: list[tuple[str, DocumentValue]] = []
    for raw_key, raw_value in value.items():
        if not isinstance(raw_key, str):
            raise TypeError("configuration document keys must be text")
        _check_normalized_text(raw_key, key=True)
        _consume_entry(budget)
        frozen.append((raw_key, _freeze_value(raw_value, depth=depth + 1, budget=budget)))
    frozen.sort(key=lambda item: item[0])
    return tuple(frozen)


def _freeze_value(value: object, *, depth: int, budget: list[int]) -> DocumentValue:
    _check_depth(depth)
    if value is None:
        return None
    if type(value) is bool:
        return value
    if type(value) is int:
        return value
    if isinstance(value, str):
        _check_normalized_text(value, key=False)
        return value
    if isinstance(value, Mapping):
        return NestedDocumentObject(
            _freeze_object(cast(Mapping[object, object], value), depth=depth, budget=budget)
        )
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        sequence = cast(Sequence[object], value)
        result: list[DocumentValue] = []
        for item in sequence:
            _consume_entry(budget)
            result.append(_freeze_value(item, depth=depth + 1, budget=budget))
        return DocumentArray(tuple(result))
    raise TypeError("configuration document values must be JSON-compatible without floats")


def _thaw_value(value: DocumentValue) -> object:
    if isinstance(value, NestedDocumentObject):
        return {key: _thaw_value(item) for key, item in value.items}
    if isinstance(value, DocumentArray):
        return [_thaw_value(item) for item in value.values]
    return value


def _validate_document_object(value: object, *, depth: int, budget: list[int]) -> DocumentObject:
    _check_depth(depth)
    if not isinstance(value, tuple):
        raise TypeError("configuration document items must be an immutable tuple")
    result: list[tuple[str, DocumentValue]] = []
    for raw_item in cast(tuple[object, ...], value):
        if not isinstance(raw_item, tuple):
            raise TypeError("configuration document entries must be key-value pairs")
        raw_pair = cast(tuple[object, ...], raw_item)
        if len(raw_pair) != 2:
            raise TypeError("configuration document entries must be key-value pairs")
        key_value = raw_pair
        if not isinstance(key_value[0], str):
            raise TypeError("configuration document keys must be text")
        _check_normalized_text(key_value[0], key=True)
        _consume_entry(budget)
        result.append(
            (
                key_value[0],
                _validate_document_value(key_value[1], depth=depth + 1, budget=budget),
            )
        )
    canonical = tuple(result)
    keys = tuple(key for key, _item in canonical)
    if keys != tuple(sorted(keys)) or len(set(keys)) != len(keys):
        raise ValueError(
            "configuration document items must be canonical and sorted with unique keys"
        )
    return canonical


def _validate_document_value(value: object, *, depth: int, budget: list[int]) -> DocumentValue:
    _check_depth(depth)
    if isinstance(value, str):
        _check_normalized_text(value, key=False)
        return value
    if value is None:
        return None
    if type(value) is bool:
        return value
    if type(value) is int:
        return value
    if isinstance(value, NestedDocumentObject):
        return NestedDocumentObject(
            _validate_document_object(value.items, depth=depth, budget=budget)
        )
    if isinstance(value, DocumentArray):
        result: list[DocumentValue] = []
        for item in value.values:
            _consume_entry(budget)
            result.append(_validate_document_value(item, depth=depth + 1, budget=budget))
        return DocumentArray(tuple(result))
    raise TypeError("configuration document values must be immutable structural values")


def _check_depth(depth: int) -> None:
    if depth > MAX_DOCUMENT_DEPTH:
        raise ValueError("configuration document exceeds the maximum nesting depth")


def _consume_entry(budget: list[int]) -> None:
    budget[0] += 1
    if budget[0] > MAX_DOCUMENT_ENTRIES:
        raise ValueError("configuration document contains too many entries")


def _check_normalized_text(value: str, *, key: bool) -> None:
    maximum = MAX_DOCUMENT_KEY_LENGTH if key else MAX_DOCUMENT_STRING_LENGTH
    if len(value) > maximum:
        raise ValueError("configuration document text exceeds the supported length")
    if unicodedata.normalize("NFC", value) != value:
        raise ValueError("configuration document text must use normalized Unicode")

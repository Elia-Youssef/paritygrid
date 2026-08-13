"""Dependency-neutral artifact storage contracts."""

import re
import unicodedata
from collections.abc import Iterable
from dataclasses import dataclass
from typing import Protocol, Self, cast

from paritygrid.domain.models import ArtifactId, NodeId, RunId, UtcTimestamp
from paritygrid.domain.pipeline import PartitionKey

MAX_ARTIFACT_RELATIVE_PATH_BYTES = 1_024
MAX_ARTIFACT_PATH_SEGMENT_BYTES = 255
MAX_ARTIFACT_PATH_SEGMENTS = 32
MAX_ARTIFACT_WRITE_BYTES = 1_099_511_627_776
MAX_ARTIFACT_CHUNK_BYTES = 8_388_608
MAX_ARTIFACT_PAGE_SIZE = 100
MAX_ARTIFACT_SCHEMA_VERSION = 2_147_483_647
MAX_ARTIFACT_ROW_COUNT = 9_223_372_036_854_775_807

_UNSAFE_PORTABLE_CHARACTERS = frozenset('<>:"\\|?*')
_WINDOWS_DEVICE_NAME = re.compile(
    r"(?:(?:con|prn|aux|nul|com[1-9¹²³]|lpt[1-9¹²³])(?:\..*)?|con(?:in|out)\$)",
    flags=re.IGNORECASE,
)


class ArtifactPathError(ValueError):
    """Base failure for an unsafe or noncanonical artifact path."""


class ArtifactWriteError(RuntimeError):
    """Base failure while staging or publishing an artifact file."""


class ArtifactInvalidWriteError(ArtifactWriteError):
    """The requested write violates the bounded writer contract."""


class ArtifactSizeLimitError(ArtifactWriteError):
    """The streamed artifact exceeds its configured byte limit."""


class ArtifactAlreadyExistsError(ArtifactWriteError):
    """The immutable destination already exists."""


class ArtifactStorageError(ArtifactWriteError):
    """The filesystem rejected a write before publication."""


class ArtifactPublishOutcomeUnknownError(ArtifactWriteError):
    """Publication succeeded but its final durability step failed."""


class ArtifactManifestError(RuntimeError):
    """Base failure for durable artifact metadata."""


class ArtifactManifestInvalidError(ArtifactManifestError):
    """Manifest input violates the public contract."""


class ArtifactManifestConflictError(ArtifactManifestError):
    """An immutable manifest identity or path has divergent metadata."""


class ArtifactManifestCorruptionError(ArtifactManifestError):
    """Stored manifest metadata violates its durable contract."""


class ArtifactIntegrityError(ArtifactManifestError):
    """A manifest and its immutable file do not agree."""


class ArtifactManifestStorageError(ArtifactManifestError):
    """Manifest persistence failed without exposing storage details."""


class ArtifactManifestStorageUnavailableError(ArtifactManifestError):
    """Manifest persistence is temporarily unavailable."""


@dataclass(frozen=True, slots=True, order=True)
class ArtifactRelativePath:
    """Canonical portable path relative to the configured artifact root."""

    value: str

    def __post_init__(self) -> None:
        object.__setattr__(self, "value", _validate_relative_path(self.value))

    @classmethod
    def parse(cls, value: str) -> Self:
        """Parse canonical text without normalization or platform interpretation."""
        return cls(value=value)

    @classmethod
    def from_bytes(cls, value: bytes) -> Self:
        """Parse a canonical UTF-8 representation."""
        encoded = cast(object, value)
        if not isinstance(encoded, bytes):
            raise TypeError("artifact path encoding must be bytes")
        try:
            decoded = encoded.decode("utf-8")
        except UnicodeDecodeError as error:
            raise ArtifactPathError("artifact path encoding must be valid UTF-8") from error
        return cls.parse(decoded)

    @property
    def parts(self) -> tuple[str, ...]:
        """Return detached canonical path segments."""
        return tuple(self.value.split("/"))

    def to_bytes(self) -> bytes:
        """Return the stable UTF-8 representation used by manifests."""
        return self.value.encode("utf-8")

    def __bytes__(self) -> bytes:
        return self.to_bytes()

    def __str__(self) -> str:
        return self.value


@dataclass(frozen=True, slots=True)
class ArtifactWriteReceipt:
    """Content identity and size of one atomically published file."""

    relative_path: ArtifactRelativePath
    byte_size: int
    sha256: str

    def __post_init__(self) -> None:
        relative_path = cast(object, self.relative_path)
        byte_size = cast(object, self.byte_size)
        sha256 = cast(object, self.sha256)
        if not isinstance(relative_path, ArtifactRelativePath):
            raise TypeError("artifact receipt path must be validated")
        if isinstance(byte_size, bool) or not isinstance(byte_size, int):
            raise TypeError("artifact receipt byte size must be an integer")
        if not 0 <= byte_size <= MAX_ARTIFACT_WRITE_BYTES:
            raise ValueError("artifact receipt byte size is outside the supported range")
        if not isinstance(sha256, str):
            raise TypeError("artifact receipt SHA-256 must be text")
        if re.fullmatch(r"[0-9a-f]{64}", sha256, flags=re.ASCII) is None:
            raise ValueError("artifact receipt SHA-256 must be canonical lowercase hexadecimal")


@dataclass(frozen=True, slots=True)
class ArtifactManifestRecord:
    """Immutable durable metadata for one verified artifact file."""

    artifact_id: ArtifactId
    run_id: RunId
    node_id: NodeId
    partition_key: PartitionKey
    relative_path: ArtifactRelativePath
    media_type: str
    schema_version: int
    byte_size: int
    row_count: int
    sha256: str
    created_at: UtcTimestamp

    def __post_init__(self) -> None:
        _validate_manifest_record(self)


@dataclass(frozen=True, slots=True)
class ArtifactManifestPage:
    """Bounded artifact manifests ordered by canonical artifact identity."""

    items: tuple[ArtifactManifestRecord, ...]
    next_cursor: ArtifactId | None

    def __post_init__(self) -> None:
        items = cast(object, self.items)
        cursor = cast(object, self.next_cursor)
        if not isinstance(items, tuple):
            raise TypeError("artifact manifest page items must be an immutable record tuple")
        records = cast(tuple[object, ...], items)
        if any(type(item) is not ArtifactManifestRecord for item in records):
            raise TypeError("artifact manifest page items must be an immutable record tuple")
        if cursor is not None and type(cursor) is not ArtifactId:
            raise TypeError("artifact manifest page cursor must be an artifact identifier")


class ArtifactWriter(Protocol):
    """Port for bounded immutable artifact publication."""

    def write(
        self,
        relative_path: ArtifactRelativePath,
        chunks: Iterable[bytes],
    ) -> ArtifactWriteReceipt:
        """Publish one new artifact without replacing an existing destination."""
        ...


class ArtifactManifestRepository(Protocol):
    """Port for immutable verified artifact metadata."""

    def register(
        self,
        *,
        artifact_id: ArtifactId,
        run_id: RunId,
        node_id: NodeId,
        partition_key: PartitionKey,
        write_receipt: ArtifactWriteReceipt,
        media_type: str,
        schema_version: int,
        row_count: int,
        created_at: UtcTimestamp,
    ) -> ArtifactManifestRecord:
        """Verify and durably register one published file."""
        ...

    def get(self, artifact_id: ArtifactId) -> ArtifactManifestRecord | None:
        """Return one verified manifest when present."""
        ...

    def list_for_run(
        self,
        run_id: RunId,
        *,
        limit: int,
        after: ArtifactId | None = None,
    ) -> ArtifactManifestPage:
        """Return one stable page of verified manifests for a run."""
        ...


def validate_artifact_page_limit(value: object) -> int:
    """Validate one exact bounded artifact page size."""
    if type(value) is not int or not 1 <= value <= MAX_ARTIFACT_PAGE_SIZE:
        raise ArtifactManifestInvalidError("artifact page limit is outside the supported range")
    return value


def validate_artifact_media_type(value: object) -> str:
    """Validate one canonical lowercase ASCII media type without parameters."""
    if not isinstance(value, str):
        raise TypeError("artifact media type must be text")
    pattern = r"[a-z0-9][a-z0-9!#$&^_.+-]*/[a-z0-9][a-z0-9!#$&^_.+-]*"
    if len(value) > 127 or re.fullmatch(pattern, value, flags=re.ASCII) is None:
        raise ValueError("artifact media type must be canonical lowercase ASCII")
    return value


def _validate_relative_path(value: object) -> str:
    if not isinstance(value, str):
        raise TypeError("artifact relative path must be text")
    if not value:
        raise ArtifactPathError("artifact relative path must not be blank")
    try:
        encoded_length = len(value.encode("utf-8"))
    except UnicodeEncodeError as error:
        raise ArtifactPathError("artifact relative path must be valid Unicode") from error
    if unicodedata.normalize("NFC", value) != value:
        raise ArtifactPathError("artifact relative path must use NFC Unicode")
    if any(unicodedata.category(character).startswith("C") for character in value):
        raise ArtifactPathError("artifact relative path contains a control character")
    if value.startswith("/") or value.endswith("/"):
        raise ArtifactPathError("artifact path must be relative and have no empty edge segment")
    if "\\" in value:
        raise ArtifactPathError("artifact relative path must use forward-slash separators")

    if encoded_length > MAX_ARTIFACT_RELATIVE_PATH_BYTES:
        raise ArtifactPathError("artifact relative path exceeds the byte limit")

    segments = value.split("/")
    if len(segments) > MAX_ARTIFACT_PATH_SEGMENTS:
        raise ArtifactPathError("artifact relative path has too many segments")
    for segment in segments:
        _validate_segment(segment)
    return value


def _validate_segment(segment: str) -> None:
    if not segment or segment in {".", ".."}:
        raise ArtifactPathError("artifact relative path contains an unsafe segment")
    if segment[0].isspace() or segment[-1].isspace() or segment.endswith("."):
        raise ArtifactPathError("artifact relative path contains an ambiguous segment")
    if segment.startswith("."):
        raise ArtifactPathError("artifact relative path cannot contain hidden segments")
    if any(character in _UNSAFE_PORTABLE_CHARACTERS for character in segment):
        raise ArtifactPathError("artifact relative path contains a nonportable character")
    if _WINDOWS_DEVICE_NAME.fullmatch(segment) is not None:
        raise ArtifactPathError("artifact relative path contains a reserved device name")
    if len(segment.encode("utf-8")) > MAX_ARTIFACT_PATH_SEGMENT_BYTES:
        raise ArtifactPathError("artifact path segment exceeds the byte limit")


def _validate_manifest_record(record: ArtifactManifestRecord) -> None:
    exact_types: tuple[tuple[object, type[object], str], ...] = (
        (record.artifact_id, ArtifactId, "artifact identifier"),
        (record.run_id, RunId, "run identifier"),
        (record.node_id, NodeId, "node identifier"),
        (record.partition_key, PartitionKey, "partition key"),
        (record.relative_path, ArtifactRelativePath, "artifact relative path"),
        (record.created_at, UtcTimestamp, "artifact creation time"),
    )
    for value, expected, subject in exact_types:
        if type(value) is not expected:
            raise TypeError(f"{subject} must use {expected.__name__}")
    validate_artifact_media_type(record.media_type)
    _validate_manifest_integer(
        record.schema_version,
        "artifact schema version",
        minimum=1,
        maximum=MAX_ARTIFACT_SCHEMA_VERSION,
    )
    _validate_manifest_integer(
        record.byte_size,
        "artifact byte size",
        minimum=0,
        maximum=MAX_ARTIFACT_WRITE_BYTES,
    )
    _validate_manifest_integer(
        record.row_count,
        "artifact row count",
        minimum=0,
        maximum=MAX_ARTIFACT_ROW_COUNT,
    )
    digest = cast(object, record.sha256)
    if not isinstance(digest, str):
        raise TypeError("artifact SHA-256 must be text")
    if re.fullmatch(r"[0-9a-f]{64}", digest, flags=re.ASCII) is None:
        raise ValueError("artifact SHA-256 must be canonical lowercase hexadecimal")


def _validate_manifest_integer(
    value: object,
    subject: str,
    *,
    minimum: int,
    maximum: int,
) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError(f"{subject} must be an integer")
    if not minimum <= value <= maximum:
        raise ValueError(f"{subject} is outside the supported range")
    return value

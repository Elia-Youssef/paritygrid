"""Dependency-neutral contracts for confined artifact byte streaming."""

import re
from collections.abc import Iterator
from dataclasses import dataclass
from typing import Protocol, Self

from paritygrid.application.ports.artifacts import (
    MAX_ARTIFACT_WRITE_BYTES,
    ArtifactRelativePath,
    validate_artifact_media_type,
)
from paritygrid.domain.models import ArtifactId

_SHA256 = re.compile(r"[0-9a-f]{64}\Z", flags=re.ASCII)


class ArtifactStreamError(RuntimeError):
    """Base failure for artifact stream preparation or consumption."""


class ArtifactStreamInvalidError(ArtifactStreamError):
    """A stream request violates the bounded public contract."""


class ArtifactStreamNotFoundError(ArtifactStreamError):
    """The requested committed artifact does not exist."""


class ArtifactStreamRangeError(ArtifactStreamError):
    """A requested byte range is not satisfiable for the artifact."""


class ArtifactStreamIntegrityError(ArtifactStreamError):
    """The immutable file or durable manifest relationship is invalid."""


class ArtifactStreamStorageError(ArtifactStreamError):
    """A stream operation failed without exposing storage details."""


class ArtifactStreamStorageUnavailableError(ArtifactStreamError):
    """Manifest persistence is temporarily unavailable."""


@dataclass(frozen=True, slots=True)
class ArtifactByteRange:
    """One nonempty half-open artifact byte interval."""

    start: int
    end_exclusive: int

    def __post_init__(self) -> None:
        if type(self.start) is not int or type(self.end_exclusive) is not int:
            raise TypeError("artifact byte range bounds must be integers")
        if (
            not 0 <= self.start < self.end_exclusive
            or self.end_exclusive > MAX_ARTIFACT_WRITE_BYTES
        ):
            raise ArtifactStreamInvalidError("artifact byte range is outside the bounds")

    @property
    def length(self) -> int:
        """Return the exact selected byte count."""
        return self.end_exclusive - self.start


@dataclass(frozen=True, slots=True)
class ArtifactStreamMetadata:
    """Safe response metadata for one verified full or partial stream."""

    artifact_id: ArtifactId
    relative_path: ArtifactRelativePath
    media_type: str
    schema_version: int
    total_byte_size: int
    range_start: int
    range_end_exclusive: int
    content_sha256: str

    def __post_init__(self) -> None:
        if type(self.artifact_id) is not ArtifactId:
            raise TypeError("artifact stream identity is invalid")
        if type(self.relative_path) is not ArtifactRelativePath:
            raise TypeError("artifact stream path is invalid")
        try:
            validate_artifact_media_type(self.media_type)
        except TypeError, ValueError:
            raise TypeError("artifact stream media type is invalid") from None
        if type(self.schema_version) is not int or not 1 <= self.schema_version <= 2_147_483_647:
            raise ValueError("artifact stream schema version is outside the range")
        for value in (
            self.total_byte_size,
            self.range_start,
            self.range_end_exclusive,
        ):
            if type(value) is not int:
                raise TypeError("artifact stream byte values must be integers")
        if not (
            0
            <= self.range_start
            <= self.range_end_exclusive
            <= self.total_byte_size
            <= MAX_ARTIFACT_WRITE_BYTES
        ):
            raise ArtifactStreamInvalidError("artifact stream byte values are inconsistent")
        if type(self.content_sha256) is not str or _SHA256.fullmatch(self.content_sha256) is None:
            raise TypeError("artifact stream digest is invalid")

    @property
    def content_length(self) -> int:
        """Return the selected response length."""
        return self.range_end_exclusive - self.range_start

    @property
    def is_partial(self) -> bool:
        """Return whether the response selects less than the whole artifact."""
        return self.range_start != 0 or self.range_end_exclusive != self.total_byte_size


class ArtifactByteStream(Protocol):
    """One explicitly closeable, single-pass byte iterator."""

    @property
    def metadata(self) -> ArtifactStreamMetadata:
        """Return immutable verified response metadata."""
        ...

    def __iter__(self) -> Iterator[bytes]: ...

    def __next__(self) -> bytes: ...

    def close(self) -> None:
        """Release the owned file descriptor and verify its identity."""
        ...

    def __enter__(self) -> Self: ...

    def __exit__(
        self,
        exception_type: type[BaseException] | None,
        exception: BaseException | None,
        traceback: object,
    ) -> None: ...


class ArtifactStreamReader(Protocol):
    """Prepare verified byte streams from immutable artifact manifests."""

    def open(
        self,
        artifact_id: ArtifactId,
        *,
        byte_range: ArtifactByteRange | None = None,
    ) -> ArtifactByteStream:
        """Open one full or partial stream inside a short caller transaction."""
        ...

"""Dependency-neutral artifact path contracts."""

import re
import unicodedata
from dataclasses import dataclass
from typing import Self, cast

MAX_ARTIFACT_RELATIVE_PATH_BYTES = 1_024
MAX_ARTIFACT_PATH_SEGMENT_BYTES = 255
MAX_ARTIFACT_PATH_SEGMENTS = 32

_UNSAFE_PORTABLE_CHARACTERS = frozenset('<>:"\\|?*')
_WINDOWS_DEVICE_NAME = re.compile(
    r"(?:(?:con|prn|aux|nul|com[1-9¹²³]|lpt[1-9¹²³])(?:\..*)?|con(?:in|out)\$)",
    flags=re.IGNORECASE,
)


class ArtifactPathError(ValueError):
    """Base failure for an unsafe or noncanonical artifact path."""


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

"""Canonical identifiers and monotonic sequence values."""

import re
from dataclasses import dataclass
from typing import ClassVar, Self

_MIN_PAYLOAD_LENGTH = 3
_MAX_PAYLOAD_LENGTH = 64
_MAX_SEQUENCE_NUMBER = 2_147_483_647
_PAYLOAD_PATTERN = re.compile(r"[a-z0-9]+(?:-[a-z0-9]+)*", flags=re.ASCII)
_SEQUENCE_PATTERN = re.compile(r"[1-9][0-9]*", flags=re.ASCII)


@dataclass(frozen=True, slots=True)
class _PrefixedIdentifier:
    """Base contract for immutable, type-specific identifiers."""

    value: str
    _prefix: ClassVar[str]

    def __post_init__(self) -> None:
        canonical = _validate_identifier(self.value, prefix=self._prefix)
        object.__setattr__(self, "value", canonical)

    @classmethod
    def parse(cls, value: str) -> Self:
        """Parse a canonical text representation without normalization."""
        return cls(value=value)

    @classmethod
    def from_bytes(cls, value: bytes) -> Self:
        """Parse the canonical ASCII representation."""
        return cls.parse(_decode_ascii(value, subject="identifier"))

    def to_bytes(self) -> bytes:
        """Return the stable ASCII representation."""
        return self.value.encode("ascii")

    def __bytes__(self) -> bytes:
        return self.to_bytes()

    def __str__(self) -> str:
        return self.value


@dataclass(frozen=True, slots=True)
class PipelineId(_PrefixedIdentifier):
    """Stable identity of a pipeline across its immutable versions."""

    _prefix: ClassVar[str] = "pip"


@dataclass(frozen=True, slots=True)
class NodeId(_PrefixedIdentifier):
    """Stable identity of a node within a pipeline definition."""

    _prefix: ClassVar[str] = "nod"


@dataclass(frozen=True, slots=True)
class ConnectorId(_PrefixedIdentifier):
    """Identity of a validated connector configuration."""

    _prefix: ClassVar[str] = "con"


@dataclass(frozen=True, slots=True)
class RunId(_PrefixedIdentifier):
    """Identity of one captured pipeline execution."""

    _prefix: ClassVar[str] = "run"


@dataclass(frozen=True, slots=True)
class WorkItemId(_PrefixedIdentifier):
    """Durable identity of one scheduled work item."""

    _prefix: ClassVar[str] = "wrk"


@dataclass(frozen=True, slots=True)
class ArtifactId(_PrefixedIdentifier):
    """Identity used to address a committed artifact manifest."""

    _prefix: ClassVar[str] = "art"


@dataclass(frozen=True, slots=True)
class ConflictId(_PrefixedIdentifier):
    """Identity of one materialized reconciliation conflict."""

    _prefix: ClassVar[str] = "cnf"


@dataclass(frozen=True, slots=True)
class RepairPlanId(_PrefixedIdentifier):
    """Identity of an immutable repair plan."""

    _prefix: ClassVar[str] = "rpl"


type EntityId = (
    PipelineId | NodeId | ConnectorId | RunId | WorkItemId | ArtifactId | ConflictId | RepairPlanId
)


@dataclass(frozen=True, slots=True, order=True)
class _PositiveSequence:
    """Base contract for bounded, one-based sequence values."""

    number: int

    def __post_init__(self) -> None:
        canonical = _validate_sequence_number(self.number)
        object.__setattr__(self, "number", canonical)

    @classmethod
    def parse(cls, value: str) -> Self:
        """Parse canonical, unsigned decimal text."""
        return cls(number=_parse_sequence_text(value))

    @classmethod
    def from_bytes(cls, value: bytes) -> Self:
        """Parse canonical ASCII decimal bytes."""
        return cls.parse(_decode_ascii(value, subject="sequence"))

    def to_bytes(self) -> bytes:
        """Return canonical ASCII decimal bytes."""
        return str(self).encode("ascii")

    def __bytes__(self) -> bytes:
        return self.to_bytes()

    def __int__(self) -> int:
        return self.number

    def __str__(self) -> str:
        return str(self.number)


@dataclass(frozen=True, slots=True, order=True)
class PipelineVersion(_PositiveSequence):
    """One-based version number within a pipeline identity."""


@dataclass(frozen=True, slots=True, order=True)
class AttemptNumber(_PositiveSequence):
    """One-based immutable attempt number within a work item."""


def _validate_identifier(value: object, *, prefix: str) -> str:
    if not isinstance(value, str):
        raise TypeError("identifier must be text")
    if not value:
        raise ValueError("identifier must not be blank")
    if not value.isascii():
        raise ValueError("identifier must contain only ASCII")

    marker = f"{prefix}_"
    if not value.startswith(marker):
        raise ValueError(f"identifier must use the {prefix}_ prefix")

    payload = value.removeprefix(marker)
    if not _MIN_PAYLOAD_LENGTH <= len(payload) <= _MAX_PAYLOAD_LENGTH:
        raise ValueError(
            f"identifier payload must be between {_MIN_PAYLOAD_LENGTH} "
            f"and {_MAX_PAYLOAD_LENGTH} characters"
        )
    if _PAYLOAD_PATTERN.fullmatch(payload) is None:
        raise ValueError("identifier payload must be canonical lowercase ASCII")
    return value


def _validate_sequence_number(value: object) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError("sequence number must be an integer")
    if not 1 <= value <= _MAX_SEQUENCE_NUMBER:
        raise ValueError(f"sequence number must be between 1 and {_MAX_SEQUENCE_NUMBER}")
    return value


def _parse_sequence_text(value: object) -> int:
    if not isinstance(value, str):
        raise TypeError("sequence representation must be text")
    if _SEQUENCE_PATTERN.fullmatch(value) is None:
        raise ValueError("sequence representation must be canonical positive decimal text")
    return int(value)


def _decode_ascii(value: object, *, subject: str) -> str:
    if not isinstance(value, bytes):
        raise TypeError(f"{subject} encoding must be bytes")
    try:
        return value.decode("ascii")
    except UnicodeDecodeError as error:
        raise ValueError(f"{subject} encoding must contain only ASCII") from error

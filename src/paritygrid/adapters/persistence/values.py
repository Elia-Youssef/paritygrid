"""Closed values used only by the SQLite persistence boundary."""

import json
import re
import unicodedata
from dataclasses import dataclass
from enum import StrEnum
from typing import Self, cast

type StoragePrimitive = (
    bool | int | str | list["StoragePrimitive"] | dict[str, "StoragePrimitive"] | None
)

_SHA256_PATTERN = re.compile(r"[0-9a-f]{64}", flags=re.ASCII)
_SECRET_REFERENCE_PATTERN = re.compile(r"[a-z][a-z0-9]*(?:[_.-][a-z0-9]+)*", flags=re.ASCII)
_ENVIRONMENT_VARIABLE_PATTERN = re.compile(r"[A-Z_][A-Z0-9_]*", flags=re.ASCII)


class RunNodeState(StrEnum):
    """Aggregate execution state persisted for one node in a run."""

    PENDING = "pending"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    PARTIALLY_SUCCEEDED = "partially_succeeded"
    FAILED = "failed"
    CANCELLED = "cancelled"


class WorkAttemptOutcome(StrEnum):
    """Durable result of a completed or recovered work attempt."""

    SUCCEEDED = "succeeded"
    RETRY_SCHEDULED = "retry_scheduled"
    QUARANTINED = "quarantined"
    FAILED = "failed"
    CANCELLED = "cancelled"
    LEASE_EXPIRED = "lease_expired"


class IdempotencyStatus(StrEnum):
    """Persistence state of one idempotent command."""

    IN_PROGRESS = "in_progress"
    COMPLETED = "completed"
    FAILED = "failed"


class RepairPlanStatus(StrEnum):
    """Stored lifecycle of an immutable repair plan."""

    PROPOSED = "proposed"
    APPROVED = "approved"
    APPLYING = "applying"
    APPLIED = "applied"
    REJECTED = "rejected"
    FAILED = "failed"


class RepairActionApplicationStatus(StrEnum):
    """Stored application result for one repair action."""

    PENDING = "pending"
    APPLIED = "applied"
    FAILED = "failed"


@dataclass(frozen=True, slots=True, order=True)
class Sha256Digest:
    """A lowercase SHA-256 value that is not necessarily a state fingerprint."""

    value: str

    def __post_init__(self) -> None:
        value = cast(object, self.value)
        if not isinstance(value, str):
            raise TypeError("SHA-256 digest must be text")
        if _SHA256_PATTERN.fullmatch(self.value) is None:
            raise ValueError("SHA-256 digest must be 64 lowercase hexadecimal characters")

    @classmethod
    def parse(cls, value: str) -> Self:
        """Parse one exact lowercase hexadecimal digest."""
        return cls(value)

    def __str__(self) -> str:
        return self.value


@dataclass(frozen=True, slots=True, order=True)
class SecretReferenceName:
    """A stable local name for a connector secret reference."""

    value: str

    def __post_init__(self) -> None:
        value = cast(object, self.value)
        if not isinstance(value, str):
            raise TypeError("secret reference name must be text")
        if not 1 <= len(self.value) <= 64:
            raise ValueError("secret reference name must contain between 1 and 64 characters")
        if _SECRET_REFERENCE_PATTERN.fullmatch(self.value) is None:
            raise ValueError("secret reference name must use canonical lowercase ASCII")


@dataclass(frozen=True, slots=True, order=True)
class EnvironmentVariableName:
    """A portable environment-variable name persisted instead of a secret value."""

    value: str

    def __post_init__(self) -> None:
        value = cast(object, self.value)
        if not isinstance(value, str):
            raise TypeError("environment-variable name must be text")
        if not 1 <= len(self.value) <= 128:
            raise ValueError("environment-variable name must contain between 1 and 128 characters")
        if _ENVIRONMENT_VARIABLE_PATTERN.fullmatch(self.value) is None:
            raise ValueError("environment-variable name must use portable uppercase ASCII")


@dataclass(frozen=True, slots=True)
class CanonicalStorageJson:
    """Deterministic JSON storage bytes independent of the fingerprint protocol."""

    text: str

    def __post_init__(self) -> None:
        text = cast(object, self.text)
        if not isinstance(text, str):
            raise TypeError("canonical storage JSON must be text")
        decoded = _decode_json(self.text)
        canonical = _encode_json(decoded)
        if canonical != self.text:
            raise ValueError("storage JSON must use its canonical representation")

    @classmethod
    def encode(cls, value: StoragePrimitive) -> Self:
        """Encode a closed JSON value using stable storage rules."""
        _validate_primitive(value)
        return cls(_encode_json(value))

    def decode(self) -> StoragePrimitive:
        """Return a detached primitive representation."""
        return _decode_json(self.text)

    def __str__(self) -> str:
        return self.text


def _reject_float(_value: str) -> float:
    raise ValueError("storage JSON does not support floating-point numbers")


def _reject_constant(_value: str) -> None:
    raise ValueError("storage JSON does not support non-finite numbers")


def _decode_json(value: str) -> StoragePrimitive:
    try:
        decoded = json.loads(
            value,
            parse_float=_reject_float,
            parse_constant=_reject_constant,
        )
    except (json.JSONDecodeError, TypeError) as error:
        raise ValueError("storage JSON is not valid") from error
    _validate_primitive(decoded)
    return cast(StoragePrimitive, decoded)


def _encode_json(value: StoragePrimitive) -> str:
    return json.dumps(
        value,
        ensure_ascii=True,
        allow_nan=False,
        separators=(",", ":"),
        sort_keys=True,
    )


def _validate_primitive(value: object) -> None:
    if value is None or type(value) in {bool, int}:
        return
    if type(value) is str:
        if unicodedata.normalize("NFC", value) != value:
            raise ValueError("storage JSON strings must use NFC normalization")
        return
    if type(value) is list:
        for item in cast(list[object], value):
            _validate_primitive(item)
        return
    if type(value) is dict:
        for key, item in cast(dict[object, object], value).items():
            if type(key) is not str:
                raise TypeError("storage JSON object keys must be text")
            _validate_primitive(key)
            _validate_primitive(item)
        return
    raise TypeError("storage JSON supports only closed JSON primitives without floats")

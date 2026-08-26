"""Ordered, deterministic, inspectable scripted failures for simulators.

A failure script maps 1-based request sequence numbers to transport or
response-shape faults. Simulators count requests to their data or mutating
endpoints in arrival order and apply the scripted failure exactly when the
counter reaches the scripted sequence, so the same script always reproduces
the same failure sequence.
"""

import json
from collections.abc import Iterable
from dataclasses import dataclass
from enum import StrEnum
from typing import cast

SCRIPTED_FAILURE_VERSION = 1
_FAILURE_FORMAT_NAME = "paritygrid-scripted-failure"

_MAX_SEQUENCE = 2_147_483_647
_MAX_RETRY_AFTER_SECONDS = 60
_MAX_DELAY_MICROSECONDS = 60_000_000
_MAX_SCRIPT_ENTRIES = 1_000


class FailureScriptError(ValueError):
    """Raised when a scripted failure or script is invalid."""


class ScriptedFailureKind(StrEnum):
    """Closed set of reproducible fault behaviors."""

    RATE_LIMIT = "rate_limit"
    TRANSIENT_ERROR = "transient_error"
    TIMEOUT = "timeout"
    MALFORMED_RESPONSE = "malformed_response"
    DUPLICATE_RECORDS = "duplicate_records"
    CONNECTION_LOSS = "connection_loss"
    HANG = "hang"


#: Kinds that only alter transport behavior and may therefore target the
#: warehouse as well as the sources. Response-shape kinds (malformed body,
#: duplicated records) are source-only because the target contract has no
#: page body to corrupt.
_TRANSPORT_KINDS = frozenset(
    {
        ScriptedFailureKind.RATE_LIMIT,
        ScriptedFailureKind.TRANSIENT_ERROR,
        ScriptedFailureKind.TIMEOUT,
        ScriptedFailureKind.CONNECTION_LOSS,
        ScriptedFailureKind.HANG,
    }
)


@dataclass(frozen=True, slots=True)
class ScriptedFailure:
    """One scripted fault bound to an exact request sequence number."""

    sequence: int
    kind: ScriptedFailureKind
    retry_after_seconds: int | None = None
    delay_microseconds: int | None = None
    partial_bytes: int | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "sequence", _validate_sequence(self.sequence))
        object.__setattr__(self, "kind", _validate_kind(self.kind))
        object.__setattr__(self, "retry_after_seconds", self._validated_retry_after())
        object.__setattr__(self, "delay_microseconds", self._validated_delay())
        object.__setattr__(self, "partial_bytes", self._validated_partial_bytes())

    def describe(self) -> dict[str, object]:
        """Return the inspectable closed description of this entry."""
        description: dict[str, object] = {"kind": self.kind.value, "sequence": self.sequence}
        if self.retry_after_seconds is not None:
            description["retry_after_seconds"] = self.retry_after_seconds
        if self.delay_microseconds is not None:
            description["delay_microseconds"] = self.delay_microseconds
        if self.partial_bytes is not None:
            description["partial_bytes"] = self.partial_bytes
        return description

    def _validated_retry_after(self) -> int | None:
        if self.kind is not ScriptedFailureKind.RATE_LIMIT:
            if self.retry_after_seconds is not None:
                raise FailureScriptError("retry_after_seconds only applies to rate_limit")
            return None
        return _validated_integer(
            self.retry_after_seconds,
            field="retry_after_seconds",
            minimum=1,
            maximum=_MAX_RETRY_AFTER_SECONDS,
        )

    def _validated_delay(self) -> int | None:
        applies = self.kind in (
            ScriptedFailureKind.TIMEOUT,
            ScriptedFailureKind.HANG,
        )
        if not applies:
            if self.delay_microseconds is not None:
                raise FailureScriptError("delay_microseconds only applies to timeout or hang")
            return None
        return _validated_integer(
            self.delay_microseconds,
            field="delay_microseconds",
            minimum=0,
            maximum=_MAX_DELAY_MICROSECONDS,
        )

    def _validated_partial_bytes(self) -> int | None:
        if self.kind is not ScriptedFailureKind.CONNECTION_LOSS:
            if self.partial_bytes is not None:
                raise FailureScriptError("partial_bytes only applies to connection_loss")
            return None
        return _validated_integer(
            self.partial_bytes,
            field="partial_bytes",
            minimum=1,
            maximum=65_536,
        )


@dataclass(frozen=True, slots=True)
class AppliedFailure:
    """One observed application of a scripted failure."""

    sequence: int
    kind: ScriptedFailureKind


@dataclass(frozen=True, slots=True)
class FailureScript:
    """An ordered, duplicate-free set of scripted failures."""

    failures: tuple[ScriptedFailure, ...] = ()

    def __post_init__(self) -> None:
        seen: set[int] = set()
        for failure in self.failures:
            if failure.sequence in seen:
                raise FailureScriptError(
                    f"failure sequence {failure.sequence} is scripted more than once"
                )
            seen.add(failure.sequence)
        if len(self.failures) > _MAX_SCRIPT_ENTRIES:
            raise FailureScriptError(f"scripts are limited to {_MAX_SCRIPT_ENTRIES} entries")
        object.__setattr__(
            self, "failures", tuple(sorted(self.failures, key=lambda failure: failure.sequence))
        )

    @classmethod
    def empty(cls) -> FailureScript:
        """Return the script that never fails."""
        return cls()

    @classmethod
    def from_entries(cls, entries: Iterable[ScriptedFailure]) -> FailureScript:
        """Build a script from any iterable of validated entries."""
        if isinstance(entries, (str, bytes)):
            raise FailureScriptError("entries must be an iterable of ScriptedFailure values")
        return cls(failures=tuple(entries))

    def failure_for(self, sequence: int) -> ScriptedFailure | None:
        """Return the scripted failure for one sequence, if any."""
        for failure in self.failures:
            if failure.sequence == sequence:
                return failure
            if failure.sequence > sequence:
                return None
        return None

    def entry_count(self) -> int:
        """Return the number of scripted entries."""
        return len(self.failures)

    def describe(self) -> tuple[dict[str, object], ...]:
        """Return the inspectable ordered description of every entry."""
        return tuple(failure.describe() for failure in self.failures)

    def to_canonical_bytes(self) -> bytes:
        """Return the deterministic script document."""
        return json.dumps(
            {
                "failures": list(self.describe()),
                "format": _FAILURE_FORMAT_NAME,
                "version": SCRIPTED_FAILURE_VERSION,
            },
            ensure_ascii=True,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("ascii")

    @classmethod
    def from_canonical_bytes(cls, payload: bytes) -> FailureScript:
        """Parse canonical script bytes with strict validation."""
        try:
            document_value = json.loads(payload.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as error:
            raise FailureScriptError("script payload is not valid JSON") from error
        if not isinstance(document_value, dict):
            raise FailureScriptError("script payload must be a JSON object")
        document = cast("dict[str, object]", document_value)
        if document.get("format") != _FAILURE_FORMAT_NAME:
            raise FailureScriptError("script payload carries an unknown format")
        if document.get("version") != SCRIPTED_FAILURE_VERSION:
            raise FailureScriptError("script payload carries an unsupported version")
        entries_value = document.get("failures")
        if not isinstance(entries_value, list):
            raise FailureScriptError("script payload must carry a failures list")
        entries = cast("list[object]", entries_value)
        failures: list[ScriptedFailure] = []
        for entry in entries:
            if not isinstance(entry, dict):
                raise FailureScriptError("each script entry must be a JSON object")
            entry_map = cast("dict[str, object]", entry)
            kind_value = entry_map.get("kind")
            if not isinstance(kind_value, str):
                raise FailureScriptError("script entry field kind must be text")
            try:
                kind = ScriptedFailureKind(kind_value)
            except ValueError as error:
                raise FailureScriptError(
                    f"unknown scripted failure kind: {kind_value!r}"
                ) from error
            failures.append(
                ScriptedFailure(
                    sequence=_entry_int(entry_map, "sequence"),
                    kind=kind,
                    retry_after_seconds=_optional_entry_int(entry_map, "retry_after_seconds"),
                    delay_microseconds=_optional_entry_int(entry_map, "delay_microseconds"),
                    partial_bytes=_optional_entry_int(entry_map, "partial_bytes"),
                )
            )
        return cls(failures=tuple(failures))


def require_transport_script(script: FailureScript, *, subject: str) -> FailureScript:
    """Reject response-shape failures for endpoints with no page body."""
    for failure in script.failures:
        if failure.kind not in _TRANSPORT_KINDS:
            raise FailureScriptError(f"{subject} scripts may not use {failure.kind.value} failures")
    return script


def _entry_int(entry: dict[str, object], key: str) -> int:
    value = entry.get(key)
    if isinstance(value, bool) or not isinstance(value, int):
        raise FailureScriptError(f"script entry field {key} must be an integer")
    return value


def _optional_entry_int(entry: dict[str, object], key: str) -> int | None:
    if key not in entry:
        return None
    return _entry_int(entry, key)


def _validate_sequence(value: object) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise FailureScriptError("failure sequence must be an integer")
    if not 1 <= value <= _MAX_SEQUENCE:
        raise FailureScriptError("failure sequence must be between 1 and 2^31-1")
    return value


def _validate_kind(value: object) -> ScriptedFailureKind:
    if not isinstance(value, ScriptedFailureKind):
        raise FailureScriptError("failure kind must be a ScriptedFailureKind")
    return value


def _validated_integer(value: object, *, field: str, minimum: int, maximum: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise FailureScriptError(f"{field} must be an integer")
    if not minimum <= value <= maximum:
        raise FailureScriptError(f"{field} must be between {minimum} and {maximum}")
    return value

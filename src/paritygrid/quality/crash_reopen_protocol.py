"""Strict durable control and marker protocol for crash-reopen verification."""

from __future__ import annotations

import hashlib
import json
import os
import sys
import unicodedata
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path
from threading import Lock
from typing import BinaryIO, cast

CRASH_REOPEN_PROTOCOL_VERSION = 1
CRASH_REOPEN_SCENARIO = "checkpoint-crash-v1"
MAX_PROTOCOL_LINE_BYTES = 512
MAX_CONTROL_BYTES = 4_096
_BINARY_OPEN_FLAG = getattr(os, "O_BINARY", 0)


class CrashReopenProtocolError(Exception):
    """A crash-reopen control or marker violates the closed protocol."""


class CrashFailpoint(StrEnum):
    NORMAL = "normal"
    BEFORE_COMMIT = "before_commit"
    COMMIT_AMBIGUOUS = "commit_ambiguous"
    AFTER_COMMIT_BEFORE_RECEIPT = "after_commit_before_receipt"
    AFTER_RECEIPT_BEFORE_NOTIFICATION = "after_receipt_before_notification"
    SHUTDOWN_DRAIN = "shutdown_drain"


class CrashMarker(StrEnum):
    WORKER_READY = "worker_ready"
    COMMAND_ADMITTED = "command_admitted"
    PRE_COMMIT = "pre_commit"
    COMMIT_ENTERED = "commit_entered"
    COMMIT_CONFIRMED = "commit_confirmed"
    SESSION_CLOSE_ENTERED = "session_close_entered"
    SESSION_CLOSED = "session_closed"
    RECEIPT_RESOLVED = "receipt_resolved"
    NOTIFICATION_ENTERED = "notification_entered"
    NOTIFICATION_OFFERED = "notification_offered"
    COMMAND_OBSERVED = "command_observed"
    SHUTDOWN_ENTERED = "shutdown_entered"
    SHUTDOWN_DRAINED = "shutdown_drained"


_CONTROL_FIELDS = frozenset(
    {
        "protocol_version",
        "scenario",
        "case_id",
        "failpoint",
        "command_ordinal",
        "database_path",
        "marker_path",
        "seed",
        "hold_timeout_seconds",
        "invocation_token",
    }
)
_MARKER_FIELDS = frozenset(
    {
        "protocol_version",
        "invocation_token",
        "case_id",
        "sequence",
        "command_ordinal",
        "marker",
    }
)
_RELEASE_FIELDS = frozenset({"protocol_version", "invocation_token", "action"})


def _strict_json_object(
    encoded: bytes, *, maximum_bytes: int = MAX_PROTOCOL_LINE_BYTES
) -> dict[str, object]:
    if not encoded or len(encoded) > maximum_bytes or encoded.startswith(b"\xef\xbb\xbf"):
        raise CrashReopenProtocolError("protocol frame size or encoding is invalid")
    if b"\r" in encoded or b"\x00" in encoded:
        raise CrashReopenProtocolError("protocol frame contains unsupported bytes")

    def object_pairs(pairs: list[tuple[str, object]]) -> dict[str, object]:
        result: dict[str, object] = {}
        for key, value in pairs:
            if key in result:
                raise CrashReopenProtocolError("protocol frame contains duplicate fields")
            result[key] = value
        return result

    try:
        decoded = json.loads(encoded.decode("utf-8"), object_pairs_hook=object_pairs)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise CrashReopenProtocolError("protocol frame is not canonical UTF-8 JSON") from error
    if type(decoded) is not dict:
        raise CrashReopenProtocolError("protocol frame must be a JSON object")
    return cast(dict[str, object], decoded)


def _canonical_bytes(
    value: dict[str, object], *, maximum_bytes: int = MAX_PROTOCOL_LINE_BYTES
) -> bytes:
    encoded = (
        json.dumps(value, ensure_ascii=True, sort_keys=True, separators=(",", ":")) + "\n"
    ).encode("utf-8")
    if len(encoded) > maximum_bytes:
        raise CrashReopenProtocolError("protocol frame exceeds its byte bound")
    return encoded


def _require_nfc_text(value: object, subject: str) -> str:
    if type(value) is not str or not value or unicodedata.normalize("NFC", value) != value:
        raise CrashReopenProtocolError(f"{subject} is invalid")
    if any(ord(character) < 32 or ord(character) == 127 for character in value):
        raise CrashReopenProtocolError(f"{subject} is invalid")
    return value


def _require_path_text(value: object, subject: str) -> str:
    if type(value) is not str or not value:
        raise CrashReopenProtocolError(f"{subject} is invalid")
    if any(ord(character) < 32 or ord(character) == 127 for character in value):
        raise CrashReopenProtocolError(f"{subject} is invalid")
    return value


def _require_exact_int(value: object, subject: str, minimum: int, maximum: int) -> int:
    if type(value) is not int or not minimum <= value <= maximum:
        raise CrashReopenProtocolError(f"{subject} is invalid")
    return value


def _require_absolute_path(value: object, subject: str) -> Path:
    text = _require_path_text(value, subject)
    path = Path(text)
    if not path.is_absolute():
        raise CrashReopenProtocolError(f"{subject} must be absolute")
    try:
        for component in (path, *path.parents):
            if component.is_symlink() or component.is_junction():
                raise CrashReopenProtocolError(f"{subject} cannot contain linked components")
        return path.resolve(strict=False)
    except CrashReopenProtocolError:
        raise
    except OSError as error:
        raise CrashReopenProtocolError(f"{subject} could not be inspected") from error


def invocation_token(failpoint: CrashFailpoint, command_ordinal: int, seed: int) -> str:
    """Return the fixed synthetic correlation token for one closed case."""
    if type(failpoint) is not CrashFailpoint:
        raise TypeError("crash failpoint is invalid")
    _require_exact_int(command_ordinal, "command ordinal", 1, 3)
    _require_exact_int(seed, "scenario seed", 0, 4_294_967_295)
    payload = f"{CRASH_REOPEN_SCENARIO}|{failpoint.value}|{command_ordinal}|{seed}".encode()
    return hashlib.sha256(payload).hexdigest()


@dataclass(frozen=True, slots=True)
class CrashControlEnvelope:
    failpoint: CrashFailpoint
    command_ordinal: int
    database_path: Path
    marker_path: Path
    seed: int
    hold_timeout_seconds: float
    invocation_token: str

    @property
    def case_id(self) -> str:
        return self.failpoint.value

    def __post_init__(self) -> None:
        if type(self.failpoint) is not CrashFailpoint:
            raise CrashReopenProtocolError("control failpoint is invalid")
        _require_exact_int(self.command_ordinal, "command ordinal", 1, 3)
        _require_exact_int(self.seed, "scenario seed", 0, 4_294_967_295)
        if type(self.hold_timeout_seconds) is not float or not 1 <= self.hold_timeout_seconds <= 30:
            raise CrashReopenProtocolError("hold timeout is invalid")
        database_path = _require_absolute_path(str(self.database_path), "database path")
        marker_path = _require_absolute_path(str(self.marker_path), "marker path")
        if database_path == marker_path:
            raise CrashReopenProtocolError("database and marker paths must be distinct")
        token = _require_nfc_text(self.invocation_token, "invocation token")
        expected = invocation_token(self.failpoint, self.command_ordinal, self.seed)
        if token != expected:
            raise CrashReopenProtocolError("invocation token does not match the case")
        object.__setattr__(self, "database_path", database_path)
        object.__setattr__(self, "marker_path", marker_path)

    def to_bytes(self) -> bytes:
        return _canonical_bytes(
            {
                "case_id": self.case_id,
                "command_ordinal": self.command_ordinal,
                "database_path": str(self.database_path),
                "failpoint": self.failpoint.value,
                "hold_timeout_seconds": self.hold_timeout_seconds,
                "invocation_token": self.invocation_token,
                "marker_path": str(self.marker_path),
                "protocol_version": CRASH_REOPEN_PROTOCOL_VERSION,
                "scenario": CRASH_REOPEN_SCENARIO,
                "seed": self.seed,
            },
            maximum_bytes=MAX_CONTROL_BYTES,
        )

    @classmethod
    def from_bytes(cls, encoded: bytes) -> CrashControlEnvelope:
        value = _strict_json_object(encoded, maximum_bytes=MAX_CONTROL_BYTES)
        if frozenset(value) != _CONTROL_FIELDS:
            raise CrashReopenProtocolError("control fields do not match the protocol")
        if value["protocol_version"] != CRASH_REOPEN_PROTOCOL_VERSION:
            raise CrashReopenProtocolError("control protocol version is unsupported")
        if value["scenario"] != CRASH_REOPEN_SCENARIO:
            raise CrashReopenProtocolError("control scenario is unsupported")
        failpoint_text = _require_nfc_text(value["failpoint"], "control failpoint")
        try:
            failpoint = CrashFailpoint(failpoint_text)
        except ValueError as error:
            raise CrashReopenProtocolError("control failpoint is unsupported") from error
        if value["case_id"] != failpoint.value:
            raise CrashReopenProtocolError("control case identity is invalid")
        timeout = value["hold_timeout_seconds"]
        if type(timeout) is int:
            timeout_value = float(timeout)
        elif type(timeout) is float:
            timeout_value = timeout
        else:
            raise CrashReopenProtocolError("hold timeout is invalid")
        return cls(
            failpoint=failpoint,
            command_ordinal=_require_exact_int(value["command_ordinal"], "command ordinal", 1, 3),
            database_path=_require_absolute_path(value["database_path"], "database path"),
            marker_path=_require_absolute_path(value["marker_path"], "marker path"),
            seed=_require_exact_int(value["seed"], "scenario seed", 0, 4_294_967_295),
            hold_timeout_seconds=timeout_value,
            invocation_token=_require_nfc_text(value["invocation_token"], "invocation token"),
        )


def load_control(control_path: Path) -> CrashControlEnvelope:
    """Load one control file after validating its exact private-directory boundary."""
    control_path = _require_absolute_path(str(control_path), "control path")
    try:
        if not control_path.is_file() or control_path.is_symlink() or control_path.is_junction():
            raise CrashReopenProtocolError("control path must be a regular file")
        envelope = CrashControlEnvelope.from_bytes(control_path.read_bytes())
    except CrashReopenProtocolError:
        raise
    except OSError as error:
        raise CrashReopenProtocolError("control file could not be read") from error
    if envelope.marker_path.parent != control_path.parent:
        raise CrashReopenProtocolError("marker path must remain in the control directory")
    if envelope.database_path.parent != control_path.parent:
        raise CrashReopenProtocolError("database path must remain in the control directory")
    return envelope


@dataclass(frozen=True, slots=True)
class CrashMarkerRecord:
    invocation_token: str
    case_id: str
    sequence: int
    command_ordinal: int
    marker: CrashMarker

    def __post_init__(self) -> None:
        token = _require_nfc_text(self.invocation_token, "marker token")
        case_id = _require_nfc_text(self.case_id, "marker case")
        if len(token) != 64 or any(character not in "0123456789abcdef" for character in token):
            raise CrashReopenProtocolError("marker token is invalid")
        try:
            CrashFailpoint(case_id)
        except ValueError as error:
            raise CrashReopenProtocolError("marker case is unsupported") from error
        _require_exact_int(self.sequence, "marker sequence", 1, 1_000)
        _require_exact_int(self.command_ordinal, "marker command ordinal", 0, 3)
        if type(self.marker) is not CrashMarker:
            raise CrashReopenProtocolError("marker value is invalid")

    def to_bytes(self) -> bytes:
        return _canonical_bytes(
            {
                "case_id": self.case_id,
                "command_ordinal": self.command_ordinal,
                "invocation_token": self.invocation_token,
                "marker": self.marker.value,
                "protocol_version": CRASH_REOPEN_PROTOCOL_VERSION,
                "sequence": self.sequence,
            }
        )

    @classmethod
    def from_bytes(cls, encoded: bytes) -> CrashMarkerRecord:
        value = _strict_json_object(encoded)
        if frozenset(value) != _MARKER_FIELDS:
            raise CrashReopenProtocolError("marker fields do not match the protocol")
        if value["protocol_version"] != CRASH_REOPEN_PROTOCOL_VERSION:
            raise CrashReopenProtocolError("marker protocol version is unsupported")
        try:
            marker = CrashMarker(_require_nfc_text(value["marker"], "marker value"))
        except ValueError as error:
            raise CrashReopenProtocolError("marker value is unsupported") from error
        return cls(
            invocation_token=_require_nfc_text(value["invocation_token"], "marker token"),
            case_id=_require_nfc_text(value["case_id"], "marker case"),
            sequence=_require_exact_int(value["sequence"], "marker sequence", 1, 1_000),
            command_ordinal=_require_exact_int(
                value["command_ordinal"], "marker command ordinal", 0, 3
            ),
            marker=marker,
        )


class CrashMarkerEmitter:
    """Append and fsync each marker before exposing the same bytes on stdout."""

    def __init__(self, envelope: CrashControlEnvelope, stdout: BinaryIO | None = None) -> None:
        if type(envelope) is not CrashControlEnvelope:
            raise CrashReopenProtocolError("marker envelope is invalid")
        self._envelope = envelope
        self._stdout = sys.stdout.buffer if stdout is None else stdout
        self._sequence = 0
        self._lock = Lock()

    def emit(self, marker: CrashMarker, command_ordinal: int) -> CrashMarkerRecord:
        with self._lock:
            self._sequence += 1
            record = CrashMarkerRecord(
                invocation_token=self._envelope.invocation_token,
                case_id=self._envelope.case_id,
                sequence=self._sequence,
                command_ordinal=command_ordinal,
                marker=marker,
            )
            encoded = record.to_bytes()
            descriptor = os.open(
                self._envelope.marker_path,
                os.O_APPEND | os.O_CREAT | os.O_WRONLY | _BINARY_OPEN_FLAG,
                0o600,
            )
            try:
                written = os.write(descriptor, encoded)
                if written != len(encoded):
                    raise CrashReopenProtocolError("marker ledger write was incomplete")
                os.fsync(descriptor)
            finally:
                os.close(descriptor)
            self._stdout.write(encoded)
            self._stdout.flush()
            return record


def release_commit_bytes(token: str) -> bytes:
    return _canonical_bytes(
        {
            "action": "release_commit",
            "invocation_token": _require_nfc_text(token, "release token"),
            "protocol_version": CRASH_REOPEN_PROTOCOL_VERSION,
        }
    )


def validate_release_commit(encoded: bytes, token: str) -> None:
    value = _strict_json_object(encoded)
    if frozenset(value) != _RELEASE_FIELDS:
        raise CrashReopenProtocolError("release fields do not match the protocol")
    if value["protocol_version"] != CRASH_REOPEN_PROTOCOL_VERSION:
        raise CrashReopenProtocolError("release protocol version is unsupported")
    if value["action"] != "release_commit" or value["invocation_token"] != token:
        raise CrashReopenProtocolError("release control does not match the invocation")


__all__ = [
    "CRASH_REOPEN_PROTOCOL_VERSION",
    "CRASH_REOPEN_SCENARIO",
    "MAX_CONTROL_BYTES",
    "MAX_PROTOCOL_LINE_BYTES",
    "CrashControlEnvelope",
    "CrashFailpoint",
    "CrashMarker",
    "CrashMarkerEmitter",
    "CrashMarkerRecord",
    "CrashReopenProtocolError",
    "invocation_token",
    "load_control",
    "release_commit_bytes",
    "validate_release_commit",
]

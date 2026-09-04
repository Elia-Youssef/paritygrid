"""Closed, versioned capability matrix for every known runtime profile (P21.3).

The matrix reports each known profile exactly once -- the sequential,
threaded, and asyncio full-plan strategies plus the two subordinate
pools -- with exactly one semantic state.  ``available`` entries carry
no reason; ``unavailable`` and ``unsupported`` entries carry a bounded
sanitized reason composed from closed templates plus at most an
exception class name, never an exception message or a path.  Full-plan
strategies and subordinate pools are structurally distinct through the
``kind`` field and are never interchangeable.

State assignment:

- ``sequential`` is always available in the supported runtime.
- ``threaded`` and ``asyncio`` follow their accepted detectors.
- The subordinate process pool is ``unsupported`` when the spawn start
  method does not exist, ``unavailable`` when the accepted detector
  reports unavailable under a working spawn context, and otherwise
  ``available``.
- The subordinate interpreter pool is ``unsupported`` when
  ``InterpreterPoolExecutor`` does not exist, ``unavailable`` when the
  accepted live probe fails, and otherwise ``available``.

The producer records safe platform facts only and the consumer rejects
any payload that is not exactly the closed contract, so an omitted
profile is never inferred available and one strategy never substitutes
for another.
"""

from __future__ import annotations

import concurrent.futures
import json
import multiprocessing
import platform
import sqlite3
from dataclasses import dataclass
from enum import StrEnum
from typing import cast

from paritygrid import __version__
from paritygrid.application.execution.concurrency_settings import (
    CapturedConcurrencySettings,
    describe_known_strategies,
)
from paritygrid.application.execution.runtime_capabilities import (
    SUBORDINATE_INTERPRETER_POOL_ID,
    SUBORDINATE_PROCESS_POOL_ID,
    detect_asyncio_capability,
    detect_interpreter_capability,
    detect_process_pool_capability,
    detect_threaded_capability,
)

CAPABILITY_MATRIX_FORMAT = "paritygrid-runtime-capability-matrix"
CAPABILITY_MATRIX_VERSION = 1
MAX_REASON_LENGTH = 160

_MAX_TEXT_LENGTH = 128
_MAX_THREADSAFETY_BOUND = 99
_FORBIDDEN_REASON_CHARACTERS = frozenset({"/", "\\", "%"})
_REASON_DETAIL_SEPARATOR = ": "

_THREAD_CAPABILITY_UNAVAILABLE = "threaded detector reported unavailable"
_ASYNCIO_CAPABILITY_UNAVAILABLE = "asyncio detector reported unavailable"
_SPAWN_CONTEXT_UNSUPPORTED = "spawn start method is not provided by this runtime"
_PROCESS_POOL_CAPABILITY_UNAVAILABLE = "process pool detector reported unavailable"
_INTERPRETER_EXECUTOR_UNSUPPORTED = "InterpreterPoolExecutor is not provided by this runtime"
_INTERPRETER_POOL_CAPABILITY_UNAVAILABLE = "interpreter pool detector reported unavailable"


class CapabilityMatrixError(ValueError):
    """A capability matrix document violated the closed versioned contract."""


class CapabilityState(StrEnum):
    """The exact semantic states one profile can report."""

    AVAILABLE = "available"
    UNAVAILABLE = "unavailable"
    UNSUPPORTED = "unsupported"


class ProfileKind(StrEnum):
    """The structural kind that keeps full-plan strategies and pools distinct."""

    FULL_PLAN = "full_plan"
    SUBORDINATE_POOL = "subordinate_pool"


def _accepted_full_plan_ids() -> tuple[str, ...]:
    """Return the accepted full-plan strategy identifiers in fixed order."""
    return tuple(
        availability.strategy_id
        for availability in describe_known_strategies(CapturedConcurrencySettings())
    )


_PROFILE_REGISTRY: tuple[tuple[str, ProfileKind], ...] = (
    *((strategy_id, ProfileKind.FULL_PLAN) for strategy_id in _accepted_full_plan_ids()),
    (SUBORDINATE_PROCESS_POOL_ID, ProfileKind.SUBORDINATE_POOL),
    (SUBORDINATE_INTERPRETER_POOL_ID, ProfileKind.SUBORDINATE_POOL),
)
_PROFILE_KINDS: dict[str, ProfileKind] = dict(_PROFILE_REGISTRY)

_DOCUMENT_FIELD_NAMES = frozenset({"format", "version", "package_version", "platform", "profiles"})
_PLATFORM_FIELD_NAMES = frozenset(
    {
        "system",
        "machine",
        "python_implementation",
        "python_version",
        "sqlite_version",
        "sqlite_threadsafety",
    }
)
_ENTRY_FIELD_NAMES = frozenset({"profile_id", "kind", "state", "reason"})


def _check_reason_characters(value: str, subject: str) -> None:
    """Reject any character that a reason component may never carry."""
    if not value:
        raise CapabilityMatrixError(f"{subject} must not be empty")
    for character in value:
        if not "\x20" <= character <= "\x7e" or character in _FORBIDDEN_REASON_CHARACTERS:
            raise CapabilityMatrixError(f"{subject} uses a forbidden character")
    if value != value.strip():
        raise CapabilityMatrixError(f"{subject} must not carry leading or trailing whitespace")


def _validate_reason_text(value: object) -> None:
    """Enforce the closed reason bound and content rules."""
    if type(value) is not str:
        raise CapabilityMatrixError("capability reason must be text")
    reason = value
    if not 1 <= len(reason) <= MAX_REASON_LENGTH:
        raise CapabilityMatrixError("capability reason length is outside the supported range")
    _check_reason_characters(reason, "capability reason")


def _sanitize_reason(template: str, detail: str | None = None) -> str:
    """Compose a bounded reason from a closed template and an optional class name."""
    _check_reason_characters(template, "reason template")
    if detail is None:
        reason = template
    else:
        _check_reason_characters(detail, "reason detail")
        budget = MAX_REASON_LENGTH - len(template) - len(_REASON_DETAIL_SEPARATOR)
        if budget < 1:
            raise CapabilityMatrixError("reason template leaves no room for detail")
        reason = f"{template}{_REASON_DETAIL_SEPARATOR}{detail[:budget].rstrip()}"
    _validate_reason_text(reason)
    return reason


def _bounded_ascii_text(value: object, subject: str) -> str:
    """Validate one bounded, non-empty printable-ASCII text field."""
    if type(value) is not str:
        raise CapabilityMatrixError(f"{subject} must be text")
    text = value
    if not 1 <= len(text) <= _MAX_TEXT_LENGTH:
        raise CapabilityMatrixError(f"{subject} length is outside the supported bound")
    for character in text:
        if not "\x20" <= character <= "\x7e":
            raise CapabilityMatrixError(f"{subject} must use printable ASCII characters")
    return text


def _threadsafety_value(value: object) -> int:
    """Validate the bounded sqlite threadsafety fact and return it."""
    if type(value) is not int:
        raise CapabilityMatrixError("platform sqlite threadsafety must be an integer")
    if not 0 <= value <= _MAX_THREADSAFETY_BOUND:
        raise CapabilityMatrixError("platform sqlite threadsafety is outside the supported range")
    return value


def _reason_value(value: object) -> str:
    """Validate one non-available reason and return it."""
    _validate_reason_text(value)
    return cast("str", value)


@dataclass(frozen=True, slots=True)
class PlatformFacts:
    """The exact safe platform facts the contract allows."""

    system: str
    machine: str
    python_implementation: str
    python_version: str
    sqlite_version: str
    sqlite_threadsafety: int

    def __post_init__(self) -> None:
        _bounded_ascii_text(self.system, "platform system")
        _bounded_ascii_text(self.machine, "platform machine")
        _bounded_ascii_text(self.python_implementation, "platform python implementation")
        _bounded_ascii_text(self.python_version, "platform python version")
        _bounded_ascii_text(self.sqlite_version, "platform sqlite version")
        _threadsafety_value(self.sqlite_threadsafety)


@dataclass(frozen=True, slots=True)
class CapabilityProfileEntry:
    """One known profile with exactly one semantic state."""

    profile_id: str
    kind: ProfileKind
    state: CapabilityState
    reason: str | None

    def __post_init__(self) -> None:
        if type(self.profile_id) is not str or self.profile_id not in _PROFILE_KINDS:
            raise CapabilityMatrixError("capability matrix profile identifier is unknown")
        expected_kind = _PROFILE_KINDS[self.profile_id]
        if type(self.kind) is not ProfileKind or self.kind is not expected_kind:
            raise CapabilityMatrixError("capability matrix profile kind contradicts the registry")
        if type(self.state) is not CapabilityState:
            raise CapabilityMatrixError("capability matrix profile state is unknown")
        if self.state is CapabilityState.AVAILABLE:
            if self.reason is not None:
                raise CapabilityMatrixError("an available profile must not carry a reason")
        else:
            _validate_reason_text(self.reason)


@dataclass(frozen=True, slots=True)
class CapabilityMatrixDocument:
    """Immutable closed capability matrix for one runtime."""

    format: str
    version: int
    package_version: str
    platform: PlatformFacts
    profiles: tuple[CapabilityProfileEntry, ...]

    def __post_init__(self) -> None:
        if self.format != CAPABILITY_MATRIX_FORMAT:
            raise CapabilityMatrixError("capability matrix format name is unknown")
        if type(self.version) is not int or self.version != CAPABILITY_MATRIX_VERSION:
            raise CapabilityMatrixError("capability matrix version is not supported")
        _bounded_ascii_text(self.package_version, "package version")
        if type(self.platform) is not PlatformFacts:
            raise CapabilityMatrixError("capability matrix platform facts must use PlatformFacts")
        if type(self.profiles) is not tuple:
            raise CapabilityMatrixError("capability matrix profiles must be a tuple")
        expected_ids = tuple(profile_id for profile_id, _ in _PROFILE_REGISTRY)
        if tuple(entry.profile_id for entry in self.profiles) != expected_ids:
            raise CapabilityMatrixError(
                "capability matrix must report every known profile exactly once"
            )

    def to_mapping(self) -> dict[str, object]:
        """Return the exact closed JSON mapping for deterministic encoding."""
        return {
            "format": self.format,
            "version": self.version,
            "package_version": self.package_version,
            "platform": {
                "system": self.platform.system,
                "machine": self.platform.machine,
                "python_implementation": self.platform.python_implementation,
                "python_version": self.platform.python_version,
                "sqlite_version": self.platform.sqlite_version,
                "sqlite_threadsafety": self.platform.sqlite_threadsafety,
            },
            "profiles": [
                {
                    "profile_id": entry.profile_id,
                    "kind": entry.kind.value,
                    "state": entry.state.value,
                    "reason": entry.reason,
                }
                for entry in self.profiles
            ],
        }


def _capture_platform_facts() -> PlatformFacts:
    """Capture only the six safe platform facts allowed by the contract."""
    return PlatformFacts(
        system=platform.system(),
        machine=platform.machine(),
        python_implementation=platform.python_implementation(),
        python_version=platform.python_version(),
        sqlite_version=sqlite3.sqlite_version,
        sqlite_threadsafety=sqlite3.threadsafety,
    )


def _full_plan_probe_entry(profile_id: str) -> CapabilityProfileEntry:
    """Probe one full-plan strategy and return its exact entry."""
    if profile_id == "sequential":
        state, reason = CapabilityState.AVAILABLE, None
    elif profile_id == "threaded":
        detected = detect_threaded_capability()
        state = CapabilityState.AVAILABLE if detected else CapabilityState.UNAVAILABLE
        reason = None if detected else _sanitize_reason(_THREAD_CAPABILITY_UNAVAILABLE)
    elif profile_id == "asyncio":
        detected = detect_asyncio_capability()
        state = CapabilityState.AVAILABLE if detected else CapabilityState.UNAVAILABLE
        reason = None if detected else _sanitize_reason(_ASYNCIO_CAPABILITY_UNAVAILABLE)
    else:
        raise CapabilityMatrixError("full-plan profile has no accepted probe")
    return CapabilityProfileEntry(
        profile_id=profile_id,
        kind=ProfileKind.FULL_PLAN,
        state=state,
        reason=reason,
    )


def _process_pool_entry() -> CapabilityProfileEntry:
    """Classify the subordinate process pool from the spawn probe and detector."""
    try:
        multiprocessing.get_context("spawn")
    except ValueError:
        return CapabilityProfileEntry(
            profile_id=SUBORDINATE_PROCESS_POOL_ID,
            kind=ProfileKind.SUBORDINATE_POOL,
            state=CapabilityState.UNSUPPORTED,
            reason=_sanitize_reason(_SPAWN_CONTEXT_UNSUPPORTED),
        )
    if detect_process_pool_capability().available:
        return CapabilityProfileEntry(
            profile_id=SUBORDINATE_PROCESS_POOL_ID,
            kind=ProfileKind.SUBORDINATE_POOL,
            state=CapabilityState.AVAILABLE,
            reason=None,
        )
    return CapabilityProfileEntry(
        profile_id=SUBORDINATE_PROCESS_POOL_ID,
        kind=ProfileKind.SUBORDINATE_POOL,
        state=CapabilityState.UNAVAILABLE,
        reason=_sanitize_reason(_PROCESS_POOL_CAPABILITY_UNAVAILABLE),
    )


def _interpreter_executor_exists() -> bool:
    """Return whether this runtime provides InterpreterPoolExecutor at all."""
    return hasattr(concurrent.futures, "InterpreterPoolExecutor")


def _interpreter_pool_entry() -> CapabilityProfileEntry:
    """Classify the subordinate interpreter pool from existence and the live probe."""
    if not _interpreter_executor_exists():
        return CapabilityProfileEntry(
            profile_id=SUBORDINATE_INTERPRETER_POOL_ID,
            kind=ProfileKind.SUBORDINATE_POOL,
            state=CapabilityState.UNSUPPORTED,
            reason=_sanitize_reason(_INTERPRETER_EXECUTOR_UNSUPPORTED),
        )
    if detect_interpreter_capability().available:
        return CapabilityProfileEntry(
            profile_id=SUBORDINATE_INTERPRETER_POOL_ID,
            kind=ProfileKind.SUBORDINATE_POOL,
            state=CapabilityState.AVAILABLE,
            reason=None,
        )
    return CapabilityProfileEntry(
        profile_id=SUBORDINATE_INTERPRETER_POOL_ID,
        kind=ProfileKind.SUBORDINATE_POOL,
        state=CapabilityState.UNAVAILABLE,
        reason=_sanitize_reason(_INTERPRETER_POOL_CAPABILITY_UNAVAILABLE),
    )


def build_capability_matrix() -> CapabilityMatrixDocument:
    """Probe this runtime and return the closed capability matrix document.

    The state assignment follows the module contract: sequential is
    always available, threaded and asyncio follow their accepted
    detectors, the process pool distinguishes a missing spawn start
    method (unsupported) from a failing detector (unavailable), and the
    interpreter pool distinguishes a missing executor class
    (unsupported) from a failing live probe (unavailable).
    """
    return CapabilityMatrixDocument(
        format=CAPABILITY_MATRIX_FORMAT,
        version=CAPABILITY_MATRIX_VERSION,
        package_version=__version__,
        platform=_capture_platform_facts(),
        profiles=(
            _full_plan_probe_entry("sequential"),
            _full_plan_probe_entry("threaded"),
            _full_plan_probe_entry("asyncio"),
            _process_pool_entry(),
            _interpreter_pool_entry(),
        ),
    )


def capability_matrix_bytes() -> bytes:
    """Encode the built document as canonical deterministic ASCII JSON bytes."""
    return json.dumps(
        build_capability_matrix().to_mapping(),
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("ascii")


def _parse_profile_entries(raw_entries: list[object]) -> tuple[CapabilityProfileEntry, ...]:
    """Parse every profile entry strictly and normalize to the registry order."""
    parsed: dict[str, CapabilityProfileEntry] = {}
    for raw_entry in raw_entries:
        if not isinstance(raw_entry, dict):
            raise CapabilityMatrixError("capability matrix profile entries must be JSON objects")
        entry_fields = cast("dict[str, object]", raw_entry)
        if frozenset(entry_fields) != _ENTRY_FIELD_NAMES:
            raise CapabilityMatrixError("profile entries must match the closed contract exactly")
        profile_id = entry_fields["profile_id"]
        if type(profile_id) is not str or profile_id not in _PROFILE_KINDS:
            raise CapabilityMatrixError("capability matrix profile identifier is unknown")
        if profile_id in parsed:
            raise CapabilityMatrixError("capability matrix profiles must appear exactly once")
        expected_kind = _PROFILE_KINDS[profile_id]
        entry_kind = entry_fields["kind"]
        if type(entry_kind) is not str or entry_kind != expected_kind.value:
            raise CapabilityMatrixError("capability matrix profile kind contradicts the registry")
        raw_state = entry_fields["state"]
        if type(raw_state) is not str:
            raise CapabilityMatrixError("capability matrix profile state must be text")
        try:
            state = CapabilityState(raw_state)
        except ValueError:
            raise CapabilityMatrixError("capability matrix profile state is unknown") from None
        if state is CapabilityState.AVAILABLE:
            if entry_fields["reason"] is not None:
                raise CapabilityMatrixError("an available profile must not carry a reason")
            reason = None
        else:
            reason = _reason_value(entry_fields["reason"])
        parsed[profile_id] = CapabilityProfileEntry(
            profile_id=profile_id,
            kind=expected_kind,
            state=state,
            reason=reason,
        )
    if frozenset(parsed) != frozenset(_PROFILE_KINDS):
        raise CapabilityMatrixError(
            "capability matrix must report every known profile exactly once"
        )
    return tuple(parsed[profile_id] for profile_id, _ in _PROFILE_REGISTRY)


def parse_capability_matrix(payload: bytes) -> CapabilityMatrixDocument:
    """Strictly validate one capability matrix payload, failing closed."""
    try:
        text = payload.decode("utf-8")
    except (AttributeError, UnicodeDecodeError) as error:
        raise CapabilityMatrixError("capability matrix payload must be UTF-8 bytes") from error
    try:
        parsed: object = json.loads(text)
    except json.JSONDecodeError as error:
        raise CapabilityMatrixError("capability matrix payload must be valid JSON") from error
    if not isinstance(parsed, dict):
        raise CapabilityMatrixError("capability matrix payload must be a JSON object")
    fields = cast("dict[str, object]", parsed)
    if frozenset(fields) != _DOCUMENT_FIELD_NAMES:
        raise CapabilityMatrixError("capability matrix fields must match the closed contract")
    document_format = fields["format"]
    if type(document_format) is not str or document_format != CAPABILITY_MATRIX_FORMAT:
        raise CapabilityMatrixError("capability matrix format name is unknown")
    document_version = fields["version"]
    if type(document_version) is not int:
        raise CapabilityMatrixError("capability matrix version must be an integer")
    if document_version != CAPABILITY_MATRIX_VERSION:
        raise CapabilityMatrixError("capability matrix version is not supported")
    package_version = _bounded_ascii_text(fields["package_version"], "package version")
    raw_platform = fields["platform"]
    if not isinstance(raw_platform, dict):
        raise CapabilityMatrixError("capability matrix platform facts must be a JSON object")
    platform_fields = cast("dict[str, object]", raw_platform)
    if frozenset(platform_fields) != _PLATFORM_FIELD_NAMES:
        raise CapabilityMatrixError("platform facts must match the closed contract")
    raw_profiles = fields["profiles"]
    if not isinstance(raw_profiles, list):
        raise CapabilityMatrixError("capability matrix profiles must be a JSON array")
    return CapabilityMatrixDocument(
        format=CAPABILITY_MATRIX_FORMAT,
        version=CAPABILITY_MATRIX_VERSION,
        package_version=package_version,
        platform=PlatformFacts(
            system=_bounded_ascii_text(platform_fields["system"], "platform system"),
            machine=_bounded_ascii_text(platform_fields["machine"], "platform machine"),
            python_implementation=_bounded_ascii_text(
                platform_fields["python_implementation"], "platform python implementation"
            ),
            python_version=_bounded_ascii_text(
                platform_fields["python_version"], "platform python version"
            ),
            sqlite_version=_bounded_ascii_text(
                platform_fields["sqlite_version"], "platform sqlite version"
            ),
            sqlite_threadsafety=_threadsafety_value(platform_fields["sqlite_threadsafety"]),
        ),
        profiles=_parse_profile_entries(cast("list[object]", raw_profiles)),
    )


__all__ = [
    "CAPABILITY_MATRIX_FORMAT",
    "CAPABILITY_MATRIX_VERSION",
    "MAX_REASON_LENGTH",
    "CapabilityMatrixDocument",
    "CapabilityMatrixError",
    "CapabilityProfileEntry",
    "CapabilityState",
    "PlatformFacts",
    "ProfileKind",
    "build_capability_matrix",
    "capability_matrix_bytes",
    "parse_capability_matrix",
]

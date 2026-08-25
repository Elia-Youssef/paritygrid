"""Bounded idempotent cleanup with structured unresolved evidence (P7.10).

Concurrent cleanup closes every owned resource — channels, capacity
ledgers, strategy workers, pools, clients — through one bounded,
repeatable registry.  Every registered callback is attempted even when
an earlier one fails; the first failure is preserved as the raising
error while later failures attach to it, and anything that cannot close
inside the captured bound is reported as structured
:class:`UnresolvedResource` evidence instead of being silently dropped.

Running cleanup twice is safe: closed resources are skipped, and the
second report still enumerates anything that stayed unresolved.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from threading import Lock
from typing import Protocol, runtime_checkable

MAX_CLEANUP_RESOURCES = 64
MAX_CLEANUP_DETAIL_LENGTH = 256
MAX_CLEANUP_TIMEOUT_SECONDS = 86_400.0
CLEANUP_EVIDENCE_VERSION = 1


class ConcurrentCleanupError(RuntimeError):
    """Base failure for concurrent cleanup coordination."""


class ConcurrentCleanupInvalidRequestError(ConcurrentCleanupError):
    """A cleanup registration or request violated the contract."""


class ConcurrentCleanupFailedError(ConcurrentCleanupError):
    """One or more cleanup callbacks failed; evidence is attached."""


@dataclass(frozen=True, slots=True)
class UnresolvedResource:
    """One owned resource cleanup could not prove closed."""

    kind: str
    name: str
    detail: str

    def __post_init__(self) -> None:
        _require_detail_text(self.kind, "unresolved resource kind")
        _require_detail_text(self.name, "unresolved resource name")
        if self.detail:
            _require_detail_text(self.detail, "unresolved resource detail")


@dataclass(frozen=True, slots=True)
class CleanupReport:
    """Structured outcome of one bounded cleanup pass."""

    attempted: int
    closed: int
    already_closed: int
    unresolved: tuple[UnresolvedResource, ...]

    def __post_init__(self) -> None:
        for name in ("attempted", "closed", "already_closed"):
            value = getattr(self, name)
            if type(value) is not int or not 0 <= value <= MAX_CLEANUP_RESOURCES:
                raise ConcurrentCleanupInvalidRequestError(
                    f"cleanup report {name} is outside the supported range"
                )
        if type(self.unresolved) is not tuple:
            raise TypeError("cleanup unresolved evidence must be a tuple")
        ordered = tuple(sorted(self.unresolved, key=_unresolved_order_key))
        if ordered != self.unresolved:
            raise ConcurrentCleanupInvalidRequestError(
                "cleanup unresolved evidence must be deterministically sorted"
            )

    @property
    def is_complete(self) -> bool:
        """Report whether every attempted resource closed."""
        return self.attempted == self.closed + self.already_closed


@runtime_checkable
class CleanupResource(Protocol):
    """One owned closeable resource with bounded diagnostics."""

    @property
    def kind(self) -> str: ...

    @property
    def name(self) -> str: ...

    def close(self, *, timeout_seconds: float) -> None: ...


class ConcurrentCleanupCoordinator:
    """Registry-driven cleanup that never pretends unresolved state is closed."""

    __slots__ = ("_closed", "_lock", "_resources")

    def __init__(self) -> None:
        self._resources: list[CleanupResource] = []
        self._closed: set[tuple[str, str]] = set()
        self._lock = Lock()

    def register(self, resource: CleanupResource) -> None:
        """Register one owned resource for bounded idempotent cleanup."""
        _require_detail_text(resource.kind, "cleanup resource kind")
        _require_detail_text(resource.name, "cleanup resource name")
        with self._lock:
            if any(
                existing.kind == resource.kind and existing.name == resource.name
                for existing in self._resources
            ):
                raise ConcurrentCleanupInvalidRequestError("cleanup resource is already registered")
            if len(self._resources) >= MAX_CLEANUP_RESOURCES:
                raise ConcurrentCleanupInvalidRequestError(
                    "cleanup resource registry exceeds the supported bound"
                )
            self._resources.append(resource)

    @property
    def registered_count(self) -> int:
        with self._lock:
            return len(self._resources)

    def cleanup(self, *, timeout_seconds: float) -> CleanupReport:
        """Attempt every registered resource once, idempotently.

        Each unclosed resource is attempted with the captured bound even
        after earlier failures.  The first raised failure propagates as
        :class:`ConcurrentCleanupFailedError` after every attempt, with
        later failures attached through exception notes; resources that
        raised or timed out appear as unresolved evidence.
        """

        _require_timeout(timeout_seconds)
        first_failure: BaseException | None = None
        unresolved: list[UnresolvedResource] = []
        attempted = 0
        closed = 0
        already_closed = 0
        with self._lock:
            resources = tuple(self._resources)
        for resource in resources:
            key = (resource.kind, resource.name)
            with self._lock:
                if key in self._closed:
                    already_closed += 1
                    continue
            attempted += 1
            try:
                resource.close(timeout_seconds=timeout_seconds)
                with self._lock:
                    self._closed.add(key)
                closed += 1
            except BaseException as error:
                unresolved.append(
                    UnresolvedResource(
                        kind=resource.kind,
                        name=resource.name,
                        detail=str(error)[:MAX_CLEANUP_DETAIL_LENGTH] or error.__class__.__name__,
                    )
                )
                if first_failure is None:
                    first_failure = error
                else:
                    first_failure.add_note(
                        f"additional cleanup failure: {resource.kind}:{resource.name}: {error}"
                    )
        report = CleanupReport(
            attempted=attempted,
            closed=closed,
            already_closed=already_closed,
            unresolved=tuple(sorted(unresolved, key=_unresolved_order_key)),
        )
        if first_failure is not None:
            raise ConcurrentCleanupFailedError(
                "concurrent cleanup could not close every owned resource"
            ) from first_failure
        return report

    def snapshot(self) -> CleanupReport:
        """Return the current registry state without closing anything."""
        with self._lock:
            closed = sum(
                1 for resource in self._resources if (resource.kind, resource.name) in self._closed
            )
            return CleanupReport(
                attempted=0,
                closed=0,
                already_closed=closed,
                unresolved=(),
            )


def _unresolved_order_key(resource: UnresolvedResource) -> tuple[str, str]:
    return (resource.kind, resource.name)


def _require_timeout(value: object) -> None:
    if type(value) is not float and type(value) is not int:
        raise TypeError("cleanup timeout must be a finite non-negative second count")
    seconds = float(value)
    if not math.isfinite(seconds) or not 0.0 <= seconds <= MAX_CLEANUP_TIMEOUT_SECONDS:
        raise ConcurrentCleanupInvalidRequestError("cleanup timeout is outside the supported range")


def _require_detail_text(value: object, subject: str) -> None:
    if type(value) is not str:
        raise TypeError(f"{subject} must be text")
    text = value
    if not 1 <= len(text) <= MAX_CLEANUP_DETAIL_LENGTH:
        raise ConcurrentCleanupInvalidRequestError(
            f"{subject} length is outside the supported range"
        )
    for character in text:
        if not "\x20" <= character <= "\x7e":
            raise ConcurrentCleanupInvalidRequestError(
                f"{subject} must use printable ASCII characters"
            )


__all__ = [
    "CLEANUP_EVIDENCE_VERSION",
    "MAX_CLEANUP_RESOURCES",
    "MAX_CLEANUP_TIMEOUT_SECONDS",
    "CleanupReport",
    "CleanupResource",
    "ConcurrentCleanupCoordinator",
    "ConcurrentCleanupError",
    "ConcurrentCleanupFailedError",
    "ConcurrentCleanupInvalidRequestError",
    "UnresolvedResource",
]

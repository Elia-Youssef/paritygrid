"""Stable hand-off contract for controlling an owned active run.

The HTTP/application boundary deliberately never owns an executing runner.
Only a runtime that owns that runner may request a pause, resume it, or
cancel and clean it up.  This port carries the small, durable evidence surface
needed to hand those requests across that ownership boundary.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from typing import Protocol, runtime_checkable

from paritygrid.application.ports.execution import RunRecord
from paritygrid.application.ports.writer import WriterSubmissionId
from paritygrid.domain.models import RunId

MAX_RUN_CONTROL_TIMEOUT_SECONDS = 300.0


class RunControlAction(StrEnum):
    """The closed set of lifecycle controls owned by an active executor."""

    PAUSE = "pause"
    RESUME = "resume"
    CANCEL = "cancel"


class ActiveRunControlError(RuntimeError):
    """Base failure for the runtime-owned active-run control boundary."""


class ActiveRunControlNotFoundError(ActiveRunControlError):
    """No live execution owner is registered for the addressed run."""


class ActiveRunControlBusyError(ActiveRunControlError):
    """The owner could not accept one bounded overlapping control request."""


class ActiveRunControlTimeoutError(ActiveRunControlError):
    """A runtime-owned control operation exhausted its explicit time bound."""


class ActiveRunControlClosedError(ActiveRunControlError):
    """The runtime has begun releasing active execution ownership."""


class ActiveRunControlEvidenceError(ActiveRunControlError):
    """An owner returned malformed or insufficient durable control evidence."""


@dataclass(frozen=True, slots=True, repr=False)
class RunControlEvidence:
    """A run record and writer identities proving one owner-side outcome."""

    run: RunRecord
    submission_ids: tuple[WriterSubmissionId, ...]

    def __post_init__(self) -> None:
        if type(self.run) is not RunRecord:
            raise TypeError("run-control evidence must carry a RunRecord")
        if type(self.submission_ids) is not tuple or any(
            type(item) is not WriterSubmissionId for item in self.submission_ids
        ):
            raise TypeError("run-control evidence submissions must use WriterSubmissionId")

    def __repr__(self) -> str:
        return (
            "RunControlEvidence("
            f"run_id={self.run.run_id!r}, run_row_version={self.run.row_version!r}, "
            f"state={self.run.state.value!r}, submissions={len(self.submission_ids)})"
        )


@runtime_checkable
class ActiveRunControlOwner(Protocol):
    """One runtime-owned execution instance that can safely control its run.

    Implementations use the accepted sequential or concurrent lifecycle
    coordinators.  They must return only after they have durable evidence for
    the requested state, honor ``timeout_seconds`` for all waits, and release
    their own resources through ``close``.
    """

    def pause(
        self,
        *,
        correlation_id: str | None,
        timeout_seconds: float,
        converge_on_duplicate: bool,
    ) -> RunControlEvidence: ...

    def resume(
        self,
        *,
        correlation_id: str | None,
        timeout_seconds: float,
        converge_on_duplicate: bool,
    ) -> RunControlEvidence: ...

    def cancel(
        self,
        *,
        correlation_id: str | None,
        timeout_seconds: float,
        converge_on_duplicate: bool,
    ) -> RunControlEvidence: ...

    def close(self, *, timeout_seconds: float) -> None:
        """Release execution-owned resources within the supplied bound."""
        ...


@runtime_checkable
class ActiveRunControlRegistry(Protocol):
    """Application-facing lookup and delegation surface owned by runtime."""

    def dispatch(
        self,
        run_id: RunId,
        *,
        action: RunControlAction,
        correlation_id: str | None,
        timeout_seconds: float,
        converge_on_duplicate: bool,
    ) -> RunControlEvidence: ...

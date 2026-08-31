"""Bounded runtime ownership registry for active execution controls."""

from __future__ import annotations

from dataclasses import dataclass
from threading import Lock
from time import monotonic

from paritygrid.application.ports.run_control import (
    MAX_RUN_CONTROL_TIMEOUT_SECONDS,
    ActiveRunControlBusyError,
    ActiveRunControlClosedError,
    ActiveRunControlError,
    ActiveRunControlEvidenceError,
    ActiveRunControlNotFoundError,
    ActiveRunControlOwner,
    ActiveRunControlRegistry,
    ActiveRunControlTimeoutError,
    RunControlAction,
    RunControlEvidence,
)
from paritygrid.domain.models import RunId

MAX_ACTIVE_RUN_CONTROLS = 64
DEFAULT_ACTIVE_RUN_CONTROL_TIMEOUT_SECONDS = 5.0


@dataclass(slots=True)
class _OwnerSlot:
    owner: ActiveRunControlOwner
    gate: Lock
    retired: bool = False


class RuntimeActiveRunControlRegistry(ActiveRunControlRegistry):
    """Own a bounded set of live execution controllers until cleanup.

    A slot is serialized so one run cannot receive competing lifecycle
    commands.  Lookup and registry mutations only hold the registry lock;
    every potentially blocking owner call happens outside it.  Runtime
    shutdown first closes admission to this registry, then gives each owner
    the remaining global cleanup budget.
    """

    __slots__ = ("_capacity", "_closed", "_controls", "_lock")

    def __init__(self, *, capacity: int = MAX_ACTIVE_RUN_CONTROLS) -> None:
        if type(capacity) is not int or not 1 <= capacity <= MAX_ACTIVE_RUN_CONTROLS:
            raise ValueError("active-run control capacity is outside the supported range")
        self._capacity = capacity
        self._controls: dict[RunId, _OwnerSlot] = {}
        self._lock = Lock()
        self._closed = False

    @property
    def capacity(self) -> int:
        """Return the fixed maximum number of simultaneously owned runs."""
        return self._capacity

    @property
    def active_count(self) -> int:
        """Return the bounded number of currently registered owners."""
        with self._lock:
            return len(self._controls)

    def register(self, run_id: RunId, owner: object) -> None:
        """Register exactly one execution owner for one active durable run."""
        identity = _run_id(run_id)
        if not isinstance(owner, ActiveRunControlOwner):
            raise TypeError("active run control owner does not satisfy the control contract")
        with self._lock:
            if self._closed:
                raise ActiveRunControlClosedError("active-run control registry is closing")
            if identity in self._controls:
                raise ActiveRunControlBusyError("an execution owner is already registered")
            if len(self._controls) >= self._capacity:
                raise ActiveRunControlBusyError("active-run control registry capacity is exhausted")
            self._controls[identity] = _OwnerSlot(owner, Lock())

    def unregister(
        self,
        run_id: RunId,
        *,
        timeout_seconds: float = DEFAULT_ACTIVE_RUN_CONTROL_TIMEOUT_SECONDS,
    ) -> None:
        """Attempt bounded cleanup, then retire one owner from delegation."""
        identity = _run_id(run_id)
        deadline = _deadline(timeout_seconds)
        with self._lock:
            slot = self._controls.get(identity)
            if slot is not None:
                # Retire and remove the slot before waiting for its operation
                # gate. A dispatcher that already captured this slot must
                # revalidate it after acquiring the gate and will fail closed.
                slot.retired = True
                del self._controls[identity]
        if slot is None:
            return
        self._close_slot(slot, deadline)

    def dispatch(
        self,
        run_id: RunId,
        *,
        action: RunControlAction,
        correlation_id: str | None,
        timeout_seconds: float,
        converge_on_duplicate: bool,
    ) -> RunControlEvidence:
        """Delegate a lifecycle request to its sole live execution owner."""
        identity = _run_id(run_id)
        if type(action) is not RunControlAction:
            raise TypeError("run-control action must use RunControlAction")
        if type(converge_on_duplicate) is not bool:
            raise TypeError("run-control convergence flag must be boolean")
        deadline = _deadline(timeout_seconds)
        with self._lock:
            if self._closed:
                raise ActiveRunControlClosedError("active-run control registry is closing")
            slot = self._controls.get(identity)
        if slot is None:
            raise ActiveRunControlNotFoundError("no active execution owner is registered")
        remaining = _remaining(deadline)
        if not slot.gate.acquire(timeout=remaining):
            raise ActiveRunControlBusyError("active execution control is already in progress")
        try:
            with self._lock:
                closed = self._closed
                still_owned = not slot.retired and self._controls.get(identity) is slot
            if not still_owned:
                if closed:
                    raise ActiveRunControlClosedError("active-run control registry is closing")
                raise ActiveRunControlNotFoundError(
                    "active execution owner was retired before control dispatch"
                )
            remaining = _remaining(deadline)
            operation = getattr(slot.owner, action.value)
            try:
                evidence = operation(
                    correlation_id=correlation_id,
                    timeout_seconds=remaining,
                    converge_on_duplicate=converge_on_duplicate,
                )
                _remaining(deadline)
            except ActiveRunControlError:
                raise
            except TimeoutError as error:
                raise ActiveRunControlTimeoutError(
                    "active execution control exceeded its time budget"
                ) from error
            except Exception as error:
                raise ActiveRunControlEvidenceError(
                    "active execution owner did not return a proven lifecycle outcome"
                ) from error
        finally:
            slot.gate.release()
        if type(evidence) is not RunControlEvidence or evidence.run.run_id != identity:
            raise ActiveRunControlEvidenceError(
                "active execution owner returned invalid lifecycle evidence"
            )
        return evidence

    def close(
        self,
        *,
        timeout_seconds: float = DEFAULT_ACTIVE_RUN_CONTROL_TIMEOUT_SECONDS,
    ) -> None:
        """Stop delegation and release every registered owner within one budget."""
        deadline = _deadline(timeout_seconds)
        with self._lock:
            if self._closed:
                return
            self._closed = True
            slots = tuple(self._controls.values())
            for slot in slots:
                slot.retired = True
            self._controls.clear()
        first_error: ActiveRunControlError | None = None
        for slot in slots:
            try:
                self._close_slot(slot, deadline)
            except ActiveRunControlError as error:
                if first_error is None:
                    first_error = error
        if first_error is not None:
            raise first_error

    def _close_slot(self, slot: _OwnerSlot, deadline: float) -> None:
        remaining = _remaining(deadline)
        if not slot.gate.acquire(timeout=remaining):
            raise ActiveRunControlTimeoutError("active execution cleanup exceeded its time budget")
        try:
            try:
                slot.owner.close(timeout_seconds=_remaining(deadline))
                _remaining(deadline)
            except ActiveRunControlError:
                raise
            except TimeoutError as error:
                raise ActiveRunControlTimeoutError(
                    "active execution cleanup exceeded its time budget"
                ) from error
            except Exception as error:
                raise ActiveRunControlEvidenceError("active execution cleanup failed") from error
        finally:
            slot.gate.release()


def _run_id(value: object) -> RunId:
    if type(value) is not RunId:
        raise TypeError("active run control identity must use RunId")
    return value


def _deadline(timeout_seconds: object) -> float:
    if (
        type(timeout_seconds) is not float
        or not 0.0 < timeout_seconds <= MAX_RUN_CONTROL_TIMEOUT_SECONDS
    ):
        raise ValueError("active-run control timeout is outside the supported range")
    return monotonic() + timeout_seconds


def _remaining(deadline: float) -> float:
    remaining = deadline - monotonic()
    if remaining <= 0.0:
        raise ActiveRunControlTimeoutError("active execution control exceeded its time budget")
    return remaining

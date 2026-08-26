"""Optional subordinate interpreter CPU pool (P7.16).

The interpreter pool is an experimental, optional subordinate CPU
executor under the same operation and isolation rules as the process
pool: registered connector-free operations only, the same versioned
bounded primitive codec, and no plan scheduling, persistence, or
artifact ownership.  It is never a fallback for the process pool —
when the runtime capability is absent it reports a structured reason
and construction fails closed.
"""

from __future__ import annotations

import concurrent.futures
import math
from collections.abc import Callable
from threading import Condition
from typing import Protocol, cast

from paritygrid.adapters.runners.process_pool import (
    SubordinatePoolError,
    SubordinateResult,
)
from paritygrid.adapters.runners.process_workers.worker import worker_entry
from paritygrid.adapters.runners.subordinate_codec import (
    decode_response,
    encode_request,
)
from paritygrid.application.execution.capacity import (
    CapacityPermit,
    SubordinateCallLimiter,
)

INTERPRETER_POOL_ID = "subordinate-interpreter-pool"


class InterpreterPoolUnavailableError(SubordinatePoolError):
    """This runtime cannot host the interpreter pool."""


class SubordinateInterpreterPool:
    """Interpreter-backed subordinate CPU pool over the same codec.

    The pool runs each registered operation through the runtime's
    ``InterpreterPoolExecutor`` when — and only when — that capability
    exists in the actual interpreter.  Capacity is the P7.6 subordinate
    CPU permit, exactly like the process pool.
    """

    __slots__ = (
        "_active_permits",
        "_capacity",
        "_closed",
        "_condition",
        "_executor",
        "_pending_submissions",
        "_timeout_seconds",
    )

    def __init__(
        self,
        *,
        capacity: SubordinateCallLimiter,
        timeout_seconds: float,
    ) -> None:
        if type(capacity) is not SubordinateCallLimiter:
            raise TypeError("interpreter pool capacity must use SubordinateCallLimiter")
        if type(timeout_seconds) is not float and type(timeout_seconds) is not int:
            raise TypeError("interpreter pool timeout must be a second count")
        seconds = float(timeout_seconds)
        if not 0.0 < seconds <= 600.0 or not math.isfinite(seconds):
            raise SubordinatePoolError("interpreter pool timeout is outside the bound")
        executor_factory = _resolve_interpreter_executor()
        if executor_factory is None:
            raise InterpreterPoolUnavailableError(
                "the runtime does not provide an interpreter pool executor"
            )
        self._capacity = capacity
        self._executor: _BoundedExecutor = executor_factory(max_workers=capacity.snapshot().limit)
        self._timeout_seconds = seconds
        self._condition = Condition()
        self._active_permits: dict[int, CapacityPermit] = {}
        self._pending_submissions = 0
        self._closed = False

    @property
    def is_closed(self) -> bool:
        return self._closed

    def submit(
        self,
        owner: str,
        operation_id: str,
        payload: dict[str, object],
        *,
        parent: tuple[CapacityPermit, CapacityPermit, CapacityPermit],
    ) -> SubordinateResult:
        """Run one registered operation in a bounded interpreter."""
        with self._condition:
            if self._closed:
                raise SubordinatePoolError("interpreter pool no longer admits operations")
            self._pending_submissions += 1
        permit: CapacityPermit | None = None
        future: _BoundedFuture | None = None
        registered = False
        try:
            request = encode_request(operation_id, 1, payload)
            permit = self._capacity.acquire(owner, parent=parent)
            future = self._executor.submit(worker_entry, request)
            with self._condition:
                self._active_permits[id(future)] = permit
            registered = True
            try:
                future.add_done_callback(self._complete_future)
            except Exception:
                self._complete_future(future)
                raise
            response = cast(bytes, future.result(timeout=self._timeout_seconds))
            self._complete_future(future)
        except TimeoutError as error:
            raise SubordinatePoolError(
                "interpreter operation exceeded its captured timeout and remains tracked"
            ) from error
        except Exception:
            if future is not None and future.done():
                self._complete_future(future)
            raise
        finally:
            if permit is not None and not registered:
                self._capacity.release(permit)
            with self._condition:
                self._pending_submissions -= 1
                self._condition.notify_all()
        from paritygrid.adapters.runners.subordinate_codec import decode_response

        responded_operation, result = decode_response(response)
        if responded_operation != operation_id:
            raise SubordinatePoolError(
                "interpreter response operation does not match the submitted operation"
            )
        return SubordinateResult(operation_id=responded_operation, result=result)

    def close(self) -> None:
        """Stop admission after every tracked operation reaches a safe boundary."""
        deadline = _deadline_after(self._timeout_seconds)
        with self._condition:
            self._closed = True
            while self._pending_submissions or self._active_permits:
                remaining = deadline - _monotonic_seconds()
                if remaining <= 0:
                    self._executor.shutdown(wait=False, cancel_futures=True)
                    raise SubordinatePoolError(
                        "interpreter pool shutdown left tracked operations unresolved"
                    )
                self._condition.wait(remaining)
        self._executor.shutdown(wait=True, cancel_futures=True)

    def _complete_future(self, future: _BoundedFuture) -> None:
        """Release a CPU permit only after its interpreter future completed."""
        with self._condition:
            permit = self._active_permits.pop(id(future), None)
        if permit is None:
            return
        try:
            self._capacity.release(permit)
        finally:
            with self._condition:
                self._condition.notify_all()


class _BoundedFuture(Protocol):
    """The result surface the pool consumes from one submitted call."""

    def result(self, *, timeout: float | None = None) -> object: ...

    def done(self) -> bool: ...

    def add_done_callback(self, callback: Callable[[_BoundedFuture], object], /) -> None: ...


class _BoundedExecutor(Protocol):
    """The executor surface the pool owns."""

    def submit(
        self, function: Callable[[bytes], bytes], /, *arguments: bytes
    ) -> _BoundedFuture: ...

    def shutdown(self, *, wait: bool, cancel_futures: bool) -> None: ...


def _resolve_interpreter_executor() -> Callable[..., _BoundedExecutor] | None:
    """Return the interpreter executor factory, or ``None`` when absent."""
    factory = getattr(concurrent.futures, "InterpreterPoolExecutor", None)
    if factory is None:
        return None
    return cast("Callable[..., _BoundedExecutor]", factory)


def interpreter_pool_availability() -> tuple[bool, str | None]:
    """Probe the actual runtime for interpreter-pool support."""
    factory = _resolve_interpreter_executor()
    if factory is None:
        return False, "concurrent.futures.InterpreterPoolExecutor is absent"
    try:
        probe: _BoundedExecutor = factory(max_workers=1)
        future = probe.submit(worker_entry, _PROBE_REQUEST)
        decode_response(cast(bytes, future.result(timeout=5.0)))
        probe.shutdown(wait=True, cancel_futures=True)
    except Exception as error:
        return False, f"interpreter probe failed: {error.__class__.__name__}"
    return True, None


_PROBE_REQUEST = encode_request("sort_integers", 1, {"values": [0]})


def _monotonic_seconds() -> float:
    """Read the monotonic cleanup clock without entering execution policy."""
    from time import monotonic

    return monotonic()


def _deadline_after(timeout_seconds: float) -> float:
    """Return one bounded wall-clock cleanup deadline."""
    return _monotonic_seconds() + timeout_seconds


__all__ = [
    "INTERPRETER_POOL_ID",
    "InterpreterPoolUnavailableError",
    "SubordinateInterpreterPool",
    "interpreter_pool_availability",
]

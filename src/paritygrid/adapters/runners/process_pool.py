"""Parent-owned subordinate process CPU pool (P7.14).

The pool is a subordinate CPU executor, never a full-plan runner: it
accepts only registered connector-free operations, encodes them with
the versioned subordinate codec, and runs each request in its own
spawn-context process so a worker crash can never corrupt the parent
or another request.  The parent owns every durable fact — artifacts,
results, leases, checkpoints, and SQLite stay entirely parent-side.

Capacity is the P7.6 subordinate CPU permit: every request acquires
its ``cpu_pool`` permit before a process exists and releases it when
the response lands.  Cancellation, worker crash, pool failure, and
shutdown terminate and join every owned process inside the captured
bound, so no orphan process survives.
"""

from __future__ import annotations

import multiprocessing
import multiprocessing.connection
import multiprocessing.process
from collections.abc import Callable
from dataclasses import dataclass
from threading import Condition
from time import monotonic

from paritygrid.adapters.runners.process_workers.worker import worker_entry
from paritygrid.adapters.runners.subordinate_codec import (
    MAX_PAYLOAD_BYTES,
    SubordinateCodecError,
    decode_response,
    encode_request,
)
from paritygrid.application.execution.capacity import (
    CapacityPermit,
    SubordinateCallLimiter,
)

PROCESS_POOL_CATEGORY = "cpu_pool"
DEFAULT_PROCESS_TIMEOUT_SECONDS = 30.0
MAX_PROCESS_TIMEOUT_SECONDS = 600.0


class SubordinatePoolError(RuntimeError):
    """Base failure for the subordinate process pool."""


class SubordinatePoolClosedError(SubordinatePoolError):
    """The pool no longer admits operations."""


class SubordinateWorkerCrashError(SubordinatePoolError):
    """A worker process terminated without a response."""


class SubordinateWorkerTimeoutError(SubordinatePoolError):
    """A worker process exceeded its captured execution bound."""


@dataclass(frozen=True, slots=True)
class SubordinateResult:
    """One completed subordinate CPU operation."""

    operation_id: str
    result: dict[str, object]

    @property
    def worker_error(self) -> str | None:
        """Return the marker detail when the worker refused the request."""
        detail = self.result.get("worker_error")
        return detail if type(detail) is str else None


class SubordinateProcessPool:
    """Spawn-context process pool for registered CPU operations."""

    __slots__ = (
        "_active_processes",
        "_capacity",
        "_closed",
        "_condition",
        "_context",
        "_entry",
        "_pending_submissions",
        "_timeout_seconds",
    )

    def __init__(
        self,
        *,
        capacity: SubordinateCallLimiter,
        timeout_seconds: float = DEFAULT_PROCESS_TIMEOUT_SECONDS,
        entry: Callable[[bytes], bytes] = worker_entry,
    ) -> None:
        """Build the pool; ``entry`` is the spawned worker function seam."""

        if type(capacity) is not SubordinateCallLimiter:
            raise TypeError("process pool capacity must use SubordinateCallLimiter")
        if type(timeout_seconds) is not float and type(timeout_seconds) is not int:
            raise TypeError("process timeout must be a second count")
        seconds = float(timeout_seconds)
        if not 0.0 < seconds <= MAX_PROCESS_TIMEOUT_SECONDS:
            raise SubordinatePoolError("process timeout is outside the bound")
        self._capacity = capacity
        self._timeout_seconds = seconds
        self._entry = entry
        self._context = multiprocessing.get_context("spawn")
        self._condition = Condition()
        self._active_processes: dict[int, multiprocessing.process.BaseProcess] = {}
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
        """Run one registered operation in a bounded spawn process.

        ``parent`` is the caller's exact scheduled-work triple: a
        subordinate permit can never replace or release it.
        """

        with self._condition:
            if self._closed:
                raise SubordinatePoolClosedError("process pool no longer admits operations")
            self._pending_submissions += 1
        permit: CapacityPermit | None = None
        try:
            request = encode_request(operation_id, 1, payload)
            permit = self._capacity.acquire(owner, parent=parent)
            return self._run_process(request, operation_id)
        finally:
            if permit is not None:
                self._capacity.release(permit)
            with self._condition:
                self._pending_submissions -= 1
                self._condition.notify_all()

    def close(self) -> None:
        """Stop admission and terminate every active worker within the captured bound."""
        deadline = monotonic() + self._timeout_seconds
        with self._condition:
            if self._closed and self._pending_submissions == 0 and not self._active_processes:
                return
            self._closed = True
            active = tuple(self._active_processes.values())
        unresolved: list[str] = []
        for process in active:
            try:
                self._terminate_and_join(process, graceful=False, deadline=deadline)
            except SubordinatePoolError as error:
                unresolved.append(str(error))
        with self._condition:
            while self._pending_submissions:
                remaining = deadline - monotonic()
                if remaining <= 0:
                    unresolved.append("process pool shutdown exceeded the captured timeout")
                    break
                self._condition.wait(remaining)
        if unresolved:
            raise SubordinatePoolError("; ".join(unresolved))

    def _run_process(self, request: bytes, operation_id: str) -> SubordinateResult:
        receiver, sender = self._context.Pipe(duplex=False)
        process: multiprocessing.process.BaseProcess = self._context.Process(
            target=_pool_worker_target,
            args=(self._entry, request, sender),
        )
        try:
            process.start()
        except Exception:
            receiver.close()
            sender.close()
            _close_process_handle(process)
            raise
        sender.close()
        with self._condition:
            self._active_processes[id(process)] = process
            closed = self._closed
        if closed:
            self._terminate_and_join(process, graceful=False)
        try:
            poll_ready: bool = receiver.poll(self._timeout_seconds)
            if not poll_ready:
                self._terminate_and_join(process, graceful=False)
                raise SubordinateWorkerTimeoutError("worker process exceeded its execution bound")
            response = receiver.recv_bytes(maxlength=MAX_PAYLOAD_BYTES)
        except EOFError, OSError:
            response = b""
        finally:
            receiver.close()
            self._terminate_and_join(process)
            with self._condition:
                self._active_processes.pop(id(process), None)
                self._condition.notify_all()
            _close_process_handle(process)
        if not response:
            raise SubordinateWorkerCrashError("worker process terminated without a response")
        try:
            responded_operation, result = decode_response(response)
        except SubordinateCodecError as error:
            raise SubordinateWorkerCrashError(
                f"worker response failed codec validation: {error.__class__.__name__}"
            ) from error
        if responded_operation != operation_id:
            raise SubordinateWorkerCrashError(
                "worker response operation does not match the submitted operation"
            )
        return SubordinateResult(operation_id=responded_operation, result=result)

    def _terminate_and_join(
        self,
        process: multiprocessing.process.BaseProcess,
        *,
        graceful: bool = True,
        deadline: float | None = None,
    ) -> None:
        stop_deadline = monotonic() + self._timeout_seconds if deadline is None else deadline
        try:
            if graceful:
                process.join(timeout=max(0.0, stop_deadline - monotonic()))
            if process.is_alive():
                process.terminate()
                process.join(timeout=max(0.0, stop_deadline - monotonic()))
            if process.is_alive():
                process.kill()
                process.join(timeout=max(0.0, stop_deadline - monotonic()))
            if process.is_alive():
                raise SubordinatePoolError("worker process survived the captured shutdown bound")
        except ValueError:
            # The submit owner may have completed its finally block and released
            # the Windows Process handle after close() took its active snapshot.
            return


def _close_process_handle(process: multiprocessing.process.BaseProcess) -> None:
    """Release an owned Process handle, tolerating an already-closed race."""

    try:
        process.close()
    except ValueError:
        return


def _pool_worker_target(
    entry: Callable[[bytes], bytes],
    request: bytes,
    sender: multiprocessing.connection.Connection,
) -> None:
    try:
        response = entry(request)
        sender.send_bytes(response)
    finally:
        sender.close()


__all__ = [
    "DEFAULT_PROCESS_TIMEOUT_SECONDS",
    "PROCESS_POOL_CATEGORY",
    "SubordinatePoolClosedError",
    "SubordinatePoolError",
    "SubordinateProcessPool",
    "SubordinateResult",
    "SubordinateWorkerCrashError",
    "SubordinateWorkerTimeoutError",
]

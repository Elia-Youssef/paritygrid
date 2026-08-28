"""Runtime ownership bridge for controls of live concurrent execution.

The HTTP boundary does not own runners.  This module is the narrow runtime
bridge that does: it registers an owner immediately before a real accepted
``ConcurrentRunEngine`` begins its parent loop, forwards lifecycle requests
to that engine, and unregisters the owner only after its execution pass has
reached a durable boundary.  The bridge never issues bare writer transitions.
"""

from __future__ import annotations

from collections.abc import Callable
from threading import Condition, Thread, current_thread
from time import monotonic

from paritygrid.application.execution.concurrent_engine import (
    ConcurrentEngineError,
    ConcurrentRunEngine,
    ConcurrentRunReport,
    EngineStatus,
)
from paritygrid.application.execution.concurrent_lifecycle import ConcurrentLifecycleReport
from paritygrid.application.ports.execution import RunRecord
from paritygrid.application.ports.run_control import (
    MAX_RUN_CONTROL_TIMEOUT_SECONDS,
    ActiveRunControlBusyError,
    ActiveRunControlEvidenceError,
    ActiveRunControlOwner,
    ActiveRunControlTimeoutError,
    RunControlEvidence,
)
from paritygrid.domain.execution import RunState
from paritygrid.domain.models import RunId
from paritygrid.runtime.run_controls import RuntimeActiveRunControlRegistry


class RuntimeExecutionOwnership:
    """Start real concurrent engines under runtime-owned control registration.

    A future execution launcher supplies an already composed accepted engine
    after it has durably moved the run to ``RUNNING``.  Registration happens
    before the engine thread starts; terminal completion retires the
    registration.  This deliberately leaves pipeline-to-engine construction
    in the execution composition that owns connector and work dependencies.
    """

    __slots__ = ("_active_run_controls", "_read_run")

    def __init__(
        self,
        *,
        active_run_controls: RuntimeActiveRunControlRegistry,
        read_run: Callable[[RunId], RunRecord],
    ) -> None:
        if type(active_run_controls) is not RuntimeActiveRunControlRegistry:
            raise TypeError("execution ownership must use the runtime control registry")
        if not callable(read_run):
            raise TypeError("execution ownership requires a durable run reader")
        self._active_run_controls = active_run_controls
        self._read_run = read_run

    def start_concurrent(self, engine: ConcurrentRunEngine, /) -> RuntimeConcurrentRunOwner:
        """Register and start one accepted concurrent parent engine.

        The durable state check prevents a registered owner for a queued,
        paused, terminal, or unrelated run.  It does not create an execution
        decision itself; the accepted launcher has already performed that
        transition and built the engine's work topology.
        """
        if type(engine) is not ConcurrentRunEngine:
            raise TypeError("runtime execution ownership requires ConcurrentRunEngine")
        run_id = engine.run_id
        run = self._read_exact(run_id)
        if run.state is not RunState.RUNNING:
            raise ActiveRunControlEvidenceError(
                "cannot register an execution owner for a non-running durable run"
            )
        owner = RuntimeConcurrentRunOwner(
            engine=engine,
            registry=self._active_run_controls,
            read_run=self._read_run,
        )
        owner.start()
        return owner

    def _read_exact(self, run_id: RunId) -> RunRecord:
        try:
            run = self._read_run(run_id)
        except Exception as error:
            raise ActiveRunControlEvidenceError(
                "could not read the durable run before execution registration"
            ) from error
        if type(run) is not RunRecord or run.run_id != run_id:
            raise ActiveRunControlEvidenceError(
                "durable execution registration evidence is invalid"
            )
        return run


class RuntimeConcurrentRunOwner(ActiveRunControlOwner):
    """One live ``ConcurrentRunEngine`` and its bounded HTTP control surface."""

    __slots__ = (
        "_closed",
        "_condition",
        "_engine",
        "_error",
        "_last_report",
        "_pause_proof",
        "_phase",
        "_read_run",
        "_registry",
        "_run_id",
        "_thread",
    )

    def __init__(
        self,
        *,
        engine: ConcurrentRunEngine,
        registry: RuntimeActiveRunControlRegistry,
        read_run: Callable[[RunId], RunRecord],
    ) -> None:
        if type(engine) is not ConcurrentRunEngine:
            raise TypeError("concurrent run owner requires ConcurrentRunEngine")
        if type(registry) is not RuntimeActiveRunControlRegistry:
            raise TypeError("concurrent run owner requires the runtime registry")
        if not callable(read_run):
            raise TypeError("concurrent run owner requires a durable run reader")
        self._engine = engine
        self._registry = registry
        self._read_run = read_run
        self._run_id = engine.run_id
        self._condition = Condition()
        self._thread: Thread | None = None
        self._phase = "new"
        self._last_report: ConcurrentRunReport | None = None
        self._pause_proof = None
        self._error: Exception | None = None
        self._closed = False

    @property
    def run_id(self) -> RunId:
        """Return the durable run identity exclusively owned by this adapter."""
        return self._run_id

    def start(self) -> None:
        """Register the real owner, then start its parent execution loop."""
        with self._condition:
            if self._closed:
                raise ActiveRunControlBusyError("closed execution ownership cannot start")
            if self._phase != "new":
                raise ActiveRunControlBusyError("execution ownership is already started")
            self._registry.register(self._run_id, self)
            self._phase = "running"
        try:
            self._start_pass()
        except Exception:
            with self._condition:
                self._phase = "closed"
                self._closed = True
                self._condition.notify_all()
            self._registry.unregister(self._run_id)
            raise

    def pause(
        self,
        *,
        correlation_id: str | None,
        timeout_seconds: float,
        converge_on_duplicate: bool,
    ) -> RunControlEvidence:
        """Ask the active engine to pause at its accepted stable boundary."""
        del converge_on_duplicate
        deadline = _deadline(timeout_seconds)
        with self._condition:
            self._require_phase("running")
            try:
                self._engine.request_pause(correlation_id=correlation_id)
            except ConcurrentEngineError as error:
                raise ActiveRunControlEvidenceError(
                    "accepted execution engine rejected the pause request"
                ) from error
            report = self._wait_for_pass(deadline)
            if report.status is not EngineStatus.PAUSED:
                raise ActiveRunControlEvidenceError(
                    "execution reached a non-pause boundary before pause completed"
                )
        return self._evidence("paused", RunState.PAUSED)

    def resume(
        self,
        *,
        correlation_id: str | None,
        timeout_seconds: float,
        converge_on_duplicate: bool,
    ) -> RunControlEvidence:
        """Durably resume a paused engine, then restart its parent loop."""
        del converge_on_duplicate
        deadline = _deadline(timeout_seconds)
        with self._condition:
            self._require_phase("paused")
            proof = self._pause_proof
            if proof is None:
                raise ActiveRunControlEvidenceError("paused engine lacks its durable pause proof")
            try:
                self._engine.resume(proof, correlation_id=correlation_id)
            except ConcurrentEngineError as error:
                raise ActiveRunControlEvidenceError(
                    "accepted execution engine rejected the resume request"
                ) from error
            _remaining(deadline)
            self._phase = "running"
            self._pause_proof = None
            self._last_report = None
            self._start_pass()
        return self._evidence("resumed", RunState.RUNNING)

    def cancel(
        self,
        *,
        correlation_id: str | None,
        timeout_seconds: float,
        converge_on_duplicate: bool,
    ) -> RunControlEvidence:
        """Ask the active engine to drain and durably cancel its own work."""
        del converge_on_duplicate
        deadline = _deadline(timeout_seconds)
        with self._condition:
            self._require_phase("running")
            try:
                self._engine.request_cancellation(correlation_id=correlation_id)
            except ConcurrentEngineError as error:
                raise ActiveRunControlEvidenceError(
                    "accepted execution engine rejected the cancellation request"
                ) from error
            report = self._wait_for_pass(deadline)
            if report.status is not EngineStatus.CANCELLED:
                raise ActiveRunControlEvidenceError(
                    "execution reached a non-cancellation boundary before cancel completed"
                )
        return self._evidence("cancelled", RunState.CANCELLED)

    def close(self, *, timeout_seconds: float) -> None:
        """Boundedly stop the owned engine without fabricating lifecycle state."""
        deadline = _deadline(timeout_seconds)
        with self._condition:
            if self._closed:
                return
            self._closed = True
            phase = self._phase
            if phase == "running":
                try:
                    self._engine.request_cancellation(correlation_id=None)
                except ConcurrentEngineError as error:
                    raise ActiveRunControlEvidenceError(
                        "accepted execution engine rejected shutdown cancellation"
                    ) from error
                self._wait_for_pass(deadline)
            elif phase == "paused":
                # A paused engine has released worker admission.  Its durable
                # pause evidence remains recoverable; shutdown must not forge
                # an unowned cancel transition.
                self._phase = "closed"
            elif phase == "new" or phase == "terminal":
                self._phase = "closed"
        thread = self._thread
        if thread is not None and thread is not current_thread():
            thread.join(timeout=_remaining(deadline))
            if thread.is_alive():
                raise ActiveRunControlTimeoutError("execution owner did not stop within its budget")
        try:
            self._engine.cleanup()
        except ConcurrentEngineError as error:
            raise ActiveRunControlEvidenceError("execution owner cleanup failed") from error

    def _start_pass(self) -> None:
        thread = Thread(
            target=self._run_pass,
            name=f"paritygrid-run-{self._run_id.value}",
            daemon=False,
        )
        self._thread = thread
        thread.start()

    def _run_pass(self) -> None:
        try:
            report = self._engine.run()
        except Exception as error:
            with self._condition:
                self._error = error
                self._phase = "terminal"
                self._condition.notify_all()
            self._retire_terminal_owner()
            return
        with self._condition:
            self._last_report = report
            if report.status is EngineStatus.PAUSED:
                self._phase = "paused"
                self._pause_proof = report.pause_proof
            else:
                self._phase = "terminal"
            self._condition.notify_all()
        if report.status is not EngineStatus.PAUSED:
            self._retire_terminal_owner()

    def _retire_terminal_owner(self) -> None:
        """Retire on a separate thread so cancellation dispatch cannot deadlock."""
        Thread(
            target=self._unregister_terminal_owner,
            name=f"paritygrid-retire-{self._run_id.value}",
            daemon=True,
        ).start()

    def _unregister_terminal_owner(self) -> None:
        try:
            self._registry.unregister(self._run_id)
        except Exception:
            # The registry already fails closed and runtime shutdown retries
            # every closer.  A background reaper must never hide the real
            # engine result or escape as an unobserved thread exception.
            return

    def _wait_for_pass(self, deadline: float) -> ConcurrentRunReport:
        while self._phase == "running":
            self._condition.wait(timeout=_remaining(deadline))
        if self._error is not None:
            raise ActiveRunControlEvidenceError("active execution engine failed") from self._error
        report = self._last_report
        if report is None:
            raise ActiveRunControlEvidenceError("active execution returned no boundary report")
        return report

    def _evidence(self, action: str, state: RunState) -> RunControlEvidence:
        report = self._engine.last_lifecycle_report
        if (
            type(report) is not ConcurrentLifecycleReport
            or report.action != action
            or not report.submission_ids
        ):
            raise ActiveRunControlEvidenceError(
                "accepted execution returned no durable lifecycle receipt"
            )
        try:
            run = self._read_run(self._run_id)
        except Exception as error:
            raise ActiveRunControlEvidenceError(
                "could not re-read the durable lifecycle outcome"
            ) from error
        if type(run) is not RunRecord or run.run_id != self._run_id or run.state is not state:
            raise ActiveRunControlEvidenceError(
                "accepted execution lifecycle evidence disagrees with durable state"
            )
        return RunControlEvidence(run, report.submission_ids)

    def _require_phase(self, expected: str) -> None:
        if self._closed:
            raise ActiveRunControlBusyError("active execution ownership is closing")
        if self._phase != expected:
            raise ActiveRunControlBusyError(
                f"active execution is at {self._phase!r}, not {expected!r}"
            )


def _deadline(timeout_seconds: object) -> float:
    if (
        type(timeout_seconds) is not float
        or not 0.0 < timeout_seconds <= MAX_RUN_CONTROL_TIMEOUT_SECONDS
    ):
        raise ValueError("active execution ownership timeout is outside the supported range")
    return monotonic() + timeout_seconds


def _remaining(deadline: float) -> float:
    remaining = deadline - monotonic()
    if remaining <= 0.0:
        raise ActiveRunControlTimeoutError("active execution control exceeded its time budget")
    return remaining

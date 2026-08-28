"""Runtime ownership registry bounds and serialization tests."""

from __future__ import annotations

from pathlib import Path
from threading import Event, Thread
from time import sleep

import pytest

from paritygrid.application.ports.run_control import (
    ActiveRunControlBusyError,
    ActiveRunControlClosedError,
    ActiveRunControlNotFoundError,
    ActiveRunControlTimeoutError,
    RunControlAction,
    RunControlEvidence,
)
from paritygrid.domain.models import RunId
from paritygrid.runtime.composition import compose_runtime
from paritygrid.runtime.config import Settings
from paritygrid.runtime.run_controls import RuntimeActiveRunControlRegistry


class _BlockingOwner:
    """Owner whose control call occupies its slot until the test releases it."""

    def __init__(self) -> None:
        self.started = Event()
        self.release = Event()
        self.close_timeout: float | None = None

    def pause(
        self,
        *,
        correlation_id: str | None,
        timeout_seconds: float,
        converge_on_duplicate: bool,
    ) -> RunControlEvidence:
        del correlation_id, timeout_seconds, converge_on_duplicate
        self.started.set()
        if not self.release.wait(timeout=1.0):  # pragma: no cover - test timeout guard
            raise TimeoutError("test owner was not released")
        raise ActiveRunControlTimeoutError("test owner completed without a durable outcome")

    def resume(
        self,
        *,
        correlation_id: str | None,
        timeout_seconds: float,
        converge_on_duplicate: bool,
    ) -> RunControlEvidence:
        del correlation_id, timeout_seconds, converge_on_duplicate
        raise ActiveRunControlTimeoutError("resume is not used by this test owner")

    def cancel(
        self,
        *,
        correlation_id: str | None,
        timeout_seconds: float,
        converge_on_duplicate: bool,
    ) -> RunControlEvidence:
        del correlation_id, timeout_seconds, converge_on_duplicate
        raise ActiveRunControlTimeoutError("cancel is not used by this test owner")

    def close(self, *, timeout_seconds: float) -> None:
        self.close_timeout = timeout_seconds


class _LateCleanupOwner(_BlockingOwner):
    """A deliberately non-conforming owner used to prove registry fail-closed removal."""

    def close(self, *, timeout_seconds: float) -> None:
        self.close_timeout = timeout_seconds
        sleep(0.02)


def test_registry_serializes_control_waits_and_performs_bounded_cleanup() -> None:
    registry = RuntimeActiveRunControlRegistry(capacity=1)
    owner = _BlockingOwner()
    run_id = RunId("run_control-001")
    registry.register(run_id, owner)
    worker_errors: list[BaseException] = []

    def invoke_first_control() -> None:
        try:
            registry.dispatch(
                run_id,
                action=RunControlAction.PAUSE,
                correlation_id="corr-control-001",
                timeout_seconds=1.0,
                converge_on_duplicate=False,
            )
        except BaseException as error:
            worker_errors.append(error)

    worker = Thread(target=invoke_first_control)
    worker.start()
    assert owner.started.wait(timeout=1.0)
    with pytest.raises(ActiveRunControlBusyError):
        registry.dispatch(
            run_id,
            action=RunControlAction.PAUSE,
            correlation_id="corr-control-002",
            timeout_seconds=0.01,
            converge_on_duplicate=False,
        )
    owner.release.set()
    worker.join(timeout=1.0)
    assert not worker.is_alive()
    assert len(worker_errors) == 1
    assert isinstance(worker_errors[0], ActiveRunControlTimeoutError)

    registry.unregister(run_id, timeout_seconds=0.1)
    assert registry.active_count == 0
    assert owner.close_timeout is not None
    assert 0.0 < owner.close_timeout <= 0.1


def test_cleanup_overrun_removes_the_owner_and_fails_closed() -> None:
    registry = RuntimeActiveRunControlRegistry(capacity=1)
    owner = _LateCleanupOwner()
    run_id = RunId("run_control-002")
    registry.register(run_id, owner)

    with pytest.raises(ActiveRunControlTimeoutError):
        registry.unregister(run_id, timeout_seconds=0.01)

    assert registry.active_count == 0


def test_composed_runtime_releases_active_owners_before_its_writer(tmp_path: Path) -> None:
    runtime = compose_runtime(Settings(data_root=tmp_path / "runtime-data"))
    owner = _BlockingOwner()
    runtime.active_run_controls.register(RunId("run_control-003"), owner)

    runtime.shutdown()

    assert owner.close_timeout is not None
    assert runtime.active_run_controls.active_count == 0


@pytest.mark.parametrize("operation", ["unregister", "close"])
def test_dispatch_captured_before_owner_retirement_fails_closed(operation: str) -> None:
    registry = RuntimeActiveRunControlRegistry(capacity=1)
    owner = _BlockingOwner()
    run_id = RunId("run_control-004")
    registry.register(run_id, owner)
    slot = registry._controls[run_id]  # pyright: ignore[reportPrivateUsage]
    slot.gate.acquire()
    dispatch_errors: list[BaseException] = []

    def dispatch() -> None:
        try:
            registry.dispatch(
                run_id,
                action=RunControlAction.PAUSE,
                correlation_id="corr-control-retired",
                timeout_seconds=1.0,
                converge_on_duplicate=False,
            )
        except BaseException as error:
            dispatch_errors.append(error)

    dispatcher = Thread(target=dispatch)
    dispatcher.start()
    sleep(0.02)

    retirement_errors: list[BaseException] = []

    def retire() -> None:
        try:
            if operation == "unregister":
                registry.unregister(run_id, timeout_seconds=1.0)
            else:
                registry.close(timeout_seconds=1.0)
        except BaseException as error:
            retirement_errors.append(error)

    retiree = Thread(target=retire)
    retiree.start()
    for _attempt in range(100):
        if registry.active_count == 0:
            break
        sleep(0.005)
    assert registry.active_count == 0
    slot.gate.release()
    dispatcher.join(timeout=1.0)
    retiree.join(timeout=1.0)

    assert not dispatcher.is_alive()
    assert not retiree.is_alive()
    assert not retirement_errors
    assert len(dispatch_errors) == 1
    expected = (
        ActiveRunControlNotFoundError if operation == "unregister" else ActiveRunControlClosedError
    )
    assert isinstance(dispatch_errors[0], expected)
    assert not owner.started.is_set()
    assert owner.close_timeout is not None

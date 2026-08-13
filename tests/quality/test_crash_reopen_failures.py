"""Adversarial failure seams for bounded crash-reopen behavior."""

# pyright: reportArgumentType=false, reportAttributeAccessIssue=false
# pyright: reportPrivateUsage=false, reportUnknownArgumentType=false
# pyright: reportUnknownLambdaType=false

from __future__ import annotations

import io
import json
import os
import queue
import runpy
import subprocess
from pathlib import Path
from threading import Event, Thread
from types import SimpleNamespace
from typing import BinaryIO, cast
from unittest.mock import MagicMock

import pytest
from sqlalchemy.orm import Session

from paritygrid.application.ports.writer import CommittedNotification
from paritygrid.quality import crash_reopen_harness as harness
from paritygrid.quality import crash_reopen_protocol as protocol
from paritygrid.quality import crash_reopen_scenario as scenario
from paritygrid.quality import crash_reopen_worker as worker
from paritygrid.quality.crash_reopen_harness import (
    CrashReopenConfig,
    CrashReopenHarnessError,
    CrashReopenResult,
)
from paritygrid.quality.crash_reopen_protocol import (
    CrashControlEnvelope,
    CrashFailpoint,
    CrashMarker,
    CrashMarkerEmitter,
    CrashMarkerRecord,
    CrashReopenProtocolError,
    invocation_token,
)
from paritygrid.quality.crash_reopen_scenario import (
    CrashDatabaseIntegrityError,
    CrashDatabaseOutcome,
    expected_projection,
)
from paritygrid.quality.crash_reopen_worker import CrashReopenWorkerError


def envelope(
    directory: Path, failpoint: CrashFailpoint = CrashFailpoint.NORMAL
) -> CrashControlEnvelope:
    ordinal = 2 if failpoint is CrashFailpoint.SHUTDOWN_DRAIN else 1
    return CrashControlEnvelope(
        failpoint,
        ordinal,
        directory / "scenario.sqlite3",
        directory / "markers-v1.jsonl",
        1,
        15.0,
        invocation_token(failpoint, ordinal, 1),
    )


def marker(value: CrashControlEnvelope, sequence: int = 1) -> CrashMarkerRecord:
    return CrashMarkerRecord(
        value.invocation_token,
        value.case_id,
        sequence,
        0,
        CrashMarker.WORKER_READY,
    )


def test_protocol_private_validators_and_operating_system_failures(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    with pytest.raises(CrashReopenProtocolError):
        protocol._canonical_bytes({"x": "y" * 512}, maximum_bytes=10)
    for value in (None, "", "Cafe\u0301", "bad\n"):
        with pytest.raises(CrashReopenProtocolError):
            protocol._require_nfc_text(value, "text")
    for value in (None, "", "bad\n"):
        with pytest.raises(CrashReopenProtocolError):
            protocol._require_path_text(value, "path")
    with pytest.raises(CrashReopenProtocolError, match="absolute"):
        protocol._require_absolute_path("relative", "path")
    monkeypatch.setattr(Path, "is_symlink", lambda _self: True)
    with pytest.raises(CrashReopenProtocolError, match="linked"):
        protocol._require_absolute_path(str(tmp_path / "x"), "path")
    monkeypatch.setattr(Path, "is_symlink", lambda _self: (_ for _ in ()).throw(OSError()))
    with pytest.raises(CrashReopenProtocolError, match="inspected"):
        protocol._require_absolute_path(str(tmp_path / "x"), "path")


def test_protocol_integer_timeout_load_and_short_write_failures(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    value = envelope(tmp_path)
    decoded = json.loads(value.to_bytes())
    decoded["hold_timeout_seconds"] = 15
    assert (
        CrashControlEnvelope.from_bytes((json.dumps(decoded) + "\n").encode()).hold_timeout_seconds
        == 15.0
    )
    control = tmp_path / "control.json"
    control.write_bytes(value.to_bytes())
    escaped = {**decoded, "database_path": str(tmp_path.parent / "elsewhere.sqlite3")}
    control.write_bytes((json.dumps(escaped) + "\n").encode())
    with pytest.raises(CrashReopenProtocolError, match="control directory"):
        protocol.load_control(control)
    monkeypatch.setattr(Path, "read_bytes", lambda _self: (_ for _ in ()).throw(OSError()))
    with pytest.raises(CrashReopenProtocolError, match="could not be read"):
        protocol.load_control(control)
    monkeypatch.undo()
    emitter = CrashMarkerEmitter(value, io.BytesIO())
    monkeypatch.setattr(os, "write", lambda *_args: 0)
    with pytest.raises(CrashReopenProtocolError, match="incomplete"):
        emitter.emit(CrashMarker.WORKER_READY, 0)


def test_marker_direct_type_validation(tmp_path: Path) -> None:
    value = envelope(tmp_path)
    with pytest.raises(CrashReopenProtocolError):
        CrashMarkerRecord(value.invocation_token, value.case_id, 1, 0, "ready")  # type: ignore[arg-type]


def instrumentation(
    tmp_path: Path,
    failpoint: CrashFailpoint = CrashFailpoint.NORMAL,
    stdin: io.BytesIO | None = None,
) -> worker._WorkerInstrumentation:
    value = envelope(tmp_path, failpoint)
    return worker._WorkerInstrumentation(
        value,
        CrashMarkerEmitter(value, io.BytesIO()),
        io.BytesIO() if stdin is None else stdin,
    )


def test_worker_control_reader_and_identity_guards(tmp_path: Path) -> None:
    ambiguous = envelope(tmp_path, CrashFailpoint.COMMIT_AMBIGUOUS)
    invalid = worker._WorkerInstrumentation(
        ambiguous, CrashMarkerEmitter(ambiguous, io.BytesIO()), io.BytesIO(b"{}\n")
    )
    invalid.start_control_reader()
    assert invalid._control_thread is not None
    invalid._control_thread.join(1)
    assert not invalid.release_commit.is_set()
    valid = worker._WorkerInstrumentation(
        ambiguous,
        CrashMarkerEmitter(ambiguous, io.BytesIO()),
        io.BytesIO(protocol.release_commit_bytes(ambiguous.invocation_token)),
    )
    valid.start_control_reader()
    assert valid._control_thread is not None
    valid._control_thread.join(1)
    assert valid.release_commit.is_set()
    normal = instrumentation(tmp_path)
    normal.start_control_reader()
    assert normal._control_thread is None
    with pytest.raises(CrashReopenWorkerError, match="identity"):
        normal.current_ordinal()
    with pytest.raises(CrashReopenWorkerError, match="Session identity"):
        normal._session_ordinal(Session())


def test_worker_instrumentation_defensive_and_all_failpoint_paths(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    normal = instrumentation(tmp_path)
    nested = SimpleNamespace(parent=object())
    normal.after_begin(Session(), cast(object, nested), cast(object, None))
    normal._next_ordinal = 4
    with pytest.raises(CrashReopenWorkerError, match="count"):
        normal.after_begin(
            Session(), cast(object, SimpleNamespace(parent=None)), cast(object, None)
        )
    normal = instrumentation(tmp_path)
    normal.admission_gates[1] = MagicMock(wait=MagicMock(return_value=False))
    with pytest.raises(CrashReopenWorkerError, match="release"):
        normal.after_begin(
            Session(), cast(object, SimpleNamespace(parent=None)), cast(object, None)
        )

    for failpoint, action in (
        (CrashFailpoint.BEFORE_COMMIT, "before_commit"),
        (CrashFailpoint.SHUTDOWN_DRAIN, "before_commit"),
        (CrashFailpoint.AFTER_COMMIT_BEFORE_RECEIPT, "session_close_entered"),
        (CrashFailpoint.AFTER_RECEIPT_BEFORE_NOTIFICATION, "hold_after_receipt"),
    ):
        value = instrumentation(tmp_path, failpoint)
        calls: list[bool] = []

        def record_hold(target: list[bool] = calls) -> None:
            target.append(True)

        monkeypatch.setattr(value, "_hold_forever", record_hold)
        session = Session()
        session.info["crash_command_ordinal"] = (
            2 if failpoint is CrashFailpoint.SHUTDOWN_DRAIN else 1
        )
        getattr(value, action)(
            session if action == "before_commit" else int(session.info["crash_command_ordinal"])
        )
        assert calls == [True]

    ambiguous = instrumentation(tmp_path, CrashFailpoint.COMMIT_AMBIGUOUS)
    ambiguous._thread_state.ordinal = 1
    ambiguous.release_commit = MagicMock(wait=MagicMock(return_value=False))
    with pytest.raises(CrashReopenWorkerError, match="commit release"):
        ambiguous.commit_entered(cast(object, None))
    normal = instrumentation(tmp_path)
    monkeypatch.setattr(Event, "wait", lambda _self, _timeout: False)
    with pytest.raises(CrashReopenWorkerError, match="boundary"):
        normal.hold_for_parent()


def test_instrumented_session_without_command_identity_closes_normally() -> None:
    session = worker._InstrumentedSession()
    worker._InstrumentedSession.instrumentation = None
    session.close()


def test_worker_hold_success_branch_and_module_entrypoint(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    value = instrumentation(tmp_path)
    monkeypatch.setattr(Event, "wait", lambda _self, _timeout: True)
    assert value.hold_for_parent() is None
    with pytest.warns(RuntimeWarning), pytest.raises(SystemExit) as captured:
        runpy.run_module("paritygrid.quality.crash_reopen_worker", run_name="__main__")
    assert captured.value.code == 2


def test_worker_shutdown_path_is_unit_exercised_without_process_kill(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    value = envelope(tmp_path, CrashFailpoint.SHUTDOWN_DRAIN)
    scenario.prepare_crash_database(value.database_path, value.seed)
    control = tmp_path / "control-v1.json"
    control.write_bytes(value.to_bytes())
    monkeypatch.setattr(worker._WorkerInstrumentation, "_hold_forever", lambda _self: None)
    assert worker.run_worker(control, stdin=io.BytesIO(), stdout=io.BytesIO()) == 0


def test_worker_shutdown_requires_the_second_command_to_reach_precommit(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    value = envelope(tmp_path, CrashFailpoint.SHUTDOWN_DRAIN)
    scenario.prepare_crash_database(value.database_path, value.seed)
    control = tmp_path / "control-v1.json"
    control.write_bytes(value.to_bytes())
    original_init = worker._WorkerInstrumentation.__init__

    def replace_boundary(
        self: worker._WorkerInstrumentation,
        active_envelope: CrashControlEnvelope,
        emitter: CrashMarkerEmitter,
        stdin: BinaryIO,
    ) -> None:
        original_init(self, active_envelope, emitter, stdin)
        self.precommit_boundaries[2] = MagicMock(wait=MagicMock(return_value=False))

    monkeypatch.setattr(worker._WorkerInstrumentation, "__init__", replace_boundary)
    monkeypatch.setattr(worker._WorkerInstrumentation, "_hold_forever", lambda _self: None)
    with pytest.raises(CrashReopenWorkerError, match="in-flight"):
        worker.run_worker(control, stdin=io.BytesIO(), stdout=io.BytesIO())


def test_worker_notification_timeout_and_shutdown_close_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    value = CrashControlEnvelope(
        CrashFailpoint.NORMAL,
        1,
        tmp_path / "scenario.sqlite3",
        tmp_path / "markers-v1.jsonl",
        1,
        1.0,
        invocation_token(CrashFailpoint.NORMAL, 1, 1),
    )
    scenario.prepare_crash_database(value.database_path, value.seed)
    control = tmp_path / "control-v1.json"
    control.write_bytes(value.to_bytes())

    def no_signal(
        self: worker._InstrumentedNotifications,
        notification: CommittedNotification,
    ) -> bool:
        return super(worker._InstrumentedNotifications, self).offer(notification)

    monkeypatch.setattr(worker._InstrumentedNotifications, "offer", no_signal)
    with pytest.raises(CrashReopenWorkerError, match="notification"):
        worker.run_worker(control, stdin=io.BytesIO(), stdout=io.BytesIO())


def test_worker_rejects_an_undrained_normal_close(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    value = envelope(tmp_path)
    scenario.prepare_crash_database(value.database_path, value.seed)
    control = tmp_path / "control-v1.json"
    control.write_bytes(value.to_bytes())
    original_close = worker.SQLiteTransactionalWriter.close

    def report_undrained(
        self: worker.SQLiteTransactionalWriter, *, timeout_seconds: float
    ) -> SimpleNamespace:
        original_close(self, timeout_seconds=timeout_seconds)
        return SimpleNamespace(drained=False)

    monkeypatch.setattr(worker.SQLiteTransactionalWriter, "close", report_undrained)
    with pytest.raises(CrashReopenWorkerError, match="shutdown"):
        worker.run_worker(control, stdin=io.BytesIO(), stdout=io.BytesIO())


@pytest.mark.parametrize(
    "values",
    [
        {"case_directory": Path("relative")},
        {"failpoint": "normal"},
        {"seed": True},
        {"startup_timeout_seconds": 0.0},
        {"boundary_timeout_seconds": 16.0},
    ],
)
def test_harness_config_rejects_invalid_values(tmp_path: Path, values: dict[str, object]) -> None:
    arguments: dict[str, object] = {
        "case_directory": tmp_path / "new",
        "failpoint": CrashFailpoint.NORMAL,
    }
    arguments.update(values)
    with pytest.raises(CrashReopenHarnessError):
        CrashReopenConfig(**arguments)  # type: ignore[arg-type]
    existing = tmp_path / "existing"
    existing.mkdir(exist_ok=True)
    with pytest.raises(CrashReopenHarnessError):
        CrashReopenConfig(existing, CrashFailpoint.NORMAL)


def test_harness_result_rejects_inconsistent_values(tmp_path: Path) -> None:
    value = envelope(tmp_path)
    ready = (marker(value),)
    valid = {
        "failpoint": CrashFailpoint.NORMAL,
        "observed_outcome": CrashDatabaseOutcome.COMMITTED,
        "final_outcome": CrashDatabaseOutcome.COMMITTED,
        "retried": False,
        "marker_prefix": ready,
        "process_return_code": 0,
    }
    for replacement in (
        {"failpoint": "normal"},
        {"observed_outcome": "committed"},
        {"final_outcome": "committed"},
        {"retried": 1},
        {"marker_prefix": ()},
        {"marker_prefix": ("bad",)},
        {"final_outcome": CrashDatabaseOutcome.ABSENT},
        {"retried": True},
    ):
        arguments = {**valid, **replacement}
        with pytest.raises(CrashReopenHarnessError):
            CrashReopenResult(**arguments)  # type: ignore[arg-type]


def test_harness_file_and_path_failure_seams(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = tmp_path / "control"
    harness._write_durable_new(path, b"value")
    with pytest.raises(CrashReopenHarnessError, match="already"):
        harness._write_durable_new(path, b"value")
    other = tmp_path / "other"
    original = os.write
    monkeypatch.setattr(os, "write", lambda *_args: 0)
    with pytest.raises(CrashReopenHarnessError, match="incomplete"):
        harness._write_durable_new(other, b"value")
    monkeypatch.setattr(os, "write", original)
    monkeypatch.setattr(Path, "resolve", lambda _self, **_kwargs: (_ for _ in ()).throw(OSError()))
    with pytest.raises(CrashReopenHarnessError, match="inspected"):
        harness._validate_new_case_directory(tmp_path / "new")


def test_harness_missing_and_linked_parent_rejection(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    with pytest.raises(CrashReopenHarnessError, match="parent"):
        CrashReopenConfig(tmp_path / "missing" / "case", CrashFailpoint.NORMAL)
    monkeypatch.setattr(Path, "is_symlink", lambda _self: True)
    with pytest.raises(CrashReopenHarnessError, match="linked"):
        CrashReopenConfig(tmp_path / "case", CrashFailpoint.NORMAL)


class FakeStream(io.BytesIO):
    def flush(self) -> None:
        return None


class FakeProcess:
    def __init__(self) -> None:
        self.stdin = FakeStream()
        self.stdout = FakeStream()
        self.stderr = FakeStream()
        self.return_code: int | None = None
        self.kill_error: BaseException | None = None
        self.wait_error: BaseException | None = None

    def poll(self) -> int | None:
        return self.return_code

    def kill(self) -> None:
        if self.kill_error is not None:
            raise self.kill_error
        self.return_code = -9

    def wait(self, timeout: float) -> int:
        del timeout
        if self.wait_error is not None:
            raise self.wait_error
        return 0 if self.return_code is None else self.return_code


def test_harness_process_pipe_and_reader_failure_seams(tmp_path: Path) -> None:
    process = FakeProcess()
    assert harness._stop_process(cast(object, process), 1.0) == -9
    process.kill_error = OSError()
    process.return_code = None
    with pytest.raises(CrashReopenHarnessError, match="stopped"):
        harness._stop_process(cast(object, process), 1.0)
    process.kill_error = None
    process.wait_error = subprocess.TimeoutExpired("worker", 1)
    with pytest.raises(CrashReopenHarnessError, match="termination"):
        harness._stop_process(cast(object, process), 1.0)
    with pytest.raises(CrashReopenHarnessError, match="exit timed out"):
        harness._wait_for_exit(cast(object, process), 1.0)
    process.stderr = None
    assert harness._read_stderr(cast(object, process)) == b""
    process.stderr = FakeStream(b"x" * (protocol.MAX_PROTOCOL_LINE_BYTES + 1))
    with pytest.raises(CrashReopenHarnessError, match="diagnostic"):
        harness._read_stderr(cast(object, process))
    process.stdin = None
    harness._close_pipes(cast(object, process))

    alive = MagicMock()
    alive.join.return_value = None
    alive.is_alive.return_value = True
    child = harness._ChildProcess(cast(object, process), queue.Queue(), alive)
    with pytest.raises(CrashReopenHarnessError, match="reader"):
        harness._finish_reader(child, 1.0)


def test_spawn_and_release_failures_are_redacted(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(
        subprocess,
        "Popen",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(OSError()),
    )
    with pytest.raises(CrashReopenHarnessError, match="started"):
        harness._spawn_worker(tmp_path / "control", tmp_path)
    value = envelope(tmp_path, CrashFailpoint.COMMIT_AMBIGUOUS)
    process = FakeProcess()
    process.stdin.close()
    child = harness._ChildProcess(cast(object, process), queue.Queue(), MagicMock())
    with pytest.raises(CrashReopenHarnessError, match="delivered"):
        harness._release_ambiguous_commit(child, value)


def test_spawn_thread_start_failure_stops_child(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    process = FakeProcess()
    monkeypatch.setattr(subprocess, "Popen", lambda *_args, **_kwargs: process)

    class FailedThread:
        def __init__(self, **_kwargs: object) -> None:
            return None

        def start(self) -> None:
            raise RuntimeError("start failed")

    monkeypatch.setattr(harness, "Thread", FailedThread)
    with pytest.raises(RuntimeError, match="start failed"):
        harness._spawn_worker(tmp_path / "control", tmp_path)
    assert process.return_code == -9


def test_wait_marker_and_ledger_fail_closed_matrix(tmp_path: Path) -> None:
    value = envelope(tmp_path)
    fake = FakeProcess()
    reader = Thread(target=lambda: None)
    lines: queue.Queue[bytes | None] = queue.Queue()
    child = harness._ChildProcess(cast(object, fake), lines, reader)
    lines.put(None)
    with pytest.raises(CrashReopenHarnessError, match="exited"):
        harness._wait_for_marker(child, value, CrashMarker.WORKER_READY, 0, 1.0, [], [])
    lines.put(b"bad\n")
    with pytest.raises(CrashReopenHarnessError, match="invalid marker"):
        harness._wait_for_marker(child, value, CrashMarker.WORKER_READY, 0, 1.0, [], [])
    wrong = marker(value, 2).to_bytes()
    lines.put(wrong)
    with pytest.raises(CrashReopenHarnessError, match="sequence"):
        harness._wait_for_marker(child, value, CrashMarker.WORKER_READY, 0, 1.0, [], [])

    with pytest.raises(CrashReopenHarnessError, match="could not be read"):
        harness._validate_ledger(value, [])
    value.marker_path.write_bytes(b"bad\n")
    with pytest.raises(CrashReopenHarnessError, match="invalid"):
        harness._validate_ledger(value, [])
    value.marker_path.write_bytes(marker(value, 2).to_bytes())
    with pytest.raises(CrashReopenHarnessError, match="identity"):
        harness._validate_ledger(value, [])
    value.marker_path.write_bytes(marker(value).to_bytes())
    with pytest.raises(CrashReopenHarnessError, match="prefix"):
        harness._validate_ledger(value, [b"different\n"])


def test_wait_marker_timeout_empty_and_oversize_ledger(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    value = envelope(tmp_path)
    fake = FakeProcess()
    child = harness._ChildProcess(cast(object, fake), queue.Queue(), MagicMock())
    monkeypatch.setattr(harness.time, "monotonic", MagicMock(side_effect=(1.0, 2.0)))
    with pytest.raises(CrashReopenHarnessError, match="timed out"):
        harness._wait_for_marker(child, value, CrashMarker.WORKER_READY, 0, 0.5, [], [])
    monkeypatch.undo()
    with pytest.raises(CrashReopenHarnessError, match="timed out"):
        harness._wait_for_marker(child, value, CrashMarker.WORKER_READY, 0, 0.01, [], [])
    value.marker_path.write_bytes(b"")
    with pytest.raises(CrashReopenHarnessError, match="invalid"):
        harness._validate_ledger(value, [])
    value.marker_path.write_bytes(b"x" * (protocol.MAX_PROTOCOL_LINE_BYTES + 1) + b"\n")
    with pytest.raises(CrashReopenHarnessError, match="invalid"):
        harness._validate_ledger(value, [])


def test_cleanup_nonexistent_linked_and_operating_system_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    harness._cleanup_case(tmp_path / "missing")
    case = tmp_path / "case"
    case.mkdir()
    monkeypatch.setattr(Path, "is_symlink", lambda _self: True)
    with pytest.raises(CrashReopenHarnessError, match="linked"):
        harness._cleanup_case(case)
    monkeypatch.undo()
    monkeypatch.setattr(
        harness.shutil,
        "rmtree",
        lambda _path: (_ for _ in ()).throw(OSError()),
    )
    with pytest.raises(CrashReopenHarnessError, match="cleanup"):
        harness._cleanup_case(case)


def test_public_harness_rejects_wrong_type_and_cleans_preparation_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    with pytest.raises(CrashReopenHarnessError, match="configuration"):
        harness.run_crash_reopen_case("bad")  # type: ignore[arg-type]
    case = tmp_path / "prepare-failure"
    config = CrashReopenConfig(case, CrashFailpoint.NORMAL)
    monkeypatch.setattr(
        harness,
        "prepare_crash_database",
        lambda *_args: (_ for _ in ()).throw(CrashReopenHarnessError("expected")),
    )
    with pytest.raises(CrashReopenHarnessError, match="expected"):
        harness.run_crash_reopen_case(config)
    assert not case.exists()


def test_public_harness_preserves_active_error_when_cleanup_also_fails(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    case = tmp_path / "cleanup-failure"
    config = CrashReopenConfig(case, CrashFailpoint.NORMAL)
    monkeypatch.setattr(
        harness,
        "_cleanup_case",
        lambda _path: (_ for _ in ()).throw(OSError()),
    )
    monkeypatch.setattr(
        harness,
        "prepare_crash_database",
        lambda *_args: (_ for _ in ()).throw(CrashReopenHarnessError("active")),
    )
    with pytest.raises(CrashReopenHarnessError, match="active"):
        harness.run_crash_reopen_case(config)


def test_public_harness_failure_classification_and_cleanup_seams(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    normal_case = tmp_path / "normal-diagnostic"
    normal = CrashReopenConfig(normal_case, CrashFailpoint.NORMAL)
    monkeypatch.setattr(harness, "_read_stderr", lambda _process: b"failure")
    with pytest.raises(CrashReopenHarnessError, match="normal control"):
        harness.run_crash_reopen_case(normal)
    monkeypatch.undo()

    outcome_case = tmp_path / "unexpected-outcome"
    after_commit = CrashReopenConfig(outcome_case, CrashFailpoint.AFTER_COMMIT_BEFORE_RECEIPT)
    monkeypatch.setattr(
        harness,
        "classify_crash_database",
        lambda *_args: (CrashDatabaseOutcome.ABSENT, expected_projection(8675309, False)),
    )
    with pytest.raises(CrashReopenHarnessError, match="violated"):
        harness.run_crash_reopen_case(after_commit)
    monkeypatch.undo()

    active_case = tmp_path / "active-cleanup"
    active = CrashReopenConfig(active_case, CrashFailpoint.NORMAL)
    monkeypatch.setattr(
        harness,
        "_validate_ledger",
        lambda *_args: (_ for _ in ()).throw(CrashReopenHarnessError("ledger")),
    )
    monkeypatch.setattr(
        harness,
        "_stop_process",
        lambda *_args: (_ for _ in ()).throw(CrashReopenHarnessError("cleanup")),
    )
    with pytest.raises(CrashReopenHarnessError, match="ledger"):
        harness.run_crash_reopen_case(active)
    monkeypatch.undo()

    cleanup_case = tmp_path / "cleanup-only"
    cleanup = CrashReopenConfig(cleanup_case, CrashFailpoint.NORMAL)
    monkeypatch.setattr(
        harness,
        "_cleanup_case",
        lambda _path: (_ for _ in ()).throw(CrashReopenHarnessError("cleanup")),
    )
    with pytest.raises(CrashReopenHarnessError, match="cleanup"):
        harness.run_crash_reopen_case(cleanup)
    monkeypatch.undo()
    if cleanup_case.exists():
        harness._cleanup_case(cleanup_case)


def test_scenario_input_and_projection_guards(tmp_path: Path) -> None:
    with pytest.raises(TypeError):
        scenario.prepare_crash_database(Path("relative.sqlite3"), 1)
    existing = tmp_path / "existing.sqlite3"
    existing.write_bytes(b"")
    with pytest.raises(CrashDatabaseIntegrityError, match="new"):
        scenario.prepare_crash_database(existing, 1)
    assert expected_projection(1, False) != expected_projection(1, True)


class FakeScalarResult:
    def __init__(self, scalar: object = None, rows: list[object] | None = None) -> None:
        self.scalar = scalar
        self.rows = [] if rows is None else rows

    def scalar_one(self) -> object:
        return self.scalar

    def all(self) -> list[object]:
        return self.rows


class FakeValidationConnection:
    def __init__(self, overrides: dict[str, FakeScalarResult] | None = None) -> None:
        self.overrides = {} if overrides is None else overrides

    def exec_driver_sql(
        self, statement: str, _parameters: tuple[object, ...] = ()
    ) -> FakeScalarResult:
        for key, result in self.overrides.items():
            if key in statement:
                return result
        defaults = {
            "quick_check": FakeScalarResult("ok"),
            "foreign_key_check": FakeScalarResult(rows=[]),
            "foreign_keys": FakeScalarResult(1),
            "journal_mode": FakeScalarResult("wal"),
            "synchronous": FakeScalarResult(2),
            "busy_timeout": FakeScalarResult(5_000),
            "version_num": FakeScalarResult(rows=[("0001_operational",)]),
            "type='table'": FakeScalarResult(21),
            "type='trigger'": FakeScalarResult(47),
        }
        return next(result for key, result in defaults.items() if key in statement)


@pytest.mark.parametrize(
    ("override", "message"),
    [
        ({"quick_check": FakeScalarResult("bad")}, "quick check"),
        ({"foreign_key_check": FakeScalarResult(rows=[("bad",)])}, "foreign-key"),
        ({"busy_timeout": FakeScalarResult(1)}, "durability pragmas"),
        ({"version_num": FakeScalarResult(rows=[])}, "migration version"),
        ({"type='table'": FakeScalarResult(20)}, "schema inventory"),
        ({"type='trigger'": FakeScalarResult(46)}, "schema inventory"),
    ],
)
def test_scenario_reopen_validation_rejects_each_integrity_axis(
    override: dict[str, FakeScalarResult], message: str
) -> None:
    with pytest.raises(CrashDatabaseIntegrityError, match=message):
        scenario._validate_reopened_database(cast(object, FakeValidationConnection(override)))


def test_scenario_projection_requires_all_roots(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(scenario, "_rows", lambda *_args, **_kwargs: ())
    with pytest.raises(CrashDatabaseIntegrityError, match="incomplete"):
        scenario._read_projection(cast(object, None))


def test_scenario_seed_and_retry_defensive_close_branches(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    class Context:
        def __enter__(self) -> MagicMock:
            return MagicMock()

        def __exit__(self, *_args: object) -> None:
            return None

    class Database:
        engine = SimpleNamespace(connect=lambda: Context())

        def transaction(self) -> Context:
            return Context()

        def close(self) -> None:
            return None

    class CloseResult:
        drained = False

    class Writer:
        def start(self) -> None:
            return None

        def close(self, *, timeout_seconds: float) -> CloseResult:
            del timeout_seconds
            return CloseResult()

    monkeypatch.setattr(scenario.SQLiteDatabase, "open", lambda *_args: Database())
    monkeypatch.setattr(scenario, "upgrade_to_head", lambda _connection: None)
    monkeypatch.setattr(scenario, "SqlAlchemyPipelineRepository", MagicMock())
    monkeypatch.setattr(scenario, "_writer", lambda *_args: Writer())
    responses = iter(
        (
            SimpleNamespace(result=SimpleNamespace(claim=None)),
            SimpleNamespace(result=SimpleNamespace(claim=None)),
            SimpleNamespace(result=SimpleNamespace(claim=None)),
            SimpleNamespace(result=SimpleNamespace(claim=scenario.work_claim(0))),
            SimpleNamespace(result=SimpleNamespace(claim=None)),
            SimpleNamespace(result=SimpleNamespace(claim=scenario.work_claim(1))),
            SimpleNamespace(result=SimpleNamespace(claim=None)),
            SimpleNamespace(result=SimpleNamespace(claim=scenario.work_claim(2))),
        )
    )
    monkeypatch.setattr(scenario, "_submit", lambda *_args: next(responses))
    with pytest.raises(CrashDatabaseIntegrityError, match="seed writer"):
        scenario.prepare_crash_database(tmp_path / "seed-close.sqlite3", 1)
    monkeypatch.setattr(scenario, "_submit", lambda *_args: SimpleNamespace())
    with pytest.raises(CrashDatabaseIntegrityError, match="retry writer"):
        scenario.commit_target_normally(tmp_path / "retry.sqlite3")


def test_scenario_seed_rejects_wrong_claim_and_wrong_postcondition(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    class Context:
        def __enter__(self) -> MagicMock:
            return MagicMock()

        def __exit__(self, *_args: object) -> None:
            return None

    class Database:
        engine = SimpleNamespace(connect=lambda: Context())

        def transaction(self) -> Context:
            return Context()

        def close(self) -> None:
            return None

    class Writer:
        def start(self) -> None:
            return None

        def close(self, *, timeout_seconds: float) -> SimpleNamespace:
            del timeout_seconds
            return SimpleNamespace(drained=True)

    monkeypatch.setattr(scenario.SQLiteDatabase, "open", lambda *_args: Database())
    monkeypatch.setattr(scenario, "upgrade_to_head", lambda _connection: None)
    monkeypatch.setattr(scenario, "SqlAlchemyPipelineRepository", MagicMock())
    monkeypatch.setattr(scenario, "_writer", lambda *_args: Writer())
    monkeypatch.setattr(
        scenario, "_submit", lambda *_args: SimpleNamespace(result=SimpleNamespace(claim=None))
    )
    with pytest.raises(CrashDatabaseIntegrityError, match="claim"):
        scenario.prepare_crash_database(tmp_path / "bad-claim.sqlite3", 1)
    responses = iter(
        (
            SimpleNamespace(result=SimpleNamespace(claim=None)),
            SimpleNamespace(result=SimpleNamespace(claim=None)),
            SimpleNamespace(result=SimpleNamespace(claim=None)),
            SimpleNamespace(result=SimpleNamespace(claim=scenario.work_claim(0))),
            SimpleNamespace(result=SimpleNamespace(claim=None)),
            SimpleNamespace(result=SimpleNamespace(claim=scenario.work_claim(1))),
            SimpleNamespace(result=SimpleNamespace(claim=None)),
            SimpleNamespace(result=SimpleNamespace(claim=scenario.work_claim(2))),
        )
    )
    monkeypatch.setattr(scenario, "_submit", lambda *_args: next(responses))
    monkeypatch.setattr(
        scenario,
        "classify_crash_database",
        lambda *_args: (CrashDatabaseOutcome.COMMITTED, expected_projection(1, True)),
    )
    with pytest.raises(CrashDatabaseIntegrityError, match="baseline"):
        scenario.prepare_crash_database(tmp_path / "bad-post.sqlite3", 1)


def test_scenario_second_reopen_must_match_first(monkeypatch: pytest.MonkeyPatch) -> None:
    projections = iter((expected_projection(1, False), expected_projection(2, False)))

    class Context:
        def __enter__(self) -> MagicMock:
            return MagicMock()

        def __exit__(self, *_args: object) -> None:
            return None

    class Database:
        engine = SimpleNamespace(connect=lambda: Context())

        def close(self) -> None:
            return None

    monkeypatch.setattr(scenario.SQLiteDatabase, "open", lambda *_args: Database())
    monkeypatch.setattr(scenario, "upgrade_to_head", lambda _connection: None)
    monkeypatch.setattr(scenario, "_validate_reopened_database", lambda _connection: None)
    monkeypatch.setattr(scenario, "_read_projection", lambda _connection: next(projections))
    with pytest.raises(CrashDatabaseIntegrityError, match="across reopen"):
        scenario.classify_crash_database(Path.cwd() / "ignored.sqlite3", 1)


def test_raw_wal_recovery_rejects_integrity_foreign_key_and_checkpoint_drift(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    class Cursor:
        def __init__(
            self,
            *,
            rows: list[tuple[object, ...]] | None = None,
            row: tuple[object, ...] | None = None,
        ) -> None:
            self.rows = [] if rows is None else rows
            self.row = row

        def fetchall(self) -> list[tuple[object, ...]]:
            return self.rows

        def fetchone(self) -> tuple[object, ...] | None:
            return self.row

    class Connection:
        def __init__(self, responses: dict[str, Cursor]) -> None:
            self.responses = responses

        def execute(self, statement: str) -> Cursor:
            return next(
                response for fragment, response in self.responses.items() if fragment in statement
            )

        def close(self) -> None:
            return None

    base = {
        "busy_timeout": Cursor(),
        "quick_check": Cursor(rows=[("ok",)]),
        "foreign_key_check": Cursor(rows=[]),
        "wal_checkpoint": Cursor(row=(0, 0, 0)),
    }
    for replacement, message in (
        ({"quick_check": Cursor(rows=[("bad",)])}, "integrity"),
        ({"foreign_key_check": Cursor(rows=[("bad",)])}, "foreign-key"),
        ({"wal_checkpoint": Cursor(row=None)}, "result"),
        ({"wal_checkpoint": Cursor(row=(2, 0, 0))}, "result"),
    ):
        connection = Connection({**base, **replacement})
        monkeypatch.setattr(
            scenario.sqlite3,
            "connect",
            lambda *_args, selected=connection, **_kwargs: selected,
        )
        with pytest.raises(CrashDatabaseIntegrityError, match=message):
            scenario._recover_wal_after_crash(tmp_path / "scenario.sqlite3")
    monkeypatch.setattr(
        scenario.sqlite3,
        "connect",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(scenario.sqlite3.DatabaseError()),
    )
    with pytest.raises(CrashDatabaseIntegrityError, match="recovery failed"):
        scenario._recover_wal_after_crash(tmp_path / "scenario.sqlite3")


def test_raw_wal_recovery_discards_only_the_transient_wal_index(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    database_path = tmp_path / "scenario.sqlite3"
    database_path.write_bytes(b"database")
    wal_path = Path(f"{database_path}-wal")
    wal_path.write_bytes(b"durable-wal")
    wal_index_path = Path(f"{database_path}-shm")
    wal_index_path.write_bytes(b"transient-index")

    class Cursor:
        def __init__(
            self,
            *,
            rows: list[tuple[object, ...]] | None = None,
            row: tuple[object, ...] | None = None,
        ) -> None:
            self.rows = [] if rows is None else rows
            self.row = row

        def fetchall(self) -> list[tuple[object, ...]]:
            return self.rows

        def fetchone(self) -> tuple[object, ...] | None:
            return self.row

    class Connection:
        def execute(self, statement: str) -> Cursor:
            if "quick_check" in statement:
                return Cursor(rows=[("ok",)])
            if "foreign_key_check" in statement:
                return Cursor(rows=[])
            if "wal_checkpoint" in statement:
                return Cursor(row=(0, 0, 0))
            return Cursor()

        def close(self) -> None:
            return None

    monkeypatch.setattr(scenario.sqlite3, "connect", lambda *_args, **_kwargs: Connection())
    scenario._recover_wal_after_crash(database_path)

    assert database_path.read_bytes() == b"database"
    assert wal_path.read_bytes() == b"durable-wal"
    assert not wal_index_path.exists()


def test_raw_wal_recovery_fails_closed_when_wal_index_cannot_be_removed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    database_path = tmp_path / "scenario.sqlite3"
    wal_index_path = Path(f"{database_path}-shm")
    original_unlink = Path.unlink

    def fail_target(path: Path, *, missing_ok: bool = False) -> None:
        if path == wal_index_path:
            raise OSError("injected")
        original_unlink(path, missing_ok=missing_ok)

    monkeypatch.setattr(Path, "unlink", fail_target)
    with pytest.raises(CrashDatabaseIntegrityError, match="WAL index cleanup"):
        scenario._recover_wal_after_crash(database_path)

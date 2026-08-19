"""Bounded parent-side crash, reopen, classification, and retry harness."""

from __future__ import annotations

import os
import queue
import shutil
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from threading import Thread

from .crash_reopen_protocol import (
    MAX_PROTOCOL_LINE_BYTES,
    CrashControlEnvelope,
    CrashFailpoint,
    CrashMarker,
    CrashMarkerRecord,
    CrashReopenProtocolError,
    invocation_token,
    release_commit_bytes,
)
from .crash_reopen_scenario import (
    CrashDatabaseOutcome,
    classify_crash_database,
    commit_target_normally,
    prepare_crash_database,
)

_BINARY_OPEN_FLAG = getattr(os, "O_BINARY", 0)


class CrashReopenHarnessError(Exception):
    """The bounded crash-reopen case failed closed."""


@dataclass(frozen=True, slots=True)
class CrashReopenConfig:
    case_directory: Path
    failpoint: CrashFailpoint
    seed: int = 8675309
    # Cold interpreter starts on a loaded Windows host can exceed five
    # seconds; the waits stay bounded and deterministic per run.
    startup_timeout_seconds: float = 30.0
    boundary_timeout_seconds: float = 30.0
    process_timeout_seconds: float = 30.0
    hold_timeout_seconds: float = 45.0

    def __post_init__(self) -> None:
        if not self.case_directory.is_absolute():
            raise CrashReopenHarnessError("case directory must be an absolute Path")
        if type(self.failpoint) is not CrashFailpoint:
            raise CrashReopenHarnessError("case failpoint is invalid")
        if type(self.seed) is not int or not 0 <= self.seed <= 4_294_967_295:
            raise CrashReopenHarnessError("case seed is invalid")
        for value in (
            self.startup_timeout_seconds,
            self.boundary_timeout_seconds,
            self.process_timeout_seconds,
            self.hold_timeout_seconds,
        ):
            if type(value) is not float or not 0.1 <= value <= 60.0:
                raise CrashReopenHarnessError("case timeout is invalid")
        if self.hold_timeout_seconds <= self.boundary_timeout_seconds:
            raise CrashReopenHarnessError("hold timeout must exceed the boundary timeout")
        _validate_new_case_directory(self.case_directory)


@dataclass(frozen=True, slots=True)
class CrashReopenResult:
    failpoint: CrashFailpoint
    observed_outcome: CrashDatabaseOutcome
    final_outcome: CrashDatabaseOutcome
    retried: bool
    marker_prefix: tuple[CrashMarkerRecord, ...]
    process_return_code: int

    def __post_init__(self) -> None:
        if type(self.failpoint) is not CrashFailpoint:
            raise CrashReopenHarnessError("result failpoint is invalid")
        if type(self.observed_outcome) is not CrashDatabaseOutcome:
            raise CrashReopenHarnessError("observed outcome is invalid")
        if type(self.final_outcome) is not CrashDatabaseOutcome:
            raise CrashReopenHarnessError("final outcome is invalid")
        if type(self.retried) is not bool or type(self.process_return_code) is not int:
            raise CrashReopenHarnessError("result metadata is invalid")
        if not self.marker_prefix or any(
            type(marker) is not CrashMarkerRecord for marker in self.marker_prefix
        ):
            raise CrashReopenHarnessError("result marker prefix is invalid")
        if self.final_outcome is not CrashDatabaseOutcome.COMMITTED:
            raise CrashReopenHarnessError("recovery did not reach the committed outcome")
        if self.retried != (self.observed_outcome is CrashDatabaseOutcome.ABSENT):
            raise CrashReopenHarnessError("recovery retry decision is inconsistent")


@dataclass(slots=True)
class _ChildProcess:
    process: subprocess.Popen[bytes]
    lines: queue.Queue[bytes | None]
    reader: Thread


def _validate_new_case_directory(path: Path) -> None:
    try:
        resolved = path.resolve(strict=False)
        if resolved != path or path.exists():
            raise CrashReopenHarnessError("case directory must be a new canonical path")
        parent = path.parent
        if not parent.is_dir():
            raise CrashReopenHarnessError("case directory parent must exist")
        for component in (parent, *parent.parents):
            if component.is_symlink() or component.is_junction():
                raise CrashReopenHarnessError("case directory cannot use linked components")
    except CrashReopenHarnessError:
        raise
    except OSError as error:
        raise CrashReopenHarnessError("case directory could not be inspected") from error


def _write_durable_new(path: Path, payload: bytes) -> None:
    candidate = path.with_name(f".{path.name}.candidate")
    if path.exists() or candidate.exists():
        raise CrashReopenHarnessError("case control output already exists")
    descriptor = os.open(
        candidate,
        os.O_CREAT | os.O_EXCL | os.O_WRONLY | _BINARY_OPEN_FLAG,
        0o600,
    )
    try:
        written = os.write(descriptor, payload)
        if written != len(payload):
            raise CrashReopenHarnessError("case control write was incomplete")
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
    os.replace(candidate, path)


def _worker_environment() -> dict[str, str]:
    environment: dict[str, str] = {
        "PYTHONIOENCODING": "utf-8",
        "PYTHONUTF8": "1",
    }
    for name in ("SYSTEMROOT", "WINDIR", "TEMP", "TMP", "LANG", "LC_ALL"):
        value = os.environ.get(name)
        if value is not None:
            environment[name] = value
    return environment


def _spawn_worker(control_path: Path, working_directory: Path) -> _ChildProcess:
    try:
        process = subprocess.Popen(
            [
                sys.executable,
                "-I",
                "-m",
                "paritygrid.quality.crash_reopen_worker",
                "--control",
                str(control_path),
            ],
            cwd=working_directory,
            env=_worker_environment(),
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            shell=False,
            bufsize=0,
        )
    except OSError as error:
        raise CrashReopenHarnessError("crash worker could not be started") from error
    lines: queue.Queue[bytes | None] = queue.Queue()

    def read_stdout() -> None:
        assert process.stdout is not None
        try:
            while True:
                line = process.stdout.readline(MAX_PROTOCOL_LINE_BYTES + 1)
                if not line:
                    break
                lines.put(line)
        finally:
            lines.put(None)

    reader = Thread(target=read_stdout, name="paritygrid-crash-stdout", daemon=False)
    try:
        reader.start()
    except BaseException:
        _stop_process(process, 1.0)
        _close_pipes(process)
        raise
    return _ChildProcess(process=process, lines=lines, reader=reader)


def _wait_for_marker(
    child: _ChildProcess,
    envelope: CrashControlEnvelope,
    expected_marker: CrashMarker,
    expected_ordinal: int,
    timeout_seconds: float,
    records: list[CrashMarkerRecord],
    frames: list[bytes],
) -> None:
    deadline = time.monotonic() + timeout_seconds
    while True:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise CrashReopenHarnessError("crash worker marker timed out")
        try:
            frame = child.lines.get(timeout=remaining)
        except queue.Empty as error:
            raise CrashReopenHarnessError("crash worker marker timed out") from error
        if frame is None:
            raise CrashReopenHarnessError("crash worker exited before its boundary")
        try:
            record = CrashMarkerRecord.from_bytes(frame)
        except CrashReopenProtocolError as error:
            raise CrashReopenHarnessError("crash worker emitted an invalid marker") from error
        if (
            record.invocation_token != envelope.invocation_token
            or record.case_id != envelope.case_id
            or record.sequence != len(records) + 1
        ):
            raise CrashReopenHarnessError("crash worker marker sequence is invalid")
        records.append(record)
        frames.append(frame)
        if record.marker is expected_marker and record.command_ordinal == expected_ordinal:
            return


def _release_ambiguous_commit(child: _ChildProcess, envelope: CrashControlEnvelope) -> None:
    assert child.process.stdin is not None
    frame = release_commit_bytes(envelope.invocation_token)
    control_ledger = envelope.marker_path.with_name("controls-v1.jsonl")
    _write_durable_new(control_ledger, frame)
    try:
        child.process.stdin.write(frame)
        child.process.stdin.flush()
    except (BrokenPipeError, OSError, ValueError) as error:
        raise CrashReopenHarnessError("commit release could not be delivered") from error


def _stop_process(process: subprocess.Popen[bytes], timeout_seconds: float) -> int:
    if process.poll() is None:
        try:
            process.kill()
        except OSError as error:
            raise CrashReopenHarnessError("crash worker could not be stopped") from error
    try:
        return process.wait(timeout=timeout_seconds)
    except subprocess.TimeoutExpired as error:
        raise CrashReopenHarnessError("crash worker did not exit after termination") from error


def _wait_for_exit(process: subprocess.Popen[bytes], timeout_seconds: float) -> int:
    try:
        return process.wait(timeout=timeout_seconds)
    except subprocess.TimeoutExpired as error:
        raise CrashReopenHarnessError("crash worker exit timed out") from error


def _close_pipes(process: subprocess.Popen[bytes]) -> None:
    for stream in (process.stdin, process.stdout, process.stderr):
        if stream is not None:
            stream.close()


def _finish_reader(child: _ChildProcess, timeout_seconds: float) -> None:
    child.reader.join(timeout_seconds)
    if child.reader.is_alive():
        raise CrashReopenHarnessError("crash worker output reader did not stop")


def _read_stderr(process: subprocess.Popen[bytes]) -> bytes:
    if process.stderr is None:
        return b""
    content = process.stderr.read(MAX_PROTOCOL_LINE_BYTES + 1)
    if len(content) > MAX_PROTOCOL_LINE_BYTES:
        raise CrashReopenHarnessError("crash worker diagnostic exceeded its bound")
    return content


def _validate_ledger(
    envelope: CrashControlEnvelope,
    stdout_frames: list[bytes],
) -> tuple[CrashMarkerRecord, ...]:
    try:
        ledger = envelope.marker_path.read_bytes()
    except OSError as error:
        raise CrashReopenHarnessError("durable marker ledger could not be read") from error
    stdout = b"".join(stdout_frames)
    if not ledger.startswith(stdout):
        raise CrashReopenHarnessError("stdout is not a prefix of the durable marker ledger")
    frames = ledger.splitlines(keepends=True)
    if not frames or any(len(frame) > MAX_PROTOCOL_LINE_BYTES for frame in frames):
        raise CrashReopenHarnessError("durable marker ledger is invalid")
    records: list[CrashMarkerRecord] = []
    for sequence, frame in enumerate(frames, start=1):
        try:
            record = CrashMarkerRecord.from_bytes(frame)
        except CrashReopenProtocolError as error:
            raise CrashReopenHarnessError("durable marker ledger is invalid") from error
        if (
            record.sequence != sequence
            or record.invocation_token != envelope.invocation_token
            or record.case_id != envelope.case_id
        ):
            raise CrashReopenHarnessError("durable marker ledger identity is invalid")
        records.append(record)
    return tuple(records)


def _target(failpoint: CrashFailpoint) -> tuple[CrashMarker, int, bool]:
    if failpoint is CrashFailpoint.NORMAL:
        return CrashMarker.SHUTDOWN_DRAINED, 0, False
    if failpoint is CrashFailpoint.BEFORE_COMMIT:
        return CrashMarker.PRE_COMMIT, 1, True
    if failpoint is CrashFailpoint.COMMIT_AMBIGUOUS:
        return CrashMarker.COMMIT_ENTERED, 1, True
    if failpoint is CrashFailpoint.AFTER_COMMIT_BEFORE_RECEIPT:
        return CrashMarker.SESSION_CLOSE_ENTERED, 1, True
    if failpoint is CrashFailpoint.AFTER_RECEIPT_BEFORE_NOTIFICATION:
        return CrashMarker.NOTIFICATION_ENTERED, 1, True
    return CrashMarker.SHUTDOWN_ENTERED, 0, True


def _cleanup_case(case_directory: Path) -> None:
    try:
        if case_directory.exists():
            if case_directory.is_symlink() or case_directory.is_junction():
                raise CrashReopenHarnessError("case cleanup target became linked")
            shutil.rmtree(case_directory)
    except CrashReopenHarnessError:
        raise
    except OSError as error:
        raise CrashReopenHarnessError("case directory cleanup failed") from error


def run_crash_reopen_case(config: CrashReopenConfig) -> CrashReopenResult:
    """Terminate one worker at a real boundary and recover from durable SQLite state."""
    if type(config) is not CrashReopenConfig:
        raise CrashReopenHarnessError("crash-reopen configuration is invalid")
    case_directory = config.case_directory
    case_directory.mkdir(mode=0o700)
    child: _ChildProcess | None = None
    try:
        database_path = case_directory / "scenario.sqlite3"
        marker_path = case_directory / "markers-v1.jsonl"
        control_path = case_directory / "control-v1.json"
        command_ordinal = 2 if config.failpoint is CrashFailpoint.SHUTDOWN_DRAIN else 1
        token = invocation_token(config.failpoint, command_ordinal, config.seed)
        envelope = CrashControlEnvelope(
            failpoint=config.failpoint,
            command_ordinal=command_ordinal,
            database_path=database_path,
            marker_path=marker_path,
            seed=config.seed,
            hold_timeout_seconds=config.hold_timeout_seconds,
            invocation_token=token,
        )
        prepare_crash_database(database_path, config.seed)
        _write_durable_new(control_path, envelope.to_bytes())
        child = _spawn_worker(control_path, case_directory)
        target_marker, target_ordinal, should_kill = _target(config.failpoint)
        frames: list[bytes] = []
        records: list[CrashMarkerRecord] = []
        _wait_for_marker(
            child,
            envelope,
            CrashMarker.WORKER_READY,
            0,
            config.startup_timeout_seconds,
            records,
            frames,
        )
        _wait_for_marker(
            child,
            envelope,
            target_marker,
            target_ordinal,
            config.boundary_timeout_seconds,
            records,
            frames,
        )
        if config.failpoint is CrashFailpoint.COMMIT_AMBIGUOUS:
            _release_ambiguous_commit(child, envelope)
        return_code = (
            _stop_process(child.process, config.process_timeout_seconds)
            if should_kill
            else _wait_for_exit(child.process, config.process_timeout_seconds)
        )
        _finish_reader(child, config.process_timeout_seconds)
        diagnostic = _read_stderr(child.process)
        if not should_kill and (return_code != 0 or diagnostic):
            raise CrashReopenHarnessError("crash worker normal control failed")
        marker_prefix = _validate_ledger(envelope, frames)
        observed, _ = classify_crash_database(database_path, config.seed)
        expected = {
            CrashFailpoint.NORMAL: {CrashDatabaseOutcome.COMMITTED},
            CrashFailpoint.BEFORE_COMMIT: {CrashDatabaseOutcome.ABSENT},
            CrashFailpoint.COMMIT_AMBIGUOUS: {
                CrashDatabaseOutcome.ABSENT,
                CrashDatabaseOutcome.COMMITTED,
            },
            CrashFailpoint.AFTER_COMMIT_BEFORE_RECEIPT: {CrashDatabaseOutcome.COMMITTED},
            CrashFailpoint.AFTER_RECEIPT_BEFORE_NOTIFICATION: {CrashDatabaseOutcome.COMMITTED},
            CrashFailpoint.SHUTDOWN_DRAIN: {CrashDatabaseOutcome.COMMITTED},
        }[config.failpoint]
        if observed not in expected:
            raise CrashReopenHarnessError("durable outcome violated the crash boundary")
        retried = observed is CrashDatabaseOutcome.ABSENT
        if retried:
            commit_target_normally(database_path)
        final, _ = classify_crash_database(database_path, config.seed)
        return CrashReopenResult(
            failpoint=config.failpoint,
            observed_outcome=observed,
            final_outcome=final,
            retried=retried,
            marker_prefix=marker_prefix,
            process_return_code=return_code,
        )
    finally:
        cleanup_error: BaseException | None = None
        if child is not None:
            try:
                _stop_process(child.process, config.process_timeout_seconds)
                _finish_reader(child, config.process_timeout_seconds)
            except BaseException as error:
                cleanup_error = error
            finally:
                _close_pipes(child.process)
        try:
            _cleanup_case(case_directory)
        except BaseException as error:
            cleanup_error = cleanup_error or error
        if cleanup_error is not None and sys.exception() is None:
            raise cleanup_error


__all__ = [
    "CrashReopenConfig",
    "CrashReopenHarnessError",
    "CrashReopenResult",
    "run_crash_reopen_case",
]

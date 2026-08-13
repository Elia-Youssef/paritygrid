"""Closed protocol and worker instrumentation tests for crash reopening."""

from __future__ import annotations

import io
import json
import os
from pathlib import Path
from threading import Thread

import pytest

from paritygrid.quality.crash_reopen_protocol import (
    MAX_CONTROL_BYTES,
    MAX_PROTOCOL_LINE_BYTES,
    CrashControlEnvelope,
    CrashFailpoint,
    CrashMarker,
    CrashMarkerEmitter,
    CrashMarkerRecord,
    CrashReopenProtocolError,
    invocation_token,
    load_control,
    release_commit_bytes,
    validate_release_commit,
)
from paritygrid.quality.crash_reopen_scenario import (
    CrashDatabaseOutcome,
    classify_crash_database,
    prepare_crash_database,
)
from paritygrid.quality.crash_reopen_worker import main, run_worker


def envelope(
    directory: Path, failpoint: CrashFailpoint = CrashFailpoint.NORMAL
) -> CrashControlEnvelope:
    ordinal = 2 if failpoint is CrashFailpoint.SHUTDOWN_DRAIN else 1
    return CrashControlEnvelope(
        failpoint=failpoint,
        command_ordinal=ordinal,
        database_path=directory / "scenario.sqlite3",
        marker_path=directory / "markers-v1.jsonl",
        seed=8675309,
        hold_timeout_seconds=15.0,
        invocation_token=invocation_token(failpoint, ordinal, 8675309),
    )


def test_control_marker_release_round_trip_supports_unicode_paths(tmp_path: Path) -> None:
    directory = tmp_path / "Café % Cafe\u0301 عربي"
    directory.mkdir()
    value = envelope(directory)
    assert CrashControlEnvelope.from_bytes(value.to_bytes()) == value
    assert len(value.to_bytes()) <= MAX_CONTROL_BYTES
    record = CrashMarkerRecord(
        invocation_token=value.invocation_token,
        case_id=value.case_id,
        sequence=1,
        command_ordinal=0,
        marker=CrashMarker.WORKER_READY,
    )
    assert CrashMarkerRecord.from_bytes(record.to_bytes()) == record
    release = release_commit_bytes(value.invocation_token)
    assert validate_release_commit(release, value.invocation_token) is None
    assert len(release) <= MAX_PROTOCOL_LINE_BYTES


@pytest.mark.parametrize(
    "payload",
    [
        b"",
        b"\xef\xbb\xbf{}\n",
        b"{}\r\n",
        b'{"x":"' + bytes((0,)) + b'"}\n',
        b"[]\n",
        b'{"x":1,"x":2}\n',
        b"{\xff}\n",
        b"not-json\n",
        b"{" + b"x" * MAX_CONTROL_BYTES + b"}\n",
    ],
)
def test_control_rejects_malformed_frames(payload: bytes) -> None:
    with pytest.raises(CrashReopenProtocolError):
        CrashControlEnvelope.from_bytes(payload)


def test_control_rejects_wrong_fields_version_scenario_and_identity(tmp_path: Path) -> None:
    value = envelope(tmp_path)
    decoded = json.loads(value.to_bytes())
    mutations = (
        {**decoded, "extra": True},
        {**decoded, "protocol_version": 2},
        {**decoded, "scenario": "other"},
        {**decoded, "failpoint": "other"},
        {**decoded, "case_id": "before_commit"},
        {**decoded, "hold_timeout_seconds": True},
        {**decoded, "command_ordinal": 0},
        {**decoded, "seed": -1},
        {**decoded, "invocation_token": "0" * 64},
    )
    for mutation in mutations:
        with pytest.raises(CrashReopenProtocolError):
            CrashControlEnvelope.from_bytes(
                (json.dumps(mutation, separators=(",", ":")) + "\n").encode()
            )


def test_envelope_and_token_validate_exact_runtime_types(tmp_path: Path) -> None:
    valid = envelope(tmp_path)
    for replacement in (
        {"failpoint": "normal"},
        {"command_ordinal": True},
        {"seed": True},
        {"hold_timeout_seconds": 1},
        {"hold_timeout_seconds": 0.5},
        {"hold_timeout_seconds": 31.0},
        {"database_path": valid.marker_path},
        {"invocation_token": "wrong"},
    ):
        values = {
            "failpoint": valid.failpoint,
            "command_ordinal": valid.command_ordinal,
            "database_path": valid.database_path,
            "marker_path": valid.marker_path,
            "seed": valid.seed,
            "hold_timeout_seconds": valid.hold_timeout_seconds,
            "invocation_token": valid.invocation_token,
        }
        values.update(replacement)
        with pytest.raises((CrashReopenProtocolError, TypeError)):
            CrashControlEnvelope(**values)  # type: ignore[arg-type]
    with pytest.raises(TypeError):
        invocation_token("normal", 1, 1)  # type: ignore[arg-type]
    with pytest.raises(CrashReopenProtocolError):
        invocation_token(CrashFailpoint.NORMAL, 0, 1)


def test_control_loader_enforces_regular_private_sibling_paths(tmp_path: Path) -> None:
    value = envelope(tmp_path)
    control = tmp_path / "control-v1.json"
    control.write_bytes(value.to_bytes())
    assert load_control(control) == value
    with pytest.raises(CrashReopenProtocolError):
        load_control(tmp_path / "missing.json")
    nested = tmp_path / "nested"
    nested.mkdir()
    escaped = CrashControlEnvelope(
        value.failpoint,
        value.command_ordinal,
        value.database_path,
        nested / "markers.jsonl",
        value.seed,
        value.hold_timeout_seconds,
        value.invocation_token,
    )
    control.write_bytes(escaped.to_bytes())
    with pytest.raises(CrashReopenProtocolError, match="control directory"):
        load_control(control)


def test_marker_validation_and_durable_emission_are_serialized(tmp_path: Path) -> None:
    value = envelope(tmp_path)
    output = io.BytesIO()
    emitter = CrashMarkerEmitter(value, output)
    threads = [Thread(target=emitter.emit, args=(CrashMarker.PRE_COMMIT, 1)) for _ in range(8)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    ledger = value.marker_path.read_bytes().splitlines(keepends=True)
    assert output.getvalue() == b"".join(ledger)
    assert [CrashMarkerRecord.from_bytes(line).sequence for line in ledger] == list(range(1, 9))
    bad = json.loads(ledger[0])
    for mutation in (
        {**bad, "extra": 1},
        {**bad, "protocol_version": 2},
        {**bad, "marker": "bad"},
        {**bad, "sequence": 0},
        {**bad, "command_ordinal": 4},
        {**bad, "invocation_token": "g" * 64},
        {**bad, "case_id": "bad"},
    ):
        with pytest.raises(CrashReopenProtocolError):
            CrashMarkerRecord.from_bytes((json.dumps(mutation) + "\n").encode())
    with pytest.raises(CrashReopenProtocolError):
        CrashMarkerEmitter("bad", output)  # type: ignore[arg-type]


def test_release_validation_rejects_wrong_shape_version_and_token(tmp_path: Path) -> None:
    token = envelope(tmp_path).invocation_token
    decoded = json.loads(release_commit_bytes(token))
    for mutation in (
        {**decoded, "extra": 1},
        {**decoded, "protocol_version": 2},
        {**decoded, "action": "other"},
        {**decoded, "invocation_token": "0" * 64},
    ):
        with pytest.raises(CrashReopenProtocolError):
            validate_release_commit((json.dumps(mutation) + "\n").encode(), token)


def test_worker_normal_control_exercises_real_writer_boundaries(tmp_path: Path) -> None:
    value = envelope(tmp_path)
    prepare_crash_database(value.database_path, value.seed)
    control = tmp_path / "control-v1.json"
    control.write_bytes(value.to_bytes())
    output = io.BytesIO()
    assert run_worker(control, stdin=io.BytesIO(), stdout=output) == 0
    records = [CrashMarkerRecord.from_bytes(line) for line in output.getvalue().splitlines(True)]
    assert [record.marker for record in records] == list(CrashMarker)
    assert (
        classify_crash_database(value.database_path, value.seed)[0]
        is CrashDatabaseOutcome.COMMITTED
    )


def test_worker_main_returns_stable_bounded_diagnostic(tmp_path: Path) -> None:
    stderr = io.StringIO()
    assert (
        main(
            ["--control", str(tmp_path / "missing")],
            stdin=io.BytesIO(),
            stdout=io.BytesIO(),
            stderr=stderr,
        )
        == 2
    )
    assert stderr.getvalue() == '{"error":"crash_worker_failed"}\n'
    assert os.fspath(tmp_path) not in stderr.getvalue()

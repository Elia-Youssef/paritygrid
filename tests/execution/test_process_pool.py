"""Subordinate process pool: codec, spawn, isolation, and orphans (P7.14)."""

from __future__ import annotations

import multiprocessing
import multiprocessing.connection
from collections.abc import Callable
from datetime import UTC, datetime
from multiprocessing.process import BaseProcess
from pathlib import Path
from typing import cast

import pytest

import paritygrid.adapters.runners.process_pool as process_pool_module
import paritygrid.adapters.runners.process_workers.worker as worker_module
from paritygrid.adapters.runners import SubordinateProcessPool
from paritygrid.adapters.runners.process_workers.worker import (
    install_runtime_guard,
    worker_entry,
)
from paritygrid.adapters.runners.subordinate_codec import (
    MAX_PAYLOAD_BYTES,
    SubordinateCodecError,
    SubordinateOperationError,
    SubordinatePayloadError,
    decode_request,
    decode_response,
    encode_request,
    encode_response,
)
from paritygrid.application.execution.capacity import (
    CapacityPermit,
    ScheduledWorkLimiters,
    SubordinateCallLimiter,
)
from paritygrid.application.execution.clock_policy import ManualClock
from paritygrid.application.execution.concurrency_settings import (
    CapturedConcurrencySettings,
)
from paritygrid.domain.models import UtcTimestamp

_BASE = UtcTimestamp(datetime(2026, 8, 24, 8, 0, 0, tzinfo=UTC))
_NODE = "nod_c-src"


class _PoolRig:
    """One parent capacity limiter with its subordinate CPU pool."""

    def __init__(
        self,
        *,
        cpu_limit: int = 2,
        entry: Callable[[bytes], bytes] = worker_entry,
        timeout: float = 10.0,
    ) -> None:
        self.settings = CapturedConcurrencySettings(cpu_pool_operations=cpu_limit)
        self.parent = ScheduledWorkLimiters(
            self.settings,
            strategy_id="threaded",
            node_ids=(_NODE,),
            clock=ManualClock(_BASE),
        )
        self.subordinate = SubordinateCallLimiter(
            category="cpu_pool",
            limit=cpu_limit,
            clock=ManualClock(_BASE),
            parent_limiter=self.parent,
        )
        self.pool = SubordinateProcessPool(
            capacity=self.subordinate,
            timeout_seconds=timeout,
            entry=entry,
        )

    def triple(self, owner: str) -> tuple[CapacityPermit, CapacityPermit, CapacityPermit]:
        return self.parent.acquire(owner, _NODE)


@pytest.fixture
def rig() -> _PoolRig:
    fixture = _PoolRig()
    return fixture


class TestSubordinateCodec:
    def test_canonical_round_trip(self) -> None:
        request = encode_request("sha256_digest", 1, {"data_hex": "00ff"})
        operation, version, payload = decode_request(request)
        assert (operation, version, payload) == ("sha256_digest", 1, {"data_hex": "00ff"})
        response = encode_response("sha256_digest", {"digest": "ab" * 32})
        responded, result = decode_response(response)
        assert responded == "sha256_digest"
        assert result["digest"] == "ab" * 32

    def test_unknown_operation_and_version_fail_closed(self) -> None:
        with pytest.raises(SubordinateOperationError):
            encode_request("drop_table", 1, {})
        with pytest.raises(SubordinateOperationError):
            encode_request("sha256_digest", 2, {})
        with pytest.raises(SubordinateOperationError, match="printable ASCII"):
            encode_request("sha256\n", 1, {})
        with pytest.raises(SubordinateOperationError, match="version is invalid"):
            encode_request("sha256_digest", True, {})  # type: ignore[arg-type]
        with pytest.raises(SubordinatePayloadError, match="primitive mapping"):
            encode_request("sha256_digest", 1, [])  # type: ignore[arg-type]

    def test_rejected_payload_shapes(self) -> None:
        with pytest.raises(SubordinatePayloadError):
            encode_request("sha256_digest", 1, {"data_hex": "/etc/passwd-ish"})
        with pytest.raises(SubordinatePayloadError):
            encode_request("sha256_digest", 1, {"password": "x"})
        with pytest.raises(SubordinatePayloadError):
            encode_request("sha256_digest", 1, {"nested": [[[[["deep"]]]]]})
        with pytest.raises(SubordinatePayloadError):
            encode_request("sha256_digest", 1, {"blob": "x" * 5000})
        with pytest.raises(SubordinatePayloadError):
            encode_request("sha256_digest", 1, {"float": 1.5})

    def test_live_objects_never_encode(self) -> None:
        with pytest.raises(SubordinatePayloadError):
            encode_request("sha256_digest", 1, {"callback": print})  # type: ignore[dict-item]
        with pytest.raises(SubordinatePayloadError):
            encode_request("sha256_digest", 1, {"path": Path("x")})  # type: ignore[dict-item]

    @pytest.mark.parametrize("key", [None, True, 1])
    def test_mapping_keys_must_be_text_for_canonical_round_trip(self, key: object) -> None:
        with pytest.raises(SubordinatePayloadError, match="keys must be text"):
            encode_request("sha256_digest", 1, {key: "value"})  # type: ignore[dict-item]
        with pytest.raises(SubordinatePayloadError, match="keys must be text"):
            encode_response("sha256_digest", {key: "value"})  # type: ignore[dict-item]

    def test_text_mapping_keys_round_trip_through_both_envelopes(self) -> None:
        request = encode_request("sha256_digest", 1, {"data_hex": "00", "tag": "ok"})
        assert decode_request(request)[2] == {"data_hex": "00", "tag": "ok"}
        response = encode_response("sha256_digest", {"digest": "ab", "tag": "ok"})
        assert decode_response(response)[1] == {"digest": "ab", "tag": "ok"}

    def test_noncanonical_decode_fails(self) -> None:
        canonical = encode_request("sha256_digest", 1, {"data_hex": "aa"})
        tampered = canonical.replace(b'"data_hex":"aa"', b'"data_hex": "aa"')
        with pytest.raises((SubordinatePayloadError, SubordinateCodecError)):
            decode_request(tampered)
        with pytest.raises(SubordinateCodecError):
            decode_response(b'{"codec_version": 9, "operation": "sha256_digest"}')
        canonical_response = encode_response("sha256_digest", {"digest": "ab" * 32})
        with pytest.raises(SubordinatePayloadError, match="canonically"):
            decode_response(canonical_response.replace(b'"digest":"', b'"digest": "'))

    @pytest.mark.parametrize("operation", [None, "", "sha256\\n", "unknown"])
    def test_operation_identity_is_closed_and_ascii(self, operation: object) -> None:
        with pytest.raises(SubordinateOperationError):
            encode_request(operation, 1, {})  # type: ignore[arg-type]

    @pytest.mark.parametrize(
        "payload",
        [
            {"count": 2**53 + 1},
            {"location": "https://example.test/forbidden"},
            {"text": "x" * 4_097},
            {"items": list(range(257))},
            {"nested": {"a": {"b": {"c": {"d": {"e": 1}}}}}},
            {str(index): index for index in range(65)},
        ],
    )
    def test_primitive_payload_bounds_fail_closed(self, payload: dict[str, object]) -> None:
        with pytest.raises(SubordinatePayloadError):
            encode_request("sha256_digest", 1, payload)

    @pytest.mark.parametrize("value", [None, True])
    def test_null_and_boolean_primitives_have_canonical_round_trips(self, value: object) -> None:
        request = encode_request("sha256_digest", 1, {"data_hex": "00", "tag": value})
        assert decode_request(request)[2]["tag"] is value

    @pytest.mark.parametrize(
        ("decoder", "data", "message"),
        [
            (
                decode_request,
                b'{"codec_version":2,"operation":"sha256_digest","operation_version":1,"payload":{}}',
                "codec version",
            ),
            (
                decode_request,
                b'{"codec_version":1,"operation":"sha256_digest","operation_version":2,"payload":{}}',
                "operation version",
            ),
            (
                decode_request,
                b'{"codec_version":1,"operation":"sha256_digest","operation_version":1,"payload":[]}',
                "primitive mapping",
            ),
            (
                decode_response,
                b'{"codec_version":2,"operation":"sha256_digest","result":{}}',
                "codec version",
            ),
            (
                decode_response,
                b'{"codec_version":1,"operation":"sha256_digest","result":[]}',
                "primitive mapping",
            ),
        ],
    )
    def test_decoders_reject_invalid_envelope_members(
        self,
        decoder: Callable[[bytes], object],
        data: bytes,
        message: str,
    ) -> None:
        with pytest.raises(SubordinateCodecError, match=message):
            decoder(data)

    @pytest.mark.parametrize("decoder", [decode_request, decode_response])
    def test_decoders_reject_oversized_wire_messages(
        self, decoder: Callable[[bytes], object]
    ) -> None:
        with pytest.raises(SubordinatePayloadError, match="byte bound"):
            decoder(b"x" * (MAX_PAYLOAD_BYTES + 1))

    @pytest.mark.parametrize(
        ("decoder", "data", "error"),
        [
            (decode_request, object(), TypeError),
            (decode_response, object(), TypeError),
            (decode_request, b"not-json", SubordinatePayloadError),
            (decode_response, b"not-json", SubordinatePayloadError),
            (decode_request, b"[]", SubordinatePayloadError),
            (decode_response, b"[]", SubordinatePayloadError),
        ],
    )
    def test_decoders_reject_non_envelopes(
        self,
        decoder: Callable[[object], object],
        data: object,
        error: type[Exception],
    ) -> None:
        with pytest.raises(error):
            decoder(data)

    def test_dispatch_registered_operation_validates_its_specific_payload(self) -> None:
        from paritygrid.adapters.runners.subordinate_codec import (
            dispatch_registered_operation,
        )

        for operation, payload in (
            ("sha256_digest", {}),
            ("sha256_digest", {"data_hex": "not-hex"}),
            ("sort_integers", {}),
            ("sort_integers", {"values": [1, "bad"]}),
        ):
            with pytest.raises(SubordinatePayloadError):
                dispatch_registered_operation(operation, payload)  # type: ignore[arg-type]


class TestProcessWorker:
    def test_worker_entry_encodes_success_and_rejection_responses(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(worker_module, "install_runtime_guard", lambda: None)
        operation, response = decode_response(
            worker_module.worker_entry(encode_request("sort_integers", 1, {"values": [3, 1]}))
        )
        assert operation == "sort_integers"
        assert response == {"sorted": [1, 3]}

        operation, response = decode_response(
            worker_module.worker_entry(encode_request("sha256_digest", 1, {"data_hex": "bad"}))
        )
        assert operation == "sha256_digest"
        assert "operation rejected" in str(response["worker_error"])

        operation, response = decode_response(worker_module.worker_entry(b"not-a-request"))
        assert operation == "sort_integers"
        assert "request rejected" in str(response["worker_error"])

    def test_worker_entry_encodes_unexpected_operation_failure(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(worker_module, "install_runtime_guard", lambda: None)

        def crash(_operation: str, _payload: dict[str, object]) -> dict[str, object]:
            raise RuntimeError("unexpected")

        monkeypatch.setattr(worker_module, "dispatch_registered_operation", crash)
        operation, response = decode_response(
            worker_module.worker_entry(encode_request("sort_integers", 1, {"values": []}))
        )
        assert operation == "sort_integers"
        assert "operation failed" in str(response["worker_error"])

    def test_runtime_guard_is_idempotent_after_installation(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(worker_module, "_guard_installed", True)
        worker_module.install_runtime_guard()

    def test_runtime_guard_refuses_forbidden_imports_and_file_opens(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        hooks: list[Callable[[str, tuple[object, ...]], None]] = []
        monkeypatch.setattr(worker_module, "_guard_installed", False)
        monkeypatch.setattr(worker_module.sys, "addaudithook", hooks.append)
        worker_module.install_runtime_guard()
        assert len(hooks) == 1
        hook = hooks[0]
        hook("import", ("json",))
        hook("unrelated", ())
        with pytest.raises(ImportError, match="forbidden import"):
            hook("import", ("sqlite3",))
        with pytest.raises(PermissionError, match="filesystem open"):
            hook("open", ("canary.db",))


def _crashing_entry(_request: bytes) -> bytes:
    raise RuntimeError("simulated worker crash")


def _slow_entry(_request: bytes) -> bytes:
    import time

    time.sleep(5.0)
    return b"{}"


def _wrong_operation_entry(_request: bytes) -> bytes:
    return encode_response("sort_integers", {"sorted": []})


class TestProcessPool:
    @pytest.mark.parametrize("timeout", [0, -1.0, 601.0, "one"])
    def test_pool_rejects_invalid_timeouts(self, rig: _PoolRig, timeout: object) -> None:
        with pytest.raises((TypeError, Exception)):
            SubordinateProcessPool(capacity=rig.subordinate, timeout_seconds=timeout)  # type: ignore[arg-type]

    def test_close_refuses_later_submission_idempotently(self, rig: _PoolRig) -> None:
        assert not rig.pool.is_closed
        rig.pool.close()
        rig.pool.close()
        assert rig.pool.is_closed
        with pytest.raises(Exception, match="no longer admits"):
            rig.pool.submit("closed-owner", "sort_integers", {"values": []}, parent=())  # type: ignore[arg-type]

    def test_result_worker_error_property_is_non_authoritative(self) -> None:
        assert process_pool_module.SubordinateResult("sort_integers", {}).worker_error is None
        assert (
            process_pool_module.SubordinateResult(
                "sort_integers", {"worker_error": "refused"}
            ).worker_error
            == "refused"
        )

    def test_spawn_worker_computes_registered_operation(self, rig: _PoolRig) -> None:
        import hashlib

        triple = rig.triple("owner-a")
        outcome = rig.pool.submit(
            "owner-a",
            "sha256_digest",
            {"data_hex": "deadbeef"},
            parent=triple,
        )
        assert outcome.operation_id == "sha256_digest"
        assert outcome.result["digest"] == hashlib.sha256(bytes.fromhex("deadbeef")).hexdigest()
        assert outcome.worker_error is None
        rig.parent.release("owner-a", _NODE)
        rig.pool.close()

    def test_malformed_payload_is_refused_by_dispatch(self, rig: _PoolRig) -> None:
        from paritygrid.adapters.runners.subordinate_codec import (
            dispatch_registered_operation,
        )

        rig.triple("owner-b")
        with pytest.raises(SubordinatePayloadError):
            dispatch_registered_operation("sort_integers", {"values": "nope"})
        rig.parent.release("owner-b", _NODE)
        rig.pool.close()

    def test_worker_crash_is_classified_and_leaves_no_orphan(self) -> None:
        crash_rig = _PoolRig(entry=_crashing_entry)
        triple = crash_rig.triple("owner-c")
        from paritygrid.adapters.runners import SubordinateWorkerCrashError

        with pytest.raises(SubordinateWorkerCrashError):
            crash_rig.pool.submit("owner-c", "sha256_digest", {"data_hex": "00"}, parent=triple)
        crash_rig.parent.release("owner-c", _NODE)
        crash_rig.pool.close()
        assert multiprocessing.active_children() == []

    def test_worker_timeout_is_bounded_and_classified(self) -> None:
        slow_rig = _PoolRig(entry=_slow_entry, timeout=1.0)
        triple = slow_rig.triple("owner-d")
        from paritygrid.adapters.runners import SubordinateWorkerTimeoutError

        with pytest.raises(SubordinateWorkerTimeoutError):
            slow_rig.pool.submit("owner-d", "sha256_digest", {"data_hex": "00"}, parent=triple)
        slow_rig.parent.release("owner-d", _NODE)
        slow_rig.pool.close()
        assert multiprocessing.active_children() == []

    def test_worker_response_is_bound_to_the_submitted_operation(self) -> None:
        wrong_operation_rig = _PoolRig(entry=_wrong_operation_entry)
        triple = wrong_operation_rig.triple("owner-operation-correlation")
        from paritygrid.adapters.runners import SubordinateWorkerCrashError

        with pytest.raises(SubordinateWorkerCrashError, match="does not match"):
            wrong_operation_rig.pool.submit(
                "owner-operation-correlation",
                "sha256_digest",
                {"data_hex": "00"},
                parent=triple,
            )
        wrong_operation_rig.parent.release("owner-operation-correlation", _NODE)
        wrong_operation_rig.pool.close()
        assert multiprocessing.active_children() == []

    def test_cpu_permit_gates_pool_capacity(self, rig: _PoolRig) -> None:
        triple_one = rig.triple("owner-e")
        triple_two = rig.triple("owner-f")
        outcome_one = rig.pool.submit(
            "owner-e", "sort_integers", {"values": [3, 1, 2]}, parent=triple_one
        )
        assert outcome_one.result["sorted"] == [1, 2, 3]
        outcome_two = rig.pool.submit(
            "owner-f", "sort_integers", {"values": [9]}, parent=triple_two
        )
        assert outcome_two.result["sorted"] == [9]
        rig.parent.release("owner-e", _NODE)
        rig.parent.release("owner-f", _NODE)
        snapshot = rig.subordinate.snapshot()
        assert snapshot.in_use == 0
        rig.pool.close()
        assert multiprocessing.active_children() == []


class TestPoolWorkerTarget:
    class _Sender:
        def __init__(self) -> None:
            self.payload: bytes | None = None
            self.closed = False

        def send_bytes(self, payload: bytes) -> None:
            self.payload = payload

        def close(self) -> None:
            self.closed = True

    def test_target_sends_one_response_and_closes(self) -> None:
        sender = self._Sender()
        process_pool_module._pool_worker_target(  # pyright: ignore[reportPrivateUsage]
            lambda _request: b"response",
            b"request",
            sender,  # type: ignore[arg-type]
        )
        assert sender.payload == b"response"
        assert sender.closed

    def test_target_closes_after_entry_failure(self) -> None:
        sender = self._Sender()
        with pytest.raises(RuntimeError, match="crash"):
            process_pool_module._pool_worker_target(  # pyright: ignore[reportPrivateUsage]
                _crashing_entry,
                b"request",
                sender,  # type: ignore[arg-type]
            )
        assert sender.payload is None
        assert sender.closed


class TestProcessPoolLifecycleEdges:
    class _Receiver:
        def __init__(self, *, eof: bool = False) -> None:
            self.eof = eof
            self.closed = False

        def poll(self, _timeout: float) -> bool:
            return True

        def recv_bytes(self, *, maxlength: int | None = None) -> bytes:
            assert maxlength == MAX_PAYLOAD_BYTES
            if self.eof:
                raise EOFError
            return encode_response("sort_integers", {"sorted": []})

        def close(self) -> None:
            self.closed = True

    class _Sender:
        def __init__(self) -> None:
            self.closed = False

        def close(self) -> None:
            self.closed = True

    class _Process:
        def __init__(self, *, start_error: bool = False) -> None:
            self.start_error = start_error
            self.closed = False

        def start(self) -> None:
            if self.start_error:
                raise RuntimeError("start failure")

        def join(self, *, timeout: float) -> None:
            del timeout

        def is_alive(self) -> bool:
            return False

        def terminate(self) -> None:
            raise AssertionError("terminated an already-finished process")

        def kill(self) -> None:
            raise AssertionError("killed an already-finished process")

        def close(self) -> None:
            self.closed = True

    class _Context:
        def __init__(
            self,
            receiver: TestProcessPoolLifecycleEdges._Receiver,
            sender: TestProcessPoolLifecycleEdges._Sender,
            process: TestProcessPoolLifecycleEdges._Process,
        ) -> None:
            self.receiver = receiver
            self.sender = sender
            self.process = process

        def Pipe(  # noqa: N802
            self, *, duplex: bool
        ) -> tuple[TestProcessPoolLifecycleEdges._Receiver, TestProcessPoolLifecycleEdges._Sender]:
            assert not duplex
            return self.receiver, self.sender

        def Process(  # noqa: N802
            self, **_arguments: object
        ) -> TestProcessPoolLifecycleEdges._Process:
            return self.process

    def test_eof_response_is_classified_and_releases_process_handle(self, rig: _PoolRig) -> None:
        receiver = self._Receiver(eof=True)
        sender = self._Sender()
        process = self._Process()
        object.__setattr__(rig.pool, "_context", self._Context(receiver, sender, process))
        with pytest.raises(Exception, match="without a response"):
            rig.pool._run_process(b"request", "sort_integers")  # pyright: ignore[reportPrivateUsage]
        assert receiver.closed
        assert sender.closed
        assert process.closed

    def test_start_failure_closes_pipe_endpoints(self, rig: _PoolRig) -> None:
        receiver = self._Receiver()
        sender = self._Sender()
        process = self._Process(start_error=True)
        object.__setattr__(rig.pool, "_context", self._Context(receiver, sender, process))
        with pytest.raises(RuntimeError, match="start failure"):
            rig.pool._run_process(b"request", "sort_integers")  # pyright: ignore[reportPrivateUsage]
        assert receiver.closed
        assert sender.closed
        assert process.closed

    def test_termination_tolerates_submit_owner_closing_its_handle(self, rig: _PoolRig) -> None:
        class _AlreadyClosedProcess(self._Process):
            def join(self, *, timeout: float) -> None:
                del timeout
                raise ValueError("process object is closed")

        rig.pool._terminate_and_join(  # pyright: ignore[reportPrivateUsage]
            cast(BaseProcess, _AlreadyClosedProcess())
        )

    def test_oversized_pipe_response_is_classified_before_allocation(self, rig: _PoolRig) -> None:
        class _OversizedReceiver(self._Receiver):
            def recv_bytes(self, *, maxlength: int | None = None) -> bytes:
                assert maxlength == MAX_PAYLOAD_BYTES
                raise OSError("bad message length")

        receiver = _OversizedReceiver()
        sender = self._Sender()
        process = self._Process()
        object.__setattr__(rig.pool, "_context", self._Context(receiver, sender, process))
        with pytest.raises(Exception, match="without a response"):
            rig.pool._run_process(b"request", "sort_integers")  # pyright: ignore[reportPrivateUsage]
        assert receiver.closed
        assert process.closed

    def test_close_attempts_every_tracked_process_after_one_failure(
        self, rig: _PoolRig, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        first = object()
        second = object()
        attempted: list[object] = []
        deadlines: list[float | None] = []

        def terminate(
            _self: SubordinateProcessPool,
            process: object,
            **_arguments: object,
        ) -> None:
            attempted.append(process)
            deadline = _arguments.get("deadline")
            deadlines.append(deadline if isinstance(deadline, float) else None)
            if process is first:
                raise process_pool_module.SubordinatePoolError("first failed")

        monkeypatch.setattr(SubordinateProcessPool, "_terminate_and_join", terminate)
        object.__setattr__(rig.pool, "_active_processes", {1: first, 2: second})
        with pytest.raises(Exception, match="first failed"):
            rig.pool.close()
        assert attempted == [first, second]
        assert len(set(deadlines)) == 1


class _SpawnConnection(multiprocessing.connection.Connection):  # type: ignore[type-arg]
    """Typed alias for the spawned canary pipe end."""


def _guard_canary(sender: _SpawnConnection) -> None:  # pragma: no cover - spawned child
    install_runtime_guard()
    findings: list[str] = []
    try:
        import sqlite3  # noqa: F401  # type: ignore[unused-import]

        findings.append("sqlite-import-succeeded")
    except ImportError, PermissionError:
        findings.append("sqlite-import-refused")
    try:
        db_handle = open("canary.db", "w")  # noqa: SIM115
        db_handle.close()
        findings.append("open-succeeded")
    except PermissionError:
        findings.append("open-refused")
    send = sender.send_bytes
    send(";".join(findings).encode("ascii"))
    sender.close()


def test_worker_cannot_open_operational_database() -> None:
    """A spawned canary proves the runtime guard refuses persistence."""
    ctx = multiprocessing.get_context("spawn")
    receiver, sender = ctx.Pipe(duplex=False)
    process = ctx.Process(target=_guard_canary, args=(sender,))
    process.start()
    message = receiver.recv_bytes().decode("ascii")
    receiver.close()
    sender.close()
    process.join(timeout=10.0)
    assert not process.is_alive()
    assert "sqlite-import-refused" in message
    assert "open-refused" in message
    assert multiprocessing.active_children() == []

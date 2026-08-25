"""Bounded closeable channel tests for P7.7: backpressure, close order, peers."""

from __future__ import annotations

import asyncio
import threading
from collections.abc import Callable
from pathlib import Path
from typing import cast

import pytest

import paritygrid.application.execution as execution_package
import paritygrid.application.execution.channels as channels_module
from paritygrid.application.execution import (
    CHANNEL_KIND_ASSIGNMENT,
    CHANNEL_KIND_RESULT,
    CHANNEL_KIND_TELEMETRY,
    CHANNEL_KIND_WRITER,
    MAX_CHANNEL_CAPACITY,
    MIN_CHANNEL_CAPACITY,
    RUNNER_CONTRACT_VERSION,
    WORK_ASSIGNMENT_PROTOCOL,
    WORK_RESULT_PROTOCOL,
    BoundedChannel,
    ChannelClosedError,
    ChannelError,
    ChannelLoopError,
    ChannelSet,
    ChannelTimeoutError,
    ChannelUnknownOutcomeError,
    ChannelValidationError,
    ContractCleanupEvidence,
    ContractCleanupStatus,
    ContractDocument,
    ContractMetric,
    ContractOutcome,
    ControlGeneration,
    WorkAssignmentV1,
    WorkResultV1,
)

DEADLINE = "2026-08-21T12:00:00.000000Z"
FINGERPRINT = "0123456789abcdef" * 4
DESCRIPTOR = ContractDocument(items=(("operation", "normalize"), ("rows", 100)))
JOIN_PROBE_SECONDS = 0.05
JOIN_TIMEOUT_SECONDS = 10.0
PARK_TIMEOUT_SECONDS = 30.0


def _channel(
    kind: str = CHANNEL_KIND_RESULT,
    capacity: int = 2,
) -> BoundedChannel:
    return BoundedChannel(kind=kind, capacity=capacity)


def _assignment(index: int) -> WorkAssignmentV1:
    return WorkAssignmentV1(
        protocol=WORK_ASSIGNMENT_PROTOCOL,
        contract_version=RUNNER_CONTRACT_VERSION,
        plan_fingerprint=FINGERPRINT,
        run_id="run-channels",
        node_id="nod-etl",
        partition_key="region-eu",
        work_item_id=f"wi-a-{index:04d}",
        attempt_number=1,
        lease_fence=3,
        lease_owner="worker-1",
        control_generation=ControlGeneration(2),
        deadline_utc=DEADLINE,
        operation_descriptor=DESCRIPTOR,
        input_references=("artifact://inputs/one",),
        captured_settings_ref="settings://run-channels/v1",
    )


def _result(producer: int, index: int) -> WorkResultV1:
    return WorkResultV1(
        protocol=WORK_RESULT_PROTOCOL,
        contract_version=RUNNER_CONTRACT_VERSION,
        plan_fingerprint=FINGERPRINT,
        run_id="run-channels",
        node_id="nod-etl",
        partition_key="region-eu",
        work_item_id=f"wi-{producer}-{index:04d}",
        attempt_number=1,
        lease_fence=3,
        lease_owner="worker-1",
        control_generation=ControlGeneration(2),
        outcome=ContractOutcome.SUCCEEDED,
        metrics=(ContractMetric("rows", 100),),
        artifact_references=("artifact://outputs/one",),
        checkpoint_proposal=True,
        failure_detail=None,
        cleanup=ContractCleanupEvidence(
            status=ContractCleanupStatus.COMPLETED,
            actions=("release-lease",),
            idempotency_key=f"cleanup-{producer}-{index:04d}",
        ),
    )


def _telemetry(depth: int) -> ContractMetric:
    return ContractMetric("queue_depth", depth)


class _ParkedCall:
    """One blocking channel call parked in its own observable daemon thread."""

    __slots__ = ("error", "finished", "started", "thread", "value")

    def __init__(self, function: Callable[[], object]) -> None:
        self.started = threading.Event()
        self.finished = threading.Event()
        self.error: BaseException | None = None
        self.value: object = None
        self.thread = threading.Thread(target=self._run, args=(function,), daemon=True)

    def _run(self, function: Callable[[], object]) -> None:
        self.started.set()
        try:
            self.value = function()
        except BaseException as error:
            self.error = error
        finally:
            self.finished.set()

    def start(self) -> _ParkedCall:
        self.thread.start()
        return self

    def wait_started(self, timeout: float = JOIN_TIMEOUT_SECONDS) -> None:
        assert self.started.wait(timeout=timeout)

    def assert_parked(self, probe: float = JOIN_PROBE_SECONDS) -> None:
        self.thread.join(timeout=probe)
        assert self.thread.is_alive()

    def join(self, timeout: float = JOIN_TIMEOUT_SECONDS) -> None:
        self.thread.join(timeout=timeout)
        assert not self.thread.is_alive()

    def assert_ok(self) -> None:
        assert self.finished.is_set()
        assert self.error is None

    def assert_error[E: BaseException](self, error_type: type[E]) -> E:
        assert self.finished.is_set()
        assert isinstance(self.error, error_type)
        return self.error


class TestConstructionValidation:
    @pytest.mark.parametrize(
        "kind",
        [
            CHANNEL_KIND_ASSIGNMENT,
            CHANNEL_KIND_RESULT,
            CHANNEL_KIND_TELEMETRY,
            CHANNEL_KIND_WRITER,
        ],
    )
    def test_each_known_kind_is_accepted(self, kind: str) -> None:
        channel = BoundedChannel(kind=kind, capacity=1)
        assert channel.kind == kind

    @pytest.mark.parametrize("kind", ["unknown", "", "results", "ASSIGNMENT"])
    def test_unknown_kind_is_rejected(self, kind: str) -> None:
        with pytest.raises(ChannelValidationError):
            _channel(kind=kind)

    @pytest.mark.parametrize("kind", [7, b"result", None, True, 1.0])
    def test_non_text_kind_is_rejected(self, kind: object) -> None:
        with pytest.raises(ChannelValidationError):
            _channel(kind=cast(str, kind))

    @pytest.mark.parametrize("capacity", [0, -1, MIN_CHANNEL_CAPACITY - 1])
    def test_capacity_below_minimum_is_rejected(self, capacity: int) -> None:
        with pytest.raises(ChannelValidationError):
            _channel(capacity=capacity)

    @pytest.mark.parametrize("capacity", [MAX_CHANNEL_CAPACITY + 1, MAX_CHANNEL_CAPACITY * 8])
    def test_capacity_above_maximum_is_rejected(self, capacity: int) -> None:
        with pytest.raises(ChannelValidationError):
            _channel(capacity=capacity)

    @pytest.mark.parametrize("capacity", [True, False])
    def test_boolean_capacity_is_rejected(self, capacity: bool) -> None:
        with pytest.raises(ChannelValidationError):
            _channel(capacity=cast(int, capacity))

    @pytest.mark.parametrize("capacity", ["8", 8.0, None, (4,)])
    def test_non_integer_capacity_is_rejected(self, capacity: object) -> None:
        with pytest.raises(ChannelValidationError):
            _channel(capacity=cast(int, capacity))

    def test_capacity_bounds_are_accepted(self) -> None:
        assert _channel(capacity=MIN_CHANNEL_CAPACITY).capacity == MIN_CHANNEL_CAPACITY
        assert _channel(capacity=MAX_CHANNEL_CAPACITY).capacity == MAX_CHANNEL_CAPACITY

    def test_initial_observability_state(self) -> None:
        channel = _channel(kind=CHANNEL_KIND_TELEMETRY, capacity=4)
        assert channel.capacity == 4
        assert channel.accepted_count == 0
        assert channel.queued == 0
        assert channel.max_observed_queued == 0
        assert channel.is_closed is False
        assert channel.is_recovery_closed is False
        assert channel.drain() == ()

    def test_repr_reports_bounds_and_never_leaks_messages(self) -> None:
        channel = _channel(kind=CHANNEL_KIND_ASSIGNMENT, capacity=4)
        channel.send(_assignment(0), timeout=JOIN_TIMEOUT_SECONDS)
        text = repr(channel)
        assert CHANNEL_KIND_ASSIGNMENT in text
        assert "capacity=4" in text
        assert "queued=1" in text
        assert "wi-a-0000" not in text


class TestErrorContract:
    @pytest.mark.parametrize(
        "error_type",
        [
            ChannelValidationError,
            ChannelClosedError,
            ChannelTimeoutError,
            ChannelLoopError,
            ChannelUnknownOutcomeError,
        ],
    )
    def test_every_error_derives_from_the_channel_base(
        self, error_type: type[ChannelError]
    ) -> None:
        assert issubclass(error_type, ChannelError)
        assert issubclass(error_type, RuntimeError)

    def test_closed_error_defaults_to_zero_remaining(self) -> None:
        error = ChannelClosedError()
        assert error.accepted_remaining == 0

    def test_closed_error_carries_the_remaining_count(self) -> None:
        error = ChannelClosedError("custom", accepted_remaining=3)
        assert error.accepted_remaining == 3
        assert str(error) == "custom"

    def test_unknown_outcome_error_is_a_closed_error(self) -> None:
        error = ChannelUnknownOutcomeError("writer outcome unknown", accepted_remaining=2)
        assert isinstance(error, ChannelClosedError)
        assert error.accepted_remaining == 2


class TestMessageAndTimeoutValidation:
    def test_send_rejects_none_message(self) -> None:
        with pytest.raises(ChannelValidationError):
            _channel().send(None, timeout=JOIN_TIMEOUT_SECONDS)

    def test_try_send_rejects_none_message(self) -> None:
        with pytest.raises(ChannelValidationError):
            _channel().try_send(None)

    def test_send_async_rejects_none_message(self) -> None:
        channel = _channel()

        async def scenario() -> None:
            with pytest.raises(ChannelValidationError):
                await channel.send_async(None)

        asyncio.run(scenario())

    @pytest.mark.parametrize(
        "timeout",
        [True, "1", -0.5, float("inf"), float("nan")],
    )
    def test_send_rejects_invalid_timeout(self, timeout: object) -> None:
        with pytest.raises(ChannelValidationError):
            _channel().send(_result(0, 0), timeout=cast(float, timeout))

    @pytest.mark.parametrize("timeout", [False, "0", -1.0, float("inf")])
    def test_recv_rejects_invalid_timeout(self, timeout: object) -> None:
        with pytest.raises(ChannelValidationError):
            _channel().recv(timeout=cast(float, timeout))

    def test_send_accepts_integer_and_float_timeouts(self) -> None:
        channel = _channel(capacity=1)
        channel.send(_result(0, 0), timeout=1)
        assert channel.recv(timeout=1) == _result(0, 0)
        with pytest.raises(ChannelTimeoutError):
            channel.recv(timeout=0)


class TestFifoDelivery:
    def test_send_recv_preserves_fifo_order_across_many_messages(self) -> None:
        channel = _channel(kind=CHANNEL_KIND_ASSIGNMENT, capacity=8)
        expected = [_assignment(index) for index in range(16)]
        for index in range(8):
            channel.send(expected[index], timeout=JOIN_TIMEOUT_SECONDS)
            assert channel.recv(timeout=JOIN_TIMEOUT_SECONDS) == expected[index]
        for index in range(8, 16):
            channel.send(expected[index], timeout=JOIN_TIMEOUT_SECONDS)
        for index in range(8, 16):
            assert channel.recv(timeout=JOIN_TIMEOUT_SECONDS) == expected[index]
        assert channel.accepted_count == 16

    def test_fifo_order_with_interleaved_operations(self) -> None:
        channel = _channel(capacity=3)
        first, second, third = _result(0, 0), _result(0, 1), _result(0, 2)
        channel.send(first, timeout=JOIN_TIMEOUT_SECONDS)
        channel.send(second, timeout=JOIN_TIMEOUT_SECONDS)
        assert channel.recv(timeout=JOIN_TIMEOUT_SECONDS) == first
        channel.send(third, timeout=JOIN_TIMEOUT_SECONDS)
        assert channel.try_recv() == second
        assert channel.recv(timeout=JOIN_TIMEOUT_SECONDS) == third

    def test_try_recv_returns_oldest_first(self) -> None:
        channel = _channel(capacity=4)
        expected = [_telemetry(depth) for depth in range(4)]
        for message in expected:
            assert channel.try_send(message)
        assert [channel.try_recv() for _ in range(4)] == expected

    def test_drain_returns_fifo_order(self) -> None:
        channel = _channel(kind=CHANNEL_KIND_WRITER, capacity=4)
        expected = [_result(1, index) for index in range(4)]
        for message in expected:
            channel.send(message, timeout=JOIN_TIMEOUT_SECONDS)
        assert channel.drain() == tuple(expected)

    def test_recv_returns_the_exact_envelope_instance(self) -> None:
        channel = _channel()
        message = _assignment(7)
        channel.send(message, timeout=JOIN_TIMEOUT_SECONDS)
        assert channel.recv(timeout=JOIN_TIMEOUT_SECONDS) is message

    def test_two_receivers_each_receive_one_message(self) -> None:
        channel = _channel(capacity=4)
        first, second = _result(0, 0), _result(1, 0)
        left = _ParkedCall(lambda: channel.recv(timeout=PARK_TIMEOUT_SECONDS)).start()
        right = _ParkedCall(lambda: channel.recv(timeout=PARK_TIMEOUT_SECONDS)).start()
        left.wait_started()
        right.wait_started()
        left.assert_parked()
        right.assert_parked()
        channel.send(first, timeout=JOIN_TIMEOUT_SECONDS)
        channel.send(second, timeout=JOIN_TIMEOUT_SECONDS)
        left.join()
        right.join()
        left.assert_ok()
        right.assert_ok()
        assert {left.value, right.value} == {first, second}


class TestNonBlockingOperations:
    def test_try_send_succeeds_while_space_remains(self) -> None:
        assert _channel(capacity=2).try_send(_result(0, 0)) is True

    def test_try_send_fills_the_capacity(self) -> None:
        channel = _channel(capacity=3)
        for index in range(3):
            assert channel.try_send(_result(0, index))
        assert channel.queued == 3

    def test_try_send_reports_full_without_waiting(self) -> None:
        channel = _channel(capacity=1)
        assert channel.try_send(_result(0, 0))
        assert channel.try_send(_result(0, 1)) is False
        assert channel.accepted_count == 1

    def test_try_send_succeeds_after_recv_frees_space(self) -> None:
        channel = _channel(capacity=1)
        channel.send(_result(0, 0), timeout=JOIN_TIMEOUT_SECONDS)
        assert channel.recv(timeout=JOIN_TIMEOUT_SECONDS) == _result(0, 0)
        assert channel.try_send(_result(0, 1))

    def test_try_send_on_closed_channel_raises(self) -> None:
        channel = _channel()
        channel.close()
        with pytest.raises(ChannelClosedError) as raised:
            channel.try_send(_result(0, 0))
        assert raised.value.accepted_remaining == 0

    def test_try_recv_on_empty_open_channel_returns_none(self) -> None:
        assert _channel().try_recv() is None

    def test_try_recv_returns_messages_until_empty(self) -> None:
        channel = _channel(capacity=2)
        channel.send(_result(0, 0), timeout=JOIN_TIMEOUT_SECONDS)
        channel.send(_result(0, 1), timeout=JOIN_TIMEOUT_SECONDS)
        assert channel.try_recv() == _result(0, 0)
        assert channel.try_recv() == _result(0, 1)
        assert channel.try_recv() is None

    def test_try_recv_on_closed_and_empty_channel_raises(self) -> None:
        channel = _channel()
        channel.close()
        with pytest.raises(ChannelClosedError) as raised:
            channel.try_recv()
        assert raised.value.accepted_remaining == 0

    def test_try_recv_on_closed_channel_delivers_remaining_first(self) -> None:
        channel = _channel(capacity=2)
        channel.send(_result(2, 0), timeout=JOIN_TIMEOUT_SECONDS)
        channel.close()
        assert channel.try_recv() == _result(2, 0)
        with pytest.raises(ChannelClosedError):
            channel.try_recv()

    def test_try_operations_never_block_against_parked_peers(self) -> None:
        full = _channel(kind=CHANNEL_KIND_WRITER, capacity=1)
        assert full.try_send(_result(0, 0))
        sender = _ParkedCall(lambda: full.send(_result(0, 1), timeout=PARK_TIMEOUT_SECONDS)).start()
        sender.wait_started()
        sender.assert_parked()
        empty = _channel(kind=CHANNEL_KIND_ASSIGNMENT, capacity=1)
        receiver = _ParkedCall(lambda: empty.recv(timeout=PARK_TIMEOUT_SECONDS)).start()
        receiver.wait_started()
        receiver.assert_parked()
        assert full.try_send(_result(0, 2)) is False
        assert empty.try_recv() is None


class TestBlockingReceive:
    def test_recv_blocks_until_a_peer_sends(self) -> None:
        channel = _channel()
        message = _result(3, 1)
        receiver = _ParkedCall(lambda: channel.recv(timeout=PARK_TIMEOUT_SECONDS)).start()
        receiver.wait_started()
        receiver.assert_parked()
        channel.send(message, timeout=JOIN_TIMEOUT_SECONDS)
        receiver.join()
        receiver.assert_ok()
        assert receiver.value is message

    def test_recv_times_out_when_no_peer_sends(self) -> None:
        with pytest.raises(ChannelTimeoutError):
            _channel().recv(timeout=JOIN_PROBE_SECONDS)

    def test_recv_zero_timeout_is_immediate(self) -> None:
        with pytest.raises(ChannelTimeoutError):
            _channel().recv(timeout=0.0)

    def test_recv_returns_immediately_when_a_message_is_present(self) -> None:
        channel = _channel()
        channel.send(_result(0, 0), timeout=JOIN_TIMEOUT_SECONDS)
        assert channel.recv(timeout=JOIN_TIMEOUT_SECONDS) == _result(0, 0)

    def test_recv_without_timeout_blocks_until_close(self) -> None:
        channel = _channel()
        receiver = _ParkedCall(lambda: channel.recv()).start()
        receiver.wait_started()
        receiver.assert_parked()
        channel.close()
        receiver.join()
        error = receiver.assert_error(ChannelClosedError)
        assert error.accepted_remaining == 0


class TestBackpressure:
    def test_writer_bound_parks_the_overflow_send(self) -> None:
        channel = _channel(kind=CHANNEL_KIND_WRITER, capacity=3)
        for index in range(3):
            channel.send(_result(0, index), timeout=JOIN_TIMEOUT_SECONDS)
        sender = _ParkedCall(
            lambda: channel.send(_result(0, 3), timeout=PARK_TIMEOUT_SECONDS)
        ).start()
        sender.wait_started()
        sender.assert_parked()
        assert channel.queued == 3
        assert channel.accepted_count == 3
        assert channel.max_observed_queued == 3

    def test_memory_does_not_grow_with_offered_work_while_gated(self) -> None:
        capacity = 4
        channel = _channel(kind=CHANNEL_KIND_WRITER, capacity=capacity)
        for index in range(capacity):
            channel.send(_result(0, index), timeout=JOIN_TIMEOUT_SECONDS)
        sender = _ParkedCall(
            lambda: channel.send(_result(0, capacity), timeout=PARK_TIMEOUT_SECONDS)
        ).start()
        sender.wait_started()
        sender.assert_parked()
        rejected = 0
        for index in range(capacity + 1, capacity * 3):
            if not channel.try_send(_result(0, index)):
                rejected += 1
        assert rejected == 2 * capacity - 1
        assert channel.queued == capacity
        assert channel.accepted_count == capacity
        assert channel.max_observed_queued == capacity
        sender.assert_parked()

    def test_releasing_the_consumer_completes_the_parked_send(self) -> None:
        channel = _channel(kind=CHANNEL_KIND_WRITER, capacity=2)
        for index in range(2):
            channel.send(_result(0, index), timeout=JOIN_TIMEOUT_SECONDS)
        sender = _ParkedCall(
            lambda: channel.send(_result(0, 2), timeout=PARK_TIMEOUT_SECONDS)
        ).start()
        sender.wait_started()
        sender.assert_parked()
        assert channel.recv(timeout=JOIN_TIMEOUT_SECONDS) == _result(0, 0)
        sender.join()
        sender.assert_ok()
        assert channel.accepted_count == 3
        assert channel.queued == 2

    def test_backpressure_release_preserves_fifo_order(self) -> None:
        channel = _channel(kind=CHANNEL_KIND_WRITER, capacity=2)
        for index in range(2):
            channel.send(_result(0, index), timeout=JOIN_TIMEOUT_SECONDS)
        sender = _ParkedCall(
            lambda: channel.send(_result(0, 2), timeout=PARK_TIMEOUT_SECONDS)
        ).start()
        sender.wait_started()
        sender.assert_parked()
        channel.recv(timeout=JOIN_TIMEOUT_SECONDS)
        sender.join()
        sender.assert_ok()
        drained = channel.drain()
        assert drained == (_result(0, 1), _result(0, 2))

    def test_drain_frees_space_and_completes_a_parked_sender(self) -> None:
        channel = _channel(kind=CHANNEL_KIND_RESULT, capacity=1)
        assert channel.try_send(_result(0, 0))
        overflow = _result(0, 1)
        sender = _ParkedCall(lambda: channel.send(overflow, timeout=PARK_TIMEOUT_SECONDS)).start()
        sender.wait_started()
        sender.assert_parked()
        assert channel.drain() == (_result(0, 0),)
        sender.join()
        sender.assert_ok()
        assert channel.queued == 1
        assert channel.try_recv() == overflow

    def test_send_times_out_while_the_consumer_stays_blocked(self) -> None:
        channel = _channel(kind=CHANNEL_KIND_WRITER, capacity=1)
        channel.send(_result(0, 0), timeout=JOIN_TIMEOUT_SECONDS)
        with pytest.raises(ChannelTimeoutError):
            channel.send(_result(0, 1), timeout=JOIN_PROBE_SECONDS)
        assert channel.queued == 1
        assert channel.accepted_count == 1

    def test_send_without_timeout_blocks_until_space(self) -> None:
        channel = _channel(kind=CHANNEL_KIND_WRITER, capacity=1)
        channel.send(_result(0, 0), timeout=JOIN_TIMEOUT_SECONDS)
        sender = _ParkedCall(lambda: channel.send(_result(0, 1))).start()
        sender.wait_started()
        sender.assert_parked()
        assert channel.recv(timeout=JOIN_TIMEOUT_SECONDS) == _result(0, 0)
        sender.join()
        sender.assert_ok()
        assert channel.try_recv() == _result(0, 1)


class TestClose:
    def test_close_is_idempotent(self) -> None:
        channel = _channel()
        channel.close()
        channel.close()
        assert channel.is_closed is True

    def test_repeated_close_is_safe(self) -> None:
        channel = _channel()
        channel.send(_result(0, 0), timeout=JOIN_TIMEOUT_SECONDS)
        for _ in range(3):
            channel.close()
        assert channel.is_closed is True
        assert channel.drain() == (_result(0, 0),)

    def test_close_releases_a_blocked_sender_with_remaining_count(self) -> None:
        channel = _channel(kind=CHANNEL_KIND_WRITER, capacity=1)
        channel.send(_result(0, 0), timeout=JOIN_TIMEOUT_SECONDS)
        sender = _ParkedCall(
            lambda: channel.send(_result(0, 1), timeout=PARK_TIMEOUT_SECONDS)
        ).start()
        sender.wait_started()
        sender.assert_parked()
        channel.close()
        sender.join()
        error = sender.assert_error(ChannelClosedError)
        assert error.accepted_remaining == 1
        assert channel.drain() == (_result(0, 0),)
        assert channel.accepted_count == 1

    def test_close_releases_a_blocked_receiver_when_empty(self) -> None:
        channel = _channel()
        receiver = _ParkedCall(lambda: channel.recv(timeout=PARK_TIMEOUT_SECONDS)).start()
        receiver.wait_started()
        receiver.assert_parked()
        channel.close()
        receiver.join()
        error = receiver.assert_error(ChannelClosedError)
        assert error.accepted_remaining == 0

    def test_close_releases_a_blocked_sender_and_receiver_together(self) -> None:
        sender_channel = _channel(kind=CHANNEL_KIND_WRITER, capacity=1)
        sender_channel.send(_result(0, 0), timeout=JOIN_TIMEOUT_SECONDS)
        sender = _ParkedCall(
            lambda: sender_channel.send(_result(0, 1), timeout=PARK_TIMEOUT_SECONDS)
        ).start()
        sender.wait_started()
        sender.assert_parked()
        receiver_channel = _channel(kind=CHANNEL_KIND_ASSIGNMENT, capacity=1)
        receiver = _ParkedCall(lambda: receiver_channel.recv(timeout=PARK_TIMEOUT_SECONDS)).start()
        receiver.wait_started()
        receiver.assert_parked()
        sender_channel.close()
        receiver_channel.close()
        sender.join()
        receiver.join()
        sender.assert_error(ChannelClosedError)
        receiver.assert_error(ChannelClosedError)

    def test_close_releases_every_parked_sender(self) -> None:
        channel = _channel(kind=CHANNEL_KIND_RESULT, capacity=1)
        channel.send(_result(0, 0), timeout=JOIN_TIMEOUT_SECONDS)
        senders = [
            _ParkedCall(
                lambda index=index: channel.send(_result(0, index), timeout=PARK_TIMEOUT_SECONDS)
            ).start()
            for index in range(1, 4)
        ]
        for sender in senders:
            sender.wait_started()
            sender.assert_parked()
        channel.close()
        for sender in senders:
            sender.join()
            error = sender.assert_error(ChannelClosedError)
            assert error.accepted_remaining == 1

    def test_recv_delivers_remaining_messages_after_close(self) -> None:
        channel = _channel(capacity=3)
        for index in range(3):
            channel.send(_result(0, index), timeout=JOIN_TIMEOUT_SECONDS)
        channel.close()
        for index in range(3):
            assert channel.recv(timeout=JOIN_TIMEOUT_SECONDS) == _result(0, index)

    def test_recv_raises_once_closed_and_drained(self) -> None:
        channel = _channel()
        channel.send(_result(0, 0), timeout=JOIN_TIMEOUT_SECONDS)
        channel.close()
        assert channel.recv(timeout=JOIN_TIMEOUT_SECONDS) == _result(0, 0)
        with pytest.raises(ChannelClosedError) as raised:
            channel.recv(timeout=JOIN_TIMEOUT_SECONDS)
        assert raised.value.accepted_remaining == 0

    def test_close_before_send_raises_with_remaining(self) -> None:
        channel = _channel(capacity=2)
        channel.send(_result(0, 0), timeout=JOIN_TIMEOUT_SECONDS)
        channel.close()
        with pytest.raises(ChannelClosedError) as raised:
            channel.send(_result(0, 1), timeout=JOIN_TIMEOUT_SECONDS)
        assert raised.value.accepted_remaining == 1

    def test_close_then_drain_returns_accepted_messages(self) -> None:
        channel = _channel(kind=CHANNEL_KIND_WRITER, capacity=2)
        channel.send(_result(0, 0), timeout=JOIN_TIMEOUT_SECONDS)
        channel.send(_result(0, 1), timeout=JOIN_TIMEOUT_SECONDS)
        channel.close()
        assert channel.drain() == (_result(0, 0), _result(0, 1))

    def test_drain_before_close_works(self) -> None:
        channel = _channel()
        channel.send(_result(0, 0), timeout=JOIN_TIMEOUT_SECONDS)
        assert channel.drain() == (_result(0, 0),)
        channel.close()
        assert channel.drain() == ()

    def test_second_drain_is_empty(self) -> None:
        channel = _channel()
        channel.send(_result(0, 0), timeout=JOIN_TIMEOUT_SECONDS)
        assert channel.drain() == (_result(0, 0),)
        assert channel.drain() == ()
        assert channel.queued == 0

    def test_second_close_does_not_disturb_released_waiters(self) -> None:
        channel = _channel()
        receiver = _ParkedCall(lambda: channel.recv(timeout=PARK_TIMEOUT_SECONDS)).start()
        receiver.wait_started()
        receiver.assert_parked()
        channel.close()
        receiver.join()
        first_error = receiver.assert_error(ChannelClosedError)
        channel.close()
        assert receiver.error is first_error
        assert not receiver.thread.is_alive()

    def test_try_send_after_close_reports_remaining(self) -> None:
        channel = _channel(kind=CHANNEL_KIND_WRITER, capacity=2)
        channel.send(_result(0, 0), timeout=JOIN_TIMEOUT_SECONDS)
        channel.close()
        with pytest.raises(ChannelClosedError) as raised:
            channel.try_send(_result(0, 1))
        assert raised.value.accepted_remaining == 1
        assert channel.drain() == (_result(0, 0),)


class TestUnknownWriterOutcome:
    def test_mark_then_send_raises_unknown_outcome(self) -> None:
        channel = _channel(kind=CHANNEL_KIND_WRITER, capacity=2)
        channel.mark_unknown_outcome("sqlite commit outcome unknown")
        with pytest.raises(ChannelUnknownOutcomeError) as raised:
            channel.send(_result(0, 0), timeout=JOIN_TIMEOUT_SECONDS)
        assert raised.value.accepted_remaining == 0
        assert channel.is_recovery_closed is True
        assert channel.is_closed is False

    def test_mark_then_try_send_raises_unknown_outcome(self) -> None:
        channel = _channel(kind=CHANNEL_KIND_WRITER, capacity=2)
        channel.send(_result(0, 0), timeout=JOIN_TIMEOUT_SECONDS)
        channel.mark_unknown_outcome("writer timeout")
        with pytest.raises(ChannelUnknownOutcomeError) as raised:
            channel.try_send(_result(0, 1))
        assert raised.value.accepted_remaining == 1

    def test_mark_keeps_accepted_messages_drainable(self) -> None:
        channel = _channel(kind=CHANNEL_KIND_WRITER, capacity=2)
        channel.send(_result(0, 0), timeout=JOIN_TIMEOUT_SECONDS)
        channel.send(_result(0, 1), timeout=JOIN_TIMEOUT_SECONDS)
        channel.mark_unknown_outcome("outcome unknown")
        assert channel.drain() == (_result(0, 0), _result(0, 1))
        assert channel.drain() == ()
        assert channel.accepted_count == 2

    def test_recv_delivers_remaining_after_mark_until_released(self) -> None:
        channel = _channel(kind=CHANNEL_KIND_WRITER, capacity=2)
        channel.send(_result(0, 0), timeout=JOIN_TIMEOUT_SECONDS)
        channel.mark_unknown_outcome("outcome unknown")
        assert channel.recv(timeout=JOIN_TIMEOUT_SECONDS) == _result(0, 0)
        with pytest.raises(ChannelClosedError) as raised:
            channel.recv(timeout=JOIN_TIMEOUT_SECONDS)
        assert raised.value.accepted_remaining == 0

    def test_mark_releases_a_blocked_receiver_on_empty(self) -> None:
        channel = _channel(kind=CHANNEL_KIND_WRITER, capacity=1)
        receiver = _ParkedCall(lambda: channel.recv(timeout=PARK_TIMEOUT_SECONDS)).start()
        receiver.wait_started()
        receiver.assert_parked()
        channel.mark_unknown_outcome("outcome unknown")
        receiver.join()
        receiver.assert_error(ChannelClosedError)

    def test_mark_releases_a_blocked_sender(self) -> None:
        channel = _channel(kind=CHANNEL_KIND_WRITER, capacity=1)
        channel.send(_result(0, 0), timeout=JOIN_TIMEOUT_SECONDS)
        sender = _ParkedCall(
            lambda: channel.send(_result(0, 1), timeout=PARK_TIMEOUT_SECONDS)
        ).start()
        sender.wait_started()
        sender.assert_parked()
        channel.mark_unknown_outcome("outcome unknown")
        sender.join()
        error = sender.assert_error(ChannelUnknownOutcomeError)
        assert error.accepted_remaining == 1
        assert channel.drain() == (_result(0, 0),)

    def test_close_still_works_after_mark(self) -> None:
        channel = _channel(kind=CHANNEL_KIND_WRITER, capacity=1)
        channel.send(_result(0, 0), timeout=JOIN_TIMEOUT_SECONDS)
        channel.mark_unknown_outcome("outcome unknown")
        channel.close()
        assert channel.is_closed is True
        assert channel.is_recovery_closed is True
        with pytest.raises(ChannelClosedError):
            channel.send(_result(0, 1), timeout=JOIN_TIMEOUT_SECONDS)
        with pytest.raises(ChannelClosedError):
            channel.try_send(_result(0, 1))
        assert channel.drain() == (_result(0, 0),)

    def test_mark_is_idempotent(self) -> None:
        channel = _channel(kind=CHANNEL_KIND_WRITER, capacity=1)
        channel.mark_unknown_outcome("first reason")
        channel.mark_unknown_outcome("second reason")
        assert channel.is_recovery_closed is True
        with pytest.raises(ChannelUnknownOutcomeError):
            channel.try_send(_result(0, 0))

    def test_mark_before_any_send_accepts_nothing(self) -> None:
        channel = _channel(kind=CHANNEL_KIND_WRITER, capacity=1)
        channel.mark_unknown_outcome("outcome unknown")
        assert channel.accepted_count == 0
        assert channel.queued == 0
        assert channel.max_observed_queued == 0

    def test_mark_then_send_async_raises_unknown_outcome(self) -> None:
        channel = _channel(kind=CHANNEL_KIND_WRITER, capacity=1)
        channel.mark_unknown_outcome("outcome unknown")

        async def scenario() -> None:
            with pytest.raises(ChannelUnknownOutcomeError):
                await channel.send_async(_result(0, 0))

        asyncio.run(scenario())

    @pytest.mark.parametrize("reason", [7, b"reason", None, True])
    def test_mark_rejects_non_text_reason(self, reason: object) -> None:
        with pytest.raises(ChannelValidationError):
            _channel(kind=CHANNEL_KIND_WRITER, capacity=1).mark_unknown_outcome(cast(str, reason))

    @pytest.mark.parametrize("reason", ["", "r" * 257])
    def test_mark_rejects_out_of_range_reason_length(self, reason: str) -> None:
        with pytest.raises(ChannelValidationError):
            _channel(kind=CHANNEL_KIND_WRITER, capacity=1).mark_unknown_outcome(reason)

    @pytest.mark.parametrize("reason", ["bad\nreason", "bad\treason", "raison €"])
    def test_mark_rejects_non_printable_reason(self, reason: str) -> None:
        with pytest.raises(ChannelValidationError):
            _channel(kind=CHANNEL_KIND_WRITER, capacity=1).mark_unknown_outcome(reason)

    def test_mark_accepts_boundary_reason_lengths(self) -> None:
        channel = _channel(kind=CHANNEL_KIND_WRITER, capacity=1)
        channel.mark_unknown_outcome("r")
        channel.mark_unknown_outcome("r" * 256)
        assert channel.is_recovery_closed is True


class TestChannelSet:
    def test_builds_four_channels_with_expected_kinds(self) -> None:
        channel_set = ChannelSet(
            assignment_capacity=1,
            result_capacity=2,
            telemetry_capacity=3,
            writer_capacity=4,
        )
        assert channel_set.assignment.kind == CHANNEL_KIND_ASSIGNMENT
        assert channel_set.result.kind == CHANNEL_KIND_RESULT
        assert channel_set.telemetry.kind == CHANNEL_KIND_TELEMETRY
        assert channel_set.writer.kind == CHANNEL_KIND_WRITER

    def test_channel_capacities_are_configured(self) -> None:
        channel_set = ChannelSet(
            assignment_capacity=1,
            result_capacity=2,
            telemetry_capacity=3,
            writer_capacity=4,
        )
        assert channel_set.assignment.capacity == 1
        assert channel_set.result.capacity == 2
        assert channel_set.telemetry.capacity == 3
        assert channel_set.writer.capacity == 4

    @pytest.mark.parametrize(
        "kwargs",
        [
            {
                "assignment_capacity": 0,
                "result_capacity": 1,
                "telemetry_capacity": 1,
                "writer_capacity": 1,
            },
            {
                "assignment_capacity": 1,
                "result_capacity": -2,
                "telemetry_capacity": 1,
                "writer_capacity": 1,
            },
            {
                "assignment_capacity": 1,
                "result_capacity": 1,
                "telemetry_capacity": True,
                "writer_capacity": 1,
            },
            {
                "assignment_capacity": 1,
                "result_capacity": 1,
                "telemetry_capacity": 1,
                "writer_capacity": MAX_CHANNEL_CAPACITY + 1,
            },
        ],
    )
    def test_each_capacity_is_validated(self, kwargs: dict[str, int]) -> None:
        with pytest.raises(ChannelValidationError):
            ChannelSet(**kwargs)

    def test_channels_are_independent(self) -> None:
        channel_set = ChannelSet(
            assignment_capacity=1,
            result_capacity=1,
            telemetry_capacity=1,
            writer_capacity=1,
        )
        channel_set.assignment.send(_assignment(0), timeout=JOIN_TIMEOUT_SECONDS)
        channel_set.result.send(_result(0, 0), timeout=JOIN_TIMEOUT_SECONDS)
        channel_set.telemetry.send(_telemetry(1), timeout=JOIN_TIMEOUT_SECONDS)
        channel_set.writer.send(_result(9, 9), timeout=JOIN_TIMEOUT_SECONDS)
        assert channel_set.assignment.recv(timeout=JOIN_TIMEOUT_SECONDS) == _assignment(0)
        assert channel_set.result.recv(timeout=JOIN_TIMEOUT_SECONDS) == _result(0, 0)
        assert channel_set.telemetry.recv(timeout=JOIN_TIMEOUT_SECONDS) == _telemetry(1)
        assert channel_set.writer.recv(timeout=JOIN_TIMEOUT_SECONDS) == _result(9, 9)

    def test_close_all_closes_all_four_channels(self) -> None:
        channel_set = ChannelSet(
            assignment_capacity=1,
            result_capacity=1,
            telemetry_capacity=1,
            writer_capacity=1,
        )
        channel_set.close_all()
        assert channel_set.assignment.is_closed is True
        assert channel_set.result.is_closed is True
        assert channel_set.telemetry.is_closed is True
        assert channel_set.writer.is_closed is True

    def test_close_all_is_idempotent(self) -> None:
        channel_set = ChannelSet(
            assignment_capacity=1,
            result_capacity=1,
            telemetry_capacity=1,
            writer_capacity=1,
        )
        channel_set.close_all()
        channel_set.close_all()
        for channel in (
            channel_set.assignment,
            channel_set.result,
            channel_set.telemetry,
            channel_set.writer,
        ):
            assert channel.is_closed is True

    def test_close_all_order_is_writer_result_assignment_telemetry(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        channel_set = ChannelSet(
            assignment_capacity=1,
            result_capacity=1,
            telemetry_capacity=1,
            writer_capacity=1,
        )
        closed_order: list[str] = []
        original_close = BoundedChannel.close

        def recording_close(channel: BoundedChannel) -> None:
            closed_order.append(channel.kind)
            original_close(channel)

        monkeypatch.setattr(BoundedChannel, "close", recording_close)
        channel_set.close_all()
        assert closed_order == [
            CHANNEL_KIND_WRITER,
            CHANNEL_KIND_RESULT,
            CHANNEL_KIND_ASSIGNMENT,
            CHANNEL_KIND_TELEMETRY,
        ]

    def test_close_all_releases_blocked_peers_without_deadlock(self) -> None:
        channel_set = ChannelSet(
            assignment_capacity=1,
            result_capacity=1,
            telemetry_capacity=1,
            writer_capacity=1,
        )
        assert channel_set.writer.try_send(_result(0, 0))
        sender = _ParkedCall(
            lambda: channel_set.writer.send(_result(0, 1), timeout=PARK_TIMEOUT_SECONDS)
        ).start()
        receiver = _ParkedCall(
            lambda: channel_set.assignment.recv(timeout=PARK_TIMEOUT_SECONDS)
        ).start()
        sender.wait_started()
        receiver.wait_started()
        sender.assert_parked()
        receiver.assert_parked()
        channel_set.close_all()
        sender.join()
        receiver.join()
        sender.assert_error(ChannelClosedError)
        receiver.assert_error(ChannelClosedError)

    def test_drain_results_returns_result_channel_items(self) -> None:
        channel_set = ChannelSet(
            assignment_capacity=1,
            result_capacity=2,
            telemetry_capacity=1,
            writer_capacity=1,
        )
        channel_set.result.send(_result(0, 0), timeout=JOIN_TIMEOUT_SECONDS)
        channel_set.result.send(_result(0, 1), timeout=JOIN_TIMEOUT_SECONDS)
        channel_set.writer.send(_result(9, 0), timeout=JOIN_TIMEOUT_SECONDS)
        assert channel_set.drain_results() == (_result(0, 0), _result(0, 1))
        assert channel_set.drain_results() == ()
        assert channel_set.writer.queued == 1

    def test_drain_writer_returns_writer_channel_items(self) -> None:
        channel_set = ChannelSet(
            assignment_capacity=1,
            result_capacity=1,
            telemetry_capacity=1,
            writer_capacity=2,
        )
        channel_set.writer.send(_result(9, 0), timeout=JOIN_TIMEOUT_SECONDS)
        channel_set.writer.send(_result(9, 1), timeout=JOIN_TIMEOUT_SECONDS)
        channel_set.result.send(_result(0, 0), timeout=JOIN_TIMEOUT_SECONDS)
        assert channel_set.drain_writer() == (_result(9, 0), _result(9, 1))
        assert channel_set.drain_writer() == ()
        assert channel_set.result.queued == 1

    def test_mark_writer_unknown_outcome_only_affects_the_writer_channel(self) -> None:
        channel_set = ChannelSet(
            assignment_capacity=1,
            result_capacity=1,
            telemetry_capacity=1,
            writer_capacity=1,
        )
        channel_set.mark_writer_unknown_outcome("writer outcome unknown")
        assert channel_set.writer.is_recovery_closed is True
        assert channel_set.assignment.is_recovery_closed is False
        assert channel_set.result.is_recovery_closed is False
        assert channel_set.telemetry.is_recovery_closed is False
        with pytest.raises(ChannelUnknownOutcomeError):
            channel_set.writer.try_send(_result(9, 0))
        assert channel_set.assignment.try_send(_assignment(0))
        assert channel_set.result.try_send(_result(0, 0))
        assert channel_set.telemetry.try_send(_telemetry(1))

    def test_snapshots_shape_and_order(self) -> None:
        channel_set = ChannelSet(
            assignment_capacity=1,
            result_capacity=2,
            telemetry_capacity=3,
            writer_capacity=4,
        )
        assert channel_set.snapshots() == (
            (CHANNEL_KIND_ASSIGNMENT, 1, 0, 0),
            (CHANNEL_KIND_RESULT, 2, 0, 0),
            (CHANNEL_KIND_TELEMETRY, 3, 0, 0),
            (CHANNEL_KIND_WRITER, 4, 0, 0),
        )

    def test_snapshots_reflect_queue_activity(self) -> None:
        channel_set = ChannelSet(
            assignment_capacity=1,
            result_capacity=2,
            telemetry_capacity=2,
            writer_capacity=1,
        )
        channel_set.result.send(_result(0, 0), timeout=JOIN_TIMEOUT_SECONDS)
        channel_set.result.send(_result(0, 1), timeout=JOIN_TIMEOUT_SECONDS)
        channel_set.telemetry.send(_telemetry(1), timeout=JOIN_TIMEOUT_SECONDS)
        snapshots = channel_set.snapshots()
        assert snapshots[1] == (CHANNEL_KIND_RESULT, 2, 2, 2)
        assert snapshots[2] == (CHANNEL_KIND_TELEMETRY, 2, 1, 1)
        assert snapshots[0] == (CHANNEL_KIND_ASSIGNMENT, 1, 0, 0)
        assert snapshots[3] == (CHANNEL_KIND_WRITER, 1, 0, 0)

    def test_channel_set_repr_reports_only_bounded_facts(self) -> None:
        channel_set = ChannelSet(
            assignment_capacity=1,
            result_capacity=1,
            telemetry_capacity=1,
            writer_capacity=1,
        )
        channel_set.writer.send(_result(9, 0), timeout=JOIN_TIMEOUT_SECONDS)
        text = repr(channel_set)
        assert "writer=1" in text
        assert "wi-9-0000" not in text


class TestEventLoopGuard:
    def test_sync_send_inside_a_running_loop_is_rejected(self) -> None:
        channel = _channel()

        async def scenario() -> None:
            with pytest.raises(ChannelLoopError):
                channel.send(_result(0, 0), timeout=JOIN_TIMEOUT_SECONDS)

        asyncio.run(scenario())

    def test_sync_recv_inside_a_running_loop_is_rejected(self) -> None:
        channel = _channel()

        async def scenario() -> None:
            with pytest.raises(ChannelLoopError):
                channel.recv(timeout=JOIN_TIMEOUT_SECONDS)

        asyncio.run(scenario())

    def test_try_send_inside_a_running_loop_is_rejected(self) -> None:
        channel = _channel()

        async def scenario() -> None:
            with pytest.raises(ChannelLoopError):
                channel.try_send(_result(0, 0))

        asyncio.run(scenario())

    def test_try_recv_inside_a_running_loop_is_rejected(self) -> None:
        channel = _channel()

        async def scenario() -> None:
            with pytest.raises(ChannelLoopError):
                channel.try_recv()

        asyncio.run(scenario())

    def test_drain_inside_a_running_loop_is_rejected(self) -> None:
        channel = _channel()

        async def scenario() -> None:
            with pytest.raises(ChannelLoopError):
                channel.drain()

        asyncio.run(scenario())

    def test_close_works_inside_a_running_loop(self) -> None:
        channel = _channel()
        channel.send(_result(0, 0), timeout=JOIN_TIMEOUT_SECONDS)

        async def scenario() -> None:
            channel.close()

        asyncio.run(scenario())
        assert channel.is_closed is True

    def test_mark_unknown_outcome_works_inside_a_running_loop(self) -> None:
        channel = _channel(kind=CHANNEL_KIND_WRITER, capacity=1)

        async def scenario() -> None:
            channel.mark_unknown_outcome("outcome unknown")

        asyncio.run(scenario())
        assert channel.is_recovery_closed is True

    def test_observability_properties_work_inside_a_running_loop(self) -> None:
        channel = _channel()
        channel.send(_result(0, 0), timeout=JOIN_TIMEOUT_SECONDS)

        async def scenario() -> tuple[int, int, int]:
            return (channel.queued, channel.accepted_count, channel.max_observed_queued)

        assert asyncio.run(scenario()) == (1, 1, 1)


class TestAsyncWrappers:
    def test_send_async_completes_immediately_with_space(self) -> None:
        channel = _channel()
        message = _result(0, 0)

        async def scenario() -> None:
            await channel.send_async(message)

        asyncio.run(scenario())
        assert channel.try_recv() == message

    def test_recv_async_completes_immediately_with_a_message(self) -> None:
        channel = _channel()
        channel.send(_result(0, 0), timeout=JOIN_TIMEOUT_SECONDS)

        async def scenario() -> object:
            return await channel.recv_async()

        assert asyncio.run(scenario()) == _result(0, 0)

    def test_send_async_completes_after_a_peer_frees_space(self) -> None:
        channel = _channel(capacity=1)
        first = _result(0, 0)
        second = _result(0, 1)
        assert channel.try_send(first)

        async def scenario() -> object:
            task = asyncio.create_task(channel.send_async(second))
            await asyncio.sleep(0)
            delivered = await channel.recv_async()
            await task
            return delivered

        delivered = asyncio.run(scenario())
        assert delivered == first
        assert channel.queued == 1
        assert channel.try_recv() == second
        assert channel.accepted_count == 2

    def test_recv_async_completes_after_a_peer_sends(self) -> None:
        channel = _channel()
        message = _result(2, 3)

        async def scenario() -> object:
            task = asyncio.create_task(channel.recv_async())
            await asyncio.sleep(0)
            await channel.send_async(message)
            return await task

        assert asyncio.run(scenario()) == message
        assert channel.queued == 0
        assert channel.accepted_count == 1

    def test_send_async_times_out_after_exhausted_iterations(self) -> None:
        channel = _channel(capacity=1)
        channel.try_send(_result(0, 0))

        async def scenario() -> None:
            with pytest.raises(ChannelTimeoutError):
                await channel.send_async(_result(0, 1), iterations=3)

        asyncio.run(scenario())
        assert channel.queued == 1
        assert channel.accepted_count == 1

    def test_recv_async_times_out_after_exhausted_iterations(self) -> None:
        channel = _channel()

        async def scenario() -> None:
            with pytest.raises(ChannelTimeoutError):
                await channel.recv_async(iterations=3)

        asyncio.run(scenario())

    def test_send_async_on_a_closed_channel_raises(self) -> None:
        channel = _channel()
        channel.close()

        async def scenario() -> None:
            with pytest.raises(ChannelClosedError):
                await channel.send_async(_result(0, 0))

        asyncio.run(scenario())

    def test_recv_async_on_a_closed_and_empty_channel_raises(self) -> None:
        channel = _channel()
        channel.close()

        async def scenario() -> None:
            with pytest.raises(ChannelClosedError):
                await channel.recv_async()

        asyncio.run(scenario())

    def test_recv_async_on_a_closed_channel_still_delivers_remaining(self) -> None:
        channel = _channel()
        channel.send(_result(4, 5), timeout=JOIN_TIMEOUT_SECONDS)
        channel.close()

        async def scenario() -> object:
            return await channel.recv_async()

        assert asyncio.run(scenario()) == _result(4, 5)

    @pytest.mark.parametrize("iterations", [0, -1, True, "4", MAX_CHANNEL_CAPACITY + 1])
    def test_send_async_validates_the_iteration_budget(self, iterations: object) -> None:
        channel = _channel()

        async def scenario() -> None:
            with pytest.raises(ChannelValidationError):
                await channel.send_async(_result(0, 0), iterations=cast(int, iterations))

        asyncio.run(scenario())

    @pytest.mark.parametrize("iterations", [0, False, None, MAX_CHANNEL_CAPACITY + 1])
    def test_recv_async_validates_the_iteration_budget(self, iterations: object) -> None:
        channel = _channel()

        async def scenario() -> None:
            with pytest.raises(ChannelValidationError):
                await channel.recv_async(iterations=cast(int, iterations))

        asyncio.run(scenario())


class TestConcurrency:
    def test_parallel_senders_fill_the_bound_exactly(self) -> None:
        capacity = 4
        channel = _channel(kind=CHANNEL_KIND_TELEMETRY, capacity=capacity)
        barrier = threading.Barrier(capacity)

        def produce(depth: int) -> None:
            barrier.wait(timeout=JOIN_TIMEOUT_SECONDS)
            channel.send(_telemetry(depth), timeout=JOIN_TIMEOUT_SECONDS)

        threads = [threading.Thread(target=produce, args=(depth,)) for depth in range(capacity)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=JOIN_TIMEOUT_SECONDS)
            assert not thread.is_alive()
        assert channel.accepted_count == capacity
        assert channel.queued == capacity
        assert channel.max_observed_queued == capacity
        drained = channel.drain()
        assert len(drained) == capacity
        assert sorted(cast(ContractMetric, item).value for item in drained) == list(range(capacity))

    def test_many_producers_and_one_consumer_conserve_every_message(self) -> None:
        producers = 4
        per_producer = 15
        total = producers * per_producer
        channel = _channel(kind=CHANNEL_KIND_RESULT, capacity=4)
        start = threading.Barrier(producers + 1)
        received: list[WorkResultV1] = []
        received_lock = threading.Lock()

        def produce(producer: int) -> None:
            start.wait(timeout=JOIN_TIMEOUT_SECONDS)
            for index in range(per_producer):
                channel.send(_result(producer, index), timeout=PARK_TIMEOUT_SECONDS)

        def consume(count: int) -> None:
            start.wait(timeout=JOIN_TIMEOUT_SECONDS)
            while count > 0:
                try:
                    message = cast(WorkResultV1, channel.recv(timeout=PARK_TIMEOUT_SECONDS))
                except ChannelClosedError:
                    return
                with received_lock:
                    received.append(message)
                count -= 1

        threads = [
            threading.Thread(target=produce, args=(producer,)) for producer in range(producers)
        ]
        consumer = threading.Thread(target=consume, args=(total,))
        for thread in threads:
            thread.start()
        consumer.start()
        consumer.join(timeout=PARK_TIMEOUT_SECONDS)
        assert not consumer.is_alive()
        for thread in threads:
            thread.join(timeout=PARK_TIMEOUT_SECONDS)
            assert not thread.is_alive()
        drained = channel.drain()
        assert len(received) + len(drained) == total
        assert channel.accepted_count == total
        assert channel.max_observed_queued <= 4

    def test_per_producer_fifo_is_preserved_in_global_receive_order(self) -> None:
        producers = 3
        per_producer = 10
        total = producers * per_producer
        channel = _channel(kind=CHANNEL_KIND_RESULT, capacity=2)
        start = threading.Barrier(producers + 1)
        received: list[WorkResultV1] = []
        received_lock = threading.Lock()

        def produce(producer: int) -> None:
            start.wait(timeout=JOIN_TIMEOUT_SECONDS)
            for index in range(per_producer):
                channel.send(_result(producer, index), timeout=PARK_TIMEOUT_SECONDS)

        def consume() -> None:
            start.wait(timeout=JOIN_TIMEOUT_SECONDS)
            for _ in range(total):
                try:
                    message = cast(WorkResultV1, channel.recv(timeout=PARK_TIMEOUT_SECONDS))
                except ChannelClosedError:
                    return
                with received_lock:
                    received.append(message)

        threads = [
            threading.Thread(target=produce, args=(producer,)) for producer in range(producers)
        ]
        consumer = threading.Thread(target=consume)
        for thread in threads:
            thread.start()
        consumer.start()
        consumer.join(timeout=PARK_TIMEOUT_SECONDS)
        assert not consumer.is_alive()
        for thread in threads:
            thread.join(timeout=PARK_TIMEOUT_SECONDS)
            assert not thread.is_alive()
        assert len(received) == total
        for producer in range(producers):
            ids = [
                message.work_item_id
                for message in received
                if message.work_item_id.startswith(f"wi-{producer}-")
            ]
            assert ids == [f"wi-{producer}-{index:04d}" for index in range(per_producer)]

    def test_concurrent_close_neither_loses_nor_duplicates_accepted_messages(self) -> None:
        producers = 3
        per_producer = 8
        total = producers * per_producer
        channel = _channel(kind=CHANNEL_KIND_RESULT, capacity=2)
        start = threading.Barrier(producers + 1)
        sent_ok: list[WorkResultV1] = []
        errors: list[ChannelError] = []
        ledger_lock = threading.Lock()

        def produce(producer: int) -> None:
            start.wait(timeout=JOIN_TIMEOUT_SECONDS)
            for index in range(per_producer):
                message = _result(producer, index)
                try:
                    channel.send(message, timeout=JOIN_TIMEOUT_SECONDS)
                except (ChannelClosedError, ChannelTimeoutError) as error:
                    with ledger_lock:
                        errors.append(error)
                    continue
                with ledger_lock:
                    sent_ok.append(message)

        threads = [
            threading.Thread(target=produce, args=(producer,)) for producer in range(producers)
        ]
        for thread in threads:
            thread.start()
        start.wait(timeout=JOIN_TIMEOUT_SECONDS)
        channel.close()
        for thread in threads:
            thread.join(timeout=JOIN_TIMEOUT_SECONDS)
            assert not thread.is_alive()
        drained = channel.drain()
        assert len(sent_ok) + len(errors) == total
        assert len(drained) == len(sent_ok)
        assert set(drained) == set(sent_ok)
        assert channel.accepted_count == len(sent_ok)


class TestModuleContract:
    def test_module_avoids_nested_loop_entry_points(self) -> None:
        source = Path(channels_module.__file__).read_text(encoding="utf-8")
        assert "asyncio.run" not in source
        assert "run_until_complete" not in source
        assert "get_event_loop(" not in source

    def test_module_exports_exactly_the_deliberate_public_names(self) -> None:
        assert set(channels_module.__all__) == {
            "CHANNEL_KIND_ASSIGNMENT",
            "CHANNEL_KIND_RESULT",
            "CHANNEL_KIND_TELEMETRY",
            "CHANNEL_KIND_WRITER",
            "MIN_CHANNEL_CAPACITY",
            "MAX_CHANNEL_CAPACITY",
            "ChannelError",
            "ChannelValidationError",
            "ChannelClosedError",
            "ChannelTimeoutError",
            "ChannelLoopError",
            "ChannelUnknownOutcomeError",
            "BoundedChannel",
            "ChannelSet",
        }

    def test_execution_package_re_exports_the_channel_contract(self) -> None:
        for name in channels_module.__all__:
            assert name in execution_package.__all__
            assert getattr(execution_package, name) is getattr(channels_module, name)

    def test_module_documentation_states_the_clock_and_close_contracts(self) -> None:
        documentation = channels_module.__doc__ or ""
        assert "monotonic" in documentation
        assert "idempotent" in documentation
        assert "bounded" in documentation

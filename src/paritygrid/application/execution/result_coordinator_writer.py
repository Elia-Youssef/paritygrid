"""Transactional-writer adapter for validated concurrent result intents.

The concurrent coordinator owns a richer, runner-neutral ``CommitIntent``;
the SQLite writer accepts only the closed Phase 6 command set.  This adapter
is the explicit parent-side bridge between those boundaries.  A command
factory may use parent-owned durable objects and clocks, but the adapter
validates every fencing and rebase field before the concrete command reaches
the transactional writer.  It then translates a real ``WriterReceipt`` into
the acknowledgement shape consumed by the coordinator.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Protocol, cast, runtime_checkable

from paritygrid.application.execution.result_coordinator import (
    CommitIntent,
    ResultValidationRejection,
)
from paritygrid.application.ports.consistency import CheckpointCommit
from paritygrid.application.ports.execution import CompletedWork
from paritygrid.application.ports.writer import (
    WriterCommand,
    WriterCommitOutcomeUnknownError,
    WriterReceipt,
    WriterSubmissionId,
    WriterTicket,
)
from paritygrid.application.writes.execution import (
    CommitWorkAttempt,
    CommitWorkResult,
    CommitWorkWithCheckpoint,
)
from paritygrid.domain.execution import WorkItemState
from paritygrid.domain.models import UtcTimestamp

_EPOCH = datetime(1970, 1, 1, tzinfo=UTC)
_OUTCOME_STATES: dict[str, WorkItemState] = {
    "succeeded": WorkItemState.SUCCEEDED,
    "retry_wait": WorkItemState.RETRY_WAIT,
    "quarantined": WorkItemState.QUARANTINED,
    "failed": WorkItemState.FAILED,
    "cancelled": WorkItemState.CANCELLED,
}


@runtime_checkable
class ResultCommitCommandFactory(Protocol):
    """Build one concrete Phase 6 command from a validated parent intent."""

    def build(self, intent: CommitIntent, /) -> WriterCommand:
        """Return ``CommitWorkAttempt`` or ``CommitWorkWithCheckpoint``."""
        ...


@runtime_checkable
class _TransactionalWriterSubmit(Protocol):
    def submit(
        self,
        command: WriterCommand,
        *,
        timeout_seconds: float,
    ) -> WriterTicket: ...


@dataclass(frozen=True, slots=True)
class ResultCoordinatorCommittedReceipt:
    """Coordinator acknowledgement backed by one validated writer receipt."""

    submission_id: WriterSubmissionId
    committed_intent: CommitIntent
    writer_receipt: WriterReceipt
    committed: bool = True

    def __post_init__(self) -> None:
        if type(self.submission_id) is not WriterSubmissionId:
            raise TypeError("coordinator receipt identity must use WriterSubmissionId")
        if type(self.committed_intent) is not CommitIntent:
            raise TypeError("coordinator receipt intent must use CommitIntent")
        if type(self.writer_receipt) is not WriterReceipt:
            raise TypeError("coordinator receipt evidence must use WriterReceipt")
        if self.committed is not True:
            raise ValueError("coordinator committed receipt must acknowledge commit")


class _ResultCoordinatorTicket:
    """Ticket preserving real admission identity and validating durable receipt."""

    __slots__ = ("_command", "_intent", "_ticket")

    def __init__(
        self,
        ticket: WriterTicket,
        command: CommitWorkAttempt | CommitWorkWithCheckpoint,
        intent: CommitIntent,
    ) -> None:
        self._ticket = ticket
        self._command = command
        self._intent = intent

    @property
    def submission_id(self) -> WriterSubmissionId:
        submission_id = self._ticket.submission_id
        if type(submission_id) is not WriterSubmissionId:
            raise WriterCommitOutcomeUnknownError("transactional writer ticket identity is invalid")
        return submission_id

    def result(self, *, timeout_seconds: float) -> ResultCoordinatorCommittedReceipt:
        submission_id = self.submission_id
        receipt = self._ticket.result(timeout_seconds=timeout_seconds)
        _validate_receipt(receipt, submission_id, self._command, self._intent)
        return ResultCoordinatorCommittedReceipt(submission_id, self._intent, receipt)


class TransactionalResultCoordinatorWriter:
    """Submit only validated Phase 6 result commands to a transactional writer."""

    __slots__ = ("_command_factory", "_writer")

    def __init__(self, writer: object, command_factory: ResultCommitCommandFactory) -> None:
        writer_value = writer
        if not isinstance(writer_value, _TransactionalWriterSubmit):
            raise TypeError("transactional result writer must expose submit")
        factory_value = cast(object, command_factory)
        if not isinstance(factory_value, ResultCommitCommandFactory):
            raise TypeError("result command factory must implement ResultCommitCommandFactory")
        self._writer = writer_value
        self._command_factory = factory_value

    def submit(self, command: object, *, timeout_seconds: float) -> object:
        """Compile, validate, and admit one intent without exposing it to dispatch."""
        if type(command) is not CommitIntent:
            raise ResultValidationRejection(
                "transactional result adapter accepts only CommitIntent"
            )
        intent = command
        try:
            concrete = self._command_factory.build(intent)
        except ResultValidationRejection:
            raise
        except Exception as error:
            raise ResultValidationRejection(
                "result command factory failed before writer admission"
            ) from error
        closed = _validate_command(concrete, intent)
        ticket = self._writer.submit(closed, timeout_seconds=timeout_seconds)
        return _ResultCoordinatorTicket(ticket, closed, intent)


def _validate_command(
    command: object,
    intent: CommitIntent,
) -> CommitWorkAttempt | CommitWorkWithCheckpoint:
    if type(command) is CommitWorkAttempt or type(command) is CommitWorkWithCheckpoint:
        closed = command
    else:
        raise ResultValidationRejection(
            "result command factory returned an unsupported writer command"
        )
    claim = closed.claim
    event = closed.event
    facts_match = (
        str(closed.run_id) == intent.run_id
        and str(closed.node_id) == intent.node_id
        and str(claim.work_item_id) == intent.work_item_id
        and int(claim.attempt_number) == intent.attempt_number
        and claim.row_version == intent.lease_fence
        and claim.lease_owner == intent.lease_owner
        and _timestamp_micros(closed.completion.finished_at) == intent.observed_at_micros
        and _timestamp_micros(claim.lease_expires_at) == intent.lease_expires_at_micros
        and closed.expected_run_row_version == intent.expected_run_row_version
        and closed.expected_node_row_version == intent.expected_node_row_version
        and int(event.expected_next_sequence) == intent.next_event_sequence
        and event.expected_counter_row_version == intent.event_counter_row_version
        and closed.completion.target_state is _OUTCOME_STATES[intent.outcome]
        and (type(closed) is CommitWorkWithCheckpoint) is intent.checkpoint_proposed
    )
    if type(closed) is CommitWorkWithCheckpoint:
        facts_match = (
            facts_match
            and str(closed.checkpoint.expected_partition_key) == intent.partition_key
            and (
                closed.checkpoint.artifact_id is None
                or str(closed.checkpoint.artifact_id) in intent.artifact_ids
            )
        )
    if not facts_match:
        raise ResultValidationRejection(
            "result command does not match the validated intent and durable frontier"
        )
    return closed


def _validate_receipt(
    receipt: object,
    submission_id: WriterSubmissionId,
    command: CommitWorkAttempt | CommitWorkWithCheckpoint,
    intent: CommitIntent,
) -> None:
    if (
        type(receipt) is not WriterReceipt
        or receipt.submission_id != submission_id
        or receipt.command_kind is not command.kind
        or receipt.run_id != command.run_id
        or receipt.mutated is not True
        or type(receipt.result) is not CommitWorkResult
    ):
        raise WriterCommitOutcomeUnknownError(
            "transactional writer receipt does not acknowledge the result command"
        )
    result = cast(CommitWorkResult, receipt.result)
    if type(result.completed) is not CompletedWork:
        raise WriterCommitOutcomeUnknownError("transactional writer completion evidence is invalid")
    completed = result.completed
    checkpoint_matches = (
        type(result.checkpoint) is CheckpointCommit
        if type(command) is CommitWorkWithCheckpoint
        else result.checkpoint is None
    )
    evidence_matches = (
        str(completed.work_item.run_id) == intent.run_id
        and str(completed.work_item.node_id) == intent.node_id
        and str(completed.work_item.partition_key) == intent.partition_key
        and str(completed.work_item.work_item_id) == intent.work_item_id
        and int(completed.attempt.attempt_number) == intent.attempt_number
        and completed.work_item.row_version == intent.lease_fence + 1
        and completed.work_item.state is _OUTCOME_STATES[intent.outcome]
        and result.node.row_version == intent.expected_node_row_version + 1
        and result.run.row_version == intent.expected_run_row_version + 1
        and int(result.events.next_sequence) == intent.next_event_sequence + 1
        and result.events.counter_row_version == intent.event_counter_row_version + 1
        and checkpoint_matches
    )
    if not evidence_matches:
        raise WriterCommitOutcomeUnknownError(
            "transactional writer receipt evidence is inconsistent with the intent"
        )


def _timestamp_micros(value: UtcTimestamp) -> int:
    if type(value) is not UtcTimestamp:
        raise ResultValidationRejection("result command lease expiry is invalid")
    delta = value.value - _EPOCH
    return delta.days * 86_400_000_000 + delta.seconds * 1_000_000 + delta.microseconds


__all__ = [
    "ResultCommitCommandFactory",
    "ResultCoordinatorCommittedReceipt",
    "TransactionalResultCoordinatorWriter",
]

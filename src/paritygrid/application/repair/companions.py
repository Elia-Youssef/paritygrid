"""Companion construction and writer submission shared by repair services.

Every durable repair mutation commits its audit fact, its durable event,
and the advanced run row in the same writer transaction. A command retry
must replay byte-identical companions, so companions are derived from the
frontier captured immediately before the first submission and reused
verbatim while an attempt is being resolved.
"""

from collections.abc import Mapping
from dataclasses import dataclass

from paritygrid.application.ports.consistency import (
    EventSequence,
    EventSubjectKind,
    PendingExecutionEvent,
    RedactedDocument,
)
from paritygrid.application.ports.repair_audit import PendingAuditEntry
from paritygrid.application.ports.writer import (
    EventAppendRequest,
    TransactionalWriter,
    WriterCommand,
    WriterCommandResult,
    WriterCommitOutcomeUnknownError,
    WriterError,
    WriterReceipt,
    WriterResultTimeoutError,
    WriterSubmissionId,
    WriterTicket,
)
from paritygrid.application.repair.errors import (
    RepairWriterOutcomeUnknownError,
    RepairWriterUnavailableError,
)
from paritygrid.application.repair.evidence import RepairWorkflowEvidence
from paritygrid.application.writes.repairs import RepairCompanions
from paritygrid.domain.models import RunId, UtcTimestamp

REPAIR_EVENT_PAYLOAD_SCHEMA_VERSION = 1
REPAIR_AUDIT_DETAIL_SCHEMA_VERSION = 1


@dataclass(frozen=True, slots=True)
class MutationFrontier:
    """The exact durable frontier one mutation must extend."""

    run_row_version: int
    next_event_sequence: int
    event_counter_row_version: int

    def __post_init__(self) -> None:
        for name in (
            "run_row_version",
            "next_event_sequence",
            "event_counter_row_version",
        ):
            value = getattr(self, name)
            if type(value) is not int or not 1 <= value <= 2_147_483_647:
                raise ValueError(f"{name} is outside the supported range")


def build_companions(
    *,
    frontier: MutationFrontier,
    run_id: RunId,
    operation: str,
    object_kind: str,
    object_id: str,
    actor: str,
    correlation_id: str,
    occurred_at: UtcTimestamp,
    payload: Mapping[str, object],
) -> RepairCompanions:
    """Build the atomic audit, event, and run-advance facts for one mutation."""
    document = RedactedDocument.from_mapping(payload)
    event = EventAppendRequest(
        EventSequence(frontier.next_event_sequence),
        frontier.event_counter_row_version,
        PendingExecutionEvent(
            event_kind=operation,
            occurred_at=occurred_at,
            subject_kind=EventSubjectKind.RUN,
            subject_id=run_id,
            correlation_id=correlation_id,
            payload_schema_version=REPAIR_EVENT_PAYLOAD_SCHEMA_VERSION,
            payload=document,
        ),
    )
    audit = PendingAuditEntry(
        actor=actor,
        operation=operation,
        object_kind=object_kind,
        object_id=object_id,
        correlation_id=correlation_id,
        occurred_at=occurred_at,
        detail_schema_version=REPAIR_AUDIT_DETAIL_SCHEMA_VERSION,
        detail=document,
    )
    return RepairCompanions(audit, event, frontier.run_row_version)


def frontier_from_evidence(evidence: RepairWorkflowEvidence) -> MutationFrontier:
    """Capture the mutation frontier from one repair workflow evidence read."""
    if type(evidence) is not RepairWorkflowEvidence:
        raise TypeError("mutation frontier requires RepairWorkflowEvidence")
    return MutationFrontier(
        run_row_version=evidence.run.row_version,
        next_event_sequence=evidence.event_counter.next_sequence_number,
        event_counter_row_version=evidence.event_counter.row_version,
    )


def submit_command(
    writer: TransactionalWriter,
    command: WriterCommand,
    *,
    timeout_seconds: float,
) -> tuple[WriterSubmissionId, WriterCommandResult, bool]:
    """Submit one command and await its receipt with typed failure mapping.

    Returns the submission identity, the committed result, and whether the
    command mutated durable state (false for an exact replay). A wait that
    ends while the outcome is unknown, like a raised unknown commit
    outcome, surfaces as the distinct unknown-outcome failure so callers
    can replay the identical command rather than guess.
    """
    try:
        ticket = writer.submit(command, timeout_seconds=timeout_seconds)
    except WriterError as error:
        raise RepairWriterUnavailableError(
            f"repair writer rejected the command: {type(error).__name__}"
        ) from error
    return _await_ticket(ticket, timeout_seconds=timeout_seconds)


def _await_ticket(
    ticket: WriterTicket, *, timeout_seconds: float
) -> tuple[WriterSubmissionId, WriterCommandResult, bool]:
    try:
        receipt = ticket.result(timeout_seconds=timeout_seconds)
    except (WriterResultTimeoutError, WriterCommitOutcomeUnknownError) as error:
        raise RepairWriterOutcomeUnknownError(
            "the durable outcome of the repair command is unknown"
        ) from error
    except WriterError as error:
        # Writer-infrastructure failures carry no domain meaning; repository
        # rejections arrive as their typed public errors and pass through
        # intact so the service fences can translate them.
        raise RepairWriterUnavailableError(
            f"repair writer failed: {type(error).__name__}"
        ) from error
    return _receipt_parts(receipt)


def _receipt_parts(
    receipt: WriterReceipt,
) -> tuple[WriterSubmissionId, WriterCommandResult, bool]:
    if type(receipt) is not WriterReceipt:
        raise RepairWriterUnavailableError("repair writer returned an invalid receipt")
    return receipt.submission_id, receipt.result, receipt.mutated


__all__ = [
    "REPAIR_AUDIT_DETAIL_SCHEMA_VERSION",
    "REPAIR_EVENT_PAYLOAD_SCHEMA_VERSION",
    "MutationFrontier",
    "build_companions",
    "frontier_from_evidence",
    "submit_command",
]

"""Closed value and command-marker tests for the writer boundary."""

from typing import cast

import pytest

from paritygrid.adapters.persistence.writer.notifications import (
    BoundedCommittedNotificationBuffer,
)
from paritygrid.application.ports.run_aggregates import MAX_WORK_METRIC, WorkMetricDelta
from paritygrid.application.ports.writer import (
    CommittedNotification,
    WriterCommand,
    WriterCommandKind,
    WriterCommandResult,
    WriterDiagnostics,
    WriterSettings,
    WriterState,
    WriterSubmissionId,
)
from paritygrid.application.writes.execution import (
    BootstrapWork,
    BootstrapWorkResult,
    ClaimWork,
    ClaimWorkResult,
    CommitWorkAttempt,
    CommitWorkResult,
    CommitWorkWithCheckpoint,
    CreateCapturedRun,
    CreateCapturedRunResult,
    FinalizeEmptyRunNode,
    FinalizeEmptyRunNodeResult,
    RecoverExpiredWork,
    RecoverExpiredWorkResult,
    RenewWorkClaim,
    RenewWorkClaimResult,
    TransitionRun,
    TransitionRunResult,
)
from paritygrid.application.writes.reconciliation import (
    PersistReconciliation,
    PersistReconciliationResult,
    RecordTargetVerification,
    RecordTargetVerificationResult,
)
from paritygrid.application.writes.repairs import (
    ApproveRepairPlan,
    BeginRepairApplication,
    BeginRepairApplicationResult,
    CompleteRepairApplication,
    CreateRepairPlan,
    RecordRepairActionApplied,
    RecordRepairActionAttempt,
    RecordRepairActionFailed,
    RejectRepairPlan,
    RepairActionAppliedResult,
)
from paritygrid.domain.models import RunId


def test_writer_settings_reject_every_invalid_bound() -> None:
    invalid: tuple[dict[str, object], ...] = (
        {"queue_capacity": 0},
        {"queue_capacity": True},
        {"admission_waiter_capacity": 0},
        {"admission_waiter_capacity": 10_001},
        {"admission_waiter_capacity": False},
        {"notification_capacity": 10_001},
        {"notification_capacity": 1.0},
        {"max_contention_attempts": 0},
        {"max_contention_attempts": True},
        {"contention_delay_seconds": -0.1},
        {"contention_delay_seconds": 0},
        {"thread_name": ""},
        {"thread_name": 7},
        {"thread_name": "bad\nname"},
    )
    for values in invalid:
        with pytest.raises(ValueError, match=r"supported|capacity|attempts|delay|name"):
            WriterSettings(**values)  # type: ignore[arg-type]
    assert WriterSettings(contention_delay_seconds=1.0).contention_delay_seconds == 1.0


def test_writer_identity_and_metric_bounds_are_exact() -> None:
    assert int(WriterSubmissionId(1)) == 1
    assert int(WriterSubmissionId(9_223_372_036_854_775_807)) > 1
    with pytest.raises(TypeError):
        WriterSubmissionId(cast(int, True))
    for value in (0, 9_223_372_036_854_775_808):
        with pytest.raises(ValueError, match="supported range"):
            WriterSubmissionId(value)
    assert WorkMetricDelta(MAX_WORK_METRIC).records_read == MAX_WORK_METRIC
    with pytest.raises(TypeError):
        WorkMetricDelta(records_read=cast(int, True))
    with pytest.raises(ValueError, match="supported range"):
        WorkMetricDelta(bytes_written=MAX_WORK_METRIC + 1)


def test_all_command_and_result_markers_are_closed() -> None:
    command_types = (
        CreateCapturedRun,
        TransitionRun,
        BootstrapWork,
        ClaimWork,
        RenewWorkClaim,
        CommitWorkAttempt,
        CommitWorkWithCheckpoint,
        RecoverExpiredWork,
        FinalizeEmptyRunNode,
        CreateRepairPlan,
        ApproveRepairPlan,
        RejectRepairPlan,
        BeginRepairApplication,
        RecordRepairActionAttempt,
        RecordRepairActionApplied,
        RecordRepairActionFailed,
        CompleteRepairApplication,
        PersistReconciliation,
        RecordTargetVerification,
    )
    result_types = (
        CreateCapturedRunResult,
        TransitionRunResult,
        BootstrapWorkResult,
        ClaimWorkResult,
        RenewWorkClaimResult,
        RecoverExpiredWorkResult,
        FinalizeEmptyRunNodeResult,
        BeginRepairApplicationResult,
        RepairActionAppliedResult,
        PersistReconciliationResult,
        RecordTargetVerificationResult,
    )
    command_kinds = {
        cast(WriterCommand, object.__new__(command_type)).kind for command_type in command_types
    }
    result_kinds: set[WriterCommandKind] = {
        cast(WriterCommandResult, object.__new__(result_type)).result_kind
        for result_type in result_types
    }
    commit_result = object.__new__(CommitWorkResult)
    object.__setattr__(commit_result, "checkpoint", None)
    result_kinds.add(commit_result.result_kind)
    object.__setattr__(commit_result, "checkpoint", object())
    result_kinds.add(commit_result.result_kind)
    assert command_kinds == set(WriterCommandKind)
    assert result_kinds <= command_kinds


def test_notification_buffer_rejects_invalid_inputs_and_tracks_capacity() -> None:
    for capacity in (0, 10_001, True):
        with pytest.raises(ValueError, match="supported range"):
            BoundedCommittedNotificationBuffer(cast(int, capacity))
    buffer = BoundedCommittedNotificationBuffer(1)
    with pytest.raises(TypeError):
        buffer.offer(cast(CommittedNotification, object()))
    notification = CommittedNotification(
        WriterSubmissionId(1),
        WriterCommandKind.CREATE_CAPTURED_RUN,
        RunId("run_contract-test"),
    )
    assert buffer.offer(notification)
    assert not buffer.offer(notification)
    assert buffer.take() == notification
    assert buffer.take() is None


def diagnostics(**changes: object) -> WriterDiagnostics:
    values: dict[str, object] = {
        "state": WriterState.RUNNING,
        "queue_capacity": 4,
        "admission_capacity": 3,
        "accepted": 3,
        "completed": 1,
        "queue_depth": 1,
        "admission_waiters": 1,
        "in_flight": 1,
        "max_queue_depth": 2,
        "max_admission_waiters": 2,
        "max_resident": 3,
        "contention_retries": 1,
    }
    values.update(changes)
    return WriterDiagnostics(**values)  # type: ignore[arg-type]


def test_writer_diagnostics_validate_exact_types_bounds_and_relationships() -> None:
    assert diagnostics().state is WriterState.RUNNING
    invalid: tuple[tuple[dict[str, object], type[Exception]], ...] = (
        ({"state": "running"}, TypeError),
        ({"accepted": 1.0}, TypeError),
        ({"queue_capacity": 0}, ValueError),
        ({"admission_capacity": 10_001}, ValueError),
        ({"contention_retries": -1}, ValueError),
        ({"accepted": 1, "completed": 2}, ValueError),
        ({"accepted": 2}, ValueError),
        ({"accepted": 7, "queue_depth": 5}, ValueError),
        ({"admission_waiters": 4}, ValueError),
        ({"accepted": 4, "in_flight": 2}, ValueError),
        ({"max_queue_depth": 0}, ValueError),
        ({"max_queue_depth": 5}, ValueError),
        ({"max_admission_waiters": 0}, ValueError),
        ({"max_admission_waiters": 4}, ValueError),
        ({"max_resident": 1}, ValueError),
        ({"max_resident": 6}, ValueError),
        ({"max_queue_depth": 3, "max_resident": 2}, ValueError),
    )
    for changes, expected in invalid:
        with pytest.raises(expected):
            diagnostics(**changes)


def test_writer_diagnostics_repr_contains_only_bounded_counters() -> None:
    rendered = repr(diagnostics())
    assert "WriterDiagnostics" in rendered
    assert "payload" not in rendered
    assert "seconds" not in rendered

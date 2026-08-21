# pyright: reportPrivateUsage=false
"""Adversarial tests for execution persistence validation boundaries."""

import ast
import inspect
import sqlite3
from typing import NoReturn, cast

import pytest
from sqlalchemy.engine import RowMapping
from sqlalchemy.exc import IntegrityError, OperationalError
from sqlalchemy.orm import Session

from paritygrid.adapters.persistence.repositories import execution_common as common
from paritygrid.adapters.persistence.repositories import execution_mapping as mapping
from paritygrid.adapters.persistence.repositories import runs as run_runtime
from paritygrid.adapters.persistence.repositories import work_items as work_runtime
from paritygrid.adapters.persistence.values import RunNodeState, WorkAttemptOutcome
from paritygrid.application.ports import ConfigurationDocument
from paritygrid.application.ports import execution as execution_port
from paritygrid.application.ports.execution import (
    AttemptOutcome,
    ExecutionCorruptionError,
    ExecutionInvalidRequestError,
    ExecutionLeaseExpiredError,
    ExecutionLeaseMismatchError,
    ExecutionRecordNotFoundError,
    ExecutionStaleRowVersionError,
    ExecutionStateConflictError,
    ExecutionStorageError,
    ExecutionStorageUnavailableError,
    RunNodeStatus,
    RunRecord,
    WorkAttemptRecord,
    WorkClaim,
    WorkItemRecord,
    validate_execution_page_limit,
)
from paritygrid.domain.execution import FailureClassification, RunState, WorkItemState
from paritygrid.domain.models import (
    AttemptNumber,
    Duration,
    NodeId,
    PipelineId,
    PipelineVersion,
    RunId,
    UtcTimestamp,
    WorkItemId,
)
from paritygrid.domain.pipeline import PartitionKey


def row(**values: object) -> RowMapping:
    return cast(RowMapping, values)


def run_row(**overrides: object) -> RowMapping:
    values: dict[str, object] = {
        "run_id": "run_valid",
        "pipeline_id": "pip_valid",
        "pipeline_version_number": 1,
        "runner_kind": "threaded",
        "runner_configuration_json": "{}",
        "state": "queued",
        "row_version": 1,
        "scenario_seed": None,
        "created_at": "2026-08-12T12:00:00.000000Z",
        "started_at": None,
        "finished_at": None,
        "cancellation_requested_at": None,
        "recovery_started_at": None,
        "recovered_at": None,
        "execution_evidence_fingerprint": None,
        "execution_evidence_fingerprint_version": None,
    }
    values.update(overrides)
    return row(**values)


def node_row(**overrides: object) -> RowMapping:
    values: dict[str, object] = {
        "run_id": "run_valid",
        "node_id": "nod_valid",
        "state": "pending",
        "row_version": 1,
        "work_total": 0,
        "work_pending": 0,
        "work_running": 0,
        "work_succeeded": 0,
        "work_quarantined": 0,
        "work_failed": 0,
        "work_cancelled": 0,
        "records_read": 0,
        "records_written": 0,
        "records_quarantined": 0,
        "bytes_read": 0,
        "bytes_written": 0,
        "retry_count": 0,
        "duration_microseconds": 0,
        "started_at": None,
        "finished_at": None,
    }
    values.update(overrides)
    return row(**values)


def work_row(**overrides: object) -> RowMapping:
    values: dict[str, object] = {
        "work_item_id": "wrk_valid",
        "run_id": "run_valid",
        "node_id": "nod_valid",
        "partition_key": "page-0001",
        "state": "pending",
        "row_version": 1,
        "completed_attempt_count": 0,
        "expected_checkpoint_version": 0,
        "input_reference_json": None,
        "retry_available_at": None,
        "lease_owner": None,
        "lease_expires_at": None,
        "active_attempt_number": None,
        "active_attempt_started_at": None,
        "active_runner_kind": None,
        "active_worker_identity": None,
        "created_at": "2026-08-12T12:00:00.000000Z",
        "updated_at": "2026-08-12T12:00:00.000000Z",
    }
    values.update(overrides)
    return row(**values)


def attempt_row(**overrides: object) -> RowMapping:
    values: dict[str, object] = {
        "work_item_id": "wrk_valid",
        "attempt_number": 1,
        "started_at": "2026-08-12T12:00:00.000000Z",
        "finished_at": "2026-08-12T12:00:01.000000Z",
        "runner_kind": "threaded",
        "worker_identity": "worker-01",
        "outcome": "succeeded",
        "failure_classification": None,
        "redacted_detail": None,
        "result_reference_json": None,
        "records_processed": 0,
        "bytes_processed": 0,
        "duration_microseconds": 1_000_000,
    }
    values.update(overrides)
    return row(**values)


def pipeline_row(**overrides: object) -> RowMapping:
    values: dict[str, object] = {
        "pipeline_id": "pip_valid",
        "display_name": "Pipeline",
        "description": None,
        "created_at": "2026-08-12T12:00:00.000000Z",
        "archived_at": None,
        "row_version": 1,
    }
    values.update(overrides)
    return row(**values)


def version_row(**overrides: object) -> RowMapping:
    values: dict[str, object] = {
        "pipeline_id": "pip_valid",
        "version_number": 1,
        "specification_json": "{}",
        "specification_sha256": (
            "44136fa355b3678a1146ad16f7e8649e94fb4fc21fe77e8310c060f61caaff8a"
        ),
        "planner_format_version": 1,
        "published_at": "2026-08-12T12:00:00.000000Z",
    }
    values.update(overrides)
    return row(**values)


def test_application_enum_sets_exactly_match_schema_values() -> None:
    assert {value.value for value in RunNodeStatus} == {value.value for value in RunNodeState}
    assert {value.value for value in AttemptOutcome} == {
        value.value for value in WorkAttemptOutcome
    }


def test_public_port_is_dependency_neutral_and_repositories_do_not_own_transactions() -> None:
    port_tree = ast.parse(inspect.getsource(execution_port))
    imported_modules = {
        node.module
        for node in ast.walk(port_tree)
        if isinstance(node, ast.ImportFrom) and node.module is not None
    }
    assert not any(
        module.startswith(("paritygrid.adapters", "sqlalchemy")) for module in imported_modules
    )

    forbidden_calls = {"begin", "begin_nested", "commit", "rollback", "close"}
    for repository_module in (run_runtime, work_runtime):
        repository_tree = ast.parse(inspect.getsource(repository_module))
        method_calls = {
            node.func.attr
            for node in ast.walk(repository_tree)
            if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)
        }
        assert method_calls.isdisjoint(forbidden_calls)


def test_public_representations_redact_claim_attempt_and_work_payloads() -> None:
    work = mapping.work_item_from_row(work_row(input_reference_json='{"token":"canary"}'))
    attempt = mapping.work_attempt_from_row(
        attempt_row(
            worker_identity="canary-worker",
            redacted_detail="canary-detail",
            result_reference_json='{"token":"canary"}',
        )
    )
    claim = WorkClaim(
        WorkItemId("wrk_valid"),
        AttemptNumber(1),
        "canary-owner",
        2,
        mapping.stored_timestamp("2026-08-12T12:00:00.000000Z", "time"),
        mapping.stored_timestamp("2026-08-12T12:00:01.000000Z", "time"),
        "threaded",
        "canary-worker",
    )
    assert "canary" not in repr(work)
    assert "canary" not in repr(attempt)
    assert "canary" not in repr(claim)


@pytest.mark.parametrize("value", [0, 101, True, "1"])
def test_execution_page_limit_rejects_invalid_values(value: object) -> None:
    with pytest.raises(ExecutionInvalidRequestError):
        validate_execution_page_limit(value)


def test_common_validation_edge_cases(monkeypatch: pytest.MonkeyPatch) -> None:
    assert common.optional_text(None, "detail", 4) is None
    assert common.optional_sqlite_int(None, "seed") is None
    assert common.decode_optional_execution_document(None, "document") is None
    assert common.nonnegative_int(0, "count") == 0
    assert common.encode_primitive_document(ConfigurationDocument.from_mapping({})) == {}
    for value in (1, "", "e\u0301", "xxxxx"):
        with pytest.raises(ExecutionInvalidRequestError):
            common.bounded_text(value, "text", 4)
    for value in (0, True, 2_147_483_648):
        with pytest.raises(ExecutionInvalidRequestError):
            common.positive_int(value, "counter")
    for value in (-1, True, 9_223_372_036_854_775_808):
        with pytest.raises(ExecutionInvalidRequestError):
            common.nonnegative_int(value, "counter")
    for value in (True, -9_223_372_036_854_775_809, 9_223_372_036_854_775_808):
        with pytest.raises(ExecutionInvalidRequestError):
            common.optional_sqlite_int(value, "seed")
    with pytest.raises(ExecutionStateConflictError):
        common.require_incrementable(2_147_483_647, "counter")
    with pytest.raises(ExecutionInvalidRequestError):
        common.require_run_id("run_valid")
    with pytest.raises(ExecutionInvalidRequestError):
        common.validate_node_ids(())
    with pytest.raises(ExecutionInvalidRequestError):
        common.validate_node_ids("invalid")  # type: ignore[arg-type]
    with pytest.raises(ExecutionInvalidRequestError):
        common.validate_node_ids((NodeId("nod_valid"), NodeId("nod_valid")))

    document = ConfigurationDocument.from_mapping({})
    monkeypatch.setattr(common, "MAX_CANONICAL_DOCUMENT_BYTES", 1)
    with pytest.raises(ExecutionInvalidRequestError, match="encoded size"):
        common.encode_execution_document(document, "document")

    def fail_encoding(_value: object) -> NoReturn:
        raise ValueError("canary")

    monkeypatch.setattr(common.CanonicalStorageJson, "encode", staticmethod(fail_encoding))
    with pytest.raises(ExecutionInvalidRequestError, match="invalid"):
        common.encode_execution_document(document, "document")


@pytest.mark.parametrize("value", [1, "[]", '{ "a":1}', "not-json"])
def test_decode_document_rejects_noncanonical_or_nonobject(value: object) -> None:
    with pytest.raises(ExecutionCorruptionError):
        common.decode_execution_document(value, "document")


def test_storage_error_translation_is_redacted() -> None:
    @common.translate_execution_storage_errors
    def unavailable() -> NoReturn:
        raise OperationalError(
            "SELECT canary_sql",
            {"secret": "canary_parameter"},
            sqlite3.OperationalError("canary_database"),
        )

    @common.translate_execution_storage_errors
    def generic() -> NoReturn:
        raise IntegrityError(
            "INSERT canary_sql",
            {"secret": "canary_parameter"},
            sqlite3.IntegrityError("canary_database"),
        )

    for operation, expected, message in (
        (unavailable, ExecutionStorageUnavailableError, "Execution storage is unavailable."),
        (generic, ExecutionStorageError, "Execution storage operation failed."),
    ):
        with pytest.raises(expected) as caught:
            operation()
        assert str(caught.value) == message
        assert caught.value.__cause__ is None
        assert caught.value.__context__ is None
        assert "canary" not in repr(caught.value)


@pytest.mark.parametrize(
    "change",
    [
        {"run_id": "invalid"},
        {"pipeline_version_number": 0},
        {"runner_configuration_json": "[]"},
        {"state": "invalid"},
        {"scenario_seed": 9_223_372_036_854_775_808},
        {"created_at": "invalid"},
        {"started_at": "2026-08-12T11:59:59.000000Z"},
        {"state": "running", "started_at": None},
        {"state": "queued", "started_at": "2026-08-12T12:00:01.000000Z"},
        {"state": "cancelling", "started_at": "2026-08-12T12:00:00.000000Z"},
        {
            "state": "running",
            "started_at": "2026-08-12T12:00:00.000000Z",
            "cancellation_requested_at": "2026-08-12T12:00:01.000000Z",
        },
        {"state": "failed", "started_at": "2026-08-12T12:00:00.000000Z", "finished_at": None},
        {
            "state": "succeeded",
            "started_at": "2026-08-12T12:00:00.000000Z",
            "finished_at": "2026-08-12T12:00:01.000000Z",
        },
        {
            "state": "failed",
            "started_at": "2026-08-12T12:00:00.000000Z",
            "finished_at": "2026-08-12T11:59:59.000000Z",
        },
        {"recovered_at": "2026-08-12T12:00:01.000000Z"},
    ],
)
def test_run_mapping_rejects_corruption(change: dict[str, object]) -> None:
    with pytest.raises(ExecutionCorruptionError):
        mapping.run_from_row(run_row(**change))


def test_run_mapping_accepts_cancellation_and_success_matrices() -> None:
    cancelled = mapping.run_from_row(
        run_row(
            state="cancelled",
            cancellation_requested_at="2026-08-12T12:00:01.000000Z",
            finished_at="2026-08-12T12:00:01.000000Z",
        )
    )
    succeeded = mapping.run_from_row(
        run_row(
            state="succeeded",
            started_at="2026-08-12T12:00:00.000000Z",
            finished_at="2026-08-12T12:00:01.000000Z",
            execution_evidence_fingerprint="4" * 64,
            execution_evidence_fingerprint_version=2,
        )
    )
    assert cancelled.finished_at is not None
    assert succeeded.execution_evidence_fingerprint is not None


@pytest.mark.parametrize(
    "change",
    [
        {"work_pending": 1},
        {"state": "pending", "started_at": "2026-08-12T12:00:00.000000Z"},
        {"state": "running", "started_at": None},
        {
            "state": "running",
            "started_at": "2026-08-12T12:00:00.000000Z",
            "finished_at": "2026-08-12T12:00:01.000000Z",
        },
        {"state": "succeeded", "started_at": None, "finished_at": None},
        {
            "state": "succeeded",
            "started_at": "2026-08-12T12:00:01.000000Z",
            "finished_at": "2026-08-12T12:00:00.000000Z",
        },
    ],
)
def test_run_node_mapping_rejects_corruption(change: dict[str, object]) -> None:
    with pytest.raises(ExecutionCorruptionError):
        mapping.run_node_from_row(node_row(**change))


def test_run_node_mapping_accepts_running_and_completed_states() -> None:
    running = mapping.run_node_from_row(
        node_row(state="running", started_at="2026-08-12T12:00:00.000000Z")
    )
    succeeded = mapping.run_node_from_row(
        node_row(
            state="succeeded",
            started_at="2026-08-12T12:00:00.000000Z",
            finished_at="2026-08-12T12:00:01.000000Z",
        )
    )
    assert running.status is RunNodeStatus.RUNNING
    assert succeeded.status is RunNodeStatus.SUCCEEDED


@pytest.mark.parametrize(
    "change",
    [
        {"partition_key": "Invalid"},
        {"state": "leased"},
        {"updated_at": "2026-08-12T11:59:59.000000Z"},
        {"state": "pending", "lease_owner": "owner"},
        {"state": "retry_wait", "retry_available_at": None},
        {"retry_available_at": "2026-08-12T12:00:01.000000Z"},
        {"state": "retry_wait", "retry_available_at": "2026-08-12T11:59:59.000000Z"},
        {
            "state": "running",
            "lease_owner": "owner",
            "lease_expires_at": "2026-08-12T12:00:02.000000Z",
            "active_attempt_number": 2,
            "active_attempt_started_at": "2026-08-12T12:00:00.000000Z",
            "active_runner_kind": "threaded",
            "active_worker_identity": "worker",
        },
        {
            "state": "running",
            "lease_owner": "owner",
            "lease_expires_at": "2026-08-12T12:00:00.000000Z",
            "active_attempt_number": 1,
            "active_attempt_started_at": "2026-08-12T12:00:00.000000Z",
            "active_runner_kind": "threaded",
            "active_worker_identity": "worker",
        },
    ],
)
def test_work_mapping_rejects_corruption(change: dict[str, object]) -> None:
    with pytest.raises(ExecutionCorruptionError):
        mapping.work_item_from_row(work_row(**change))


def test_work_mapping_accepts_running_claim() -> None:
    value = mapping.work_item_from_row(
        work_row(
            state="running",
            row_version=2,
            lease_owner="owner",
            lease_expires_at="2026-08-12T12:00:02.000000Z",
            active_attempt_number=1,
            active_attempt_started_at="2026-08-12T12:00:00.000000Z",
            active_runner_kind="threaded",
            active_worker_identity="worker",
        )
    )
    assert value.active_attempt_number == AttemptNumber(1)


@pytest.mark.parametrize(
    "change",
    [
        {"work_item_id": "invalid"},
        {"attempt_number": 0},
        {"outcome": "invalid"},
        {"finished_at": "2026-08-12T11:59:59.000000Z", "duration_microseconds": 0},
        {"duration_microseconds": 2},
        {"outcome": "succeeded", "failure_classification": "timeout"},
        {"outcome": "failed", "failure_classification": None},
        {"outcome": "lease_expired", "failure_classification": "connection"},
    ],
)
def test_attempt_mapping_rejects_corruption(change: dict[str, object]) -> None:
    with pytest.raises(ExecutionCorruptionError):
        mapping.work_attempt_from_row(attempt_row(**change))


def test_stored_scalar_mappers_reject_corruption() -> None:
    for operation in (
        lambda: mapping.run_event_counter_from_row(row(run_id="invalid")),
        lambda: mapping.stored_positive_int(0, "counter"),
        lambda: mapping.stored_nonnegative_int(-1, "counter"),
        lambda: mapping.stored_optional_sqlite_int(True, "seed"),
        lambda: mapping.stored_timestamp("invalid", "timestamp"),
        lambda: mapping.stored_optional_fingerprint("invalid"),
        lambda: mapping.stored_run_id(1),
        lambda: mapping.stored_partition_key(1),
        lambda: mapping.stored_text("", "text", 2),
        lambda: mapping.stored_timestamp(1, "timestamp"),
        lambda: mapping.stored_timestamp("2026-08-12T12:00:00Z", "timestamp"),
        lambda: mapping.stored_optional_fingerprint(1),
        lambda: mapping.run_event_counter_from_row(row()),
        lambda: mapping.run_node_from_row(row()),
        lambda: mapping.work_item_from_row(row()),
    ):
        with pytest.raises(ExecutionCorruptionError):
            operation()


@pytest.mark.parametrize(
    "change",
    [
        {
            "created_at": "2026-08-12T11:59:00.000000Z",
            "state": "failed",
            "started_at": "2026-08-12T12:00:00.000000Z",
            "finished_at": "2026-08-12T11:59:59.000000Z",
        },
        {
            "state": "cancelled",
            "cancellation_requested_at": "2026-08-12T12:00:02.000000Z",
            "finished_at": "2026-08-12T12:00:01.000000Z",
        },
    ],
)
def test_run_mapping_rejects_cross_timestamp_order(change: dict[str, object]) -> None:
    with pytest.raises(ExecutionCorruptionError):
        mapping.run_from_row(run_row(**change))


@pytest.mark.parametrize(
    "change",
    [
        {"state": "running", "lease_owner": "owner"},
        {
            "state": "running",
            "created_at": "2026-08-12T12:00:01.000000Z",
            "updated_at": "2026-08-12T12:00:01.000000Z",
            "lease_owner": "owner",
            "lease_expires_at": "2026-08-12T12:00:03.000000Z",
            "active_attempt_number": 1,
            "active_attempt_started_at": "2026-08-12T12:00:00.000000Z",
            "active_runner_kind": "threaded",
            "active_worker_identity": "worker",
        },
        {
            "state": "running",
            "updated_at": "2026-08-12T12:00:00.000000Z",
            "lease_owner": "owner",
            "lease_expires_at": "2026-08-12T12:00:03.000000Z",
            "active_attempt_number": 1,
            "active_attempt_started_at": "2026-08-12T12:00:01.000000Z",
            "active_runner_kind": "threaded",
            "active_worker_identity": "worker",
        },
        {
            "state": "running",
            "updated_at": "2026-08-12T12:00:01.000000Z",
            "lease_owner": "owner",
            "lease_expires_at": "2026-08-12T12:00:01.000000Z",
            "active_attempt_number": 1,
            "active_attempt_started_at": "2026-08-12T12:00:00.000000Z",
            "active_runner_kind": "threaded",
            "active_worker_identity": "worker",
        },
    ],
)
def test_work_mapping_rejects_active_claim_timestamp_corruption(
    change: dict[str, object],
) -> None:
    with pytest.raises(ExecutionCorruptionError):
        mapping.work_item_from_row(work_row(**change))


def test_attempt_record_repr_is_redacted() -> None:
    record = WorkAttemptRecord(
        WorkItemId("wrk_valid"),
        AttemptNumber(1),
        UtcTimestamp.parse("2026-08-12T12:00:00Z"),
        UtcTimestamp.parse("2026-08-12T12:00:01Z"),
        "threaded",
        "canary-worker",
        AttemptOutcome.FAILED,
        FailureClassification.UNKNOWN,
        "canary-detail",
        ConfigurationDocument.from_mapping({"canary": "payload"}),
        0,
        0,
        Duration(1_000_000),
    )
    assert "canary" not in repr(record)


class _Result:
    def __init__(self, item: object = None) -> None:
        self.item = item

    def mappings(self) -> _Result:
        return self

    def one_or_none(self) -> object:
        return self.item

    def scalar_one_or_none(self) -> object:
        return self.item

    def all(self) -> list[object]:
        value = self.item
        if isinstance(value, list):
            return cast(list[object], value)
        return []


class _Session:
    def __init__(self, results: tuple[_Result, ...] = ()) -> None:
        self.results = iter(results)

    def in_transaction(self) -> bool:
        return True

    def execute(self, _statement: object, _parameters: object = None) -> _Result:
        return next(self.results)


def run_record(*, state: RunState = RunState.RUNNING, row_version: int = 2) -> RunRecord:
    started = None if state is RunState.QUEUED else UtcTimestamp.parse("2026-08-12T12:00:01Z")
    return RunRecord(
        RunId("run_valid"),
        PipelineId("pip_valid"),
        PipelineVersion(1),
        "threaded",
        ConfigurationDocument.from_mapping({}),
        state,
        row_version,
        None,
        UtcTimestamp.parse("2026-08-12T12:00:00Z"),
        started,
        None,
        None,
        None,
        None,
        None,
    )


def work_record(
    *, state: WorkItemState = WorkItemState.PENDING, row_version: int = 1
) -> WorkItemRecord:
    return WorkItemRecord(
        WorkItemId("wrk_valid"),
        RunId("run_valid"),
        NodeId("nod_valid"),
        PartitionKey("page-0001"),
        state,
        row_version,
        0,
        0,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        UtcTimestamp.parse("2026-08-12T12:00:00Z"),
        UtcTimestamp.parse("2026-08-12T12:00:00Z"),
    )


def active_work_record(
    *, row_version: int = 2, lease_expires_at: str = "2026-08-12T12:00:08.000000Z"
) -> WorkItemRecord:
    return mapping.work_item_from_row(
        work_row(
            state="running",
            row_version=row_version,
            lease_owner="owner",
            lease_expires_at=lease_expires_at,
            active_attempt_number=1,
            active_attempt_started_at="2026-08-12T12:00:01.000000Z",
            active_runner_kind="threaded",
            active_worker_identity="worker",
            updated_at="2026-08-12T12:00:01.000000Z",
        )
    )


def work_claim(*, row_version: int = 2) -> WorkClaim:
    return WorkClaim(
        WorkItemId("wrk_valid"),
        AttemptNumber(1),
        "owner",
        row_version,
        UtcTimestamp.parse("2026-08-12T12:00:01Z"),
        UtcTimestamp.parse("2026-08-12T12:00:08Z"),
        "threaded",
        "worker",
    )


def test_run_private_cas_classification_is_exhaustive(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repository = run_runtime.SqlAlchemyRunRepository(cast(Session, cast(object, _Session())))
    identity = RunId("run_valid")
    for current, expected_error in (
        (None, run_runtime.ExecutionRecordNotFoundError),
        (run_record(row_version=3), run_runtime.ExecutionStaleRowVersionError),
        (run_record(state=RunState.PAUSING), run_runtime.ExecutionStateConflictError),
        (run_record(), run_runtime.ExecutionStateConflictError),
    ):

        def fake_get_run(
            _identity: RunId,
            value: RunRecord | None = current,
        ) -> RunRecord | None:
            return value

        monkeypatch.setattr(repository, "get", fake_get_run)
        with pytest.raises(expected_error):
            repository._raise_cas_failure(identity, 2, RunState.RUNNING)

    def missing_run(_identity: RunId) -> None:
        return None

    monkeypatch.setattr(repository, "get", missing_run)
    with pytest.raises(run_runtime.ExecutionRecordNotFoundError):
        repository._require_run(identity, 1)


def test_run_defensive_helpers_cover_empty_counter_and_time_guard() -> None:
    repository = run_runtime.SqlAlchemyRunRepository(cast(Session, cast(object, _Session())))
    repository._require_counters(())
    with pytest.raises(ExecutionInvalidRequestError, match="precede"):
        repository._transition_values(
            run_record(),
            RunState.FAILED,
            UtcTimestamp.parse("2026-08-11T12:00:00Z"),
            None,
            None,
        )


def test_work_claim_cas_classification_is_exhaustive(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repository = work_runtime.SqlAlchemyWorkItemRepository(cast(Session, cast(object, _Session())))
    identity = WorkItemId("wrk_valid")
    for current, expected_error in (
        (None, work_runtime.ExecutionRecordNotFoundError),
        (work_record(row_version=2), work_runtime.ExecutionStaleRowVersionError),
        (work_record(state=WorkItemState.RUNNING), work_runtime.ExecutionStateConflictError),
    ):

        def fake_get_work(
            _identity: WorkItemId,
            value: WorkItemRecord | None = current,
        ) -> WorkItemRecord | None:
            return value

        monkeypatch.setattr(repository, "get", fake_get_work)
        with pytest.raises(expected_error):
            repository._raise_claim_cas_failure(identity, 1, WorkItemState.PENDING)

    def current_work(_identity: WorkItemId) -> WorkItemRecord:
        return work_record()

    def claim_not_allowed(_identity: RunId) -> bool:
        return False

    def claim_allowed(_identity: RunId) -> bool:
        return True

    monkeypatch.setattr(repository, "get", current_work)
    monkeypatch.setattr(repository, "_run_allows_claim", claim_not_allowed)
    with pytest.raises(ExecutionStateConflictError, match="run state"):
        repository._raise_claim_cas_failure(identity, 1, WorkItemState.PENDING)
    monkeypatch.setattr(repository, "_run_allows_claim", claim_allowed)
    with pytest.raises(ExecutionStateConflictError, match="rejected"):
        repository._raise_claim_cas_failure(identity, 1, WorkItemState.PENDING)


def test_work_run_parent_validation_defensive_paths() -> None:
    missing = work_runtime.SqlAlchemyWorkItemRepository(
        cast(Session, cast(object, _Session((_Result(None),))))
    )
    with pytest.raises(ExecutionCorruptionError, match="parent"):
        missing._run_allows_claim(RunId("run_valid"))
    invalid = work_runtime.SqlAlchemyWorkItemRepository(
        cast(Session, cast(object, _Session((_Result("invalid"),))))
    )
    with pytest.raises(ExecutionCorruptionError, match="state"):
        invalid._run_allows_claim(RunId("run_valid"))


def test_claim_from_inactive_record_is_corruption() -> None:
    with pytest.raises(ExecutionCorruptionError, match="not durable"):
        work_runtime._claim_from_record(work_record())


@pytest.mark.parametrize(
    ("pipeline", "version", "message"),
    [
        (pipeline_row(), version_row(specification_sha256="0" * 64), "parent is corrupt"),
        (pipeline_row(pipeline_id="pip_other"), version_row(), "identity is corrupt"),
        (pipeline_row(), version_row(version_number=2), "identity is corrupt"),
    ],
)
def test_run_pipeline_parent_defensive_validation(
    pipeline: RowMapping, version: RowMapping, message: str
) -> None:
    repository = run_runtime.SqlAlchemyRunRepository(
        cast(Session, cast(object, _Session((_Result(pipeline), _Result(version)))))
    )
    with pytest.raises(ExecutionCorruptionError, match=message):
        repository._read_pipeline_parent(PipelineId("pip_valid"), PipelineVersion(1))


def test_run_counter_identity_and_zero_row_update_are_rejected(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    counter = row(run_id="run_other", next_sequence_number=1, row_version=1)
    repository = run_runtime.SqlAlchemyRunRepository(
        cast(Session, cast(object, _Session((_Result(counter),))))
    )
    with pytest.raises(ExecutionCorruptionError, match="identity"):
        repository._require_counter(RunId("run_valid"))

    repository = run_runtime.SqlAlchemyRunRepository(
        cast(Session, cast(object, _Session((_Result(None),))))
    )

    def missing_run(_identity: RunId) -> None:
        return None

    monkeypatch.setattr(repository, "get", missing_run)
    with pytest.raises(ExecutionRecordNotFoundError):
        repository._update_run(
            RunId("run_valid"),
            expected_row_version=1,
            expected_state=RunState.QUEUED,
            values={"row_version": 2},
        )


@pytest.mark.parametrize(
    ("run_result", "node_result", "message"),
    [
        (run_row(run_id="run_other"), node_row(), "identity"),
        (run_row(), node_row(node_id="nod_other"), "identity"),
    ],
)
def test_work_creation_rejects_parent_identity_mismatch(
    run_result: RowMapping,
    node_result: RowMapping,
    message: str,
) -> None:
    repository = work_runtime.SqlAlchemyWorkItemRepository(
        cast(
            Session,
            cast(object, _Session((_Result(run_result), _Result(node_result)))),
        )
    )
    with pytest.raises(ExecutionCorruptionError, match=message):
        repository.create(
            work_item_id=WorkItemId("wrk_valid"),
            run_id=RunId("run_valid"),
            node_id=NodeId("nod_valid"),
            partition_key=PartitionKey("page-0001"),
            input_reference=None,
            created_at=UtcTimestamp.parse("2026-08-12T12:00:00Z"),
        )


def test_claim_zero_row_classification_is_exhaustive(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    current = active_work_record()
    capability = work_claim()
    observed = UtcTimestamp.parse("2026-08-12T12:00:04Z")
    cases: tuple[tuple[WorkItemRecord | None, type[Exception]], ...] = (
        (None, ExecutionRecordNotFoundError),
        (active_work_record(row_version=3), ExecutionStaleRowVersionError),
        (
            active_work_record(lease_expires_at="2026-08-12T12:00:03.000000Z"),
            ExecutionLeaseExpiredError,
        ),
        (active_work_record(), ExecutionLeaseMismatchError),
    )
    for latest, expected_error in cases:
        repository = work_runtime.SqlAlchemyWorkItemRepository(
            cast(Session, cast(object, _Session((_Result(None),))))
        )

        def fake_get(
            _identity: WorkItemId,
            value: WorkItemRecord | None = latest,
        ) -> WorkItemRecord | None:
            return value

        monkeypatch.setattr(repository, "get", fake_get)
        with pytest.raises(expected_error):
            repository._claim_update(
                current,
                capability,
                observed_at=observed,
                values={"row_version": 3},
            )


def test_claim_and_recovery_zero_row_paths_delegate_to_classifiers(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pending = work_record()
    claim_repository = work_runtime.SqlAlchemyWorkItemRepository(
        cast(Session, cast(object, _Session((_Result(None),))))
    )

    def require_pending(_identity: WorkItemId, _expected: int) -> WorkItemRecord:
        return pending

    def allow_claim(_identity: RunId) -> bool:
        return True

    def rejected_claim(_identity: WorkItemId, _expected: int, _state: WorkItemState) -> NoReturn:
        raise ExecutionStateConflictError("classified claim race")

    monkeypatch.setattr(claim_repository, "_require_work", require_pending)
    monkeypatch.setattr(claim_repository, "_run_allows_claim", allow_claim)
    monkeypatch.setattr(claim_repository, "_raise_claim_cas_failure", rejected_claim)
    with pytest.raises(ExecutionStateConflictError, match="classified claim race"):
        claim_repository.claim(
            WorkItemId("wrk_valid"),
            expected_row_version=1,
            lease_owner="owner",
            started_at=UtcTimestamp.parse("2026-08-12T12:00:01Z"),
            lease_expires_at=UtcTimestamp.parse("2026-08-12T12:00:08Z"),
            runner_kind="threaded",
            worker_identity="worker",
        )

    active = active_work_record()
    recovery_repository = work_runtime.SqlAlchemyWorkItemRepository(
        cast(Session, cast(object, _Session((_Result(None),))))
    )

    def require_active(_identity: WorkItemId, _expected: int) -> WorkItemRecord:
        return active

    def rejected_recovery(
        _identity: WorkItemId,
        _expected: int,
        _attempt: AttemptNumber,
        _observed: UtcTimestamp,
    ) -> NoReturn:
        raise ExecutionStateConflictError("classified recovery race")

    monkeypatch.setattr(recovery_repository, "_require_work", require_active)
    monkeypatch.setattr(recovery_repository, "_raise_recovery_cas_failure", rejected_recovery)
    with pytest.raises(ExecutionStateConflictError, match="classified recovery race"):
        recovery_repository.recover_expired_claim(
            WorkItemId("wrk_valid"),
            expected_row_version=2,
            expected_attempt_number=AttemptNumber(1),
            observed_at=UtcTimestamp.parse("2026-08-12T12:00:08Z"),
            retry_available_at=UtcTimestamp.parse("2026-08-12T12:00:09Z"),
        )


def test_recovery_cas_classification_is_exhaustive(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repository = work_runtime.SqlAlchemyWorkItemRepository(cast(Session, cast(object, _Session())))
    observed = UtcTimestamp.parse("2026-08-12T12:00:08Z")
    cases: tuple[tuple[WorkItemRecord | None, type[Exception]], ...] = (
        (None, ExecutionRecordNotFoundError),
        (active_work_record(row_version=3), ExecutionStaleRowVersionError),
        (work_record(row_version=2), ExecutionStateConflictError),
        (
            mapping.work_item_from_row(
                work_row(
                    state="running",
                    row_version=2,
                    completed_attempt_count=1,
                    lease_owner="owner",
                    lease_expires_at="2026-08-12T12:00:08.000000Z",
                    active_attempt_number=2,
                    active_attempt_started_at="2026-08-12T12:00:01.000000Z",
                    active_runner_kind="threaded",
                    active_worker_identity="worker",
                    updated_at="2026-08-12T12:00:01.000000Z",
                )
            ),
            ExecutionLeaseMismatchError,
        ),
        (
            active_work_record(lease_expires_at="2026-08-12T12:00:09.000000Z"),
            ExecutionStateConflictError,
        ),
        (active_work_record(), ExecutionStateConflictError),
    )
    for current, expected_error in cases:

        def fake_get(
            _identity: WorkItemId,
            value: WorkItemRecord | None = current,
        ) -> WorkItemRecord | None:
            return value

        monkeypatch.setattr(repository, "get", fake_get)
        with pytest.raises(expected_error):
            repository._raise_recovery_cas_failure(
                WorkItemId("wrk_valid"), 2, AttemptNumber(1), observed
            )


def test_claim_requirements_cover_inactive_and_nonmonotonic_observation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repository = work_runtime.SqlAlchemyWorkItemRepository(cast(Session, cast(object, _Session())))

    def pending(_identity: WorkItemId, _expected: int) -> WorkItemRecord:
        return work_record(row_version=2)

    monkeypatch.setattr(repository, "_require_work", pending)
    with pytest.raises(ExecutionLeaseMismatchError, match="no longer active"):
        repository._require_claim(work_claim(), UtcTimestamp.parse("2026-08-12T12:00:02Z"))

    def active(_identity: WorkItemId, _expected: int) -> WorkItemRecord:
        return active_work_record()

    monkeypatch.setattr(repository, "_require_work", active)
    with pytest.raises(ExecutionInvalidRequestError, match="not monotonic"):
        repository._require_claim(work_claim(), UtcTimestamp.parse("2026-08-12T12:00:00Z"))


def test_completion_target_requires_exact_work_state(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repository = work_runtime.SqlAlchemyWorkItemRepository(cast(Session, cast(object, _Session())))

    def active(_claim: WorkClaim, _observed: UtcTimestamp) -> WorkItemRecord:
        return active_work_record()

    monkeypatch.setattr(repository, "_require_claim", active)
    completion = work_runtime.WorkCompletion(
        cast(WorkItemState, "failed"),
        UtcTimestamp.parse("2026-08-12T12:00:04Z"),
        None,
        FailureClassification.UNKNOWN,
        None,
        None,
        0,
        0,
    )
    with pytest.raises(ExecutionInvalidRequestError, match="must use"):
        repository.complete_claim(work_claim(), completion)


@pytest.mark.parametrize(
    ("heads", "aggregates", "message"),
    [
        ([], [], "incomplete"),
        (
            [
                row(
                    run_id="run_other",
                    node_id="nod_valid",
                    partition_key="page-0001",
                    current_version=0,
                    row_version=1,
                    updated_at="2026-08-12T12:00:00.000000Z",
                )
            ],
            [],
            "missing",
        ),
        (
            [
                row(
                    run_id="run_valid",
                    node_id="nod_valid",
                    partition_key="page-0001",
                    current_version=0,
                    row_version=1,
                    updated_at="2026-08-11T12:00:00.000000Z",
                )
            ],
            [],
            "update time",
        ),
        (
            [
                row(
                    run_id="run_valid",
                    node_id="nod_valid",
                    partition_key="page-0001",
                    current_version=0,
                    row_version=1,
                    updated_at="2026-08-12T12:00:00.000000Z",
                )
            ],
            [("wrk_valid", 1, 2, 2)],
            "not contiguous",
        ),
    ],
)
def test_work_batch_integrity_defensive_paths(
    heads: list[object], aggregates: list[object], message: str
) -> None:
    session = _Session((_Result(heads), _Result(aggregates)))
    with pytest.raises(ExecutionCorruptionError, match=message):
        work_runtime._verify_work_records(cast(Session, cast(object, session)), (work_record(),))

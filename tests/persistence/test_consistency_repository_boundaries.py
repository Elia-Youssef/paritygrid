# pyright: reportPrivateUsage=false
"""Boundary and corruption tests for consistency persistence."""

from __future__ import annotations

import ast
from datetime import UTC, datetime
from pathlib import Path
from typing import cast

import pytest
from sqlalchemy.engine import RowMapping
from sqlalchemy.exc import IntegrityError, OperationalError

from paritygrid.adapters.persistence.repositories import consistency_common as common
from paritygrid.adapters.persistence.repositories.consistency_common import (
    decode_document,
    decode_optional_document,
    decode_redacted_document,
    encode_document,
    encode_redacted_document,
    request_digest,
    translate_consistency_storage_errors,
)
from paritygrid.adapters.persistence.repositories.consistency_mapping import (
    artifact_key_from_row,
    checkpoint_from_row,
    checkpoint_head_from_row,
    event_counter_from_row,
    execution_event_from_row,
    stored_idempotency_from_row,
    updated_work_checkpoint_from_row,
)
from paritygrid.adapters.persistence.values import IdempotencyStatus as StorageIdempotencyStatus
from paritygrid.application.ports import ConfigurationDocument
from paritygrid.application.ports.consistency import (
    MAX_CONSISTENCY_SEQUENCE,
    CheckpointRecord,
    CheckpointVersion,
    ConsistencyCorruptionError,
    ConsistencyInvalidRequestError,
    ConsistencyStateConflictError,
    ConsistencyStorageError,
    ConsistencyStorageUnavailableError,
    EventSequence,
    EventSequenceConflictError,
    EventSubjectKind,
    ExecutionEventRecord,
    IdempotencyCursor,
    IdempotencyRecord,
    IdempotencyStatus,
    PendingExecutionEvent,
    RedactedDocument,
    validate_consistency_page_limit,
)
from paritygrid.domain.models import (
    ArtifactId,
    NodeId,
    RunId,
    UtcTimestamp,
    WorkItemId,
)
from paritygrid.domain.pipeline import PartitionKey

RUN_ID = RunId("run_boundary")
NODE_ID = NodeId("nod_boundary")
WORK_ID = WorkItemId("wrk_boundary")
ARTIFACT_ID = ArtifactId("art_boundary")
PARTITION = PartitionKey("partition-boundary")
NOW = UtcTimestamp(datetime(2026, 8, 12, 12, 0, tzinfo=UTC))


def document(**values: object) -> ConfigurationDocument:
    return ConfigurationDocument.from_mapping(values)


def redacted(**values: object) -> RedactedDocument:
    return RedactedDocument.from_mapping(values)


def row(**values: object) -> RowMapping:
    return cast(RowMapping, values)


@pytest.mark.parametrize("value", [True, -1, MAX_CONSISTENCY_SEQUENCE + 1, "1"])
def test_checkpoint_version_rejects_invalid_exact_values(value: object) -> None:
    error = TypeError if type(value) is not int else ValueError
    with pytest.raises(error):
        CheckpointVersion(cast(int, value))


def test_checkpoint_version_maximum_cannot_advance() -> None:
    with pytest.raises(ConsistencyStateConflictError):
        CheckpointVersion(MAX_CONSISTENCY_SEQUENCE).next()
    assert int(CheckpointVersion(4)) == 4


@pytest.mark.parametrize("value", [True, 0, -1, MAX_CONSISTENCY_SEQUENCE + 1, "1"])
def test_event_sequence_rejects_invalid_exact_values(value: object) -> None:
    error = TypeError if type(value) is not int else ValueError
    with pytest.raises(error):
        EventSequence(cast(int, value))


def test_event_sequence_advance_validates_count_and_overflow() -> None:
    with pytest.raises(ConsistencyInvalidRequestError):
        EventSequence(1).advance(0)
    with pytest.raises(ConsistencyInvalidRequestError):
        EventSequence(1).advance(cast(int, True))
    with pytest.raises(EventSequenceConflictError):
        EventSequence(MAX_CONSISTENCY_SEQUENCE).advance(1)
    assert EventSequence(2).advance(3) == EventSequence(5)
    assert int(EventSequence(3)) == 3


@pytest.mark.parametrize("value", [True, 0, 101, "1"])
def test_consistency_page_limit_is_exact_and_bounded(value: object) -> None:
    with pytest.raises(ConsistencyInvalidRequestError):
        validate_consistency_page_limit(value)
    assert validate_consistency_page_limit(100) == 100


@pytest.mark.parametrize(
    "value",
    [
        {"apiKey": "candidate"},
        {"nested": {"private-key": "candidate"}},
        {"items": [{"session_token": "candidate"}]},
        {"authorizationHeader": "candidate"},
    ],
)
def test_redacted_document_rejects_sensitive_keys_recursively(value: dict[str, object]) -> None:
    with pytest.raises(ConsistencyInvalidRequestError, match="prohibited"):
        RedactedDocument.from_mapping(value)


def test_redacted_document_requires_exact_configuration_document_and_detaches() -> None:
    with pytest.raises(TypeError):
        RedactedDocument(cast(ConfigurationDocument, object()))
    source = {"safe": [{"nested": "value"}]}
    value = RedactedDocument.from_mapping(source)
    source["safe"] = []
    assert value.to_mapping() == {"safe": [{"nested": "value"}]}
    assert repr(value) == "RedactedDocument(content=<redacted>)"


def test_public_record_reprs_redact_documents_and_idempotency_identity() -> None:
    checkpoint = CheckpointRecord(
        RUN_ID,
        NODE_ID,
        PARTITION,
        CheckpointVersion(1),
        1,
        document(cursor="canary"),
        document(position="canary"),
        None,
        NOW,
    )
    pending = PendingExecutionEvent(
        "run_started", NOW, EventSubjectKind.RUN, RUN_ID, None, 1, redacted(value="canary")
    )
    event = ExecutionEventRecord(
        RUN_ID,
        EventSequence(1),
        "run_started",
        NOW,
        EventSubjectKind.RUN,
        RUN_ID,
        None,
        1,
        redacted(value="canary"),
    )
    idem = IdempotencyRecord(
        "scope-canary",
        "key-canary",
        IdempotencyStatus.COMPLETED,
        1,
        redacted(value="canary"),
        NOW,
        NOW,
        NOW,
    )
    cursor = IdempotencyCursor(NOW, "scope-canary", "key-canary")
    for value in (checkpoint, pending, event, idem, cursor):
        rendered = repr(value)
        assert "canary" not in rendered
        assert "redacted" in rendered


def test_canonical_codec_and_digest_golden_vector() -> None:
    assert request_digest(document()) == (
        "44136fa355b3678a1146ad16f7e8649e94fb4fc21fe77e8310c060f61caaff8a"
    )
    source = document(alpha=[1, "é"], enabled=True)
    encoded = encode_document(source, "document")
    assert decode_document(encoded.text, "document") == source
    assert decode_optional_document(None, "optional") is None
    safe = redacted(alpha="value")
    assert decode_redacted_document(encode_redacted_document(safe, "safe").text, "safe") == safe


@pytest.mark.parametrize("value", [None, 3, "[]", "{not-json}"])
def test_decode_rejects_noncanonical_or_nonobject_values(value: object) -> None:
    with pytest.raises(ConsistencyCorruptionError):
        decode_document(value, "stored document")


def test_decode_rejects_stored_sensitive_payload() -> None:
    with pytest.raises(ConsistencyCorruptionError):
        decode_redacted_document('{"api_key":"candidate"}', "stored response")


def test_encode_maps_invalid_and_oversize_documents_without_payload(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(common, "MAX_CANONICAL_DOCUMENT_BYTES", 1)
    with pytest.raises(ConsistencyInvalidRequestError, match="size") as captured:
        encode_document(document(value="canary"), "request")
    assert "canary" not in str(captured.value)
    with pytest.raises(ConsistencyInvalidRequestError):
        encode_document(cast(ConfigurationDocument, object()), "request")


def test_codec_maps_canonical_and_configuration_constructor_failures(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def reject_encode(_value: object) -> object:
        raise ValueError("canary")

    monkeypatch.setattr(common.CanonicalStorageJson, "encode", reject_encode)
    with pytest.raises(ConsistencyInvalidRequestError, match="invalid"):
        encode_document(document(safe=True), "request")

    monkeypatch.undo()

    def reject_mapping(_value: object) -> ConfigurationDocument:
        raise TypeError("canary")

    monkeypatch.setattr(ConfigurationDocument, "from_mapping", reject_mapping)
    with pytest.raises(ConsistencyCorruptionError):
        decode_document('{"safe":true}', "stored request")


@pytest.mark.parametrize("kind", ["RunStarted", "run-started", "_run", "run__started", "évent"])
def test_event_kind_requires_canonical_lowercase_snake_case(kind: str) -> None:
    with pytest.raises(ConsistencyInvalidRequestError):
        common.event_kind(kind)
    assert common.event_kind("run_started_2") == "run_started_2"


def test_common_exact_validators_reject_adapter_boundary_mismatches() -> None:
    checks = (
        lambda: common.require_run_id(str(RUN_ID)),
        lambda: common.require_node_id(str(NODE_ID)),
        lambda: common.require_work_item_id(str(WORK_ID)),
        lambda: common.require_artifact_id(str(ARTIFACT_ID)),
        lambda: common.require_partition_key(str(PARTITION)),
        lambda: common.require_checkpoint_version(1),
        lambda: common.require_event_sequence(1),
        lambda: common.require_timestamp(str(NOW), "time"),
        lambda: common.require_document({}, "document"),
        lambda: common.require_redacted_document(document(), "redacted"),
        lambda: common.require_event_subject_kind("run"),
        lambda: common.require_idempotency_cursor((str(NOW), "scope", "key")),
    )
    for check in checks:
        with pytest.raises(ConsistencyInvalidRequestError):
            check()


def test_common_text_integer_optional_and_event_batch_validation() -> None:
    with pytest.raises(ConsistencyInvalidRequestError):
        common.bounded_text("e\N{COMBINING ACUTE ACCENT}", "text", 10)
    with pytest.raises(ConsistencyInvalidRequestError):
        common.bounded_text("", "text", 10)
    with pytest.raises(ConsistencyInvalidRequestError):
        common.bounded_text(cast(str, 3), "text", 10)
    assert common.optional_text(None, "optional", 10) is None
    assert common.optional_text("value", "optional", 10) == "value"
    for value in (True, 0, MAX_CONSISTENCY_SEQUENCE + 1):
        with pytest.raises(ConsistencyInvalidRequestError):
            common.positive_int(value, "integer")
    pending = PendingExecutionEvent(
        "run_started", NOW, EventSubjectKind.RUN, RUN_ID, None, 1, redacted(value="safe")
    )
    assert common.validate_events((pending,)) == (pending,)
    for events in ("event", (), (pending,) * 101, (object(),)):
        with pytest.raises(ConsistencyInvalidRequestError):
            common.validate_events(cast(object, events))  # type: ignore[arg-type]


def test_stored_scalar_helpers_validate_all_closed_types() -> None:
    assert common.stored_optional_text(None, "text", 3) is None
    assert common.stored_optional_text("ok", "text", 3) == "ok"
    assert common.stored_optional_timestamp(None, "time") is None
    assert common.stored_optional_artifact_id(None) is None
    assert common.stored_run_id(str(RUN_ID)) == RUN_ID
    assert common.stored_node_id(str(NODE_ID)) == NODE_ID
    assert common.stored_work_item_id(str(WORK_ID)) == WORK_ID
    assert common.stored_artifact_id(str(ARTIFACT_ID)) == ARTIFACT_ID
    assert common.stored_partition_key(str(PARTITION)) == PARTITION
    assert common.stored_nonnegative_int(0, "value") == 0
    assert common.stored_timestamp(str(NOW), "time") == NOW
    invalid_calls = (
        lambda: common.stored_text(1, "text", 2),
        lambda: common.stored_positive_int(0, "integer"),
        lambda: common.stored_nonnegative_int(True, "integer"),
        lambda: common.stored_timestamp(1, "time"),
        lambda: common.stored_timestamp("invalid", "time"),
        lambda: common.stored_run_id(1),
        lambda: common.stored_run_id("bad"),
        lambda: common.stored_partition_key(1),
        lambda: common.stored_partition_key(""),
    )
    for check in invalid_calls:
        with pytest.raises(ConsistencyCorruptionError):
            check()


def test_storage_errors_are_redacted_and_have_no_raw_chain() -> None:
    canary = "canary-secret-sql"

    @translate_consistency_storage_errors
    def unavailable() -> None:
        raise OperationalError("SELECT canary", {"value": canary}, RuntimeError(canary))

    @translate_consistency_storage_errors
    def generic() -> None:
        raise IntegrityError("INSERT canary", {"value": canary}, RuntimeError(canary))

    for operation, expected, message in (
        (unavailable, ConsistencyStorageUnavailableError, "Consistency storage is unavailable."),
        (generic, ConsistencyStorageError, "Consistency storage operation failed."),
    ):
        with pytest.raises(expected) as captured:
            operation()
        assert captured.value.args == (message,)
        assert captured.value.__cause__ is None
        assert captured.value.__context__ is None
        assert canary not in repr(captured.value)


def test_storage_translation_does_not_swallow_non_database_failures() -> None:
    class StopNow(BaseException):
        pass

    @translate_consistency_storage_errors
    def failpoint() -> None:
        raise StopNow

    with pytest.raises(StopNow):
        failpoint()


def valid_checkpoint_head_row() -> dict[str, object]:
    return {
        "run_id": str(RUN_ID),
        "node_id": str(NODE_ID),
        "partition_key": str(PARTITION),
        "current_version": 0,
        "updated_at": str(NOW),
        "row_version": 1,
    }


def valid_checkpoint_row() -> dict[str, object]:
    return {
        "run_id": str(RUN_ID),
        "node_id": str(NODE_ID),
        "partition_key": str(PARTITION),
        "version": 1,
        "payload_schema_version": 1,
        "source_cursor_json": None,
        "output_position_json": None,
        "artifact_id": None,
        "committed_at": str(NOW),
    }


def valid_work_row() -> dict[str, object]:
    return {
        "work_item_id": str(WORK_ID),
        "run_id": str(RUN_ID),
        "node_id": str(NODE_ID),
        "partition_key": str(PARTITION),
        "expected_checkpoint_version": 0,
        "row_version": 1,
    }


def valid_event_row() -> dict[str, object]:
    return {
        "run_id": str(RUN_ID),
        "sequence_number": 1,
        "event_kind": "run_started",
        "occurred_at": str(NOW),
        "subject_kind": "run",
        "subject_id": str(RUN_ID),
        "correlation_id": None,
        "payload_schema_version": 1,
        "payload_json": '{"safe":true}',
    }


def valid_idempotency_row() -> dict[str, object]:
    return {
        "scope": "run:create",
        "idempotency_key": "key-1",
        "request_sha256": "a" * 64,
        "status": "in_progress",
        "response_schema_version": None,
        "response_json": None,
        "created_at": str(NOW),
        "updated_at": str(NOW),
        "completed_at": None,
    }


def test_all_strict_row_mappers_accept_valid_rows() -> None:
    assert checkpoint_head_from_row(row(**valid_checkpoint_head_row())).row_version == 1
    assert checkpoint_from_row(row(**valid_checkpoint_row())).version == CheckpointVersion(1)
    assert updated_work_checkpoint_from_row(row(**valid_work_row())).work_item_id == WORK_ID
    assert event_counter_from_row(
        row(run_id=str(RUN_ID), next_sequence_number=1, row_version=1)
    ).next_sequence == EventSequence(1)
    assert execution_event_from_row(row(**valid_event_row())).subject_id == RUN_ID
    work_event = valid_event_row()
    work_event.update(subject_kind="work_item", subject_id=str(WORK_ID))
    assert execution_event_from_row(row(**work_event)).subject_id == WORK_ID
    assert (
        stored_idempotency_from_row(row(**valid_idempotency_row())).record.status
        is IdempotencyStatus.IN_PROGRESS
    )
    assert artifact_key_from_row(
        row(
            artifact_id=str(ARTIFACT_ID),
            run_id=str(RUN_ID),
            node_id=str(NODE_ID),
            partition_key=str(PARTITION),
            created_at=str(NOW),
        )
    ) == (ARTIFACT_ID, RUN_ID, NODE_ID, PARTITION, NOW)


@pytest.mark.parametrize(
    ("mapper", "factory", "field", "bad"),
    [
        (checkpoint_head_from_row, valid_checkpoint_head_row, "row_version", 0),
        (checkpoint_from_row, valid_checkpoint_row, "version", 0),
        (updated_work_checkpoint_from_row, valid_work_row, "work_item_id", "bad"),
        (execution_event_from_row, valid_event_row, "subject_kind", "unknown"),
        (execution_event_from_row, valid_event_row, "payload_json", '{"token":"value"}'),
        (stored_idempotency_from_row, valid_idempotency_row, "request_sha256", "bad"),
    ],
)
def test_strict_row_mappers_reject_corrupt_fields(
    mapper: object, factory: object, field: str, bad: object
) -> None:
    values = cast(object, factory)()  # type: ignore[operator]
    cast(dict[str, object], values)[field] = bad
    with pytest.raises(ConsistencyCorruptionError):
        cast(object, mapper)(row(**cast(dict[str, object], values)))  # type: ignore[operator]


@pytest.mark.parametrize(
    "mapper",
    [
        checkpoint_head_from_row,
        checkpoint_from_row,
        updated_work_checkpoint_from_row,
        event_counter_from_row,
        execution_event_from_row,
        stored_idempotency_from_row,
        artifact_key_from_row,
    ],
)
def test_strict_row_mappers_reject_missing_required_columns(mapper: object) -> None:
    with pytest.raises(ConsistencyCorruptionError):
        cast(object, mapper)(row())  # type: ignore[operator]


def test_strict_row_mappers_preserve_typed_corruption_paths() -> None:
    with pytest.raises(ConsistencyCorruptionError):
        event_counter_from_row(row(run_id=str(RUN_ID), next_sequence_number=0, row_version=1))
    event_values = valid_event_row()
    event_values["subject_kind"] = 1
    with pytest.raises(ConsistencyCorruptionError):
        execution_event_from_row(row(**event_values))
    idempotency_values = valid_idempotency_row()
    idempotency_values["status"] = 1
    with pytest.raises(ConsistencyCorruptionError):
        stored_idempotency_from_row(row(**idempotency_values))
    with pytest.raises(ConsistencyCorruptionError):
        artifact_key_from_row(
            row(
                artifact_id="bad",
                run_id=str(RUN_ID),
                node_id=str(NODE_ID),
                partition_key=str(PARTITION),
                created_at=str(NOW),
            )
        )


def test_idempotency_mapping_enforces_status_shape_and_chronology() -> None:
    in_progress = valid_idempotency_row()
    in_progress["response_schema_version"] = 1
    with pytest.raises(ConsistencyCorruptionError):
        stored_idempotency_from_row(row(**in_progress))

    terminal = valid_idempotency_row()
    terminal.update(
        status="completed",
        response_schema_version=1,
        response_json='{"safe":true}',
        updated_at="2026-08-12T12:00:01.000000Z",
        completed_at="2026-08-12T12:00:01.000000Z",
    )
    assert stored_idempotency_from_row(row(**terminal)).record.status is IdempotencyStatus.COMPLETED
    for field, value in (
        ("completed_at", None),
        ("updated_at", "2026-08-12T11:59:59.000000Z"),
        ("status", "unknown"),
    ):
        broken = terminal.copy()
        broken[field] = value
        with pytest.raises(ConsistencyCorruptionError):
            stored_idempotency_from_row(row(**broken))


def test_application_and_storage_idempotency_status_sets_are_exhaustive() -> None:
    assert {item.value for item in IdempotencyStatus} == {
        item.value for item in StorageIdempotencyStatus
    }


def test_application_contract_is_dependency_neutral_and_repositories_own_no_transactions() -> None:
    project = Path(__file__).parents[2]
    port_source = (project / "src/paritygrid/application/ports/consistency.py").read_text(
        encoding="utf-8"
    )
    port_tree = ast.parse(port_source)
    imports = {
        node.module
        for node in ast.walk(port_tree)
        if isinstance(node, ast.ImportFrom) and node.module is not None
    }
    assert not any(name.startswith("sqlalchemy") for name in imports)
    assert not any(name.startswith("paritygrid.adapters") for name in imports)

    for name in ("checkpoints.py", "execution_events.py", "idempotency.py"):
        source = (project / "src/paritygrid/adapters/persistence/repositories" / name).read_text(
            encoding="utf-8"
        )
        tree = ast.parse(source)
        calls = {
            node.func.attr
            for node in ast.walk(tree)
            if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)
        }
        assert calls.isdisjoint({"begin", "begin_nested", "commit", "rollback", "close"})

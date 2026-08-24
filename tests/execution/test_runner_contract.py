"""Contract, bound, adversarial, and wire-encoding tests for the runner contract."""

from __future__ import annotations

import asyncio
import concurrent.futures
import queue
from collections.abc import Callable
from dataclasses import FrozenInstanceError, replace
from pathlib import Path
from types import CoroutineType
from typing import Any

import pytest

import paritygrid.application.execution as execution_package
import paritygrid.application.execution.runner_contract as runner_contract_module
from paritygrid.application.execution import (
    MAX_CONTRACT_BYTES,
    MAX_CONTRACT_LIST_ITEMS,
    MAX_CONTRACT_MAP_ENTRIES,
    MAX_CONTRACT_STRING_LENGTH,
    MAX_FAILURE_DETAIL_LENGTH,
    MAX_METRIC_VALUE,
    RECOVERY_FRONTIER_PROTOCOL,
    RUNNER_CONTRACT_VERSION,
    STRATEGY_CAPABILITIES_PROTOCOL,
    WORK_ASSIGNMENT_PROTOCOL,
    WORK_RESULT_PROTOCOL,
    ContractCleanupEvidence,
    ContractCleanupStatus,
    ContractDocument,
    ContractLifecycleState,
    ContractList,
    ContractMetric,
    ContractOutcome,
    ControlGeneration,
    RecoveryFrontierV1,
    RunnerContractBoundError,
    RunnerContractEncodingError,
    RunnerContractError,
    RunnerContractLeakError,
    RunnerContractLoopError,
    RunnerContractVersionError,
    StrategyCapabilitiesV1,
    WorkAssignmentV1,
    WorkResultV1,
    contract_wire_size,
    decode_contract_document,
    decode_recovery_frontier,
    decode_work_assignment,
    decode_work_assignment_async,
    decode_work_result,
    encode_contract_document,
    encode_recovery_frontier,
    encode_work_assignment,
    encode_work_result,
    encode_work_result_async,
    is_event_loop_running,
    validate_public_contract_text,
)

DEADLINE = "2026-08-21T12:00:00.000000Z"
FINGERPRINT = "0123456789abcdef" * 4
DESCRIPTOR = ContractDocument(items=(("operation", "normalize"), ("rows", 100)))

CAPABILITIES = StrategyCapabilitiesV1(
    strategy_id="strategy-sequential",
    contract_version=RUNNER_CONTRACT_VERSION,
    supports_pause=True,
    supports_cancel=True,
    supports_checkpoint=False,
    max_concurrent_work=1,
    max_in_flight_records=8,
    platform_requirements=("cpython-3",),
    protocol=STRATEGY_CAPABILITIES_PROTOCOL,
)

ASSIGNMENT = WorkAssignmentV1(
    protocol=WORK_ASSIGNMENT_PROTOCOL,
    contract_version=RUNNER_CONTRACT_VERSION,
    run_id="run-alpha",
    node_id="nod-etl",
    partition_key="region-eu",
    work_item_id="wi-0001",
    attempt_number=1,
    lease_fence=3,
    lease_owner="worker-1",
    control_generation=ControlGeneration(2),
    deadline_utc=DEADLINE,
    operation_descriptor=DESCRIPTOR,
    input_references=("artifact://inputs/one", "artifact://inputs/two"),
    captured_settings_ref="settings://run-alpha/v1",
)

RESULT = WorkResultV1(
    protocol=WORK_RESULT_PROTOCOL,
    contract_version=RUNNER_CONTRACT_VERSION,
    run_id="run-alpha",
    node_id="nod-etl",
    partition_key="region-eu",
    work_item_id="wi-0001",
    attempt_number=1,
    lease_fence=3,
    lease_owner="worker-1",
    control_generation=ControlGeneration(2),
    outcome=ContractOutcome.SUCCEEDED,
    metrics=(ContractMetric("rows", 100), ContractMetric("bytes", 2048)),
    artifact_references=("artifact://outputs/one",),
    checkpoint_proposal=True,
    failure_detail=None,
    cleanup=ContractCleanupEvidence(
        status=ContractCleanupStatus.COMPLETED,
        actions=("release-lease", "remove-tmp"),
        idempotency_key="cleanup-run-alpha-0001",
    ),
)

FRONTIER = RecoveryFrontierV1(
    protocol=RECOVERY_FRONTIER_PROTOCOL,
    contract_version=RUNNER_CONTRACT_VERSION,
    plan_fingerprint=FINGERPRINT,
    control_generation=ControlGeneration(2),
    completed_work=(("run-alpha", "nod-a", "region-eu"), ("run-alpha", "nod-b", "region-eu")),
    unresolved_work=(("run-alpha", "nod-c", "region-eu"),),
    recovery_required_reason=None,
    state=ContractLifecycleState.RUNNING,
)

DOCUMENT = ContractDocument(
    items=(
        ("mode", "enrich"),
        ("nested", ContractDocument(items=(("depth", 2),))),
        ("tags", ContractList(("alpha", "beta"))),
    )
)

GOLDEN_ASSIGNMENT = (
    b"paritygrid.work-assignment.v1\n"
    b"contract_version=1\n"
    b"run_id=run-alpha\n"
    b"node_id=nod-etl\n"
    b"partition_key=region-eu\n"
    b"work_item_id=wi-0001\n"
    b"attempt_number=1\n"
    b"lease_fence=3\n"
    b"lease_owner=worker-1\n"
    b"generation=2\n"
    b"deadline_utc=2026-08-21T12:00:00.000000Z\n"
    b"operation_descriptor=doc:{operation=str:normalize\n"
    b"rows=int:100}\n"
    b"input_references=list:[str:artifact://inputs/one;str:artifact://inputs/two]\n"
    b"captured_settings_ref=settings://run-alpha/v1"
)

GOLDEN_RESULT = (
    b"paritygrid.work-result.v1\n"
    b"contract_version=1\n"
    b"run_id=run-alpha\n"
    b"node_id=nod-etl\n"
    b"partition_key=region-eu\n"
    b"work_item_id=wi-0001\n"
    b"attempt_number=1\n"
    b"lease_fence=3\n"
    b"lease_owner=worker-1\n"
    b"generation=2\n"
    b"outcome=succeeded\n"
    b"metrics=list:[doc:{name=str:rows\n"
    b"value=int:100};doc:{name=str:bytes\n"
    b"value=int:2048}]\n"
    b"artifact_references=list:[str:artifact://outputs/one]\n"
    b"checkpoint_proposal=true\n"
    b"failure_detail=\n"
    b"cleanup=doc:{actions=list:[str:release-lease;str:remove-tmp]\n"
    b"idempotency_key=str:cleanup-run-alpha-0001\n"
    b"status=str:completed}"
)

GOLDEN_FRONTIER = (
    "paritygrid.recovery-frontier.v1\n"
    "contract_version=1\n"
    f"plan_fingerprint={FINGERPRINT}\n"
    "generation=2\n"
    "completed_work=list:[list:[str:run-alpha;str:nod-a;str:region-eu];"
    "list:[str:run-alpha;str:nod-b;str:region-eu]]\n"
    "unresolved_work=list:[list:[str:run-alpha;str:nod-c;str:region-eu]]\n"
    "recovery_required_reason=\n"
    "state=running"
).encode()

GOLDEN_DOCUMENT = b"mode=str:enrich\nnested=doc:{depth=int:2}\ntags=list:[str:alpha;str:beta]"


def _assignment(**overrides: Any) -> WorkAssignmentV1:
    return replace(ASSIGNMENT, **overrides)


def _result(**overrides: Any) -> WorkResultV1:
    return replace(RESULT, **overrides)


def _frontier(**overrides: Any) -> RecoveryFrontierV1:
    return replace(FRONTIER, **overrides)


def _capabilities(**overrides: Any) -> StrategyCapabilitiesV1:
    return replace(CAPABILITIES, **overrides)


class _SessionLike:
    def execute(self, statement: object) -> object:
        raise AssertionError("a contract must never execute a session")


class _DynamicObject:
    def __getattr__(self, name: str) -> object:
        return None


def _example_callable() -> None:
    raise AssertionError("a contract must never call a live object")


async def _example_coroutine() -> None:
    raise AssertionError("a contract must never await a live object")


def _live_object(kind: str) -> object:
    if kind == "session":
        return _SessionLike()
    if kind == "callable":
        return _example_callable
    if kind == "future":
        return concurrent.futures.Future[None]()
    if kind == "queue":
        return queue.Queue[None]()
    if kind == "coroutine":
        return _example_coroutine()
    if kind == "path":
        return Path("E:/paritygrid/secrets")
    if kind == "exception":
        return RuntimeError("live failure")
    if kind == "dynamic":
        return _DynamicObject()
    raise AssertionError(kind)


LIVE_KINDS = ("session", "callable", "future", "queue", "coroutine", "path")


def _dispose(candidate: object) -> None:
    if isinstance(candidate, CoroutineType):
        candidate.close()


def test_golden_work_assignment_encoding_bytes() -> None:
    assert encode_work_assignment(ASSIGNMENT) == GOLDEN_ASSIGNMENT
    assert decode_work_assignment(GOLDEN_ASSIGNMENT) == ASSIGNMENT


def test_golden_work_result_encoding_bytes() -> None:
    assert encode_work_result(RESULT) == GOLDEN_RESULT
    assert decode_work_result(GOLDEN_RESULT) == RESULT


def test_golden_recovery_frontier_encoding_bytes() -> None:
    assert encode_recovery_frontier(FRONTIER) == GOLDEN_FRONTIER
    assert decode_recovery_frontier(GOLDEN_FRONTIER) == FRONTIER


def test_golden_contract_document_encoding_bytes() -> None:
    assert encode_contract_document(DOCUMENT) == GOLDEN_DOCUMENT
    assert decode_contract_document(GOLDEN_DOCUMENT) == DOCUMENT


def test_assignment_round_trip_with_empty_and_full_collections() -> None:
    empty = _assignment(operation_descriptor=ContractDocument(), input_references=())
    assert decode_work_assignment(encode_work_assignment(empty)) == empty
    references = tuple(
        f"artifact://inputs/ref-{index:02d}" for index in range(MAX_CONTRACT_LIST_ITEMS)
    )
    full = _assignment(input_references=references)
    payload = encode_work_assignment(full)
    assert len(payload) < MAX_CONTRACT_BYTES
    assert decode_work_assignment(payload) == full


def test_result_round_trip_with_empty_and_full_collections() -> None:
    empty = _result(
        metrics=(),
        artifact_references=(),
        failure_detail=None,
        cleanup=ContractCleanupEvidence(
            status=ContractCleanupStatus.PENDING,
            actions=(),
            idempotency_key=None,
        ),
    )
    assert decode_work_result(encode_work_result(empty)) == empty
    metrics = tuple(
        ContractMetric(f"metric-{index:02d}", index) for index in range(MAX_CONTRACT_LIST_ITEMS)
    )
    artifacts = tuple(
        f"artifact://outputs/ref-{index:02d}" for index in range(MAX_CONTRACT_LIST_ITEMS)
    )
    full = _result(
        outcome=ContractOutcome.RETRY_WAIT,
        metrics=metrics,
        artifact_references=artifacts,
        checkpoint_proposal=False,
        failure_detail="d" * MAX_FAILURE_DETAIL_LENGTH,
        cleanup=ContractCleanupEvidence(
            status=ContractCleanupStatus.FAILED,
            actions=("release-lease",),
            idempotency_key=None,
        ),
    )
    payload = encode_work_result(full)
    assert len(payload) < MAX_CONTRACT_BYTES
    assert decode_work_result(payload) == full


def test_frontier_round_trip_with_empty_and_full_triples() -> None:
    empty = _frontier(completed_work=(), unresolved_work=(), recovery_required_reason=None)
    assert decode_recovery_frontier(encode_recovery_frontier(empty)) == empty
    triples = tuple(
        ("run-alpha", f"nod-{index:02d}", "region-eu") for index in range(MAX_CONTRACT_LIST_ITEMS)
    )
    full = _frontier(
        completed_work=triples,
        unresolved_work=(("run-alpha", "nod-zz", "region-eu"),),
        recovery_required_reason="writer outcome is unknown",
        state=ContractLifecycleState.RECOVERY_REQUIRED,
    )
    payload = encode_recovery_frontier(full)
    assert len(payload) < MAX_CONTRACT_BYTES
    assert decode_recovery_frontier(payload) == full


def test_document_round_trip_preserves_reserved_characters() -> None:
    tricky = ContractDocument(
        items=(
            ("a=b", "v"),
            ("payload", "a=b;c{d}e[f]g\\h"),
            ("empty-list", ContractList()),
            ("flags", ContractList((True, False, None))),
            ("nested", ContractDocument(items=(("inner", ContractList((0, 9))),))),
        )
    )
    payload = encode_contract_document(tricky)
    canonical = ContractDocument(items=tuple(sorted(tricky.items, key=lambda entry: entry[0])))
    assert decode_contract_document(payload) == canonical
    assert encode_contract_document(canonical) == payload
    assert decode_contract_document(encode_contract_document(canonical)) == canonical
    empty = ContractDocument()
    assert encode_contract_document(empty) == b""
    assert decode_contract_document(encode_contract_document(empty)) == empty


def test_document_to_mapping_and_item_count() -> None:
    assert DOCUMENT.item_count == 3
    assert DOCUMENT.to_mapping() == {
        "mode": "enrich",
        "nested": {"depth": 2},
        "tags": ["alpha", "beta"],
    }
    assert ContractDocument().item_count == 0
    assert ContractDocument().to_mapping() == {}


def test_wire_size_matches_encoding_and_stays_bounded() -> None:
    assert contract_wire_size(ASSIGNMENT) == len(GOLDEN_ASSIGNMENT)
    assert contract_wire_size(RESULT) == len(GOLDEN_RESULT)
    assert contract_wire_size(FRONTIER) == len(GOLDEN_FRONTIER)
    for payload in (GOLDEN_ASSIGNMENT, GOLDEN_RESULT, GOLDEN_FRONTIER):
        assert len(payload) < MAX_CONTRACT_BYTES
    with pytest.raises(TypeError):
        contract_wire_size(CAPABILITIES)  # type: ignore[arg-type]


def test_mutated_envelopes_produce_different_encodings() -> None:
    mutated_assignment = _assignment(input_references=(ASSIGNMENT.input_references[1],))
    assert encode_work_assignment(mutated_assignment) != GOLDEN_ASSIGNMENT
    mutated_result = _result(outcome=ContractOutcome.QUARANTINED)
    assert encode_work_result(mutated_result) != GOLDEN_RESULT
    mutated_frontier = _frontier(control_generation=ControlGeneration(3))
    assert encode_recovery_frontier(mutated_frontier) != GOLDEN_FRONTIER
    reordered = ContractDocument(items=(("rows", 100), ("operation", "normalize")))
    assert encode_contract_document(reordered) == encode_contract_document(DESCRIPTOR)
    changed = ContractDocument(items=(("operation", "normalize"), ("rows", 101)))
    assert encode_contract_document(changed) != encode_contract_document(DESCRIPTOR)


def test_envelope_fields_are_frozen() -> None:
    with pytest.raises(FrozenInstanceError):
        ASSIGNMENT.run_id = "run-mutated"  # type: ignore[misc]
    with pytest.raises(FrozenInstanceError):
        RESULT.failure_detail = "mutated"  # type: ignore[misc]
    with pytest.raises(FrozenInstanceError):
        FRONTIER.state = ContractLifecycleState.CLOSED  # type: ignore[misc]
    with pytest.raises(FrozenInstanceError):
        ControlGeneration(1).value = 4  # type: ignore[misc]


@pytest.mark.parametrize("version", [0, 2, 99])
def test_unknown_contract_versions_fail_closed_everywhere(version: int) -> None:
    with pytest.raises(RunnerContractVersionError):
        _assignment(contract_version=version)
    with pytest.raises(RunnerContractVersionError):
        _result(contract_version=version)
    with pytest.raises(RunnerContractVersionError):
        _frontier(contract_version=version)
    with pytest.raises(RunnerContractVersionError):
        _capabilities(contract_version=version)


@pytest.mark.parametrize("kind", ["assignment", "result", "frontier", "capabilities"])
def test_unknown_protocol_strings_rejected_on_construction(kind: str) -> None:
    unknown = "paritygrid.unknown-protocol.v1"
    if kind == "assignment":
        builder: Callable[..., Any] = _assignment
    elif kind == "result":
        builder = _result
    elif kind == "frontier":
        builder = _frontier
    else:
        builder = _capabilities
    with pytest.raises(RunnerContractVersionError):
        builder(protocol=unknown)


@pytest.mark.parametrize("wire_kind", ["assignment", "result", "frontier"])
@pytest.mark.parametrize("version_line", ["v2", "v99"])
def test_decode_rejects_unknown_protocol_version_digits(wire_kind: str, version_line: str) -> None:
    suffix = b"." + version_line.encode("ascii")
    if wire_kind == "assignment":
        payload = GOLDEN_ASSIGNMENT.replace(b".v1", suffix, 1)
        decode: Callable[[bytes], Any] = decode_work_assignment
    elif wire_kind == "result":
        payload = GOLDEN_RESULT.replace(b".v1", suffix, 1)
        decode = decode_work_result
    else:
        payload = GOLDEN_FRONTIER.replace(b".v1", suffix, 1)
        decode = decode_recovery_frontier
    with pytest.raises(RunnerContractVersionError):
        decode(payload)
    bumped = payload.replace(b"contract_version=1", b"contract_version=7", 1)
    with pytest.raises(RunnerContractVersionError):
        decode(bumped)


def test_decode_rejects_cross_envelope_protocols() -> None:
    with pytest.raises(RunnerContractEncodingError):
        decode_work_assignment(GOLDEN_RESULT)
    with pytest.raises(RunnerContractEncodingError):
        decode_work_result(GOLDEN_FRONTIER)
    with pytest.raises(RunnerContractEncodingError):
        decode_recovery_frontier(GOLDEN_ASSIGNMENT)


def test_public_string_length_bound() -> None:
    longest = "r" * MAX_CONTRACT_STRING_LENGTH
    assert _assignment(run_id=longest).run_id == longest
    with pytest.raises(RunnerContractBoundError):
        _assignment(run_id="r" * (MAX_CONTRACT_STRING_LENGTH + 1))
    with pytest.raises(RunnerContractBoundError):
        _assignment(run_id="")
    with pytest.raises(RunnerContractBoundError):
        _assignment(lease_owner="w" * (MAX_CONTRACT_STRING_LENGTH + 1))
    with pytest.raises(RunnerContractBoundError):
        _assignment(run_id="run-ünïcode")
    with pytest.raises(RunnerContractBoundError):
        _assignment(deadline_utc="2026-08-21T12:00:00.000000")
    with pytest.raises(RunnerContractBoundError):
        _assignment(deadline_utc="2026-13-21T12:00:00.000000Z")


def _list_bound_values(field: str, count: int) -> Any:
    if field == "metrics":
        return tuple(ContractMetric(f"m-{index:02d}", index) for index in range(count))
    if field in ("completed_work", "unresolved_work"):
        return tuple(("run-alpha", f"nod-{index:02d}", "region-eu") for index in range(count))
    prefix = "artifact://inputs" if field == "input_references" else "artifact://outputs"
    return tuple(f"{prefix}/ref-{index:02d}" for index in range(count))


def _list_overflow_value(field: str) -> Any:
    if field == "metrics":
        return (ContractMetric("m-overflow", 0),)
    if field in ("completed_work", "unresolved_work"):
        return (("run-alpha", "nod-zzz", "region-eu"),)
    return ("artifact://inputs/overflow",)


def _bounded_envelope(field: str, values: Any) -> object:
    if field == "input_references":
        return _assignment(input_references=values)
    if field == "metrics":
        return _result(metrics=values)
    if field == "artifact_references":
        return _result(artifact_references=values)
    return _frontier(**{field: values})


@pytest.mark.parametrize(
    "field",
    [
        "input_references",
        "metrics",
        "artifact_references",
        "completed_work",
        "unresolved_work",
    ],
)
def test_envelope_list_item_bounds(field: str) -> None:
    values = _list_bound_values(field, MAX_CONTRACT_LIST_ITEMS)
    accepted = _bounded_envelope(field, values)
    assert len(getattr(accepted, field)) == MAX_CONTRACT_LIST_ITEMS
    with pytest.raises(RunnerContractBoundError):
        _bounded_envelope(field, tuple(values) + _list_overflow_value(field))


def test_contract_list_and_platform_bounds() -> None:
    assert len(ContractList(("v",) * MAX_CONTRACT_LIST_ITEMS).values) == MAX_CONTRACT_LIST_ITEMS
    with pytest.raises(RunnerContractBoundError):
        ContractList(("v",) * (MAX_CONTRACT_LIST_ITEMS + 1))
    platforms = tuple(f"platform-{index}" for index in range(8))
    assert len(_capabilities(platform_requirements=platforms).platform_requirements) == 8
    with pytest.raises(RunnerContractBoundError):
        _capabilities(platform_requirements=(*platforms, "platform-8"))


def test_contract_map_entry_bound() -> None:
    entries = tuple((f"key-{index:02d}", index) for index in range(MAX_CONTRACT_MAP_ENTRIES))
    assert ContractDocument(items=entries).item_count == MAX_CONTRACT_MAP_ENTRIES
    with pytest.raises(RunnerContractBoundError):
        ContractDocument(items=(*entries, ("overflow", 0)))
    with pytest.raises(RunnerContractBoundError):
        ContractDocument(items=(("dup", 1), ("dup", 2)))


def test_document_nesting_bound() -> None:
    depth_4 = ContractDocument(
        items=(
            (
                "leaf",
                ContractList(
                    (
                        ContractDocument(
                            items=(("inner", ContractList((1, 2))),),
                        ),
                    ),
                ),
            ),
        )
    )
    assert decode_contract_document(encode_contract_document(depth_4)) == depth_4
    with pytest.raises(RunnerContractBoundError):
        ContractDocument(items=(("nested", ContractList((depth_4,))),))


def test_integer_and_metric_bounds() -> None:
    assert ContractMetric("rows", MAX_METRIC_VALUE).value == MAX_METRIC_VALUE
    with pytest.raises(RunnerContractBoundError):
        ContractMetric("rows", MAX_METRIC_VALUE + 1)
    with pytest.raises(RunnerContractBoundError):
        ContractMetric("rows", -1)
    with pytest.raises(RunnerContractBoundError):
        _assignment(attempt_number=0)
    with pytest.raises(RunnerContractBoundError):
        _assignment(attempt_number=MAX_METRIC_VALUE + 1)
    with pytest.raises(RunnerContractBoundError):
        _assignment(lease_fence=0)
    with pytest.raises(RunnerContractBoundError):
        ControlGeneration(0)
    with pytest.raises(RunnerContractBoundError):
        ControlGeneration(MAX_METRIC_VALUE + 1)
    with pytest.raises(RunnerContractBoundError):
        ContractDocument(items=(("row-count", MAX_METRIC_VALUE + 1),))


def test_failure_detail_length_bound() -> None:
    longest = "d" * MAX_FAILURE_DETAIL_LENGTH
    assert _result(failure_detail=longest).failure_detail == longest
    with pytest.raises(RunnerContractBoundError):
        _result(failure_detail="d" * (MAX_FAILURE_DETAIL_LENGTH + 1))
    with pytest.raises(RunnerContractBoundError):
        _result(failure_detail="")


@pytest.mark.parametrize(
    "decoder",
    [
        decode_work_assignment,
        decode_work_result,
        decode_recovery_frontier,
        decode_contract_document,
    ],
)
def test_decode_rejects_oversized_bytes(decoder: Callable[[bytes], Any]) -> None:
    with pytest.raises(RunnerContractEncodingError):
        decoder(b"x" * (MAX_CONTRACT_BYTES + 1))


@pytest.mark.parametrize(
    "rejected",
    ["run-token:abc123", "run-password=hunter2", "run-Bearer abc123"],
)
def test_secret_markers_rejected_in_public_fields(rejected: str) -> None:
    with pytest.raises(RunnerContractLeakError):
        _assignment(run_id=rejected)
    with pytest.raises(RunnerContractLeakError):
        _assignment(lease_owner=rejected)
    with pytest.raises(RunnerContractLeakError):
        _assignment(captured_settings_ref=rejected)
    with pytest.raises(RunnerContractLeakError):
        _result(partition_key=rejected)
    with pytest.raises(RunnerContractLeakError):
        _result(failure_detail=f"failed because of {rejected}")


@pytest.mark.parametrize(
    "rejected",
    ["/etc/passwd", "C:\\secrets", "\\\\server\\share", "..\\..\\escape"],
)
@pytest.mark.parametrize("field", ["run_id", "work_item_id", "input_references"])
def test_path_shapes_rejected_in_public_fields(rejected: str, field: str) -> None:
    value: Any = (rejected,) if field == "input_references" else rejected
    with pytest.raises(RunnerContractLeakError):
        _assignment(**{field: value})


def test_validate_public_contract_text_direct_matrix() -> None:
    validate_public_contract_text("run-alpha", "subject")
    with pytest.raises(TypeError):
        validate_public_contract_text(7, "subject")
    with pytest.raises(RunnerContractBoundError):
        validate_public_contract_text("", "subject")
    with pytest.raises(RunnerContractBoundError):
        validate_public_contract_text("tab\tvalue", "subject")
    with pytest.raises(RunnerContractLeakError):
        validate_public_contract_text("token:abc", "subject")
    with pytest.raises(RunnerContractLeakError):
        validate_public_contract_text("notes with password=x", "subject")
    with pytest.raises(RunnerContractLeakError):
        validate_public_contract_text("artifact/../escape", "subject")


@pytest.mark.parametrize("kind", LIVE_KINDS)
@pytest.mark.parametrize(
    "field",
    ["run_id", "attempt_number", "control_generation", "operation_descriptor"],
)
def test_live_objects_rejected_in_assignment_fields(kind: str, field: str) -> None:
    candidate = _live_object(kind)
    try:
        with pytest.raises(RunnerContractLeakError):
            _assignment(**{field: candidate})
    finally:
        _dispose(candidate)


@pytest.mark.parametrize("kind", LIVE_KINDS)
@pytest.mark.parametrize("field", ["outcome", "metrics", "cleanup"])
def test_live_objects_rejected_in_result_fields(kind: str, field: str) -> None:
    candidate = _live_object(kind)
    try:
        with pytest.raises(RunnerContractLeakError):
            _result(**{field: candidate})
    finally:
        _dispose(candidate)


@pytest.mark.parametrize("kind", LIVE_KINDS)
@pytest.mark.parametrize("field", ["plan_fingerprint", "completed_work", "state"])
def test_live_objects_rejected_in_frontier_fields(kind: str, field: str) -> None:
    candidate = _live_object(kind)
    try:
        with pytest.raises(RunnerContractLeakError):
            _frontier(**{field: candidate})
    finally:
        _dispose(candidate)


@pytest.mark.parametrize("kind", ["exception", "dynamic"])
def test_adversarial_objects_rejected_as_leaks(kind: str) -> None:
    candidate = _live_object(kind)
    try:
        with pytest.raises(RunnerContractLeakError):
            _assignment(input_references=(candidate,))
        with pytest.raises(RunnerContractLeakError):
            _assignment(deadline_utc=candidate)
        with pytest.raises(RunnerContractLeakError):
            _frontier(recovery_required_reason=candidate)
    finally:
        _dispose(candidate)


@pytest.mark.parametrize(
    ("builder", "field", "value_factory"),
    [
        (_assignment, "attempt_number", lambda: True),
        (_assignment, "lease_fence", lambda: True),
        (_result, "checkpoint_proposal", lambda: 1),
        (_result, "metrics", lambda: (ContractMetric("rows", True),)),
        (_capabilities, "max_concurrent_work", lambda: True),
    ],
)
def test_bool_int_confusion_rejected_both_directions(
    builder: Callable[..., Any], field: str, value_factory: Callable[[], Any]
) -> None:
    with pytest.raises(TypeError):
        builder(**{field: value_factory()})
    with pytest.raises(TypeError):
        ControlGeneration(True)
    with pytest.raises(TypeError):
        _assignment(protocol=b"paritygrid.work-assignment.v1")  # type: ignore[arg-type]
    with pytest.raises(TypeError):
        _result(metrics=["rows"])  # type: ignore[list-item]


def test_document_distinguishes_bool_and_int_scalars() -> None:
    boolean_document = ContractDocument(items=(("flag", True),))
    integer_document = ContractDocument(items=(("flag", 1),))
    assert encode_contract_document(boolean_document) != encode_contract_document(integer_document)
    assert decode_contract_document(encode_contract_document(boolean_document)) == boolean_document
    assert decode_contract_document(b"flag=true") == boolean_document
    with pytest.raises(RunnerContractEncodingError):
        decode_contract_document(b"flag=true\n")
    with pytest.raises(RunnerContractEncodingError):
        decode_work_result(GOLDEN_RESULT.replace(b"value=int:100", b"value=true", 1))


@pytest.mark.parametrize(
    "payload",
    [
        b"",
        b"paritygrid.work-assignment.v1",
        GOLDEN_ASSIGNMENT[:40],
        GOLDEN_ASSIGNMENT[:200],
        GOLDEN_ASSIGNMENT.replace(b"run_id=", b"run_ref=", 1),
        GOLDEN_ASSIGNMENT.replace(b"operation_descriptor", b"operation_spec", 1),
        GOLDEN_ASSIGNMENT + b"\nextra",
        GOLDEN_ASSIGNMENT.replace(b"deadline_utc=2026", b"deadline_utc=x026", 1),
        GOLDEN_ASSIGNMENT + "é".encode(),
        GOLDEN_ASSIGNMENT.replace(b"attempt_number=1", b"attempt_number=01", 1),
        GOLDEN_ASSIGNMENT.replace(b"input_references=list:[", b"input_references=list:(", 1),
    ],
)
def test_decode_assignment_failure_matrix(payload: bytes) -> None:
    with pytest.raises(RunnerContractError):
        decode_work_assignment(payload)


@pytest.mark.parametrize(
    "payload",
    [
        b"k=v",
        b"k=int:",
        b"k=int:007",
        b"k=int:2147483648",
        b"k=trueX",
        b"=v",
        b"k=str:a\\z",
        b"k=str:a;b=str:c",
        b"k=doc:{inner=str:v",
        b"k=list:[str:a",
    ],
)
def test_decode_contract_document_failure_matrix(payload: bytes) -> None:
    with pytest.raises(RunnerContractEncodingError):
        decode_contract_document(payload)


@pytest.mark.parametrize(
    "decoder",
    [
        decode_work_assignment,
        decode_work_result,
        decode_recovery_frontier,
        decode_contract_document,
    ],
)
@pytest.mark.parametrize("payload", [bytearray(b"x"), "paritygrid.work-assignment.v1", None, 16])
def test_decode_rejects_non_bytes_inputs(decoder: Callable[[bytes], Any], payload: Any) -> None:
    with pytest.raises(RunnerContractEncodingError):
        decoder(payload)


def test_cleanup_evidence_is_idempotent_sorted_and_unique() -> None:
    first = ContractCleanupEvidence(
        status=ContractCleanupStatus.COMPLETED,
        actions=("release-lease", "remove-tmp"),
        idempotency_key="cleanup-run-alpha-0001",
    )
    second = ContractCleanupEvidence(
        status=ContractCleanupStatus.COMPLETED,
        actions=("release-lease", "remove-tmp"),
        idempotency_key="cleanup-run-alpha-0001",
    )
    assert first == second
    assert hash(first) == hash(second)
    with pytest.raises(RunnerContractBoundError):
        ContractCleanupEvidence(
            status=ContractCleanupStatus.PENDING,
            actions=("remove-tmp", "release-lease"),
            idempotency_key=None,
        )
    with pytest.raises(RunnerContractBoundError):
        ContractCleanupEvidence(
            status=ContractCleanupStatus.PENDING,
            actions=("release-lease", "release-lease"),
            idempotency_key=None,
        )


def test_frontier_triples_must_be_unique_and_sorted() -> None:
    duplicate = (("run-alpha", "nod-a", "region-eu"), ("run-alpha", "nod-a", "region-eu"))
    with pytest.raises(RunnerContractBoundError):
        _frontier(completed_work=duplicate)
    unsorted_triples = (("run-alpha", "nod-b", "region-eu"), ("run-alpha", "nod-a", "region-eu"))
    with pytest.raises(RunnerContractBoundError):
        _frontier(unresolved_work=unsorted_triples)
    with pytest.raises(RunnerContractBoundError):
        _frontier(completed_work=(("run-alpha", "nod-a"),))
    with pytest.raises(TypeError):
        _frontier(completed_work=(["run-alpha", "nod-a", "region-eu"],))  # type: ignore[list-item]


def test_work_identity_is_the_scheduled_triple() -> None:
    assert ASSIGNMENT.work_identity() == ("run-alpha", "nod-etl", "region-eu")
    assert RESULT.work_identity() == ASSIGNMENT.work_identity()


def test_reprs_redact_secrets_and_stay_bounded() -> None:
    assignment_repr = repr(ASSIGNMENT)
    assert "worker-1" not in assignment_repr
    assert "lease_owner=<redacted>" in assignment_repr
    result_repr = repr(_result(failure_detail="connection refused"))
    assert "connection refused" not in result_repr
    assert "failure_detail=<redacted>" in result_repr
    assert "worker-1" not in result_repr
    frontier_repr = repr(FRONTIER)
    assert FINGERPRINT in frontier_repr
    assert "worker-1" not in frontier_repr
    assert repr(DOCUMENT) == "ContractDocument(items=3)"


def test_capabilities_bounds_and_validation() -> None:
    assert CAPABILITIES.strategy_id == "strategy-sequential"
    with pytest.raises(RunnerContractBoundError):
        _capabilities(max_concurrent_work=0)
    with pytest.raises(RunnerContractBoundError):
        _capabilities(max_concurrent_work=257)
    with pytest.raises(RunnerContractBoundError):
        _capabilities(max_in_flight_records=0)
    with pytest.raises(RunnerContractBoundError):
        _capabilities(max_in_flight_records=MAX_METRIC_VALUE + 1)
    with pytest.raises(TypeError):
        _capabilities(supports_pause=1)
    with pytest.raises(TypeError):
        _capabilities(strategy_id=object())  # type: ignore[arg-type]
    with pytest.raises(RunnerContractLeakError):
        _capabilities(platform_requirements=("/etc/platform",))


def test_lifecycle_and_outcome_enums_are_closed() -> None:
    assert [state.value for state in ContractLifecycleState] == [
        "new",
        "open",
        "running",
        "quiescing",
        "paused",
        "cancelling",
        "closed",
        "recovery_required",
    ]
    assert [outcome.value for outcome in ContractOutcome] == [
        "succeeded",
        "retry_wait",
        "quarantined",
        "failed",
        "cancelled",
    ]
    with pytest.raises(TypeError):
        _result(outcome="succeeded")  # type: ignore[arg-type]
    with pytest.raises(TypeError):
        _frontier(state="running")  # type: ignore[arg-type]


SYNC_LOOP_OPERATIONS: tuple[Callable[[], object], ...] = (
    lambda: encode_work_assignment(ASSIGNMENT),
    lambda: decode_work_assignment(GOLDEN_ASSIGNMENT),
    lambda: encode_work_result(RESULT),
    lambda: decode_work_result(GOLDEN_RESULT),
    lambda: encode_recovery_frontier(FRONTIER),
    lambda: decode_recovery_frontier(GOLDEN_FRONTIER),
    lambda: encode_contract_document(DOCUMENT),
    lambda: decode_contract_document(GOLDEN_DOCUMENT),
    lambda: contract_wire_size(ASSIGNMENT),
)


@pytest.mark.parametrize("operation", SYNC_LOOP_OPERATIONS)
def test_sync_entry_points_reject_active_event_loop(operation: Callable[[], object]) -> None:
    async def main() -> None:
        assert is_event_loop_running()
        with pytest.raises(RunnerContractLoopError):
            operation()

    asyncio.run(main())
    assert not is_event_loop_running()
    operation()


def test_async_facade_matches_sync_semantics() -> None:
    async def run() -> tuple[WorkAssignmentV1, bytes]:
        decoded = await decode_work_assignment_async(GOLDEN_ASSIGNMENT)
        payload = await encode_work_result_async(RESULT)
        return decoded, payload

    assert not is_event_loop_running()
    decoded, payload = asyncio.run(run())
    assert decoded == decode_work_assignment(GOLDEN_ASSIGNMENT)
    assert payload == GOLDEN_RESULT
    assert decode_work_result(payload) == RESULT


def test_public_surface_is_importable_from_the_execution_package() -> None:
    public_names = list(runner_contract_module.__all__)
    assert len(public_names) == len(set(public_names))
    for name in public_names:
        assert name in execution_package.__all__, name
        assert getattr(execution_package, name) is getattr(runner_contract_module, name)
    for name in (
        "WorkAssignmentV1",
        "WorkResultV1",
        "RecoveryFrontierV1",
        "RunnerContractLoopError",
        "contract_wire_size",
        "validate_public_contract_text",
    ):
        assert name in execution_package.__all__


class _RaisingAttributeObject:
    def __getattr__(self, name: str) -> object:
        raise RuntimeError(f"attribute probe failed for {name}")


def test_attribute_probes_that_raise_are_treated_as_live_leaks() -> None:
    with pytest.raises(RunnerContractLeakError):
        _assignment(run_id=_RaisingAttributeObject())


@pytest.mark.parametrize(
    ("builder", "overrides"),
    [
        (_assignment, {"contract_version": "1"}),
        (_assignment, {"deadline_utc": 20_260_821}),
        (_assignment, {"control_generation": 2}),
        (_assignment, {"operation_descriptor": "doc:{}"}),
        (_assignment, {"input_references": ["artifact://inputs/one"]}),
        (_result, {"protocol": b"paritygrid.work-result.v1"}),
        (_result, {"control_generation": None}),
        (_result, {"cleanup": None}),
        (_result, {"metrics": (("rows", 100),)}),
        (_result, {"artifact_references": "artifact://outputs/one"}),
        (_frontier, {"protocol": 1}),
        (_frontier, {"control_generation": "2"}),
        (_frontier, {"plan_fingerprint": 64}),
        (_frontier, {"completed_work": [("run-alpha", "nod-a", "region-eu")]}),
        (_capabilities, {"protocol": b"paritygrid.strategy-capabilities.v1"}),
        (_capabilities, {"contract_version": None}),
        (_capabilities, {"platform_requirements": ["cpython-3"]}),
    ],
)
def test_field_validators_reject_wrong_value_types(
    builder: Callable[..., Any], overrides: dict[str, Any]
) -> None:
    with pytest.raises(TypeError):
        builder(**overrides)


@pytest.mark.parametrize(
    "deadline",
    [
        "2026/08/21T12:00:00.000000Z",
        "2026-08-21 12:00:00.000000Z",
        "2026-08-21T12:00:00.000000X",
    ],
)
def test_deadline_utc_separator_positions_are_enforced(deadline: str) -> None:
    with pytest.raises(RunnerContractBoundError):
        _assignment(deadline_utc=deadline)


@pytest.mark.parametrize(
    "fingerprint",
    ["z" * 64, "A" * 64, FINGERPRINT[:-1], FINGERPRINT + "0", "0123456789abc"],
)
def test_frontier_fingerprint_must_use_64_lowercase_hex_digits(fingerprint: str) -> None:
    with pytest.raises(RunnerContractBoundError):
        _frontier(plan_fingerprint=fingerprint)


def test_contract_collections_reject_open_value_shapes() -> None:
    with pytest.raises(TypeError):
        ContractList((3.5,))  # type: ignore[arg-type]
    with pytest.raises(TypeError):
        ContractList(values=["v"])  # type: ignore[arg-type]
    with pytest.raises(TypeError):
        ContractDocument(items=[("key", 1)])  # type: ignore[arg-type]
    with pytest.raises(TypeError):
        ContractDocument(items=("key",))  # type: ignore[arg-type]
    with pytest.raises(TypeError):
        ContractDocument(items=((7, "value"),))  # type: ignore[arg-type]
    with pytest.raises(RunnerContractBoundError):
        ContractDocument(items=(("key", 1, True),))  # type: ignore[arg-type]


def test_document_value_nesting_is_depth_bounded() -> None:
    deep = ContractDocument(items=(("leaf", 1),))
    for _ in range(3):
        deep = ContractDocument(items=(("nested", deep),))
    with pytest.raises(RunnerContractBoundError):
        ContractDocument(items=(("nested", deep),))


def test_wire_value_encoder_rejects_open_value_types() -> None:
    poisoned = ContractDocument(items=(("payload", ContractList(("value",))),))
    inner = poisoned.items[0][1]
    object.__setattr__(inner, "values", (3.5,))
    with pytest.raises(TypeError):
        encode_contract_document(poisoned)


DOCUMENT_PARSER_FAILURES: tuple[bytes, ...] = (
    b"k",
    b"k\\z=v",
    b"a{b=v",
    b"k=str:",
    b"k=str:a=b",
    b"k=str:a}",
    b"k=doc:{a=none\n}",
    b"k=doc:{a=doc:{b=doc:{c=doc:{d=none}}}}",
    b"k=list:[list:[list:[list:[none]]]]",
    b"k=list:[doc:{a=none}x]",
    b"k=str:a\nk=str:b",
    b"k=list:[" + b";".join([b"none"] * (MAX_CONTRACT_LIST_ITEMS + 1)) + b"]",
)


@pytest.mark.parametrize("payload", DOCUMENT_PARSER_FAILURES)
def test_document_parser_rejects_malformed_payloads(payload: bytes) -> None:
    with pytest.raises(RunnerContractEncodingError):
        decode_contract_document(payload)


@pytest.mark.parametrize(
    ("payload", "decoder"),
    [
        (
            GOLDEN_ASSIGNMENT.replace(b"contract_version=1\n", b"contract_version=2\n"),
            decode_work_assignment,
        ),
        (
            GOLDEN_RESULT.replace(b"contract_version=1\n", b"contract_version=9\n"),
            decode_work_result,
        ),
        (
            GOLDEN_FRONTIER.replace(b"contract_version=1\n", b"contract_version=0\n"),
            decode_recovery_frontier,
        ),
    ],
)
def test_decode_rejects_wrong_contract_version_digits(
    payload: bytes, decoder: Callable[[bytes], Any]
) -> None:
    with pytest.raises(RunnerContractVersionError):
        decoder(payload)


_ASSIGNMENT_DESCRIPTOR_VALUE = b"doc:{operation=str:normalize\nrows=int:100}"
_ASSIGNMENT_REFERENCES_VALUE = b"list:[str:artifact://inputs/one;str:artifact://inputs/two]"

ASSIGNMENT_READER_FAILURES: tuple[bytes, ...] = (
    GOLDEN_ASSIGNMENT.replace(b"contract_version=1\n", b"contract_version=\n"),
    GOLDEN_ASSIGNMENT.replace(b"contract_version=1\n", b"contract_version=x\n"),
    GOLDEN_ASSIGNMENT.replace(b"attempt_number=1\n", b"attempt_number=x\n"),
    GOLDEN_ASSIGNMENT.replace(b"lease_fence=3\n", b"lease_fence=-3\n"),
    GOLDEN_ASSIGNMENT.replace(b"generation=2\n", b"generation=x\n"),
    GOLDEN_ASSIGNMENT.replace(b"generation=2\n", b"generation=0\n"),
    GOLDEN_ASSIGNMENT.replace(_ASSIGNMENT_REFERENCES_VALUE, b"list:[]x"),
    GOLDEN_ASSIGNMENT.replace(_ASSIGNMENT_DESCRIPTOR_VALUE, b"str:x"),
    GOLDEN_ASSIGNMENT.replace(_ASSIGNMENT_REFERENCES_VALUE, b"none"),
    GOLDEN_ASSIGNMENT.replace(_ASSIGNMENT_REFERENCES_VALUE, b"list:[int:3]"),
)


@pytest.mark.parametrize("payload", ASSIGNMENT_READER_FAILURES)
def test_work_assignment_reader_rejects_malformed_fields(payload: bytes) -> None:
    with pytest.raises(RunnerContractError):
        decode_work_assignment(payload)


_RESULT_METRICS_VALUE = (
    b"list:[doc:{name=str:rows\nvalue=int:100};doc:{name=str:bytes\nvalue=int:2048}]"
)
_RESULT_ARTIFACTS_VALUE = b"list:[str:artifact://outputs/one]"
_RESULT_CLEANUP_VALUE = (
    b"doc:{actions=list:[str:release-lease;str:remove-tmp]\n"
    b"idempotency_key=str:cleanup-run-alpha-0001\n"
    b"status=str:completed}"
)

RESULT_READER_FAILURES: tuple[bytes, ...] = (
    GOLDEN_RESULT.replace(b"contract_version=1\n", b"contract_version=\n"),
    GOLDEN_RESULT.replace(b"attempt_number=1\n", b"attempt_number=x\n"),
    GOLDEN_RESULT.replace(b"generation=2\n", b"generation=x\n"),
    GOLDEN_RESULT.replace(b"outcome=succeeded\n", b"outcome=paused\n"),
    GOLDEN_RESULT.replace(b"checkpoint_proposal=true\n", b"checkpoint_proposal=maybe\n"),
    GOLDEN_RESULT.replace(_RESULT_METRICS_VALUE, b"none"),
    GOLDEN_RESULT.replace(_RESULT_METRICS_VALUE, b"list:[str:rows]"),
    GOLDEN_RESULT.replace(_RESULT_METRICS_VALUE, b"list:[doc:{name=str:rows}]"),
    GOLDEN_RESULT.replace(_RESULT_ARTIFACTS_VALUE, b"none"),
    GOLDEN_RESULT.replace(_RESULT_ARTIFACTS_VALUE, b"list:[int:3]"),
    GOLDEN_RESULT.replace(_RESULT_CLEANUP_VALUE, b"none"),
    GOLDEN_RESULT.replace(_RESULT_CLEANUP_VALUE, b"doc:{status=str:completed}"),
    GOLDEN_RESULT.replace(
        _RESULT_CLEANUP_VALUE, b"doc:{status=int:3\nactions=list:[]\nidempotency_key=none}"
    ),
    GOLDEN_RESULT.replace(
        _RESULT_CLEANUP_VALUE, b"doc:{status=str:completed\nactions=none\nidempotency_key=none}"
    ),
    GOLDEN_RESULT.replace(
        _RESULT_CLEANUP_VALUE,
        b"doc:{status=str:completed\nactions=list:[int:1]\nidempotency_key=none}",
    ),
    GOLDEN_RESULT.replace(
        _RESULT_CLEANUP_VALUE,
        b"doc:{status=str:completed\nactions=list:[]\nidempotency_key=int:7}",
    ),
    GOLDEN_RESULT.replace(
        _RESULT_CLEANUP_VALUE,
        b"doc:{status=str:exploded\nactions=list:[]\nidempotency_key=none}",
    ),
    GOLDEN_RESULT.replace(
        _RESULT_CLEANUP_VALUE,
        b"doc:{status=str:completed\nactions=list:[str:z;str:a]\nidempotency_key=none}",
    ),
    GOLDEN_RESULT.replace(b"run_id=run-alpha\n", b"run_id=run\talpha\n"),
)


@pytest.mark.parametrize("payload", RESULT_READER_FAILURES)
def test_work_result_reader_rejects_malformed_fields(payload: bytes) -> None:
    with pytest.raises(RunnerContractError):
        decode_work_result(payload)


_FRONTIER_COMPLETED_VALUE = (
    b"list:[list:[str:run-alpha;str:nod-a;str:region-eu];"
    b"list:[str:run-alpha;str:nod-b;str:region-eu]]"
)

FRONTIER_READER_FAILURES: tuple[bytes, ...] = (
    GOLDEN_FRONTIER.replace(b"contract_version=1\n", b"contract_version=\n"),
    GOLDEN_FRONTIER.replace(b"generation=2\n", b"generation=x\n"),
    GOLDEN_FRONTIER.replace(b"generation=2\n", b"generation=0\n"),
    GOLDEN_FRONTIER.replace(b"state=running", b"state=halted"),
    GOLDEN_FRONTIER.replace(_FRONTIER_COMPLETED_VALUE, b"none"),
    GOLDEN_FRONTIER.replace(_FRONTIER_COMPLETED_VALUE, b"list:[str:x]"),
    GOLDEN_FRONTIER.replace(_FRONTIER_COMPLETED_VALUE, b"list:[list:[str:a;int:1;str:b]]"),
    GOLDEN_FRONTIER.replace(_FRONTIER_COMPLETED_VALUE, b"list:[list:[str:a;str:b]]"),
    GOLDEN_FRONTIER.replace(FINGERPRINT.encode("ascii"), b"g" * 64),
)


@pytest.mark.parametrize("payload", FRONTIER_READER_FAILURES)
def test_recovery_frontier_reader_rejects_malformed_fields(payload: bytes) -> None:
    with pytest.raises(RunnerContractError):
        decode_recovery_frontier(payload)


def test_encode_entry_points_reject_foreign_envelope_types() -> None:
    with pytest.raises(TypeError):
        encode_contract_document("mode=str:enrich")  # type: ignore[arg-type]
    with pytest.raises(TypeError):
        encode_work_assignment(FRONTIER)  # type: ignore[arg-type]
    with pytest.raises(TypeError):
        encode_work_result(ASSIGNMENT)  # type: ignore[arg-type]
    with pytest.raises(TypeError):
        encode_recovery_frontier(RESULT)  # type: ignore[arg-type]


def test_oversized_envelopes_fail_the_wire_byte_bound() -> None:
    oversized_descriptor = ContractDocument(
        items=tuple(
            (
                f"bulk-{index:02d}",
                ContractList(
                    values=tuple(
                        "v" * MAX_CONTRACT_STRING_LENGTH for _ in range(MAX_CONTRACT_LIST_ITEMS)
                    ),
                ),
            )
            for index in range(MAX_CONTRACT_MAP_ENTRIES)
        )
    )
    oversized = _assignment(operation_descriptor=oversized_descriptor)
    with pytest.raises(RunnerContractEncodingError):
        encode_work_assignment(oversized)
    with pytest.raises(RunnerContractEncodingError):
        contract_wire_size(oversized)

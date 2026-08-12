"""Boundary and corruption tests for repair and audit persistence values."""

from __future__ import annotations

import ast
import inspect
from dataclasses import replace
from datetime import UTC, datetime
from decimal import Decimal
from typing import cast

import pytest
from sqlalchemy.exc import IntegrityError, OperationalError

from paritygrid.adapters.persistence.repositories.repair_audit_common import (
    audit_bounded_text,
    audit_portable_identity,
    audit_positive_int,
    audit_snake_case,
    bounded_text,
    decode_application_result,
    decode_audit_detail,
    decode_effect,
    decode_redacted_document,
    effect_content_fingerprint,
    effect_digest,
    encode_application_result,
    encode_audit_detail,
    encode_effect,
    encode_mismatch_evidence,
    encode_redacted_document,
    portable_identity,
    positive_int,
    require_audit_exact,
    require_exact,
    stored_fingerprint,
    stored_identifier,
    stored_optional_timestamp,
    stored_positive_int,
    stored_timestamp,
    translate_audit_storage_errors,
    translate_repair_storage_errors,
    validate_mismatch_evidence,
)
from paritygrid.adapters.persistence.repositories.repair_audit_mapping import (
    action_from_row,
    approval_from_row,
    audit_from_row,
    plan_from_row,
    validate_aggregate,
)
from paritygrid.application.ports.consistency import RedactedDocument
from paritygrid.application.ports.repair_audit import (
    MAX_PERSISTED_INTEGER,
    AuditCorruptionError,
    AuditEntryRecord,
    AuditInvalidRequestError,
    AuditSequence,
    AuditStorageError,
    AuditStorageUnavailableError,
    InventoryEffect,
    PendingAuditEntry,
    RepairActionCursor,
    RepairActionEffect,
    RepairActionKeyMap,
    RepairActionRecord,
    RepairActionStatus,
    RepairApplicationBeginDisposition,
    RepairApplicationBeginResult,
    RepairApplicationReservation,
    RepairApplicationResult,
    RepairApprovalRecord,
    RepairCorruptionError,
    RepairInvalidRequestError,
    RepairPlanAggregate,
    RepairPlanCursor,
    RepairPlanRecord,
    RepairPlanStatus,
    RepairStorageError,
    RepairStorageUnavailableError,
    validate_audit_page_limit,
    validate_repair_page_limit,
)
from paritygrid.domain.models import (
    ConflictId,
    ConnectorId,
    CurrencyCode,
    InventoryRecord,
    Money,
    RepairActionId,
    RepairPlanId,
    RunId,
    StateFingerprint,
    UtcTimestamp,
)
from paritygrid.domain.repair import RepairAction, RepairActionKind

PLAN_ID = RepairPlanId("rpl_boundary")
ACTION_ID = RepairActionId("rac_boundary")
RUN_ID = RunId("run_boundary")
FINGERPRINT = StateFingerprint("4" * 64)


def timestamp(second: int) -> UtcTimestamp:
    return UtcTimestamp(datetime(2026, 8, 12, 12, 0, second, tzinfo=UTC))


def redacted(**values: object) -> RedactedDocument:
    return RedactedDocument.from_mapping(values)


def inventory(name: str = "Lamp") -> InventoryRecord:
    return InventoryRecord.create(
        sku="LAMP-1",
        name=name,
        quantity=2,
        unit_price=Money(Decimal("4.50"), CurrencyCode("USD"), 2),
        updated_at=timestamp(1),
        connector_id=ConnectorId("con_boundary"),
        source_record_key="record-1",
        attributes={"color": "Blue"},
    )


def create_effect() -> RepairActionEffect:
    return RepairActionEffect.from_action(
        RepairAction(
            ACTION_ID,
            ConflictId("cnf_boundary"),
            FINGERPRINT,
            RepairActionKind.CREATE_TARGET,
            inventory(),
        )
    )


def update_effect() -> RepairActionEffect:
    return RepairActionEffect.from_action(
        RepairAction(
            ACTION_ID,
            ConflictId("cnf_boundary"),
            FINGERPRINT,
            RepairActionKind.UPDATE_TARGET,
            inventory("New Lamp"),
            inventory("Old Lamp"),
        )
    )


def action_row(effect: RepairActionEffect | None = None) -> dict[str, object]:
    value = create_effect() if effect is None else effect
    expected = value.expected_target
    return {
        "repair_action_id": value.action_id.value,
        "repair_plan_id": PLAN_ID.value,
        "run_id": RUN_ID.value,
        "conflict_id": value.conflict_id.value,
        "canonical_key": value.proposed.sku,
        "action_kind": value.kind.value,
        "external_idempotency_key": "repair-boundary-v1",
        "before_sha256": None if expected is None else effect_digest(expected).value,
        "proposed_after_sha256": effect_digest(value.proposed).value,
        "proposed_record_json": encode_effect(value.proposed).text,
        "expected_target_record_json": None if expected is None else encode_effect(expected).text,
        "mismatch_evidence_json": encode_mismatch_evidence(value).text,
        "application_status": "pending",
        "application_result_json": None,
        "target_version": None,
        "applied_at": None,
        "failed_at": None,
        "reconciliation_fingerprint": FINGERPRINT.value,
    }


def plan_row(**overrides: object) -> dict[str, object]:
    base: dict[str, object] = {
        "repair_plan_id": PLAN_ID.value,
        "run_id": RUN_ID.value,
        "reconciliation_fingerprint": FINGERPRINT.value,
        "content_fingerprint": "5" * 64,
        "status": "proposed",
        "row_version": 1,
        "created_at": str(timestamp(2)),
        "applying_at": None,
        "applied_at": None,
        "rejected_at": None,
        "failed_at": None,
        "failure_detail": None,
    }
    base.update(overrides)
    return base


def test_port_values_reject_wrong_shapes_and_redact_reprs() -> None:
    effect = create_effect()
    assert "proposed=<redacted>" in repr(effect)
    with pytest.raises(TypeError):
        InventoryEffect.from_record(cast(InventoryRecord, object()))
    with pytest.raises(TypeError):
        RepairActionEffect.from_action(cast(RepairAction, object()))
    with pytest.raises(ValueError, match="absent target"):
        RepairActionEffect(
            effect.action_id,
            effect.conflict_id,
            effect.reconciliation_fingerprint,
            RepairActionKind.CREATE_TARGET,
            effect.proposed,
            effect.proposed,
            (),
        )
    updated = update_effect()
    with pytest.raises(ValueError, match="not canonical"):
        RepairActionEffect(
            updated.action_id,
            updated.conflict_id,
            updated.reconciliation_fingerprint,
            updated.kind,
            updated.proposed,
            updated.expected_target,
            (),
        )
    keys = RepairActionKeyMap.from_mapping({ACTION_ID: "effect-key"})
    assert keys.to_mapping() == {ACTION_ID: "effect-key"}
    assert "keys=<redacted>" in repr(keys)
    with pytest.raises(ValueError, match="identities must be unique"):
        RepairActionKeyMap(((ACTION_ID, "same"), (ACTION_ID, "other")))
    with pytest.raises(ValueError, match="keys must be unique"):
        RepairActionKeyMap(((ACTION_ID, "same"), (RepairActionId("rac_other"), "same")))
    result = RepairApplicationResult(1, redacted(outcome="ok"))
    assert "detail=<redacted>" in repr(result)
    with pytest.raises(TypeError):
        RepairApplicationResult(cast(int, True), redacted(outcome="bad"))
    with pytest.raises(ValueError, match="outside the supported range"):
        RepairApplicationResult(0, redacted(outcome="bad"))


@pytest.mark.parametrize("value", [0, MAX_PERSISTED_INTEGER + 1, True, "1"])
def test_audit_sequence_rejects_out_of_range_values(value: object) -> None:
    expected = TypeError if type(value) is not int else ValueError
    with pytest.raises(expected):
        AuditSequence(cast(int, value))


def test_begin_result_requires_reservation_only_for_started() -> None:
    plan_record = plan_from_row(plan_row())
    aggregate = RepairPlanAggregate(plan_record, None, ())
    reservation = RepairApplicationReservation(
        PLAN_ID, RUN_ID, FINGERPRINT, StateFingerprint("5" * 64), timestamp(3), 3
    )
    with pytest.raises(ValueError, match="only a started"):
        RepairApplicationBeginResult(RepairApplicationBeginDisposition.STARTED, aggregate, None)
    with pytest.raises(ValueError, match="only a started"):
        RepairApplicationBeginResult(
            RepairApplicationBeginDisposition.FAILED_REPLAY, aggregate, reservation
        )


@pytest.mark.parametrize("limit", [0, 101, True, "1"])
def test_page_limits_are_exact(limit: object) -> None:
    with pytest.raises(RepairInvalidRequestError):
        validate_repair_page_limit(limit)
    with pytest.raises(AuditInvalidRequestError):
        validate_audit_page_limit(limit)
    assert validate_repair_page_limit(100) == 100
    assert validate_audit_page_limit(1) == 1


def test_validation_helpers_reject_unsafe_text_and_numbers() -> None:
    assert bounded_text("Élia", "actor", 8) == "Élia"
    assert portable_identity("corr:1", "correlation", 8) == "corr:1"
    assert positive_int(1, "version") == 1
    for value in (object(), "", "e\u0301", "line\nfeed"):
        with pytest.raises(RepairInvalidRequestError):
            bounded_text(value, "field", 8)
    with pytest.raises(RepairInvalidRequestError, match="unsupported"):
        bounded_text("\n", "field", 8)
    with pytest.raises(RepairInvalidRequestError):
        portable_identity("space here", "field", 20)
    with pytest.raises(RepairInvalidRequestError):
        positive_int(True, "version")
    assert audit_snake_case("repair_plan_applied", "operation", 96) == ("repair_plan_applied")
    with pytest.raises(AuditInvalidRequestError):
        audit_snake_case("RepairPlan", "operation", 96)
    with pytest.raises(AuditInvalidRequestError):
        audit_portable_identity("not portable", "identity", 32)
    with pytest.raises(AuditInvalidRequestError):
        audit_positive_int(0, "version")
    with pytest.raises(AuditInvalidRequestError):
        audit_bounded_text("", "actor", 8)
    with pytest.raises(RepairInvalidRequestError):
        require_exact(object(), RepairPlanId, "plan")
    with pytest.raises(AuditInvalidRequestError):
        require_audit_exact(object(), AuditSequence, "sequence")


def test_effect_and_result_codecs_reject_corrupt_storage() -> None:
    effect = update_effect()
    encoded = encode_effect(effect.proposed)
    assert decode_effect(encoded.text, "effect") == effect.proposed
    validate_mismatch_evidence(encode_mismatch_evidence(effect).text, effect)
    for value in (None, "[]", '{"sku":"LAMP-1"}', '{"sku":1}'):
        with pytest.raises(RepairCorruptionError):
            decode_effect(value, "effect")
    with pytest.raises(RepairCorruptionError):
        validate_mismatch_evidence("[]", effect)
    result = RepairApplicationResult(1, redacted(outcome="applied"))
    assert decode_application_result(encode_application_result(result).text) == result
    for value in (None, "[]", "{}", '{"detail":[],"schema_version":1}'):
        with pytest.raises(RepairCorruptionError):
            decode_application_result(value)
    assert decode_redacted_document('{"safe":true}', "detail") == redacted(safe=True)
    with pytest.raises(RepairCorruptionError):
        decode_redacted_document('{"api_key":"canary"}', "detail")
    assert decode_audit_detail('{"safe":true}') == redacted(safe=True)
    with pytest.raises(AuditCorruptionError):
        decode_audit_detail('{"secret":"canary"}')
    assert encode_audit_detail(redacted(safe=True)).text == '{"safe":true}'


def test_stored_scalar_helpers_translate_corruption() -> None:
    assert stored_identifier(PLAN_ID.value, RepairPlanId, "plan") == PLAN_ID
    assert stored_fingerprint(FINGERPRINT.value, "fingerprint") == FINGERPRINT
    assert stored_positive_int(1, "version") == 1
    assert stored_timestamp(str(timestamp(1)), "time") == timestamp(1)
    assert stored_optional_timestamp(None, "time") is None
    for operation in (
        lambda: stored_identifier(1, RepairPlanId, "plan"),
        lambda: stored_fingerprint("x", "fingerprint"),
        lambda: stored_positive_int(True, "version"),
        lambda: stored_timestamp("bad", "time"),
    ):
        with pytest.raises(RepairCorruptionError):
            operation()


def test_mapping_detects_action_and_plan_corruption() -> None:
    valid = action_row()
    assert action_from_row(valid).effect == create_effect()
    mutations = (
        {"action_kind": "delete_target"},
        {"canonical_key": "OTHER"},
        {"before_sha256": "1" * 64},
        {"proposed_after_sha256": "1" * 64},
        {"mismatch_evidence_json": "[]" if create_effect().mismatches else "[{}]"},
        {"application_status": "invalid"},
        {"application_status": "applied"},
    )
    for mutation in mutations:
        row = dict(valid)
        row.update(mutation)
        with pytest.raises(RepairCorruptionError):
            action_from_row(row)
    with pytest.raises(RepairCorruptionError):
        plan_from_row(plan_row(status="invalid"))
    with pytest.raises(RepairCorruptionError):
        approval_from_row(
            {
                "repair_plan_id": PLAN_ID.value,
                "reconciliation_fingerprint": FINGERPRINT.value,
                "approved_by": "not portable",
                "approved_at": str(timestamp(3)),
                "correlation_id": "corr-1",
                "approval_schema_version": 1,
                "detail_json": "{}",
            }
        )


def test_audit_mapping_accepts_gaps_and_rejects_corruption() -> None:
    row: dict[str, object] = {
        "sequence_number": 10,
        "actor": "operator-1",
        "operation": "repair_plan_applied",
        "object_kind": "repair_plan",
        "object_id": PLAN_ID.value,
        "correlation_id": "corr-1",
        "occurred_at": str(timestamp(3)),
        "detail_schema_version": 1,
        "detail_json": "{}",
    }
    assert audit_from_row(row).sequence == AuditSequence(10)
    for key, value in (
        ("sequence_number", 0),
        ("operation", "BadOperation"),
        ("object_id", "not portable"),
        ("occurred_at", "bad"),
        ("detail_json", "[]"),
    ):
        corrupt = dict(row)
        corrupt[key] = value
        with pytest.raises(AuditCorruptionError):
            audit_from_row(corrupt)


def test_storage_translation_is_typed_redacted_and_unchained() -> None:
    canary = "secret-canary"

    @translate_repair_storage_errors
    def repair_operational() -> None:
        raise OperationalError("select secret-canary", {"key": canary}, Exception())

    @translate_repair_storage_errors
    def repair_integrity() -> None:
        raise IntegrityError("insert secret-canary", {"key": canary}, Exception())

    @translate_audit_storage_errors
    def audit_operational() -> None:
        raise OperationalError("select secret-canary", {"key": canary}, Exception())

    @translate_audit_storage_errors
    def audit_integrity() -> None:
        raise IntegrityError("insert secret-canary", {"key": canary}, Exception())

    for operation, expected, message in (
        (repair_operational, RepairStorageUnavailableError, "Repair storage is unavailable."),
        (repair_integrity, RepairStorageError, "Repair storage operation failed."),
        (audit_operational, AuditStorageUnavailableError, "Audit storage is unavailable."),
        (audit_integrity, AuditStorageError, "Audit storage operation failed."),
    ):
        with pytest.raises(expected) as captured:
            operation()
        error = captured.value
        assert str(error) == message
        assert error.__cause__ is None
        assert error.__context__ is None
        assert canary not in repr(error)


def test_application_runtime_errors_are_not_translated() -> None:
    @translate_repair_storage_errors
    def fail() -> None:
        raise RuntimeError("application failure")

    with pytest.raises(RuntimeError, match="application failure"):
        fail()


def test_remaining_port_guards_and_repr_contracts() -> None:
    base = create_effect()
    with pytest.raises(ValueError, match="canonical values"):
        InventoryEffect(
            base.proposed.sku,
            "Cafe\u0301 Lamp",
            base.proposed.quantity,
            base.proposed.unit_price,
            base.proposed.updated_at,
            base.proposed.attributes,
        )
    invalid_fields = (
        ("action_id", object()),
        ("conflict_id", object()),
        ("reconciliation_fingerprint", object()),
        ("kind", object()),
        ("proposed", object()),
        ("mismatches", [object()]),
    )
    for field_name, value in invalid_fields:
        values: dict[str, object] = {
            "action_id": base.action_id,
            "conflict_id": base.conflict_id,
            "reconciliation_fingerprint": base.reconciliation_fingerprint,
            "kind": base.kind,
            "proposed": base.proposed,
            "expected_target": None,
            "mismatches": (),
        }
        values[field_name] = value
        with pytest.raises(TypeError):
            RepairActionEffect(**cast(dict[str, object], values))  # type: ignore[arg-type]
    with pytest.raises(TypeError, match="expected InventoryEffect"):
        RepairActionEffect(
            base.action_id,
            base.conflict_id,
            base.reconciliation_fingerprint,
            RepairActionKind.UPDATE_TARGET,
            base.proposed,
            None,
            (),
        )
    for value in (
        cast(tuple[tuple[RepairActionId, str], ...], []),
        cast(tuple[tuple[RepairActionId, str], ...], ([ACTION_ID, "key"],)),
        cast(tuple[tuple[RepairActionId, str], ...], ((ACTION_ID,),)),
        cast(tuple[tuple[RepairActionId, str], ...], ((object(), "key"),)),
    ):
        with pytest.raises(TypeError):
            RepairActionKeyMap(value)
    with pytest.raises(TypeError, match="must be a mapping"):
        RepairActionKeyMap.from_mapping(cast(dict[RepairActionId, str], []))
    with pytest.raises(TypeError, match="RedactedDocument"):
        RepairApplicationResult(1, cast(RedactedDocument, object()))
    action = action_from_row(action_row())
    plan_record = replace(
        plan_from_row(plan_row()),
        content_fingerprint=effect_content_fingerprint(PLAN_ID, FINGERPRINT, (base,)),
    )
    approval = RepairApprovalRecord(
        PLAN_ID,
        FINGERPRINT,
        "operator-1",
        timestamp(3),
        "corr-1",
        1,
        redacted(safe=True),
    )
    reservation = RepairApplicationReservation(
        PLAN_ID,
        RUN_ID,
        FINGERPRINT,
        plan_record.content_fingerprint,
        timestamp(4),
        3,
    )
    assert "approval=<redacted>" in repr(approval)
    assert "failure=<redacted>" in repr(plan_record)
    assert "effect=<redacted>" in repr(action)
    assert "identity=<redacted>" in repr(reservation)
    assert "identity=<redacted>" in repr(RepairPlanCursor(timestamp(2), PLAN_ID))
    assert "identity=<redacted>" in repr(RepairActionCursor("LAMP-1", ACTION_ID))
    assert int(AuditSequence(1)) == 1
    pending = PendingAuditEntry(
        "operator-1",
        "repair_plan_applied",
        "repair_plan",
        PLAN_ID.value,
        "corr-1",
        timestamp(5),
        1,
        redacted(safe=True),
    )
    audit_record = AuditEntryRecord(
        AuditSequence(1),
        pending.actor,
        pending.operation,
        pending.object_kind,
        pending.object_id,
        pending.correlation_id,
        pending.occurred_at,
        pending.detail_schema_version,
        pending.detail,
    )
    assert "actor=<redacted>" in repr(pending)
    assert "actor=<redacted>" in repr(audit_record)


def test_codec_defensive_shapes_and_all_mismatch_value_kinds(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    base = create_effect().proposed
    alternate = InventoryRecord.create(
        sku=base.sku,
        name="Other Lamp",
        quantity=9,
        unit_price=Money(Decimal("8.75"), CurrencyCode("USD"), 2),
        updated_at=timestamp(2),
        connector_id=ConnectorId("con_boundary"),
        source_record_key="record-2",
        attributes={"color": "Red"},
    )
    all_fields = RepairActionEffect.from_action(
        RepairAction(
            ACTION_ID,
            ConflictId("cnf_boundary"),
            FINGERPRINT,
            RepairActionKind.UPDATE_TARGET,
            alternate,
            inventory(),
        )
    )
    assert len(all_fields.mismatches) == 5
    assert encode_mismatch_evidence(all_fields).text.startswith("[")
    with pytest.raises(RepairCorruptionError):
        validate_mismatch_evidence(None, all_fields)
    with pytest.raises(RepairCorruptionError):
        validate_mismatch_evidence("not-json", all_fields)
    malformed_effects = (
        '{"attributes":[],"name":"Lamp","quantity":2,"sku":"LAMP-1",'
        '"unit_price":[],"updated_at":"2026-08-12T12:00:01.000000Z"}',
        '{"attributes":[],"name":"Lamp","quantity":2,"sku":"LAMP-1",'
        '"unit_price":{"currency":"USD"},"updated_at":"2026-08-12T12:00:01.000000Z"}',
        '{"attributes":{},"name":"Lamp","quantity":2,"sku":"LAMP-1",'
        '"unit_price":{"currency":"USD","minor_unit_exponent":2,"minor_units":450},'
        '"updated_at":"2026-08-12T12:00:01.000000Z"}',
        '{"attributes":["bad"],"name":"Lamp","quantity":2,"sku":"LAMP-1",'
        '"unit_price":{"currency":"USD","minor_unit_exponent":2,"minor_units":450},'
        '"updated_at":"2026-08-12T12:00:01.000000Z"}',
        '{"attributes":[["bad"]],"name":"Lamp","quantity":2,"sku":"LAMP-1",'
        '"unit_price":{"currency":"USD","minor_unit_exponent":2,"minor_units":450},'
        '"updated_at":"2026-08-12T12:00:01.000000Z"}',
        '{"attributes":[],"name":1,"quantity":2,"sku":"LAMP-1",'
        '"unit_price":{"currency":"USD","minor_unit_exponent":2,"minor_units":450},'
        '"updated_at":"2026-08-12T12:00:01.000000Z"}',
        '{"attributes":[],"name":"Lamp","quantity":"2","sku":"LAMP-1",'
        '"unit_price":{"currency":"USD","minor_unit_exponent":2,"minor_units":450},'
        '"updated_at":"2026-08-12T12:00:01.000000Z"}',
    )
    for value in malformed_effects:
        with pytest.raises(RepairCorruptionError):
            decode_effect(value, "effect")
    for value in (
        '{"detail":{},"schema_version":0}',
        '{"detail":{},"schema_version":"1"}',
    ):
        with pytest.raises(RepairCorruptionError):
            decode_application_result(value)
    with pytest.raises(AuditCorruptionError):
        decode_audit_detail(None)
    with pytest.raises(RepairCorruptionError):
        stored_timestamp(None, "time")
    with pytest.raises(RepairInvalidRequestError, match="exceeds"):
        encode_redacted_document(redacted(safe="value"), "detail", maximum_bytes=1)
    import paritygrid.adapters.persistence.repositories.repair_audit_common as common

    monkeypatch.setattr(common, "MAX_CANONICAL_DOCUMENT_BYTES", 1)
    with pytest.raises(AuditInvalidRequestError, match="exceeds"):
        encode_audit_detail(redacted(safe="value"))


def test_mapping_action_terminal_shapes_and_aggregate_corruption() -> None:
    base_effect = create_effect()
    base_row = action_row(base_effect)
    create_with_target = dict(base_row)
    create_with_target["expected_target_record_json"] = encode_effect(base_effect.proposed).text
    with pytest.raises(RepairCorruptionError, match="create repair action shape"):
        action_from_row(create_with_target)
    update = update_effect()
    update_without_target = action_row(update)
    update_without_target["expected_target_record_json"] = None
    with pytest.raises(RepairCorruptionError, match="update repair action shape"):
        action_from_row(update_without_target)
    applied_row = dict(base_row)
    applied_row.update(
        application_status="applied",
        application_result_json=encode_application_result(
            RepairApplicationResult(1, redacted(outcome="ok"))
        ).text,
        target_version=1,
        applied_at=str(timestamp(4)),
    )
    applied = action_from_row(applied_row)
    failed_row = dict(base_row)
    failed_row.update(
        application_status="failed",
        application_result_json=encode_application_result(
            RepairApplicationResult(1, redacted(outcome="failed"))
        ).text,
        failed_at=str(timestamp(4)),
    )
    failed = action_from_row(failed_row)
    assert applied.status is RepairActionStatus.APPLIED
    assert failed.status is RepairActionStatus.FAILED
    content = effect_content_fingerprint(PLAN_ID, FINGERPRINT, (base_effect,))
    proposed = replace(plan_from_row(plan_row()), content_fingerprint=content)
    assert validate_aggregate(proposed, None, (action_from_row(base_row),)).plan == proposed
    corrupt_cases: tuple[
        tuple[RepairPlanRecord, RepairApprovalRecord | None, tuple[RepairActionRecord, ...]], ...
    ] = (
        (proposed, None, ()),
        (
            replace(proposed, content_fingerprint=StateFingerprint("9" * 64)),
            None,
            (action_from_row(base_row),),
        ),
        (replace(proposed, row_version=2), None, (action_from_row(base_row),)),
        (proposed, None, (replace(action_from_row(base_row), run_id=RunId("run_other")),)),
    )
    for plan_value, approval, actions in corrupt_cases:
        with pytest.raises(RepairCorruptionError):
            validate_aggregate(plan_value, approval, actions)
    approval = RepairApprovalRecord(
        PLAN_ID,
        FINGERPRINT,
        "operator-1",
        timestamp(3),
        "corr-1",
        1,
        redacted(safe=True),
    )
    with pytest.raises(RepairCorruptionError, match="approval identity"):
        validate_aggregate(
            replace(proposed, status=RepairPlanStatus.APPROVED, row_version=2),
            replace(approval, repair_plan_id=RepairPlanId("rpl_other")),
            (action_from_row(base_row),),
        )
    with pytest.raises(RepairCorruptionError, match="approval chronology"):
        validate_aggregate(
            replace(
                proposed,
                status=RepairPlanStatus.APPROVED,
                row_version=2,
                created_at=timestamp(4),
            ),
            approval,
            (action_from_row(base_row),),
        )


def test_remaining_mapping_corruption_branches() -> None:
    first = action_from_row(action_row())
    second_effect = replace(
        create_effect(),
        action_id=RepairActionId("rac_second"),
        conflict_id=ConflictId("cnf_second"),
        proposed=replace(create_effect().proposed, sku="LAMP-2"),
    )
    second_row = action_row(second_effect)
    second_row["canonical_key"] = "LAMP-2"
    second = action_from_row(second_row)
    content = effect_content_fingerprint(PLAN_ID, FINGERPRINT, (first.effect, second.effect))
    proposed = replace(plan_from_row(plan_row()), content_fingerprint=content)
    for actions in (
        (second, first),
        (first, replace(second, effect=replace(second.effect, action_id=first.effect.action_id))),
        (
            first,
            replace(second, effect=replace(second.effect, conflict_id=first.effect.conflict_id)),
        ),
        (first, replace(second, effect=replace(second.effect, proposed=first.effect.proposed))),
    ):
        with pytest.raises(RepairCorruptionError):
            validate_aggregate(proposed, None, actions)
    audit_row: dict[str, object] = {
        "sequence_number": "1",
        "actor": "operator-1",
        "operation": "repair_plan_applied",
        "object_kind": "repair_plan",
        "object_id": None,
        "correlation_id": "corr-1",
        "occurred_at": 1,
        "detail_schema_version": 1,
        "detail_json": "{}",
    }
    with pytest.raises(AuditCorruptionError):
        audit_from_row(audit_row)
    audit_row["sequence_number"] = 1
    with pytest.raises(AuditCorruptionError):
        audit_from_row(audit_row)
    for key, value in (
        ("status", 1),
        ("status", "unknown"),
    ):
        row = plan_row()
        row[key] = value
        with pytest.raises(RepairCorruptionError):
            plan_from_row(row)
    for key, value in (
        ("application_status", 1),
        ("action_kind", 1),
        ("external_idempotency_key", "e\u0301"),
        ("canonical_key", ""),
        ("canonical_key", "e\u0301"),
    ):
        row = action_row()
        row[key] = value
        with pytest.raises(RepairCorruptionError):
            action_from_row(row)


def test_encoding_translation_and_chronology_defenses(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def invalid_mapping(_document: RedactedDocument) -> dict[str, object]:
        raise ValueError("invalid mapping")

    monkeypatch.setattr(RedactedDocument, "to_mapping", invalid_mapping)
    with pytest.raises(RepairInvalidRequestError, match="invalid"):
        encode_redacted_document(redacted(safe=True), "detail")
    with pytest.raises(AuditInvalidRequestError, match="invalid"):
        encode_audit_detail(redacted(safe=True))
    monkeypatch.undo()

    pending = action_from_row(action_row())
    approval = RepairApprovalRecord(
        PLAN_ID,
        FINGERPRINT,
        "operator-1",
        timestamp(3),
        "corr-1",
        1,
        redacted(safe=True),
    )
    content = effect_content_fingerprint(PLAN_ID, FINGERPRINT, (pending.effect,))
    applying_too_early = replace(
        plan_from_row(plan_row()),
        content_fingerprint=content,
        status=RepairPlanStatus.APPLYING,
        row_version=3,
        applying_at=timestamp(2),
    )
    with pytest.raises(RepairCorruptionError, match="application chronology"):
        validate_aggregate(applying_too_early, approval, (pending,))
    applied_row = action_row()
    applied_row.update(
        application_status="applied",
        application_result_json=encode_application_result(
            RepairApplicationResult(1, redacted(outcome="ok"))
        ).text,
        target_version=1,
        applied_at=str(timestamp(3)),
    )
    action_too_early = action_from_row(applied_row)
    applying = replace(
        applying_too_early,
        applying_at=timestamp(4),
        row_version=4,
    )
    with pytest.raises(RepairCorruptionError, match="action chronology"):
        validate_aggregate(applying, approval, (action_too_early,))
    applied_late_row = dict(applied_row)
    applied_late_row["applied_at"] = str(timestamp(6))
    action_late = action_from_row(applied_late_row)
    terminal_too_early = replace(
        applying,
        status=RepairPlanStatus.APPLIED,
        row_version=5,
        applied_at=timestamp(5),
    )
    with pytest.raises(RepairCorruptionError, match="plan chronology"):
        validate_aggregate(terminal_too_early, approval, (action_late,))


def test_ports_are_dependency_neutral_and_repositories_do_not_own_transactions() -> None:
    import paritygrid.application.ports.repair_audit as port_module
    from paritygrid.adapters.persistence.repositories import audits, repairs

    tree = ast.parse(inspect.getsource(port_module))
    imported = {
        name.name for node in ast.walk(tree) if isinstance(node, ast.Import) for name in node.names
    }
    imported.update(
        node.module or "" for node in ast.walk(tree) if isinstance(node, ast.ImportFrom)
    )
    assert not any(name.startswith("sqlalchemy") or ".adapters." in name for name in imported)
    for module in (repairs, audits):
        source = inspect.getsource(module)
        for forbidden in (
            ".begin(",
            ".begin_nested(",
            ".commit(",
            ".rollback(",
            ".close(",
        ):
            assert forbidden not in source

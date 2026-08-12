"""Golden verification for the versioned canonical byte protocol."""

from dataclasses import dataclass
from decimal import Decimal, localcontext

import pytest

from paritygrid.domain.canonical import CanonicalEncoder, encode_canonical
from paritygrid.domain.errors import CanonicalEncodingError, CanonicalErrorCode
from paritygrid.domain.execution import (
    FailureClassification,
    FailureDisposition,
    RunState,
    WorkItemState,
)
from paritygrid.domain.models import (
    ArtifactId,
    AttemptNumber,
    ConflictId,
    ConnectorId,
    CurrencyCode,
    Duration,
    InventoryAttributes,
    InventoryRecord,
    Money,
    NodeId,
    PipelineId,
    PipelineVersion,
    RepairActionId,
    RepairPlanId,
    RunId,
    StateFingerprint,
    UtcTimestamp,
    WorkItemId,
)
from paritygrid.domain.pipeline import NodeKind, PartitionKey, PipelineEdge, PipelineNode, PortName
from paritygrid.domain.reconciliation import (
    FieldMismatch,
    ReconciliationClassification,
    ReconciliationField,
    ReconciliationOutcome,
)
from paritygrid.domain.repair import RepairAction, RepairActionKind, RepairPlan

STATE = StateFingerprint("1" * 64)


def _envelope(type_tag: str, value_json: bytes) -> bytes:
    return (
        b'{"format":"paritygrid-canonical","type":"'
        + type_tag.encode("ascii")
        + b'","value":'
        + value_json
        + b',"version":1}'
    )


def _record(
    *,
    sku: str = "SKU-001",
    name: str = "Caf\u00e9 Widget",
    quantity: int = 10,
    price: str = "12.30",
    connector: str = "con_source-api",
    source_key: str = "source-001",
) -> InventoryRecord:
    return InventoryRecord.create(
        sku=sku,
        name=name,
        quantity=quantity,
        unit_price=Money.parse(f"USD {price}"),
        updated_at=UtcTimestamp.parse("2026-08-12T10:00:00Z"),
        connector_id=ConnectorId(connector),
        source_record_key=source_key,
        attributes={"size": "M", "color": "blue"},
    )


def _create_action(
    *,
    action_id: str = "rac_create-001",
    conflict_id: str = "cnf_missing-001",
    sku: str = "SKU-001",
) -> RepairAction:
    return RepairAction.from_outcome(
        action_id=RepairActionId(action_id),
        conflict_id=ConflictId(conflict_id),
        state_fingerprint=STATE,
        outcome=ReconciliationOutcome((_record(sku=sku),), ()),
    )


@pytest.mark.parametrize(
    ("value", "type_tag", "value_json"),
    [
        (PipelineId("pip_catalog-001"), "pipeline-id", b'"pip_catalog-001"'),
        (NodeId("nod_source-001"), "node-id", b'"nod_source-001"'),
        (ConnectorId("con_source-api"), "connector-id", b'"con_source-api"'),
        (RunId("run_demo-001"), "run-id", b'"run_demo-001"'),
        (WorkItemId("wrk_batch-001"), "work-item-id", b'"wrk_batch-001"'),
        (ArtifactId("art_records-001"), "artifact-id", b'"art_records-001"'),
        (ConflictId("cnf_missing-001"), "conflict-id", b'"cnf_missing-001"'),
        (RepairPlanId("rpl_inventory-001"), "repair-plan-id", b'"rpl_inventory-001"'),
        (RepairActionId("rac_create-001"), "repair-action-id", b'"rac_create-001"'),
        (PipelineVersion(7), "pipeline-version", b"7"),
        (AttemptNumber(3), "attempt-number", b"3"),
        (
            UtcTimestamp.parse("2026-08-12T13:00:00+03:00"),
            "utc-timestamp",
            b'"2026-08-12T10:00:00.000000Z"',
        ),
        (Duration(1_234_567), "duration", b"1234567"),
        (CurrencyCode("USD"), "currency-code", b'"USD"'),
        (
            StateFingerprint("abcdef01" * 8),
            "state-fingerprint",
            b'"abcdef01abcdef01abcdef01abcdef01abcdef01abcdef01abcdef01abcdef01"',
        ),
        (NodeKind("source.http"), "node-kind", b'"source.http"'),
        (PortName("records-out"), "port-name", b'"records-out"'),
        (PartitionKey("region:emea.001"), "partition-key", b'"region:emea.001"'),
    ],
)
def test_scalar_family_golden_bytes(value: object, type_tag: str, value_json: bytes) -> None:
    assert encode_canonical(value) == _envelope(type_tag, value_json)


def test_money_and_attributes_have_exact_non_lossy_encodings() -> None:
    money = Money(Decimal("12.3"), CurrencyCode("USD"), 2)
    attributes = InventoryAttributes.from_mapping({"zeta": "last", "alpha": "\u00e9"})

    assert encode_canonical(money) == _envelope(
        "money",
        b'{"currency":"USD","minor_unit_exponent":2,"minor_units":1230}',
    )
    assert encode_canonical(attributes) == _envelope(
        "inventory-attributes",
        b'[["alpha","\\u00e9"],["zeta","last"]]',
    )


def test_inventory_record_golden_bytes_lock_unicode_and_field_order() -> None:
    assert encode_canonical(_record()) == (
        b'{"format":"paritygrid-canonical","type":"inventory-record","value":'
        b'{"attributes":[["color","blue"],["size","M"]],'
        b'"connector_id":"con_source-api","name":"Caf\\u00e9 Widget","quantity":10,'
        b'"sku":"SKU-001","source_record_key":"source-001",'
        b'"unit_price":{"currency":"USD","minor_unit_exponent":2,"minor_units":1230},'
        b'"updated_at":"2026-08-12T10:00:00.000000Z"},"version":1}'
    )


def test_pipeline_values_have_versioned_golden_bytes_separate_from_local_encoding() -> None:
    node = PipelineNode(
        node_id=NodeId("nod_transform-001"),
        kind=NodeKind("transform.normalize"),
        input_ports=(PortName("z-records"), PortName("a-records")),
        output_ports=(PortName("canonical-records"),),
    )
    edge = PipelineEdge(
        source_node_id=NodeId("nod_source-001"),
        source_port=PortName("records"),
        target_node_id=NodeId("nod_transform-001"),
        target_port=PortName("a-records"),
    )

    assert encode_canonical(node) == _envelope(
        "pipeline-node",
        b'{"id":"nod_transform-001","inputs":["a-records","z-records"],'
        b'"kind":"transform.normalize","outputs":["canonical-records"]}',
    )
    assert encode_canonical(edge) == _envelope(
        "pipeline-edge",
        b'{"source":{"node_id":"nod_source-001","port":"records"},'
        b'"target":{"node_id":"nod_transform-001","port":"a-records"}}',
    )
    assert encode_canonical(node) != bytes(node)
    assert encode_canonical(edge) != bytes(edge)


@pytest.mark.parametrize(
    ("enum_type", "type_tag"),
    [
        (RunState, "run-state"),
        (WorkItemState, "work-item-state"),
        (FailureClassification, "failure-classification"),
        (FailureDisposition, "failure-disposition"),
        (ReconciliationClassification, "reconciliation-classification"),
        (ReconciliationField, "reconciliation-field"),
        (RepairActionKind, "repair-action-kind"),
    ],
)
def test_every_closed_enum_variant_has_stable_bytes(
    enum_type: type[RunState]
    | type[WorkItemState]
    | type[FailureClassification]
    | type[FailureDisposition]
    | type[ReconciliationClassification]
    | type[ReconciliationField]
    | type[RepairActionKind],
    type_tag: str,
) -> None:
    for member in enum_type:
        assert encode_canonical(member) == _envelope(
            type_tag,
            f'"{member.value}"'.encode("ascii"),
        )


def test_every_field_mismatch_value_family_has_an_explicit_encoding() -> None:
    source = _record()
    target = InventoryRecord.create(
        sku="SKU-001",
        name="Older Widget",
        quantity=4,
        unit_price=Money.parse("USD 11.00"),
        updated_at=UtcTimestamp.parse("2026-08-11T10:00:00Z"),
        connector_id=ConnectorId("con_target-api"),
        source_record_key="target-001",
        attributes={"color": "red"},
    )
    outcome = ReconciliationOutcome((source,), (target,))

    assert tuple(item.field for item in outcome.mismatches) == tuple(ReconciliationField)
    for mismatch in outcome.mismatches:
        encoded = encode_canonical(mismatch)
        assert encoded.startswith(
            b'{"format":"paritygrid-canonical","type":"field-mismatch","value":'
        )
        assert f'"field":"{mismatch.field.value}"'.encode("ascii") in encoded
    assert encode_canonical(outcome).startswith(
        b'{"format":"paritygrid-canonical","type":"reconciliation-outcome","value":'
    )


def test_reconciliation_quantity_mismatch_has_exact_golden_bytes() -> None:
    mismatch = FieldMismatch(ReconciliationField.QUANTITY, 10, 4)

    assert encode_canonical(mismatch) == _envelope(
        "field-mismatch",
        b'{"field":"quantity","source":10,"target":4}',
    )


def test_both_repair_action_variants_and_full_plan_encode_identity_explicitly() -> None:
    create = _create_action()
    update = RepairAction.from_outcome(
        action_id=RepairActionId("rac_update-002"),
        conflict_id=ConflictId("cnf_mismatch-002"),
        state_fingerprint=STATE,
        outcome=ReconciliationOutcome(
            (_record(sku="SKU-002", quantity=9, source_key="source-002"),),
            (
                _record(
                    sku="SKU-002", quantity=2, connector="con_target-api", source_key="target-002"
                ),
            ),
        ),
    )
    plan = RepairPlan(RepairPlanId("rpl_inventory-001"), STATE, (update, create))
    create_value = (
        b'{"action_id":"rac_create-001","conflict_id":"cnf_missing-001",'
        b'"expected_target_record":null,"kind":"create_target","mismatches":[],'
        b'"proposed_record":{"attributes":[["color","blue"],["size","M"]],'
        b'"connector_id":"con_source-api","name":"Caf\\u00e9 Widget","quantity":10,'
        b'"sku":"SKU-001","source_record_key":"source-001",'
        b'"unit_price":{"currency":"USD","minor_unit_exponent":2,"minor_units":1230},'
        b'"updated_at":"2026-08-12T10:00:00.000000Z"},'
        b'"state_fingerprint":"1111111111111111111111111111111111111111111111111111111111111111"}'
    )
    update_value = (
        b'{"action_id":"rac_update-002","conflict_id":"cnf_mismatch-002",'
        b'"expected_target_record":{"attributes":[["color","blue"],["size","M"]],'
        b'"connector_id":"con_target-api","name":"Caf\\u00e9 Widget","quantity":2,'
        b'"sku":"SKU-002","source_record_key":"target-002",'
        b'"unit_price":{"currency":"USD","minor_unit_exponent":2,"minor_units":1230},'
        b'"updated_at":"2026-08-12T10:00:00.000000Z"},"kind":"update_target",'
        b'"mismatches":[{"field":"quantity","source":9,"target":2}],'
        b'"proposed_record":{"attributes":[["color","blue"],["size","M"]],'
        b'"connector_id":"con_source-api","name":"Caf\\u00e9 Widget","quantity":9,'
        b'"sku":"SKU-002","source_record_key":"source-002",'
        b'"unit_price":{"currency":"USD","minor_unit_exponent":2,"minor_units":1230},'
        b'"updated_at":"2026-08-12T10:00:00.000000Z"},'
        b'"state_fingerprint":"1111111111111111111111111111111111111111111111111111111111111111"}'
    )

    assert encode_canonical(create) == _envelope("repair-action", create_value)
    assert encode_canonical(update) == _envelope("repair-action", update_value)
    assert encode_canonical(plan) == _envelope(
        "repair-plan",
        b'{"actions":['
        + create_value
        + b","
        + update_value
        + b'],"plan_id":"rpl_inventory-001",'
        + b'"state_fingerprint":"11111111111111111111111111111111'
        + b'11111111111111111111111111111111"}',
    )


@pytest.mark.parametrize("version", [0, 1, 2, 2_147_483_647])
def test_raw_integer_versions_are_rejected_as_unsupported(version: int) -> None:
    with pytest.raises(CanonicalEncodingError) as captured:
        encode_canonical(PipelineVersion(1), version=version)  # type: ignore[arg-type]

    assert captured.value.reason is CanonicalErrorCode.UNSUPPORTED_CANONICAL_VERSION
    assert captured.value.version == version


@pytest.mark.parametrize("version", [True, False, "1", None, -1, 2_147_483_648])
def test_non_version_values_are_rejected_without_leaking_conversion_errors(
    version: object,
) -> None:
    with pytest.raises(CanonicalEncodingError) as captured:
        CanonicalEncoder(version=version)  # type: ignore[arg-type]

    assert captured.value.reason is CanonicalErrorCode.INVALID_CANONICAL_VALUE
    assert captured.value.subject_type == "canonical.version"


@dataclass(frozen=True)
class _UnregisteredValue:
    value: str


class _PipelineIdSubclass(PipelineId):
    pass


@pytest.mark.parametrize(
    "value",
    [
        None,
        True,
        1,
        1.0,
        "text",
        b"bytes",
        bytearray(b"bytes"),
        Decimal("1"),
        [PipelineVersion(1)],
        (PipelineVersion(1),),
        {"version": 1},
        {PipelineVersion(1)},
        _UnregisteredValue("value"),
        _PipelineIdSubclass("pip_catalog-001"),
    ],
)
def test_raw_primitives_containers_dataclasses_and_subclasses_are_rejected(value: object) -> None:
    with pytest.raises(CanonicalEncodingError) as captured:
        encode_canonical(value)

    assert captured.value.reason is CanonicalErrorCode.UNSUPPORTED_CANONICAL_TYPE
    assert captured.value.subject_type is not None


def test_unusual_runtime_type_names_still_produce_a_bounded_typed_error() -> None:
    unusual_type = type("\u00c9" * 100, (), {})

    with pytest.raises(CanonicalEncodingError) as captured:
        encode_canonical(unusual_type())

    assert captured.value.subject_type == "unsupported-runtime-type"


def test_corrupted_trusted_value_is_translated_to_invalid_canonical_value() -> None:
    record = _record()
    object.__setattr__(record, "name", 1.5)

    with pytest.raises(CanonicalEncodingError) as captured:
        encode_canonical(record)

    assert captured.value.reason is CanonicalErrorCode.INVALID_CANONICAL_VALUE
    assert captured.value.subject_type == "inventory-record"


def test_money_encoding_is_independent_of_ambient_decimal_precision() -> None:
    money = Money.parse("USD 9999999999999.99")
    expected = encode_canonical(money)

    with localcontext() as context:
        context.prec = 2
        actual = encode_canonical(money)

    assert actual == expected
    assert b'"minor_units":999999999999999' in actual

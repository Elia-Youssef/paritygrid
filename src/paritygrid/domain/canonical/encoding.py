"""Versioned canonical encoding for trusted domain values."""

import json
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from enum import IntEnum
from types import MappingProxyType
from typing import cast

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

type CanonicalPrimitive = (
    bool | int | str | list["CanonicalPrimitive"] | dict[str, "CanonicalPrimitive"] | None
)
type _Identifier = (
    PipelineId
    | NodeId
    | ConnectorId
    | RunId
    | WorkItemId
    | ArtifactId
    | ConflictId
    | RepairPlanId
    | RepairActionId
)
type _SequenceValue = PipelineVersion | AttemptNumber
type _StableText = NodeKind | PortName | PartitionKey
type _StableEnum = (
    RunState
    | WorkItemState
    | FailureClassification
    | FailureDisposition
    | ReconciliationClassification
    | ReconciliationField
    | RepairActionKind
)

_FORMAT_NAME = "paritygrid-canonical"
_MAX_SUPPORTED_VERSION_NUMBER = 2_147_483_647


class CanonicalVersion(IntEnum):
    """Independent version of the canonical byte protocol."""

    V1 = 1


@dataclass(frozen=True, slots=True)
class _CanonicalAdapter:
    type_tag: str
    to_primitive: Callable[[object], CanonicalPrimitive]


@dataclass(frozen=True, slots=True)
class CanonicalEncoder:
    """Encode a closed set of domain values using one protocol version."""

    version: CanonicalVersion = CanonicalVersion.V1

    def __post_init__(self) -> None:
        object.__setattr__(self, "version", _require_version(self.version))

    def encode(self, value: object) -> bytes:
        """Return the authoritative versioned bytes for a supported exact type."""
        adapter = _ADAPTERS.get(type(value))
        if adapter is None:
            raise _unsupported_type(value)
        try:
            primitive = adapter.to_primitive(value)
            return _encode_envelope(
                type_tag=adapter.type_tag,
                value=primitive,
                version=self.version,
            )
        except (OverflowError, TypeError, ValueError) as error:
            raise CanonicalEncodingError(
                reason=CanonicalErrorCode.INVALID_CANONICAL_VALUE,
                subject_type=adapter.type_tag,
            ) from error


def encode_canonical(
    value: object,
    version: CanonicalVersion = CanonicalVersion.V1,
) -> bytes:
    """Encode one trusted value through the closed canonical registry."""
    return CanonicalEncoder(version=version).encode(value)


def _require_version(value: object) -> CanonicalVersion:
    if type(value) is CanonicalVersion and value is CanonicalVersion.V1:
        return value
    if type(value) is int and 0 <= value <= _MAX_SUPPORTED_VERSION_NUMBER:
        raise CanonicalEncodingError(
            reason=CanonicalErrorCode.UNSUPPORTED_CANONICAL_VERSION,
            version=value,
        )
    raise CanonicalEncodingError(
        reason=CanonicalErrorCode.INVALID_CANONICAL_VALUE,
        subject_type="canonical.version",
    )


def _unsupported_type(value: object) -> CanonicalEncodingError:
    value_type = type(value)
    subject = f"{value_type.__module__}.{value_type.__qualname__}"
    if not subject.isascii() or not subject.isprintable() or len(subject) > 96:
        subject = "unsupported-runtime-type"
    return CanonicalEncodingError(
        reason=CanonicalErrorCode.UNSUPPORTED_CANONICAL_TYPE,
        subject_type=subject,
    )


def _encode_envelope(
    *,
    type_tag: str,
    value: CanonicalPrimitive,
    version: CanonicalVersion,
) -> bytes:
    _validate_primitive(value)
    envelope: dict[str, CanonicalPrimitive] = {
        "format": _FORMAT_NAME,
        "type": type_tag,
        "value": value,
        "version": int(version),
    }
    return json.dumps(
        envelope,
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("ascii")


def _validate_primitive(value: CanonicalPrimitive) -> None:
    if value is None or type(value) in {bool, int, str}:
        return
    if type(value) is list:
        for item in cast(list[CanonicalPrimitive], value):
            _validate_primitive(item)
        return
    if type(value) is dict:
        for item in cast(dict[str, CanonicalPrimitive], value).values():
            _validate_primitive(item)
        return
    raise TypeError("canonical adapters may emit only closed JSON primitives")


def _identifier_primitive(value: object) -> CanonicalPrimitive:
    return cast(_Identifier, value).value


def _sequence_primitive(value: object) -> CanonicalPrimitive:
    return cast(_SequenceValue, value).number


def _stable_text_primitive(value: object) -> CanonicalPrimitive:
    return cast(_StableText, value).value


def _enum_primitive(value: object) -> CanonicalPrimitive:
    return cast(_StableEnum, value).value


def _timestamp_primitive(value: object) -> CanonicalPrimitive:
    return str(cast(UtcTimestamp, value))


def _duration_primitive(value: object) -> CanonicalPrimitive:
    return cast(Duration, value).microseconds


def _currency_primitive(value: object) -> CanonicalPrimitive:
    return cast(CurrencyCode, value).value


def _money_value(value: Money) -> dict[str, CanonicalPrimitive]:
    return {
        "currency": value.currency.value,
        "minor_unit_exponent": value.minor_unit_exponent,
        "minor_units": value.minor_units,
    }


def _money_primitive(value: object) -> CanonicalPrimitive:
    return _money_value(cast(Money, value))


def _attributes_value(value: InventoryAttributes) -> list[CanonicalPrimitive]:
    return [[key, item_value] for key, item_value in value.items]


def _attributes_primitive(value: object) -> CanonicalPrimitive:
    return _attributes_value(cast(InventoryAttributes, value))


def _inventory_record_value(value: InventoryRecord) -> dict[str, CanonicalPrimitive]:
    return {
        "attributes": _attributes_value(value.attributes),
        "connector_id": value.connector_id.value,
        "name": value.name,
        "quantity": value.quantity,
        "sku": value.sku,
        "source_record_key": value.source_record_key,
        "unit_price": _money_value(value.unit_price),
        "updated_at": str(value.updated_at),
    }


def _inventory_effect_value(value: InventoryRecord) -> dict[str, CanonicalPrimitive]:
    """Project the business state affected by a repair without observation provenance."""
    return {
        "attributes": _attributes_value(value.attributes),
        "name": value.name,
        "quantity": value.quantity,
        "sku": value.sku,
        "unit_price": _money_value(value.unit_price),
        "updated_at": str(value.updated_at),
    }


def _inventory_record_primitive(value: object) -> CanonicalPrimitive:
    return _inventory_record_value(cast(InventoryRecord, value))


def _state_fingerprint_primitive(value: object) -> CanonicalPrimitive:
    return cast(StateFingerprint, value).value


def _pipeline_node_primitive(value: object) -> CanonicalPrimitive:
    node = cast(PipelineNode, value)
    return {
        "id": node.node_id.value,
        "inputs": [port.value for port in node.input_ports],
        "kind": node.kind.value,
        "outputs": [port.value for port in node.output_ports],
    }


def _pipeline_edge_primitive(value: object) -> CanonicalPrimitive:
    edge = cast(PipelineEdge, value)
    return {
        "source": {
            "node_id": edge.source_node_id.value,
            "port": edge.source_port.value,
        },
        "target": {
            "node_id": edge.target_node_id.value,
            "port": edge.target_port.value,
        },
    }


def _comparable_value(
    field: ReconciliationField,
    value: object,
) -> CanonicalPrimitive:
    if field is ReconciliationField.NAME:
        return cast(str, value)
    if field is ReconciliationField.QUANTITY:
        return cast(int, value)
    if field is ReconciliationField.UNIT_PRICE:
        return _money_value(cast(Money, value))
    if field is ReconciliationField.UPDATED_AT:
        return str(cast(UtcTimestamp, value))
    return _attributes_value(cast(InventoryAttributes, value))


def _field_mismatch_value(value: FieldMismatch) -> dict[str, CanonicalPrimitive]:
    return {
        "field": value.field.value,
        "source": _comparable_value(value.field, value.source_value),
        "target": _comparable_value(value.field, value.target_value),
    }


def _field_mismatch_primitive(value: object) -> CanonicalPrimitive:
    return _field_mismatch_value(cast(FieldMismatch, value))


def _reconciliation_outcome_primitive(value: object) -> CanonicalPrimitive:
    outcome = cast(ReconciliationOutcome, value)
    return {
        "classification": outcome.classification.value,
        "mismatches": [_field_mismatch_value(item) for item in outcome.mismatches],
        "sku": outcome.sku,
        "source_records": [_inventory_record_value(item) for item in outcome.source_records],
        "target_records": [_inventory_record_value(item) for item in outcome.target_records],
    }


def _repair_action_value(
    value: RepairAction,
    *,
    include_identity: bool,
    include_record_provenance: bool,
    include_state_fingerprint: bool,
) -> dict[str, CanonicalPrimitive]:
    record_value = _inventory_record_value if include_record_provenance else _inventory_effect_value
    primitive: dict[str, CanonicalPrimitive] = {
        "expected_target_record": (
            None
            if value.expected_target_record is None
            else record_value(value.expected_target_record)
        ),
        "kind": value.kind.value,
        "mismatches": [_field_mismatch_value(item) for item in value.mismatches],
        "proposed_record": record_value(value.proposed_record),
    }
    if include_identity:
        primitive["action_id"] = value.action_id.value
        primitive["conflict_id"] = value.conflict_id.value
    if include_state_fingerprint:
        primitive["state_fingerprint"] = value.state_fingerprint.value
    return primitive


def _repair_action_primitive(value: object) -> CanonicalPrimitive:
    return _repair_action_value(
        cast(RepairAction, value),
        include_identity=True,
        include_record_provenance=True,
        include_state_fingerprint=True,
    )


def _repair_plan_primitive(value: object) -> CanonicalPrimitive:
    plan = cast(RepairPlan, value)
    return {
        "actions": [
            _repair_action_value(
                action,
                include_identity=True,
                include_record_provenance=True,
                include_state_fingerprint=True,
            )
            for action in plan.actions
        ],
        "plan_id": plan.plan_id.value,
        "state_fingerprint": plan.state_fingerprint.value,
    }


def _repair_plan_content_primitive(plan: RepairPlan) -> CanonicalPrimitive:
    """Return logical repair contents without generated plan, action, or conflict identities."""
    value: dict[str, CanonicalPrimitive] = {
        "actions": [
            _repair_action_value(
                action,
                include_identity=False,
                include_record_provenance=False,
                include_state_fingerprint=False,
            )
            for action in plan.actions
        ],
        "state_fingerprint": plan.state_fingerprint.value,
    }
    if plan.binding is not None:
        value["binding"] = {
            "run_id": plan.binding.run_id.value,
            "reconciliation_fingerprint": plan.binding.reconciliation_fingerprint.value,
            "source_input_identity": plan.binding.source_input_identity,
            "target_input_identity": plan.binding.target_input_identity,
            "policy_version": plan.binding.policy_version,
            "generation_version": plan.binding.generation_version,
            "rules_version": plan.binding.rules_version,
            "analysis_version": plan.binding.analysis_version,
            "analytical_query_version": plan.binding.analytical_query_version,
            "action_count": plan.binding.action_count,
        }
    return value


def encode_repair_plan_content(
    plan: RepairPlan,
    *,
    version: CanonicalVersion,
) -> bytes:
    return _encode_envelope(
        type_tag="repair-plan-content",
        value=_repair_plan_content_primitive(plan),
        version=version,
    )


def encode_inventory_observation(
    record: InventoryRecord,
    *,
    version: CanonicalVersion,
) -> bytes:
    """Encode one inventory observation without connector provenance.

    Target-state identity covers the logical business content a record
    carries, never the connector or source position that observed it, so
    the same stored record observed through different reads encodes
    identically.
    """
    if type(record) is not InventoryRecord:
        raise CanonicalEncodingError(
            reason=CanonicalErrorCode.UNSUPPORTED_CANONICAL_TYPE,
            subject_type="inventory-observation",
        )
    return _encode_envelope(
        type_tag="inventory-observation",
        value=_inventory_effect_value(record),
        version=version,
    )


_ADAPTERS: Mapping[type[object], _CanonicalAdapter] = MappingProxyType(
    {
        PipelineId: _CanonicalAdapter("pipeline-id", _identifier_primitive),
        NodeId: _CanonicalAdapter("node-id", _identifier_primitive),
        ConnectorId: _CanonicalAdapter("connector-id", _identifier_primitive),
        RunId: _CanonicalAdapter("run-id", _identifier_primitive),
        WorkItemId: _CanonicalAdapter("work-item-id", _identifier_primitive),
        ArtifactId: _CanonicalAdapter("artifact-id", _identifier_primitive),
        ConflictId: _CanonicalAdapter("conflict-id", _identifier_primitive),
        RepairPlanId: _CanonicalAdapter("repair-plan-id", _identifier_primitive),
        RepairActionId: _CanonicalAdapter("repair-action-id", _identifier_primitive),
        PipelineVersion: _CanonicalAdapter("pipeline-version", _sequence_primitive),
        AttemptNumber: _CanonicalAdapter("attempt-number", _sequence_primitive),
        UtcTimestamp: _CanonicalAdapter("utc-timestamp", _timestamp_primitive),
        Duration: _CanonicalAdapter("duration", _duration_primitive),
        CurrencyCode: _CanonicalAdapter("currency-code", _currency_primitive),
        Money: _CanonicalAdapter("money", _money_primitive),
        InventoryAttributes: _CanonicalAdapter(
            "inventory-attributes",
            _attributes_primitive,
        ),
        InventoryRecord: _CanonicalAdapter("inventory-record", _inventory_record_primitive),
        StateFingerprint: _CanonicalAdapter(
            "state-fingerprint",
            _state_fingerprint_primitive,
        ),
        NodeKind: _CanonicalAdapter("node-kind", _stable_text_primitive),
        PortName: _CanonicalAdapter("port-name", _stable_text_primitive),
        PartitionKey: _CanonicalAdapter("partition-key", _stable_text_primitive),
        PipelineNode: _CanonicalAdapter("pipeline-node", _pipeline_node_primitive),
        PipelineEdge: _CanonicalAdapter("pipeline-edge", _pipeline_edge_primitive),
        RunState: _CanonicalAdapter("run-state", _enum_primitive),
        WorkItemState: _CanonicalAdapter("work-item-state", _enum_primitive),
        FailureClassification: _CanonicalAdapter("failure-classification", _enum_primitive),
        FailureDisposition: _CanonicalAdapter("failure-disposition", _enum_primitive),
        ReconciliationClassification: _CanonicalAdapter(
            "reconciliation-classification",
            _enum_primitive,
        ),
        ReconciliationField: _CanonicalAdapter("reconciliation-field", _enum_primitive),
        FieldMismatch: _CanonicalAdapter("field-mismatch", _field_mismatch_primitive),
        ReconciliationOutcome: _CanonicalAdapter(
            "reconciliation-outcome",
            _reconciliation_outcome_primitive,
        ),
        RepairActionKind: _CanonicalAdapter("repair-action-kind", _enum_primitive),
        RepairAction: _CanonicalAdapter("repair-action", _repair_action_primitive),
        RepairPlan: _CanonicalAdapter("repair-plan", _repair_plan_primitive),
    }
)

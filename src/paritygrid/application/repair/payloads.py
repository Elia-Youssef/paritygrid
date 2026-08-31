"""Canonical wire payloads for target effects and observed target reads.

Target writes and verification reads share one closed wire shape: the
Phase 8 simulator payload contract that Phase 9 connectors validate and
Phase 10 normalization projects back onto domain records.
"""

from collections.abc import Mapping
from dataclasses import dataclass

from paritygrid.application.ports.repair_audit import InventoryEffect
from paritygrid.domain.models import ConnectorId, InventoryRecord
from paritygrid.domain.reconciliation.normalization import (
    NormalizedRecord,
    QuarantinedObservation,
    SourceObservation,
    normalize_observation,
)

TARGET_OBSERVATION_CONNECTOR = ConnectorId("con_target-observation")


@dataclass(frozen=True, slots=True)
class ObservedTargetPayload:
    """One observed target payload parsed into a domain record or quarantine."""

    record: InventoryRecord | None
    quarantined: QuarantinedObservation | None

    def __post_init__(self) -> None:
        if (self.record is None) == (self.quarantined is None):
            raise ValueError("observed payload must carry exactly one outcome")


def render_target_payload(record: InventoryRecord) -> dict[str, object]:
    """Render one inventory record as the canonical target wire payload."""
    if type(record) is not InventoryRecord:
        raise TypeError("target payload source must be an InventoryRecord")
    canonical_price = str(record.unit_price)
    currency, _, amount = canonical_price.partition(" ")
    payload: dict[str, object] = {
        "attributes": dict(record.attributes.items),
        "name": record.name,
        "quantity": record.quantity,
        "sku": record.sku,
        "source_record_key": record.source_record_key,
        "unit_price": {"amount": amount, "currency": currency},
        "updated_at": str(record.updated_at),
    }
    return payload


def render_effect_payload(effect: InventoryEffect) -> dict[str, object]:
    """Render one provenance-free repair effect as the target wire payload."""
    if type(effect) is not InventoryEffect:
        raise TypeError("target effect payload requires InventoryEffect")
    canonical_price = str(effect.unit_price)
    currency, _, amount = canonical_price.partition(" ")
    return {
        "attributes": dict(effect.attributes.items),
        "name": effect.name,
        "quantity": effect.quantity,
        "sku": effect.sku,
        "source_record_key": f"repair:{effect.sku.lower()}",
        "unit_price": {"amount": amount, "currency": currency},
        "updated_at": str(effect.updated_at),
    }


def parse_observed_payload(
    position: int, payload: Mapping[str, object] | None
) -> ObservedTargetPayload:
    """Parse one observed target payload through the Phase 10 rules."""
    if type(position) is not int or position < 0:
        raise TypeError("observed payload position must be a nonnegative integer")
    observation = SourceObservation(
        position=position,
        connector_id=TARGET_OBSERVATION_CONNECTOR,
        payload=payload if payload is not None else None,
        malformed_reason=None
        if payload is not None
        else "the observed target payload is not an object",
    )
    result = normalize_observation(observation)
    if isinstance(result, NormalizedRecord):
        return ObservedTargetPayload(record=result.record, quarantined=None)
    return ObservedTargetPayload(record=None, quarantined=result)


__all__ = [
    "TARGET_OBSERVATION_CONNECTOR",
    "ObservedTargetPayload",
    "parse_observed_payload",
    "render_effect_payload",
    "render_target_payload",
]

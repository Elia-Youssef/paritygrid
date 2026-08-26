"""Shared fixtures and builders for Phase 10 reconciliation tests."""

from dataclasses import dataclass

from paritygrid.domain.models import ConnectorId
from paritygrid.domain.reconciliation import SourceObservation

SOURCE_CONNECTOR = ConnectorId("con_reconciliation-source")
TARGET_CONNECTOR = ConnectorId("con_reconciliation-target")


def wire_payload(
    sku: str = "GRID-0001",
    source_record_key: str = "src-000001",
    *,
    name: str = "Cafe valve",
    quantity: int = 5,
    amount: str = "12.34",
    currency: str = "USD",
    updated_at: str = "2024-03-04T05:06:07.000000Z",
    attributes: dict[str, str] | None = None,
) -> dict[str, object]:
    """Build one closed wire payload shaped like the Phase 8/9 sources."""
    payload: dict[str, object] = {
        "attributes": {"color": "blue"} if attributes is None else dict(attributes),
        "name": name,
        "quantity": quantity,
        "sku": sku,
        "source_record_key": source_record_key,
        "unit_price": {"amount": amount, "currency": currency},
        "updated_at": updated_at,
    }
    return payload


def source_observation(
    position: int,
    payload: dict[str, object] | None,
    reason: str | None = None,
) -> SourceObservation:
    """Build one source observation for the normalization pipeline."""
    return SourceObservation(
        position=position,
        connector_id=SOURCE_CONNECTOR,
        payload=payload,
        malformed_reason=reason,
    )


@dataclass(frozen=True, slots=True)
class Sides:
    """One observation pair ready for a full analysis."""

    source: tuple[SourceObservation, ...]
    target: tuple[SourceObservation, ...]


def connector() -> ConnectorId:
    """Return the canonical source connector used by these tests."""
    return SOURCE_CONNECTOR

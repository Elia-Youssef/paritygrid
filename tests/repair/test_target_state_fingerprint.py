"""Golden and sensitivity tests for the Phase 11 target-state fingerprint."""

from datetime import UTC, datetime
from decimal import Decimal

import pytest

from paritygrid.domain.canonical import (
    CanonicalVersion,
    FingerprintScope,
    encode_inventory_observation,
    fingerprint_state,
)
from paritygrid.domain.models import (
    ConnectorId,
    CurrencyCode,
    InventoryRecord,
    Money,
    StateFingerprint,
    UtcTimestamp,
)
from paritygrid.domain.repair import (
    TARGET_OBSERVATION_VERSION,
    TARGET_STATE_FINGERPRINT_KIND,
    TARGET_STATE_FINGERPRINT_VERSION,
    TargetStateIdentity,
    compute_target_state_fingerprint,
)

_GOLDEN_FINGERPRINT = "75b0c00def741f8402eeda82de061b7d1895e5bf71afe58addf124a6c1277421"
_MOMENT = UtcTimestamp(datetime(2024, 3, 4, 5, 6, 7, tzinfo=UTC))


def observed_record(sku: str, *, quantity: int = 5, name: str = "Cafe valve") -> InventoryRecord:
    return InventoryRecord.create(
        sku=sku,
        name=name,
        quantity=quantity,
        unit_price=Money(Decimal("12.34"), CurrencyCode("USD"), 2),
        updated_at=_MOMENT,
        connector_id=ConnectorId("con_target-observation"),
        source_record_key=f"obs-{sku.lower()}",
        attributes={"finish": "Brass"},
    )


def _inventory_digest(records: tuple[InventoryRecord, ...]) -> StateFingerprint:
    return fingerprint_state(records, scope=FingerprintScope.TARGET_OBSERVATION_STATE)


def test_golden_fingerprint_is_locked_to_exact_bytes() -> None:
    records = (
        observed_record("GRID-0001"),
        observed_record("GRID-0002", quantity=7),
    )
    fingerprint = compute_target_state_fingerprint(
        observation_version=TARGET_OBSERVATION_VERSION,
        record_count=2,
        inventory_digest=_inventory_digest(records),
    )
    assert fingerprint.value == _GOLDEN_FINGERPRINT
    identity = TargetStateIdentity(
        fingerprint_kind=TARGET_STATE_FINGERPRINT_KIND,
        fingerprint_version=TARGET_STATE_FINGERPRINT_VERSION,
        observation_version=TARGET_OBSERVATION_VERSION,
        record_count=2,
        fingerprint=fingerprint,
    )
    assert identity.fingerprint is fingerprint


def test_changed_target_data_changes_the_target_state_fingerprint() -> None:
    records = (observed_record("GRID-0001"), observed_record("GRID-0002"))
    baseline = compute_target_state_fingerprint(
        observation_version=TARGET_OBSERVATION_VERSION,
        record_count=2,
        inventory_digest=_inventory_digest(records),
    )
    mutations = {
        "changed content": (observed_record("GRID-0001", quantity=6), observed_record("GRID-0002")),
        "changed key": (observed_record("GRID-0003"), observed_record("GRID-0002")),
        "dropped record": (observed_record("GRID-0001"),),
        "added record": (
            observed_record("GRID-0001"),
            observed_record("GRID-0002"),
            observed_record("GRID-0003"),
        ),
    }
    for label, mutated in mutations.items():
        changed = compute_target_state_fingerprint(
            observation_version=TARGET_OBSERVATION_VERSION,
            record_count=len(mutated),
            inventory_digest=_inventory_digest(mutated),
        )
        assert changed != baseline, label


def test_changed_observation_count_changes_the_fingerprint() -> None:
    records = (observed_record("GRID-0001"),)
    digest = _inventory_digest(records)
    baseline = compute_target_state_fingerprint(
        observation_version=TARGET_OBSERVATION_VERSION,
        record_count=1,
        inventory_digest=digest,
    )
    changed = compute_target_state_fingerprint(
        observation_version=TARGET_OBSERVATION_VERSION,
        record_count=2,
        inventory_digest=digest,
    )
    assert changed != baseline


def test_observation_provenance_never_changes_the_fingerprint() -> None:
    first = observed_record("GRID-0001")
    second = InventoryRecord(
        sku=first.sku,
        name=first.name,
        quantity=first.quantity,
        unit_price=first.unit_price,
        updated_at=first.updated_at,
        connector_id=ConnectorId("con_another-observer"),
        source_record_key="different-provenance",
        attributes=first.attributes,
    )
    assert _inventory_digest((first,)) == _inventory_digest((second,))
    assert encode_inventory_observation(first, version=CanonicalVersion.V1) == (
        encode_inventory_observation(second, version=CanonicalVersion.V1)
    )


def test_observation_order_never_changes_the_fingerprint() -> None:
    first, second = observed_record("GRID-0001"), observed_record("GRID-0002")
    assert _inventory_digest((first, second)) == _inventory_digest((second, first))


def test_target_state_fingerprint_is_its_own_kind() -> None:
    record = observed_record("GRID-0001")
    digest = _inventory_digest((record,))
    target_state = compute_target_state_fingerprint(
        observation_version=TARGET_OBSERVATION_VERSION,
        record_count=1,
        inventory_digest=digest,
    )
    assert target_state != digest
    assert target_state != fingerprint_state((record,), scope=FingerprintScope.INVENTORY_STATE)
    from paritygrid.domain.reconciliation.outcomes import ReconciliationOutcome

    outcome = ReconciliationOutcome(source_records=(record,), target_records=())
    assert target_state != fingerprint_state(
        (outcome,), scope=FingerprintScope.RECONCILIATION_STATE
    )
    assert TARGET_STATE_FINGERPRINT_KIND == "target_state"
    assert TARGET_STATE_FINGERPRINT_VERSION == 1
    assert TARGET_OBSERVATION_VERSION == 1


def test_target_state_identity_rejects_foreign_kinds_and_versions() -> None:
    fingerprint = compute_target_state_fingerprint(
        observation_version=TARGET_OBSERVATION_VERSION,
        record_count=0,
        inventory_digest=_inventory_digest(()),
    )
    with pytest.raises(ValueError, match="kind"):
        TargetStateIdentity(
            fingerprint_kind="reconciliation",
            fingerprint_version=TARGET_STATE_FINGERPRINT_VERSION,
            observation_version=TARGET_OBSERVATION_VERSION,
            record_count=0,
            fingerprint=fingerprint,
        )
    with pytest.raises(ValueError, match="version"):
        TargetStateIdentity(
            fingerprint_kind=TARGET_STATE_FINGERPRINT_KIND,
            fingerprint_version=2,
            observation_version=TARGET_OBSERVATION_VERSION,
            record_count=0,
            fingerprint=fingerprint,
        )
    with pytest.raises(ValueError, match="version"):
        TargetStateIdentity(
            fingerprint_kind=TARGET_STATE_FINGERPRINT_KIND,
            fingerprint_version=TARGET_STATE_FINGERPRINT_VERSION,
            observation_version=0,
            record_count=0,
            fingerprint=fingerprint,
        )
    with pytest.raises(ValueError, match="range"):
        TargetStateIdentity(
            fingerprint_kind=TARGET_STATE_FINGERPRINT_KIND,
            fingerprint_version=TARGET_STATE_FINGERPRINT_VERSION,
            observation_version=TARGET_OBSERVATION_VERSION,
            record_count=-1,
            fingerprint=fingerprint,
        )


def test_compute_target_state_fingerprint_rejects_invalid_inputs() -> None:
    digest = _inventory_digest(())
    for kwargs in (
        {"observation_version": 0, "record_count": 0, "inventory_digest": digest},
        {"observation_version": 1, "record_count": -1, "inventory_digest": digest},
        {"observation_version": 1, "record_count": 10_000_001, "inventory_digest": digest},
    ):
        with pytest.raises(ValueError, match="target-state"):
            compute_target_state_fingerprint(**kwargs)  # type: ignore[arg-type]
    with pytest.raises(TypeError):
        compute_target_state_fingerprint(
            observation_version=1,
            record_count=0,
            inventory_digest="0" * 64,  # type: ignore[arg-type]
        )


def test_target_observation_scope_rejects_foreign_value_types() -> None:
    from paritygrid.domain.errors import CanonicalEncodingError

    with pytest.raises(CanonicalEncodingError):
        fingerprint_state(("GRID-0001",), scope=FingerprintScope.TARGET_OBSERVATION_STATE)


def test_inventory_observation_encoding_is_versioned_and_stable() -> None:
    record = observed_record("GRID-0001")
    encoded = encode_inventory_observation(record, version=CanonicalVersion.V1)
    again = encode_inventory_observation(observed_record("GRID-0001"), version=CanonicalVersion.V1)
    assert encoded == again
    assert b"inventory-observation" in encoded
    assert b"connector_id" not in encoded
    assert record.sku.encode() in encoded

"""Property verification for repair plan ordering and stale-state guards."""

from decimal import Decimal

from hypothesis import given
from hypothesis import strategies as st

from paritygrid.domain.models import (
    ConflictId,
    ConnectorId,
    InventoryRecord,
    Money,
    RepairActionId,
    RepairPlanId,
    StateFingerprint,
    UtcTimestamp,
)
from paritygrid.domain.reconciliation import ReconciliationOutcome
from paritygrid.domain.repair import RepairAction, RepairPlan


def _action(index: int, fingerprint: StateFingerprint) -> RepairAction:
    record = InventoryRecord.create(
        sku=f"SKU-{index:03d}",
        name=f"Widget {index}",
        quantity=index,
        unit_price=Money(Decimal("1.00"), Money.parse("USD 0.00").currency, 2),
        updated_at=UtcTimestamp.parse("2026-08-12T10:00:00Z"),
        connector_id=ConnectorId("con_property-source"),
        source_record_key=f"source-{index}",
    )
    return RepairAction.from_outcome(
        action_id=RepairActionId(f"rac_create-{index:03d}"),
        conflict_id=ConflictId(f"cnf_missing-{index:03d}"),
        state_fingerprint=fingerprint,
        outcome=ReconciliationOutcome((record,), ()),
    )


@given(st.lists(st.integers(min_value=1, max_value=100), min_size=1, max_size=12, unique=True))
def test_plan_identity_and_hash_are_independent_of_input_order(indices: list[int]) -> None:
    fingerprint = StateFingerprint("a" * 64)
    actions = tuple(_action(index, fingerprint) for index in indices)

    forward = RepairPlan(RepairPlanId("rpl_property-001"), fingerprint, actions)
    reverse = RepairPlan(
        RepairPlanId("rpl_property-001"),
        fingerprint,
        tuple(reversed(actions)),
    )

    assert forward == reverse
    assert hash(forward) == hash(reverse)
    assert tuple(action.sku for action in forward.actions) == tuple(
        sorted(action.sku for action in actions)
    )


@given(st.binary(min_size=32, max_size=32), st.binary(min_size=32, max_size=32))
def test_plan_current_check_uses_exact_fingerprint_bytes(
    expected_bytes: bytes, observed_bytes: bytes
) -> None:
    expected = StateFingerprint(expected_bytes.hex())
    observed = StateFingerprint(observed_bytes.hex())
    plan = RepairPlan(RepairPlanId("rpl_property-001"), expected, (_action(1, expected),))

    assert plan.is_current(observed) is (expected_bytes == observed_bytes)

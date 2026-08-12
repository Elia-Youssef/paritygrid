"""Example verification for immutable safe repair plans."""

from dataclasses import FrozenInstanceError
from decimal import Decimal

import pytest

from paritygrid.domain.errors import DomainError, DomainErrorCode
from paritygrid.domain.errors import StaleRepairPlanError as CentralStaleRepairPlanError
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
from paritygrid.domain.repair import (
    RepairAction,
    RepairActionKind,
    RepairPlan,
    StaleRepairPlanError,
)

CURRENT = StateFingerprint("1" * 64)
STALE = StateFingerprint("2" * 64)


def test_repair_package_preserves_the_stale_error_import() -> None:
    assert StaleRepairPlanError is CentralStaleRepairPlanError


def _record(
    *, sku: str = "SKU-001", quantity: int = 10, source_key: str = "source"
) -> InventoryRecord:
    return InventoryRecord.create(
        sku=sku,
        name="Widget",
        quantity=quantity,
        unit_price=Money(Decimal("12.34"), Money.parse("USD 0.00").currency, 2),
        updated_at=UtcTimestamp.parse("2026-08-12T10:00:00Z"),
        connector_id=ConnectorId("con_inventory-source"),
        source_record_key=source_key,
        attributes={"color": "blue"},
    )


def _create_action(
    *,
    suffix: str = "001",
    sku: str = "SKU-001",
    fingerprint: StateFingerprint = CURRENT,
) -> RepairAction:
    outcome = ReconciliationOutcome((_record(sku=sku, source_key=f"source-{suffix}"),), ())
    return RepairAction.from_outcome(
        action_id=RepairActionId(f"rac_create-{suffix}"),
        conflict_id=ConflictId(f"cnf_missing-{suffix}"),
        state_fingerprint=fingerprint,
        outcome=outcome,
    )


def test_missing_target_outcome_builds_a_create_action() -> None:
    source = _record()
    action = RepairAction.from_outcome(
        action_id=RepairActionId("rac_create-001"),
        conflict_id=ConflictId("cnf_missing-001"),
        state_fingerprint=CURRENT,
        outcome=ReconciliationOutcome((source,), ()),
    )

    assert action.kind is RepairActionKind.CREATE_TARGET
    assert action.proposed_record is source
    assert action.expected_target_record is None
    assert action.mismatches == ()
    assert action.sku == "SKU-001"


def test_field_mismatch_outcome_builds_an_exact_guarded_update() -> None:
    source = _record(quantity=10)
    target = _record(quantity=4, source_key="target")
    outcome = ReconciliationOutcome((source,), (target,))
    action = RepairAction.from_outcome(
        action_id=RepairActionId("rac_update-001"),
        conflict_id=ConflictId("cnf_mismatch-001"),
        state_fingerprint=CURRENT,
        outcome=outcome,
    )

    assert action.kind is RepairActionKind.UPDATE_TARGET
    assert action.proposed_record is source
    assert action.expected_target_record is target
    assert action.mismatches == outcome.mismatches


def test_repair_action_set_has_no_destructive_variant() -> None:
    assert set(RepairActionKind) == {
        RepairActionKind.CREATE_TARGET,
        RepairActionKind.UPDATE_TARGET,
    }
    assert all("delete" not in kind.value for kind in RepairActionKind)


@pytest.mark.parametrize(
    "outcome",
    [
        ReconciliationOutcome((_record(),), (_record(source_key="target"),)),
        ReconciliationOutcome((), (_record(source_key="target"),)),
        ReconciliationOutcome((_record(), _record(source_key="duplicate")), ()),
    ],
)
def test_non_repairable_outcomes_cannot_create_actions(outcome: ReconciliationOutcome) -> None:
    with pytest.raises(ValueError, match="not safely repairable"):
        RepairAction.from_outcome(
            action_id=RepairActionId("rac_unsafe-001"),
            conflict_id=ConflictId("cnf_unsafe-001"),
            state_fingerprint=CURRENT,
            outcome=outcome,
        )


def test_action_factory_rejects_a_non_outcome() -> None:
    with pytest.raises(TypeError, match="ReconciliationOutcome"):
        RepairAction.from_outcome(
            action_id=RepairActionId("rac_invalid-001"),
            conflict_id=ConflictId("cnf_invalid-001"),
            state_fingerprint=CURRENT,
            outcome="outcome",
        )


@pytest.mark.parametrize(
    ("field_name", "replacement", "message"),
    [
        ("action_id", "rac_create-001", "RepairActionId"),
        ("conflict_id", "cnf_missing-001", "ConflictId"),
        ("state_fingerprint", "1" * 64, "StateFingerprint"),
        ("kind", "create_target", "RepairActionKind"),
        ("proposed_record", "record", "InventoryRecord"),
    ],
)
def test_action_rejects_untrusted_field_values(
    field_name: str, replacement: object, message: str
) -> None:
    values: dict[str, object] = {
        "action_id": RepairActionId("rac_create-001"),
        "conflict_id": ConflictId("cnf_missing-001"),
        "state_fingerprint": CURRENT,
        "kind": RepairActionKind.CREATE_TARGET,
        "proposed_record": _record(),
    }
    values[field_name] = replacement

    with pytest.raises(TypeError, match=message):
        RepairAction(**values)  # type: ignore[arg-type]


def test_create_rejects_an_existing_target() -> None:
    with pytest.raises(ValueError, match="absent"):
        RepairAction(
            RepairActionId("rac_create-001"),
            ConflictId("cnf_missing-001"),
            CURRENT,
            RepairActionKind.CREATE_TARGET,
            _record(),
            _record(source_key="target"),
        )


def test_update_rejects_missing_invalid_equal_or_different_key_targets() -> None:
    common = (
        RepairActionId("rac_update-001"),
        ConflictId("cnf_mismatch-001"),
        CURRENT,
        RepairActionKind.UPDATE_TARGET,
        _record(),
    )
    with pytest.raises(TypeError, match="expected target"):
        RepairAction(*common)
    with pytest.raises(TypeError, match="expected target"):
        RepairAction(*common, "target")  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="at least one"):
        RepairAction(*common, _record(source_key="target"))
    with pytest.raises(ValueError, match="matching SKUs"):
        RepairAction(*common, _record(sku="SKU-002", source_key="target"))


def test_plan_canonicalizes_action_order_and_remains_hashable() -> None:
    first = _create_action(suffix="001", sku="SKU-001")
    second = _create_action(suffix="002", sku="SKU-002")

    plan = RepairPlan(RepairPlanId("rpl_inventory-001"), CURRENT, (second, first))
    same = RepairPlan(RepairPlanId("rpl_inventory-001"), CURRENT, (first, second))

    assert plan == same
    assert hash(plan) == hash(same)
    assert tuple(action.sku for action in plan.actions) == ("SKU-001", "SKU-002")
    with pytest.raises(FrozenInstanceError):
        plan.actions = ()  # type: ignore[misc]


@pytest.mark.parametrize(
    ("field_name", "replacement", "expected_error", "message"),
    [
        ("plan_id", "rpl_inventory-001", TypeError, "RepairPlanId"),
        ("state_fingerprint", "1" * 64, TypeError, "StateFingerprint"),
        ("actions", [_create_action()], TypeError, "tuple"),
        ("actions", (), ValueError, "at least one"),
        ("actions", ("action",), TypeError, "RepairAction"),
    ],
)
def test_plan_rejects_untrusted_fields(
    field_name: str,
    replacement: object,
    expected_error: type[Exception],
    message: str,
) -> None:
    values: dict[str, object] = {
        "plan_id": RepairPlanId("rpl_inventory-001"),
        "state_fingerprint": CURRENT,
        "actions": (_create_action(),),
    }
    values[field_name] = replacement

    with pytest.raises(expected_error, match=message):
        RepairPlan(**values)  # type: ignore[arg-type]


def test_plan_bounds_the_number_of_actions_before_canonicalization() -> None:
    action = _create_action()
    oversized = (action,) * (RepairPlan.MAX_ACTIONS + 1)

    with pytest.raises(ValueError, match="must contain at most"):
        RepairPlan(RepairPlanId("rpl_inventory-001"), CURRENT, oversized)


def test_plan_rejects_stale_actions_and_duplicate_identities_or_keys() -> None:
    first = _create_action(suffix="001", sku="SKU-001")
    with pytest.raises(ValueError, match="state fingerprint"):
        RepairPlan(
            RepairPlanId("rpl_inventory-001"),
            CURRENT,
            (_create_action(suffix="002", sku="SKU-002", fingerprint=STALE),),
        )
    with pytest.raises(ValueError, match="action identities"):
        RepairPlan(RepairPlanId("rpl_inventory-001"), CURRENT, (first, first))

    duplicate_conflict = RepairAction.from_outcome(
        action_id=RepairActionId("rac_create-002"),
        conflict_id=first.conflict_id,
        state_fingerprint=CURRENT,
        outcome=ReconciliationOutcome((_record(sku="SKU-002"),), ()),
    )
    with pytest.raises(ValueError, match="conflict identities"):
        RepairPlan(RepairPlanId("rpl_inventory-001"), CURRENT, (first, duplicate_conflict))

    duplicate_sku = _create_action(suffix="002", sku="SKU-001")
    with pytest.raises(ValueError, match="inventory keys"):
        RepairPlan(RepairPlanId("rpl_inventory-001"), CURRENT, (first, duplicate_sku))


def test_plan_detects_and_rejects_stale_state() -> None:
    plan = RepairPlan(
        RepairPlanId("rpl_inventory-001"),
        CURRENT,
        (_create_action(),),
    )

    assert plan.is_current(CURRENT)
    assert not plan.is_current(STALE)
    plan.require_current(CURRENT)
    with pytest.raises(StaleRepairPlanError) as captured:
        plan.require_current(STALE)
    assert isinstance(captured.value, DomainError)
    assert captured.value.expected == CURRENT
    assert captured.value.actual == STALE
    assert captured.value.code is DomainErrorCode.STALE_REPAIR_PLAN
    assert str(captured.value) == (
        f"repair plan expects state {CURRENT}, but current state is {STALE}"
    )
    assert captured.value.args == (str(captured.value),)


def test_plan_state_checks_reject_unvalidated_fingerprints() -> None:
    plan = RepairPlan(RepairPlanId("rpl_inventory-001"), CURRENT, (_create_action(),))

    with pytest.raises(TypeError, match="StateFingerprint"):
        plan.is_current("1" * 64)
    with pytest.raises(TypeError, match="StateFingerprint"):
        plan.require_current("1" * 64)

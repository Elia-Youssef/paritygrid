"""Golden and adversarial verification for logical state fingerprints."""

import os
import subprocess
import sys
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from decimal import Decimal

import pytest
from hypothesis import given
from hypothesis import strategies as st

from paritygrid.domain.canonical import (
    MAX_FINGERPRINT_ITEMS,
    CanonicalVersion,
    FingerprintScope,
    fingerprint_state,
)
from paritygrid.domain.errors import CanonicalEncodingError, CanonicalErrorCode
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

STATE = StateFingerprint("1" * 64)


def _record(
    *,
    sku: str = "SKU-001",
    quantity: int = 10,
    source_key: str = "source-001",
) -> InventoryRecord:
    return InventoryRecord.create(
        sku=sku,
        name="Widget",
        quantity=quantity,
        unit_price=Money(Decimal("12.34"), Money.parse("USD 0.00").currency, 2),
        updated_at=UtcTimestamp.parse("2026-08-12T10:00:00Z"),
        connector_id=ConnectorId("con_source-api"),
        source_record_key=source_key,
        attributes={"color": "blue"},
    )


def _plan(
    *,
    plan_id: str = "rpl_inventory-001",
    action_id: str = "rac_create-001",
    conflict_id: str = "cnf_missing-001",
) -> RepairPlan:
    record = _record()
    action = RepairAction.from_outcome(
        action_id=RepairActionId(action_id),
        conflict_id=ConflictId(conflict_id),
        state_fingerprint=STATE,
        outcome=ReconciliationOutcome((record,), ()),
    )
    return RepairPlan(RepairPlanId(plan_id), STATE, (action,))


def test_exact_golden_digests_lock_empty_single_multi_and_duplicate_states() -> None:
    first = _record(sku="SKU-001", quantity=1, source_key="source-001")
    second = _record(sku="SKU-002", quantity=2, source_key="source-002")

    assert str(fingerprint_state([], scope=FingerprintScope.INVENTORY_STATE)) == (
        "53ce6372a6916b0f4c4559a1c1f5ab40699b1682340f774b3f56b0203e05df4d"
    )
    assert str(fingerprint_state([first], scope=FingerprintScope.INVENTORY_STATE)) == (
        "41f1529367ef233a9cb2acee2a964d411fc62d252258e02a21510fbad89d6445"
    )
    assert str(fingerprint_state([first, second], scope=FingerprintScope.INVENTORY_STATE)) == (
        "da3536aca0f6cbf65e10df8d629f65d0718a3bc3ebea840586bae4bf3c9e7a0c"
    )
    assert str(fingerprint_state([first, first], scope=FingerprintScope.INVENTORY_STATE)) == (
        "122bd6de21e42533161cd80418b4d4367e23040c342c26574038d9b57f6b995c"
    )


def test_every_scope_has_a_locked_golden_digest() -> None:
    record = _record()
    outcome = ReconciliationOutcome((record,), ())
    plan = _plan()

    assert str(fingerprint_state([record], scope=FingerprintScope.INVENTORY_STATE)) == (
        "3be9a69958dcdb6e1d0868b74bcca1fbdba7390d00e3ddaf7b4c3ef841b2b24c"
    )
    assert (
        str(fingerprint_state([outcome], scope=FingerprintScope.RECONCILIATION_STATE))
        == "7ea8bd3d2e37d494246317e61ddec9ab540ae4ffce74b32fe6b4d4e7fd62fe8d"
    )
    assert str(fingerprint_state([plan], scope=FingerprintScope.REPAIR_PLAN_CONTENT)) == (
        "bd5e7be1c2cfc145e1cdb0fca8fc5d137bfb0da56aea2961df061261cc41a8d3"
    )


@given(st.lists(st.integers(min_value=0, max_value=100), min_size=0, max_size=30))
def test_inventory_fingerprint_is_permutation_invariant_and_semantically_sensitive(
    quantities: list[int],
) -> None:
    records = [
        _record(sku=f"SKU-{index:03d}", quantity=quantity, source_key=f"source-{index:03d}")
        for index, quantity in enumerate(quantities)
    ]

    assert fingerprint_state(records, scope=FingerprintScope.INVENTORY_STATE) == fingerprint_state(
        reversed(records),
        scope=FingerprintScope.INVENTORY_STATE,
    )
    if records:
        changed = [
            *records[:-1],
            _record(sku=records[-1].sku, quantity=101, source_key=records[-1].source_record_key),
        ]
        if records[-1].quantity != 101:
            assert fingerprint_state(
                records,
                scope=FingerprintScope.INVENTORY_STATE,
            ) != fingerprint_state(changed, scope=FingerprintScope.INVENTORY_STATE)


def test_duplicate_multiplicity_changes_the_digest() -> None:
    record = _record()

    once = fingerprint_state([record], scope=FingerprintScope.INVENTORY_STATE)
    twice = fingerprint_state([record, record], scope=FingerprintScope.INVENTORY_STATE)

    assert once != twice


def test_scope_domain_separation_changes_empty_state_digest() -> None:
    digests = {fingerprint_state([], scope=scope) for scope in FingerprintScope}

    assert len(digests) == len(FingerprintScope)


def test_repair_plan_content_excludes_generated_plan_action_and_conflict_identities() -> None:
    first = _plan()
    renamed = _plan(
        plan_id="rpl_inventory-999",
        action_id="rac_create-999",
        conflict_id="cnf_missing-999",
    )

    assert first != renamed
    assert fingerprint_state(
        [first],
        scope=FingerprintScope.REPAIR_PLAN_CONTENT,
    ) == fingerprint_state([renamed], scope=FingerprintScope.REPAIR_PLAN_CONTENT)


def test_repair_plan_content_excludes_record_provenance_but_retains_exact_state() -> None:
    source = _record()
    alternate_provenance = InventoryRecord.create(
        sku=source.sku,
        name=source.name,
        quantity=source.quantity,
        unit_price=source.unit_price,
        updated_at=source.updated_at,
        connector_id=ConnectorId("con_alternate-source"),
        source_record_key="alternate-001",
        attributes=dict(source.attributes),
    )

    def plan_for(record: InventoryRecord, state: StateFingerprint) -> RepairPlan:
        action = RepairAction.from_outcome(
            action_id=RepairActionId("rac_create-001"),
            conflict_id=ConflictId("cnf_missing-001"),
            state_fingerprint=state,
            outcome=ReconciliationOutcome((record,), ()),
        )
        return RepairPlan(RepairPlanId("rpl_inventory-001"), state, (action,))

    original = plan_for(source, STATE)
    renamed_provenance = plan_for(alternate_provenance, STATE)
    changed_state = plan_for(alternate_provenance, StateFingerprint("2" * 64))

    original_digest = fingerprint_state([original], scope=FingerprintScope.REPAIR_PLAN_CONTENT)
    assert original_digest == fingerprint_state(
        [renamed_provenance], scope=FingerprintScope.REPAIR_PLAN_CONTENT
    )
    assert original_digest != fingerprint_state(
        [changed_state], scope=FingerprintScope.REPAIR_PLAN_CONTENT
    )


def test_multiple_repair_actions_cannot_reintroduce_generated_identity_ordering() -> None:
    def plan_for(identities: tuple[tuple[str, str], ...]) -> RepairPlan:
        actions = tuple(
            RepairAction.from_outcome(
                action_id=RepairActionId(action_id),
                conflict_id=ConflictId(conflict_id),
                state_fingerprint=STATE,
                outcome=ReconciliationOutcome(
                    (_record(sku=f"SKU-{index:03d}", source_key=f"source-{index:03d}"),),
                    (),
                ),
            )
            for index, (action_id, conflict_id) in enumerate(identities, start=1)
        )
        return RepairPlan(RepairPlanId("rpl_inventory-001"), STATE, actions)

    first = plan_for(
        (("rac_generated-z", "cnf_generated-z"), ("rac_generated-a", "cnf_generated-a"))
    )
    renamed = plan_for(
        (("rac_generated-a", "cnf_generated-a"), ("rac_generated-z", "cnf_generated-z"))
    )

    assert fingerprint_state(
        [first], scope=FingerprintScope.REPAIR_PLAN_CONTENT
    ) == fingerprint_state([renamed], scope=FingerprintScope.REPAIR_PLAN_CONTENT)


def test_reconciliation_fingerprint_retains_exact_observation_provenance() -> None:
    original = _record()
    alternate = InventoryRecord.create(
        sku=original.sku,
        name=original.name,
        quantity=original.quantity,
        unit_price=original.unit_price,
        updated_at=original.updated_at,
        connector_id=ConnectorId("con_alternate-source"),
        source_record_key="alternate-001",
        attributes=dict(original.attributes),
    )
    original_outcome = ReconciliationOutcome((original,), ())
    alternate_outcome = ReconciliationOutcome((alternate,), ())

    assert original_outcome.classification == alternate_outcome.classification
    assert fingerprint_state(
        [original_outcome], scope=FingerprintScope.RECONCILIATION_STATE
    ) != fingerprint_state([alternate_outcome], scope=FingerprintScope.RECONCILIATION_STATE)


def test_repair_plan_semantic_change_changes_its_content_digest() -> None:
    first = _plan()
    changed_record = _record(quantity=11)
    changed_action = RepairAction.from_outcome(
        action_id=RepairActionId("rac_create-001"),
        conflict_id=ConflictId("cnf_missing-001"),
        state_fingerprint=STATE,
        outcome=ReconciliationOutcome((changed_record,), ()),
    )
    changed = RepairPlan(RepairPlanId("rpl_inventory-001"), STATE, (changed_action,))

    assert fingerprint_state(
        [first],
        scope=FingerprintScope.REPAIR_PLAN_CONTENT,
    ) != fingerprint_state([changed], scope=FingerprintScope.REPAIR_PLAN_CONTENT)


def test_fingerprint_consumes_a_one_shot_iterable_once() -> None:
    iterations = 0

    def values() -> Iterator[InventoryRecord]:
        nonlocal iterations
        iterations += 1
        yield _record()

    fingerprint = fingerprint_state(values(), scope=FingerprintScope.INVENTORY_STATE)

    assert isinstance(fingerprint, StateFingerprint)
    assert iterations == 1


def test_fingerprint_item_limit_accepts_boundary_and_rejects_one_more() -> None:
    record = _record()

    at_limit = fingerprint_state(
        (record for _ in range(MAX_FINGERPRINT_ITEMS)),
        scope=FingerprintScope.INVENTORY_STATE,
    )
    assert isinstance(at_limit, StateFingerprint)
    with pytest.raises(CanonicalEncodingError) as captured:
        fingerprint_state(
            (record for _ in range(MAX_FINGERPRINT_ITEMS + 1)),
            scope=FingerprintScope.INVENTORY_STATE,
        )
    assert captured.value.reason is CanonicalErrorCode.INVALID_CANONICAL_VALUE
    assert captured.value.subject_type == "fingerprint.item-count"


@pytest.mark.parametrize(
    ("scope", "value"),
    [
        (FingerprintScope.INVENTORY_STATE, ReconciliationOutcome((_record(),), ())),
        (FingerprintScope.RECONCILIATION_STATE, _record()),
        (FingerprintScope.REPAIR_PLAN_CONTENT, _record()),
    ],
)
def test_each_scope_rejects_values_from_another_semantic_state(
    scope: FingerprintScope,
    value: object,
) -> None:
    with pytest.raises(CanonicalEncodingError) as captured:
        fingerprint_state([value], scope=scope)

    assert captured.value.reason is CanonicalErrorCode.UNSUPPORTED_CANONICAL_TYPE
    assert captured.value.subject_type == f"fingerprint.{scope.value}"


def test_invalid_scope_values_and_non_iterables_raise_typed_errors() -> None:
    with pytest.raises(CanonicalEncodingError) as captured_scope:
        fingerprint_state([], scope="inventory_state")  # type: ignore[arg-type]
    with pytest.raises(CanonicalEncodingError) as captured_values:
        fingerprint_state(1, scope=FingerprintScope.INVENTORY_STATE)  # type: ignore[arg-type]

    assert captured_scope.value.subject_type == "fingerprint.scope"
    assert captured_values.value.subject_type == "fingerprint.values"


def test_iterator_failure_is_translated_to_a_typed_fingerprint_error() -> None:
    class BrokenIterator:
        def __iter__(self) -> BrokenIterator:
            return self

        def __next__(self) -> InventoryRecord:
            raise RuntimeError("synthetic iterator failure")

    with pytest.raises(CanonicalEncodingError) as captured:
        fingerprint_state(BrokenIterator(), scope=FingerprintScope.INVENTORY_STATE)

    assert captured.value.reason is CanonicalErrorCode.INVALID_CANONICAL_VALUE
    assert captured.value.subject_type == "fingerprint.values"
    assert isinstance(captured.value.__cause__, RuntimeError)


def test_iterator_creation_failure_is_translated_to_a_typed_fingerprint_error() -> None:
    class BrokenIterable:
        def __iter__(self) -> Iterator[InventoryRecord]:
            raise RuntimeError("synthetic iterator creation failure")

    with pytest.raises(CanonicalEncodingError) as captured:
        fingerprint_state(BrokenIterable(), scope=FingerprintScope.INVENTORY_STATE)

    assert captured.value.reason is CanonicalErrorCode.INVALID_CANONICAL_VALUE
    assert captured.value.subject_type == "fingerprint.values"
    assert isinstance(captured.value.__cause__, RuntimeError)


def test_fingerprint_version_validation_uses_the_canonical_encoder_contract() -> None:
    with pytest.raises(CanonicalEncodingError) as captured:
        fingerprint_state(
            [],
            scope=FingerprintScope.INVENTORY_STATE,
            version=2,  # type: ignore[arg-type]
        )

    assert captured.value.reason is CanonicalErrorCode.UNSUPPORTED_CANONICAL_VERSION
    assert captured.value.version == 2
    assert fingerprint_state(
        [],
        scope=FingerprintScope.INVENTORY_STATE,
        version=CanonicalVersion.V1,
    )


def test_concurrent_computation_matches_the_sequential_reference() -> None:
    records = [_record(sku=f"SKU-{index:03d}", quantity=index) for index in range(50)]
    expected = fingerprint_state(records, scope=FingerprintScope.INVENTORY_STATE)

    with ThreadPoolExecutor(max_workers=8) as executor:
        futures = tuple(executor.submit(_inventory_fingerprint, records) for _ in range(32))
        results = tuple(future.result() for future in futures)

    assert results == (expected,) * 32


def test_hash_seed_does_not_change_cross_process_bytes_or_digest() -> None:
    script = """
from decimal import Decimal
from paritygrid.domain.canonical import FingerprintScope, encode_canonical, fingerprint_state
from paritygrid.domain.models import ConnectorId, InventoryRecord, Money, UtcTimestamp
record = InventoryRecord.create(
    sku="SKU-001",
    name="Widget",
    quantity=10,
    unit_price=Money.parse("USD 12.34"),
    updated_at=UtcTimestamp.parse("2026-08-12T10:00:00Z"),
    connector_id=ConnectorId("con_source-api"),
    source_record_key="source-001",
    attributes=dict({("zeta", "last"), ("alpha", "first")}),
)
print(encode_canonical(record).decode("ascii"))
print(fingerprint_state([record], scope=FingerprintScope.INVENTORY_STATE))
"""
    outputs: list[str] = []
    for seed in ("1", "73", "999"):
        environment = os.environ.copy()
        environment["PYTHONHASHSEED"] = seed
        outputs.append(
            subprocess.check_output(
                [sys.executable, "-c", script],
                env=environment,
                text=True,
            )
        )

    assert outputs[0] == outputs[1] == outputs[2]


def _inventory_fingerprint(records: list[InventoryRecord]) -> StateFingerprint:
    return fingerprint_state(records, scope=FingerprintScope.INVENTORY_STATE)

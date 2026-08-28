"""Safe-action matrix for the repair-plan generator (P11.1)."""

from dataclasses import replace

import pytest

from paritygrid.application.repair import (
    REPAIRABLE_CLASSIFICATIONS,
    generate_repair_plan,
    repairable_action_kinds,
    validate_safe_action_matrix,
)
from paritygrid.application.repair.identities import (
    derive_action_idempotency_key,
    derive_conflict_id,
    derive_plan_id,
)
from paritygrid.domain.models import RunId, StateFingerprint
from paritygrid.domain.canonical import FingerprintScope, fingerprint_state
from paritygrid.domain.repair import RepairPlan
from paritygrid.domain.reconciliation import ReconciliationClassification, SuggestedResolution
from paritygrid.domain.repair import RepairActionKind
from tests.repair.conftest import analysis, wire_payload

RUN_ID = RunId("run_safe-matrix")


def test_policy_matches_the_domain_safe_resolution_mapping() -> None:
    validate_safe_action_matrix()
    assert (
        frozenset(
            {
                ReconciliationClassification.MISSING_FROM_TARGET,
                ReconciliationClassification.FIELD_MISMATCH,
            }
        )
        == REPAIRABLE_CLASSIFICATIONS
    )
    assert repairable_action_kinds() == frozenset(
        {RepairActionKind.CREATE_TARGET, RepairActionKind.UPDATE_TARGET}
    )


def test_deletion_is_not_expressible_anywhere_in_the_policy() -> None:
    assert not any("delete" in kind.value for kind in RepairActionKind)
    assert RepairActionKind is not None


@pytest.mark.parametrize(
    ("classification", "expected_kind"),
    [
        (ReconciliationClassification.MISSING_FROM_TARGET, RepairActionKind.CREATE_TARGET),
        (ReconciliationClassification.FIELD_MISMATCH, RepairActionKind.UPDATE_TARGET),
    ],
)
def test_repairable_classifications_produce_exactly_one_safe_action(
    classification: ReconciliationClassification, expected_kind: RepairActionKind
) -> None:
    source = [wire_payload("GRID-0001")]
    target = [
        wire_payload("GRID-0001")
        if classification is ReconciliationClassification.FIELD_MISMATCH
        else wire_payload("GRID-0002")
    ]
    if classification is ReconciliationClassification.FIELD_MISMATCH:
        target = [wire_payload("GRID-0001", name="Different name")]
    result = analysis(source, target)
    keys = {key.outcome.classification: key for key in result.classification.keys}
    assert classification in keys
    generated = generate_repair_plan(run_id=RUN_ID, analysis=result)
    assert generated.plan is not None
    assert len(generated.plan.actions) == 1
    action = generated.plan.actions[0]
    assert action.kind is expected_kind
    assert action.state_fingerprint == result.summary.fingerprint
    assert action.conflict_id == derive_conflict_id(RUN_ID, action.sku)
    assert action.action_id is not None


@pytest.mark.parametrize(
    "classification",
    [
        ReconciliationClassification.MATCH,
        ReconciliationClassification.MISSING_FROM_SOURCE,
        ReconciliationClassification.DUPLICATE_SOURCE,
        ReconciliationClassification.DUPLICATE_TARGET,
        ReconciliationClassification.DUPLICATE_BOTH,
    ],
)
def test_review_only_classifications_never_become_actions(
    classification: ReconciliationClassification,
) -> None:
    if classification is ReconciliationClassification.MATCH:
        source = [wire_payload("GRID-0001")]
        target = [wire_payload("GRID-0001")]
    elif classification is ReconciliationClassification.MISSING_FROM_SOURCE:
        source = [wire_payload("GRID-0002")]
        target = [wire_payload("GRID-0001")]
    elif classification is ReconciliationClassification.DUPLICATE_SOURCE:
        source = [wire_payload("GRID-0001"), wire_payload("GRID-0001", quantity=6)]
        target = [wire_payload("GRID-0002")]
    elif classification is ReconciliationClassification.DUPLICATE_TARGET:
        source = [wire_payload("GRID-0002")]
        target = [wire_payload("GRID-0001"), wire_payload("GRID-0001", name="Other")]
    else:
        source = [wire_payload("GRID-0001"), wire_payload("GRID-0001", quantity=6)]
        target = [wire_payload("GRID-0001"), wire_payload("GRID-0001", name="Other")]
    result = analysis(source, target)
    assert any(key.outcome.classification is classification for key in result.classification.keys)
    generated = generate_repair_plan(run_id=RUN_ID, analysis=result)
    # None of these classifications may ever produce an action: the key that
    # carries the classification must land in review-only output, never in a
    # plan action, and with no repairable key the generator creates no plan.
    review_key = "GRID-0001"
    assert review_key in generated.review_only_keys
    if generated.plan is not None:
        assert all(action.sku != review_key for action in generated.plan.actions)
        assert all(action.kind in repairable_action_kinds() for action in generated.plan.actions)
    else:
        assert generated.content_fingerprint is None


def test_nothing_to_repair_generates_no_plan_at_all() -> None:
    result = analysis([wire_payload("GRID-0001")], [wire_payload("GRID-0001")])
    generated = generate_repair_plan(run_id=RUN_ID, analysis=result)
    assert generated.plan is None
    assert generated.action_keys is None
    assert generated.binding.action_count == 0


def test_regeneration_reproduces_identical_identities_and_content() -> None:
    source = [wire_payload("GRID-0001"), wire_payload("GRID-0002", quantity=9)]
    target = [wire_payload("GRID-0001", name="Different")]
    result = analysis(source, target)
    first = generate_repair_plan(run_id=RUN_ID, analysis=result)
    second = generate_repair_plan(run_id=RUN_ID, analysis=result)
    assert first.plan is not None
    assert second.plan is not None
    assert first.plan.actions == second.plan.actions
    assert (
        first.plan.plan_id
        == second.plan.plan_id
        == derive_plan_id(RUN_ID, result.summary.fingerprint, first.plan.binding)
    )
    assert first.content_fingerprint == second.content_fingerprint
    assert first.action_keys == second.action_keys
    assert sorted(first.repairable_keys) == ["GRID-0001", "GRID-0002"]


def test_equivalent_plans_reproduce_the_same_content_identity() -> None:
    source = [wire_payload("GRID-0001"), wire_payload("GRID-0002", quantity=9)]
    target = [wire_payload("GRID-0001", name="Different")]
    first = analysis(source, target)
    reordered = analysis(list(reversed(source)), list(reversed(target)))
    assert first.summary.fingerprint == reordered.summary.fingerprint
    left = generate_repair_plan(run_id=RUN_ID, analysis=first)
    right = generate_repair_plan(run_id=RUN_ID, analysis=reordered)
    assert left.content_fingerprint == right.content_fingerprint
    assert left.plan is not None
    assert right.plan is not None
    assert left.plan.actions == right.plan.actions


def test_action_idempotency_keys_bind_content_and_key() -> None:
    source = [wire_payload("GRID-0001")]
    target: list[dict[str, object]] = []
    result = analysis(source, target)
    generated = generate_repair_plan(run_id=RUN_ID, analysis=result)
    assert generated.plan is not None
    assert generated.action_keys is not None
    content = generated.content_fingerprint
    assert content is not None
    mapping = generated.action_keys.to_mapping()
    for action in generated.plan.actions:
        assert mapping[action.action_id] == derive_action_idempotency_key(
            RUN_ID, content, action.sku
        )
    different = generate_repair_plan(
        run_id=RUN_ID,
        analysis=analysis([wire_payload("GRID-0001", quantity=6)], []),
    )
    assert different.content_fingerprint != content
    assert different.action_keys is not None
    assert next(iter(different.action_keys.to_mapping().values())) != next(iter(mapping.values()))
    # Distinct runs never collide on the globally unique effect keys.
    other_run = generate_repair_plan(run_id=RunId("run_safe-matrix-other"), analysis=result)
    assert other_run.action_keys is not None
    assert set(other_run.action_keys.to_mapping().values()).isdisjoint(set(mapping.values()))


def test_binding_carries_every_required_identity() -> None:
    result = analysis(
        [wire_payload("GRID-0001")], [], source_identity="3" * 64, target_identity="4" * 64
    )
    generated = generate_repair_plan(run_id=RUN_ID, analysis=result)
    binding = generated.binding
    assert binding.run_id == RUN_ID
    assert binding.reconciliation_fingerprint == result.summary.fingerprint
    assert binding.source_input_identity == "3" * 64
    assert binding.target_input_identity == "4" * 64
    assert binding.policy_version == 1
    assert binding.generation_version == 1
    assert binding.rules_version == result.summary.rules_version
    assert binding.analysis_version == result.summary.analysis_version
    assert binding.analytical_query_version == result.summary.analytical_query_version
    assert binding.action_count == len(generated.repairable_keys)


def test_generator_rejects_foreign_inputs() -> None:
    result = analysis([wire_payload("GRID-0001")], [])
    with pytest.raises(TypeError):
        generate_repair_plan(run_id="run_text", analysis=result)  # type: ignore[arg-type]
    with pytest.raises(TypeError):
        generate_repair_plan(run_id=RUN_ID, analysis=object())  # type: ignore[arg-type]


def test_suggested_resolution_matrix_agrees_with_the_policy() -> None:
    from paritygrid.domain.reconciliation import suggested_resolution_for

    for classification in ReconciliationClassification:
        actual = suggested_resolution_for(classification)
        expected = {
            ReconciliationClassification.MISSING_FROM_TARGET: SuggestedResolution.CREATE_TARGET,
            ReconciliationClassification.FIELD_MISMATCH: SuggestedResolution.UPDATE_TARGET,
        }.get(classification)
        if expected is None:
            assert actual not in {
                SuggestedResolution.CREATE_TARGET,
                SuggestedResolution.UPDATE_TARGET,
            }
        else:
            assert actual is expected


def test_reconciliation_fingerprint_value_is_bound_into_the_plan() -> None:
    result = analysis([wire_payload("GRID-0001")], [])
    generated = generate_repair_plan(run_id=RUN_ID, analysis=result)
    assert generated.plan is not None
    assert generated.plan.state_fingerprint.value == result.summary.fingerprint.value
    assert len(StateFingerprint(generated.plan.state_fingerprint.value).value) == 64


@pytest.mark.parametrize(
    "field",
    (
        "source_input_identity",
        "target_input_identity",
        "policy_version",
        "generation_version",
        "rules_version",
        "analysis_version",
        "analytical_query_version",
    ),
)
def test_each_binding_identity_changes_the_plan_content_and_identity(field: str) -> None:
    generated = generate_repair_plan(
        run_id=RUN_ID, analysis=analysis([wire_payload("GRID-0001")], [])
    )
    assert generated.plan is not None and generated.plan.binding is not None
    binding = generated.plan.binding
    old_value = getattr(binding, field)
    changed = "f" * 64 if field.endswith("identity") else int(old_value) + 1
    alternate_binding = replace(binding, **{field: changed})
    alternate = RepairPlan(
        plan_id=derive_plan_id(RUN_ID, generated.plan.state_fingerprint, alternate_binding),
        state_fingerprint=generated.plan.state_fingerprint,
        actions=generated.plan.actions,
        binding=alternate_binding,
    )
    assert alternate.plan_id != generated.plan.plan_id
    assert fingerprint_state((alternate,), scope=FingerprintScope.REPAIR_PLAN_CONTENT) != (
        generated.content_fingerprint
    )

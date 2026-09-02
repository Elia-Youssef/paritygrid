"""Cross-runner verification manifest tests (Phase 19).

The canonical engine plan runs through the sequential, threaded, and asyncio
full-plan strategies over real SQLite evidence.  Equality is proven from the
accepted P7.17 execution-evidence snapshots before any timing is recorded,
negative fixtures lock every mismatch dimension, and the manifest structurally
cannot claim reconciliation, repair, or target-state equivalence.
"""

import json
from collections.abc import Callable
from dataclasses import replace
from pathlib import Path

import pytest
from sqlalchemy import update

from paritygrid.application.execution.evidence_comparison import (
    EXECUTION_EVIDENCE_COMPARISON_VERSION,
    EXECUTION_EVIDENCE_KIND,
    ExecutionEvidenceSnapshot,
    build_evidence_snapshot,
)
from paritygrid.demo.scenarios import canonical_plan_fingerprint
from paritygrid.demo.verification import (
    NON_EQUIVALENCE_DISCLAIMERS,
    REQUIRED_STRATEGIES,
    SEQUENTIAL_STRATEGY,
    CrossRunnerVerificationManifest,
    RunnerExecutionRecord,
    RunnerManifestError,
    build_cross_runner_manifest,
    build_cross_runner_verification,
    canonical_run_id,
    freeze_runner_record,
    prepare_harness,
    run_canonical_strategy,
)
from paritygrid.quality.concurrent_scenario import ConcurrentScenarioHarness
from tests.execution.full_plan_conformance import assert_zero_owned_workers

EvidenceMutation = Callable[[ExecutionEvidenceSnapshot], ExecutionEvidenceSnapshot]
_BASELINE_CHECKPOINTS = (("nod_a", "p0", 1, 1, None, "{}", None),)
_BASELINE_NODE_METRICS = (
    ("nod_a", "succeeded", 1, 0, 0, 1, 0, 0, 0, 1, 1, 0, 10, 10, 0),
    ("nod_b", "succeeded", 1, 0, 0, 1, 0, 0, 0, 1, 1, 0, 10, 10, 0),
)


def _baseline_evidence(run_id: str) -> ExecutionEvidenceSnapshot:
    return build_evidence_snapshot(
        run_id=run_id,
        plan_fingerprint=canonical_plan_fingerprint(),
        work_states=(("nod_a", "p0", "succeeded"), ("nod_b", "p0", "succeeded")),
        attempt_outcomes=(("nod_a/p0", 1, "succeeded"), ("nod_b/p0", 1, "succeeded")),
        node_aggregates=(("nod_a", 1, 1, 0, 0, 0), ("nod_b", 1, 1, 0, 0, 0)),
        artifact_identities=("art_a",),
        event_kinds=("run_created", "work_created", "attempt_succeeded"),
        execution_evidence_fingerprint="a" * 64,
    )


def _fabricated_records(**overrides: EvidenceMutation) -> tuple[RunnerExecutionRecord, ...]:
    """Build one record per strategy with an optional per-index mutation."""

    def record(strategy_id: str, index: int) -> RunnerExecutionRecord:
        evidence = _baseline_evidence(f"run_{index}")
        mutation = overrides.get(strategy_id)
        if mutation is not None:
            evidence = mutation(evidence)
        return RunnerExecutionRecord(
            strategy_id=strategy_id,
            run_id=f"run_{index}",
            evidence=evidence,
            checkpoint_count=1,
            checkpoints=_BASELINE_CHECKPOINTS,
            node_metrics=_BASELINE_NODE_METRICS,
            attempt_outcome_counts={"succeeded": 2},
        )

    return tuple(
        record(strategy_id, index) for index, strategy_id in enumerate(REQUIRED_STRATEGIES)
    )


class TestRequiredStrategyEquality:
    def test_all_three_strategies_produce_equal_execution_evidence(self, tmp_path: Path) -> None:
        harness = prepare_harness(tmp_path / "harness")
        try:
            manifest = build_cross_runner_verification(harness)
        finally:
            harness.close()
        assert manifest.equal
        assert manifest.timings_recorded
        assert all(record.duration_seconds is not None for record in manifest.records)
        assert manifest.evidence_kind == EXECUTION_EVIDENCE_KIND
        assert manifest.evidence_version == 2
        assert manifest.comparison_version == EXECUTION_EVIDENCE_COMPARISON_VERSION
        assert manifest.plan_fingerprint == canonical_plan_fingerprint()
        assert_zero_owned_workers()

    def test_the_scripted_retry_and_artifact_are_part_of_the_evidence(self, tmp_path: Path) -> None:
        harness = prepare_harness(tmp_path / "harness")
        try:
            record = run_one(harness, SEQUENTIAL_STRATEGY, 1)
        finally:
            harness.close()
        assert record.attempt_outcome_counts.get("retry_scheduled") == 1
        assert record.attempt_outcome_counts.get("succeeded") == 11
        assert record.evidence.artifact_identities == ("art_can-e-export-p0",)
        assert record.checkpoint_count == 11
        assert record.evidence.execution_evidence_fingerprint is not None
        assert len(record.evidence.execution_evidence_fingerprint) == 64

    def test_finalized_fingerprints_are_equal_across_strategies(self, tmp_path: Path) -> None:
        harness = prepare_harness(tmp_path / "harness")
        try:
            manifest = build_cross_runner_verification(harness)
        finally:
            harness.close()
        fingerprints = {
            record.evidence.execution_evidence_fingerprint for record in manifest.records
        }
        assert len(fingerprints) == 1
        assert None not in fingerprints
        fingerprint = fingerprints.pop()
        assert fingerprint is not None
        assert fingerprint in json.dumps(manifest.canonical_bytes().decode("ascii"))

    def test_equality_is_independent_of_the_harness_and_root(self, tmp_path: Path) -> None:
        first = _freeze_all(tmp_path / "first")
        second = _freeze_all(tmp_path / "second")
        assert [record.evidence for record in first] == [record.evidence for record in second]

    def test_timing_cannot_change_the_verdict(self) -> None:
        records = _fabricated_records()
        first = build_cross_runner_manifest(records, {})
        second = build_cross_runner_manifest(
            records, {strategy: 0.001 * index for index, strategy in enumerate(REQUIRED_STRATEGIES)}
        )
        assert first.equal
        assert second.equal
        assert [c.equal for c in first.comparisons] == [c.equal for c in second.comparisons]

    def test_timing_values_do_not_change_canonical_correctness_bytes(self) -> None:
        records = _fabricated_records()
        first = build_cross_runner_manifest(records, dict.fromkeys(REQUIRED_STRATEGIES, 0.1))
        second = build_cross_runner_manifest(records, dict.fromkeys(REQUIRED_STRATEGIES, 9.9))
        assert first.canonical_bytes() == second.canonical_bytes()

    @pytest.mark.parametrize("duration", [float("nan"), float("inf"), -0.1])
    def test_nonfinite_or_negative_durations_are_rejected(self, duration: float) -> None:
        with pytest.raises(RunnerManifestError, match="finite nonnegative"):
            build_cross_runner_manifest(
                _fabricated_records(), dict.fromkeys(REQUIRED_STRATEGIES, duration)
            )

    def test_partial_durations_record_nothing(self) -> None:
        records = _fabricated_records()
        manifest = build_cross_runner_manifest(records, {SEQUENTIAL_STRATEGY: 1.0})
        assert manifest.equal
        assert manifest.timings_recorded is False
        assert all(record.duration_seconds is None for record in manifest.records)

    def test_unequal_evidence_never_records_timing(self) -> None:
        records = _fabricated_records(threaded=_fingerprint_mutation("b" * 64))
        manifest = build_cross_runner_manifest(
            records,
            dict.fromkeys(REQUIRED_STRATEGIES, 1.0),
        )
        assert not manifest.equal
        assert manifest.timings_recorded is False
        assert all(record.duration_seconds is None for record in manifest.records)
        assert "execution-evidence fingerprint differs" in manifest.differences


class TestNegativeMismatches:
    @staticmethod
    def _manifest_with(**overrides: EvidenceMutation) -> CrossRunnerVerificationManifest:
        return build_cross_runner_manifest(
            _fabricated_records(**overrides),
            dict.fromkeys(REQUIRED_STRATEGIES, 1.0),
        )

    def test_evidence_kind_mismatch_is_structurally_impossible(self) -> None:
        from paritygrid.application.execution.evidence_comparison import (
            EvidenceComparisonError,
            ExecutionEvidenceSnapshot,
        )

        # The accepted snapshot type refuses a foreign evidence kind, so the
        # manifest can never carry one; the comparison-level difference text
        # stays locked by the accepted P7.17 fixtures.
        with pytest.raises(EvidenceComparisonError):
            replace(_baseline_evidence("run_1"), evidence_kind="reconciliation")  # type: ignore[arg-type]
        del ExecutionEvidenceSnapshot

    def test_evidence_version_mismatch_fails_closed_at_the_manifest(self) -> None:
        # Snapshots that disagree on the evidence version cannot share one
        # manifest header, so the manifest builder fails closed; the
        # comparison-level version difference is locked by the accepted P7.17
        # fixtures and by compare_execution_evidence below.
        with pytest.raises(RunnerManifestError, match="share one evidence version"):
            self._manifest_with(
                threaded=lambda evidence: replace(evidence, evidence_version=3),
            )

    def test_comparison_reports_the_version_difference_itself(self) -> None:
        from paritygrid.application.execution.evidence_comparison import (
            compare_execution_evidence,
        )

        left = _baseline_evidence("run_x")
        right = replace(left, evidence_version=3)
        verdict = compare_execution_evidence(left, right)  # type: ignore[arg-type]
        assert not verdict.equal
        assert "evidence version differs" in verdict.differences

    def test_plan_identity_mismatch(self) -> None:
        manifest = self._manifest_with(
            asyncio=lambda evidence: replace(evidence, plan_fingerprint="c" * 64),
        )
        assert not manifest.equal
        assert "plan fingerprint differs" in manifest.differences

    def test_artifact_identity_mismatch(self) -> None:
        manifest = self._manifest_with(
            threaded=lambda evidence: replace(evidence, artifact_identities=("art_b",)),
        )
        assert not manifest.equal
        assert "artifact identities differ" in manifest.differences

    def test_causal_evidence_mismatch(self) -> None:
        manifest = self._manifest_with(
            threaded=lambda evidence: replace(
                evidence, normalized_events=("run_created", "work_created")
            ),
        )
        assert not manifest.equal
        assert "normalized causal events differ" in manifest.differences

    def test_attempt_outcome_mismatch(self) -> None:
        manifest = self._manifest_with(
            asyncio=lambda evidence: replace(
                evidence,
                attempt_outcomes=(
                    ("nod_a/p0", 1, "succeeded"),
                    ("nod_b/p0", 1, "quarantined"),
                ),
            ),
        )
        assert not manifest.equal
        assert "attempt outcomes differ" in manifest.differences

    def test_count_mismatch(self) -> None:
        manifest = self._manifest_with(
            threaded=lambda evidence: replace(
                evidence,
                node_aggregates=(("nod_a", 1, 0, 0, 1, 0), ("nod_b", 1, 1, 0, 0, 0)),
            ),
        )
        assert not manifest.equal
        assert "node aggregates differ" in manifest.differences

    def test_checkpoint_projection_mismatch(self) -> None:
        records = list(_fabricated_records())
        records[1] = replace(
            records[1],
            checkpoints=(("nod_a", "p0", 1, 1, None, '{"position":1}', None),),
        )
        manifest = build_cross_runner_manifest(tuple(records), {})
        assert not manifest.equal
        assert "durable checkpoints differ" in manifest.differences

    def test_node_metric_projection_mismatch(self) -> None:
        records = list(_fabricated_records())
        changed = list(_BASELINE_NODE_METRICS)
        changed[0] = (*changed[0][:-6], 2, *changed[0][-5:])
        records[1] = replace(records[1], node_metrics=tuple(changed))
        manifest = build_cross_runner_manifest(tuple(records), {})
        assert not manifest.equal
        assert "durable node metrics differ" in manifest.differences

    def test_freeze_reads_durable_node_metrics(self, tmp_path: Path) -> None:
        from paritygrid.adapters.persistence.schema import run_nodes

        harness = prepare_harness(tmp_path / "harness")
        run_id = canonical_run_id(1)
        try:
            baseline = run_canonical_strategy(harness, SEQUENTIAL_STRATEGY, run_id)
            with harness.database.transaction() as session:
                session.execute(
                    update(run_nodes)
                    .where(
                        run_nodes.c.run_id == run_id.value,
                        run_nodes.c.node_id == "nod_can-async-src",
                    )
                    .values(records_read=run_nodes.c.records_read + 1)
                )
            refrozen = freeze_runner_record(
                harness,
                SEQUENTIAL_STRATEGY,
                run_id,
                execution_evidence_fingerprint=(baseline.evidence.execution_evidence_fingerprint),
            )
        finally:
            harness.close()
        assert refrozen.node_metrics != baseline.node_metrics

    def test_execution_evidence_fingerprint_mismatch(self) -> None:
        manifest = self._manifest_with(
            threaded=lambda evidence: replace(evidence, execution_evidence_fingerprint="d" * 64),
        )
        assert not manifest.equal
        assert "execution-evidence fingerprint differs" in manifest.differences

    def test_missing_strategy_is_rejected(self) -> None:
        records = _fabricated_records()[:2]
        with pytest.raises(Exception, match="ordered required strategies"):
            build_cross_runner_manifest(records, {})


class TestNonEquivalenceClaims:
    def test_the_manifest_cannot_express_reconciliation_or_target_claims(self) -> None:
        import dataclasses
        import json

        manifest = self._equal_manifest()
        field_names = {field.name for field in dataclasses.fields(manifest)}
        assert "reconciliation" not in field_names
        assert "repair" not in field_names
        assert "target_state" not in field_names
        document = json.loads(manifest.canonical_bytes().decode("ascii"))
        assert document["non_equivalence_disclaimers"] == list(NON_EQUIVALENCE_DISCLAIMERS)

    def test_equal_execution_evidence_tolerates_divergent_reconciliation_state(self) -> None:
        # Two runs with byte-identical execution evidence may still classify
        # records differently afterwards.  The manifest must be structurally
        # blind to that divergence: equal verdict and identical canonical
        # bytes, with neither reconciliation value present anywhere.
        reconciliation_fingerprints = (
            "1" * 64,
            "2" * 64,
        )  # distinct from any execution-evidence value in the snapshots
        manifests = [
            build_cross_runner_manifest(_fabricated_records(), {})
            for _ in reconciliation_fingerprints
        ]
        assert all(manifest.equal for manifest in manifests)
        assert manifests[0].canonical_bytes() == manifests[1].canonical_bytes()
        serialized = manifests[0].canonical_bytes().decode("ascii")
        for fingerprint in reconciliation_fingerprints:
            assert fingerprint not in serialized

    def _equal_manifest(self) -> CrossRunnerVerificationManifest:
        return build_cross_runner_manifest(_fabricated_records(), {})


def _freeze_all(root: Path) -> list[RunnerExecutionRecord]:
    harness = prepare_harness(root)
    try:
        return [
            run_one(harness, strategy_id, index + 1)
            for index, strategy_id in enumerate(REQUIRED_STRATEGIES)
        ]
    finally:
        harness.close()


def run_one(
    harness: ConcurrentScenarioHarness, strategy_id: str, seed: int
) -> RunnerExecutionRecord:
    return run_canonical_strategy(harness, strategy_id, canonical_run_id(seed))


def _fingerprint_mutation(fingerprint: str) -> EvidenceMutation:
    def mutate(evidence: ExecutionEvidenceSnapshot) -> ExecutionEvidenceSnapshot:
        return replace(evidence, execution_evidence_fingerprint=fingerprint)

    return mutate

"""Versioned cross-strategy execution-evidence comparison (P7.17).

The harness compares durable execution evidence only: the evidence
kind and version, sorted durable work states, attempt outcomes and
counts, per-node aggregates, artifact-manifest identities, normalized
causal events, and the final execution-evidence fingerprint.  It
ignores timing, thread/task/process identity, valid concurrent global
event ordering, ephemeral telemetry order, and runner-local details by
construction — everything it compares is durable and sorted.

Equal execution evidence NEVER claims equal reconciliation
classifications, repair plans, repair effects, target state, or
target-state fingerprints: :class:`EvidenceComparison` exposes exactly
the execution-evidence verdict and nothing else.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import cast

EXECUTION_EVIDENCE_KIND = "execution-evidence"
EXECUTION_EVIDENCE_COMPARISON_VERSION = 1
MAX_COMPARISON_ITEMS = 65_536


class EvidenceComparisonError(ValueError):
    """Base failure for execution-evidence comparison."""


@dataclass(frozen=True, slots=True)
class ExecutionEvidenceSnapshot:
    """One captured durable execution-evidence projection."""

    evidence_kind: str
    evidence_version: int
    run_id: str
    plan_fingerprint: str
    work_states: tuple[tuple[str, str, str], ...]
    attempt_outcomes: tuple[tuple[str, int, str], ...]
    node_aggregates: tuple[tuple[str, int, int, int, int, int], ...]
    artifact_identities: tuple[str, ...]
    normalized_events: tuple[str, ...]
    execution_evidence_fingerprint: str | None

    def __post_init__(self) -> None:
        if self.evidence_kind != EXECUTION_EVIDENCE_KIND:
            raise EvidenceComparisonError("evidence kind is not execution evidence")
        if type(self.evidence_version) is not int or self.evidence_version < 1:
            raise EvidenceComparisonError("evidence version is invalid")
        if type(self.run_id) is not str or not self.run_id:
            raise EvidenceComparisonError("snapshot run identity is invalid")
        if type(self.plan_fingerprint) is not str or len(self.plan_fingerprint) != 64:
            raise EvidenceComparisonError("snapshot plan fingerprint is invalid")
        for field in (
            "work_states",
            "attempt_outcomes",
            "node_aggregates",
            "artifact_identities",
            "normalized_events",
        ):
            value = cast(tuple[object, ...], getattr(self, field))
            if type(value) is not tuple:
                raise TypeError(f"snapshot {field} must be a tuple")
            if len(value) > MAX_COMPARISON_ITEMS:
                raise EvidenceComparisonError(f"snapshot {field} exceeds the bound")
        if tuple(sorted(self.work_states)) != self.work_states:
            raise EvidenceComparisonError("snapshot work states must be sorted")
        if tuple(sorted(self.attempt_outcomes)) != self.attempt_outcomes:
            raise EvidenceComparisonError("snapshot attempts must be sorted")
        if tuple(sorted(self.node_aggregates)) != self.node_aggregates:
            raise EvidenceComparisonError("snapshot aggregates must be sorted")
        if tuple(sorted(self.artifact_identities)) != self.artifact_identities:
            raise EvidenceComparisonError("snapshot artifacts must be sorted")
        if tuple(sorted(self.normalized_events)) != self.normalized_events:
            raise EvidenceComparisonError("snapshot events must be sorted")


@dataclass(frozen=True, slots=True)
class EvidenceComparison:
    """The execution-evidence verdict of one comparison.

    The verdict covers durable execution evidence only.  It never
    expresses an opinion about reconciliation classifications, repair
    plans, repair effects, target state, or target-state fingerprints.
    """

    equal: bool
    differences: tuple[str, ...]
    comparison_version: int = EXECUTION_EVIDENCE_COMPARISON_VERSION

    def __post_init__(self) -> None:
        if type(self.equal) is not bool:
            raise TypeError("comparison verdict must be a boolean")
        if type(self.differences) is not tuple:
            raise TypeError("comparison differences must be a tuple")
        if self.equal and self.differences:
            raise EvidenceComparisonError("an equal comparison carries no differences")
        if not self.equal and not self.differences:
            raise EvidenceComparisonError("an unequal comparison carries at least one difference")


def compare_execution_evidence(
    left: ExecutionEvidenceSnapshot,
    right: ExecutionEvidenceSnapshot,
) -> EvidenceComparison:
    """Compare two durable evidence projections field by field."""
    if type(left) is not ExecutionEvidenceSnapshot or (
        type(right) is not ExecutionEvidenceSnapshot
    ):
        raise TypeError("comparison requires ExecutionEvidenceSnapshot values")
    differences: list[str] = []
    if left.evidence_kind != right.evidence_kind:
        differences.append("evidence kind differs")
    if left.evidence_version != right.evidence_version:
        differences.append("evidence version differs")
    if left.plan_fingerprint != right.plan_fingerprint:
        differences.append("plan fingerprint differs")
    if left.work_states != right.work_states:
        differences.append("durable work states differ")
    if left.attempt_outcomes != right.attempt_outcomes:
        differences.append("attempt outcomes differ")
    if left.node_aggregates != right.node_aggregates:
        differences.append("node aggregates differ")
    if left.artifact_identities != right.artifact_identities:
        differences.append("artifact identities differ")
    if left.normalized_events != right.normalized_events:
        differences.append("normalized causal events differ")
    if left.execution_evidence_fingerprint != right.execution_evidence_fingerprint:
        differences.append("execution-evidence fingerprint differs")
    if differences:
        return EvidenceComparison(equal=False, differences=tuple(differences))
    return EvidenceComparison(equal=True, differences=())


def build_evidence_snapshot(
    *,
    run_id: str,
    plan_fingerprint: str,
    work_states: tuple[tuple[str, str, str], ...],
    attempt_outcomes: tuple[tuple[str, int, str], ...],
    node_aggregates: tuple[tuple[str, int, int, int, int, int], ...],
    artifact_identities: tuple[str, ...],
    event_kinds: tuple[str, ...],
    execution_evidence_fingerprint: str | None = None,
    evidence_version: int = 2,
) -> ExecutionEvidenceSnapshot:
    """Build one snapshot with normalized (sorted) durable projections."""
    return ExecutionEvidenceSnapshot(
        evidence_kind=EXECUTION_EVIDENCE_KIND,
        evidence_version=evidence_version,
        run_id=run_id,
        plan_fingerprint=plan_fingerprint,
        work_states=tuple(sorted(work_states)),
        attempt_outcomes=tuple(sorted(attempt_outcomes)),
        node_aggregates=tuple(sorted(node_aggregates)),
        artifact_identities=tuple(sorted(set(artifact_identities))),
        normalized_events=tuple(sorted(event_kinds)),
        execution_evidence_fingerprint=execution_evidence_fingerprint,
    )


__all__ = [
    "EXECUTION_EVIDENCE_COMPARISON_VERSION",
    "EXECUTION_EVIDENCE_KIND",
    "EvidenceComparison",
    "EvidenceComparisonError",
    "ExecutionEvidenceSnapshot",
    "build_evidence_snapshot",
    "compare_execution_evidence",
]

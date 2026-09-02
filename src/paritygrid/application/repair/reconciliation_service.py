"""Durable persistence of one reconciliation snapshot and its conflicts.

The reconciliation result service turns one Phase 10 analysis into the
immutable ``reconciliation_summaries`` and ``reconciliation_conflicts``
rows every later repair fact is foreign-keyed to. Persisting is
idempotent: replaying the same analysis returns the stored fact, while a
different analysis for the same run is rejected as a conflict because the
stored snapshot can never be mutated or replaced.
"""

from collections.abc import Callable
from dataclasses import dataclass
from typing import cast

from paritygrid.application.ports.reconciliation_persistence import (
    PersistedConflict,
    ReconciliationInvalidRequestError,
    ReconciliationResultConflictError,
    ReconciliationResultRecord,
    ReconciliationSummaryRecord,
)
from paritygrid.application.ports.writer import TransactionalWriter
from paritygrid.application.reconciliation.analysis import ReconciliationAnalysis
from paritygrid.application.repair.companions import (
    MutationFrontier,
    build_companions,
    frontier_from_evidence,
    submit_command,
)
from paritygrid.application.repair.errors import (
    RepairRunNotFoundError,
    RepairWriterOutcomeUnknownError,
)
from paritygrid.application.repair.evidence import RepairWorkflowReader
from paritygrid.application.repair.identities import derive_conflict_id
from paritygrid.application.writes.reconciliation import (
    RECONCILIATION_EVENT_PAYLOAD_SCHEMA_VERSION,
    PersistReconciliation,
    PersistReconciliationResult,
)
from paritygrid.domain.models import RunId, UtcTimestamp

MAX_CONFLICT_PROJECTION = 100_000


@dataclass(frozen=True, slots=True)
class PersistedReconciliationOutcome:
    """The durable result of one reconciliation persistence attempt."""

    record: ReconciliationResultRecord
    replayed: bool
    frontier: MutationFrontier


class ReconciliationResultService:
    """Persist reconciliation snapshots through the transactional writer."""

    def __init__(
        self,
        writer: TransactionalWriter,
        reader: RepairWorkflowReader,
        *,
        now: Callable[[], UtcTimestamp],
        timeout_seconds: float = 30.0,
    ) -> None:
        self._writer = writer
        self._reader = reader
        self._now = now
        self._timeout_seconds = timeout_seconds

    def persist(
        self,
        *,
        run_id: RunId,
        analysis: ReconciliationAnalysis,
        actor: str,
        correlation_id: str,
    ) -> PersistedReconciliationOutcome:
        """Persist one analysis idempotently for exactly one run."""
        if type(run_id) is not RunId:
            raise TypeError("reconciliation persistence requires RunId")
        if type(analysis) is not ReconciliationAnalysis:
            raise TypeError("reconciliation persistence requires ReconciliationAnalysis")
        evidence = self._reader.load(run_id)
        from paritygrid.domain.execution import RunState

        if evidence.run.state not in {RunState.SUCCEEDED, RunState.PARTIALLY_SUCCEEDED}:
            raise ReconciliationInvalidRequestError(
                "reconciliation results require a completed run"
            )
        replayed = evidence.summary is not None
        if replayed:
            _require_same_snapshot(cast(ReconciliationSummaryRecord, evidence.summary), analysis)
            stored = self._reader.load_reconciliation_result(run_id)
            if stored is None:
                raise RepairRunNotFoundError("the durable reconciliation result disappeared")
            return PersistedReconciliationOutcome(
                record=stored,
                replayed=True,
                frontier=frontier_from_evidence(evidence),
            )
        occurred_at = self._now()
        conflicts = build_persisted_conflicts(run_id, analysis, occurred_at)
        companions = build_companions(
            frontier=frontier_from_evidence(evidence),
            run_id=run_id,
            operation="reconciliation_persisted",
            object_kind="reconciliation_summary",
            object_id=run_id.value,
            actor=actor,
            correlation_id=correlation_id,
            occurred_at=occurred_at,
            payload=_summary_payload(analysis, len(conflicts)),
            event_payload_schema_version=RECONCILIATION_EVENT_PAYLOAD_SCHEMA_VERSION,
        )
        command = PersistReconciliation(
            run_id=run_id,
            summary=analysis.summary,
            conflicts=conflicts,
            created_at=occurred_at,
            companions=companions,
        )
        try:
            _, result, _mutated = self._submit_with_replay(command)
        except ReconciliationResultConflictError:
            # A concurrent persist of a different analysis won the race; the
            # durable snapshot is immutable, so the divergence is terminal.
            raise
        record = cast(PersistReconciliationResult, result).record
        return PersistedReconciliationOutcome(
            record=record,
            replayed=False,
            frontier=MutationFrontier(
                run_row_version=evidence.run.row_version + 1,
                next_event_sequence=evidence.event_counter.next_sequence_number + 1,
                event_counter_row_version=evidence.event_counter.row_version + 1,
            ),
        )

    def _submit_with_replay(self, command: PersistReconciliation) -> tuple[object, object, bool]:
        """Submit the persist, resubmitting the identical command on unknown outcome."""
        attempts = 0
        while True:
            attempts += 1
            try:
                return submit_command(self._writer, command, timeout_seconds=self._timeout_seconds)
            except RepairWriterOutcomeUnknownError:
                if attempts >= 3:
                    raise
                continue


def build_persisted_conflicts(
    run_id: RunId,
    analysis: ReconciliationAnalysis,
    created_at: UtcTimestamp,
) -> tuple[PersistedConflict, ...]:
    """Project one analysis's conflict rows onto immutable persisted conflicts."""
    if len(analysis.conflicts) > MAX_CONFLICT_PROJECTION:
        raise ValueError("reconciliation conflicts exceed the supported bound")
    return tuple(
        PersistedConflict(
            conflict_id=derive_conflict_id(run_id, row.sku),
            canonical_key=row.sku,
            classification=row.classification,
            source_references=tuple(
                sorted(zip(row.source_positions, row.source_record_keys, strict=True))
            ),
            target_references=tuple(
                sorted(zip(row.target_positions, row.target_record_keys, strict=True))
            ),
            differences=row.differences,
            suggested_resolution=row.suggested_resolution,
            created_at=created_at,
        )
        for row in sorted(analysis.conflicts, key=lambda item: item.sku)
    )


def _require_same_snapshot(
    stored: ReconciliationSummaryRecord, analysis: ReconciliationAnalysis
) -> None:
    summary = analysis.summary
    if (
        stored.reconciliation_fingerprint != summary.fingerprint
        or stored.source_fingerprint.value != summary.source_input_identity
        or stored.target_fingerprint.value != summary.target_input_identity
    ):
        raise ReconciliationResultConflictError(
            "a different reconciliation snapshot is already durable for this run"
        )


def _summary_payload(analysis: ReconciliationAnalysis, conflict_count: int) -> dict[str, object]:
    summary = analysis.summary
    return {
        "analytical_query_version": summary.analytical_query_version,
        "canonical_key_count": summary.counts.canonical_key_count,
        "conflict_count": conflict_count,
        "reconciliation_fingerprint": summary.fingerprint.value,
        "source_quarantined_count": summary.counts.source_quarantined_count,
        "source_input_identity": summary.source_input_identity,
        "target_input_identity": summary.target_input_identity,
    }


__all__ = [
    "PersistedReconciliationOutcome",
    "ReconciliationResultService",
    "build_persisted_conflicts",
]

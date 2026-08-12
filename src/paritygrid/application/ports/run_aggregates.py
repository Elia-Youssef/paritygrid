"""Dependency-neutral run-node aggregate and run-revision contracts."""

from dataclasses import dataclass
from typing import Protocol

from paritygrid.application.ports.consistency import CheckpointCommit
from paritygrid.application.ports.execution import (
    CompletedWork,
    RunNodeRecord,
    RunRecord,
    WorkClaim,
    WorkItemRecord,
)
from paritygrid.domain.models import NodeId, RunId, UtcTimestamp

MAX_WORK_METRIC = 9_223_372_036_854_775_807


@dataclass(frozen=True, slots=True)
class WorkMetricDelta:
    """Directional metrics contributed by one completed work attempt."""

    records_read: int = 0
    records_written: int = 0
    records_quarantined: int = 0
    bytes_read: int = 0
    bytes_written: int = 0

    def __post_init__(self) -> None:
        for value in (
            self.records_read,
            self.records_written,
            self.records_quarantined,
            self.bytes_read,
            self.bytes_written,
        ):
            if type(value) is not int:
                raise TypeError("work metric values must be integers")
            if not 0 <= value <= MAX_WORK_METRIC:
                raise ValueError("work metric value is outside the supported range")


class RunNodeAggregateRepository(Protocol):
    """CAS updates derived from already-durable work and attempt rows."""

    def register_work(
        self,
        work: WorkItemRecord,
        *,
        expected_node_row_version: int,
    ) -> RunNodeRecord: ...

    def apply_claim(
        self,
        claim: WorkClaim,
        *,
        expected_node_row_version: int,
    ) -> RunNodeRecord: ...

    def apply_completion(
        self,
        completed: CompletedWork,
        *,
        checkpoint: CheckpointCommit | None,
        expected_node_row_version: int,
        metrics: WorkMetricDelta,
    ) -> RunNodeRecord: ...

    def apply_recovery(
        self,
        completed: CompletedWork,
        *,
        expected_node_row_version: int,
    ) -> RunNodeRecord: ...

    def finalize_empty(
        self,
        run_id: RunId,
        node_id: NodeId,
        *,
        expected_node_row_version: int,
        finalized_at: UtcTimestamp,
    ) -> RunNodeRecord: ...


class RunRevisionRepository(Protocol):
    """Advance a run's subordinate-state revision without lifecycle changes."""

    def advance(self, run_id: RunId, *, expected_row_version: int) -> RunRecord: ...


__all__ = [
    "MAX_WORK_METRIC",
    "RunNodeAggregateRepository",
    "RunRevisionRepository",
    "WorkMetricDelta",
]

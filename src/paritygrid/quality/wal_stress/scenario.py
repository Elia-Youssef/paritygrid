"""Deterministic public-command scenario for WAL stress verification."""

import hashlib
import json
import random
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from paritygrid.application.ports.configuration import ConfigurationDocument
from paritygrid.application.ports.consistency import (
    EventSequence,
    EventSubjectKind,
    PendingExecutionEvent,
    RedactedDocument,
)
from paritygrid.application.ports.writer import EventAppendRequest
from paritygrid.application.writes.execution import (
    BootstrapWork,
    CreateCapturedRun,
    TransitionRun,
)
from paritygrid.domain.execution import RunState
from paritygrid.domain.models import (
    NodeId,
    PipelineId,
    PipelineVersion,
    RunId,
    UtcTimestamp,
    WorkItemId,
)
from paritygrid.domain.pipeline import PartitionKey

from .models import WalStressProfile, WalStressWorkload


@dataclass(frozen=True, slots=True, repr=False)
class WalStressScenario:
    pipeline_id: PipelineId
    run_id: RunId
    node_ids: tuple[NodeId, ...]
    create_run: CreateCapturedRun
    start_run: TransitionRun
    work: tuple[BootstrapWork, ...]
    owners: tuple[int, ...]
    manifest_sha256: str


def _timestamp(offset: int) -> UtcTimestamp:
    base = datetime(2026, 8, 12, 12, 0, 0, tzinfo=UTC)
    return UtcTimestamp(base + timedelta(seconds=offset))


def _document(**values: object) -> ConfigurationDocument:
    return ConfigurationDocument.from_mapping(values)


def _event(
    sequence: int,
    kind: str,
    subject_id: RunId | WorkItemId,
    occurred_at: UtcTimestamp,
) -> EventAppendRequest:
    subject_kind = EventSubjectKind.RUN if type(subject_id) is RunId else EventSubjectKind.WORK_ITEM
    return EventAppendRequest(
        EventSequence(sequence),
        sequence,
        PendingExecutionEvent(
            event_kind=kind,
            occurred_at=occurred_at,
            subject_kind=subject_kind,
            subject_id=subject_id,
            correlation_id="corr-wal-stress",
            payload_schema_version=1,
            payload=RedactedDocument.from_mapping({"kind": kind, "sequence": sequence}),
        ),
    )


def build_scenario(
    profile: WalStressProfile,
    seed: int,
    workload: WalStressWorkload,
) -> WalStressScenario:
    """Precompute exact identities, frontiers, ownership, and manifest digest."""
    token = f"{seed:08x}"
    pipeline_id = PipelineId(f"pip_walstress-{token}")
    run_id = RunId(f"run_walstress-{token}")
    node_ids = tuple(NodeId(f"nod_walstress-{index}") for index in range(4))
    create = CreateCapturedRun(
        run_id=run_id,
        pipeline_id=pipeline_id,
        pipeline_version=PipelineVersion(1),
        runner_kind="threaded",
        runner_configuration=_document(profile=profile.value, producers=workload.producer_count),
        scenario_seed=seed,
        node_ids=node_ids,
        created_at=_timestamp(1),
        event=_event(1, "run_created", run_id, _timestamp(1)),
    )
    start = TransitionRun(
        run_id=run_id,
        expected_run_row_version=1,
        target_state=RunState.RUNNING,
        transitioned_at=_timestamp(2),
        execution_evidence_fingerprint=None,
        execution_evidence_fingerprint_version=None,
        event=_event(2, "run_started", run_id, _timestamp(2)),
    )
    node_versions = dict.fromkeys(node_ids, 1)
    commands: list[BootstrapWork] = []
    for index in range(workload.work_commands):
        node_id = node_ids[index % len(node_ids)]
        work_id = WorkItemId(f"wrk_walstress-{token}-{index:04d}")
        sequence = index + 3
        timestamp = _timestamp(sequence)
        commands.append(
            BootstrapWork(
                run_id=run_id,
                node_id=node_id,
                work_item_id=work_id,
                partition_key=PartitionKey(f"partition-{token}-{index:04d}"),
                input_reference=_document(index=index),
                created_at=timestamp,
                expected_node_row_version=node_versions[node_id],
                expected_run_row_version=index + 2,
                event=_event(sequence, "work_created", work_id, timestamp),
            )
        )
        node_versions[node_id] += 1
    owners = [index % workload.producer_count for index in range(workload.work_commands)]
    random.Random(seed).shuffle(owners)
    manifest = {
        "profile": profile.value,
        "seed": seed,
        "pipeline_id": str(pipeline_id),
        "run_id": str(run_id),
        "nodes": [str(value) for value in node_ids],
        "work": [
            {
                "index": index,
                "owner": owners[index],
                "work_item_id": str(command.work_item_id),
                "node_id": str(command.node_id),
                "partition_key": str(command.partition_key),
                "event_sequence": int(command.event.expected_next_sequence),
                "run_row_version": command.expected_run_row_version,
                "node_row_version": command.expected_node_row_version,
            }
            for index, command in enumerate(commands)
        ],
    }
    encoded = json.dumps(
        manifest, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")
    digest = hashlib.sha256(encoded).hexdigest()
    return WalStressScenario(
        pipeline_id, run_id, node_ids, create, start, tuple(commands), tuple(owners), digest
    )


__all__ = ["WalStressScenario", "build_scenario"]

"""Shared fixtures for the Phase 12 HTTP boundary tests."""

from collections.abc import AsyncIterator, Iterator
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path

import httpx
import pytest
from fastapi import FastAPI

from paritygrid.adapters.persistence.operational import SQLOperationalUnitOfWork
from paritygrid.adapters.persistence.repositories.runs import SqlAlchemyRunRepository
from paritygrid.api.app import create_app
from paritygrid.application.ports.consistency import (
    EventSequence,
    EventSubjectKind,
    PendingExecutionEvent,
    RedactedDocument,
)
from paritygrid.application.ports.writer import EventAppendRequest, WriterReceipt
from paritygrid.application.writes.execution import TransitionRun
from paritygrid.domain.execution import RunState
from paritygrid.domain.models import RunId, StateFingerprint, UtcTimestamp
from paritygrid.runtime.composition import (
    RuntimeContainer,
    RuntimeReadinessProbe,
    RuntimeServices,
    SystemEnvironment,
    compose_runtime,
)
from paritygrid.runtime.config import Settings

DOCUMENT: dict[str, object] = {
    "canonical_format_version": 1,
    "edges": [
        {
            "source_node_id": "nod_source-001",
            "source_port": "records",
            "target_node_id": "nod_export-001",
            "target_port": "records",
        }
    ],
    "layout": [
        {"node_id": "nod_source-001", "x": 0, "y": 0},
        {"node_id": "nod_export-001", "x": 10, "y": 20},
    ],
    "nodes": [
        {
            "configuration": {"encoding": "utf-8"},
            "configuration_version": 1,
            "connector_id": "con_source-001",
            "id": "nod_source-001",
            "kind": "source.csv",
        },
        {
            "configuration": {"compression": "zstd"},
            "configuration_version": 1,
            "connector_id": None,
            "id": "nod_export-001",
            "kind": "export.parquet",
        },
    ],
    "resource_policy": {"max_concurrency": 2},
    "schema_version": 1,
}

PIPELINE_ID = "pip_demo-alpha"
CONNECTOR_ID = "con_source-001"


def anyio_backend() -> str:
    return "asyncio"


@dataclass
class DeterministicClock:
    """Injected clock for lease, expiry, and replay determinism."""

    instant: datetime = field(default_factory=lambda: datetime(2026, 8, 28, 9, 0, 0, tzinfo=UTC))

    def now(self) -> UtcTimestamp:
        return UtcTimestamp(self.instant)

    def advance_seconds(self, seconds: float) -> None:
        self.instant = self.instant + timedelta(seconds=seconds)


@pytest.fixture
def clock() -> DeterministicClock:
    return DeterministicClock()


@pytest.fixture
def settings(tmp_path: Path) -> Settings:
    return Settings(
        data_root=tmp_path / "data",
        idempotency_lease_seconds=60.0,
        request_timeout_seconds=10.0,
    )


@pytest.fixture
def container(settings: Settings) -> Iterator[RuntimeContainer]:
    runtime = compose_runtime(settings)
    try:
        yield runtime
    finally:
        runtime.writer.close(timeout_seconds=5.0)
        runtime.database.close()


def build_app(container: RuntimeContainer, services: RuntimeServices | None = None) -> FastAPI:
    """Build one application bound to an already composed container."""
    application = create_app(
        readiness=RuntimeReadinessProbe(container_provider=lambda: container),
        limits=container.limits,
    )
    application.state.services = services if services is not None else container.services
    return application


@pytest.fixture
def app(container: RuntimeContainer) -> FastAPI:
    return build_app(container)


@pytest.fixture
async def client(app: FastAPI) -> AsyncIterator[httpx.AsyncClient]:
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as c:
        yield c


def clock_driven_services(
    container: RuntimeContainer, clock: DeterministicClock, *, lease_seconds: float
) -> RuntimeServices:
    """Rebuild the service surface over one injected clock and lease window."""
    from paritygrid.adapters.persistence.repair_workflow import SQLiteRepairWorkflowReader
    from paritygrid.application.services.artifacts import ArtifactService
    from paritygrid.application.services.connectors import (
        ConnectorService,
        ConnectorTestService,
    )
    from paritygrid.application.services.idempotency import (
        IdempotencyLeasePolicy,
        IdempotentCommandService,
    )
    from paritygrid.application.services.pipelines import PipelineService
    from paritygrid.application.services.reconciliation import ReconciliationService
    from paritygrid.application.services.repair import RepairApplyService, RepairService
    from paritygrid.application.services.runs import RunLifecycleService, RunService

    unit_of_work = SQLOperationalUnitOfWork(
        container.database,
        artifact_root=container.settings.artifact_root_path,
        artifact_chunk_bytes=container.settings.artifact_chunk_bytes,
    )
    repair_reader = SQLiteRepairWorkflowReader(container.database)
    now = clock.now
    return RuntimeServices(
        pipelines=PipelineService(unit_of_work=unit_of_work, now=now),
        connectors=ConnectorService(unit_of_work=unit_of_work, now=now),
        connector_tests=ConnectorTestService(
            unit_of_work=unit_of_work, environment=SystemEnvironment(), now=now
        ),
        runs=RunService(unit_of_work=unit_of_work, writer=container.writer, now=now),
        run_lifecycle=RunLifecycleService(
            unit_of_work=unit_of_work, writer=container.writer, now=now
        ),
        artifacts=ArtifactService(unit_of_work=unit_of_work, now=now),
        idempotency=IdempotentCommandService(
            unit_of_work=unit_of_work,
            policy=IdempotencyLeasePolicy(lease_seconds=lease_seconds),
            now=now,
        ),
        capabilities=container.services.capabilities,
        reconciliation=ReconciliationService(unit_of_work=unit_of_work),
        repair=RepairService(
            writer=container.writer,
            reader=repair_reader,
            unit_of_work=unit_of_work,
            now=now,
        ),
        repair_application=RepairApplyService(
            writer=container.writer,
            reader=repair_reader,
            unit_of_work=unit_of_work,
            now=now,
        ),
        event_stream=container.services.event_stream,
        telemetry=container.services.telemetry,
        clock=now,
    )


async def seed_scenario(
    client: httpx.AsyncClient, *, run_id: str = "run_scenario-01"
) -> httpx.Response:
    """Create the connector, pipeline, published version, and one run."""
    await client.post(
        "/api/v1/connectors",
        json={
            "connector_id": CONNECTOR_ID,
            "kind": "csv_source",
            "display_name": "CSV source",
            "configuration": {"path": "fixture.csv"},
            "capabilities": {"read": True, "schema_discovery": True},
            "secret_references": [],
        },
    )
    await client.post(
        "/api/v1/pipelines",
        json={"pipeline_id": PIPELINE_ID, "display_name": "Demo pipeline"},
    )
    await client.post(
        f"/api/v1/pipelines/{PIPELINE_ID}/versions",
        json={"document": DOCUMENT},
    )
    return await client.post(
        "/api/v1/runs",
        json={
            "run_id": run_id,
            "pipeline_id": PIPELINE_ID,
            "pipeline_version": 1,
            "runner_kind": "sequential",
            "scenario_seed": 42,
        },
    )


def transition_run(
    container: RuntimeContainer,
    run_id: str,
    target: RunState,
    *,
    execution_evidence_fingerprint: str | None = None,
    execution_evidence_fingerprint_version: int | None = None,
) -> WriterReceipt:
    """Advance one run through the durable writer exactly as the engine does."""
    with container.database.transaction() as session:
        repository = SqlAlchemyRunRepository(session)
        record = repository.get(RunId(run_id))
        assert record is not None, "seed run must exist before transitioning"
        counter = repository.get_event_counter(record.run_id)
        assert counter is not None
    command = TransitionRun(
        run_id=record.run_id,
        expected_run_row_version=record.row_version,
        target_state=target,
        transitioned_at=record.created_at,
        execution_evidence_fingerprint=(
            None
            if execution_evidence_fingerprint is None
            else StateFingerprint(execution_evidence_fingerprint)
        ),
        execution_evidence_fingerprint_version=execution_evidence_fingerprint_version,
        event=EventAppendRequest(
            expected_next_sequence=EventSequence(counter.next_sequence_number),
            expected_counter_row_version=counter.row_version,
            event=PendingExecutionEvent(
                event_kind=_ENGINE_EVENT_KINDS[target],
                occurred_at=record.created_at,
                subject_kind=EventSubjectKind.RUN,
                subject_id=record.run_id,
                correlation_id="engine-seed",
                payload_schema_version=1,
                payload=RedactedDocument.from_mapping(
                    {"from_state": record.state.value, "to_state": target.value}
                ),
            ),
        ),
    )
    ticket = container.writer.submit(command, timeout_seconds=5.0)
    receipt = ticket.result(timeout_seconds=5.0)
    return receipt


def seed_reconciled_run(
    container: RuntimeContainer,
    *,
    run_id: str,
    analysis: object,
    correlation_id: str = "seed-reconciliation",
) -> None:
    """Drive one run to a terminal state and persist its reconciliation."""
    from paritygrid.adapters.persistence.repair_workflow import SQLiteRepairWorkflowReader
    from paritygrid.application.reconciliation.analysis import ReconciliationAnalysis
    from paritygrid.application.repair import ReconciliationResultService
    from paritygrid.domain.models import StateFingerprint

    assert isinstance(analysis, ReconciliationAnalysis)
    with container.database.transaction() as session:
        repository = SqlAlchemyRunRepository(session)
        record = repository.get(RunId(run_id))
        assert record is not None, "seed run must exist before reconciliation"
        moment = record.created_at
        repository.transition(
            record.run_id,
            expected_row_version=record.row_version,
            target_state=RunState.RUNNING,
            transitioned_at=moment,
        )
        running = repository.get(record.run_id)
        assert running is not None
        repository.transition(
            running.run_id,
            expected_row_version=running.row_version,
            target_state=RunState.SUCCEEDED,
            transitioned_at=moment,
            execution_evidence_fingerprint=StateFingerprint("a" * 64),
            execution_evidence_fingerprint_version=2,
        )
    service = ReconciliationResultService(
        container.writer,
        SQLiteRepairWorkflowReader(container.database),
        now=container.services.clock,
    )
    service.persist(
        run_id=RunId(run_id),
        analysis=analysis,
        actor="seed-operator",
        correlation_id=correlation_id,
    )


_ENGINE_EVENT_KINDS = {
    RunState.RUNNING: "run_started",
    RunState.PAUSING: "run_pausing",
    RunState.PAUSED: "run_paused",
    RunState.RESUMING: "run_resuming",
    RunState.CANCELLING: "run_cancelling",
    RunState.CANCELLED: "run_cancelled",
    RunState.SUCCEEDED: "run_succeeded",
    RunState.PARTIALLY_SUCCEEDED: "run_partially_succeeded",
    RunState.FAILED: "run_failed",
}

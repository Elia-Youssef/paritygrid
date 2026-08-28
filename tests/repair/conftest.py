"""Shared fixtures for the Phase 11 repair and verification tests."""

from collections.abc import Iterator
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pytest
from sqlalchemy import insert

from paritygrid.adapters.connectors.warehouse_target import (
    WarehouseTargetConfig,
    WarehouseTargetConnector,
)
from paritygrid.adapters.persistence import SQLiteDatabase, SQLiteDatabaseConfig
from paritygrid.adapters.persistence.migration import upgrade_to_head
from paritygrid.adapters.persistence.repair_workflow import SQLiteRepairWorkflowReader
from paritygrid.adapters.persistence.repositories import (
    SqlAlchemyPipelineRepository,
    SqlAlchemyRunRepository,
)
from paritygrid.adapters.persistence.schema import reconciliation_summaries
from paritygrid.adapters.persistence.sqlite import create_session_factory
from paritygrid.adapters.persistence.writer.core import SQLiteTransactionalWriter, WriterSettings
from paritygrid.application.ports.configuration import ConfigurationDocument
from paritygrid.application.ports.reconciliation_persistence import PersistedConflict
from paritygrid.application.reconciliation.analysis import (
    ReconciliationAnalysis,
    ReconciliationAnalysisRequest,
    analyze_reconciliation,
)
from paritygrid.application.repair.reconciliation_service import build_persisted_conflicts
from paritygrid.demo.simulators.warehouse import SimulatedWarehouse
from paritygrid.domain.execution import RunState
from paritygrid.domain.models import (
    ConnectorId,
    CurrencyCode,
    InventoryRecord,
    Money,
    NodeId,
    PipelineId,
    PipelineVersion,
    RepairPlanId,
    RunId,
    StateFingerprint,
    UtcTimestamp,
)
from paritygrid.domain.reconciliation import SourceObservation
from paritygrid.domain.repair import RepairPlan

SOURCE_CONNECTOR = ConnectorId("con_repair-source")
TARGET_CONNECTOR = ConnectorId("con_repair-target")
PIPELINE_ID = PipelineId("pip_repair-phase11")
RUN_ID = RunId("run_phase11-showcase")
PLAN_COUNTER_START = 900


@pytest.fixture
def anyio_backend() -> str:
    return "asyncio"


@pytest.fixture
def database(tmp_path: Path) -> Iterator[SQLiteDatabase]:
    database = SQLiteDatabase.open(SQLiteDatabaseConfig(tmp_path / "repair phase %25.db"))
    with database.engine.connect() as connection:
        upgrade_to_head(connection)
    try:
        yield database
    finally:
        database.close()


@pytest.fixture
def writer(database: SQLiteDatabase) -> Iterator[SQLiteTransactionalWriter]:
    writer = SQLiteTransactionalWriter(create_session_factory(database.engine), WriterSettings())
    writer.start()
    try:
        yield writer
    finally:
        writer.close(timeout_seconds=5.0)


@pytest.fixture
def reader(database: SQLiteDatabase) -> SQLiteRepairWorkflowReader:
    return SQLiteRepairWorkflowReader(database)


@dataclass(slots=True)
class DeterministicClock:
    """A strictly increasing injected clock; domain time is explicit."""

    _next: datetime

    @classmethod
    def create(cls) -> DeterministicClock:
        return cls(datetime(2026, 8, 27, 9, 0, 0, tzinfo=UTC))

    def now(self) -> UtcTimestamp:
        current = self._next
        self._next = self._next + timedelta(seconds=1)
        return UtcTimestamp(current)


@pytest.fixture
def clock() -> DeterministicClock:
    return DeterministicClock.create()


def wire_payload(
    sku: str,
    *,
    name: str = "Cafe valve",
    quantity: int = 5,
    amount: str = "12.34",
    currency: str = "USD",
    updated_at: str = "2024-03-04T05:06:07.000000Z",
    attributes: dict[str, str] | None = None,
) -> dict[str, object]:
    payload: dict[str, object] = {
        "name": name,
        "quantity": quantity,
        "sku": sku,
        "source_record_key": f"src-{sku.lower()}",
        "unit_price": {"amount": amount, "currency": currency},
        "updated_at": updated_at,
    }
    if attributes is not None:
        payload["attributes"] = attributes
    return payload


def observation(
    position: int, payload: dict[str, object], *, target_side: bool = False
) -> SourceObservation:
    return SourceObservation(
        position=position,
        connector_id=TARGET_CONNECTOR if target_side else SOURCE_CONNECTOR,
        payload=payload,
    )


def analysis(
    source: list[dict[str, object]],
    target: list[dict[str, object]],
    *,
    source_identity: str = "1" * 64,
    target_identity: str = "2" * 64,
) -> ReconciliationAnalysis:
    return analyze_reconciliation(
        _analysis_request(
            tuple(observation(index, payload) for index, payload in enumerate(source)),
            tuple(
                observation(10_000 + index, payload, target_side=True)
                for index, payload in enumerate(target)
            ),
            source_identity,
            target_identity,
        )
    )


def _analysis_request(
    source: tuple[SourceObservation, ...],
    target: tuple[SourceObservation, ...],
    source_identity: str,
    target_identity: str,
) -> ReconciliationAnalysisRequest:
    return ReconciliationAnalysisRequest(
        source_observations=source,
        target_observations=target,
        source_input_identity=source_identity,
        target_input_identity=target_identity,
    )


def seed_terminal_run(
    database: SQLiteDatabase,
    run_id: RunId = RUN_ID,
    *,
    terminal: bool = True,
    seed_pipeline: bool = True,
) -> None:
    """Seed one finalized run with a distinct execution-evidence fingerprint."""
    moment = UtcTimestamp(datetime(2026, 8, 27, 8, 0, 0, tzinfo=UTC))
    with database.transaction() as session:
        if seed_pipeline:
            pipelines = SqlAlchemyPipelineRepository(session)
            pipelines.create(
                pipeline_id=PIPELINE_ID,
                display_name="Phase 11 repair pipeline",
                description=None,
                created_at=moment,
            )
            pipelines.publish_version(
                pipeline_id=PIPELINE_ID,
                expected_latest_version=None,
                specification=ConfigurationDocument.from_mapping({"nodes": []}),
                planner_format_version=1,
                published_at=moment,
            )
        runs = SqlAlchemyRunRepository(session)
        runs.create(
            run_id=run_id,
            pipeline_id=PIPELINE_ID,
            pipeline_version=PipelineVersion(1),
            runner_kind="sequential",
            runner_configuration=ConfigurationDocument.from_mapping({}),
            scenario_seed=None,
            node_ids=(NodeId("nod_repair-apply"),),
            created_at=moment,
        )
        runs.transition(
            run_id,
            expected_row_version=1,
            target_state=RunState.RUNNING,
            transitioned_at=moment,
        )
        if terminal:
            runs.transition(
                run_id,
                expected_row_version=2,
                target_state=RunState.SUCCEEDED,
                transitioned_at=moment,
                execution_evidence_fingerprint=StateFingerprint("a" * 64),
                execution_evidence_fingerprint_version=2,
            )


def seed_summary_row(
    database: SQLiteDatabase,
    run_id: RunId,
    fingerprint: StateFingerprint,
) -> None:
    """Seed a raw summary row without conflicts for fence-focused tests."""
    moment = UtcTimestamp(datetime(2026, 8, 27, 8, 30, 0, tzinfo=UTC))
    with database.transaction() as session:
        session.execute(
            insert(reconciliation_summaries).values(
                run_id=run_id.value,
                match_count=0,
                missing_from_target_count=1,
                missing_from_source_count=0,
                field_mismatch_count=0,
                duplicate_source_count=0,
                duplicate_target_count=0,
                duplicate_both_count=0,
                total_count=1,
                source_fingerprint="1" * 64,
                target_fingerprint="2" * 64,
                reconciliation_fingerprint=fingerprint.value,
                analytical_query_version=1,
                created_at=str(moment),
            )
        )


async def open_target(
    warehouse: SimulatedWarehouse,
) -> WarehouseTargetConnector:
    connector = WarehouseTargetConnector(WarehouseTargetConfig(warehouse.base_url))
    await connector.open_async()
    return connector


def record_for(sku: str, *, name: str = "Cafe valve", quantity: int = 5) -> InventoryRecord:
    return InventoryRecord.create(
        sku=sku,
        name=name,
        quantity=quantity,
        unit_price=Money(Decimal("12.34"), CurrencyCode("USD"), 2),
        updated_at=UtcTimestamp(datetime(2024, 3, 4, 5, 6, 7, tzinfo=UTC)),
        connector_id=SOURCE_CONNECTOR,
        source_record_key=f"src-{sku.lower()}",
        attributes={"finish": "Brass"},
    )


def conflicts_from(
    run_id: RunId, analysis: ReconciliationAnalysis, created_at: UtcTimestamp
) -> tuple[PersistedConflict, ...]:
    return build_persisted_conflicts(run_id, analysis, created_at)


def plan_id_for(run_id: RunId, analysis: ReconciliationAnalysis) -> RepairPlanId:
    from paritygrid.application.repair.identities import derive_plan_id

    return derive_plan_id(run_id, analysis.summary.fingerprint)


def count_actions(plan: RepairPlan | None) -> int:
    return 0 if plan is None else len(plan.actions)


__all__ = [
    "PIPELINE_ID",
    "PLAN_COUNTER_START",
    "RUN_ID",
    "DeterministicClock",
    "analysis",
    "clock",
    "conflicts_from",
    "count_actions",
    "database",
    "observation",
    "open_target",
    "plan_id_for",
    "reader",
    "record_for",
    "seed_summary_row",
    "seed_terminal_run",
    "wire_payload",
    "writer",
]

"""Repair route contracts: fencing, idempotency, and application."""

from typing import Any, cast

import httpx
import pytest
from fastapi import Request

from paritygrid.adapters.connectors.warehouse_target import (
    WarehouseTargetConfig,
    WarehouseTargetConnector,
)
from paritygrid.adapters.persistence.repositories.pipelines import (
    SqlAlchemyPipelineRepository,
)
from paritygrid.adapters.persistence.repositories.runs import SqlAlchemyRunRepository
from paritygrid.api.errors.mapping import translate_error
from paritygrid.api.routers import repairs as repair_router
from paritygrid.application.ports.configuration import ConfigurationDocument
from paritygrid.application.ports.connectors import (
    ConnectorCallContext,
    TargetWriteRequest,
)
from paritygrid.application.reconciliation.analysis import (
    ReconciliationAnalysis,
    analyze_reconciliation,
)
from paritygrid.application.repair.applier import RepairApplicationDisposition
from paritygrid.application.repair.errors import (
    TargetApplicationInterruptedError,
    TargetApplicationUnresolvedError,
)
from paritygrid.application.repair.payloads import render_target_payload
from paritygrid.demo.simulators.warehouse import SimulatedWarehouse
from paritygrid.domain.models import NodeId, PipelineId, PipelineVersion, RunId
from paritygrid.domain.reconciliation import SourceObservation
from paritygrid.runtime.composition import RuntimeContainer
from tests.api.conftest import seed_scenario
from tests.repair.conftest import observation, wire_payload

pytestmark = pytest.mark.anyio

PIPELINE_ID = "pip_repair-http"
SOURCE_IDENTITY = "3" * 64
TARGET_IDENTITY = "4" * 64


def test_unresolved_and_interrupted_reports_are_retry_safe_503s(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Nonterminal applier reports must not become cached 200 command outcomes."""

    class Application:
        async def apply(self, *, plan_id: str, context_id: str) -> object:
            del plan_id, context_id
            return view

    def fake_services(_request: Request) -> Any:
        return type("S", (), {"repair_application": Application()})()

    def fake_correlation(_request: Request) -> str:
        return "test-correlation"

    monkeypatch.setattr(repair_router, "get_services", fake_services)
    monkeypatch.setattr(repair_router, "correlation_of", fake_correlation)

    for disposition, error_type, code in (
        (
            RepairApplicationDisposition.UNRESOLVED,
            TargetApplicationUnresolvedError,
            "repair_outcome_unresolved",
        ),
        (
            RepairApplicationDisposition.INTERRUPTED,
            TargetApplicationInterruptedError,
            "repair_application_interrupted",
        ),
    ):
        view = type("View", (), {"report": type("Report", (), {"disposition": disposition})()})()
        with pytest.raises(error_type) as raised:
            repair_router._apply_outcome(  # pyright: ignore[reportPrivateUsage]
                cast(Request, object()), "rpl_retry-001"
            )
        problem = translate_error(raised.value)
        assert problem.status == 503
        assert problem.code == code


def _sides() -> tuple[list[SourceObservation], list[SourceObservation]]:
    source = [
        observation(0, wire_payload("SKU-AAA")),
        observation(1, wire_payload("SKU-BBB", quantity=7)),
        observation(2, wire_payload("SKU-CCC")),
    ]
    target = [
        observation(0, wire_payload("SKU-BBB", quantity=5), target_side=True),
    ]
    return source, target


def _analysis(
    source: list[SourceObservation], target: list[SourceObservation]
) -> ReconciliationAnalysis:
    from paritygrid.application.reconciliation.analysis import (
        ReconciliationAnalysisRequest,
    )

    return analyze_reconciliation(
        ReconciliationAnalysisRequest(
            source_observations=tuple(source),
            target_observations=tuple(target),
            source_input_identity=SOURCE_IDENTITY,
            target_input_identity=TARGET_IDENTITY,
        )
    )


def _observations_body(
    source: list[SourceObservation], target: list[SourceObservation]
) -> dict[str, object]:
    def side(rows: list[SourceObservation], connector: str, identity: str) -> dict[str, object]:
        return {
            "connector_id": connector,
            "input_identity": identity,
            "observations": [
                (
                    {"position": index, "payload": dict(row.payload)}
                    if row.payload is not None
                    else {"position": index, "malformed_reason": "malformed"}
                )
                for index, row in enumerate(rows)
            ],
        }

    return {
        "schema_version": 1,
        "source": side(source, "con_repair-source", SOURCE_IDENTITY),
        "target": side(target, "con_repair-target", TARGET_IDENTITY),
    }


async def _seed_reconciled_run_with_warehouse(
    container: RuntimeContainer, *, run_id: str, base_url: str, analysis: ReconciliationAnalysis
) -> None:
    from tests.api.conftest import seed_reconciled_run

    envelope: dict[str, object] = {
        "connector_bindings": [
            {
                "connector_id": "con_warehouse-01",
                "kind": "warehouse_target",
                "revision": 1,
                "configuration": {"base_url": base_url},
                "capabilities": {"read": True, "write": True},
                "schema_discovery": None,
                "secret_references": [],
            }
        ],
        "pipeline": {
            "canonical_format_version": 1,
            "edges": [],
            "layout": [{"node_id": "nod_apply-001", "x": 0, "y": 0}],
            "nodes": [
                {
                    "configuration": dict[str, object](),
                    "configuration_version": 1,
                    "connector_id": "con_warehouse-01",
                    "id": "nod_apply-001",
                    "kind": "repair.apply",
                }
            ],
            "resource_policy": {
                "max_concurrency": 1,
                "max_in_flight": 16,
                "memory_limit_bytes": 536870912,
                "operation_timeout_seconds": 60,
                "queue_capacity": 256,
            },
            "schema_version": 1,
        },
        "published_specification_version": 1,
    }
    with container.database.transaction() as session:
        pipelines = SqlAlchemyPipelineRepository(session)
        pipelines.create(
            pipeline_id=PipelineId.parse(PIPELINE_ID),
            display_name="HTTP repair pipeline",
            description=None,
            created_at=container.services.clock(),
        )
        pipelines.publish_version(
            pipeline_id=PipelineId.parse(PIPELINE_ID),
            expected_latest_version=None,
            specification=ConfigurationDocument.from_mapping(envelope),
            planner_format_version=1,
            published_at=container.services.clock(),
        )
        runs = SqlAlchemyRunRepository(session)
        runs.create(
            run_id=RunId.parse(run_id),
            pipeline_id=PipelineId.parse(PIPELINE_ID),
            pipeline_version=PipelineVersion(1),
            runner_kind="sequential",
            runner_configuration=ConfigurationDocument.from_mapping({}),
            scenario_seed=None,
            node_ids=(NodeId("nod_apply-001"),),
            created_at=container.services.clock(),
        )
    seed_reconciled_run(container, run_id=run_id, analysis=analysis)


async def _load_warehouse(base_url: str, analysis: ReconciliationAnalysis) -> None:
    connector = WarehouseTargetConnector(WarehouseTargetConfig(base_url))
    await connector.open_async()
    try:
        for key in analysis.classification.keys:
            for record in key.outcome.target_records:
                await connector.write_record_async(
                    TargetWriteRequest(
                        sku=record.sku,
                        payload=render_target_payload(record),
                        idempotency_key=f"http-seed-{record.sku}",
                    ),
                    ConnectorCallContext(correlation_id="http-seed"),
                )
    finally:
        await connector.aclose()


async def test_full_repair_workflow_over_http(
    client: httpx.AsyncClient, container: RuntimeContainer
) -> None:
    run_id = "run_repair-01"
    source, target = _sides()
    analysis = _analysis(source, target)
    warehouse = SimulatedWarehouse()
    await warehouse.start()
    try:
        await _seed_reconciled_run_with_warehouse(
            container, run_id=run_id, base_url=warehouse.base_url, analysis=analysis
        )
        await _load_warehouse(warehouse.base_url, analysis)

        created = await client.post(
            f"/api/v1/runs/{run_id}/repair-plans",
            json=_observations_body(source, target),
        )
        assert created.status_code == 201, created.text
        plan = created.json()
        assert plan["status"] == "proposed"
        assert plan["run_id"] == run_id
        assert plan["reconciliation_fingerprint"] != ""
        assert plan["content_fingerprint"] != ""
        assert len(plan["actions"]) == 3
        kinds = {action["canonical_key"]: action["kind"] for action in plan["actions"]}
        assert kinds["SKU-AAA"] == "create_target"
        assert kinds["SKU-BBB"] == "update_target"
        assert kinds["SKU-CCC"] == "create_target"
        plan_id = plan["plan_id"]

        fetched = await client.get(f"/api/v1/repair-plans/{plan_id}")
        assert fetched.status_code == 200
        assert fetched.json()["plan_id"] == plan_id
        assert fetched.json()["approval"] is None

        approved = await client.post(
            f"/api/v1/repair-plans/{plan_id}/approve",
            json={
                "schema_version": 1,
                "approved_by": "operator-1",
                "approved_content_fingerprint": plan["content_fingerprint"],
                "approved_reconciliation_fingerprint": plan["reconciliation_fingerprint"],
            },
        )
        assert approved.status_code == 200, approved.text
        assert approved.json()["status"] == "approved"
        assert approved.json()["approval"]["approved_by"] == "operator-1"

        writes_before = warehouse.request_count()
        body_rejected = await client.post(
            f"/api/v1/repair-plans/{plan_id}/apply",
            json={"unexpected": True},
            headers={"Idempotency-Key": "apply-bodyless-contract"},
        )
        assert body_rejected.status_code == 400
        assert body_rejected.json()["code"] == "request_body_not_allowed"
        assert warehouse.request_count() == writes_before

        # The rejected request reserved no idempotency outcome: the same key
        # remains safe for the actual bodyless application command.
        applied = await client.post(
            f"/api/v1/repair-plans/{plan_id}/apply",
            headers={"Idempotency-Key": "apply-bodyless-contract"},
        )
        assert applied.status_code == 200, applied.text
        body = applied.json()
        assert body["disposition"] == "completed"
        assert body["status"] == "applied"
        assert len(body["effects"]) == 3
        assert warehouse.request_count() > writes_before

        replayed = await client.post(f"/api/v1/repair-plans/{plan_id}/apply")
        assert replayed.status_code == 200
        assert replayed.json()["disposition"] == "already_applied"
    finally:
        await warehouse.aclose()


async def test_apply_replay_does_not_duplicate_target_effects(
    client: httpx.AsyncClient, container: RuntimeContainer
) -> None:
    run_id = "run_repair-02"
    source, target = _sides()
    analysis = _analysis(source, target)
    warehouse = SimulatedWarehouse()
    await warehouse.start()
    try:
        await _seed_reconciled_run_with_warehouse(
            container, run_id=run_id, base_url=warehouse.base_url, analysis=analysis
        )
        await _load_warehouse(warehouse.base_url, analysis)
        body = _observations_body(source, target)
        plan = (await client.post(f"/api/v1/runs/{run_id}/repair-plans", json=body)).json()
        plan_id = plan["plan_id"]
        await client.post(
            f"/api/v1/repair-plans/{plan_id}/approve",
            json={
                "schema_version": 1,
                "approved_by": "operator-2",
                "approved_content_fingerprint": plan["content_fingerprint"],
                "approved_reconciliation_fingerprint": plan["reconciliation_fingerprint"],
            },
        )
        first = await client.post(f"/api/v1/repair-plans/{plan_id}/apply")
        assert first.json()["disposition"] == "completed"
        writes_after_first = warehouse.request_count()
        second = await client.post(f"/api/v1/repair-plans/{plan_id}/apply")
        assert second.json()["disposition"] == "already_applied"
        assert warehouse.request_count() == writes_after_first
    finally:
        await warehouse.aclose()


async def test_plan_creation_requires_matching_reconciliation(
    client: httpx.AsyncClient, container: RuntimeContainer
) -> None:
    run_id = "run_repair-03"
    source, target = _sides()
    analysis = _analysis(source, target)
    warehouse = SimulatedWarehouse()
    await warehouse.start()
    try:
        await _seed_reconciled_run_with_warehouse(
            container, run_id=run_id, base_url=warehouse.base_url, analysis=analysis
        )
        divergent = [observation(0, wire_payload("SKU-ZZZ"))]
        stale = await client.post(
            f"/api/v1/runs/{run_id}/repair-plans",
            json=_observations_body(divergent, []),
        )
        assert stale.status_code == 409
        assert stale.json()["code"] == "reconciliation_stale"
    finally:
        await warehouse.aclose()


async def test_plan_creation_without_reconciliation_conflicts(client: httpx.AsyncClient) -> None:
    await seed_scenario(client, run_id="run_bare-002")
    source, target = _sides()
    response = await client.post(
        "/api/v1/runs/run_bare-002/repair-plans",
        json=_observations_body(source, target),
    )
    assert response.status_code == 409
    assert response.json()["code"] == "reconciliation_missing"


async def test_approve_rejects_wrong_fingerprints(
    client: httpx.AsyncClient, container: RuntimeContainer
) -> None:
    run_id = "run_repair-04"
    source, target = _sides()
    analysis = _analysis(source, target)
    warehouse = SimulatedWarehouse()
    await warehouse.start()
    try:
        await _seed_reconciled_run_with_warehouse(
            container, run_id=run_id, base_url=warehouse.base_url, analysis=analysis
        )
        plan = (
            await client.post(
                f"/api/v1/runs/{run_id}/repair-plans",
                json=_observations_body(source, target),
            )
        ).json()
        wrong = await client.post(
            f"/api/v1/repair-plans/{plan['plan_id']}/approve",
            json={
                "schema_version": 1,
                "approved_by": "operator-3",
                "approved_content_fingerprint": "0" * 64,
                "approved_reconciliation_fingerprint": plan["reconciliation_fingerprint"],
            },
        )
        assert wrong.status_code == 409

        unapproved_apply = await client.post(f"/api/v1/repair-plans/{plan['plan_id']}/apply")
        assert unapproved_apply.status_code == 409
        assert unapproved_apply.json()["code"] in {"repair_plan_state", "repair_state_conflict"}
    finally:
        await warehouse.aclose()


async def test_plan_creation_replays_under_idempotency_key(
    client: httpx.AsyncClient, container: RuntimeContainer
) -> None:
    run_id = "run_repair-05"
    source, target = _sides()
    analysis = _analysis(source, target)
    warehouse = SimulatedWarehouse()
    await warehouse.start()
    try:
        await _seed_reconciled_run_with_warehouse(
            container, run_id=run_id, base_url=warehouse.base_url, analysis=analysis
        )
        body = _observations_body(source, target)
        first = await client.post(
            f"/api/v1/runs/{run_id}/repair-plans",
            json=body,
            headers={"Idempotency-Key": "plan-once"},
        )
        assert first.status_code == 201
        replay = await client.post(
            f"/api/v1/runs/{run_id}/repair-plans",
            json=body,
            headers={"Idempotency-Key": "plan-once"},
        )
        assert replay.status_code == 201
        assert replay.json()["plan_id"] == first.json()["plan_id"]
        mismatch = await client.post(
            f"/api/v1/runs/{run_id}/repair-plans",
            json=_observations_body(target, source),
            headers={"Idempotency-Key": "plan-once"},
        )
        assert mismatch.status_code == 409
        assert mismatch.json()["code"] == "idempotency_key_reused"
    finally:
        await warehouse.aclose()


async def test_unknown_plan_is_not_found(client: httpx.AsyncClient) -> None:
    response = await client.get("/api/v1/repair-plans/rpl_ghost-001")
    assert response.status_code == 404
    assert response.headers["content-type"].startswith("application/problem+json")

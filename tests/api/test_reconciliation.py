"""Reconciliation route contracts: pagination, fingerprints, coherence."""

import httpx
import pytest

from paritygrid.application.reconciliation.analysis import analyze_reconciliation
from paritygrid.domain.reconciliation import SourceObservation
from paritygrid.runtime.composition import RuntimeContainer
from tests.api.conftest import seed_reconciled_run, seed_scenario
from tests.repair.conftest import observation, wire_payload

pytestmark = pytest.mark.anyio

SOURCE_IDENTITY = "1" * 64
TARGET_IDENTITY = "2" * 64


def _analysis() -> object:
    source: list[SourceObservation] = [
        observation(0, wire_payload("SKU-AAA")),
        observation(1, wire_payload("SKU-BBB", quantity=7)),
        observation(2, wire_payload("SKU-CCC")),
        observation(3, wire_payload("SKU-DDD", quantity=2)),
    ]
    target: list[SourceObservation] = [
        observation(0, wire_payload("SKU-AAA"), target_side=True),
        observation(1, wire_payload("SKU-BBB", quantity=5), target_side=True),
        observation(2, wire_payload("SKU-EEE"), target_side=True),
    ]
    return analyze_reconciliation(
        _request(source, target)  # type: ignore[arg-type]
    )


def _request(source: list[SourceObservation], target: list[SourceObservation]) -> object:
    from paritygrid.application.reconciliation.analysis import (
        ReconciliationAnalysisRequest,
    )

    return ReconciliationAnalysisRequest(
        source_observations=tuple(source),
        target_observations=tuple(target),
        source_input_identity=SOURCE_IDENTITY,
        target_input_identity=TARGET_IDENTITY,
    )


async def _seed(
    client: httpx.AsyncClient, container: RuntimeContainer, *, run_id: str = "run_recon-01"
) -> str:
    await seed_scenario(client, run_id=run_id)
    seed_reconciled_run(container, run_id=run_id, analysis=_analysis())
    return run_id


async def test_reconciliation_summary_returns_coherent_fingerprints(
    client: httpx.AsyncClient, container: RuntimeContainer
) -> None:
    run_id = await _seed(client, container)
    response = await client.get(f"/api/v1/runs/{run_id}/reconciliation")
    assert response.status_code == 200
    body = response.json()
    assert body["run_id"] == run_id
    assert body["state"] == "succeeded"
    assert body["run_version"] >= 3
    assert body["reconciliation_fingerprint"] != body["source_input_identity"]
    assert body["source_input_identity"] == SOURCE_IDENTITY
    assert body["target_input_identity"] == TARGET_IDENTITY
    assert body["counts"]["match"] == 1
    assert body["counts"]["missing_from_target"] == 2
    assert body["counts"]["missing_from_source"] == 1
    assert body["counts"]["field_mismatch"] == 1
    assert body["total_count"] == 5
    assert body["analytical_query_version"] == 1
    assert response.headers["content-type"].startswith("application/json")
    assert response.headers["cache-control"] == "no-store"


async def test_reconciliation_missing_run_and_missing_result(client: httpx.AsyncClient) -> None:
    missing_run = await client.get("/api/v1/runs/run_ghost-999/reconciliation")
    assert missing_run.status_code == 404
    assert missing_run.headers["content-type"].startswith("application/problem+json")
    await seed_scenario(client, run_id="run_bare-001")
    missing_result = await client.get("/api/v1/runs/run_bare-001/reconciliation")
    assert missing_result.status_code == 404
    assert missing_result.json()["code"] == "reconciliation_not_found"


async def test_conflict_pages_walk_without_duplicates(
    client: httpx.AsyncClient, container: RuntimeContainer
) -> None:
    run_id = await _seed(client, container, run_id="run_confl-01")
    first = await client.get(f"/api/v1/runs/{run_id}/conflicts?limit=1")
    assert first.status_code == 200
    body = first.json()
    assert len(body["items"]) == 1
    assert body["limit"] == 1
    assert body["next_cursor"] is not None
    assert body["reconciliation_fingerprint"] != ""
    assert body["run_id"] == run_id
    assert body["run_version"] >= 3
    keys = [item["canonical_key"] for item in body["items"]]

    second = await client.get(
        f"/api/v1/runs/{run_id}/conflicts?limit=100&cursor={body['next_cursor']}"
    )
    assert second.status_code == 200
    keys.extend(item["canonical_key"] for item in second.json()["items"])
    assert second.json()["next_cursor"] is None
    assert sorted(keys) == ["SKU-BBB", "SKU-CCC", "SKU-DDD", "SKU-EEE"]


async def test_conflict_item_carries_evidence_projection(
    client: httpx.AsyncClient, container: RuntimeContainer
) -> None:
    run_id = await _seed(client, container, run_id="run_evide-01")
    response = await client.get(f"/api/v1/runs/{run_id}/conflicts")
    item = next(entry for entry in response.json()["items"] if entry["canonical_key"] == "SKU-BBB")
    assert item["classification"] == "field_mismatch"
    assert item["suggested_resolution"] == "update_target"
    assert item["source_references"][0]["record_key"].startswith("src-")
    assert any(difference["field"] == "quantity" for difference in item["differences"])
    assert item["conflict_id"].startswith("cnf_")


async def test_conflict_limit_and_cursor_bounds(
    client: httpx.AsyncClient, container: RuntimeContainer
) -> None:
    run_id = await _seed(client, container, run_id="run_bound-01")
    too_large = await client.get(f"/api/v1/runs/{run_id}/conflicts?limit=101")
    assert too_large.status_code == 422
    zero = await client.get(f"/api/v1/runs/{run_id}/conflicts?limit=0")
    assert zero.status_code == 422
    unknown_cursor = await client.get(f"/api/v1/runs/{run_id}/conflicts?cursor=SKU-ZZZ")
    assert unknown_cursor.status_code == 200
    assert unknown_cursor.json()["items"] == []
    assert unknown_cursor.json()["next_cursor"] is None
    overlong_cursor = await client.get(f"/api/v1/runs/{run_id}/conflicts?cursor={'A' * 65}")
    assert overlong_cursor.status_code == 422
    malformed_cursor = await client.get(f"/api/v1/runs/{run_id}/conflicts?cursor=SKU_INVALID")
    assert malformed_cursor.status_code == 422


async def test_conflicts_require_known_run(client: httpx.AsyncClient) -> None:
    response = await client.get("/api/v1/runs/run_ghost-998/conflicts")
    assert response.status_code == 404
    assert response.json()["code"] == "run_not_found"

"""Pipeline route contract tests."""

import httpx
import pytest

from tests.api.conftest import DOCUMENT, PIPELINE_ID, seed_scenario


@pytest.mark.anyio
async def test_create_and_get_pipeline(client: httpx.AsyncClient) -> None:
    response = await client.post(
        "/api/v1/pipelines",
        json={"pipeline_id": PIPELINE_ID, "display_name": "Demo", "description": "d"},
    )
    assert response.status_code == 201
    body = response.json()
    assert body["pipeline_id"] == PIPELINE_ID
    assert body["schema_version"] == 1
    assert body["row_version"] == 1

    fetched = await client.get(f"/api/v1/pipelines/{PIPELINE_ID}")
    assert fetched.status_code == 200
    assert fetched.json()["display_name"] == "Demo"


@pytest.mark.anyio
async def test_duplicate_pipeline_returns_conflict(client: httpx.AsyncClient) -> None:
    payload = {"pipeline_id": PIPELINE_ID, "display_name": "Demo"}
    await client.post("/api/v1/pipelines", json=payload)
    response = await client.post("/api/v1/pipelines", json=payload)
    assert response.status_code == 409
    assert response.json()["code"] == "duplicate_record"


@pytest.mark.anyio
async def test_pipeline_listing_is_paginated_in_identifier_order(
    client: httpx.AsyncClient,
) -> None:
    for index in range(3):
        await client.post(
            "/api/v1/pipelines",
            json={"pipeline_id": f"pip_list-{index:03d}", "display_name": "d"},
        )
    first = await client.get("/api/v1/pipelines", params={"limit": 2})
    assert first.status_code == 200
    body = first.json()
    assert [item["pipeline_id"] for item in body["items"]] == [
        "pip_list-000",
        "pip_list-001",
    ]
    assert body["next_cursor"] == "pip_list-001"
    second = await client.get(
        "/api/v1/pipelines", params={"limit": 2, "cursor": body["next_cursor"]}
    )
    assert [item["pipeline_id"] for item in second.json()["items"]] == ["pip_list-002"]
    assert second.json()["next_cursor"] is None


@pytest.mark.anyio
async def test_pagination_bounds_are_validated_before_storage_work(
    client: httpx.AsyncClient,
) -> None:
    for limit in (0, 101, -3):
        response = await client.get("/api/v1/pipelines", params={"limit": limit})
        assert response.status_code == 422, limit
    response = await client.get("/api/v1/pipelines", params={"cursor": "not-canonical"})
    assert response.status_code == 422


@pytest.mark.anyio
async def test_publish_version_freezes_and_replays_the_document(
    client: httpx.AsyncClient,
) -> None:
    created = await seed_scenario(client)
    assert created.status_code == 201
    first = await client.get(f"/api/v1/pipelines/{PIPELINE_ID}/versions/1")
    assert first.status_code == 200

    replay = await client.post(
        f"/api/v1/pipelines/{PIPELINE_ID}/versions",
        json={"document": DOCUMENT, "expected_latest_version": 1},
    )
    assert replay.status_code == 201
    assert replay.json()["version"] == 2
    assert replay.json()["specification_sha256"] == first.json()["specification_sha256"]

    changed = {**DOCUMENT, "resource_policy": {"max_concurrency": 4}}
    second = await client.post(
        f"/api/v1/pipelines/{PIPELINE_ID}/versions",
        json={"document": changed, "expected_latest_version": 1},
    )
    assert second.status_code == 409
    assert second.json()["code"] == "pipeline_version_conflict"


@pytest.mark.anyio
async def test_publish_unknown_pipeline_returns_not_found(
    client: httpx.AsyncClient,
) -> None:
    response = await client.post(
        "/api/v1/pipelines/pip_missing-one/versions", json={"document": DOCUMENT}
    )
    assert response.status_code == 404
    assert response.json()["code"] == "pipeline_not_found"


@pytest.mark.anyio
async def test_publish_invalid_document_returns_validation_problem(
    client: httpx.AsyncClient,
) -> None:
    await client.post(
        "/api/v1/connectors",
        json={
            "connector_id": "con_source-001",
            "kind": "csv_source",
            "display_name": "CSV",
            "configuration": {"path": "f.csv"},
            "capabilities": {"read": True, "schema_discovery": True},
            "secret_references": [],
        },
    )
    await client.post("/api/v1/pipelines", json={"pipeline_id": PIPELINE_ID, "display_name": "d"})
    broken: dict[str, object] = {**DOCUMENT, "nodes": []}
    response = await client.post(
        f"/api/v1/pipelines/{PIPELINE_ID}/versions", json={"document": broken}
    )
    assert response.status_code == 422
    document = response.json()
    assert document["code"] == "validation"


@pytest.mark.anyio
async def test_get_version_returns_the_stored_specification(
    client: httpx.AsyncClient,
) -> None:
    await seed_scenario(client)
    response = await client.get(f"/api/v1/pipelines/{PIPELINE_ID}/versions/1")
    assert response.status_code == 200
    body = response.json()
    assert body["version"] == 1
    assert body["planner_format_version"] == 1
    assert len(body["specification_sha256"]) == 64
    assert "specification" in body

    missing = await client.get(f"/api/v1/pipelines/{PIPELINE_ID}/versions/9")
    assert missing.status_code == 404
    assert missing.json()["code"] == "pipeline_version_not_found"


@pytest.mark.anyio
async def test_invalid_pipeline_identifier_returns_validation_problem(
    client: httpx.AsyncClient,
) -> None:
    response = await client.get("/api/v1/pipelines/pip_BAD-ID")
    assert response.status_code == 422
    assert response.json()["code"] == "validation"

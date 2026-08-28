"""Connector route contract and connection-test tests."""

import httpx
import pytest

from tests.api.conftest import CONNECTOR_ID


def _registration(**overrides: object) -> dict[str, object]:
    payload: dict[str, object] = {
        "connector_id": CONNECTOR_ID,
        "kind": "csv_source",
        "display_name": "CSV source",
        "configuration": {"path": "fixture.csv"},
        "capabilities": {"read": True, "schema_discovery": True},
        "secret_references": [],
    }
    payload.update(overrides)
    return payload


@pytest.mark.anyio
async def test_register_connector_returns_reference_names_only(
    client: httpx.AsyncClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("PG_TEST_TOKEN", "super-secret-value")
    response = await client.post(
        "/api/v1/connectors",
        json=_registration(
            configuration={"path": "fixture.csv", "token_reference": "api_token"},
            secret_references=[
                {"reference_name": "api_token", "environment_variable_name": "PG_TEST_TOKEN"}
            ],
        ),
    )
    assert response.status_code == 201
    body = response.json()
    assert body["connector_id"] == CONNECTOR_ID
    assert "secret_references" not in body

    listing = await client.get("/api/v1/connectors")
    assert listing.status_code == 200
    record = next(item for item in listing.json()["items"] if item["connector_id"] == CONNECTOR_ID)
    references = record["secret_references"]
    assert references == [
        {
            "reference_name": "api_token",
            "environment_variable_name": "PG_TEST_TOKEN",
        }
    ]
    # Reference names only: the environment variable name appears, but a
    # resolved value must never surface anywhere in the response.
    assert listing.text.count("PG_TEST_TOKEN") >= 1
    assert "super-secret-value" not in listing.text


@pytest.mark.anyio
async def test_unknown_connector_kind_is_rejected(client: httpx.AsyncClient) -> None:
    response = await client.post("/api/v1/connectors", json=_registration(kind="ftp_source"))
    assert response.status_code == 422
    assert "closed registry" in response.json()["detail"]


@pytest.mark.anyio
async def test_unsafe_secret_configuration_is_refused(
    client: httpx.AsyncClient,
) -> None:
    response = await client.post(
        "/api/v1/connectors",
        json=_registration(configuration={"path": "f.csv", "password": "hunter2"}),
    )
    assert response.status_code == 422
    assert response.json()["type"].endswith("/unsafe-connector-configuration")
    assert "hunter2" not in response.text


@pytest.mark.anyio
async def test_connector_listing_is_paginated(client: httpx.AsyncClient) -> None:
    for index in range(2):
        await client.post(
            "/api/v1/connectors",
            json=_registration(connector_id=f"con_list-{index:03d}"),
        )
    page = await client.get("/api/v1/connectors", params={"limit": 1})
    assert page.status_code == 200
    body = page.json()
    assert [item["connector_id"] for item in body["items"]] == ["con_list-000"]
    assert body["next_cursor"] == "con_list-000"
    rest = await client.get(
        "/api/v1/connectors", params={"limit": 5, "cursor": body["next_cursor"]}
    )
    assert [item["connector_id"] for item in rest.json()["items"]] == ["con_list-001"]


@pytest.mark.anyio
async def test_connection_test_reports_bounded_checks(
    client: httpx.AsyncClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("PARITYGRID_TEST_TOKEN", "super-secret-value")
    await client.post(
        "/api/v1/connectors",
        json=_registration(
            configuration={"path": "fixture.csv", "token_reference": "api_token"},
            secret_references=[
                {
                    "reference_name": "api_token",
                    "environment_variable_name": "PARITYGRID_TEST_TOKEN",
                }
            ],
        ),
    )
    response = await client.post(f"/api/v1/connectors/{CONNECTOR_ID}/test")
    assert response.status_code == 200
    body = response.json()
    assert body["passed"] is True
    assert {check["name"] for check in body["checks"]} >= {
        "kind_registered",
        "capabilities_contract",
        "secret_references_resolvable",
    }
    assert "super-secret-value" not in response.text


@pytest.mark.anyio
async def test_connection_test_reports_missing_environment_names(
    client: httpx.AsyncClient,
) -> None:
    await client.post(
        "/api/v1/connectors",
        json=_registration(
            configuration={"path": "fixture.csv", "token_reference": "api_token"},
            secret_references=[
                {
                    "reference_name": "api_token",
                    "environment_variable_name": "PARITYGRID_DEFINITELY_MISSING_TOKEN",
                }
            ],
        ),
    )
    response = await client.post(f"/api/v1/connectors/{CONNECTOR_ID}/test")
    assert response.status_code == 200
    body = response.json()
    assert body["passed"] is False
    missing = next(
        check for check in body["checks"] if check["name"] == "secret_references_resolvable"
    )
    assert "PARITYGRID_DEFINITELY_MISSING_TOKEN" in missing["detail"]


@pytest.mark.anyio
async def test_connection_test_for_unknown_connector_returns_not_found(
    client: httpx.AsyncClient,
) -> None:
    response = await client.post("/api/v1/connectors/con_missing-1/test")
    assert response.status_code == 404
    assert response.json()["code"] == "connector_not_found"

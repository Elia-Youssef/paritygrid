"""System capabilities route and settings tests."""

from pathlib import Path

import httpx
import pytest

from paritygrid.runtime.config import Settings


@pytest.mark.anyio
async def test_capabilities_report_runners_sqlite_and_limits(
    client: httpx.AsyncClient,
) -> None:
    response = await client.get("/api/v1/system/capabilities")
    assert response.status_code == 200
    body = response.json()
    assert body["schema_version"] == 1
    assert body["service"] == "ParityGrid"
    assert body["sqlite"]["library_version"].count(".") == 2
    assert body["sqlite"]["supports_json_sql"] is True
    assert body["sqlite"]["supports_returning"] is True
    runner_ids = {runner["strategy_id"] for runner in body["runners"]}
    assert {"sequential", "threaded", "asyncio"} <= runner_ids
    assert all(
        runner["available"] is False or runner["unavailability_reason"] is None
        for runner in body["runners"]
    )
    limits = body["limits"]
    assert limits["max_page_size"] == 100
    assert limits["max_request_body_bytes"] == 1_048_576
    assert limits["max_concurrent_requests"] == 64
    assert limits["idempotency_lease_seconds"] > limits["request_timeout_seconds"]
    assert {"process_pool", "interpreter_pool"} <= {feature["name"] for feature in body["features"]}
    # No secret or host detail may appear anywhere in the report.
    for forbidden in ("password", "credential", "token", "\\\\", "C:/"):
        assert forbidden not in response.text


@pytest.mark.anyio
async def test_capabilities_absent_runtime_returns_problem() -> None:
    from paritygrid.api.app import create_app

    transport = httpx.ASGITransport(app=create_app())
    async with httpx.AsyncClient(transport=transport, base_url="http://t") as client:
        response = await client.get("/api/v1/system/capabilities")
    assert response.status_code == 503
    assert response.json()["code"] == "runtime_unavailable"


def test_settings_reject_lease_shorter_than_the_request_budget() -> None:
    with pytest.raises(ValueError, match="idempotency_lease_seconds"):
        Settings(idempotency_lease_seconds=5.0, request_timeout_seconds=10.0)


def test_settings_resolve_storage_paths_under_the_data_root(tmp_path: Path) -> None:
    settings = Settings(data_root=tmp_path / "nested" / "root")
    assert settings.database_path.parent == (tmp_path / "nested" / "root").resolve()
    assert settings.database_path.name == "paritygrid.db"
    assert settings.artifact_root_path.name == "artifacts"


def test_settings_reject_unsafe_storage_names(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="database_filename"):
        Settings(data_root=tmp_path, database_filename="../escape.db")
    with pytest.raises(ValueError, match="artifact_root_name"):
        Settings(data_root=tmp_path, artifact_root_name="..")

"""Problem Details contract matrix tests."""

from typing import Any

import httpx
import pytest

from paritygrid.api.app import create_app
from paritygrid.api.errors.mapping import translate_error
from paritygrid.api.errors.problems import ProblemError
from paritygrid.application.services.errors import (
    IdempotencyInProgressError,
    OperationalRecordNotFoundError,
    RunInvalidTransitionError,
)
from paritygrid.domain.execution import RunState
from paritygrid.runtime.composition import RuntimeContainer
from tests.api.conftest import seed_scenario, transition_run


def _problem_of(response: httpx.Response) -> dict[str, Any]:
    assert response.headers["content-type"].startswith("application/problem+json")
    document = response.json()
    for field in ("type", "title", "status", "detail", "instance", "correlation_id", "code"):
        assert field in document, field
    assert document["status"] == response.status_code
    assert document["type"].startswith("https://paritygrid.dev/problems/")
    return document


@pytest.mark.anyio
async def test_unknown_route_returns_not_found_problem(client: httpx.AsyncClient) -> None:
    response = await client.get("/api/v1/does-not-exist")
    document = _problem_of(response)
    assert response.status_code == 404
    assert document["code"] == "not_found"


@pytest.mark.anyio
async def test_method_not_allowed_returns_problem(client: httpx.AsyncClient) -> None:
    response = await client.delete("/api/v1/pipelines")
    document = _problem_of(response)
    assert response.status_code == 405
    assert document["code"] == "method_not_allowed"


@pytest.mark.anyio
async def test_missing_resource_returns_not_found_problem(
    client: httpx.AsyncClient,
) -> None:
    response = await client.get("/api/v1/pipelines/pip_missing-one")
    document = _problem_of(response)
    assert response.status_code == 404
    assert document["code"] == "pipeline_not_found"


@pytest.mark.anyio
async def test_request_validation_failure_returns_bounded_field_errors(
    client: httpx.AsyncClient,
) -> None:
    response = await client.post("/api/v1/pipelines", json={"pipeline_id": "not-canonical"})
    document = _problem_of(response)
    assert response.status_code == 422
    assert document["code"] == "validation"
    assert len(document["errors"]) <= 10
    fields = {error["field"] for error in document["errors"]}
    assert "pipeline_id" in fields or "display_name" in fields


@pytest.mark.anyio
async def test_invalid_transition_returns_conflict_problem(
    container: RuntimeContainer, client: httpx.AsyncClient
) -> None:
    await seed_scenario(client)
    transition_run(container, "run_scenario-01", RunState.RUNNING)
    transition_run(container, "run_scenario-01", RunState.FAILED)
    response = await client.post("/api/v1/runs/run_scenario-01/resume")
    document = _problem_of(response)
    assert response.status_code == 409
    assert document["type"].endswith("/invalid-transition")
    assert document["code"] == "run_invalid_transition"


@pytest.mark.anyio
async def test_malformed_json_returns_invalid_input_problem(
    client: httpx.AsyncClient,
) -> None:
    response = await client.post(
        "/api/v1/pipelines", content=b"{nope", headers={"Content-Type": "application/json"}
    )
    document = _problem_of(response)
    assert response.status_code == 400
    assert document["code"] == "malformed_json_body"


@pytest.mark.anyio
async def test_wrong_media_type_returns_unsupported_media_type(
    client: httpx.AsyncClient,
) -> None:
    response = await client.post(
        "/api/v1/pipelines", content=b"{}", headers={"Content-Type": "text/plain"}
    )
    document = _problem_of(response)
    assert response.status_code == 415
    assert document["code"] == "unsupported_media_type"


@pytest.mark.anyio
async def test_oversized_body_returns_request_too_large(client: httpx.AsyncClient) -> None:
    body = b'{"a": "' + b"x" * 1_200_000 + b'"}'
    response = await client.post(
        "/api/v1/pipelines", content=body, headers={"Content-Type": "application/json"}
    )
    document = _problem_of(response)
    assert response.status_code == 413
    assert document["code"] == "request_body_too_large"


@pytest.mark.anyio
async def test_deeply_nested_json_returns_invalid_input_problem(
    client: httpx.AsyncClient,
) -> None:
    payload = {"document": _nested(200)}
    response = await client.post(
        "/api/v1/pipelines/pip_demo-alpha/versions",
        json=payload,
    )
    document = _problem_of(response)
    assert response.status_code == 400
    assert document["code"] == "malformed_json_body"


@pytest.mark.anyio
async def test_range_outside_artifact_returns_range_problem(
    container: RuntimeContainer, client: httpx.AsyncClient
) -> None:
    await seed_scenario(client)
    response = await client.get(
        "/api/v1/artifacts/art_missing-01", headers={"Range": "bytes=0-5000"}
    )
    document = _problem_of(response)
    assert response.status_code in {404, 416}
    if response.status_code == 416:
        assert document["type"].endswith("/range-not-satisfiable")


@pytest.mark.anyio
async def test_services_absent_returns_runtime_unavailable_problem() -> None:
    transport = httpx.ASGITransport(app=create_app())
    async with httpx.AsyncClient(transport=transport, base_url="http://t") as client:
        response = await client.get("/api/v1/pipelines")
    document = _problem_of(response)
    assert response.status_code == 503
    assert document["code"] == "runtime_unavailable"


def test_translate_error_covers_the_problem_matrix() -> None:
    cases: list[tuple[Exception, int, str]] = [
        (OperationalRecordNotFoundError("run", "run_x"), 404, "run_not_found"),
        (RunInvalidTransitionError("nope"), 409, "run_invalid_transition"),
        (IdempotencyInProgressError("busy"), 409, "idempotency_in_progress"),
        (ProblemError(type_slug="conflict", title="t", status=409), 409, "conflict"),
        (RuntimeError("boom"), 500, "internal_error"),
    ]
    for error, status, code in cases:
        problem = translate_error(error)
        assert problem.status == status, (error, status)
        assert problem.code == code, (error, code)


def test_problem_documents_never_expose_internals() -> None:
    problem = ProblemError(
        type_slug="internal",
        title="Internal service error",
        status=500,
        detail="x" * 2_000,
    )
    document = problem.to_document(instance="/api/v1/runs", correlation_id="c")
    detail = document["detail"]
    assert isinstance(detail, str)
    assert len(detail) <= 512
    assert "Traceback" not in detail


def _nested(depth: int) -> object:
    value: object = {"leaf": True}
    for _ in range(depth):
        value = {"child": value}
    return value

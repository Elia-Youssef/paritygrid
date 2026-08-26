"""Simulator stack lifecycle: dynamic ports, readiness, rollback, and cleanup."""

import asyncio
import threading
from collections.abc import AsyncIterator, Callable

import httpx
import pytest

from paritygrid.demo.datasets import (
    DatasetProfile,
    ScenarioSeed,
    ScenarioVersion,
    generate_dataset,
)
from paritygrid.demo.failures import (
    FailureScript,
    ScriptedFailure,
    ScriptedFailureKind,
)
from paritygrid.demo.simulators.lifecycle import (
    SimulatorEndpoints,
    SimulatorLifecycleError,
    SimulatorStack,
    SimulatorStackConfig,
    SimulatorStartupFault,
)

pytestmark = pytest.mark.anyio
_CLIENT_TIMEOUT = httpx.Timeout(10.0)
_DATASET = generate_dataset(
    ScenarioSeed(64),
    ScenarioVersion(1),
    DatasetProfile(record_count=8, malformed_count=1, boundary_count=1, duplicate_count=1),
)


@pytest.fixture
async def stack() -> AsyncIterator[SimulatorStack]:
    candidate = SimulatorStack(_DATASET)
    await candidate.start()
    try:
        yield candidate
    finally:
        await candidate.aclose()


async def test_start_publishes_three_distinct_dynamic_ports(stack: SimulatorStack) -> None:
    endpoints = stack.endpoints
    assert isinstance(endpoints, SimulatorEndpoints)
    ports = {
        endpoints.async_source_port,
        endpoints.blocking_source_port,
        endpoints.warehouse_port,
    }
    assert len(ports) == 3
    assert all(0 < port < 65_536 for port in ports)
    assert endpoints.async_source_base_url == f"http://127.0.0.1:{endpoints.async_source_port}"
    assert (
        endpoints.blocking_source_base_url == f"http://127.0.0.1:{endpoints.blocking_source_port}"
    )
    assert endpoints.warehouse_base_url == f"http://127.0.0.1:{endpoints.warehouse_port}"
    assert stack.is_started() is True


async def test_every_service_answers_readiness_over_real_http(stack: SimulatorStack) -> None:
    endpoints = stack.endpoints
    expected = {
        endpoints.async_source_base_url: "async-source",
        endpoints.blocking_source_base_url: "blocking-source",
        endpoints.warehouse_base_url: "warehouse",
    }
    async with httpx.AsyncClient(timeout=_CLIENT_TIMEOUT) as client:
        for base_url, service in expected.items():
            response = await client.get(f"{base_url}/healthz")
            assert response.status_code == 200
            assert response.json() == {"service": service, "status": "ok"}


async def test_full_stack_serves_equivalent_source_data_and_target_state(
    stack: SimulatorStack,
) -> None:
    endpoints = stack.endpoints
    async with httpx.AsyncClient(timeout=_CLIENT_TIMEOUT) as client:
        async_page = await client.get(f"{endpoints.async_source_base_url}/v1/inventory")
        async_records = async_page.json()["records"]
        blocking_records = await asyncio.to_thread(
            _fetch_blocking_page, endpoints.blocking_source_base_url
        )
        state = await client.get(f"{endpoints.warehouse_base_url}/v1/state")
    assert async_records == blocking_records
    assert len(async_records) == len(_DATASET.rows)
    assert state.json()["record_count"] == 0


def _fetch_blocking_page(base_url: str) -> list[dict[str, object]]:
    with httpx.Client(base_url=base_url, timeout=_CLIENT_TIMEOUT) as client:
        return client.get("/v1/inventory/pages/1", params={"page_size": "50"}).json()["records"]


async def test_endpoints_are_unavailable_before_start() -> None:
    candidate = SimulatorStack(_DATASET)
    assert candidate.is_started() is False
    with pytest.raises(SimulatorLifecycleError, match="has not completed startup"):
        _ = candidate.endpoints


async def test_double_start_is_refused(stack: SimulatorStack) -> None:
    with pytest.raises(SimulatorLifecycleError, match="already started"):
        await stack.start()


async def test_repeated_shutdown_is_safe_and_releases_owned_resources(
    stack: SimulatorStack,
) -> None:
    endpoints = stack.endpoints
    blocking_thread = stack.blocking_source.serving_thread
    await stack.aclose()
    await stack.aclose()
    assert stack.is_closed() is True
    assert not blocking_thread.is_alive()
    assert stack.async_source.is_serving() is False
    assert stack.warehouse.is_serving() is False
    async with httpx.AsyncClient(timeout=httpx.Timeout(2.0)) as client:
        for base_url in (
            endpoints.async_source_base_url,
            endpoints.blocking_source_base_url,
            endpoints.warehouse_base_url,
        ):
            with pytest.raises(httpx.TransportError):
                await client.get(f"{base_url}/healthz")


async def test_start_after_close_is_refused(stack: SimulatorStack) -> None:
    await stack.aclose()
    with pytest.raises(SimulatorLifecycleError, match="cannot restart"):
        await stack.start()


@pytest.mark.parametrize(
    "fault",
    [
        SimulatorStartupFault.ASYNC_SOURCE,
        SimulatorStartupFault.BLOCKING_SOURCE,
        SimulatorStartupFault.WAREHOUSE,
    ],
)
async def test_partial_startup_rolls_back_every_started_service(
    fault: SimulatorStartupFault,
) -> None:
    candidate = SimulatorStack(_DATASET, config=SimulatorStackConfig(fault=fault))
    with pytest.raises(SimulatorLifecycleError, match="Injected startup fault"):
        await candidate.start()
    assert candidate.is_started() is False
    if fault is not SimulatorStartupFault.ASYNC_SOURCE:
        # The async source started first and must have been closed again.
        assert candidate.async_source.is_serving() is False
        with pytest.raises(RuntimeError):
            _ = candidate.async_source.port
    if fault is SimulatorStartupFault.WAREHOUSE:
        assert candidate.blocking_source.is_serving() is False
        live_names = {thread.name for thread in threading.enumerate()}
        assert "paritygrid-blocking-source" not in live_names
    await candidate.aclose()


async def test_startup_timeout_rolls_back_and_reports() -> None:
    candidate = SimulatorStack(
        _DATASET,
        config=SimulatorStackConfig(startup_timeout_microseconds=1),
    )
    with pytest.raises(TimeoutError):
        await candidate.start()
    assert candidate.is_started() is False
    await candidate.aclose()


async def test_cancellation_during_startup_rolls_back() -> None:
    candidate = SimulatorStack(_DATASET)
    task = asyncio.create_task(candidate.start())
    await asyncio.sleep(0)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert candidate.is_started() is False
    assert candidate.is_closed() is False
    await candidate.aclose()


async def test_config_validation_rejects_out_of_bounds() -> None:
    with pytest.raises(SimulatorLifecycleError):
        SimulatorStackConfig(startup_timeout_microseconds=0)
    with pytest.raises(SimulatorLifecycleError):
        SimulatorStackConfig(probe_timeout_microseconds=61_000_000)
    with pytest.raises(SimulatorLifecycleError):
        SimulatorStackConfig(max_page_size=201)
    with pytest.raises(SimulatorLifecycleError):
        SimulatorStackConfig(source_request_latency_microseconds=-1)


async def test_scripts_flow_through_the_stack_services() -> None:
    script = FailureScript.from_entries(
        [
            ScriptedFailure(sequence=1, kind=ScriptedFailureKind.RATE_LIMIT, retry_after_seconds=2),
            ScriptedFailure(sequence=2, kind=ScriptedFailureKind.TRANSIENT_ERROR),
        ]
    )
    candidate = SimulatorStack(_DATASET, source_script=script)
    endpoints = await candidate.start()
    try:
        async with httpx.AsyncClient(timeout=_CLIENT_TIMEOUT) as client:
            statuses = [
                (await client.get(f"{endpoints.async_source_base_url}/v1/inventory")).status_code
                for _ in range(3)
            ]
        assert statuses == [429, 503, 200]
        assert [
            (item.sequence, item.kind.value) for item in candidate.async_source.applied_failures()
        ] == [(1, "rate_limit"), (2, "transient_error")]
    finally:
        await candidate.aclose()


async def test_two_stacks_coexist_on_independent_dynamic_ports() -> None:
    first = SimulatorStack(_DATASET)
    second = SimulatorStack(_DATASET)
    first_endpoints = await first.start()
    second_endpoints = await second.start()
    try:
        assert first_endpoints.async_source_port != second_endpoints.async_source_port
        assert first_endpoints.blocking_source_port != second_endpoints.blocking_source_port
        assert first_endpoints.warehouse_port != second_endpoints.warehouse_port
        async with httpx.AsyncClient(timeout=_CLIENT_TIMEOUT) as client:
            for endpoints in (first_endpoints, second_endpoints):
                response = await client.get(f"{endpoints.warehouse_base_url}/healthz")
                assert response.status_code == 200
    finally:
        await first.aclose()
        await second.aclose()


async def test_readiness_probe_requires_strict_health_documents() -> None:
    from paritygrid.demo.simulators.async_server import AsyncHttpService
    from paritygrid.demo.simulators.http_wire import HttpRequest, PlannedResponse, json_response
    from paritygrid.demo.simulators.lifecycle import probe_service_health

    def make_handler(defect: str) -> Callable[[HttpRequest], PlannedResponse]:
        def handler(request: HttpRequest) -> PlannedResponse:
            if request.path != "/healthz":
                return json_response(404, {})
            if defect == "status-500":
                return json_response(503, {"service": "stub", "status": "ok"})
            if defect == "not-json":
                return PlannedResponse(
                    status=200,
                    body=b"<html>",
                    headers=(("Content-Type", "text/html"),),
                )
            if defect == "non-object":
                return json_response(200, ["list"])
            if defect == "not-ok":
                return json_response(200, {"service": "stub", "status": "degraded"})
            return json_response(200, {"service": "other", "status": "ok"})

        return handler

    for defect in (
        "status-500",
        "not-json",
        "non-object",
        "not-ok",
        "wrong-service",
    ):
        service = AsyncHttpService(service_name="stub", handler=make_handler(defect))
        await service.start()
        try:
            with pytest.raises(SimulatorLifecycleError, match="readiness probe"):
                await probe_service_health(
                    service.base_url, expected_service="stub", timeout_seconds=5.0
                )
        finally:
            await service.aclose()


async def test_readiness_probe_accepts_a_strictly_matching_service() -> None:
    from paritygrid.demo.simulators.async_server import AsyncHttpService
    from paritygrid.demo.simulators.http_wire import HttpRequest, PlannedResponse, json_response
    from paritygrid.demo.simulators.lifecycle import probe_service_health

    def handler(request: HttpRequest) -> PlannedResponse:
        if request.path == "/healthz":
            return json_response(200, {"service": "stub", "status": "ok"})
        return json_response(404, {})

    service = AsyncHttpService(service_name="stub", handler=handler)
    await service.start()
    try:
        await probe_service_health(service.base_url, expected_service="stub", timeout_seconds=5.0)
    finally:
        await service.aclose()


async def test_readiness_probe_times_out_against_an_unreachable_port() -> None:
    from paritygrid.demo.simulators.lifecycle import probe_service_health

    with pytest.raises((TimeoutError, OSError)):
        await probe_service_health(
            "http://127.0.0.1:9", expected_service="stub", timeout_seconds=0.2
        )

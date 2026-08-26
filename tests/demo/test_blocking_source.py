"""Blocking source simulator: page semantics, blocking boundary, and failures."""

import asyncio
import socket
import threading
import time
from collections.abc import AsyncIterator

import httpx
import pytest

from paritygrid.demo.datasets import DatasetProfile, ScenarioSeed, ScenarioVersion, generate_dataset
from paritygrid.demo.failures import (
    FailureScript,
    ScriptedFailure,
    ScriptedFailureKind,
)
from paritygrid.demo.simulators.async_source import AsyncInventorySource
from paritygrid.demo.simulators.blocking_source import BlockingInventorySource

_PROFILE = DatasetProfile(record_count=22, malformed_count=2, boundary_count=2, duplicate_count=2)
_DATASET = generate_dataset(ScenarioSeed(314), ScenarioVersion(1), _PROFILE)
_CLIENT_TIMEOUT = httpx.Timeout(10.0)
pytestmark = pytest.mark.anyio


@pytest.fixture
async def source() -> AsyncIterator[BlockingInventorySource]:
    simulator = BlockingInventorySource(_DATASET, FailureScript.empty())
    simulator.start()
    try:
        yield simulator
    finally:
        await simulator.aclose()


def _blocking_client(base_url: str) -> httpx.Client:
    return httpx.Client(base_url=base_url, timeout=_CLIENT_TIMEOUT)


async def test_health_reports_legacy_service_identity(source: BlockingInventorySource) -> None:
    def fetch() -> httpx.Response:
        with _blocking_client(source.base_url) as client:
            return client.get("/healthz")

    response = await asyncio.to_thread(fetch)
    assert response.status_code == 200
    assert response.json() == {"service": "blocking-source", "status": "ok"}


async def test_page_traversal_returns_every_row_in_order(source: BlockingInventorySource) -> None:
    def fetch_all() -> list[dict[str, object]]:
        with _blocking_client(source.base_url) as client:
            records: list[dict[str, object]] = []
            page = 1
            total_pages = None
            while total_pages is None or page <= total_pages:
                response = client.get(f"/v1/inventory/pages/{page}", params={"page_size": "5"})
                response.raise_for_status()
                document = response.json()
                total_pages = document["total_pages"]
                records.extend(document["records"])
                page += 1
            return records

    records = await asyncio.to_thread(fetch_all)
    expected = [dict(row.payload) for row in _DATASET.rows]
    assert records == expected


async def test_last_page_is_partial_and_totals_are_exact(source: BlockingInventorySource) -> None:
    def fetch() -> tuple[int, int, httpx.Response]:
        with _blocking_client(source.base_url) as client:
            first = client.get("/v1/inventory/pages/1", params={"page_size": "8"})
            document = first.json()
            last_page = document["total_pages"]
            last = client.get(f"/v1/inventory/pages/{last_page}", params={"page_size": "8"})
            return last_page, len(_DATASET.rows), last

    last_page, total_rows, last = await asyncio.to_thread(fetch)
    assert last.status_code == 200
    assert last_page == (total_rows + 7) // 8
    assert len(last.json()["records"]) == total_rows - (last_page - 1) * 8


async def test_page_size_is_clamped_and_invalid_values_rejected(
    source: BlockingInventorySource,
) -> None:
    def fetch(page_size: str) -> httpx.Response:
        with _blocking_client(source.base_url) as client:
            return client.get("/v1/inventory/pages/1", params={"page_size": page_size})

    clamped = await asyncio.to_thread(fetch, "999999")
    assert clamped.status_code == 200
    assert clamped.json()["page_size"] == 200
    for bad in ("0", "-2", "abc"):
        response = await asyncio.to_thread(fetch, bad)
        assert response.status_code == 400


async def test_invalid_or_out_of_range_pages_are_rejected(source: BlockingInventorySource) -> None:
    def fetch(path: str) -> httpx.Response:
        with _blocking_client(source.base_url) as client:
            return client.get(path)

    assert (await asyncio.to_thread(fetch, "/v1/inventory/pages/0")).status_code == 400
    assert (await asyncio.to_thread(fetch, "/v1/inventory/pages/-1")).status_code == 400
    assert (await asyncio.to_thread(fetch, "/v1/inventory/pages/abc")).status_code == 400
    beyond = await asyncio.to_thread(fetch, "/v1/inventory/pages/9999")
    assert beyond.status_code == 404
    assert beyond.json()["error"]["code"] == "page_out_of_range"


async def test_blocking_source_serves_the_same_logical_rows_as_the_async_source() -> None:
    async_simulator = AsyncInventorySource(_DATASET, FailureScript.empty())
    blocking_simulator = BlockingInventorySource(_DATASET, FailureScript.empty())
    await async_simulator.start()
    blocking_simulator.start()
    try:
        async_records: list[dict[str, object]] = []
        cursor = ""
        async with httpx.AsyncClient(timeout=_CLIENT_TIMEOUT) as client:
            while True:
                params: dict[str, str] = {"limit": "6"}
                if cursor:
                    params["cursor"] = cursor
                response = await client.get(
                    f"{async_simulator.base_url}/v1/inventory", params=params
                )
                document = response.json()
                async_records.extend(document["records"])
                cursor = document["next_cursor"]
                if cursor == "":
                    break

        def fetch_blocking() -> list[dict[str, object]]:
            with _blocking_client(blocking_simulator.base_url) as client:
                records: list[dict[str, object]] = []
                page = 1
                while True:
                    response = client.get(f"/v1/inventory/pages/{page}", params={"page_size": "6"})
                    document = response.json()
                    records.extend(document["records"])
                    if page >= document["total_pages"]:
                        return records
                    page += 1

        blocking_records = await asyncio.to_thread(fetch_blocking)
        assert async_records == blocking_records
        assert len(async_records) == len(_DATASET.rows)
    finally:
        await async_simulator.aclose()
        await blocking_simulator.aclose()


async def test_timeout_script_blocks_the_client_thread_then_recovers() -> None:
    script = FailureScript.from_entries(
        [ScriptedFailure(sequence=1, kind=ScriptedFailureKind.TIMEOUT, delay_microseconds=300_000)]
    )
    simulator = BlockingInventorySource(_DATASET, script)
    simulator.start()
    try:

        def timed_fetch() -> tuple[float, int]:
            start = time.monotonic()
            with _blocking_client(simulator.base_url) as client:
                response = client.get("/v1/inventory/pages/1", params={"page_size": "5"})
            return time.monotonic() - start, response.status_code

        elapsed, status = await asyncio.to_thread(timed_fetch)
        # A generous client sees the full server-side delay: the boundary is
        # genuinely blocking rather than event-loop scheduled.
        assert elapsed >= 0.25
        assert status == 200
        assert simulator.applied_failures()[0].kind is ScriptedFailureKind.TIMEOUT
    finally:
        await simulator.aclose()


async def test_small_client_deadline_times_out_against_the_blocking_delay() -> None:
    script = FailureScript.from_entries(
        [ScriptedFailure(sequence=1, kind=ScriptedFailureKind.TIMEOUT, delay_microseconds=400_000)]
    )
    simulator = BlockingInventorySource(_DATASET, script)
    simulator.start()
    try:

        def timed_out_fetch() -> None:
            with httpx.Client(base_url=simulator.base_url, timeout=httpx.Timeout(0.05)) as client:
                client.get("/v1/inventory/pages/1")

        with pytest.raises(httpx.ReadTimeout):
            await asyncio.to_thread(timed_out_fetch)
    finally:
        await simulator.aclose()


@pytest.mark.parametrize(
    ("failure", "expected_status", "error_code"),
    [
        (
            ScriptedFailure(sequence=1, kind=ScriptedFailureKind.RATE_LIMIT, retry_after_seconds=9),
            429,
            "rate_limited",
        ),
        (
            ScriptedFailure(sequence=1, kind=ScriptedFailureKind.TRANSIENT_ERROR),
            503,
            "transient",
        ),
    ],
)
async def test_http_status_failures_apply_on_the_blocking_boundary(
    failure: ScriptedFailure, expected_status: int, error_code: str
) -> None:
    simulator = BlockingInventorySource(_DATASET, FailureScript.from_entries([failure]))
    simulator.start()
    try:

        def fetch() -> tuple[httpx.Response, httpx.Response]:
            with _blocking_client(simulator.base_url) as client:
                first = client.get("/v1/inventory/pages/1")
                second = client.get("/v1/inventory/pages/1")
                return first, second

        first, second = await asyncio.to_thread(fetch)
        assert first.status_code == expected_status
        assert first.json()["error"]["code"] == error_code
        if failure.kind is ScriptedFailureKind.RATE_LIMIT:
            assert first.headers["retry-after"] == "9"
        assert second.status_code == 200
        applied = simulator.applied_failures()
        assert len(applied) == 1
        assert applied[0].sequence == 1
    finally:
        await simulator.aclose()


async def test_malformed_response_failure_serves_invalid_json() -> None:
    script = FailureScript.from_entries(
        [ScriptedFailure(sequence=1, kind=ScriptedFailureKind.MALFORMED_RESPONSE)]
    )
    simulator = BlockingInventorySource(_DATASET, script)
    simulator.start()
    try:

        def fetch() -> httpx.Response:
            with _blocking_client(simulator.base_url) as client:
                return client.get("/v1/inventory/pages/1")

        response = await asyncio.to_thread(fetch)
        assert response.status_code == 200
        with pytest.raises(Exception):  # noqa: B017, PT011 - any JSON parse failure
            response.json()
    finally:
        await simulator.aclose()


async def test_duplicate_records_failure_stays_within_the_requested_page_limit() -> None:
    script = FailureScript.from_entries(
        [ScriptedFailure(sequence=1, kind=ScriptedFailureKind.DUPLICATE_RECORDS)]
    )
    simulator = BlockingInventorySource(_DATASET, script)
    simulator.start()
    try:

        def fetch() -> list[dict[str, object]]:
            with _blocking_client(simulator.base_url) as client:
                return client.get("/v1/inventory/pages/1", params={"page_size": "3"}).json()[
                    "records"
                ]

        records = await asyncio.to_thread(fetch)
        assert len(records) == 3
        assert records[0] == records[1]
    finally:
        await simulator.aclose()


async def test_connection_loss_failure_aborts_the_blocking_response() -> None:
    script = FailureScript.from_entries(
        [ScriptedFailure(sequence=1, kind=ScriptedFailureKind.CONNECTION_LOSS, partial_bytes=32)]
    )
    simulator = BlockingInventorySource(_DATASET, script)
    simulator.start()
    try:

        def fetch_lossy() -> None:
            with _blocking_client(simulator.base_url) as client:
                client.get("/v1/inventory/pages/1")

        with pytest.raises(httpx.TransportError):
            await asyncio.to_thread(fetch_lossy)

        def fetch_recovered() -> int:
            with _blocking_client(simulator.base_url) as client:
                return client.get("/v1/inventory/pages/1").status_code

        assert await asyncio.to_thread(fetch_recovered) == 200
    finally:
        await simulator.aclose()


async def test_request_latency_sleeps_on_the_handler_thread() -> None:
    slow = BlockingInventorySource(
        _DATASET,
        FailureScript.empty(),
        request_latency_microseconds=250_000,
    )
    slow.start()
    try:
        observed: dict[str, float] = {}

        def fetch() -> None:
            start = time.monotonic()
            with _blocking_client(slow.base_url) as client:
                client.get("/v1/inventory/pages/1", params={"page_size": "5"})
            observed["elapsed"] = time.monotonic() - start

        await asyncio.to_thread(fetch)
        assert observed["elapsed"] >= 0.2
    finally:
        await slow.aclose()


async def test_close_joins_the_owned_serving_thread() -> None:
    simulator = BlockingInventorySource(_DATASET, FailureScript.empty())
    simulator.start()
    thread = simulator.serving_thread
    assert thread.is_alive()
    await simulator.aclose()
    await simulator.aclose()
    assert not thread.is_alive()
    assert simulator.is_serving() is False
    with pytest.raises(RuntimeError, match="has not started"):
        _ = simulator.port


@pytest.mark.parametrize(
    ("script", "request_latency_microseconds"),
    [
        (
            FailureScript.from_entries(
                [
                    ScriptedFailure(
                        sequence=1,
                        kind=ScriptedFailureKind.HANG,
                        delay_microseconds=5_000_000,
                    )
                ]
            ),
            0,
        ),
        (FailureScript.empty(), 5_000_000),
    ],
)
async def test_aclose_interrupts_active_hang_and_delayed_handler_threads(
    script: FailureScript, request_latency_microseconds: int
) -> None:
    simulator = BlockingInventorySource(
        _DATASET,
        script,
        request_latency_microseconds=request_latency_microseconds,
    )
    simulator.start()
    connection = await asyncio.to_thread(_open_active_request, simulator.port)
    try:
        await _wait_for_handled_request(simulator)
        started = asyncio.get_running_loop().time()
        await asyncio.wait_for(simulator.aclose(), timeout=0.5)
        assert asyncio.get_running_loop().time() - started < 0.5
        try:
            response = await asyncio.to_thread(connection.recv, 1)
        except ConnectionResetError:
            response = b""
        assert response == b""
    finally:
        connection.close()
        await simulator.aclose()


async def test_concurrent_blocking_clients_are_served_in_parallel() -> None:
    simulator = BlockingInventorySource(_DATASET, FailureScript.empty())
    simulator.start()
    try:
        results: dict[int, tuple[int, int]] = {}
        lock = threading.Lock()

        def fetch(page: int) -> None:
            with _blocking_client(simulator.base_url) as client:
                document = client.get(
                    f"/v1/inventory/pages/{page}", params={"page_size": "5"}
                ).json()
            with lock:
                results[page] = (document["page"], len(document["records"]))

        threads = [threading.Thread(target=fetch, args=(number,)) for number in (1, 2, 3)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=10.0)
        assert results == {1: (1, 5), 2: (2, 5), 3: (3, 5)}
        assert simulator.request_count() == 3
    finally:
        await simulator.aclose()


def _open_active_request(port: int) -> socket.socket:
    connection = socket.create_connection(("127.0.0.1", port), timeout=1.0)
    connection.settimeout(1.0)
    connection.sendall(b"GET /v1/inventory/pages/1?page_size=5 HTTP/1.1\r\nHost: localhost\r\n\r\n")
    return connection


async def _wait_for_handled_request(simulator: BlockingInventorySource) -> None:
    for _ in range(50):
        if simulator.request_count() == 1:
            return
        await asyncio.sleep(0.01)
    pytest.fail("the simulator did not begin handling the active request")


async def test_hang_failure_on_the_blocking_boundary_is_cancellable() -> None:
    script = FailureScript.from_entries(
        [ScriptedFailure(sequence=1, kind=ScriptedFailureKind.HANG, delay_microseconds=5_000_000)]
    )
    simulator = BlockingInventorySource(_DATASET, script)
    simulator.start()
    try:
        cancelled: dict[str, bool] = {}

        def hang_request(cancel_event: threading.Event) -> None:
            try:
                with httpx.Client(
                    base_url=simulator.base_url, timeout=httpx.Timeout(1.0)
                ) as client:
                    client.get("/v1/inventory/pages/1", params={"page_size": "5"})
            except httpx.HTTPError:
                # The held response ends in a client-side timeout once the
                # hold outlives this short deadline.
                cancelled["timed_out"] = True
            finally:
                cancel_event.set()

        event = threading.Event()
        thread = threading.Thread(target=hang_request, args=(event,), daemon=True)
        thread.start()
        await asyncio.to_thread(time.sleep, 0.1)
        assert not cancelled.get("timed_out", False)
        with _blocking_client(simulator.base_url) as client:
            recovered = client.get("/healthz")
        assert recovered.status_code == 200
        applied = simulator.applied_failures()
        assert [item.kind for item in applied] == [ScriptedFailureKind.HANG]
        assert event.wait(timeout=10.0)
        assert cancelled["timed_out"] is True
        thread.join(timeout=5.0)
    finally:
        await simulator.aclose()

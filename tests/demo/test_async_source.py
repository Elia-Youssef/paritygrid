"""Async source simulator: pagination bounds, HTTP behavior, and failures."""

import asyncio
import base64
import contextlib
import json
from collections.abc import AsyncIterator

import httpx
import pytest
from hypothesis import given
from hypothesis import settings as hypothesis_settings
from hypothesis import strategies as st

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
from paritygrid.demo.simulators.async_source import AsyncInventorySource
from paritygrid.demo.simulators.source_behavior import DEFAULT_MAX_PAGE_SIZE

_PROFILE = DatasetProfile(record_count=24, malformed_count=3, boundary_count=2, duplicate_count=3)
_DATASET = generate_dataset(ScenarioSeed(101), ScenarioVersion(1), _PROFILE)
_CLIENT_TIMEOUT = httpx.Timeout(10.0)
pytestmark = pytest.mark.anyio


def _encode_position(position: int) -> str:
    return base64.urlsafe_b64encode(f"pg1:{position:010d}".encode()).decode().rstrip("=")


def _malformed_cursor(position: int) -> str:
    return base64.urlsafe_b64encode(f"pg1:{position:010d}".encode()).decode().rstrip("=")


def _script(*failures: ScriptedFailure) -> FailureScript:
    return FailureScript.from_entries(list(failures))


@pytest.fixture
async def source() -> AsyncIterator[AsyncInventorySource]:
    simulator = AsyncInventorySource(_DATASET, FailureScript.empty())
    await simulator.start()
    try:
        yield simulator
    finally:
        await simulator.aclose()


async def _collect_all(
    client: httpx.AsyncClient, base_url: str, limit: int
) -> list[dict[str, object]]:
    records: list[dict[str, object]] = []
    cursor = ""
    while True:
        params: dict[str, str] = {"limit": str(limit)}
        if cursor:
            params["cursor"] = cursor
        response = await client.get(f"{base_url}/v1/inventory", params=params)
        response.raise_for_status()
        document = response.json()
        records.extend(document["records"])
        cursor = document["next_cursor"]
        if cursor == "":
            return records


async def test_health_reports_service_identity_without_consuming_script() -> None:
    script = _script(
        ScriptedFailure(sequence=1, kind=ScriptedFailureKind.RATE_LIMIT, retry_after_seconds=1)
    )
    simulator = AsyncInventorySource(_DATASET, script)
    await simulator.start()
    try:
        async with httpx.AsyncClient(
            base_url=simulator.base_url, timeout=_CLIENT_TIMEOUT
        ) as client:
            for _ in range(3):
                response = await client.get("/healthz")
                assert response.status_code == 200
                assert response.json() == {"service": "async-source", "status": "ok"}
        assert simulator.request_count() == 0
        assert simulator.applied_failures() == ()
    finally:
        await simulator.aclose()


async def test_full_cursor_traversal_returns_every_row_exactly_once(
    source: AsyncInventorySource,
) -> None:
    async with httpx.AsyncClient(base_url=source.base_url, timeout=_CLIENT_TIMEOUT) as client:
        records = await _collect_all(client, source.base_url, limit=5)
    expected = [dict(row.payload) for row in _DATASET.rows]
    assert records == expected
    assert source.request_count() == (len(_DATASET.rows) + 4) // 5


async def test_page_size_is_clamped_to_the_configured_maximum(source: AsyncInventorySource) -> None:
    async with httpx.AsyncClient(base_url=source.base_url, timeout=_CLIENT_TIMEOUT) as client:
        response = await client.get("/v1/inventory", params={"limit": 10_000})
    document = response.json()
    assert response.status_code == 200
    assert len(document["records"]) == len(_DATASET.rows)
    assert document["next_cursor"] == ""


async def test_zero_or_negative_limits_are_rejected(source: AsyncInventorySource) -> None:
    async with httpx.AsyncClient(base_url=source.base_url, timeout=_CLIENT_TIMEOUT) as client:
        for limit in ("0", "-3", "abc", ""):
            response = await client.get("/v1/inventory", params={"limit": limit})
            assert response.status_code == 400
            assert response.json()["error"]["code"] == "invalid_limit"


async def test_repeated_query_parameters_are_rejected(source: AsyncInventorySource) -> None:
    async with httpx.AsyncClient(base_url=source.base_url, timeout=_CLIENT_TIMEOUT) as client:
        response = await client.get(
            "/v1/inventory?urlparams", params=[("limit", "5"), ("limit", "6")]
        )
    assert response.status_code == 400


@pytest.mark.parametrize(
    "cursor",
    ["garbage", "!!!", "pg9:0000000005", _malformed_cursor(99_999_999_999), "cGcxOi0wMDAwMDAwMDA1"],
)
async def test_invalid_cursors_are_rejected(source: AsyncInventorySource, cursor: str) -> None:
    async with httpx.AsyncClient(base_url=source.base_url, timeout=_CLIENT_TIMEOUT) as client:
        response = await client.get("/v1/inventory", params={"cursor": cursor})
    assert response.status_code == 400
    assert response.json()["error"]["code"] == "invalid_cursor"


async def test_end_position_cursor_yields_an_empty_terminal_page(
    source: AsyncInventorySource,
) -> None:
    async with httpx.AsyncClient(base_url=source.base_url, timeout=_CLIENT_TIMEOUT) as client:
        first = await client.get("/v1/inventory", params={"limit": str(len(_DATASET.rows))})
        terminal_cursor = first.json()["next_cursor"]
        assert terminal_cursor == ""
        end = await client.get(
            "/v1/inventory", params={"cursor": _encode_position(len(_DATASET.rows))}
        )
    assert end.status_code == 200
    assert end.json()["records"] == []
    assert end.json()["next_cursor"] == ""


async def test_unknown_paths_and_methods_are_rejected(source: AsyncInventorySource) -> None:
    async with httpx.AsyncClient(base_url=source.base_url, timeout=_CLIENT_TIMEOUT) as client:
        missing = await client.get("/v1/unknown")
        assert missing.status_code == 404
        posted = await client.post("/v1/inventory")
        assert posted.status_code == 405
        assert posted.headers["allow"] == "GET"


async def test_keep_alive_connection_serves_multiple_requests(source: AsyncInventorySource) -> None:
    transport = httpx.AsyncHTTPTransport(retries=0)
    async with httpx.AsyncClient(
        base_url=source.base_url, timeout=_CLIENT_TIMEOUT, transport=transport
    ) as client:
        for _ in range(4):
            response = await client.get("/healthz")
            assert response.status_code == 200


async def test_responses_are_byte_deterministic_across_instances() -> None:
    payloads: list[bytes] = []
    for _ in range(2):
        simulator = AsyncInventorySource(_DATASET, FailureScript.empty())
        await simulator.start()
        try:
            async with httpx.AsyncClient(
                base_url=simulator.base_url, timeout=_CLIENT_TIMEOUT
            ) as client:
                response = await client.get("/v1/inventory", params={"limit": "6"})
                payloads.append(response.content)
        finally:
            await simulator.aclose()
    assert payloads[0] == payloads[1]


async def test_rate_limit_failure_returns_429_with_retry_after() -> None:
    script = _script(
        ScriptedFailure(sequence=1, kind=ScriptedFailureKind.RATE_LIMIT, retry_after_seconds=7)
    )
    simulator = AsyncInventorySource(_DATASET, script)
    await simulator.start()
    try:
        async with httpx.AsyncClient(
            base_url=simulator.base_url, timeout=_CLIENT_TIMEOUT
        ) as client:
            limited = await client.get("/v1/inventory")
            assert limited.status_code == 429
            assert limited.headers["retry-after"] == "7"
            assert limited.json()["error"]["code"] == "rate_limited"
            recovered = await client.get("/v1/inventory")
            assert recovered.status_code == 200
        applied = simulator.applied_failures()
        assert len(applied) == 1
        assert applied[0].sequence == 1
        assert applied[0].kind is ScriptedFailureKind.RATE_LIMIT
    finally:
        await simulator.aclose()


async def test_transient_failure_returns_503_then_recovers() -> None:
    script = _script(ScriptedFailure(sequence=2, kind=ScriptedFailureKind.TRANSIENT_ERROR))
    simulator = AsyncInventorySource(_DATASET, script)
    await simulator.start()
    try:
        async with httpx.AsyncClient(
            base_url=simulator.base_url, timeout=_CLIENT_TIMEOUT
        ) as client:
            first = await client.get("/v1/inventory")
            assert first.status_code == 200
            second = await client.get("/v1/inventory")
            assert second.status_code == 503
            assert second.json()["error"]["code"] == "transient"
            third = await client.get("/v1/inventory")
            assert third.status_code == 200
    finally:
        await simulator.aclose()


async def test_timeout_failure_delays_beyond_a_small_client_deadline() -> None:
    script = _script(
        ScriptedFailure(sequence=1, kind=ScriptedFailureKind.TIMEOUT, delay_microseconds=400_000)
    )
    simulator = AsyncInventorySource(_DATASET, script)
    await simulator.start()
    try:
        timing = httpx.Timeout(0.05)
        async with httpx.AsyncClient(base_url=simulator.base_url, timeout=timing) as client:
            with pytest.raises(httpx.ReadTimeout):
                await client.get("/v1/inventory")
        async with httpx.AsyncClient(
            base_url=simulator.base_url, timeout=_CLIENT_TIMEOUT
        ) as client:
            recovered = await client.get("/v1/inventory")
            assert recovered.status_code == 200
        assert simulator.applied_failures()[0].kind is ScriptedFailureKind.TIMEOUT
    finally:
        await simulator.aclose()


async def test_malformed_response_returns_invalid_json_body() -> None:
    script = _script(ScriptedFailure(sequence=1, kind=ScriptedFailureKind.MALFORMED_RESPONSE))
    simulator = AsyncInventorySource(_DATASET, script)
    await simulator.start()
    try:
        async with httpx.AsyncClient(
            base_url=simulator.base_url, timeout=_CLIENT_TIMEOUT
        ) as client:
            response = await client.get("/v1/inventory")
            assert response.status_code == 200
            assert response.headers["content-type"].startswith("application/json")
            with pytest.raises(json.JSONDecodeError):
                response.json()
    finally:
        await simulator.aclose()


async def test_duplicate_records_failure_stays_within_the_requested_page_limit() -> None:
    script = _script(ScriptedFailure(sequence=1, kind=ScriptedFailureKind.DUPLICATE_RECORDS))
    simulator = AsyncInventorySource(_DATASET, script)
    await simulator.start()
    try:
        async with httpx.AsyncClient(
            base_url=simulator.base_url, timeout=_CLIENT_TIMEOUT
        ) as client:
            response = await client.get("/v1/inventory", params={"limit": "4"})
            records = response.json()["records"]
        assert len(records) == 4
        assert records[0] == records[1]
        assert records[2] == records[3]
    finally:
        await simulator.aclose()


async def test_duplicate_records_failure_respects_the_maximum_page_limit() -> None:
    script = _script(ScriptedFailure(sequence=1, kind=ScriptedFailureKind.DUPLICATE_RECORDS))
    large_dataset = generate_dataset(
        ScenarioSeed(102),
        ScenarioVersion(1),
        DatasetProfile(record_count=201, malformed_count=0, boundary_count=0, duplicate_count=0),
    )
    simulator = AsyncInventorySource(large_dataset, script)
    await simulator.start()
    try:
        async with httpx.AsyncClient(
            base_url=simulator.base_url, timeout=_CLIENT_TIMEOUT
        ) as client:
            response = await client.get("/v1/inventory", params={"limit": "10000"})
        assert response.status_code == 200
        assert len(response.json()["records"]) == DEFAULT_MAX_PAGE_SIZE
    finally:
        await simulator.aclose()


async def test_connection_loss_aborts_mid_response() -> None:
    script = _script(
        ScriptedFailure(sequence=1, kind=ScriptedFailureKind.CONNECTION_LOSS, partial_bytes=40)
    )
    simulator = AsyncInventorySource(_DATASET, script)
    await simulator.start()
    try:
        async with httpx.AsyncClient(
            base_url=simulator.base_url, timeout=_CLIENT_TIMEOUT
        ) as client:
            with pytest.raises(httpx.TransportError):
                await client.get("/v1/inventory")
        async with httpx.AsyncClient(
            base_url=simulator.base_url, timeout=_CLIENT_TIMEOUT
        ) as client:
            recovered = await client.get("/v1/inventory")
            assert recovered.status_code == 200
    finally:
        await simulator.aclose()


async def test_hang_failure_is_cancellable_and_leaves_the_server_healthy() -> None:
    script = _script(
        ScriptedFailure(sequence=1, kind=ScriptedFailureKind.HANG, delay_microseconds=5_000_000)
    )
    simulator = AsyncInventorySource(_DATASET, script)
    await simulator.start()
    try:
        async with httpx.AsyncClient(
            base_url=simulator.base_url, timeout=_CLIENT_TIMEOUT
        ) as client:
            with pytest.raises(TimeoutError):
                await asyncio.wait_for(client.get("/v1/inventory"), timeout=0.1)
            healthy = await client.get("/healthz")
            assert healthy.status_code == 200
            second = await asyncio.wait_for(client.get("/v1/inventory"), timeout=5.0)
            assert second.status_code == 200
        assert simulator.applied_failures()[0].kind is ScriptedFailureKind.HANG
    finally:
        await simulator.aclose()


async def test_cancelled_request_releases_the_connection() -> None:
    script = _script(
        ScriptedFailure(sequence=1, kind=ScriptedFailureKind.HANG, delay_microseconds=5_000_000)
    )
    simulator = AsyncInventorySource(_DATASET, script)
    await simulator.start()
    try:
        task = asyncio.create_task(_request_hang(simulator.base_url))
        await asyncio.sleep(0.05)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        async with httpx.AsyncClient(
            base_url=simulator.base_url, timeout=_CLIENT_TIMEOUT
        ) as client:
            healthy = await client.get("/healthz")
            assert healthy.status_code == 200
    finally:
        await simulator.aclose()


@pytest.mark.parametrize(
    ("script", "request_latency_microseconds"),
    [
        (
            _script(
                ScriptedFailure(
                    sequence=1,
                    kind=ScriptedFailureKind.HANG,
                    delay_microseconds=5_000_000,
                )
            ),
            0,
        ),
        (FailureScript.empty(), 5_000_000),
    ],
)
async def test_aclose_interrupts_active_hang_and_delayed_handlers(
    script: FailureScript, request_latency_microseconds: int
) -> None:
    simulator = AsyncInventorySource(
        _DATASET,
        script,
        request_latency_microseconds=request_latency_microseconds,
    )
    await simulator.start()
    reader, writer = await asyncio.open_connection("127.0.0.1", simulator.port)
    try:
        writer.write(b"GET /v1/inventory HTTP/1.1\r\nHost: localhost\r\n\r\n")
        await writer.drain()
        await _wait_for_handled_request(simulator)
        started = asyncio.get_running_loop().time()
        await asyncio.wait_for(simulator.aclose(), timeout=0.5)
        assert asyncio.get_running_loop().time() - started < 0.5
        assert await asyncio.wait_for(reader.read(), timeout=0.5) == b""
    finally:
        writer.close()
        with contextlib.suppress(ConnectionError, OSError):
            await writer.wait_closed()
        await simulator.aclose()


async def _request_hang(base_url: str) -> None:
    timing = httpx.Timeout(30.0)
    async with httpx.AsyncClient(base_url=base_url, timeout=timing) as client:
        await client.get("/v1/inventory")


async def _wait_for_handled_request(simulator: AsyncInventorySource) -> None:
    for _ in range(50):
        if simulator.request_count() == 1:
            return
        await asyncio.sleep(0.01)
    pytest.fail("the simulator did not begin handling the active request")


async def test_applied_failure_sequence_is_deterministic() -> None:
    script = _script(
        ScriptedFailure(sequence=1, kind=ScriptedFailureKind.RATE_LIMIT, retry_after_seconds=1),
        ScriptedFailure(sequence=3, kind=ScriptedFailureKind.TRANSIENT_ERROR),
    )
    observed: list[tuple[tuple[int, str], ...]] = []
    for _ in range(2):
        simulator = AsyncInventorySource(_DATASET, script)
        await simulator.start()
        try:
            async with httpx.AsyncClient(
                base_url=simulator.base_url, timeout=_CLIENT_TIMEOUT
            ) as client:
                for expected_status in (429, 200, 503, 200):
                    response = await client.get("/v1/inventory")
                    assert response.status_code == expected_status
            observed.append(
                tuple([(item.sequence, item.kind.value) for item in simulator.applied_failures()])
            )
        finally:
            await simulator.aclose()
    assert observed[0] == observed[1] == ((1, "rate_limit"), (3, "transient_error"))


async def test_oversized_request_head_is_refused(source: AsyncInventorySource) -> None:
    reader, writer = await asyncio.open_connection("127.0.0.1", source.port)
    try:
        writer.write(
            b"GET /v1/inventory HTTP/1.1\r\nHost: x\r\nX-Oversized: " + b"a" * 20_000 + b"\r\n\r\n"
        )
        await writer.drain()
        raw = await asyncio.wait_for(reader.read(-1), timeout=5.0)
    finally:
        writer.close()
    assert raw.startswith(b"HTTP/1.1 431")


async def test_source_rejects_transfer_encoding_requests(source: AsyncInventorySource) -> None:
    reader, writer = await asyncio.open_connection("127.0.0.1", source.port)
    try:
        writer.write(b"PUT /v1/inventory HTTP/1.1\r\nHost: x\r\nTransfer-Encoding: chunked\r\n\r\n")
        await writer.drain()
        raw = await asyncio.wait_for(reader.read(-1), timeout=5.0)
    finally:
        writer.close()
    assert raw.startswith(b"HTTP/1.1 400")


async def test_malformed_request_line_is_refused(source: AsyncInventorySource) -> None:
    reader, writer = await asyncio.open_connection("127.0.0.1", source.port)
    try:
        writer.write(b"NONSENSE\r\n\r\n")
        await writer.drain()
        raw = await asyncio.wait_for(reader.read(-1), timeout=5.0)
    finally:
        writer.close()
    assert raw.startswith(b"HTTP/1.1 400")


async def test_double_start_and_use_after_close_are_refused() -> None:
    simulator = AsyncInventorySource(_DATASET, FailureScript.empty())
    await simulator.start()
    with pytest.raises(RuntimeError, match="already started"):
        await simulator.start()
    await simulator.aclose()
    await simulator.aclose()
    assert simulator.is_serving() is False
    with pytest.raises(RuntimeError, match="has not started"):
        _ = simulator.port


@hypothesis_settings(max_examples=25, deadline=None)
@given(limit=st.integers(min_value=1, max_value=64), offset=st.integers(min_value=0, max_value=20))
async def test_paging_windows_never_overlap_or_skip_rows(limit: int, offset: int) -> None:
    simulator = AsyncInventorySource(_DATASET, FailureScript.empty())
    await simulator.start()
    try:
        async with httpx.AsyncClient(
            base_url=simulator.base_url, timeout=_CLIENT_TIMEOUT
        ) as client:
            cursor = _encode_position(offset) if offset else ""
            params: dict[str, str] = {"limit": str(limit)}
            if cursor:
                params["cursor"] = cursor
            response = await client.get("/v1/inventory", params=params)
            document = response.json()
            expected = [dict(row.payload) for row in _DATASET.rows[offset : offset + limit]]
            assert document["records"] == expected
    finally:
        await simulator.aclose()


def test_max_page_size_setting_is_bounded() -> None:
    from paritygrid.demo.simulators.source_behavior import SourceError, SourceSettings

    with pytest.raises(SourceError):
        SourceSettings(max_page_size=DEFAULT_MAX_PAGE_SIZE + 1)
    with pytest.raises(SourceError):
        SourceSettings(max_page_size=0)

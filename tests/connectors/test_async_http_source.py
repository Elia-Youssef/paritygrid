"""Async HTTP source connector integration against the Phase 8 simulator."""

import asyncio

import pytest

from paritygrid.adapters.connectors import (
    AsyncHttpSourceConfig,
    AsyncHttpSourceConnector,
    ConnectorAuthentication,
    ConnectorCallBounds,
    ConnectorCallContext,
    ConnectorCancelledError,
    ConnectorEvent,
    ConnectorEventKind,
    ConnectorKind,
    ConnectorLifecycleError,
    ConnectorPermanentError,
    ConnectorRateLimitedError,
    ConnectorRetryableError,
    ConnectorState,
    ConnectorTimeoutError,
    ConnectorUnknownError,
    EventCancellationToken,
    SourceRecord,
)
from paritygrid.adapters.connectors.redaction import SecretMaterial
from paritygrid.demo.datasets import (
    DatasetProfile,
    ScenarioSeed,
    ScenarioVersion,
    SyntheticDataset,
    generate_dataset,
)
from paritygrid.demo.failures import (
    FailureScript,
    ScriptedFailure,
    ScriptedFailureKind,
)
from paritygrid.demo.simulators.async_source import AsyncInventorySource

pytestmark = pytest.mark.anyio

_FAST = ConnectorCallBounds(request_timeout_microseconds=2_000_000)
_PAGED = ConnectorCallBounds(max_page_records=6, request_timeout_microseconds=2_000_000)


def _script(*failures: ScriptedFailure) -> FailureScript:
    return FailureScript.from_entries(failures)


def _context() -> ConnectorCallContext:
    return ConnectorCallContext(correlation_id="test-call-1")


async def _start_source(dataset: SyntheticDataset, script: FailureScript) -> AsyncInventorySource:
    source = AsyncInventorySource(dataset, script)
    await source.start()
    return source


async def _open(base_url: str, bounds: ConnectorCallBounds = _FAST) -> AsyncHttpSourceConnector:
    connector = AsyncHttpSourceConnector(AsyncHttpSourceConfig(base_url, bounds=bounds))
    await connector.open_async()
    return connector


async def _collect(
    connector: AsyncHttpSourceConnector, bounds: ConnectorCallBounds
) -> tuple[list[SourceRecord], int]:
    records: list[SourceRecord] = []
    cursor: str | None = None
    requests = 0
    while True:
        page = await connector.read_page_async(cursor, _context())
        records.extend(page.records)
        requests += page.request_count
        assert len(page.records) <= bounds.max_page_records
        if page.next_cursor is None:
            return records, requests
        cursor = page.next_cursor


class TestPagination:
    async def test_reads_every_record_in_order(
        self, connector_dataset: SyntheticDataset, async_source: AsyncInventorySource
    ) -> None:
        connector = await _open(async_source.base_url, _PAGED)
        try:
            records, requests = await _collect(connector, _PAGED)
        finally:
            await connector.aclose()
        assert len(records) == len(connector_dataset.rows)
        assert requests == 4  # 24 rows over pages of six
        positions = [record.position for record in records]
        assert positions == sorted(positions)

    async def test_cursor_continuation_resumes_at_the_next_record(
        self, async_source: AsyncInventorySource
    ) -> None:
        connector = await _open(async_source.base_url, _PAGED)
        try:
            first = await connector.read_page_async(None, _context())
            assert first.next_cursor is not None
            resumed = await connector.read_page_async(first.next_cursor, _context())
        finally:
            await connector.aclose()
        assert len(first.records) == 6
        assert resumed.records[0].position == 6

    async def test_empty_source_yields_one_empty_exhausted_page(self) -> None:
        empty = generate_dataset(
            ScenarioSeed(7),
            ScenarioVersion(1),
            DatasetProfile(record_count=0, malformed_count=0, boundary_count=0, duplicate_count=0),
        )
        source = await _start_source(empty, FailureScript.empty())
        try:
            connector = await _open(source.base_url)
            try:
                page = await connector.read_page_async(None, _context())
                assert page.records == ()
                assert page.exhausted
            finally:
                await connector.aclose()
        finally:
            await source.aclose()

    async def test_page_size_bound_is_respected(self, async_source: AsyncInventorySource) -> None:
        connector = await _open(async_source.base_url, _PAGED)
        try:
            page = await connector.read_page_async(None, _context())
            assert len(page.records) == 6
        finally:
            await connector.aclose()


class TestFailureClassification:
    async def test_rate_limit_is_retryable_with_server_delay(
        self, connector_dataset: SyntheticDataset
    ) -> None:
        source = await _start_source(
            connector_dataset,
            _script(
                ScriptedFailure(
                    sequence=1, kind=ScriptedFailureKind.RATE_LIMIT, retry_after_seconds=4
                )
            ),
        )
        try:
            connector = await _open(source.base_url)
            try:
                with pytest.raises(ConnectorRateLimitedError) as details:
                    await connector.read_page_async(None, _context())
                assert details.value.retry_after_seconds == 4
                assert details.value.disposition.value == "retry"
                page = await connector.read_page_async(None, _context())
                assert page.records
            finally:
                await connector.aclose()
        finally:
            await source.aclose()

    async def test_transient_error_is_retryable(self, connector_dataset: SyntheticDataset) -> None:
        source = await _start_source(
            connector_dataset,
            _script(ScriptedFailure(sequence=1, kind=ScriptedFailureKind.TRANSIENT_ERROR)),
        )
        try:
            connector = await _open(source.base_url)
            try:
                with pytest.raises(ConnectorRetryableError):
                    await connector.read_page_async(None, _context())
                page = await connector.read_page_async(None, _context())
                assert page.records
            finally:
                await connector.aclose()
        finally:
            await source.aclose()

    async def test_server_delay_beyond_deadline_is_a_timeout(
        self, connector_dataset: SyntheticDataset
    ) -> None:
        source = await _start_source(
            connector_dataset,
            _script(
                ScriptedFailure(
                    sequence=1, kind=ScriptedFailureKind.TIMEOUT, delay_microseconds=400_000
                )
            ),
        )
        try:
            connector = await _open(
                source.base_url, ConnectorCallBounds(request_timeout_microseconds=100_000)
            )
            try:
                with pytest.raises(ConnectorTimeoutError):
                    await connector.read_page_async(None, _context())
                page = await connector.read_page_async(None, _context())
                assert page.records
            finally:
                await connector.aclose()
        finally:
            await source.aclose()

    async def test_connection_loss_is_retryable(self, connector_dataset: SyntheticDataset) -> None:
        source = await _start_source(
            connector_dataset,
            _script(
                ScriptedFailure(
                    sequence=1, kind=ScriptedFailureKind.CONNECTION_LOSS, partial_bytes=32
                )
            ),
        )
        try:
            connector = await _open(source.base_url)
            try:
                with pytest.raises(ConnectorRetryableError):
                    await connector.read_page_async(None, _context())
                page = await connector.read_page_async(None, _context())
                assert page.records
            finally:
                await connector.aclose()
        finally:
            await source.aclose()

    async def test_malformed_success_body_is_fail_closed_unknown(
        self, connector_dataset: SyntheticDataset
    ) -> None:
        source = await _start_source(
            connector_dataset,
            _script(ScriptedFailure(sequence=1, kind=ScriptedFailureKind.MALFORMED_RESPONSE)),
        )
        try:
            connector = await _open(source.base_url)
            try:
                with pytest.raises(ConnectorUnknownError) as details:
                    await connector.read_page_async(None, _context())
                assert details.value.disposition.value == "permanent"
            finally:
                await connector.aclose()
        finally:
            await source.aclose()

    async def test_invalid_cursor_is_permanent(self, async_source: AsyncInventorySource) -> None:
        connector = await _open(async_source.base_url)
        try:
            with pytest.raises(ConnectorPermanentError):
                await connector.read_page_async("not-a-valid-cursor", _context())
        finally:
            await connector.aclose()

    async def test_connect_failure_to_dead_port_is_retryable(self) -> None:
        connector = await _open("http://127.0.0.1:9")
        try:
            with pytest.raises(ConnectorRetryableError):
                await connector.read_page_async(None, _context())
        finally:
            await connector.aclose()

    async def test_response_above_the_size_bound_is_fail_closed(
        self, async_source: AsyncInventorySource
    ) -> None:
        bounds = ConnectorCallBounds(max_response_bytes=16, request_timeout_microseconds=2_000_000)
        connector = await _open(async_source.base_url, bounds)
        try:
            with pytest.raises(ConnectorUnknownError) as details:
                await connector.read_page_async(None, _context())
            assert details.value.disposition.value == "permanent"
        finally:
            await connector.aclose()


class TestDuplicates:
    async def test_duplicate_records_pass_through_untouched(
        self, connector_dataset: SyntheticDataset
    ) -> None:
        source = await _start_source(
            connector_dataset,
            _script(ScriptedFailure(sequence=1, kind=ScriptedFailureKind.DUPLICATE_RECORDS)),
        )
        try:
            connector = await _open(source.base_url)
            try:
                page = await connector.read_page_async(None, _context())
                assert len(page.records) == 2 * len(connector_dataset.rows)
                payloads = [dict(r.payload) for r in page.records if r.payload is not None]
                for even in range(0, len(payloads) - 1, 2):
                    assert payloads[even] == payloads[even + 1]
            finally:
                await connector.aclose()
        finally:
            await source.aclose()


class TestCancellation:
    async def test_pre_cancelled_token_fails_before_io(
        self, async_source: AsyncInventorySource
    ) -> None:
        connector = await _open(async_source.base_url)
        try:
            token = EventCancellationToken()
            token.cancel()
            context = ConnectorCallContext(cancellation_token=token)
            with pytest.raises(ConnectorCancelledError):
                await connector.read_page_async(None, context)
            assert async_source.request_count() == 0
        finally:
            await connector.aclose()

    async def test_task_cancellation_mid_call_propagates_and_releases(
        self, connector_dataset: SyntheticDataset
    ) -> None:
        source = await _start_source(
            connector_dataset,
            _script(
                ScriptedFailure(
                    sequence=1, kind=ScriptedFailureKind.HANG, delay_microseconds=5_000_000
                )
            ),
        )
        try:
            connector = await _open(source.base_url)
            task = asyncio.create_task(connector.read_page_async(None, _context()))
            await asyncio.sleep(0.05)
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
            page = await connector.read_page_async(None, _context())
            assert page.records
            await connector.aclose()
            assert connector.state() is ConnectorState.CLOSED
        finally:
            await source.aclose()


class TestLoopFreedom:
    async def test_pending_connector_call_does_not_block_the_loop(
        self, connector_dataset: SyntheticDataset
    ) -> None:
        source = await _start_source(
            connector_dataset,
            _script(
                ScriptedFailure(
                    sequence=1, kind=ScriptedFailureKind.TIMEOUT, delay_microseconds=600_000
                )
            ),
        )
        try:
            connector = await _open(
                source.base_url, ConnectorCallBounds(request_timeout_microseconds=5_000_000)
            )
            try:
                ticks: list[int] = []

                async def ticker() -> None:
                    for _ in range(6):
                        ticks.append(1)
                        await asyncio.sleep(0.02)

                read = asyncio.create_task(connector.read_page_async(None, _context()))
                ticker_task = asyncio.create_task(ticker())
                await asyncio.wait_for(ticker_task, timeout=1.0)
                # The ticker finished while the connector call was still
                # pending: the event loop stayed responsive throughout.
                assert not read.done()
                page = await read
                assert page.records
                assert len(ticks) == 6
            finally:
                await connector.aclose()
        finally:
            await source.aclose()


class TestLifecycleAndCleanup:
    async def test_lifecycle_transitions_and_errors(
        self, async_source: AsyncInventorySource
    ) -> None:
        connector = AsyncHttpSourceConnector(AsyncHttpSourceConfig(async_source.base_url))
        assert connector.state() is ConnectorState.CREATED
        with pytest.raises(ConnectorLifecycleError):
            await connector.read_page_async(None, _context())
        await connector.open_async()
        assert connector.state() is ConnectorState.OPEN
        with pytest.raises(ConnectorLifecycleError):
            await connector.open_async()
        await connector.aclose()
        assert connector.state() is ConnectorState.CLOSED
        with pytest.raises(ConnectorLifecycleError):
            await connector.read_page_async(None, _context())
        await connector.aclose()

    async def test_close_after_failure_leaves_connector_closed(
        self, connector_dataset: SyntheticDataset
    ) -> None:
        source = await _start_source(
            connector_dataset,
            _script(ScriptedFailure(sequence=1, kind=ScriptedFailureKind.TRANSIENT_ERROR)),
        )
        try:
            connector = await _open(source.base_url)
            with pytest.raises(ConnectorRetryableError):
                await connector.read_page_async(None, _context())
            await connector.aclose()
            with pytest.raises(ConnectorLifecycleError):
                await connector.read_page_async(None, _context())
        finally:
            await source.aclose()


class TestObservabilityAndRedaction:
    async def test_call_context_secrets_are_redacted_from_status_bodies(
        self, connector_dataset: SyntheticDataset
    ) -> None:
        secret = "The source is temporarily unavailable."
        source = await _start_source(
            connector_dataset,
            _script(ScriptedFailure(sequence=1, kind=ScriptedFailureKind.TRANSIENT_ERROR)),
        )
        try:
            connector = await _open(source.base_url)
            context = ConnectorCallContext(secrets=SecretMaterial((secret,)))
            with pytest.raises(ConnectorRetryableError) as details:
                await connector.read_page_async(None, context)
            assert secret not in details.value.detail
            await connector.aclose()
        finally:
            await source.aclose()

    async def test_event_stream_covers_lifecycle_and_calls(
        self, async_source: AsyncInventorySource
    ) -> None:
        events: list[ConnectorEvent] = []
        connector = AsyncHttpSourceConnector(
            AsyncHttpSourceConfig(async_source.base_url, bounds=_FAST),
            observers=[events.append],
        )
        await connector.open_async()
        await connector.read_page_async(None, _context())
        await connector.aclose()
        kinds = [event.kind for event in events]
        assert ConnectorEventKind.OPENED in kinds
        assert ConnectorEventKind.CALL_STARTED in kinds
        assert ConnectorEventKind.PAGE_COMPLETED in kinds
        assert ConnectorEventKind.CLOSED in kinds
        assert all(event.connector_kind is ConnectorKind.ASYNC_HTTP_SOURCE for event in events)

    async def test_failure_events_carry_classification_without_secrets(
        self, connector_dataset: SyntheticDataset
    ) -> None:
        token = "tok_live_9f8e7d6c"
        events: list[ConnectorEvent] = []
        source = await _start_source(
            connector_dataset,
            _script(ScriptedFailure(sequence=1, kind=ScriptedFailureKind.TRANSIENT_ERROR)),
        )
        try:
            connector = AsyncHttpSourceConnector(
                AsyncHttpSourceConfig(source.base_url, bounds=_FAST),
                authentication=ConnectorAuthentication(token=token),
                observers=[events.append],
            )
            await connector.open_async()
            with pytest.raises(ConnectorRetryableError):
                await connector.read_page_async(None, _context())
            await connector.aclose()
        finally:
            await source.aclose()
        rendered = repr(events) + str(events)
        assert token not in rendered
        failed = [event for event in events if event.kind is ConnectorEventKind.CALL_FAILED]
        assert failed
        assert failed[0].details["classification"] == "http_5xx"

    async def test_authorization_secret_never_reaches_errors_or_events(
        self, connector_dataset: SyntheticDataset
    ) -> None:
        token = "tok_live_9f8e7d6c"
        source = await _start_source(
            connector_dataset,
            _script(
                ScriptedFailure(
                    sequence=1, kind=ScriptedFailureKind.CONNECTION_LOSS, partial_bytes=8
                )
            ),
        )
        try:
            events: list[ConnectorEvent] = []
            connector = AsyncHttpSourceConnector(
                AsyncHttpSourceConfig(source.base_url, bounds=_FAST),
                authentication=ConnectorAuthentication(token=token),
                observers=[events.append],
            )
            await connector.open_async()
            with pytest.raises(ConnectorRetryableError) as details:
                await connector.read_page_async(None, _context())
            await connector.aclose()
            assert token not in details.value.detail
            assert token not in str(details.value)
            assert token not in repr(details.value)
            assert token not in repr(events)
        finally:
            await source.aclose()

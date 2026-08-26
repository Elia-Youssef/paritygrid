"""Blocking HTTP source connector integration against the Phase 8 simulator.

The blocking adapter is exercised from worker threads (the established
blocking boundary) and proves it refuses to run on an active event-loop
thread, mirroring how the legacy systems it models must be isolated.
Each synchronous test owns its simulator lifecycle directly because the
async fixtures serve coroutine tests only.
"""

import asyncio
import threading
import time
from collections.abc import Generator
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager

import pytest

from paritygrid.adapters.connectors import (
    BlockingHttpSourceConfig,
    BlockingHttpSourceConnector,
    ConnectorCallBounds,
    ConnectorCallContext,
    ConnectorCancelledError,
    ConnectorLifecycleError,
    ConnectorLoopError,
    ConnectorPermanentError,
    ConnectorRateLimitedError,
    ConnectorRetryableError,
    ConnectorState,
    ConnectorTimeoutError,
    ConnectorValidationError,
    EventCancellationToken,
    SourceRecord,
    decode_page_cursor,
    encode_page_cursor,
)
from paritygrid.adapters.connectors.redaction import SecretMaterial
from paritygrid.demo.datasets import SyntheticDataset
from paritygrid.demo.failures import (
    FailureScript,
    ScriptedFailure,
    ScriptedFailureKind,
)
from paritygrid.demo.simulators.blocking_source import BlockingInventorySource

pytestmark = pytest.mark.anyio

_FAST = ConnectorCallBounds(request_timeout_microseconds=2_000_000)
_PAGED = ConnectorCallBounds(max_page_records=5, request_timeout_microseconds=2_000_000)


def _script(*failures: ScriptedFailure) -> FailureScript:
    return FailureScript.from_entries(failures)


def _context() -> ConnectorCallContext:
    return ConnectorCallContext(correlation_id="blocking-call-1")


def _close_source(source: BlockingInventorySource) -> None:
    """Close the source from any thread, loop-running or not."""
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        asyncio.run(source.aclose())
        return
    closer = threading.Thread(target=lambda: asyncio.run(source.aclose()))
    closer.start()
    closer.join(timeout=10)


@contextmanager
def _source(
    dataset: SyntheticDataset, script: FailureScript | None = None
) -> Generator[BlockingInventorySource]:
    source = BlockingInventorySource(
        dataset, script if script is not None else FailureScript.empty()
    )
    source.start()
    try:
        yield source
    finally:
        _close_source(source)


def _open(base_url: str, bounds: ConnectorCallBounds = _FAST) -> BlockingHttpSourceConnector:
    connector = BlockingHttpSourceConnector(BlockingHttpSourceConfig(base_url, bounds=bounds))
    connector.open()
    return connector


def _collect(connector: BlockingHttpSourceConnector) -> list[SourceRecord]:
    records: list[SourceRecord] = []
    cursor: str | None = None
    while True:
        page = connector.read_page(cursor, _context())
        records.extend(page.records)
        if page.next_cursor is None:
            return records
        cursor = page.next_cursor


class TestCursorCoding:
    def test_page_cursor_round_trip(self) -> None:
        assert decode_page_cursor(encode_page_cursor(12)) == 12
        assert decode_page_cursor(encode_page_cursor(1)) == 1

    @pytest.mark.parametrize("cursor", ["rows:3", "page:x", "page:", "page:0"])
    def test_foreign_or_invalid_cursors_are_rejected(self, cursor: str) -> None:
        with pytest.raises(ConnectorValidationError):
            decode_page_cursor(cursor)


class TestPagination:
    def test_reads_every_record_page_by_page(self, connector_dataset: SyntheticDataset) -> None:
        with _source(connector_dataset) as source:
            connector = _open(source.base_url, _PAGED)
            try:
                records = _collect(connector)
            finally:
                connector.close()
        assert len(records) == len(connector_dataset.rows)
        positions = [record.position for record in records]
        assert positions == sorted(positions)

    def test_cursor_continuation_resumes_the_next_page(
        self, connector_dataset: SyntheticDataset
    ) -> None:
        with _source(connector_dataset) as source:
            connector = _open(source.base_url, _PAGED)
            try:
                first = connector.read_page(None, _context())
                assert first.next_cursor is not None
                resumed = connector.read_page(first.next_cursor, _context())
            finally:
                connector.close()
        assert len(first.records) == 5
        assert resumed.records[0].position == 5

    def test_single_page_source_reports_exhaustion(
        self, connector_dataset: SyntheticDataset
    ) -> None:
        with _source(connector_dataset) as source:
            connector = _open(source.base_url)
            try:
                page = connector.read_page(None, _context())
                assert page.next_cursor is None
            finally:
                connector.close()


class TestFailureClassification:
    def test_call_context_secrets_are_redacted_from_status_bodies(
        self, connector_dataset: SyntheticDataset
    ) -> None:
        secret = "The source is temporarily unavailable."
        with _source(
            connector_dataset,
            _script(ScriptedFailure(sequence=1, kind=ScriptedFailureKind.TRANSIENT_ERROR)),
        ) as source:
            connector = _open(source.base_url)
            try:
                context = ConnectorCallContext(secrets=SecretMaterial((secret,)))
                with pytest.raises(ConnectorRetryableError) as details:
                    connector.read_page(None, context)
                assert secret not in details.value.detail
            finally:
                connector.close()

    def test_rate_limit_is_retryable_with_delay(self, connector_dataset: SyntheticDataset) -> None:
        with _source(
            connector_dataset,
            _script(
                ScriptedFailure(
                    sequence=1, kind=ScriptedFailureKind.RATE_LIMIT, retry_after_seconds=9
                )
            ),
        ) as source:
            connector = _open(source.base_url)
            try:
                with pytest.raises(ConnectorRateLimitedError) as details:
                    connector.read_page(None, _context())
                assert details.value.retry_after_seconds == 9
                page = connector.read_page(None, _context())
                assert page.records
            finally:
                connector.close()

    def test_transient_error_is_retryable(self, connector_dataset: SyntheticDataset) -> None:
        with _source(
            connector_dataset,
            _script(ScriptedFailure(sequence=1, kind=ScriptedFailureKind.TRANSIENT_ERROR)),
        ) as source:
            connector = _open(source.base_url)
            try:
                with pytest.raises(ConnectorRetryableError):
                    connector.read_page(None, _context())
                page = connector.read_page(None, _context())
                assert page.records
            finally:
                connector.close()

    def test_delay_beyond_deadline_is_a_timeout(self, connector_dataset: SyntheticDataset) -> None:
        with _source(
            connector_dataset,
            _script(
                ScriptedFailure(
                    sequence=1, kind=ScriptedFailureKind.TIMEOUT, delay_microseconds=400_000
                )
            ),
        ) as source:
            connector = _open(
                source.base_url, ConnectorCallBounds(request_timeout_microseconds=100_000)
            )
            try:
                with pytest.raises(ConnectorTimeoutError):
                    connector.read_page(None, _context())
                page = connector.read_page(None, _context())
                assert page.records
            finally:
                connector.close()

    def test_connection_loss_is_retryable(self, connector_dataset: SyntheticDataset) -> None:
        with _source(
            connector_dataset,
            _script(
                ScriptedFailure(
                    sequence=1, kind=ScriptedFailureKind.CONNECTION_LOSS, partial_bytes=32
                )
            ),
        ) as source:
            connector = _open(source.base_url)
            try:
                with pytest.raises(ConnectorRetryableError):
                    connector.read_page(None, _context())
                page = connector.read_page(None, _context())
                assert page.records
            finally:
                connector.close()

    def test_page_out_of_range_is_permanent(self, connector_dataset: SyntheticDataset) -> None:
        with _source(connector_dataset) as source:
            connector = _open(source.base_url)
            try:
                with pytest.raises(ConnectorPermanentError):
                    connector.read_page(encode_page_cursor(99), _context())
            finally:
                connector.close()

    def test_connect_failure_is_retryable(self) -> None:
        connector = _open("http://127.0.0.1:9")
        try:
            with pytest.raises(ConnectorRetryableError):
                connector.read_page(None, _context())
        finally:
            connector.close()


class TestCancellation:
    def test_pre_cancelled_token_fails_before_io(self, connector_dataset: SyntheticDataset) -> None:
        with _source(connector_dataset) as source:
            connector = _open(source.base_url)
            try:
                token = EventCancellationToken()
                token.cancel()
                context = ConnectorCallContext(cancellation_token=token)
                with pytest.raises(ConnectorCancelledError):
                    connector.read_page(None, context)
                assert source.request_count() == 0
            finally:
                connector.close()

    async def test_in_flight_cancellation_interrupts_blocking_io(
        self, connector_dataset: SyntheticDataset
    ) -> None:
        with _source(
            connector_dataset,
            _script(
                ScriptedFailure(
                    sequence=1,
                    kind=ScriptedFailureKind.HANG,
                    delay_microseconds=5_000_000,
                )
            ),
        ) as source:
            connector = await asyncio.to_thread(
                _open,
                source.base_url,
                ConnectorCallBounds(request_timeout_microseconds=5_000_000),
            )
            token = EventCancellationToken()
            context = ConnectorCallContext(cancellation_token=token)
            read = asyncio.create_task(asyncio.to_thread(connector.read_page, None, context))
            try:
                for _ in range(100):
                    if source.request_count() == 1:
                        break
                    await asyncio.sleep(0.01)
                assert source.request_count() == 1
                started = time.monotonic()
                token.cancel()
                with pytest.raises(ConnectorCancelledError):
                    await asyncio.wait_for(read, timeout=1.0)
                assert time.monotonic() - started < 0.75
            finally:
                await asyncio.to_thread(connector.close)


class TestLoopIsolation:
    async def test_open_refuses_an_active_event_loop(
        self, connector_dataset: SyntheticDataset
    ) -> None:
        with _source(connector_dataset) as source:
            connector = BlockingHttpSourceConnector(BlockingHttpSourceConfig(source.base_url))
            with pytest.raises(ConnectorLoopError):
                connector.open()

    async def test_read_refuses_an_active_event_loop(
        self, connector_dataset: SyntheticDataset
    ) -> None:
        with _source(connector_dataset) as source:
            connector = BlockingHttpSourceConnector(BlockingHttpSourceConfig(source.base_url))
            await asyncio.to_thread(connector.open)
            try:
                with pytest.raises(ConnectorLoopError):
                    connector.read_page(None, _context())
            finally:
                await asyncio.to_thread(connector.close)

    async def test_blocking_boundary_isolation_serves_the_connector(
        self, connector_dataset: SyntheticDataset
    ) -> None:
        with _source(connector_dataset) as source:
            executor = ThreadPoolExecutor(max_workers=2, thread_name_prefix="paritygrid-test")
            try:
                loop = asyncio.get_running_loop()
                connector = await loop.run_in_executor(executor, _open, source.base_url, _PAGED)
                ticks: list[int] = []

                async def ticker() -> None:
                    for _ in range(4):
                        ticks.append(1)
                        await asyncio.sleep(0.02)

                read = loop.run_in_executor(executor, _collect, connector)
                ticker_task = asyncio.create_task(ticker())
                await asyncio.wait_for(ticker_task, timeout=1.0)
                records = await asyncio.wait_for(read, timeout=10.0)
                assert len(records) == len(connector_dataset.rows)
                assert len(ticks) == 4
                await loop.run_in_executor(executor, connector.close)
            finally:
                executor.shutdown(wait=True)


class TestLifecycleAndCleanup:
    def test_lifecycle_transitions_and_errors(self, connector_dataset: SyntheticDataset) -> None:
        with _source(connector_dataset) as source:
            connector = BlockingHttpSourceConnector(BlockingHttpSourceConfig(source.base_url))
            assert connector.state() is ConnectorState.CREATED
            with pytest.raises(ConnectorLifecycleError):
                connector.read_page(None, _context())
            connector.open()
            assert connector.state() is ConnectorState.OPEN
            with pytest.raises(ConnectorLifecycleError):
                connector.open()
            connector.close()
            assert connector.state() is ConnectorState.CLOSED
            with pytest.raises(ConnectorLifecycleError):
                connector.read_page(None, _context())
            connector.close()

    def test_close_after_failure(self, connector_dataset: SyntheticDataset) -> None:
        with _source(
            connector_dataset,
            _script(ScriptedFailure(sequence=1, kind=ScriptedFailureKind.TRANSIENT_ERROR)),
        ) as source:
            connector = _open(source.base_url)
            with pytest.raises(ConnectorRetryableError):
                connector.read_page(None, _context())
            connector.close()
            assert connector.state() is ConnectorState.CLOSED

    def test_close_without_any_call_is_safe(self) -> None:
        connector = BlockingHttpSourceConnector(BlockingHttpSourceConfig("http://127.0.0.1:9"))
        connector.open()
        connector.close()
        assert connector.state() is ConnectorState.CLOSED

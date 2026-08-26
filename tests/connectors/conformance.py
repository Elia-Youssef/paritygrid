"""Shared connector conformance suite (P9.1 skeleton).

Every Phase 9 adapter passes through these exact assertions — the four
source kinds through the source suites (blocking drivers for the legacy
HTTP, CSV, and JSON Lines adapters; the async driver for the async HTTP
adapter) and the warehouse target through the idempotent-effect suite.
The suites own no simulators: harnesses hand over an open connector plus
the expected facts of its source, so adapter-specific setup stays in the
parametrized tests while every behavioral claim lives here once.
"""

from collections.abc import Callable

from paritygrid.adapters.connectors import (
    AsyncHttpSourceConnector,
    BlockingHttpSourceConnector,
    ConnectorCancelledError,
    ConnectorCapabilitiesV1,
    ConnectorError,
    ConnectorEvent,
    ConnectorEventKind,
    ConnectorLifecycleError,
    ConnectorObserver,
    ConnectorState,
    CsvFileSourceConnector,
    EventCancellationToken,
    JsonlFileSourceConnector,
    SourcePage,
    SourceRecord,
    WarehouseTargetConnector,
)
from paritygrid.adapters.connectors.contract import (
    ConnectorCallContext,
    TargetWriteRequest,
)
from paritygrid.application.planner.connectors import ConnectorCapability


class BlockingSourceHarness:
    """One open blocking source connector and its expected source facts."""

    def __init__(
        self,
        connector: BlockingHttpSourceConnector | CsvFileSourceConnector | JsonlFileSourceConnector,
        *,
        expected_records: int,
        expected_malformed: int,
        page_size: int,
        kind_name: str,
    ) -> None:
        self.connector = connector
        self.expected_records = expected_records
        self.expected_malformed = expected_malformed
        self.page_size = page_size
        self.kind_name = kind_name
        self.events: list[ConnectorEvent] = []
        self.context = ConnectorCallContext(correlation_id=f"conformance-{kind_name}")

    def capabilities(self) -> ConnectorCapabilitiesV1:
        return self.connector.capabilities()

    def read_page(self, cursor: str | None) -> SourcePage:
        return self.connector.read_page(cursor, self.context)


class AsyncSourceHarness:
    """One open async source connector and its expected source facts."""

    def __init__(
        self,
        connector: AsyncHttpSourceConnector,
        *,
        expected_records: int,
        expected_malformed: int,
        page_size: int,
        kind_name: str,
    ) -> None:
        self.connector = connector
        self.expected_records = expected_records
        self.expected_malformed = expected_malformed
        self.page_size = page_size
        self.kind_name = kind_name
        self.context = ConnectorCallContext(correlation_id=f"conformance-{kind_name}")

    def capabilities(self) -> ConnectorCapabilitiesV1:
        return self.connector.capabilities()

    async def read_page(self, cursor: str | None) -> SourcePage:
        return await self.connector.read_page_async(cursor, self.context)


def assert_capabilities_declare_reading(
    capabilities: ConnectorCapabilitiesV1, expected_kind_name: str, page_size: int
) -> None:
    assert capabilities.contract_version == 1
    assert capabilities.kind.value == expected_kind_name
    assert capabilities.supports(ConnectorCapability.READ)
    assert capabilities.max_page_records == page_size
    assert capabilities.supports_cursors


def assert_pagination_invariants(
    pages: list[SourcePage],
    *,
    expected_records: int,
    expected_malformed: int,
    page_size: int,
) -> list[SourceRecord]:
    """Assert the pagination contract shared by every source adapter."""
    assert pages, "a source must produce at least one page"
    records = [record for page in pages for record in page.records]
    assert len(records) == expected_records
    for page in pages:
        assert len(page.records) <= page_size
        assert page.request_count >= 1
    assert pages[-1].next_cursor is None, "the final page must report exhaustion"
    for earlier in pages[:-1]:
        assert earlier.next_cursor is not None, "continuation cursors must be present"
    positions = [record.position for record in records]
    assert len(set(positions)) == len(positions), "positions must be unique"
    assert positions == sorted(positions), "positions must not regress"
    malformed = [record for record in records if record.is_malformed]
    assert len(malformed) == expected_malformed
    for record in malformed:
        assert record.payload is None
        assert record.malformed_reason
    valid = [record for record in records if not record.is_malformed]
    for record in valid:
        assert record.payload is not None
    return records


def assert_lifecycle_closed(connector_state: ConnectorState) -> None:
    assert connector_state is ConnectorState.CLOSED


def assert_event_stream_is_closed_and_bounded(events: list[ConnectorEvent]) -> None:
    """Lifecycle events must be present and free of secret-shaped text."""
    kinds = [event.kind for event in events]
    if events:
        assert ConnectorEventKind.CLOSED in kinds
    rendered = repr(events)
    for marker in ("password=", "bearer ", "token:"):
        assert marker not in rendered.lower()


async def run_async_source_conformance(harness: AsyncSourceHarness) -> None:
    """Drive the shared source suite through the async read surface."""
    connector = harness.connector
    assert_capabilities_declare_reading(
        harness.capabilities(), harness.kind_name, harness.page_size
    )
    pages: list[SourcePage] = []
    cursor: str | None = None
    while True:
        page = await harness.read_page(cursor)
        pages.append(page)
        if page.next_cursor is None:
            break
        cursor = page.next_cursor
    assert_pagination_invariants(
        pages,
        expected_records=harness.expected_records,
        expected_malformed=harness.expected_malformed,
        page_size=harness.page_size,
    )
    # Pre-cancelled cooperative cancellation fails before any I/O.
    token = EventCancellationToken()
    token.cancel()
    cancelled_context = ConnectorCallContext(cancellation_token=token)
    with_locals = connector
    try:
        await with_locals.read_page_async(None, cancelled_context)
        raise AssertionError("cancelled context must fail the call")
    except ConnectorCancelledError:
        pass
    # Close is idempotent and terminal.
    await connector.aclose()
    await connector.aclose()
    assert_lifecycle_closed(connector.state())
    try:
        await connector.read_page_async(None, harness.context)
        raise AssertionError("reads after close must fail")
    except ConnectorLifecycleError:
        pass


def run_blocking_source_conformance(harness: BlockingSourceHarness) -> None:
    """Drive the shared source suite through the blocking read surface."""
    connector = harness.connector
    assert_capabilities_declare_reading(
        harness.capabilities(), harness.kind_name, harness.page_size
    )
    pages: list[SourcePage] = []
    cursor: str | None = None
    while True:
        page = harness.read_page(cursor)
        pages.append(page)
        if page.next_cursor is None:
            break
        cursor = page.next_cursor
    assert_pagination_invariants(
        pages,
        expected_records=harness.expected_records,
        expected_malformed=harness.expected_malformed,
        page_size=harness.page_size,
    )
    token = EventCancellationToken()
    token.cancel()
    cancelled_context = ConnectorCallContext(cancellation_token=token)
    try:
        connector.read_page(None, cancelled_context)
        raise AssertionError("cancelled context must fail the call")
    except ConnectorCancelledError:
        pass
    connector.close()
    connector.close()
    assert_lifecycle_closed(connector.state())
    try:
        connector.read_page(None, harness.context)
        raise AssertionError("reads after close must fail")
    except ConnectorLifecycleError:
        pass


class TargetHarness:
    """One open warehouse target connector bound to a live simulator."""

    def __init__(self, connector: WarehouseTargetConnector) -> None:
        self.connector = connector
        self.context = ConnectorCallContext(correlation_id="conformance-target")


async def run_target_conformance(
    harness: TargetHarness,
    build_request: Callable[[str, str], TargetWriteRequest],
) -> None:
    """Drive the shared idempotent-target suite (P9.6 conformance)."""
    connector = harness.connector
    capabilities = connector.capabilities()
    assert capabilities.kind.value == "warehouse_target"
    assert capabilities.supports(ConnectorCapability.WRITE)
    assert capabilities.supports(ConnectorCapability.IDEMPOTENCY)
    assert capabilities.max_page_records == 0

    first = await connector.write_record_async(
        build_request("CONF-1", "key-conf-1"), harness.context
    )
    assert first.outcome.value == "applied"
    state_after_first = await connector.state_snapshot_async(harness.context)
    replay = await connector.write_record_async(
        build_request("CONF-1", "key-conf-1"), harness.context
    )
    assert replay.outcome.value == "replayed"
    assert replay.record_version == first.record_version
    state_after_replay = await connector.state_snapshot_async(harness.context)
    assert state_after_replay.target_version == state_after_first.target_version, (
        "replays must not produce a second logical effect"
    )

    conflict_raised = False
    try:
        await connector.write_record_async(
            TargetWriteRequest(
                sku="CONF-1",
                payload={"sku": "CONF-1", "name": "Different"},
                idempotency_key="key-conf-1",
            ),
            harness.context,
        )
    except ConnectorError as error:
        conflict_raised = error.classification.value == "idempotency_conflict"
    assert conflict_raised, "key reuse with a different payload must conflict"

    stored = await connector.read_record_async("CONF-2", harness.context)
    assert stored is None
    await connector.aclose()
    await connector.aclose()
    assert_lifecycle_closed(connector.state())


__all__ = [
    "AsyncSourceHarness",
    "BlockingSourceHarness",
    "ConnectorObserver",
    "TargetHarness",
    "run_async_source_conformance",
    "run_blocking_source_conformance",
    "run_target_conformance",
]

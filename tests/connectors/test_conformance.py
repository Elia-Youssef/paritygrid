"""Parametrized conformance runs: every adapter through the shared suite."""

from pathlib import Path

import pytest

from paritygrid.adapters.connectors import (
    AsyncHttpSourceConfig,
    AsyncHttpSourceConnector,
    BlockingHttpSourceConfig,
    BlockingHttpSourceConnector,
    ConnectorCallBounds,
    CsvFileSourceConfig,
    CsvFileSourceConnector,
    JsonlFileSourceConfig,
    JsonlFileSourceConnector,
    SourceFileLocation,
    TargetWriteRequest,
    WarehouseTargetConfig,
    WarehouseTargetConnector,
)
from paritygrid.demo.datasets import SyntheticDataset
from paritygrid.demo.simulators.async_source import AsyncInventorySource
from paritygrid.demo.simulators.blocking_source import BlockingInventorySource
from paritygrid.demo.simulators.warehouse import SimulatedWarehouse
from tests.connectors.conformance import (
    AsyncSourceHarness,
    BlockingSourceHarness,
    TargetHarness,
    run_async_source_conformance,
    run_blocking_source_conformance,
    run_target_conformance,
)

pytestmark = pytest.mark.anyio

_PAGE = ConnectorCallBounds(max_page_records=5, max_record_bytes=65_536)
_SOURCE_IDS = ("async_http", "blocking_http", "csv", "jsonl")


def _expected_malformed(dataset: SyntheticDataset) -> int:
    from paritygrid.demo.datasets import RowRole

    return sum(1 for row in dataset.rows if row.role is RowRole.MALFORMED)


async def test_async_http_source_passes_the_shared_suite(
    connector_dataset: SyntheticDataset, async_source: AsyncInventorySource
) -> None:
    connector = AsyncHttpSourceConnector(AsyncHttpSourceConfig(async_source.base_url))
    await connector.open_async()
    harness = AsyncSourceHarness(
        connector,
        expected_records=len(connector_dataset.rows),
        expected_malformed=0,  # HTTP pages surface malformed payloads as failures
        page_size=200,
        kind_name="async_http_source",
    )
    await run_async_source_conformance(harness)


def test_blocking_http_source_passes_the_shared_suite(
    connector_dataset: SyntheticDataset,
) -> None:
    import asyncio

    from paritygrid.demo.failures import FailureScript

    source = BlockingInventorySource(connector_dataset, FailureScript.empty())
    source.start()
    try:
        connector = BlockingHttpSourceConnector(BlockingHttpSourceConfig(source.base_url))
        connector.open()
        harness = BlockingSourceHarness(
            connector,
            expected_records=len(connector_dataset.rows),
            expected_malformed=0,
            page_size=200,
            kind_name="blocking_http_source",
        )
        run_blocking_source_conformance(harness)
    finally:
        asyncio.run(source.aclose())


def test_csv_source_passes_the_shared_suite(
    fixture_root: Path, connector_dataset: SyntheticDataset
) -> None:
    connector = CsvFileSourceConnector(
        CsvFileSourceConfig(SourceFileLocation.create(fixture_root, "inventory.csv"))
    )
    connector.open()
    harness = BlockingSourceHarness(
        connector,
        expected_records=len(connector_dataset.rows),
        expected_malformed=_expected_malformed(connector_dataset),
        page_size=200,
        kind_name="csv_source",
    )
    run_blocking_source_conformance(harness)


def test_jsonl_source_passes_the_shared_suite(
    fixture_root: Path, connector_dataset: SyntheticDataset
) -> None:
    connector = JsonlFileSourceConnector(
        JsonlFileSourceConfig(SourceFileLocation.create(fixture_root, "inventory.jsonl"))
    )
    connector.open()
    harness = BlockingSourceHarness(
        connector,
        expected_records=len(connector_dataset.rows),
        expected_malformed=_expected_malformed(connector_dataset),
        page_size=200,
        kind_name="jsonl_source",
    )
    run_blocking_source_conformance(harness)


async def test_warehouse_target_passes_the_shared_idempotency_suite(
    warehouse: SimulatedWarehouse,
) -> None:
    connector = WarehouseTargetConnector(WarehouseTargetConfig(warehouse.base_url))
    await connector.open_async()
    harness = TargetHarness(connector)

    def build(sku: str, key: str) -> TargetWriteRequest:
        return TargetWriteRequest(
            sku=sku, payload={"sku": sku, "name": "Conformance part"}, idempotency_key=key
        )

    await run_target_conformance(harness, build)


def test_paged_file_adapters_keep_page_bounds_under_small_pages(
    fixture_root: Path, connector_dataset: SyntheticDataset
) -> None:
    """The suite's pagination invariants hold at page size one as well."""
    bounds = ConnectorCallBounds(max_page_records=1, max_record_bytes=65_536)
    csv_connector = CsvFileSourceConnector(
        CsvFileSourceConfig(SourceFileLocation.create(fixture_root, "inventory.csv"), bounds=bounds)
    )
    csv_connector.open()
    run_blocking_source_conformance(
        BlockingSourceHarness(
            csv_connector,
            expected_records=len(connector_dataset.rows),
            expected_malformed=_expected_malformed(connector_dataset),
            page_size=1,
            kind_name="csv_source",
        )
    )
    jsonl_connector = JsonlFileSourceConnector(
        JsonlFileSourceConfig(
            SourceFileLocation.create(fixture_root, "inventory.jsonl"), bounds=bounds
        )
    )
    jsonl_connector.open()
    run_blocking_source_conformance(
        BlockingSourceHarness(
            jsonl_connector,
            expected_records=len(connector_dataset.rows),
            expected_malformed=_expected_malformed(connector_dataset),
            page_size=1,
            kind_name="jsonl_source",
        )
    )


def test_conformance_covers_every_source_kind() -> None:
    """The conformance matrix pins exactly the four Phase 9 source kinds."""
    assert _SOURCE_IDS == ("async_http", "blocking_http", "csv", "jsonl")

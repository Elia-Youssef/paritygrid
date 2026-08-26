"""Shared fixtures for the Phase 9 connector tests."""

from collections.abc import AsyncIterator
from pathlib import Path

import pytest

from paritygrid.demo.datasets import (
    DatasetProfile,
    ScenarioSeed,
    ScenarioVersion,
    SyntheticDataset,
    generate_dataset,
)
from paritygrid.demo.failures import FailureScript
from paritygrid.demo.fixtures import write_csv_fixture, write_jsonl_fixture
from paritygrid.demo.simulators.async_source import AsyncInventorySource
from paritygrid.demo.simulators.blocking_source import BlockingInventorySource
from paritygrid.demo.simulators.warehouse import SimulatedWarehouse


@pytest.fixture
def anyio_backend() -> str:
    return "asyncio"


@pytest.fixture(scope="session")
def connector_dataset() -> SyntheticDataset:
    """The fixed dataset every connector test reads."""
    profile = DatasetProfile(
        record_count=24, malformed_count=3, boundary_count=2, duplicate_count=3
    )
    return generate_dataset(ScenarioSeed(909), ScenarioVersion(1), profile)


@pytest.fixture
def fixture_root(connector_dataset: SyntheticDataset, tmp_path: Path) -> Path:
    """A allowlisted root holding the CSV and JSONL fixture files."""
    root = tmp_path / "sources"
    root.mkdir()
    write_csv_fixture(connector_dataset, root / "inventory.csv")
    write_jsonl_fixture(connector_dataset, root / "inventory.jsonl")
    return root


@pytest.fixture
async def async_source(
    connector_dataset: SyntheticDataset,
) -> AsyncIterator[AsyncInventorySource]:
    """A started async source simulator with no scripted failures."""
    simulator = AsyncInventorySource(connector_dataset, FailureScript.empty())
    await simulator.start()
    try:
        yield simulator
    finally:
        await simulator.aclose()


@pytest.fixture
async def blocking_source(
    connector_dataset: SyntheticDataset,
) -> AsyncIterator[BlockingInventorySource]:
    """A started blocking legacy source simulator with no scripted failures."""
    simulator = BlockingInventorySource(connector_dataset, FailureScript.empty())
    simulator.start()
    try:
        yield simulator
    finally:
        await simulator.aclose()


@pytest.fixture
async def warehouse() -> AsyncIterator[SimulatedWarehouse]:
    """A started empty simulated warehouse."""
    simulator = SimulatedWarehouse(FailureScript.empty())
    await simulator.start()
    try:
        yield simulator
    finally:
        await simulator.aclose()

"""Shared fixtures for the simulator test modules."""

import pytest


@pytest.fixture
def anyio_backend() -> str:
    return "asyncio"

"""Frozen default and bound contracts for planner resources."""

from __future__ import annotations

from dataclasses import FrozenInstanceError, replace
from pathlib import Path
from typing import Any, cast

import pytest

from paritygrid.application.planner import (
    DEFAULT_RESOURCE_POLICY,
    MAX_RESOURCE_CONCURRENCY,
    MAX_RESOURCE_IN_FLIGHT,
    MAX_RESOURCE_MEMORY_BYTES,
    MAX_RESOURCE_QUEUE_CAPACITY,
    MAX_RESOURCE_TIMEOUT_SECONDS,
    MIN_RESOURCE_CONCURRENCY,
    MIN_RESOURCE_IN_FLIGHT,
    MIN_RESOURCE_MEMORY_BYTES,
    MIN_RESOURCE_QUEUE_CAPACITY,
    MIN_RESOURCE_TIMEOUT_SECONDS,
    ResourcePolicy,
    ResourcePolicyError,
)
from paritygrid.application.planner import resources as contract


def test_resource_contract_is_dependency_neutral_and_has_frozen_bounds() -> None:
    assert (MIN_RESOURCE_CONCURRENCY, MAX_RESOURCE_CONCURRENCY) == (1, 64)
    assert (MIN_RESOURCE_IN_FLIGHT, MAX_RESOURCE_IN_FLIGHT) == (1, 1_024)
    assert (MIN_RESOURCE_MEMORY_BYTES, MAX_RESOURCE_MEMORY_BYTES) == (
        67_108_864,
        17_179_869_184,
    )
    assert (MIN_RESOURCE_TIMEOUT_SECONDS, MAX_RESOURCE_TIMEOUT_SECONDS) == (1, 3_600)
    assert (MIN_RESOURCE_QUEUE_CAPACITY, MAX_RESOURCE_QUEUE_CAPACITY) == (1, 65_536)
    source = Path(contract.__file__).read_text(encoding="utf-8")
    assert "sqlalchemy" not in source
    assert "fastapi" not in source
    assert "pydantic" not in source


def test_default_policy_is_exact_total_immutable_and_cross_platform() -> None:
    assert ResourcePolicy() == DEFAULT_RESOURCE_POLICY
    assert DEFAULT_RESOURCE_POLICY.to_mapping() == {
        "max_concurrency": 4,
        "max_in_flight": 16,
        "memory_limit_bytes": 536_870_912,
        "operation_timeout_seconds": 60,
        "queue_capacity": 256,
    }
    with pytest.raises(FrozenInstanceError):
        DEFAULT_RESOURCE_POLICY.max_concurrency = 8  # type: ignore[misc]


def test_minimum_and_maximum_coherent_policies_are_accepted() -> None:
    assert (
        ResourcePolicy(
            max_concurrency=MIN_RESOURCE_CONCURRENCY,
            max_in_flight=MIN_RESOURCE_IN_FLIGHT,
            memory_limit_bytes=MIN_RESOURCE_MEMORY_BYTES,
            operation_timeout_seconds=MIN_RESOURCE_TIMEOUT_SECONDS,
            queue_capacity=MIN_RESOURCE_QUEUE_CAPACITY,
        ).max_concurrency
        == 1
    )
    assert (
        ResourcePolicy(
            max_concurrency=MAX_RESOURCE_CONCURRENCY,
            max_in_flight=MAX_RESOURCE_IN_FLIGHT,
            memory_limit_bytes=MAX_RESOURCE_MEMORY_BYTES,
            operation_timeout_seconds=MAX_RESOURCE_TIMEOUT_SECONDS,
            queue_capacity=MAX_RESOURCE_QUEUE_CAPACITY,
        ).queue_capacity
        == MAX_RESOURCE_QUEUE_CAPACITY
    )


@pytest.mark.parametrize(
    ("field", "value", "error"),
    [
        ("max_concurrency", cast(Any, True), TypeError),
        ("max_concurrency", MIN_RESOURCE_CONCURRENCY - 1, ResourcePolicyError),
        ("max_concurrency", MAX_RESOURCE_CONCURRENCY + 1, ResourcePolicyError),
        ("max_in_flight", cast(Any, True), TypeError),
        ("max_in_flight", MIN_RESOURCE_IN_FLIGHT - 1, ResourcePolicyError),
        ("max_in_flight", MAX_RESOURCE_IN_FLIGHT + 1, ResourcePolicyError),
        ("memory_limit_bytes", cast(Any, True), TypeError),
        ("memory_limit_bytes", MIN_RESOURCE_MEMORY_BYTES - 1, ResourcePolicyError),
        ("memory_limit_bytes", MAX_RESOURCE_MEMORY_BYTES + 1, ResourcePolicyError),
        ("operation_timeout_seconds", cast(Any, True), TypeError),
        ("operation_timeout_seconds", MIN_RESOURCE_TIMEOUT_SECONDS - 1, ResourcePolicyError),
        ("operation_timeout_seconds", MAX_RESOURCE_TIMEOUT_SECONDS + 1, ResourcePolicyError),
        ("queue_capacity", cast(Any, True), TypeError),
        ("queue_capacity", MIN_RESOURCE_QUEUE_CAPACITY - 1, ResourcePolicyError),
        ("queue_capacity", MAX_RESOURCE_QUEUE_CAPACITY + 1, ResourcePolicyError),
    ],
)
def test_each_resource_limit_rejects_type_confusion_and_out_of_bounds(
    field: str,
    value: object,
    error: type[Exception],
) -> None:
    with pytest.raises(error):
        replace(DEFAULT_RESOURCE_POLICY, **{field: value})


def test_relational_bounds_fail_closed() -> None:
    with pytest.raises(ResourcePolicyError, match="in-flight work must cover"):
        replace(DEFAULT_RESOURCE_POLICY, max_concurrency=8, max_in_flight=4)
    with pytest.raises(ResourcePolicyError, match="queue capacity must cover"):
        replace(DEFAULT_RESOURCE_POLICY, max_in_flight=32, queue_capacity=16)

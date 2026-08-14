"""Frozen contract tests for stable partition strategies and keys."""

from __future__ import annotations

from dataclasses import FrozenInstanceError, replace

import pytest

from paritygrid.application.planner import (
    MAX_PARTITION_COUNT,
    MIN_PARTITION_COUNT,
    PARTITION_KEY_DIGITS,
    PARTITION_NODE_KIND,
    PARTITION_STRATEGY_VERSION,
    SINGLE_PARTITION_STRATEGY,
    InvalidPartitionIndexError,
    PartitionStrategy,
    PartitionStrategyError,
    PartitionStrategyKind,
    partition_strategy_from_configuration,
)
from paritygrid.application.ports.configuration import ConfigurationDocument
from paritygrid.domain.pipeline import NodeKind, PartitionKey


def test_partition_constants_enum_and_error_family_are_frozen() -> None:
    assert PARTITION_STRATEGY_VERSION == 1
    assert MIN_PARTITION_COUNT == 1
    assert MAX_PARTITION_COUNT == 1_024
    assert PARTITION_KEY_DIGITS == 8
    assert NodeKind("transform.partition") == PARTITION_NODE_KIND
    assert tuple(PartitionStrategyKind) == (
        PartitionStrategyKind.SINGLE,
        PartitionStrategyKind.FIXED,
    )
    assert issubclass(InvalidPartitionIndexError, PartitionStrategyError)


def test_single_strategy_has_one_frozen_key_and_exact_mapping() -> None:
    assert PartitionStrategy(PartitionStrategyKind.SINGLE, 1) == SINGLE_PARTITION_STRATEGY
    assert SINGLE_PARTITION_STRATEGY.keys() == (PartitionKey("partition-00000000"),)
    assert SINGLE_PARTITION_STRATEGY.to_mapping() == {
        "kind": "single",
        "partition_count": 1,
        "version": 1,
    }


def test_fixed_strategy_derives_stable_zero_based_keys() -> None:
    strategy = PartitionStrategy(PartitionStrategyKind.FIXED, 3)
    assert strategy.key(0) == PartitionKey("partition-00000000")
    assert strategy.key(2) == PartitionKey("partition-00000002")
    assert strategy.keys() == (
        PartitionKey("partition-00000000"),
        PartitionKey("partition-00000001"),
        PartitionKey("partition-00000002"),
    )
    assert strategy.to_mapping() == {"kind": "fixed", "partition_count": 3, "version": 1}


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        ("kind", "single", "PartitionStrategyKind"),
        ("partition_count", True, "integer"),
        ("version", True, "integer"),
    ],
)
def test_strategy_requires_exact_public_types(field: str, value: object, message: str) -> None:
    with pytest.raises(TypeError, match=message):
        replace(SINGLE_PARTITION_STRATEGY, **{field: value})


@pytest.mark.parametrize("count", [0, MAX_PARTITION_COUNT + 1])
def test_partition_count_is_bounded(count: int) -> None:
    with pytest.raises(PartitionStrategyError, match="outside"):
        PartitionStrategy(PartitionStrategyKind.FIXED, count)


def test_strategy_kind_and_count_must_agree() -> None:
    with pytest.raises(PartitionStrategyError, match="exactly one"):
        PartitionStrategy(PartitionStrategyKind.SINGLE, 2)
    with pytest.raises(PartitionStrategyError, match="multiple"):
        PartitionStrategy(PartitionStrategyKind.FIXED, 1)


def test_strategy_rejects_unsupported_version() -> None:
    with pytest.raises(PartitionStrategyError, match="unsupported"):
        replace(SINGLE_PARTITION_STRATEGY, version=2)


def test_partition_key_index_is_exact_and_bounded() -> None:
    strategy = PartitionStrategy(PartitionStrategyKind.FIXED, 2)
    with pytest.raises(TypeError, match="integer"):
        strategy.key(True)
    for index in (-1, 2):
        with pytest.raises(InvalidPartitionIndexError, match="outside"):
            strategy.key(index)


def test_partition_strategy_is_immutable() -> None:
    with pytest.raises(FrozenInstanceError):
        SINGLE_PARTITION_STRATEGY.partition_count = 2  # type: ignore[misc]


def test_partition_strategy_derivation_defaults_non_partition_and_empty_nodes_to_single() -> None:
    empty = ConfigurationDocument.from_mapping({})
    assert (
        partition_strategy_from_configuration(NodeKind("transform.normalize"), empty)
        is SINGLE_PARTITION_STRATEGY
    )
    assert (
        partition_strategy_from_configuration(PARTITION_NODE_KIND, empty)
        is SINGLE_PARTITION_STRATEGY
    )
    assert (
        partition_strategy_from_configuration(
            PARTITION_NODE_KIND,
            ConfigurationDocument.from_mapping({"partition_count": 1}),
        )
        is SINGLE_PARTITION_STRATEGY
    )


def test_partition_strategy_derivation_builds_stable_fixed_strategy() -> None:
    strategy = partition_strategy_from_configuration(
        PARTITION_NODE_KIND,
        ConfigurationDocument.from_mapping({"partition_count": 3}),
    )
    assert strategy == PartitionStrategy(PartitionStrategyKind.FIXED, 3)
    assert strategy.keys() == (
        PartitionKey("partition-00000000"),
        PartitionKey("partition-00000001"),
        PartitionKey("partition-00000002"),
    )


@pytest.mark.parametrize(
    ("kind", "configuration", "message"),
    [
        ("transform.partition", ConfigurationDocument.from_mapping({}), "NodeKind"),
        (PARTITION_NODE_KIND, {}, "ConfigurationDocument"),
    ],
)
def test_partition_strategy_derivation_requires_exact_types(
    kind: object,
    configuration: object,
    message: str,
) -> None:
    with pytest.raises(TypeError, match=message):
        partition_strategy_from_configuration(kind, configuration)  # type: ignore[arg-type]


@pytest.mark.parametrize("count", [True, 0, MAX_PARTITION_COUNT + 1])
def test_partition_strategy_derivation_rejects_invalid_counts(count: object) -> None:
    configuration = ConfigurationDocument.from_mapping({"partition_count": count})
    error = TypeError if type(count) is bool else PartitionStrategyError
    with pytest.raises(error):
        partition_strategy_from_configuration(PARTITION_NODE_KIND, configuration)

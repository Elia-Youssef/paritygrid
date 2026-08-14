"""Dependency-neutral stable partition strategy contracts."""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from typing import cast

from paritygrid.application.ports.configuration import ConfigurationDocument
from paritygrid.domain.pipeline import NodeKind, PartitionKey

PARTITION_STRATEGY_VERSION = 1
MIN_PARTITION_COUNT = 1
MAX_PARTITION_COUNT = 1_024
PARTITION_KEY_DIGITS = 8
PARTITION_NODE_KIND = NodeKind("transform.partition")


class PartitionStrategyError(ValueError):
    """Base failure for an invalid partition strategy or key request."""


class InvalidPartitionIndexError(PartitionStrategyError):
    """A partition index is outside its strategy's closed range."""


class PartitionStrategyKind(StrEnum):
    """Closed logical partitioning behaviors."""

    SINGLE = "single"
    FIXED = "fixed"


@dataclass(frozen=True, slots=True)
class PartitionStrategy:
    """A bounded strategy that deterministically derives stable keys."""

    kind: PartitionStrategyKind
    partition_count: int
    version: int = PARTITION_STRATEGY_VERSION

    def __post_init__(self) -> None:
        if type(self.kind) is not PartitionStrategyKind:
            raise TypeError("partition strategy kind must use PartitionStrategyKind")
        count = cast(object, self.partition_count)
        if type(count) is not int:
            raise TypeError("partition count must be an integer")
        if not MIN_PARTITION_COUNT <= count <= MAX_PARTITION_COUNT:
            raise PartitionStrategyError("partition count is outside the supported range")
        version = cast(object, self.version)
        if type(version) is not int:
            raise TypeError("partition strategy version must be an integer")
        if version != PARTITION_STRATEGY_VERSION:
            raise PartitionStrategyError("partition strategy version is unsupported")
        if self.kind is PartitionStrategyKind.SINGLE and count != 1:
            raise PartitionStrategyError("single partition strategy requires exactly one key")
        if self.kind is PartitionStrategyKind.FIXED and count == 1:
            raise PartitionStrategyError("fixed partition strategy requires multiple keys")

    def key(self, index: int) -> PartitionKey:
        """Return one version-stable key for a zero-based partition index."""
        value = cast(object, index)
        if type(value) is not int:
            raise TypeError("partition index must be an integer")
        if not 0 <= value < self.partition_count:
            raise InvalidPartitionIndexError("partition index is outside the strategy range")
        return PartitionKey(f"partition-{value:0{PARTITION_KEY_DIGITS}d}")

    def keys(self) -> tuple[PartitionKey, ...]:
        """Return every stable key in ascending index order."""
        return tuple(self.key(index) for index in range(self.partition_count))

    def to_mapping(self) -> dict[str, object]:
        """Return the exact logical partition strategy representation."""
        return {
            "kind": self.kind.value,
            "partition_count": self.partition_count,
            "version": self.version,
        }


SINGLE_PARTITION_STRATEGY = PartitionStrategy(PartitionStrategyKind.SINGLE, 1)


def partition_strategy_from_configuration(
    kind: NodeKind,
    configuration: ConfigurationDocument,
) -> PartitionStrategy:
    """Derive a logical strategy without depending on an execution backend."""
    if type(kind) is not NodeKind:
        raise TypeError("partition strategy node kind must use NodeKind")
    if type(configuration) is not ConfigurationDocument:
        raise TypeError("partition strategy configuration must use ConfigurationDocument")
    if kind != PARTITION_NODE_KIND:
        return SINGLE_PARTITION_STRATEGY
    count = configuration.to_mapping().get("partition_count", 1)
    if type(count) is not int:
        raise TypeError("partition count must be an integer")
    if count == 1:
        return SINGLE_PARTITION_STRATEGY
    return PartitionStrategy(PartitionStrategyKind.FIXED, count)

"""Dependency-neutral stable partition strategy contracts."""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from typing import cast

from paritygrid.domain.pipeline import PartitionKey

PARTITION_STRATEGY_VERSION = 1
MIN_PARTITION_COUNT = 1
MAX_PARTITION_COUNT = 1_024
PARTITION_KEY_DIGITS = 8


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

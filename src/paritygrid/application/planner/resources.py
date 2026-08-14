"""Dependency-neutral bounded resource policy contracts."""

from __future__ import annotations

from dataclasses import dataclass
from typing import cast

from paritygrid.application.planner.documents import PipelineDocument
from paritygrid.application.ports.configuration import ConfigurationDocument

MEBIBYTE = 1_048_576
MIN_RESOURCE_CONCURRENCY = 1
MAX_RESOURCE_CONCURRENCY = 64
MIN_RESOURCE_IN_FLIGHT = 1
MAX_RESOURCE_IN_FLIGHT = 1_024
MIN_RESOURCE_MEMORY_BYTES = 64 * MEBIBYTE
MAX_RESOURCE_MEMORY_BYTES = 16 * 1_024 * MEBIBYTE
MIN_RESOURCE_TIMEOUT_SECONDS = 1
MAX_RESOURCE_TIMEOUT_SECONDS = 3_600
MIN_RESOURCE_QUEUE_CAPACITY = 1
MAX_RESOURCE_QUEUE_CAPACITY = 65_536


class ResourcePolicyError(ValueError):
    """A pipeline resource policy is unknown, unbounded, or incoherent."""


@dataclass(frozen=True, slots=True)
class ResourcePolicy:
    """Closed planner limits with deterministic cross-platform defaults."""

    max_concurrency: int = 4
    max_in_flight: int = 16
    memory_limit_bytes: int = 512 * MEBIBYTE
    operation_timeout_seconds: int = 60
    queue_capacity: int = 256

    def __post_init__(self) -> None:
        _validate_integer(
            self.max_concurrency,
            MIN_RESOURCE_CONCURRENCY,
            MAX_RESOURCE_CONCURRENCY,
            "resource maximum concurrency",
        )
        _validate_integer(
            self.max_in_flight,
            MIN_RESOURCE_IN_FLIGHT,
            MAX_RESOURCE_IN_FLIGHT,
            "resource maximum in-flight work",
        )
        _validate_integer(
            self.memory_limit_bytes,
            MIN_RESOURCE_MEMORY_BYTES,
            MAX_RESOURCE_MEMORY_BYTES,
            "resource memory limit",
        )
        _validate_integer(
            self.operation_timeout_seconds,
            MIN_RESOURCE_TIMEOUT_SECONDS,
            MAX_RESOURCE_TIMEOUT_SECONDS,
            "resource operation timeout",
        )
        _validate_integer(
            self.queue_capacity,
            MIN_RESOURCE_QUEUE_CAPACITY,
            MAX_RESOURCE_QUEUE_CAPACITY,
            "resource queue capacity",
        )
        if self.max_in_flight < self.max_concurrency:
            raise ResourcePolicyError(
                "resource maximum in-flight work must cover maximum concurrency"
            )
        if self.queue_capacity < self.max_in_flight:
            raise ResourcePolicyError("resource queue capacity must cover in-flight work")

    def to_mapping(self) -> dict[str, int]:
        """Return the exact total version 1 resource policy object."""
        return {
            "max_concurrency": self.max_concurrency,
            "max_in_flight": self.max_in_flight,
            "memory_limit_bytes": self.memory_limit_bytes,
            "operation_timeout_seconds": self.operation_timeout_seconds,
            "queue_capacity": self.queue_capacity,
        }


def _validate_integer(value: object, minimum: int, maximum: int, subject: str) -> None:
    if type(value) is not int:
        raise TypeError(f"{subject} must be an integer")
    if not minimum <= value <= maximum:
        raise ResourcePolicyError(f"{subject} is outside the supported range")


DEFAULT_RESOURCE_POLICY = ResourcePolicy()

_RESOURCE_POLICY_FIELDS = frozenset(DEFAULT_RESOURCE_POLICY.to_mapping())


def parse_resource_policy(document: ConfigurationDocument) -> ResourcePolicy:
    """Apply deterministic defaults to one exact partial resource object."""
    if type(document) is not ConfigurationDocument:
        raise TypeError("resource policy must use ConfigurationDocument")
    mapping = document.to_mapping()
    if frozenset(mapping) - _RESOURCE_POLICY_FIELDS:
        raise ResourcePolicyError("resource policy contains unknown fields")
    if any(type(value) is not int for value in mapping.values()):
        raise TypeError("resource policy values must be integers")
    values = DEFAULT_RESOURCE_POLICY.to_mapping()
    values.update({key: cast(int, value) for key, value in mapping.items()})
    return ResourcePolicy(**values)


def validate_resource_policy(document: PipelineDocument) -> ResourcePolicy:
    """Validate and materialize the total resource policy for one pipeline."""
    if type(document) is not PipelineDocument:
        raise TypeError("pipeline document must use PipelineDocument")
    return parse_resource_policy(document.resource_policy)

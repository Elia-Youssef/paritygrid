"""Contracts used by operational health endpoints."""

from dataclasses import dataclass
from typing import Protocol


@dataclass(frozen=True, slots=True)
class ReadinessResult:
    """Result of checking whether the runtime can accept requests."""

    ready: bool
    detail: str


class ReadinessProbe(Protocol):
    """Return the current runtime readiness without changing state."""

    async def check(self) -> ReadinessResult:
        """Check the runtime dependencies required to serve traffic."""
        ...


@dataclass(frozen=True, slots=True)
class StaticReadinessProbe:
    """Provide a fixed readiness result for an assembled runtime."""

    result: ReadinessResult

    async def check(self) -> ReadinessResult:
        """Return the configured readiness result."""
        return self.result

"""Dependency-neutral inward-facing result-sink port."""

from __future__ import annotations

from typing import TYPE_CHECKING, Protocol, runtime_checkable

if TYPE_CHECKING:
    from paritygrid.application.execution.result_sink import (
        ResultSinkOutcome,
        ResultSubmission,
    )


@runtime_checkable
class ResultSink(Protocol):
    """Borrowed sink that proves commit or durable non-mutation for one result."""

    def submit(self, submission: ResultSubmission, /) -> ResultSinkOutcome:
        """Return only after commit or durable non-mutation is proven."""
        ...


__all__ = ["ResultSink"]

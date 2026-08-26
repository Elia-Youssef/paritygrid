"""Blocking legacy inventory source simulator.

The legacy source serves the same deterministic dataset and scripted failure
semantics as the async source through page-numbered routes, behind a
thread-per-connection blocking HTTP server.
"""

import asyncio
import threading

from paritygrid.demo.datasets import SyntheticDataset
from paritygrid.demo.failures import AppliedFailure, FailureScript
from paritygrid.demo.simulators.blocking_server import BlockingHttpService
from paritygrid.demo.simulators.source_behavior import (
    SourceBehavior,
    SourcePagingStyle,
    SourceSettings,
)

BLOCKING_SOURCE_SERVICE_NAME = "blocking-source"


class BlockingInventorySource:
    """The page-numbered legacy source served over blocking loopback HTTP."""

    def __init__(
        self,
        dataset: SyntheticDataset,
        script: FailureScript,
        *,
        max_page_size: int = 200,
        request_latency_microseconds: int = 0,
    ) -> None:
        self._behavior = SourceBehavior(
            dataset,
            script,
            SourceSettings(
                service_name=BLOCKING_SOURCE_SERVICE_NAME,
                paging_style=SourcePagingStyle.PAGES,
                max_page_size=max_page_size,
                request_latency_microseconds=request_latency_microseconds,
            ),
        )
        self._service = BlockingHttpService(
            service_name=BLOCKING_SOURCE_SERVICE_NAME,
            handler=self._behavior.handle,
        )

    @property
    def port(self) -> int:
        """Return the dynamically assigned loopback port."""
        return self._service.port

    @property
    def base_url(self) -> str:
        """Return the loopback base URL of this source."""
        return self._service.base_url

    @property
    def serving_thread(self) -> threading.Thread:
        """Return the owned serving thread of the blocking boundary."""
        return self._service.thread

    def is_serving(self) -> bool:
        """Report whether the owned listener thread is still alive."""
        return self._service.is_serving()

    def request_count(self) -> int:
        """Return how many data requests have been counted."""
        return self._behavior.request_count()

    def applied_failures(self) -> tuple[AppliedFailure, ...]:
        """Return every applied scripted failure in application order."""
        return self._behavior.applied_failures()

    def start(self) -> None:
        """Bind the dynamic loopback port and spawn the serving thread."""
        self._service.start()

    async def aclose(self) -> None:
        """Stop the blocking server without blocking the event loop."""
        await asyncio.to_thread(self._service.close)

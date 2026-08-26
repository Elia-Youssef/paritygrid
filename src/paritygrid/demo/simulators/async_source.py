"""Asynchronous paginated inventory source simulator."""

from paritygrid.demo.datasets import SyntheticDataset
from paritygrid.demo.failures import AppliedFailure, FailureScript
from paritygrid.demo.simulators.async_server import AsyncHttpService
from paritygrid.demo.simulators.source_behavior import (
    SourceBehavior,
    SourcePagingStyle,
    SourceSettings,
)

ASYNC_SOURCE_SERVICE_NAME = "async-source"


class AsyncInventorySource:
    """The cursor-paginated modern source served over real loopback HTTP."""

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
                service_name=ASYNC_SOURCE_SERVICE_NAME,
                paging_style=SourcePagingStyle.CURSOR,
                max_page_size=max_page_size,
                request_latency_microseconds=request_latency_microseconds,
            ),
        )
        self._service = AsyncHttpService(
            service_name=ASYNC_SOURCE_SERVICE_NAME,
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

    def is_serving(self) -> bool:
        """Report whether the listener still accepts connections."""
        return self._service.is_serving()

    def request_count(self) -> int:
        """Return how many data requests have been counted."""
        return self._behavior.request_count()

    def applied_failures(self) -> tuple[AppliedFailure, ...]:
        """Return every applied scripted failure in application order."""
        return self._behavior.applied_failures()

    async def start(self) -> None:
        """Bind the dynamic loopback port and begin serving."""
        await self._service.start()

    async def aclose(self) -> None:
        """Stop serving and release the owned listener."""
        await self._service.aclose()

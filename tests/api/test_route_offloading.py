"""The synchronous operational surface is offloaded from the event loop."""

from inspect import iscoroutinefunction

from fastapi import APIRouter
from fastapi.routing import APIRoute

from paritygrid.api.routers.artifacts import router as artifacts_router
from paritygrid.api.routers.connectors import router as connectors_router
from paritygrid.api.routers.pipelines import router as pipelines_router
from paritygrid.api.routers.runs import router as runs_router
from paritygrid.api.routers.system import router as system_router


def test_blocking_operational_routes_are_synchronous() -> None:
    routers: tuple[APIRouter, ...] = (
        pipelines_router,
        connectors_router,
        runs_router,
        artifacts_router,
        system_router,
    )

    count = 0
    for router in routers:
        for route in router.routes:
            if isinstance(route, APIRoute):
                count += 1
                assert not iscoroutinefunction(route.endpoint)
    assert count > 0

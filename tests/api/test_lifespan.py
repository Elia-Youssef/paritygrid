"""Lifespan ownership, partial-startup rollback, and readiness tests."""

from pathlib import Path

import httpx
import pytest

from paritygrid.runtime.composition import (
    RuntimeContainer,
    StartupStep,
    compose_runtime,
    create_runtime_app,
    run_startup_sequence,
    shutdown_started,
)
from paritygrid.runtime.config import Settings


def test_startup_sequence_opens_in_order_and_closes_in_reverse() -> None:
    events: list[str] = []
    steps = [
        StartupStep(
            name="first",
            opener=lambda: events.append("open:first") or 1,
            closer=lambda value: events.append(f"close:first:{value}"),
        ),
        StartupStep(
            name="second",
            opener=lambda: events.append("open:second") or 2,
            closer=lambda value: events.append(f"close:second:{value}"),
        ),
    ]
    started, order = run_startup_sequence(steps)
    assert order == ("first", "second")
    shutdown_started(steps, started)
    assert events == [
        "open:first",
        "open:second",
        "close:second:2",
        "close:first:1",
    ]


def test_startup_sequence_rolls_back_partial_startup_in_reverse() -> None:
    events: list[str] = []

    def _failing() -> object:
        raise RuntimeError("third resource failed")

    steps = [
        StartupStep(
            name="first",
            opener=lambda: events.append("open:first") or 1,
            closer=lambda _value: events.append("close:first"),
        ),
        StartupStep(
            name="second",
            opener=lambda: events.append("open:second") or 2,
            closer=lambda _value: events.append("close:second"),
        ),
        StartupStep(name="third", opener=_failing, closer=lambda _v: None),
    ]
    with pytest.raises(RuntimeError, match="third resource failed"):
        run_startup_sequence(steps)
    assert events == ["open:first", "open:second", "close:second", "close:first"]


def test_shutdown_attempts_every_closer_after_one_fails() -> None:
    events: list[str] = []

    def fail_close(_value: object) -> None:
        events.append("close:second")
        raise RuntimeError("synthetic close failure")

    steps = [
        StartupStep("first", lambda: object(), lambda _value: events.append("close:first")),
        StartupStep("second", lambda: object(), fail_close),
    ]
    started, _order = run_startup_sequence(steps)

    with pytest.raises(RuntimeError, match="synthetic close failure"):
        shutdown_started(steps, started)

    assert events == ["close:second", "close:first"]


def test_compose_runtime_reports_startup_order(
    container: RuntimeContainer,
) -> None:
    assert container.started_steps == (
        "data-root",
        "database",
        "migration",
        "artifact-root",
        "writer",
    )


def test_compose_runtime_rolls_back_when_a_later_step_fails(tmp_path: Path) -> None:
    # A file where the artifact root directory belongs makes the artifact-root
    # step fail after the database has opened.
    data_root = tmp_path / "data"
    data_root.mkdir(parents=True)
    (data_root / "artifacts").write_text("not a directory", encoding="utf-8")
    settings = Settings(data_root=data_root)
    with pytest.raises(OSError, match="artifacts"):
        compose_runtime(settings)
    # The rolled-back database engine must not hold the file: a second
    # composition after clearing the blocker succeeds on the same path.
    (data_root / "artifacts").unlink()
    runtime = compose_runtime(settings)
    try:
        assert runtime.started_steps[0] == "data-root"
    finally:
        runtime.writer.close(timeout_seconds=5.0)
        runtime.database.close()


@pytest.mark.anyio
async def test_lifespan_startup_and_shutdown_close_owned_resources(
    settings: Settings,
) -> None:
    application = create_runtime_app(settings)
    transport = httpx.ASGITransport(app=application)
    async with application.router.lifespan_context(application):
        async with httpx.AsyncClient(transport=transport, base_url="http://t") as client:
            ready = await client.get("/readyz")
            assert ready.status_code == 200
            assert ready.json()["status"] == "ready"
        container: RuntimeContainer = application.state.container
        assert container.started_steps[-1] == "writer"
    assert container.writer.snapshot().state.value == "closed"


@pytest.mark.anyio
async def test_readyz_reports_not_ready_before_lifespan_startup(
    settings: Settings,
) -> None:
    application = create_runtime_app(settings)
    transport = httpx.ASGITransport(app=application)
    async with httpx.AsyncClient(transport=transport, base_url="http://t") as client:
        response = await client.get("/readyz")
    assert response.status_code == 503
    assert response.json()["status"] == "not_ready"

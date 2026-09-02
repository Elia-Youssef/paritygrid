"""Run lifecycle route and transition contract tests."""

import httpx
import pytest
from sqlalchemy import update

from paritygrid.adapters.persistence.schema import runs as runs_table
from paritygrid.application.ports.run_control import RunControlEvidence
from paritygrid.application.writes.execution import TransitionRunResult
from paritygrid.domain.execution import RunState
from paritygrid.domain.models import RunId
from paritygrid.runtime.composition import RuntimeContainer
from tests.api.conftest import PIPELINE_ID, seed_scenario, transition_run

RUN_ID = "run_scenario-01"


class _DurableTestExecutionOwner:
    """Test-only execution owner that records actual writer receipts."""

    def __init__(self, container: RuntimeContainer, run_id: str) -> None:
        self._container = container
        self._run_id = run_id
        self.calls: list[tuple[str, str | None, float, bool]] = []
        self.closed_with: float | None = None

    def pause(
        self,
        *,
        correlation_id: str | None,
        timeout_seconds: float,
        converge_on_duplicate: bool,
    ) -> RunControlEvidence:
        return self._control(
            "pause",
            (RunState.PAUSING, RunState.PAUSED),
            correlation_id,
            timeout_seconds,
            converge_on_duplicate,
        )

    def resume(
        self,
        *,
        correlation_id: str | None,
        timeout_seconds: float,
        converge_on_duplicate: bool,
    ) -> RunControlEvidence:
        return self._control(
            "resume",
            (RunState.RESUMING, RunState.RUNNING),
            correlation_id,
            timeout_seconds,
            converge_on_duplicate,
        )

    def cancel(
        self,
        *,
        correlation_id: str | None,
        timeout_seconds: float,
        converge_on_duplicate: bool,
    ) -> RunControlEvidence:
        return self._control(
            "cancel",
            (RunState.CANCELLING, RunState.CANCELLED),
            correlation_id,
            timeout_seconds,
            converge_on_duplicate,
        )

    def close(self, *, timeout_seconds: float) -> None:
        self.closed_with = timeout_seconds

    def _control(
        self,
        action: str,
        targets: tuple[RunState, RunState],
        correlation_id: str | None,
        timeout_seconds: float,
        converge_on_duplicate: bool,
    ) -> RunControlEvidence:
        self.calls.append((action, correlation_id, timeout_seconds, converge_on_duplicate))
        receipts = tuple(
            transition_run(self._container, self._run_id, target) for target in targets
        )
        result = receipts[-1].result
        assert isinstance(result, TransitionRunResult)
        return RunControlEvidence(result.run, tuple(item.submission_id for item in receipts))


class _AdvancingResumeExecutionOwner(_DurableTestExecutionOwner):
    """Model engine progress committed after its proven resume transition."""

    def resume(
        self,
        *,
        correlation_id: str | None,
        timeout_seconds: float,
        converge_on_duplicate: bool,
    ) -> RunControlEvidence:
        evidence = super().resume(
            correlation_id=correlation_id,
            timeout_seconds=timeout_seconds,
            converge_on_duplicate=converge_on_duplicate,
        )
        with self._container.database.transaction() as session:
            session.execute(
                update(runs_table)
                .where(runs_table.c.run_id == self._run_id)
                .values(row_version=runs_table.c.row_version + 1)
            )
        return evidence


def _run_body(run_id: str = RUN_ID, **overrides: object) -> dict[str, object]:
    payload: dict[str, object] = {
        "run_id": run_id,
        "pipeline_id": PIPELINE_ID,
        "pipeline_version": 1,
        "runner_kind": "sequential",
        "scenario_seed": 42,
    }
    payload.update(overrides)
    return payload


@pytest.mark.anyio
async def test_created_run_reports_version_coherence(
    client: httpx.AsyncClient,
) -> None:
    response = await seed_scenario(client)
    assert response.status_code == 201
    body = response.json()
    assert body["run_id"] == RUN_ID
    assert body["run_version"] == 1
    assert body["state"] == "queued"
    assert body["observed_at"]
    assert body["pipeline_version"] == 1
    assert body["scenario_seed"] == 42


@pytest.mark.anyio
async def test_run_creation_requires_published_version(client: httpx.AsyncClient) -> None:
    await client.post("/api/v1/pipelines", json={"pipeline_id": PIPELINE_ID, "display_name": "d"})
    response = await client.post("/api/v1/runs", json=_run_body())
    assert response.status_code == 404
    assert response.json()["code"] == "pipeline_version_not_found"


@pytest.mark.anyio
async def test_unknown_runner_kind_is_rejected_before_any_work(
    client: httpx.AsyncClient,
) -> None:
    await seed_scenario(client)
    response = await client.post(
        "/api/v1/runs", json=_run_body("run_scenario-02", runner_kind="process")
    )
    assert response.status_code == 422
    assert response.json()["errors"][0]["field"] == "runner_kind"


@pytest.mark.anyio
async def test_duplicate_run_identity_with_different_capture_conflicts(
    client: httpx.AsyncClient,
) -> None:
    await seed_scenario(client)
    response = await client.post("/api/v1/runs", json=_run_body(RUN_ID, scenario_seed=43))
    assert response.status_code == 409
    assert response.json()["code"] == "run_duplicate_identity"


@pytest.mark.anyio
async def test_run_listing_filters_by_state(client: httpx.AsyncClient) -> None:
    await seed_scenario(client, run_id="run_alpha-001")
    await seed_scenario(client, run_id="run_beta-002")
    listing = await client.get("/api/v1/runs", params={"state": "queued"})
    assert listing.status_code == 200
    assert {item["run_id"] for item in listing.json()["items"]} == {
        "run_alpha-001",
        "run_beta-002",
    }
    invalid = await client.get("/api/v1/runs", params={"state": "exploded"})
    assert invalid.status_code == 422


@pytest.mark.anyio
async def test_pause_resumes_and_cancels_follow_the_domain_machine(
    container: RuntimeContainer, client: httpx.AsyncClient
) -> None:
    await seed_scenario(client)
    transition_run(container, RUN_ID, RunState.RUNNING)
    owner = _DurableTestExecutionOwner(container, RUN_ID)
    container.active_run_controls.register(RunId(RUN_ID), owner)

    paused = await client.post(f"/api/v1/runs/{RUN_ID}/pause")
    assert paused.status_code == 200
    assert paused.json()["state"] == "paused"
    assert paused.json()["run_version"] == 4

    again = await client.post(f"/api/v1/runs/{RUN_ID}/pause")
    assert again.status_code == 409
    assert again.json()["code"] == "run_invalid_transition"

    resumed = await client.post(f"/api/v1/runs/{RUN_ID}/resume")
    assert resumed.status_code == 200
    assert resumed.json()["state"] == "running"

    cancelled = await client.post(f"/api/v1/runs/{RUN_ID}/cancel")
    assert cancelled.status_code == 200
    assert cancelled.json()["state"] == "cancelled"
    assert [call[0] for call in owner.calls] == ["pause", "resume", "cancel"]
    assert all(call[1] for call in owner.calls)

    for direction in ("pause", "resume", "cancel"):
        response = await client.post(f"/api/v1/runs/{RUN_ID}/{direction}")
        assert response.status_code == 409, direction


@pytest.mark.anyio
async def test_resume_accepts_newer_durable_progress_after_owner_evidence(
    container: RuntimeContainer, client: httpx.AsyncClient
) -> None:
    await seed_scenario(client)
    transition_run(container, RUN_ID, RunState.RUNNING)
    transition_run(container, RUN_ID, RunState.PAUSING)
    transition_run(container, RUN_ID, RunState.PAUSED)
    owner = _AdvancingResumeExecutionOwner(container, RUN_ID)
    container.active_run_controls.register(RunId(RUN_ID), owner)

    resumed = await client.post(f"/api/v1/runs/{RUN_ID}/resume")

    assert resumed.status_code == 200
    assert resumed.json()["state"] == "running"
    assert resumed.json()["run_version"] == 7


@pytest.mark.anyio
async def test_queued_run_cannot_be_paused(client: httpx.AsyncClient) -> None:
    await seed_scenario(client)
    response = await client.post(f"/api/v1/runs/{RUN_ID}/pause")
    assert response.status_code == 409
    assert response.json()["code"] == "run_invalid_transition"


@pytest.mark.anyio
async def test_queued_run_cancels_directly(client: httpx.AsyncClient) -> None:
    await seed_scenario(client)
    response = await client.post(f"/api/v1/runs/{RUN_ID}/cancel")
    assert response.status_code == 200
    assert response.json()["state"] == "cancelled"
    assert response.json()["run_version"] == 2


@pytest.mark.anyio
async def test_running_run_without_an_owner_fails_closed_without_a_transition(
    container: RuntimeContainer, client: httpx.AsyncClient
) -> None:
    await seed_scenario(client)
    transition_run(container, RUN_ID, RunState.RUNNING)
    before = await client.get(f"/api/v1/runs/{RUN_ID}")
    response = await client.post(f"/api/v1/runs/{RUN_ID}/pause")
    after = await client.get(f"/api/v1/runs/{RUN_ID}")
    assert response.status_code == 503
    assert response.json()["code"] == "unavailable"
    assert (after.json()["state"], after.json()["run_version"]) == (
        before.json()["state"],
        before.json()["run_version"],
    )


@pytest.mark.anyio
async def test_paused_run_without_an_owner_fails_closed_without_a_transition(
    container: RuntimeContainer, client: httpx.AsyncClient
) -> None:
    await seed_scenario(client)
    transition_run(container, RUN_ID, RunState.RUNNING)
    transition_run(container, RUN_ID, RunState.PAUSING)
    transition_run(container, RUN_ID, RunState.PAUSED)
    before = await client.get(f"/api/v1/runs/{RUN_ID}")
    response = await client.post(f"/api/v1/runs/{RUN_ID}/resume")
    after = await client.get(f"/api/v1/runs/{RUN_ID}")
    assert response.status_code == 503
    assert (after.json()["state"], after.json()["run_version"]) == (
        before.json()["state"],
        before.json()["run_version"],
    )


@pytest.mark.anyio
async def test_missing_run_returns_not_found(client: httpx.AsyncClient) -> None:
    response = await client.get("/api/v1/runs/run_missing-1")
    assert response.status_code == 404
    assert response.json()["code"] == "run_not_found"

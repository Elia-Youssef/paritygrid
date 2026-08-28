"""Durable command-idempotency boundary tests.

Covers canonical hashing, successful replay, key reuse with a different
request, concurrency, lease expiry, stranded reservations, unknown commit
outcomes, and restart recovery.
"""

from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from threading import Barrier, Event, Lock
from typing import cast

import httpx
import pytest
from sqlalchemy import func, select

from paritygrid.adapters.persistence.schema import idempotency_records, runs
from paritygrid.application.ports.configuration import ConfigurationDocument
from paritygrid.application.ports.consistency import (
    ConsistencyStorageError,
    IdempotencyConflictError,
    IdempotencyReservation,
    IdempotencyStatus,
)
from paritygrid.application.services.errors import (
    IdempotencyInProgressError,
    IdempotencyKeyConflictError,
)
from paritygrid.application.services.idempotency import (
    CommandOutcome,
    IdempotentCommandService,
)
from paritygrid.runtime.composition import RuntimeContainer
from paritygrid.runtime.config import Settings
from tests.api.conftest import (
    PIPELINE_ID,
    DeterministicClock,
    clock_driven_services,
    seed_scenario,
)


def _service(
    container: RuntimeContainer, clock: DeterministicClock, *, lease: float
) -> IdempotentCommandService:
    return clock_driven_services(container, clock, lease_seconds=lease).idempotency


def _strand(
    container: RuntimeContainer,
    scope: str,
    key: str,
    request: dict[str, object],
    clock: DeterministicClock,
) -> None:
    """Leave a durable in-progress reservation exactly as a dead owner would."""
    from paritygrid.adapters.persistence.repositories.idempotency import (
        SqlAlchemyIdempotencyRepository,
    )

    with container.database.transaction() as session:
        SqlAlchemyIdempotencyRepository(session).begin(
            scope=scope,
            key=key,
            request=ConfigurationDocument.from_mapping(request),
            started_at=clock.now(),
        )


def _ok_outcome() -> CommandOutcome:
    return CommandOutcome(
        status_code=201,
        media_type="application/json",
        body={"result": "created"},
        terminal=True,
    )


def test_replay_returns_the_stored_response_without_reexecution(
    container: RuntimeContainer,
) -> None:
    clock = DeterministicClock()
    service = _service(container, clock, lease=60.0)
    calls: list[int] = []

    def handler() -> CommandOutcome:
        calls.append(1)
        return _ok_outcome()

    request = {"pipeline_id": "pip_demo-alpha", "display_name": "Demo"}
    first = service.execute(scope="pipelines:create", key="key-1", request=request, handler=handler)
    second = service.execute(
        scope="pipelines:create", key="key-1", request=request, handler=handler
    )
    assert first.replayed is False
    assert second.replayed is True
    assert second.outcome.body == first.outcome.body
    assert second.outcome.status_code == 201
    assert len(calls) == 1


def test_same_key_with_a_different_request_conflicts(
    container: RuntimeContainer,
) -> None:
    clock = DeterministicClock()
    service = _service(container, clock, lease=60.0)
    service.execute(
        scope="pipelines:create",
        key="key-1",
        request={"a": 1},
        handler=_ok_outcome,
    )
    with pytest.raises(IdempotencyKeyConflictError):
        service.execute(
            scope="pipelines:create",
            key="key-1",
            request={"a": 2},
            handler=_ok_outcome,
        )


def test_concurrent_owner_blocks_replay_until_the_lease_expires(
    container: RuntimeContainer,
) -> None:
    clock = DeterministicClock()
    service = _service(container, clock, lease=60.0)
    # A stranded in-progress reservation is exactly what an owner that died
    # after reserving leaves behind: begin committed, nothing else did.
    _strand(container, "runs:create", "key-r", {"run_id": "run_a"}, clock)
    with pytest.raises(IdempotencyInProgressError):
        service.execute(
            scope="runs:create", key="key-r", request={"run_id": "run_a"}, handler=_ok_outcome
        )
    clock.advance_seconds(61.0)
    reclaimed = service.execute(
        scope="runs:create", key="key-r", request={"run_id": "run_a"}, handler=_ok_outcome
    )
    assert reclaimed.replayed is False
    with container.database.transaction() as session:
        status = session.execute(
            select(idempotency_records.c.status).where(
                idempotency_records.c.idempotency_key == "key-r"
            )
        ).scalar_one()
    assert status == "completed"


def test_expiry_reclaim_executes_once_across_two_owners(
    container: RuntimeContainer,
) -> None:
    clock = DeterministicClock()
    _strand(container, "pipelines:create", "key-exp", {"pipeline_id": "pip_x"}, clock)
    clock.advance_seconds(31.0)
    second_owner = _service(container, clock, lease=30.0)
    executions: list[int] = []
    outcome = CommandOutcome(
        status_code=201,
        media_type="application/json",
        body={"owner": "second"},
        terminal=True,
    )

    def handler() -> CommandOutcome:
        executions.append(1)
        return outcome

    result = second_owner.execute(
        scope="pipelines:create", key="key-exp", request={"pipeline_id": "pip_x"}, handler=handler
    )
    assert result.replayed is False
    assert executions == [1]
    # A third retry now replays the second owner's stored response.
    third = second_owner.execute(
        scope="pipelines:create", key="key-exp", request={"pipeline_id": "pip_x"}, handler=handler
    )
    assert third.replayed is True
    assert third.outcome.body == {"owner": "second"}


def test_unknown_completion_outcome_fails_closed_then_converges(
    container: RuntimeContainer, monkeypatch: pytest.MonkeyPatch
) -> None:
    clock = DeterministicClock()
    service = _service(container, clock, lease=5.0)
    from paritygrid.adapters.persistence.repositories import idempotency as repo_module
    from paritygrid.adapters.persistence.repositories.idempotency import (
        SqlAlchemyIdempotencyRepository,
    )
    from paritygrid.application.ports.consistency import ConsistencyStorageError as RepoStorageError

    original = repo_module.SqlAlchemyIdempotencyRepository.complete
    typed_original = cast(Callable[..., object], original)
    state = {"failed_once": False}

    def flaky(self: SqlAlchemyIdempotencyRepository, *args: object, **kwargs: object) -> object:
        if not state["failed_once"]:
            state["failed_once"] = True
            raise RepoStorageError("commit outcome unknown")
        return typed_original(self, *args, **kwargs)

    monkeypatch.setattr(repo_module.SqlAlchemyIdempotencyRepository, "complete", flaky)
    with pytest.raises(ConsistencyStorageError):
        service.execute(
            scope="pipelines:create", key="key-u", request={"a": 1}, handler=_ok_outcome
        )
    monkeypatch.setattr(repo_module.SqlAlchemyIdempotencyRepository, "complete", original)
    # The reservation is still in progress: a same-lease retry is refused.
    with pytest.raises(IdempotencyInProgressError):
        service.execute(
            scope="pipelines:create", key="key-u", request={"a": 1}, handler=_ok_outcome
        )
    # After the bounded lease the retry reclaims and converges.
    clock.advance_seconds(6.0)
    converged = service.execute(
        scope="pipelines:create", key="key-u", request={"a": 1}, handler=_ok_outcome
    )
    assert converged.replayed is False


def test_stranded_reservations_are_counted_for_diagnostics(
    container: RuntimeContainer,
) -> None:
    clock = DeterministicClock()
    service = _service(container, clock, lease=60.0)
    service.execute(scope="pipelines:create", key="key-s", request={"a": 1}, handler=_ok_outcome)
    assert service.stranded_reservations() == 0
    _strand(container, "pipelines:create", "key-s2", {"a": 2}, clock)
    assert service.stranded_reservations() == 1


def test_terminal_failure_outcomes_are_replayed_verbatim(
    container: RuntimeContainer,
) -> None:
    clock = DeterministicClock()
    service = _service(container, clock, lease=60.0)

    def handler() -> CommandOutcome:
        return CommandOutcome(
            status_code=409,
            media_type="application/problem+json",
            body={"code": "duplicate_record"},
            terminal=True,
        )

    first = service.execute(scope="runs:create", key="key-f", request={"a": 1}, handler=handler)
    second = service.execute(scope="runs:create", key="key-f", request={"a": 1}, handler=handler)
    assert first.outcome.status_code == 409
    assert second.replayed is True
    assert second.outcome.body == {"code": "duplicate_record"}


@pytest.mark.anyio
async def test_http_replay_produces_exactly_one_durable_run(
    container: RuntimeContainer, client: httpx.AsyncClient
) -> None:
    await seed_scenario(client)
    payload = {
        "run_id": "run_idem-001",
        "pipeline_id": PIPELINE_ID,
        "pipeline_version": 1,
        "runner_kind": "sequential",
        "scenario_seed": 3,
    }
    headers = {"Idempotency-Key": "http-run-1"}
    first = await client.post("/api/v1/runs", json=payload, headers=headers)
    replay = await client.post("/api/v1/runs", json=payload, headers=headers)
    assert first.status_code == 201
    assert replay.status_code == 201
    assert replay.json() == first.json()
    with container.database.transaction() as session:
        count = session.execute(
            select(func.count()).select_from(runs).where(runs.c.run_id == "run_idem-001")
        ).scalar_one()
    assert count == 1


@pytest.mark.anyio
async def test_http_key_reuse_with_different_request_conflicts(
    client: httpx.AsyncClient,
) -> None:
    await seed_scenario(client)
    headers = {"Idempotency-Key": "http-run-2"}
    first = await client.post(
        "/api/v1/runs",
        json={
            "run_id": "run_idem-002",
            "pipeline_id": PIPELINE_ID,
            "pipeline_version": 1,
            "runner_kind": "sequential",
            "scenario_seed": 3,
        },
        headers=headers,
    )
    conflict = await client.post(
        "/api/v1/runs",
        json={
            "run_id": "run_idem-003",
            "pipeline_id": PIPELINE_ID,
            "pipeline_version": 1,
            "runner_kind": "sequential",
            "scenario_seed": 3,
        },
        headers=headers,
    )
    assert first.status_code == 201
    assert conflict.status_code == 409
    assert conflict.json()["code"] == "idempotency_key_reused"


@pytest.mark.anyio
async def test_invalid_idempotency_key_is_rejected(client: httpx.AsyncClient) -> None:
    response = await client.post(
        "/api/v1/pipelines",
        json={"pipeline_id": "pip_demo-alpha", "display_name": "d"},
        headers={"Idempotency-Key": "bad key!"},
    )
    assert response.status_code == 400
    assert response.json()["code"] == "invalid_idempotency_key"


@pytest.mark.anyio
async def test_restart_recovers_replay_and_stranded_ownership(
    container: RuntimeContainer, settings: Settings
) -> None:
    clock = DeterministicClock()
    service = _service(container, clock, lease=60.0)
    service.execute(
        scope="pipelines:create",
        key="key-restart",
        request={"pipeline_id": "pip_x"},
        handler=_ok_outcome,
    )
    # Simulate the process stopping with a stranded reservation.
    _strand(container, "pipelines:create", "key-strand", {"pipeline_id": "pip_strand"}, clock)
    # A fresh process composes over the same data root.
    from paritygrid.runtime.composition import compose_runtime

    second_container = compose_runtime(settings)
    try:
        restarted = DeterministicClock()
        restarted.advance_seconds(120.0)
        second_service = _service(second_container, restarted, lease=60.0)
        replayed = second_service.execute(
            scope="pipelines:create",
            key="key-restart",
            request={"pipeline_id": "pip_x"},
            handler=_ok_outcome,
        )
        assert replayed.replayed is True
        reclaimed = second_service.execute(
            scope="pipelines:create",
            key="key-strand",
            request={"pipeline_id": "pip_strand"},
            handler=_ok_outcome,
        )
        assert reclaimed.replayed is False
    finally:
        second_container.writer.close(timeout_seconds=5.0)
        second_container.database.close()


def test_repository_reclaim_rejects_digest_mismatch(
    container: RuntimeContainer,
) -> None:
    from paritygrid.adapters.persistence.repositories.idempotency import (
        SqlAlchemyIdempotencyRepository,
    )

    clock = DeterministicClock()
    with container.database.transaction() as session:
        repository = SqlAlchemyIdempotencyRepository(session)
        repository.begin(
            scope="pipelines:create",
            key="key-raw",
            request=ConfigurationDocument.from_mapping({"a": 1}),
            started_at=clock.now(),
        )
    with pytest.raises(IdempotencyConflictError), container.database.transaction() as session:
        SqlAlchemyIdempotencyRepository(session).reclaim(
            scope="pipelines:create",
            key="key-raw",
            request=ConfigurationDocument.from_mapping({"a": 2}),
            lease_expires_after_seconds=10.0,
            now=clock.now(),
        )
    later = DeterministicClock()
    later.advance_seconds(11.0)
    with container.database.transaction() as session:
        result = SqlAlchemyIdempotencyRepository(session).reclaim(
            scope="pipelines:create",
            key="key-raw",
            request=ConfigurationDocument.from_mapping({"a": 1}),
            lease_expires_after_seconds=10.0,
            now=later.now(),
        )
    assert isinstance(result.reservation, IdempotencyReservation)
    assert IdempotencyStatus(result.record.status) is IdempotencyStatus.IN_PROGRESS


def test_expired_reservation_has_one_exclusive_reclaim_owner(
    container: RuntimeContainer,
) -> None:
    clock = DeterministicClock()
    request: dict[str, object] = {"pipeline_id": "pip_exclusive-reclaim"}
    _strand(container, "pipelines:create", "exclusive-reclaim", request, clock)
    clock.advance_seconds(61.0)
    service = _service(container, clock, lease=60.0)
    start = Barrier(3)
    handler_started = Event()
    loser_finished = Event()
    release_handler = Event()
    handler_count = 0
    count_lock = Lock()

    def handler() -> CommandOutcome:
        nonlocal handler_count
        with count_lock:
            handler_count += 1
        handler_started.set()
        assert release_handler.wait(timeout=5.0)
        return _ok_outcome()

    def contend() -> str:
        start.wait(timeout=5.0)
        try:
            execution = service.execute(
                scope="pipelines:create",
                key="exclusive-reclaim",
                request=request,
                handler=handler,
            )
        except IdempotencyInProgressError:
            loser_finished.set()
            return "in_progress"
        return "replayed" if execution.replayed else "owner"

    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = [executor.submit(contend) for _ in range(2)]
        start.wait(timeout=5.0)
        assert handler_started.wait(timeout=5.0)
        assert loser_finished.wait(timeout=5.0)
        release_handler.set()
        outcomes = sorted(future.result(timeout=5.0) for future in futures)

    assert outcomes == ["in_progress", "owner"]
    assert handler_count == 1

"""Warehouse target: observable versions, idempotent effects, and failures."""

import asyncio
import json
from collections.abc import AsyncIterator

import httpx
import pytest
from hypothesis import given
from hypothesis import settings as hypothesis_settings
from hypothesis import strategies as st

from paritygrid.demo.datasets import (
    DatasetProfile,
    ScenarioSeed,
    ScenarioVersion,
    generate_dataset,
)
from paritygrid.demo.failures import (
    FailureScript,
    ScriptedFailure,
    ScriptedFailureKind,
)
from paritygrid.demo.simulators.warehouse import (
    SimulatedWarehouse,
    WarehouseBehavior,
    WarehouseError,
    WarehouseSettings,
)

pytestmark = pytest.mark.anyio
_CLIENT_TIMEOUT = httpx.Timeout(10.0)
_DATASET = generate_dataset(
    ScenarioSeed(555),
    ScenarioVersion(1),
    DatasetProfile(record_count=12, malformed_count=2, boundary_count=1, duplicate_count=1),
)


def _record_payload(sku: str, quantity: int = 5) -> dict[str, object]:
    return {
        "attributes": {"color": "blue"},
        "name": f"Record {sku}",
        "quantity": quantity,
        "sku": sku,
        "unit_price": {"amount": "10.50", "currency": "USD"},
        "updated_at": "2025-01-02T03:04:05.000000Z",
    }


@pytest.fixture
async def warehouse() -> AsyncIterator[SimulatedWarehouse]:
    simulator = SimulatedWarehouse()
    await simulator.start()
    try:
        yield simulator
    finally:
        await simulator.aclose()


async def _put(
    client: httpx.AsyncClient, base_url: str, sku: str, payload: dict[str, object], key: str
) -> httpx.Response:
    return await client.put(
        f"{base_url}/v1/records/{sku}", json=payload, headers={"Idempotency-Key": key}
    )


async def test_health_and_initial_state(warehouse: SimulatedWarehouse) -> None:
    async with httpx.AsyncClient(base_url=warehouse.base_url, timeout=_CLIENT_TIMEOUT) as client:
        health = await client.get("/healthz")
        assert health.status_code == 200
        assert health.json() == {"service": "warehouse", "status": "ok"}
        state = await client.get("/v1/state")
        document = state.json()
    assert document["target_version"] == 0
    assert document["record_count"] == 0
    assert document["content_fingerprint"] != ""
    assert document["capacity"] == 10_000


async def test_put_applies_effect_and_bumps_versions(warehouse: SimulatedWarehouse) -> None:
    async with httpx.AsyncClient(base_url=warehouse.base_url, timeout=_CLIENT_TIMEOUT) as client:
        response = await _put(
            client, warehouse.base_url, "GRID-0001", _record_payload("GRID-0001"), "key-1"
        )
        assert response.status_code == 200
        assert response.json() == {
            "outcome": "applied",
            "record_version": 1,
            "replayed": False,
            "target_version": 1,
        }
        fetched = await client.get("/v1/records/GRID-0001")
        assert fetched.status_code == 200
        document = fetched.json()
        assert document["payload"] == _record_payload("GRID-0001")
        assert document["record_version"] == 1
        assert document["sku"] == "GRID-0001"


async def test_replay_of_same_key_and_body_returns_original_outcome(
    warehouse: SimulatedWarehouse,
) -> None:
    async with httpx.AsyncClient(base_url=warehouse.base_url, timeout=_CLIENT_TIMEOUT) as client:
        first = await _put(
            client, warehouse.base_url, "GRID-0002", _record_payload("GRID-0002"), "key-2"
        )
        replay = await _put(
            client, warehouse.base_url, "GRID-0002", _record_payload("GRID-0002"), "key-2"
        )
    assert replay.status_code == 200
    assert replay.json() == {
        "outcome": "applied",
        "record_version": 1,
        "replayed": True,
        "target_version": 1,
    }
    assert replay.headers["idempotency-replayed"] == "true"
    assert first.json()["target_version"] == replay.json()["target_version"]


async def test_replaying_a_key_with_a_different_body_is_a_conflict(
    warehouse: SimulatedWarehouse,
) -> None:
    async with httpx.AsyncClient(base_url=warehouse.base_url, timeout=_CLIENT_TIMEOUT) as client:
        await _put(client, warehouse.base_url, "GRID-0003", _record_payload("GRID-0003"), "key-3")
        conflicting = await _put(
            client,
            warehouse.base_url,
            "GRID-0003",
            _record_payload("GRID-0003", quantity=99),
            "key-3",
        )
    assert conflicting.status_code == 409
    assert conflicting.json()["error"]["code"] == "idempotency_conflict"


async def test_identical_content_under_a_new_key_does_not_bump_versions(
    warehouse: SimulatedWarehouse,
) -> None:
    async with httpx.AsyncClient(base_url=warehouse.base_url, timeout=_CLIENT_TIMEOUT) as client:
        first = await _put(
            client, warehouse.base_url, "GRID-0004", _record_payload("GRID-0004"), "key-4a"
        )
        unchanged = await _put(
            client, warehouse.base_url, "GRID-0004", _record_payload("GRID-0004"), "key-4b"
        )
    assert first.json()["outcome"] == "applied"
    assert unchanged.json() == {
        "outcome": "unchanged",
        "record_version": 1,
        "replayed": False,
        "target_version": 1,
    }


async def test_content_change_bumps_record_and_target_versions(
    warehouse: SimulatedWarehouse,
) -> None:
    async with httpx.AsyncClient(base_url=warehouse.base_url, timeout=_CLIENT_TIMEOUT) as client:
        await _put(client, warehouse.base_url, "GRID-0005", _record_payload("GRID-0005"), "key-5a")
        changed = await _put(
            client,
            warehouse.base_url,
            "GRID-0005",
            _record_payload("GRID-0005", quantity=42),
            "key-5b",
        )
    assert changed.json() == {
        "outcome": "applied",
        "record_version": 2,
        "replayed": False,
        "target_version": 2,
    }


async def test_put_requires_canonical_idempotency_key_and_matching_sku(
    warehouse: SimulatedWarehouse,
) -> None:
    async with httpx.AsyncClient(base_url=warehouse.base_url, timeout=_CLIENT_TIMEOUT) as client:
        missing = await client.put("/v1/records/GRID-0006", json=_record_payload("GRID-0006"))
        assert missing.status_code == 400
        assert missing.json()["error"]["code"] == "missing_idempotency_key"
        mismatched = await client.put(
            "/v1/records/GRID-0006",
            json=_record_payload("OTHER-0001"),
            headers={"Idempotency-Key": "key-6"},
        )
        assert mismatched.status_code == 400
        assert mismatched.json()["error"]["code"] == "body_mismatch"
        invalid_body = await client.put(
            "/v1/records/GRID-0006",
            content=b"not json",
            headers={"Idempotency-Key": "key-6b"},
        )
        assert invalid_body.status_code == 400


async def test_fetch_missing_record_returns_404_and_delete_is_refused(
    warehouse: SimulatedWarehouse,
) -> None:
    async with httpx.AsyncClient(base_url=warehouse.base_url, timeout=_CLIENT_TIMEOUT) as client:
        missing = await client.get("/v1/records/GRID-NONE")
        assert missing.status_code == 404
        deleted = await client.delete("/v1/records/GRID-NONE", headers={"Idempotency-Key": "key-x"})
        assert deleted.status_code == 405
        assert "DELETE" not in deleted.headers.get("allow", "DELETE")


async def test_listing_is_bounded_sorted_and_cursor_paginated(
    warehouse: SimulatedWarehouse,
) -> None:
    async with httpx.AsyncClient(base_url=warehouse.base_url, timeout=_CLIENT_TIMEOUT) as client:
        for index in range(7):
            sku = f"GRID-{index:04d}"
            await _put(client, warehouse.base_url, sku, _record_payload(sku), f"list-{index}")
        first = await client.get("/v1/records", params={"limit": "3"})
        document = first.json()
        assert [record["sku"] for record in document["records"]] == [
            "GRID-0000",
            "GRID-0001",
            "GRID-0002",
        ]
        assert document["next_cursor"] != ""
        second = await client.get(
            "/v1/records", params={"limit": "3", "cursor": document["next_cursor"]}
        )
        second_document = second.json()
        assert [record["sku"] for record in second_document["records"]] == [
            "GRID-0003",
            "GRID-0004",
            "GRID-0005",
        ]
        huge = await client.get("/v1/records", params={"limit": "999999"})
        assert len(huge.json()["records"]) == 7
        invalid = await client.get("/v1/records", params={"limit": "0"})
        assert invalid.status_code == 400


async def test_content_fingerprint_is_order_independent_and_version_observable(
    warehouse: SimulatedWarehouse,
) -> None:
    other = SimulatedWarehouse()
    await other.start()
    try:
        async with httpx.AsyncClient(timeout=_CLIENT_TIMEOUT) as client:
            skus = [f"GRID-{index:04d}" for index in range(5)]
            for sku in skus:
                await _put(client, warehouse.base_url, sku, _record_payload(sku), f"finger-a-{sku}")
            for sku in reversed(skus):
                await _put(client, other.base_url, sku, _record_payload(sku), f"finger-b-{sku}")
            first_state = (await client.get(f"{warehouse.base_url}/v1/state")).json()
            second_state = (await client.get(f"{other.base_url}/v1/state")).json()
    finally:
        await other.aclose()
    assert first_state["content_fingerprint"] == second_state["content_fingerprint"]
    assert first_state["target_version"] == second_state["target_version"] == 5
    assert first_state["record_count"] == second_state["record_count"] == 5


async def test_content_fingerprint_changes_only_on_logical_change(
    warehouse: SimulatedWarehouse,
) -> None:
    async with httpx.AsyncClient(base_url=warehouse.base_url, timeout=_CLIENT_TIMEOUT) as client:
        await _put(client, warehouse.base_url, "GRID-0007", _record_payload("GRID-0007"), "k-7a")
        after_apply = (await client.get("/v1/state")).json()["content_fingerprint"]
        await _put(client, warehouse.base_url, "GRID-0007", _record_payload("GRID-0007"), "k-7b")
        after_unchanged = (await client.get("/v1/state")).json()["content_fingerprint"]
        await _put(
            client,
            warehouse.base_url,
            "GRID-0007",
            _record_payload("GRID-0007", quantity=8),
            "k-7c",
        )
        after_change = (await client.get("/v1/state")).json()["content_fingerprint"]
    assert after_apply == after_unchanged
    assert after_unchanged != after_change


async def test_capacity_is_enforced_with_a_typed_error() -> None:
    simulator = SimulatedWarehouse(settings=WarehouseSettings(capacity=2))
    await simulator.start()
    try:
        async with httpx.AsyncClient(
            base_url=simulator.base_url, timeout=_CLIENT_TIMEOUT
        ) as client:
            for index in range(2):
                sku = f"GRID-{index:04d}"
                await _put(client, simulator.base_url, sku, _record_payload(sku), f"cap-{index}")
            exhausted = await _put(
                client, simulator.base_url, "GRID-0009", _record_payload("GRID-0009"), "cap-9"
            )
            assert exhausted.status_code == 507
            assert exhausted.json()["error"]["code"] == "capacity_exceeded"
    finally:
        await simulator.aclose()


async def test_invalid_path_skus_are_rejected(warehouse: SimulatedWarehouse) -> None:
    async with httpx.AsyncClient(base_url=warehouse.base_url, timeout=_CLIENT_TIMEOUT) as client:
        for bad_sku in ("grid-0001", "GRID 1", "GRID/1", ""):
            response = await client.put(
                f"/v1/records/{bad_sku}",
                json=_record_payload("GRID-0001"),
                headers={"Idempotency-Key": "bad"},
            )
            assert response.status_code in (400, 404)


@pytest.mark.parametrize(
    ("failure", "expected_status"),
    [
        (
            ScriptedFailure(sequence=1, kind=ScriptedFailureKind.RATE_LIMIT, retry_after_seconds=4),
            429,
        ),
        (ScriptedFailure(sequence=1, kind=ScriptedFailureKind.TRANSIENT_ERROR), 503),
    ],
)
async def test_transport_failures_apply_to_mutations_then_recover(
    failure: ScriptedFailure, expected_status: int
) -> None:
    simulator = SimulatedWarehouse(FailureScript.from_entries([failure]))
    await simulator.start()
    try:
        async with httpx.AsyncClient(
            base_url=simulator.base_url, timeout=_CLIENT_TIMEOUT
        ) as client:
            first = await _put(
                client, simulator.base_url, "GRID-0010", _record_payload("GRID-0010"), "f-10a"
            )
            assert first.status_code == expected_status
            recovered = await _put(
                client, simulator.base_url, "GRID-0010", _record_payload("GRID-0010"), "f-10b"
            )
            assert recovered.status_code == 200
            state = (await client.get("/v1/state")).json()
            assert state["record_count"] == 1
    finally:
        await simulator.aclose()


@pytest.mark.parametrize(
    ("failure", "client_timeout"),
    [
        (
            ScriptedFailure(
                sequence=1,
                kind=ScriptedFailureKind.TIMEOUT,
                delay_microseconds=400_000,
            ),
            httpx.Timeout(0.05),
        ),
        (
            ScriptedFailure(
                sequence=1,
                kind=ScriptedFailureKind.CONNECTION_LOSS,
                partial_bytes=24,
            ),
            _CLIENT_TIMEOUT,
        ),
    ],
)
async def test_ambiguous_transport_failures_commit_once_then_replay_idempotently(
    failure: ScriptedFailure, client_timeout: httpx.Timeout
) -> None:
    simulator = SimulatedWarehouse(FailureScript.from_entries([failure]))
    await simulator.start()
    try:
        async with httpx.AsyncClient(base_url=simulator.base_url, timeout=client_timeout) as client:
            with pytest.raises(httpx.TransportError):
                await _put(
                    client,
                    simulator.base_url,
                    "GRID-0011",
                    _record_payload("GRID-0011"),
                    "ambiguous-11",
                )
        async with httpx.AsyncClient(
            base_url=simulator.base_url, timeout=_CLIENT_TIMEOUT
        ) as client:
            committed = (await client.get("/v1/state")).json()
            assert committed["record_count"] == 1
            assert committed["target_version"] == 1
            replayed_once = await _put(
                client,
                simulator.base_url,
                "GRID-0011",
                _record_payload("GRID-0011"),
                "ambiguous-11",
            )
            replayed_twice = await _put(
                client,
                simulator.base_url,
                "GRID-0011",
                _record_payload("GRID-0011"),
                "ambiguous-11",
            )
            state_after_replays = (await client.get("/v1/state")).json()
        expected_replay = {
            "outcome": "applied",
            "record_version": 1,
            "replayed": True,
            "target_version": 1,
        }
        assert replayed_once.status_code == replayed_twice.status_code == 200
        assert replayed_once.json() == replayed_twice.json() == expected_replay
        assert replayed_once.headers["idempotency-replayed"] == "true"
        assert replayed_twice.headers["idempotency-replayed"] == "true"
        assert state_after_replays == committed
        assert simulator.request_count() == 3
    finally:
        await simulator.aclose()


async def test_warehouse_hang_is_cancellable_without_losing_liveness() -> None:
    script = FailureScript.from_entries(
        [ScriptedFailure(sequence=1, kind=ScriptedFailureKind.HANG, delay_microseconds=5_000_000)]
    )
    simulator = SimulatedWarehouse(script)
    await simulator.start()
    try:
        async with httpx.AsyncClient(
            base_url=simulator.base_url, timeout=_CLIENT_TIMEOUT
        ) as client:
            with pytest.raises(TimeoutError):
                await asyncio.wait_for(
                    _put(
                        client,
                        simulator.base_url,
                        "GRID-0013",
                        _record_payload("GRID-0013"),
                        "f-13",
                    ),
                    timeout=0.1,
                )
            health = await client.get("/healthz")
            assert health.status_code == 200
            applied = simulator.applied_failures()
            assert [item.kind for item in applied] == [ScriptedFailureKind.HANG]
    finally:
        await simulator.aclose()


async def test_warehouse_reads_do_not_consume_failure_sequences(
    warehouse: SimulatedWarehouse,
) -> None:
    script = FailureScript.from_entries(
        [ScriptedFailure(sequence=1, kind=ScriptedFailureKind.TRANSIENT_ERROR)]
    )
    simulator = SimulatedWarehouse(script)
    await simulator.start()
    try:
        async with httpx.AsyncClient(
            base_url=simulator.base_url, timeout=_CLIENT_TIMEOUT
        ) as client:
            for _ in range(3):
                response = await client.get("/v1/state")
                assert response.status_code == 200
            failing = await _put(
                client, simulator.base_url, "GRID-0014", _record_payload("GRID-0014"), "f-14"
            )
            assert failing.status_code == 503
        assert simulator.request_count() == 1
    finally:
        await simulator.aclose()


def test_warehouse_rejects_response_shape_failure_kinds() -> None:
    script = FailureScript.from_entries(
        [ScriptedFailure(sequence=1, kind=ScriptedFailureKind.DUPLICATE_RECORDS)]
    )
    with pytest.raises(ValueError, match="warehouse scripts may not use"):
        WarehouseBehavior(script)


def test_warehouse_settings_are_validated() -> None:
    with pytest.raises(WarehouseError):
        WarehouseSettings(capacity=0)
    with pytest.raises(WarehouseError):
        WarehouseSettings(max_list_page_size=501)


async def test_repeated_replay_stays_idempotent_under_many_keys(
    warehouse: SimulatedWarehouse,
) -> None:
    async with httpx.AsyncClient(base_url=warehouse.base_url, timeout=_CLIENT_TIMEOUT) as client:
        outcomes: list[dict[str, object]] = []
        for _attempt in range(4):
            response = await _put(
                client,
                warehouse.base_url,
                "GRID-0015",
                _record_payload("GRID-0015"),
                "replay-key",
            )
            outcomes.append(response.json())
        state = (await client.get("/v1/state")).json()
    assert outcomes[0]["outcome"] == "applied"
    for replayed in outcomes[1:]:
        assert replayed["replayed"] is True
        assert replayed["target_version"] == outcomes[0]["target_version"]
    assert state["target_version"] == 1
    assert state["record_count"] == 1


@hypothesis_settings(max_examples=25, deadline=None)
@given(
    order_a=st.permutations(["A-1", "B-2", "C-3"]),
    order_b=st.permutations(["A-1", "B-2", "C-3"]),
)
async def test_fingerprint_and_versions_are_insertion_order_independent(
    order_a: list[str], order_b: list[str]
) -> None:
    first = WarehouseBehavior(FailureScript.empty())
    second = WarehouseBehavior(FailureScript.empty())
    for index, sku in enumerate(order_a):
        _apply(first, sku, _record_payload(sku), f"key-a-{index}")
    for index, sku in enumerate(order_b):
        _apply(second, sku, _record_payload(sku), f"key-b-{index}")
    assert first.content_fingerprint() == second.content_fingerprint()
    assert first.target_version == second.target_version == 3
    assert first.record_count == second.record_count == 3


def _apply(behavior: WarehouseBehavior, sku: str, payload: dict[str, object], key: str) -> None:
    from paritygrid.demo.simulators.http_wire import HttpRequest

    request = HttpRequest(
        method="PUT",
        path=f"/v1/records/{sku}",
        query={},
        headers={"idempotency-key": key},
        body=json.dumps(payload, ensure_ascii=True, separators=(",", ":"), sort_keys=True).encode(
            "ascii"
        ),
    )
    behavior.handle(request)

"""Warehouse target connector integration: idempotent effects under replay."""

import pytest

from paritygrid.adapters.connectors import (
    ConnectorAmbiguousError,
    ConnectorAuthentication,
    ConnectorCallBounds,
    ConnectorCallContext,
    ConnectorConflictError,
    ConnectorEvent,
    ConnectorEventKind,
    ConnectorKind,
    ConnectorLifecycleError,
    ConnectorRateLimitedError,
    ConnectorRetryableError,
    ConnectorServerFailureError,
    ConnectorState,
    TargetEffectOutcome,
    TargetWritePrecondition,
    TargetWriteRequest,
    WarehouseTargetConfig,
    WarehouseTargetConnector,
    derive_idempotency_key,
)
from paritygrid.adapters.connectors.redaction import SecretMaterial
from paritygrid.demo.datasets import SyntheticDataset
from paritygrid.demo.failures import (
    FailureScript,
    ScriptedFailure,
    ScriptedFailureKind,
)
from paritygrid.demo.simulators.warehouse import SimulatedWarehouse

pytestmark = pytest.mark.anyio

_FAST = ConnectorCallBounds(request_timeout_microseconds=2_000_000)


def _context() -> ConnectorCallContext:
    return ConnectorCallContext(correlation_id="target-call-1")


def _request(
    sku: str,
    payload: dict[str, object] | None = None,
    key: str | None = None,
) -> TargetWriteRequest:
    body: dict[str, object] = payload if payload is not None else {"sku": sku, "name": "Part"}
    return TargetWriteRequest(
        sku=sku,
        payload=body,
        idempotency_key=key if key is not None else derive_idempotency_key("pg", _raw(sku, body)),
    )


def _raw(sku: str, payload: dict[str, object]) -> TargetWriteRequest:
    return TargetWriteRequest(sku=sku, payload=payload, idempotency_key=f"derive:{sku}")


def _scripted(*failures: ScriptedFailure) -> FailureScript:
    return FailureScript.from_entries(failures)


async def _connector(
    base_url: str, bounds: ConnectorCallBounds = _FAST
) -> WarehouseTargetConnector:
    connector = WarehouseTargetConnector(WarehouseTargetConfig(base_url, bounds=bounds))
    await connector.open_async()
    return connector


class TestIdempotency:
    async def test_first_write_applies_one_effect(self, warehouse: SimulatedWarehouse) -> None:
        connector = await _connector(warehouse.base_url)
        try:
            outcome = await connector.write_record_async(_request("GRID-1"), _context())
            assert outcome.outcome is TargetEffectOutcome.APPLIED
            assert outcome.record_version == 1
        finally:
            await connector.aclose()
        assert warehouse.behavior.target_version == 1

    async def test_replay_with_the_same_key_replays_without_a_second_effect(
        self, warehouse: SimulatedWarehouse
    ) -> None:
        connector = await _connector(warehouse.base_url)
        try:
            request = _request("GRID-1")
            first = await connector.write_record_async(request, _context())
            replay = await connector.write_record_async(request, _context())
            replay_again = await connector.write_record_async(request, _context())
        finally:
            await connector.aclose()
        assert first.outcome is TargetEffectOutcome.APPLIED
        assert replay.outcome is TargetEffectOutcome.REPLAYED
        assert replay_again.outcome is TargetEffectOutcome.REPLAYED
        assert replay.record_version == replay_again.record_version == 1
        assert warehouse.behavior.target_version == 1

    async def test_repeated_calls_with_the_same_identity_are_logically_once(
        self, warehouse: SimulatedWarehouse
    ) -> None:
        connector = await _connector(warehouse.base_url)
        try:
            for _ in range(5):
                await connector.write_record_async(_request("GRID-1"), _context())
        finally:
            await connector.aclose()
        assert warehouse.behavior.record_count == 1
        assert warehouse.behavior.target_version == 1

    async def test_same_content_under_a_new_key_is_unchanged(
        self, warehouse: SimulatedWarehouse
    ) -> None:
        connector = await _connector(warehouse.base_url)
        try:
            first = await connector.write_record_async(_request("GRID-1", key="key-1"), _context())
            second = await connector.write_record_async(_request("GRID-1", key="key-2"), _context())
        finally:
            await connector.aclose()
        assert first.outcome is TargetEffectOutcome.APPLIED
        assert second.outcome is TargetEffectOutcome.UNCHANGED
        assert warehouse.behavior.target_version == 1

    async def test_key_reuse_with_a_different_payload_is_a_terminal_conflict(
        self, warehouse: SimulatedWarehouse
    ) -> None:
        connector = await _connector(warehouse.base_url)
        try:
            await connector.write_record_async(
                _request("GRID-1", payload={"sku": "GRID-1", "name": "A"}, key="shared-key"),
                _context(),
            )
            with pytest.raises(ConnectorConflictError) as details:
                await connector.write_record_async(
                    _request("GRID-1", payload={"sku": "GRID-1", "name": "B"}, key="shared-key"),
                    _context(),
                )
            assert details.value.disposition.value == "conflict"
        finally:
            await connector.aclose()
        assert warehouse.behavior.target_version == 1

    async def test_derived_keys_bind_identity_and_payload(
        self, warehouse: SimulatedWarehouse
    ) -> None:
        request = _request("GRID-1")
        same = _request("GRID-1")
        changed = _request("GRID-1", payload={"sku": "GRID-1", "name": "Other"})
        assert derive_idempotency_key("pg", _raw("GRID-1", dict(request.payload))) == (
            derive_idempotency_key("pg", _raw("GRID-1", dict(same.payload)))
        )
        assert derive_idempotency_key("pg", _raw("GRID-1", dict(request.payload))) != (
            derive_idempotency_key("pg", _raw("GRID-1", dict(changed.payload)))
        )
        connector = await _connector(warehouse.base_url)
        try:
            await connector.write_record_async(request, _context())
            replay = await connector.write_record_async(same, _context())
        finally:
            await connector.aclose()
        assert replay.outcome is TargetEffectOutcome.REPLAYED

    async def test_derived_keys_bind_the_target_precondition(
        self, warehouse: SimulatedWarehouse
    ) -> None:
        payload = {"sku": "GRID-1", "name": "Part"}
        absent = TargetWriteRequest(
            sku="GRID-1",
            payload=payload,
            idempotency_key="derive:absent",
            precondition=TargetWritePrecondition.must_be_absent(),
        )
        expected = TargetWriteRequest(
            sku="GRID-1",
            payload=payload,
            idempotency_key="derive:expected",
            precondition=TargetWritePrecondition.expected_payload(payload),
        )
        assert derive_idempotency_key("pg", absent) != derive_idempotency_key("pg", expected)

    async def test_create_precondition_rejects_a_racing_insert_without_mutation(
        self, warehouse: SimulatedWarehouse
    ) -> None:
        connector = await _connector(warehouse.base_url)
        try:
            intervening = _request(
                "GRID-1", {"sku": "GRID-1", "name": "Intervening"}, key="intervening"
            )
            await connector.write_record_async(intervening, _context())
            guarded = TargetWriteRequest(
                sku="GRID-1",
                payload={"sku": "GRID-1", "name": "Repair proposal"},
                idempotency_key="conditional-create",
                precondition=TargetWritePrecondition.must_be_absent(),
            )
            with pytest.raises(ConnectorConflictError):
                await connector.write_record_async(guarded, _context())
            stored = await connector.read_record_async("GRID-1", _context())
        finally:
            await connector.aclose()
        assert stored is not None
        assert stored.payload == intervening.payload
        assert warehouse.behavior.target_version == 1

    async def test_update_precondition_rejects_a_stale_preimage_without_mutation(
        self, warehouse: SimulatedWarehouse
    ) -> None:
        connector = await _connector(warehouse.base_url)
        try:
            expected_payload = {"sku": "GRID-1", "name": "Expected"}
            await connector.write_record_async(
                _request("GRID-1", expected_payload, key="expected"), _context()
            )
            intervening = _request(
                "GRID-1", {"sku": "GRID-1", "name": "Intervening"}, key="intervening"
            )
            await connector.write_record_async(intervening, _context())
            guarded = TargetWriteRequest(
                sku="GRID-1",
                payload={"sku": "GRID-1", "name": "Repair proposal"},
                idempotency_key="conditional-update",
                precondition=TargetWritePrecondition.expected_payload(expected_payload),
            )
            with pytest.raises(ConnectorConflictError):
                await connector.write_record_async(guarded, _context())
            stored = await connector.read_record_async("GRID-1", _context())
        finally:
            await connector.aclose()
        assert stored is not None
        assert stored.payload == intervening.payload
        assert warehouse.behavior.target_version == 2

    async def test_replay_resolves_stored_effect_before_rechecking_precondition(
        self, warehouse: SimulatedWarehouse
    ) -> None:
        connector = await _connector(warehouse.base_url)
        try:
            guarded = TargetWriteRequest(
                sku="GRID-1",
                payload={"sku": "GRID-1", "name": "Repair proposal"},
                idempotency_key="conditional-replay",
                precondition=TargetWritePrecondition.must_be_absent(),
            )
            first = await connector.write_record_async(guarded, _context())
            intervening = _request(
                "GRID-1", {"sku": "GRID-1", "name": "Intervening"}, key="intervening"
            )
            await connector.write_record_async(intervening, _context())
            replay = await connector.write_record_async(guarded, _context())
            stored = await connector.read_record_async("GRID-1", _context())
        finally:
            await connector.aclose()
        assert first.outcome is TargetEffectOutcome.APPLIED
        assert replay.outcome is TargetEffectOutcome.REPLAYED
        assert stored is not None
        assert stored.payload == intervening.payload
        assert warehouse.behavior.target_version == 2


class TestAmbiguousOutcomes:
    async def test_connection_loss_after_send_is_ambiguous_never_retryable(self) -> None:
        sim = SimulatedWarehouse(
            _scripted(
                ScriptedFailure(
                    sequence=1, kind=ScriptedFailureKind.CONNECTION_LOSS, partial_bytes=24
                )
            )
        )
        await sim.start()
        try:
            connector = await _connector(sim.base_url)
            try:
                with pytest.raises(ConnectorAmbiguousError) as details:
                    await connector.write_record_async(_request("GRID-1"), _context())
                # Unknown outcome fails closed: the connector refuses to
                # call it retryable or resolved.
                assert details.value.disposition.value == "permanent"
            finally:
                await connector.aclose()
        finally:
            await sim.aclose()

    async def test_write_deadline_expiry_is_ambiguous(self) -> None:
        sim = SimulatedWarehouse(
            _scripted(
                ScriptedFailure(
                    sequence=1, kind=ScriptedFailureKind.HANG, delay_microseconds=5_000_000
                )
            )
        )
        await sim.start()
        try:
            bounds = ConnectorCallBounds(request_timeout_microseconds=150_000)
            connector = await _connector(sim.base_url, bounds)
            try:
                with pytest.raises(ConnectorAmbiguousError):
                    await connector.write_record_async(_request("GRID-1"), _context())
            finally:
                await connector.aclose()
        finally:
            await sim.aclose()

    async def test_ambiguous_write_resolves_by_same_key_replay_without_second_effect(
        self,
    ) -> None:
        sim = SimulatedWarehouse(
            _scripted(
                ScriptedFailure(
                    sequence=1,
                    kind=ScriptedFailureKind.CONNECTION_LOSS,
                    partial_bytes=24,
                )
            )
        )
        await sim.start()
        try:
            connector = await _connector(sim.base_url)
            try:
                request = _request("GRID-1")
                with pytest.raises(ConnectorAmbiguousError):
                    await connector.write_record_async(request, _context())
                # The effect may or may not have committed; replaying the
                # same identity resolves to exactly one logical effect.
                resolved = await connector.write_record_async(request, _context())
                assert resolved.outcome in (
                    TargetEffectOutcome.APPLIED,
                    TargetEffectOutcome.REPLAYED,
                )
                assert resolved.record_version == 1
                confirmed = await connector.write_record_async(request, _context())
                assert confirmed.outcome is TargetEffectOutcome.REPLAYED
                assert confirmed.record_version == 1
            finally:
                await connector.aclose()
            assert sim.behavior.target_version == 1
            assert sim.behavior.record_count == 1
        finally:
            await sim.aclose()


class TestFailureClassification:
    async def test_rate_limit_is_retryable_with_delay(
        self, connector_dataset: SyntheticDataset
    ) -> None:
        sim = SimulatedWarehouse(
            _scripted(
                ScriptedFailure(
                    sequence=1, kind=ScriptedFailureKind.RATE_LIMIT, retry_after_seconds=6
                )
            )
        )
        await sim.start()
        try:
            connector = await _connector(sim.base_url)
            try:
                with pytest.raises(ConnectorRateLimitedError) as details:
                    await connector.write_record_async(_request("GRID-1"), _context())
                assert details.value.retry_after_seconds == 6
                outcome = await connector.write_record_async(_request("GRID-1"), _context())
                assert outcome.outcome is TargetEffectOutcome.APPLIED
            finally:
                await connector.aclose()
        finally:
            await sim.aclose()

    async def test_transient_error_is_a_server_failure(
        self, connector_dataset: SyntheticDataset
    ) -> None:
        sim = SimulatedWarehouse(
            _scripted(ScriptedFailure(sequence=1, kind=ScriptedFailureKind.TRANSIENT_ERROR))
        )
        await sim.start()
        try:
            connector = await _connector(sim.base_url)
            try:
                with pytest.raises(ConnectorServerFailureError):
                    await connector.write_record_async(_request("GRID-1"), _context())
            finally:
                await connector.aclose()
        finally:
            await sim.aclose()

    async def test_call_context_secrets_are_redacted_from_status_bodies(self) -> None:
        secret = "The warehouse is temporarily unavailable."
        sim = SimulatedWarehouse(
            _scripted(ScriptedFailure(sequence=1, kind=ScriptedFailureKind.TRANSIENT_ERROR))
        )
        await sim.start()
        try:
            connector = await _connector(sim.base_url)
            try:
                context = ConnectorCallContext(secrets=SecretMaterial((secret,)))
                with pytest.raises(ConnectorServerFailureError) as details:
                    await connector.write_record_async(_request("GRID-1"), context)
                assert secret not in details.value.detail
            finally:
                await connector.aclose()
        finally:
            await sim.aclose()

    async def test_capacity_exhaustion_follows_the_closed_5xx_class(self) -> None:
        from paritygrid.demo.simulators.warehouse import WarehouseSettings

        sim = SimulatedWarehouse(FailureScript.empty(), settings=WarehouseSettings(capacity=1))
        await sim.start()
        try:
            connector = await _connector(sim.base_url)
            try:
                await connector.write_record_async(_request("GRID-1"), _context())
                # 507 sits in the closed 5xx retryable class; the connector
                # adds no status-specific branch to the execution contract.
                with pytest.raises(ConnectorServerFailureError):
                    await connector.write_record_async(_request("GRID-2"), _context())
            finally:
                await connector.aclose()
        finally:
            await sim.aclose()

    async def test_connect_failure_before_send_is_retryable(self) -> None:
        import socket

        # A bound socket that never listens refuses every connection
        # deterministically, without racing an ephemeral port reuse.
        held = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        held.bind(("127.0.0.1", 0))
        refused_port = held.getsockname()[1]
        try:
            connector = await _connector(f"http://127.0.0.1:{refused_port}")
            try:
                with pytest.raises(ConnectorRetryableError):
                    await connector.write_record_async(_request("GRID-1"), _context())
            finally:
                await connector.aclose()
        finally:
            held.close()


class TestReads:
    async def test_read_record_round_trip_and_absence(self, warehouse: SimulatedWarehouse) -> None:
        connector = await _connector(warehouse.base_url)
        try:
            assert await connector.read_record_async("GRID-1", _context()) is None
            await connector.write_record_async(_request("GRID-1"), _context())
            record = await connector.read_record_async("GRID-1", _context())
            assert record is not None
            assert record.sku == "GRID-1"
            assert record.record_version == 1
        finally:
            await connector.aclose()

    async def test_state_snapshot_reports_target_truth(self, warehouse: SimulatedWarehouse) -> None:
        connector = await _connector(warehouse.base_url)
        try:
            empty = await connector.state_snapshot_async(_context())
            assert empty.record_count == 0
            assert empty.target_version == 0
            await connector.write_record_async(_request("GRID-1"), _context())
            state = await connector.state_snapshot_async(_context())
            assert state.record_count == 1
            assert state.target_version == 1
            assert state.content_fingerprint == warehouse.behavior.content_fingerprint()
        finally:
            await connector.aclose()

    async def test_list_records_pages_with_configured_bound(
        self, warehouse: SimulatedWarehouse
    ) -> None:
        connector = await _connector(warehouse.base_url, ConnectorCallBounds(max_page_records=1))
        try:
            for sku in ("GRID-1", "GRID-2", "GRID-3"):
                await connector.write_record_async(_request(sku), _context())
            first = await connector.list_records_async(None, _context())
            second = await connector.list_records_async(first.next_cursor, _context())
            third = await connector.list_records_async(second.next_cursor, _context())
        finally:
            await connector.aclose()
        assert [record.sku for record in first.records] == ["GRID-1"]
        assert [record.sku for record in second.records] == ["GRID-2"]
        assert [record.sku for record in third.records] == ["GRID-3"]
        assert third.next_cursor is None


class TestLifecycleAndRedaction:
    async def test_lifecycle_transitions_and_close_is_idempotent(
        self, warehouse: SimulatedWarehouse
    ) -> None:
        connector = WarehouseTargetConnector(WarehouseTargetConfig(warehouse.base_url))
        assert connector.state() is ConnectorState.CREATED
        with pytest.raises(ConnectorLifecycleError):
            await connector.write_record_async(_request("GRID-1"), _context())
        await connector.open_async()
        with pytest.raises(ConnectorLifecycleError):
            await connector.open_async()
        await connector.aclose()
        assert connector.state() is ConnectorState.CLOSED
        with pytest.raises(ConnectorLifecycleError):
            await connector.write_record_async(_request("GRID-1"), _context())
        await connector.aclose()

    async def test_write_events_carry_effects_not_secrets(
        self, warehouse: SimulatedWarehouse
    ) -> None:
        token = "tok_warehouse_secret_1"
        events: list[ConnectorEvent] = []
        connector = WarehouseTargetConnector(
            WarehouseTargetConfig(warehouse.base_url),
            authentication=ConnectorAuthentication(token=token),
            observers=[events.append],
        )
        await connector.open_async()
        try:
            await connector.write_record_async(_request("GRID-1"), _context())
        finally:
            await connector.aclose()
        rendered = repr(events) + str(events)
        assert token not in rendered
        kinds = [event.kind for event in events]
        assert ConnectorEventKind.TARGET_EFFECT_OBSERVED in kinds
        assert ConnectorEventKind.CLOSED in kinds
        assert all(event.connector_kind is ConnectorKind.WAREHOUSE_TARGET for event in events)

    async def test_applied_write_emits_the_effect_event(
        self, warehouse: SimulatedWarehouse
    ) -> None:
        events: list[ConnectorEvent] = []
        connector = WarehouseTargetConnector(
            WarehouseTargetConfig(warehouse.base_url), observers=[events.append]
        )
        await connector.open_async()
        try:
            await connector.write_record_async(_request("GRID-1"), _context())
        finally:
            await connector.aclose()
        effects = [e for e in events if e.kind is ConnectorEventKind.TARGET_EFFECT_OBSERVED]
        assert effects
        assert effects[0].details["effect"] == "applied"

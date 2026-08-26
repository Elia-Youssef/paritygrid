"""Connector contract unit tests: bounds, cursors, errors, events, context."""

import pytest

from paritygrid.adapters.connectors import (
    CONNECTOR_CAPABILITIES_PROTOCOL,
    CONNECTOR_CONTRACT_VERSION,
    NEVER_CANCELLED,
    ConnectorAmbiguousError,
    ConnectorAuthentication,
    ConnectorCallBounds,
    ConnectorCallContext,
    ConnectorCancelledError,
    ConnectorCapabilitiesV1,
    ConnectorConflictError,
    ConnectorContractError,
    ConnectorError,
    ConnectorEvent,
    ConnectorEventKind,
    ConnectorEventPublisher,
    ConnectorKind,
    ConnectorLoopError,
    ConnectorPermanentError,
    ConnectorRateLimitedError,
    ConnectorRetryableError,
    ConnectorServerFailureError,
    ConnectorState,
    ConnectorTimeoutError,
    ConnectorUnknownError,
    ConnectorValidationError,
    EventCancellationToken,
    FileReadBounds,
    SourceOutcome,
    SourcePage,
    SourceRecord,
    TargetEffectOutcome,
    TargetRecord,
    TargetStateSnapshot,
    TargetWriteOutcome,
    TargetWriteRequest,
    describe_connector_error,
    require_no_running_loop,
    validate_base_url,
    validate_cursor,
    validate_idempotency_key,
    validate_sku,
)
from paritygrid.adapters.connectors.redaction import SecretMaterial
from paritygrid.application.planner.connectors import ConnectorCapability, ConnectorCapabilitySet
from paritygrid.domain.execution.failures import (
    FailureClassification,
    FailureDisposition,
    disposition_for,
)

pytestmark = pytest.mark.anyio


def test_runner_neutral_contract_is_owned_by_application_ports() -> None:
    assert ConnectorCallContext.__module__ == "paritygrid.application.ports.connectors"
    assert SecretMaterial.__module__ == "paritygrid.application.ports.connector_redaction"


def _capabilities(kind: ConnectorKind = ConnectorKind.ASYNC_HTTP_SOURCE) -> ConnectorCapabilitiesV1:
    return ConnectorCapabilitiesV1(
        protocol=CONNECTOR_CAPABILITIES_PROTOCOL,
        contract_version=CONNECTOR_CONTRACT_VERSION,
        kind=kind,
        capabilities=ConnectorCapabilitySet((ConnectorCapability.READ,)),
        max_page_records=10,
        supports_cursors=True,
    )


class TestCapabilities:
    def test_valid_capabilities_round_trip(self) -> None:
        capabilities = _capabilities()
        assert capabilities.supports(ConnectorCapability.READ)
        assert not capabilities.supports(ConnectorCapability.WRITE)
        assert capabilities.contract_version == CONNECTOR_CONTRACT_VERSION

    def test_unknown_protocol_is_rejected(self) -> None:
        with pytest.raises(ConnectorContractError, match="protocol"):
            ConnectorCapabilitiesV1(
                protocol="paritygrid.unknown.v9",
                contract_version=CONNECTOR_CONTRACT_VERSION,
                kind=ConnectorKind.CSV_SOURCE,
                capabilities=ConnectorCapabilitySet((ConnectorCapability.READ,)),
                max_page_records=1,
                supports_cursors=True,
            )

    def test_page_bound_is_enforced(self) -> None:
        with pytest.raises(ConnectorContractError, match="page records"):
            ConnectorCapabilitiesV1(
                protocol=CONNECTOR_CAPABILITIES_PROTOCOL,
                contract_version=CONNECTOR_CONTRACT_VERSION,
                kind=ConnectorKind.CSV_SOURCE,
                capabilities=ConnectorCapabilitySet((ConnectorCapability.READ,)),
                max_page_records=201,
                supports_cursors=False,
            )

    def test_zero_page_records_marks_non_paging_kind(self) -> None:
        capabilities = ConnectorCapabilitiesV1(
            protocol=CONNECTOR_CAPABILITIES_PROTOCOL,
            contract_version=CONNECTOR_CONTRACT_VERSION,
            kind=ConnectorKind.WAREHOUSE_TARGET,
            capabilities=ConnectorCapabilitySet((ConnectorCapability.WRITE,)),
            max_page_records=0,
            supports_cursors=False,
        )
        assert capabilities.max_page_records == 0


class TestBounds:
    def test_default_call_bounds_are_within_limits(self) -> None:
        bounds = ConnectorCallBounds()
        assert 1 <= bounds.request_timeout_microseconds <= 60_000_000
        assert bounds.max_page_records == 200

    @pytest.mark.parametrize("field", ["request_timeout_microseconds", "max_response_bytes"])
    def test_out_of_range_values_are_rejected(self, field: str) -> None:
        with pytest.raises(ConnectorContractError, match=field):
            ConnectorCallBounds(**{field: 0})

    def test_boolean_bounds_are_rejected(self) -> None:
        with pytest.raises(ConnectorContractError, match="integer"):
            ConnectorCallBounds(max_page_records=True)  # type: ignore[arg-type]

    def test_file_bounds_reject_zero(self) -> None:
        with pytest.raises(ConnectorContractError, match="max_rows"):
            FileReadBounds(max_rows=0)


class TestCursorAndIdentity:
    def test_valid_cursors_are_accepted(self) -> None:
        assert validate_cursor("page:0000000010") == "page:0000000010"

    @pytest.mark.parametrize("cursor", ["", "has space", "p" * 129, "unicode-λ"])
    def test_invalid_cursors_are_rejected(self, cursor: str) -> None:
        with pytest.raises(ConnectorValidationError):
            validate_cursor(cursor)

    def test_non_text_cursor_is_rejected(self) -> None:
        with pytest.raises(ConnectorValidationError):
            validate_cursor(12)  # type: ignore[arg-type]

    def test_valid_sku_is_accepted(self) -> None:
        assert validate_sku("GRID-001") == "GRID-001"

    @pytest.mark.parametrize("sku", ["", "lower", "GRID_", "HAS SPACE"])
    def test_invalid_sku_is_rejected(self, sku: str) -> None:
        with pytest.raises(ConnectorValidationError):
            validate_sku(sku)

    def test_idempotency_key_shape(self) -> None:
        assert validate_idempotency_key("pg-write:GRID-1:abc") == "pg-write:GRID-1:abc"
        with pytest.raises(ConnectorValidationError):
            validate_idempotency_key("-leading")

    @pytest.mark.parametrize("url", ["http://127.0.0.1:8000", "http://localhost"])
    def test_valid_base_urls(self, url: str) -> None:
        assert validate_base_url(url) == url

    @pytest.mark.parametrize(
        "url",
        [
            "https://example.com",
            "http://user:pass@example.com",
            "ftp://example.com",
            "http://",
            "not a url",
            "",
        ],
    )
    def test_invalid_base_urls(self, url: str) -> None:
        with pytest.raises(ConnectorContractError):
            validate_base_url(url)


class TestErrorTaxonomy:
    def test_every_closed_classification_has_a_disposition(self) -> None:
        for classification in FailureClassification:
            assert disposition_for(classification) in list(FailureDisposition)

    @pytest.mark.parametrize(
        ("error_type", "classification"),
        [
            (ConnectorRetryableError, FailureClassification.CONNECTION),
            (ConnectorTimeoutError, FailureClassification.TIMEOUT),
            (ConnectorRateLimitedError, FailureClassification.HTTP_429),
            (ConnectorServerFailureError, FailureClassification.HTTP_5XX),
            (ConnectorPermanentError, FailureClassification.HTTP_4XX),
            (ConnectorValidationError, FailureClassification.VALIDATION),
            (ConnectorConflictError, FailureClassification.IDEMPOTENCY_CONFLICT),
            (ConnectorCancelledError, FailureClassification.USER_CANCELLATION),
            (ConnectorAmbiguousError, FailureClassification.UNKNOWN),
            (ConnectorUnknownError, FailureClassification.UNKNOWN),
        ],
    )
    def test_classifications_match_the_execution_contract(
        self, error_type: type[ConnectorError], classification: FailureClassification
    ) -> None:
        error = error_type("summary")
        assert error.classification is classification
        assert error.disposition is disposition_for(classification)

    def test_retry_dispositions_follow_the_execution_model(self) -> None:
        assert ConnectorRetryableError("x").disposition is FailureDisposition.RETRY
        assert ConnectorValidationError("x").disposition is FailureDisposition.QUARANTINE
        assert ConnectorConflictError("x").disposition is FailureDisposition.CONFLICT
        assert ConnectorCancelledError("x").disposition is FailureDisposition.CANCEL
        assert ConnectorUnknownError("x").disposition is FailureDisposition.PERMANENT

    def test_retry_after_is_validated_and_exposed(self) -> None:
        error = ConnectorRateLimitedError("throttled", retry_after_seconds=7)
        assert error.retry_after_seconds == 7
        with pytest.raises(ConnectorContractError):
            ConnectorRateLimitedError("throttled", retry_after_seconds=0)

    def test_detail_defaults_to_message_and_is_bounded(self) -> None:
        error = ConnectorRetryableError("summary text")
        assert error.detail == "summary text"
        long = ConnectorPermanentError("summary", detail="d" * 900)
        assert len(long.detail) <= 512
        assert long.detail.endswith("...")

    def test_construction_fails_closed_when_detail_carries_a_secret(self) -> None:
        secrets = SecretMaterial(("tok_abc123",))
        with pytest.raises(Exception, match="secret"):
            ConnectorRetryableError("failed with tok_abc123", secrets=secrets)

    def test_repr_carries_no_detail_secrets(self) -> None:
        error = ConnectorPermanentError("visible summary")
        assert "visible summary" in repr(error)

    def test_describe_connector_error_is_bounded_public_text(self) -> None:
        text = describe_connector_error(
            ConnectorRateLimitedError("slow down", retry_after_seconds=3)
        )
        assert "http_429" in text
        assert "retry_after=3s" in text


class TestCallContext:
    def test_default_context_is_never_cancelled(self) -> None:
        context = ConnectorCallContext()
        assert context.correlation_id is None
        assert not context.cancellation_token.is_cancelled()
        context.raise_if_cancelled()

    def test_event_token_cancellation_raises_the_typed_error(self) -> None:
        token = EventCancellationToken()
        token.cancel()
        token.cancel()
        context = ConnectorCallContext(cancellation_token=token)
        with pytest.raises(ConnectorCancelledError):
            context.raise_if_cancelled()

    def test_never_cancelled_token_is_immutable(self) -> None:
        assert not NEVER_CANCELLED.is_cancelled()
        NEVER_CANCELLED.raise_if_cancelled()

    @pytest.mark.parametrize("correlation", ["", "has space", "x" * 97, "bad!char"])
    def test_invalid_correlation_ids_are_rejected(self, correlation: str) -> None:
        with pytest.raises(ConnectorContractError):
            ConnectorCallContext(correlation_id=correlation)

    def test_valid_correlation_id_is_kept(self) -> None:
        context = ConnectorCallContext(correlation_id="run-1.node-2:attempt-3")
        assert context.correlation_id == "run-1.node-2:attempt-3"


class TestAuthentication:
    def test_header_value_carries_the_scheme(self) -> None:
        auth = ConnectorAuthentication(token="tok_secret")
        assert auth.header_value() == "Bearer tok_secret"
        assert repr(auth) == "ConnectorAuthentication(redacted=True)"

    def test_secret_material_registers_only_the_token(self) -> None:
        auth = ConnectorAuthentication(token="tok_secret")
        material = auth.secret_material()
        assert len(material) == 1
        assert repr(material) == "SecretMaterial(count=1, redacted=True)"

    @pytest.mark.parametrize("token", ["", "x" * 257])
    def test_invalid_tokens_are_rejected(self, token: str) -> None:
        with pytest.raises(ConnectorContractError):
            ConnectorAuthentication(token=token)


class TestRecordsAndPages:
    def test_valid_record_round_trip(self) -> None:
        record = SourceRecord(
            position=3,
            outcome=SourceOutcome.VALID,
            payload={"sku": "GRID-1", "quantity": 5},
        )
        assert not record.is_malformed

    def test_malformed_record_requires_a_reason(self) -> None:
        record = SourceRecord(
            position=1, outcome=SourceOutcome.MALFORMED, payload=None, malformed_reason="bad"
        )
        assert record.is_malformed
        with pytest.raises(ConnectorValidationError):
            SourceRecord(position=1, outcome=SourceOutcome.MALFORMED, payload=None)

    def test_valid_record_without_payload_is_rejected(self) -> None:
        with pytest.raises(ConnectorValidationError):
            SourceRecord(position=1, outcome=SourceOutcome.VALID, payload=None)

    @pytest.mark.parametrize(
        "payload",
        [
            {"too_long": "x" * 1025},
            {"deep": {"a": {"b": {"c": {"d": {"e": 1}}}}}},
            {"bad_type": 1.5},
            {"huge_int": 2**60},
            {f"field_{i}": i for i in range(33)},
        ],
    )
    def test_payload_contract_violations_are_rejected(self, payload: dict[str, object]) -> None:
        with pytest.raises(ConnectorValidationError):
            SourceRecord(position=0, outcome=SourceOutcome.VALID, payload=payload)

    def test_page_bounds(self) -> None:
        records = tuple(
            SourceRecord(position=i, outcome=SourceOutcome.VALID, payload={"i": i})
            for i in range(3)
        )
        page = SourcePage(records=records, next_cursor=None, request_count=1, byte_count=100)
        assert page.exhausted
        with pytest.raises(ConnectorValidationError):
            SourcePage(records=records * 70, next_cursor=None, request_count=1, byte_count=1)


class TestTargetValues:
    def test_write_request_validates_key_and_payload(self) -> None:
        request = TargetWriteRequest(
            sku="GRID-1",
            payload={"sku": "GRID-1", "name": "Part"},
            idempotency_key="pg-write:GRID-1:abc",
        )
        assert request.sku == "GRID-1"
        with pytest.raises(ConnectorValidationError):
            TargetWriteRequest(
                sku="GRID-2",
                payload={"sku": "GRID-1"},
                idempotency_key="pg-write:GRID-1:abc",
            )

    def test_write_outcome_effect_flags(self) -> None:
        applied = TargetWriteOutcome(
            outcome=TargetEffectOutcome.APPLIED, record_version=1, target_version=1, request_count=1
        )
        replayed = TargetWriteOutcome(
            outcome=TargetEffectOutcome.REPLAYED,
            record_version=1,
            target_version=1,
            request_count=1,
        )
        assert applied.changed_state
        assert not replayed.changed_state

    def test_target_record_and_state_validation(self) -> None:
        record = TargetRecord(
            sku="GRID-1", payload={"sku": "GRID-1"}, record_version=2, target_version=5
        )
        assert record.record_version == 2
        state = TargetStateSnapshot(
            record_count=1,
            target_version=5,
            content_fingerprint="a" * 64,
            capacity=100,
        )
        assert state.capacity == 100
        with pytest.raises(ConnectorValidationError):
            TargetStateSnapshot(
                record_count=1,
                target_version=5,
                content_fingerprint="not-a-sha",
                capacity=100,
            )


class TestEvents:
    def test_event_accepts_bounded_details(self) -> None:
        event = ConnectorEvent(
            kind=ConnectorEventKind.PAGE_COMPLETED,
            connector_kind=ConnectorKind.CSV_SOURCE,
            correlation_id="corr-1",
            details={"records": 5, "tag": "page"},
        )
        assert event.details["records"] == 5

    @pytest.mark.parametrize(
        "details",
        [{"float_value": 1.5}, {"too_long": "x" * 129}, {f"k{i}": i for i in range(17)}],
    )
    def test_event_rejects_out_of_bound_details(self, details: dict[str, object]) -> None:
        with pytest.raises(ConnectorContractError):
            ConnectorEvent(
                kind=ConnectorEventKind.PAGE_COMPLETED,
                connector_kind=ConnectorKind.CSV_SOURCE,
                correlation_id=None,
                details=details,
            )

    def test_event_rejects_unknown_kinds(self) -> None:
        with pytest.raises(ConnectorContractError):
            ConnectorEvent(
                kind="page_done",  # type: ignore[arg-type]
                connector_kind=ConnectorKind.CSV_SOURCE,
                correlation_id=None,
                details={},
            )

    def test_publisher_isolates_observer_failures(self) -> None:
        delivered: list[ConnectorEvent] = []

        def failing(event: ConnectorEvent) -> None:
            raise RuntimeError("observer defect")

        def recording(event: ConnectorEvent) -> None:
            delivered.append(event)

        publisher = ConnectorEventPublisher([failing, recording])
        publisher.publish(
            ConnectorEvent(
                kind=ConnectorEventKind.CLOSED,
                connector_kind=ConnectorKind.CSV_SOURCE,
                correlation_id=None,
                details={},
            )
        )
        assert publisher.failed_observer_count() == 1
        assert len(delivered) == 1

    def test_publisher_rejects_non_callable_observers(self) -> None:
        publisher = ConnectorEventPublisher()
        with pytest.raises(ConnectorContractError):
            publisher.add_observer("not callable")  # type: ignore[arg-type]


class TestLoopSafety:
    async def test_loop_refusal_inside_active_loop(self) -> None:
        with pytest.raises(ConnectorLoopError):
            require_no_running_loop("connector operation")

    def test_loop_refusal_outside_loop_passes(self) -> None:
        require_no_running_loop("connector operation")


class TestLifecycleStates:
    def test_state_enum_is_closed(self) -> None:
        assert {state.value for state in ConnectorState} == {"created", "open", "closed"}

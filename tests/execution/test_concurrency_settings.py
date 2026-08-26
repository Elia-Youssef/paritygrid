"""Capture, availability, resolution, and lifecycle tests for P7.4 settings."""

from __future__ import annotations

import threading
from collections.abc import Mapping
from dataclasses import FrozenInstanceError, replace
from typing import cast

import pytest

from paritygrid.application.execution import (
    CONCURRENCY_SETTINGS_VERSION,
    KNOWN_STRATEGY_IDS,
    MAX_CAPTURED_BYTES,
    MAX_CAPTURED_LIMIT,
    MAX_CAPTURED_TIMEOUT_SECONDS,
    MAX_STRATEGY_ID_LENGTH,
    MAX_UNAVAILABILITY_REASON_LENGTH,
    MIN_CAPTURED_LIMIT,
    MIN_CAPTURED_TIMEOUT_SECONDS,
    CapturedConcurrencySettings,
    ConcurrencySettingsError,
    ConcurrencySettingsValidationError,
    ConcurrencySettingsVersionError,
    StrategyAvailability,
    StrategyAvailabilityError,
    StrategyLifecycleCoordinator,
    StrategyLifecycleError,
    StrategyLifecycleListener,
    describe_known_strategies,
    resolve_strategy,
)
from paritygrid.application.execution.runner_contract import (
    RUNNER_CONTRACT_VERSION,
    STRATEGY_CAPABILITIES_PROTOCOL,
    StrategyCapabilitiesV1,
)
from paritygrid.application.planner import ResourcePolicy

SETTINGS = CapturedConcurrencySettings()

_LIMIT_FIELDS = (
    "global_concurrent_work",
    "per_strategy_work",
    "per_node_work",
    "connector_requests",
    "cpu_pool_operations",
    "assignment_channel_capacity",
    "result_channel_capacity",
    "telemetry_capacity",
    "writer_channel_capacity",
    "max_in_flight_records",
)
_BYTES_FIELDS = ("max_response_bytes", "max_file_bytes", "max_artifact_bytes_per_run")
_TIMEOUT_FIELDS = ("cleanup_timeout_seconds", "shutdown_timeout_seconds")
_INT_FIELDS = ("version", *_LIMIT_FIELDS, *_BYTES_FIELDS)
_MAPPING_KEYS = frozenset(_INT_FIELDS + _TIMEOUT_FIELDS)

_MIB = 1_048_576
_CAPABILITIES_ALPHA = StrategyCapabilitiesV1(
    strategy_id="alpha",
    contract_version=RUNNER_CONTRACT_VERSION,
    supports_pause=True,
    supports_cancel=True,
    supports_checkpoint=False,
    max_concurrent_work=1,
    max_in_flight_records=16,
    platform_requirements=(),
    protocol=STRATEGY_CAPABILITIES_PROTOCOL,
)


def _capabilities(strategy_id: str, *, max_in_flight_records: int = 16) -> StrategyCapabilitiesV1:
    return StrategyCapabilitiesV1(
        strategy_id=strategy_id,
        contract_version=RUNNER_CONTRACT_VERSION,
        supports_pause=True,
        supports_cancel=True,
        supports_checkpoint=False,
        max_concurrent_work=1,
        max_in_flight_records=max_in_flight_records,
        platform_requirements=(),
        protocol=STRATEGY_CAPABILITIES_PROTOCOL,
    )


def _availability(strategy_id: str, *, available: bool = True) -> StrategyAvailability:
    return StrategyAvailability(
        strategy_id=strategy_id,
        available=available,
        unavailability_reason=None if available else f"{strategy_id} strategy is not implemented",
        capabilities=_capabilities(strategy_id) if available else None,
    )


class _RecordingListener:
    """Listener recording every ownership boundary and scripted failures."""

    def __init__(self) -> None:
        self.start_attempts: list[str] = []
        self.started: list[str] = []
        self.shutdown_attempts: list[str] = []
        self.shutdowns: list[str] = []
        self.fail_start: frozenset[str] = frozenset()
        self.fail_shutdown: frozenset[str] = frozenset()

    def on_strategy_started(self, strategy_id: str) -> None:
        self.start_attempts.append(strategy_id)
        if strategy_id in self.fail_start:
            raise RuntimeError(f"startup failure for {strategy_id}")
        self.started.append(strategy_id)

    def on_strategy_shutdown(self, strategy_id: str) -> None:
        self.shutdown_attempts.append(strategy_id)
        if strategy_id in self.fail_shutdown:
            raise RuntimeError(f"shutdown failure for {strategy_id}")
        self.shutdowns.append(strategy_id)


def test_module_constants_are_exact() -> None:
    assert CONCURRENCY_SETTINGS_VERSION == 1
    assert MIN_CAPTURED_LIMIT == 1
    assert MAX_CAPTURED_LIMIT == 65_536
    assert MAX_CAPTURED_BYTES == 2**31 - 1
    assert MIN_CAPTURED_TIMEOUT_SECONDS == 0.001
    assert MAX_CAPTURED_TIMEOUT_SECONDS == 86_400.0
    assert MAX_STRATEGY_ID_LENGTH == 64
    assert MAX_UNAVAILABILITY_REASON_LENGTH == 256
    assert KNOWN_STRATEGY_IDS == ("sequential", "threaded", "asyncio")


def test_default_snapshot_matches_default_resource_policy_exactly() -> None:
    settings = CapturedConcurrencySettings()
    assert settings == CapturedConcurrencySettings.from_resource_policy(ResourcePolicy())
    assert settings == CapturedConcurrencySettings()
    assert hash(settings) == hash(CapturedConcurrencySettings())
    assert settings != replace(SETTINGS, connector_requests=9)
    assert settings.version == CONCURRENCY_SETTINGS_VERSION
    assert settings.global_concurrent_work == 4
    assert settings.per_strategy_work == 4
    assert settings.per_node_work == 4
    assert settings.connector_requests == 8
    assert settings.cpu_pool_operations == 4
    assert settings.assignment_channel_capacity == 256
    assert settings.result_channel_capacity == 256
    assert settings.telemetry_capacity == 256
    assert settings.writer_channel_capacity == 256
    assert settings.max_in_flight_records == 16
    assert settings.max_response_bytes == 16 * _MIB
    assert settings.max_file_bytes == 64 * _MIB
    assert settings.max_artifact_bytes_per_run == 512 * _MIB
    assert settings.cleanup_timeout_seconds == 60.0
    assert settings.shutdown_timeout_seconds == 60.0


def test_snapshot_is_frozen() -> None:
    with pytest.raises(FrozenInstanceError):
        SETTINGS.global_concurrent_work = 8  # type: ignore[misc]


def test_snapshot_repr_shows_counts_only() -> None:
    text = repr(SETTINGS)
    assert text.startswith("CapturedConcurrencySettings(")
    assert "version=1" in text
    assert "global_concurrent_work=4" in text
    assert "max_in_flight_records=16" in text
    assert "cleanup_timeout_seconds=60.0" in text
    assert "shutdown_timeout_seconds=60.0" in text


@pytest.mark.parametrize("field", _INT_FIELDS)
def test_integer_fields_reject_non_integer_types(field: str) -> None:
    for value in (True, 1.5, "4", None):
        with pytest.raises(ConcurrencySettingsValidationError):
            replace(SETTINGS, **{field: value})


def test_limit_fields_reject_out_of_bounds_values() -> None:
    for field in _LIMIT_FIELDS:
        for value in (0, MAX_CAPTURED_LIMIT + 1):
            with pytest.raises(ConcurrencySettingsValidationError):
                replace(SETTINGS, **{field: value})


def test_byte_fields_reject_out_of_bounds_values() -> None:
    for field in _BYTES_FIELDS:
        for value in (0, MAX_CAPTURED_BYTES + 1):
            with pytest.raises(ConcurrencySettingsValidationError):
                replace(SETTINGS, **{field: value})


def test_timeout_fields_reject_out_of_bounds_and_non_float_values() -> None:
    for field in _TIMEOUT_FIELDS:
        for value in (
            0.0,
            MIN_CAPTURED_TIMEOUT_SECONDS / 2,
            MAX_CAPTURED_TIMEOUT_SECONDS * 2,
            float("inf"),
            float("nan"),
            60,
            True,
        ):
            with pytest.raises(ConcurrencySettingsValidationError):
                replace(SETTINGS, **{field: value})


def test_closed_boundaries_are_accepted_for_every_bound_kind() -> None:
    extreme = replace(
        SETTINGS,
        global_concurrent_work=MIN_CAPTURED_LIMIT,
        per_strategy_work=MIN_CAPTURED_LIMIT,
        per_node_work=MIN_CAPTURED_LIMIT,
        connector_requests=MAX_CAPTURED_LIMIT,
        cpu_pool_operations=MAX_CAPTURED_LIMIT,
        assignment_channel_capacity=MAX_CAPTURED_LIMIT,
        result_channel_capacity=MAX_CAPTURED_LIMIT,
        telemetry_capacity=MAX_CAPTURED_LIMIT,
        writer_channel_capacity=MAX_CAPTURED_LIMIT,
        max_in_flight_records=MAX_CAPTURED_LIMIT,
        max_response_bytes=1,
        max_file_bytes=MAX_CAPTURED_BYTES,
        max_artifact_bytes_per_run=MAX_CAPTURED_BYTES,
        cleanup_timeout_seconds=MIN_CAPTURED_TIMEOUT_SECONDS,
        shutdown_timeout_seconds=MAX_CAPTURED_TIMEOUT_SECONDS,
    )
    assert extreme.global_concurrent_work == MIN_CAPTURED_LIMIT
    assert extreme.connector_requests == MAX_CAPTURED_LIMIT
    assert extreme.max_file_bytes == MAX_CAPTURED_BYTES
    assert extreme.cleanup_timeout_seconds == MIN_CAPTURED_TIMEOUT_SECONDS
    assert extreme.shutdown_timeout_seconds == MAX_CAPTURED_TIMEOUT_SECONDS
    wider = replace(SETTINGS, global_concurrent_work=8)
    assert wider.per_strategy_work == 4


@pytest.mark.parametrize(
    ("field", "value"),
    [("per_strategy_work", 5), ("per_node_work", 5)],
)
def test_global_work_must_cover_each_work_axis(field: str, value: int) -> None:
    with pytest.raises(ConcurrencySettingsValidationError, match="must cover"):
        replace(SETTINGS, **{field: value})


@pytest.mark.parametrize("version", [0, 2, -1])
def test_unknown_versions_fail_closed(version: int) -> None:
    with pytest.raises(ConcurrencySettingsVersionError):
        replace(SETTINGS, version=version)


def test_version_failure_takes_precedence_over_other_invalid_fields() -> None:
    with pytest.raises(ConcurrencySettingsVersionError):
        replace(SETTINGS, version=2, global_concurrent_work=0)


def test_from_resource_policy_maps_custom_policy_values() -> None:
    policy = ResourcePolicy(
        max_concurrency=8,
        max_in_flight=64,
        memory_limit_bytes=128 * _MIB,
        operation_timeout_seconds=120,
        queue_capacity=1_024,
    )
    settings = CapturedConcurrencySettings.from_resource_policy(policy)
    assert settings.version == CONCURRENCY_SETTINGS_VERSION
    assert settings.global_concurrent_work == 8
    assert settings.per_strategy_work == 8
    assert settings.per_node_work == 8
    assert settings.connector_requests == 8
    assert settings.cpu_pool_operations == 4
    assert settings.assignment_channel_capacity == 1_024
    assert settings.result_channel_capacity == 1_024
    assert settings.telemetry_capacity == 1_024
    assert settings.writer_channel_capacity == 1_024
    assert settings.max_in_flight_records == 64
    assert settings.max_response_bytes == 16 * _MIB
    assert settings.max_file_bytes == 64 * _MIB
    assert settings.max_artifact_bytes_per_run == 128 * _MIB
    assert type(settings.cleanup_timeout_seconds) is float
    assert settings.cleanup_timeout_seconds == 120.0
    assert settings.shutdown_timeout_seconds == 120.0


def test_from_resource_policy_global_override_widens_global_work_only() -> None:
    settings = CapturedConcurrencySettings.from_resource_policy(
        ResourcePolicy(max_concurrency=4),
        global_concurrent_work=32,
    )
    assert settings.global_concurrent_work == 32
    assert settings.per_strategy_work == 4
    assert settings.per_node_work == 4


def test_from_resource_policy_rejects_incoherent_global_override() -> None:
    with pytest.raises(ConcurrencySettingsValidationError, match="must cover"):
        CapturedConcurrencySettings.from_resource_policy(
            ResourcePolicy(max_concurrency=4),
            global_concurrent_work=2,
        )


@pytest.mark.parametrize("value", [True, 0, MAX_CAPTURED_LIMIT + 1, "8"])
def test_from_resource_policy_rejects_invalid_global_overrides(value: object) -> None:
    with pytest.raises(ConcurrencySettingsValidationError):
        CapturedConcurrencySettings.from_resource_policy(
            ResourcePolicy(),
            global_concurrent_work=cast(int, value),
        )


def test_from_resource_policy_rejects_wrong_policy_type() -> None:
    with pytest.raises(TypeError):
        CapturedConcurrencySettings.from_resource_policy(cast(ResourcePolicy, object()))


def test_to_mapping_is_total_and_round_trips() -> None:
    mapping = SETTINGS.to_mapping()
    assert frozenset(mapping) == _MAPPING_KEYS
    assert len(mapping) == 16
    assert mapping["version"] == CONCURRENCY_SETTINGS_VERSION
    assert type(mapping["cleanup_timeout_seconds"]) is float
    assert CapturedConcurrencySettings.from_mapping(mapping) == SETTINGS
    custom = replace(SETTINGS, global_concurrent_work=9, cleanup_timeout_seconds=1.5)
    assert CapturedConcurrencySettings.from_mapping(custom.to_mapping()) == custom


@pytest.mark.parametrize(
    "payload",
    [cast(Mapping[str, object], ["version"]), cast(Mapping[str, object], None)],
)
def test_from_mapping_rejects_non_dictionary_payloads(payload: Mapping[str, object]) -> None:
    with pytest.raises(TypeError):
        CapturedConcurrencySettings.from_mapping(payload)


@pytest.mark.parametrize(
    ("kind", "error"),
    [
        ("missing", ConcurrencySettingsValidationError),
        ("extra", ConcurrencySettingsValidationError),
        ("version-unknown", ConcurrencySettingsVersionError),
        ("version-bool", ConcurrencySettingsValidationError),
        ("int-field-bool", ConcurrencySettingsValidationError),
        ("int-field-float", ConcurrencySettingsValidationError),
        ("float-field-int", ConcurrencySettingsValidationError),
        ("int-field-out-of-bounds", ConcurrencySettingsValidationError),
        ("cross-field", ConcurrencySettingsValidationError),
    ],
)
def test_from_mapping_rejects_malformed_payloads(kind: str, error: type[Exception]) -> None:
    mapping = SETTINGS.to_mapping()
    if kind == "missing":
        del mapping["telemetry_capacity"]
    elif kind == "extra":
        mapping["unexpected"] = 1
    elif kind == "version-unknown":
        mapping["version"] = 2
    elif kind == "version-bool":
        mapping["version"] = True
    elif kind == "int-field-bool":
        mapping["global_concurrent_work"] = True
    elif kind == "int-field-float":
        mapping["per_node_work"] = 4.0
    elif kind == "float-field-int":
        mapping["cleanup_timeout_seconds"] = 30
    elif kind == "int-field-out-of-bounds":
        mapping["max_in_flight_records"] = MAX_CAPTURED_LIMIT + 1
    else:
        mapping["per_strategy_work"] = 8
    with pytest.raises(error):
        CapturedConcurrencySettings.from_mapping(mapping)


def test_availability_construction_is_frozen_and_redacts_capabilities() -> None:
    available = _availability("alpha")
    assert available.available
    assert available.unavailability_reason is None
    assert available.capabilities is not None
    assert available.capabilities.strategy_id == "alpha"
    unavailable = _availability("threaded", available=False)
    assert not unavailable.available
    assert unavailable.unavailability_reason is not None
    assert unavailable.capabilities is None
    with pytest.raises(FrozenInstanceError):
        available.available = False  # type: ignore[misc]
    assert "strategy_id='alpha'" in repr(available)
    assert "capabilities=<present>" in repr(available)
    assert "capabilities=<absent>" in repr(unavailable)
    assert "max_in_flight" not in repr(unavailable)


@pytest.mark.parametrize(
    "identifier",
    [
        "",
        "Sequential",
        "1sequential",
        "-sequential",
        "sequential_unit",
        "séquentiel",
        "a" * (MAX_STRATEGY_ID_LENGTH + 1),
        123,
        None,
    ],
)
def test_strategy_identifiers_must_use_the_known_shape(identifier: object) -> None:
    with pytest.raises(StrategyAvailabilityError):
        StrategyAvailability(
            strategy_id=cast(str, identifier),
            available=False,
            unavailability_reason="not implemented",
            capabilities=None,
        )


@pytest.mark.parametrize("flag", [1, "true", None])
def test_availability_flag_must_be_an_exact_boolean(flag: object) -> None:
    with pytest.raises(StrategyAvailabilityError):
        StrategyAvailability(
            strategy_id="alpha",
            available=cast(bool, flag),
            unavailability_reason=None,
            capabilities=_capabilities("alpha"),
        )


@pytest.mark.parametrize(
    ("available", "reason", "capabilities", "message"),
    [
        (True, "delayed implementation", _CAPABILITIES_ALPHA, "unavailability reason"),
        (True, None, None, "carry capabilities"),
        (True, None, "sequential", "carry capabilities"),
        (False, None, None, "reason must be text"),
        (False, "awaits implementation", _CAPABILITIES_ALPHA, "must not carry capabilities"),
    ],
)
def test_availability_cross_field_rules(
    available: bool,
    reason: str | None,
    capabilities: object,
    message: str,
) -> None:
    with pytest.raises(StrategyAvailabilityError, match=message):
        StrategyAvailability(
            strategy_id="alpha",
            available=available,
            unavailability_reason=reason,
            capabilities=cast(StrategyCapabilitiesV1 | None, capabilities),
        )


@pytest.mark.parametrize(
    "reason",
    [
        "",
        "x" * (MAX_UNAVAILABILITY_REASON_LENGTH + 1),
        "awaits\timplementation",
        "awaits\x1bimplementation",
        7,
    ],
)
def test_unavailability_reasons_are_bounded_printable_text(reason: object) -> None:
    with pytest.raises(StrategyAvailabilityError):
        StrategyAvailability(
            strategy_id="alpha",
            available=False,
            unavailability_reason=cast(str, reason),
            capabilities=None,
        )


def test_describe_known_strategies_matches_constant_order() -> None:
    availabilities = describe_known_strategies(SETTINGS)
    assert tuple(entry.strategy_id for entry in availabilities) == KNOWN_STRATEGY_IDS
    for entry in availabilities:
        if entry.available:
            assert entry.capabilities is not None
            assert entry.capabilities.strategy_id == entry.strategy_id
        else:
            assert entry.capabilities is None


def test_describe_sequential_capabilities_are_exact() -> None:
    sequential = describe_known_strategies(SETTINGS)[0]
    assert sequential.available
    assert sequential.unavailability_reason is None
    capabilities = sequential.capabilities
    assert capabilities is not None
    assert capabilities.strategy_id == "sequential"
    assert capabilities.contract_version == RUNNER_CONTRACT_VERSION
    assert capabilities.supports_pause is True
    assert capabilities.supports_cancel is True
    assert capabilities.supports_checkpoint is True
    assert capabilities.max_concurrent_work == 1
    assert capabilities.max_in_flight_records == SETTINGS.max_in_flight_records
    assert capabilities.platform_requirements == ()


def test_describe_sequential_reflects_captured_in_flight_bound() -> None:
    settings = replace(SETTINGS, max_in_flight_records=MAX_CAPTURED_LIMIT)
    sequential = describe_known_strategies(settings)[0]
    assert sequential.capabilities is not None
    assert sequential.capabilities.max_in_flight_records == MAX_CAPTURED_LIMIT


@pytest.mark.parametrize(
    ("strategy_id", "reason"),
    [
        ("threaded", ""),
        ("asyncio", ""),
    ],
)
def test_describe_registered_strategies_carry_exact_capabilities(
    strategy_id: str,
    reason: str,
) -> None:
    del reason
    availabilities = describe_known_strategies(SETTINGS)
    entry = next(item for item in availabilities if item.strategy_id == strategy_id)
    assert entry.available
    assert entry.unavailability_reason is None
    capabilities = entry.capabilities
    assert capabilities is not None
    assert capabilities.strategy_id == strategy_id
    assert capabilities.max_concurrent_work >= 1
    if strategy_id == "sequential":
        assert capabilities.max_concurrent_work == 1


def test_describe_rejects_wrong_settings_type() -> None:
    with pytest.raises(TypeError):
        describe_known_strategies(cast(CapturedConcurrencySettings, object()))


@pytest.mark.parametrize("strategy_id", ["", "process", "Sequential", "sequential "])
def test_resolve_unknown_strategies_fail_with_typed_errors(strategy_id: str) -> None:
    with pytest.raises(StrategyAvailabilityError):
        resolve_strategy(strategy_id, SETTINGS)


@pytest.mark.parametrize("strategy_id", KNOWN_STRATEGY_IDS)
def test_resolve_returns_exactly_the_requested_strategy(strategy_id: str) -> None:
    availability = resolve_strategy(strategy_id, SETTINGS)
    assert availability.strategy_id == strategy_id


def test_resolve_sequential_returns_real_capabilities() -> None:
    availability = resolve_strategy("sequential", SETTINGS)
    assert availability.available
    assert availability.unavailability_reason is None
    assert availability.capabilities is not None
    assert availability.capabilities.max_in_flight_records == SETTINGS.max_in_flight_records


def test_resolve_registered_strategies_expose_exact_capabilities() -> None:
    threaded = resolve_strategy("threaded", SETTINGS)
    assert threaded.strategy_id == "threaded"
    assert threaded.available
    assert threaded.unavailability_reason is None
    assert threaded.capabilities is not None
    asyncio_availability = resolve_strategy("asyncio", SETTINGS)
    assert asyncio_availability.strategy_id == "asyncio"
    assert asyncio_availability.available
    assert asyncio_availability.capabilities is not None
    assert asyncio_availability.capabilities is not threaded.capabilities


@pytest.mark.parametrize("identifier", [None, 7])
def test_resolve_rejects_non_text_strategy_identifiers(identifier: object) -> None:
    with pytest.raises(TypeError):
        resolve_strategy(cast(str, identifier), SETTINGS)


def test_resolve_rejects_wrong_settings_type() -> None:
    with pytest.raises(TypeError):
        resolve_strategy("sequential", cast(CapturedConcurrencySettings, object()))


def test_coordinator_starts_empty_with_or_without_listener() -> None:
    plain = StrategyLifecycleCoordinator(SETTINGS)
    assert plain.owned_strategies == ()
    assert not plain.is_started
    assert not plain.is_shutdown
    observed = StrategyLifecycleCoordinator(SETTINGS, listener=_RecordingListener())
    assert observed.owned_strategies == ()
    assert not observed.is_started
    assert not observed.is_shutdown


def test_coordinator_rejects_wrong_settings_type() -> None:
    with pytest.raises(TypeError):
        StrategyLifecycleCoordinator(cast(CapturedConcurrencySettings, object()))


def test_coordinator_rejects_non_listener_objects() -> None:
    with pytest.raises(TypeError):
        StrategyLifecycleCoordinator(
            SETTINGS,
            listener=cast(StrategyLifecycleListener, object()),
        )


def test_startup_records_ownership_in_order() -> None:
    listener = _RecordingListener()
    coordinator = StrategyLifecycleCoordinator(SETTINGS, listener=listener)
    coordinator.startup((_availability("alpha"), _availability("beta"), _availability("gamma")))
    assert coordinator.owned_strategies == ("alpha", "beta", "gamma")
    assert coordinator.is_started
    assert not coordinator.is_shutdown
    assert listener.started == ["alpha", "beta", "gamma"]
    assert listener.shutdown_attempts == []


def test_startup_without_listener_records_ownership_and_shuts_down() -> None:
    coordinator = StrategyLifecycleCoordinator(SETTINGS)
    coordinator.startup((_availability("alpha"),))
    assert coordinator.owned_strategies == ("alpha",)
    assert coordinator.is_started
    coordinator.shutdown()
    assert coordinator.is_shutdown
    assert coordinator.owned_strategies == ()


def test_startup_rejects_non_tuple_availabilities() -> None:
    listener = _RecordingListener()
    coordinator = StrategyLifecycleCoordinator(SETTINGS, listener=listener)
    with pytest.raises(TypeError):
        coordinator.startup(cast(tuple[StrategyAvailability, ...], ["alpha"]))
    assert coordinator.owned_strategies == ()
    assert not coordinator.is_started
    assert listener.start_attempts == []


def test_startup_rejects_non_availability_entries() -> None:
    coordinator = StrategyLifecycleCoordinator(SETTINGS)
    with pytest.raises(TypeError):
        coordinator.startup(cast(tuple[StrategyAvailability, ...], ("alpha",)))
    assert not coordinator.is_started
    assert coordinator.owned_strategies == ()


def test_startup_rejects_unavailable_entries_before_ownership() -> None:
    listener = _RecordingListener()
    coordinator = StrategyLifecycleCoordinator(SETTINGS, listener=listener)
    with pytest.raises(StrategyLifecycleError, match="not available"):
        coordinator.startup((_availability("alpha"), _availability("threaded", available=False)))
    assert coordinator.owned_strategies == ()
    assert not coordinator.is_started
    assert not coordinator.is_shutdown
    assert listener.start_attempts == []
    assert listener.shutdown_attempts == []


def test_second_startup_raises_lifecycle_error() -> None:
    listener = _RecordingListener()
    coordinator = StrategyLifecycleCoordinator(SETTINGS, listener=listener)
    coordinator.startup((_availability("alpha"),))
    with pytest.raises(StrategyLifecycleError, match="exactly once"):
        coordinator.startup((_availability("beta"),))
    assert coordinator.owned_strategies == ("alpha",)
    assert coordinator.is_started
    assert listener.started == ["alpha"]


def test_startup_after_shutdown_raises_lifecycle_error() -> None:
    coordinator = StrategyLifecycleCoordinator(SETTINGS)
    coordinator.startup((_availability("alpha"),))
    coordinator.shutdown()
    with pytest.raises(StrategyLifecycleError, match="exactly once"):
        coordinator.startup((_availability("beta"),))
    assert coordinator.owned_strategies == ()
    assert coordinator.is_shutdown


def test_startup_listener_failure_rolls_back_in_reverse_order() -> None:
    listener = _RecordingListener()
    listener.fail_start = frozenset({"beta"})
    coordinator = StrategyLifecycleCoordinator(SETTINGS, listener=listener)
    availabilities = (_availability("alpha"), _availability("beta"), _availability("gamma"))
    with pytest.raises(RuntimeError, match="startup failure for beta"):
        coordinator.startup(availabilities)
    assert listener.started == ["alpha"]
    assert listener.shutdown_attempts == ["alpha"]
    assert coordinator.owned_strategies == ()
    assert not coordinator.is_started
    assert not coordinator.is_shutdown
    listener.fail_start = frozenset()
    coordinator.startup(availabilities)
    assert coordinator.owned_strategies == ("alpha", "beta", "gamma")
    assert coordinator.is_started


def test_startup_listener_failure_on_first_entry_owns_nothing() -> None:
    listener = _RecordingListener()
    listener.fail_start = frozenset({"alpha"})
    coordinator = StrategyLifecycleCoordinator(SETTINGS, listener=listener)
    with pytest.raises(RuntimeError, match="startup failure for alpha"):
        coordinator.startup((_availability("alpha"), _availability("beta")))
    assert listener.started == []
    assert listener.shutdown_attempts == []
    assert coordinator.owned_strategies == ()
    assert not coordinator.is_started


def test_startup_failure_attempts_all_cleanup_and_preserves_startup_error() -> None:
    listener = _RecordingListener()
    listener.fail_start = frozenset({"gamma"})
    listener.fail_shutdown = frozenset({"alpha", "beta"})
    coordinator = StrategyLifecycleCoordinator(SETTINGS, listener=listener)
    with pytest.raises(RuntimeError, match="startup failure for gamma") as error:
        coordinator.startup((_availability("alpha"), _availability("beta"), _availability("gamma")))
    assert listener.shutdown_attempts == ["beta", "alpha"]
    assert len(error.value.__notes__) == 1
    assert "beta" in error.value.__notes__[0]
    assert "alpha" in error.value.__notes__[0]
    assert coordinator.owned_strategies == ()
    assert not coordinator.is_started
    assert not coordinator.is_shutdown
    listener.fail_start = frozenset()
    listener.fail_shutdown = frozenset()
    coordinator.startup((_availability("alpha"),))
    assert coordinator.is_started


def test_shutdown_releases_ownership_in_reverse_order() -> None:
    listener = _RecordingListener()
    coordinator = StrategyLifecycleCoordinator(SETTINGS, listener=listener)
    coordinator.startup((_availability("alpha"), _availability("beta"), _availability("gamma")))
    coordinator.shutdown()
    assert listener.shutdowns == ["gamma", "beta", "alpha"]
    assert coordinator.owned_strategies == ()
    assert not coordinator.is_started
    assert coordinator.is_shutdown


def test_shutdown_is_idempotent() -> None:
    listener = _RecordingListener()
    coordinator = StrategyLifecycleCoordinator(SETTINGS, listener=listener)
    coordinator.startup((_availability("alpha"), _availability("beta")))
    coordinator.shutdown()
    attempts_after_first = len(listener.shutdown_attempts)
    coordinator.shutdown()
    assert len(listener.shutdown_attempts) == attempts_after_first
    assert coordinator.is_shutdown
    assert not coordinator.is_started


def test_shutdown_before_startup_raises_lifecycle_error() -> None:
    coordinator = StrategyLifecycleCoordinator(SETTINGS)
    with pytest.raises(StrategyLifecycleError, match="started lifecycle"):
        coordinator.shutdown()
    assert not coordinator.is_shutdown


def test_shutdown_after_rolled_back_startup_raises_lifecycle_error() -> None:
    listener = _RecordingListener()
    listener.fail_start = frozenset({"beta"})
    coordinator = StrategyLifecycleCoordinator(SETTINGS, listener=listener)
    with pytest.raises(RuntimeError):
        coordinator.startup((_availability("alpha"), _availability("beta")))
    with pytest.raises(StrategyLifecycleError, match="started lifecycle"):
        coordinator.shutdown()
    assert not coordinator.is_shutdown


def test_shutdown_listener_failure_still_releases_every_strategy() -> None:
    listener = _RecordingListener()
    listener.fail_shutdown = frozenset({"beta"})
    coordinator = StrategyLifecycleCoordinator(SETTINGS, listener=listener)
    coordinator.startup((_availability("alpha"), _availability("beta"), _availability("gamma")))
    with pytest.raises(StrategyLifecycleError, match="1 of 3"):
        coordinator.shutdown()
    assert listener.shutdown_attempts == ["gamma", "beta", "alpha"]
    assert listener.shutdowns == ["gamma", "alpha"]
    assert coordinator.owned_strategies == ()
    assert coordinator.is_shutdown
    coordinator.shutdown()
    assert len(listener.shutdown_attempts) == 3


def test_shutdown_aggregates_every_listener_failure() -> None:
    listener = _RecordingListener()
    listener.fail_shutdown = frozenset({"alpha", "beta", "gamma"})
    coordinator = StrategyLifecycleCoordinator(SETTINGS, listener=listener)
    coordinator.startup((_availability("alpha"), _availability("beta"), _availability("gamma")))
    with pytest.raises(StrategyLifecycleError, match="3 of 3"):
        coordinator.shutdown()
    assert listener.shutdowns == []
    assert len(listener.shutdown_attempts) == 3
    assert coordinator.is_shutdown
    coordinator.shutdown()
    assert len(listener.shutdown_attempts) == 3


def test_empty_startup_owns_no_strategies() -> None:
    listener = _RecordingListener()
    coordinator = StrategyLifecycleCoordinator(SETTINGS, listener=listener)
    coordinator.startup(())
    assert coordinator.is_started
    assert coordinator.owned_strategies == ()
    coordinator.shutdown()
    assert coordinator.is_shutdown
    assert listener.start_attempts == []
    assert listener.shutdown_attempts == []


def test_concurrent_startup_grants_exactly_one_owner() -> None:
    listener = _RecordingListener()
    coordinator = StrategyLifecycleCoordinator(SETTINGS, listener=listener)
    availabilities = (_availability("alpha"), _availability("beta"))
    barrier = threading.Barrier(2)
    outcomes: list[object] = []
    outcomes_lock = threading.Lock()

    def attempt() -> None:
        barrier.wait()
        outcome: object = None
        try:
            coordinator.startup(availabilities)
        except ConcurrencySettingsError as error:
            outcome = error
        with outcomes_lock:
            outcomes.append(outcome)

    threads = [threading.Thread(target=attempt) for _ in range(2)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    errors = [outcome for outcome in outcomes if outcome is not None]
    assert len(outcomes) == 2
    assert len(errors) == 1
    assert isinstance(errors[0], StrategyLifecycleError)
    assert not coordinator.is_shutdown
    assert coordinator.is_started
    assert coordinator.owned_strategies == ("alpha", "beta")
    assert listener.started == ["alpha", "beta"]
    assert len(listener.start_attempts) == 2

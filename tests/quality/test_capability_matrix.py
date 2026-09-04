"""Closed capability matrix contract tests (P21.3)."""

from __future__ import annotations

import json
import multiprocessing
from collections.abc import Callable
from typing import cast

import pytest

from paritygrid.application.execution.runtime_capabilities import (
    SUBORDINATE_INTERPRETER_POOL_ID,
    SUBORDINATE_PROCESS_POOL_ID,
    SubordinatePoolCapability,
)
from paritygrid.quality import capability_matrix
from paritygrid.quality.capability_matrix import (
    CAPABILITY_MATRIX_FORMAT,
    CAPABILITY_MATRIX_VERSION,
    MAX_REASON_LENGTH,
    CapabilityMatrixDocument,
    CapabilityMatrixError,
    CapabilityProfileEntry,
    CapabilityState,
    ProfileKind,
    build_capability_matrix,
    capability_matrix_bytes,
    parse_capability_matrix,
)

KNOWN_PROFILE_IDS: tuple[str, ...] = (
    "sequential",
    "threaded",
    "asyncio",
    SUBORDINATE_PROCESS_POOL_ID,
    SUBORDINATE_INTERPRETER_POOL_ID,
)
FULL_PLAN_IDS = frozenset({"sequential", "threaded", "asyncio"})
SUBORDINATE_POOL_IDS = frozenset({SUBORDINATE_PROCESS_POOL_ID, SUBORDINATE_INTERPRETER_POOL_ID})
DOCUMENT_FIELDS = frozenset({"format", "version", "package_version", "platform", "profiles"})
PLATFORM_FIELDS = frozenset(
    {
        "system",
        "machine",
        "python_implementation",
        "python_version",
        "sqlite_version",
        "sqlite_threadsafety",
    }
)
ENTRY_FIELDS = frozenset({"profile_id", "kind", "state", "reason"})


def _canonical_mapping() -> dict[str, object]:
    parsed: object = json.loads(capability_matrix_bytes())
    if not isinstance(parsed, dict):
        raise AssertionError("canonical capability matrix payload must decode to an object")
    return cast("dict[str, object]", parsed)


def _encode_mapping(mapping: dict[str, object]) -> bytes:
    return json.dumps(mapping, ensure_ascii=True, separators=(",", ":"), sort_keys=True).encode(
        "ascii"
    )


def _platform_fields(mapping: dict[str, object]) -> dict[str, object]:
    raw_platform = mapping["platform"]
    if not isinstance(raw_platform, dict):
        raise AssertionError("platform facts must decode to an object")
    return cast("dict[str, object]", raw_platform)


def _profile_entries(mapping: dict[str, object]) -> list[dict[str, object]]:
    raw_entries = mapping["profiles"]
    if not isinstance(raw_entries, list):
        raise AssertionError("profiles must decode to an array")
    entries_list = cast("list[object]", raw_entries)
    entries: list[dict[str, object]] = []
    for raw_entry in entries_list:
        if not isinstance(raw_entry, dict):
            raise AssertionError("profile entries must decode to objects")
        entries.append(cast("dict[str, object]", raw_entry))
    return entries


def _profile_entry(mapping: dict[str, object], profile_id: str) -> dict[str, object]:
    for entry in _profile_entries(mapping):
        if entry["profile_id"] == profile_id:
            return entry
    raise AssertionError(f"profile {profile_id!r} is missing from the payload")


def _entry_of(document: CapabilityMatrixDocument, profile_id: str) -> CapabilityProfileEntry:
    for entry in document.profiles:
        if entry.profile_id == profile_id:
            return entry
    raise AssertionError(f"profile {profile_id!r} is missing from the document")


def _sanitize_reason(template: str, detail: str | None = None) -> str:
    """Call the module sanitizer through its exact contract name."""
    return capability_matrix._sanitize_reason(template, detail)  # type: ignore[reportPrivateUsage]


def _unavailable_pool(pool_id: str) -> SubordinatePoolCapability:
    return SubordinatePoolCapability(
        pool_id=pool_id,
        available=False,
        unavailability_reason=f"probe failed: simulated {pool_id} detail",
    )


def _mutate_unknown_format(mapping: dict[str, object]) -> None:
    mapping["format"] = "unknown-capability-matrix"


def _mutate_omitted_profile(mapping: dict[str, object]) -> None:
    raw_entries = mapping["profiles"]
    if not isinstance(raw_entries, list) or not raw_entries:
        raise AssertionError("profiles payload expected as a non-empty array")
    del cast("list[object]", raw_entries)[0]


def _mutate_unknown_profile(mapping: dict[str, object]) -> None:
    raw_entries = mapping["profiles"]
    if not isinstance(raw_entries, list):
        raise AssertionError("profiles payload expected as an array")
    cast("list[object]", raw_entries).append(
        {"profile_id": "gpu", "kind": "full_plan", "state": "available", "reason": None}
    )


def _mutate_duplicated_profile(mapping: dict[str, object]) -> None:
    raw_entries = mapping["profiles"]
    if not isinstance(raw_entries, list):
        raise AssertionError("profiles payload expected as an array")
    entries_list = cast("list[object]", raw_entries)
    first = entries_list[0]
    if not isinstance(first, dict):
        raise AssertionError("first profile entry expected as an object")
    entries_list.append(dict(cast("dict[str, object]", first)))


def _mutate_unknown_state(mapping: dict[str, object]) -> None:
    _profile_entry(mapping, "threaded")["state"] = "maybe"


def _mutate_available_with_reason(mapping: dict[str, object]) -> None:
    _profile_entry(mapping, "sequential")["reason"] = "unexpected reason"


def _mutate_unavailable_without_reason(mapping: dict[str, object]) -> None:
    entry = _profile_entry(mapping, "threaded")
    entry["state"] = "unavailable"
    entry["reason"] = None


def _mutate_reason_with_path_separator(mapping: dict[str, object]) -> None:
    entry = _profile_entry(mapping, "threaded")
    entry["state"] = "unavailable"
    entry["reason"] = "blocked by /etc/passwd"


def _mutate_reason_over_bound(mapping: dict[str, object]) -> None:
    entry = _profile_entry(mapping, "threaded")
    entry["state"] = "unavailable"
    entry["reason"] = "x" * (MAX_REASON_LENGTH + 1)


def _mutate_non_ascii_reason(mapping: dict[str, object]) -> None:
    entry = _profile_entry(mapping, "threaded")
    entry["state"] = "unavailable"
    entry["reason"] = "caf\u00e9 delayed"


def _mutate_unpadded_reason(mapping: dict[str, object]) -> None:
    entry = _profile_entry(mapping, "threaded")
    entry["state"] = "unavailable"
    entry["reason"] = " padded reason "


def _mutate_missing_platform(mapping: dict[str, object]) -> None:
    del mapping["platform"]


def _mutate_missing_platform_field(mapping: dict[str, object]) -> None:
    del _platform_fields(mapping)["system"]


def _mutate_unknown_top_level_field(mapping: dict[str, object]) -> None:
    mapping["replacement_for_threaded"] = "asyncio"


def _mutate_kind_contradiction(mapping: dict[str, object]) -> None:
    _profile_entry(mapping, "sequential")["kind"] = "subordinate_pool"


def _mutate_wrong_version_type(mapping: dict[str, object]) -> None:
    mapping["version"] = "1"


def _mutate_wrong_package_version_type(mapping: dict[str, object]) -> None:
    mapping["package_version"] = 5


def _mutate_wrong_platform_type(mapping: dict[str, object]) -> None:
    mapping["platform"] = []


def _mutate_wrong_profiles_type(mapping: dict[str, object]) -> None:
    mapping["profiles"] = {}


def _mutate_wrong_threadsafety_type(mapping: dict[str, object]) -> None:
    _platform_fields(mapping)["sqlite_threadsafety"] = True


class TestContractIdentity:
    def test_format_name_and_version_constants_are_locked(self) -> None:
        assert CAPABILITY_MATRIX_FORMAT == "paritygrid-runtime-capability-matrix"
        assert CAPABILITY_MATRIX_VERSION == 1

    def test_built_document_reports_exactly_the_five_known_profiles(self) -> None:
        document = build_capability_matrix()
        assert [entry.profile_id for entry in document.profiles] == list(KNOWN_PROFILE_IDS)

    def test_full_plan_and_subordinate_kinds_are_structurally_distinct(self) -> None:
        document = build_capability_matrix()
        full_plan = {
            entry.profile_id for entry in document.profiles if entry.kind is ProfileKind.FULL_PLAN
        }
        pools = {
            entry.profile_id
            for entry in document.profiles
            if entry.kind is ProfileKind.SUBORDINATE_POOL
        }
        assert full_plan == FULL_PLAN_IDS
        assert pools == SUBORDINATE_POOL_IDS
        assert not full_plan & pools

    def test_sequential_is_available_without_reason_in_this_runtime(self) -> None:
        entry = _entry_of(build_capability_matrix(), "sequential")
        assert entry.state is CapabilityState.AVAILABLE
        assert entry.reason is None

    def test_canonical_payload_field_sets_are_closed(self) -> None:
        mapping = _canonical_mapping()
        assert frozenset(mapping) == DOCUMENT_FIELDS
        assert frozenset(_platform_fields(mapping)) == PLATFORM_FIELDS
        assert [entry["profile_id"] for entry in _profile_entries(mapping)] == list(
            KNOWN_PROFILE_IDS
        )
        for entry in _profile_entries(mapping):
            assert frozenset(entry) == ENTRY_FIELDS


class TestProducerConsumerAgreement:
    def test_parse_of_built_bytes_round_trips_every_field(self) -> None:
        document = build_capability_matrix()
        parsed = parse_capability_matrix(capability_matrix_bytes())
        assert parsed == document
        assert parsed.format == document.format
        assert parsed.version == document.version
        assert parsed.package_version == document.package_version
        assert parsed.platform == document.platform
        assert parsed.profiles == document.profiles

    def test_version_two_document_is_rejected(self) -> None:
        mapping = _canonical_mapping()
        mapping["version"] = 2
        with pytest.raises(CapabilityMatrixError):
            parse_capability_matrix(_encode_mapping(mapping))

    def test_missing_version_is_rejected(self) -> None:
        mapping = _canonical_mapping()
        del mapping["version"]
        with pytest.raises(CapabilityMatrixError):
            parse_capability_matrix(_encode_mapping(mapping))


class TestFailClosedVectors:
    @pytest.mark.parametrize(
        "mutate",
        [
            _mutate_unknown_format,
            _mutate_omitted_profile,
            _mutate_unknown_profile,
            _mutate_duplicated_profile,
            _mutate_unknown_state,
            _mutate_available_with_reason,
            _mutate_unavailable_without_reason,
            _mutate_reason_with_path_separator,
            _mutate_reason_over_bound,
            _mutate_non_ascii_reason,
            _mutate_unpadded_reason,
            _mutate_missing_platform,
            _mutate_missing_platform_field,
            _mutate_unknown_top_level_field,
            _mutate_kind_contradiction,
            _mutate_wrong_version_type,
            _mutate_wrong_package_version_type,
            _mutate_wrong_platform_type,
            _mutate_wrong_profiles_type,
            _mutate_wrong_threadsafety_type,
        ],
        ids=[
            "unknown-format",
            "omitted-profile",
            "unknown-profile",
            "duplicated-profile",
            "unknown-state",
            "available-with-reason",
            "unavailable-without-reason",
            "reason-with-path-separator",
            "reason-over-bound",
            "non-ascii-reason",
            "unpadded-reason",
            "missing-platform",
            "missing-platform-field",
            "unknown-top-level-field",
            "kind-contradiction",
            "wrong-version-type",
            "wrong-package-version-type",
            "wrong-platform-type",
            "wrong-profiles-type",
            "wrong-threadsafety-type",
        ],
    )
    def test_fail_closed_vector_is_rejected(
        self, mutate: Callable[[dict[str, object]], None]
    ) -> None:
        mapping = _canonical_mapping()
        mutate(mapping)
        with pytest.raises(CapabilityMatrixError):
            parse_capability_matrix(_encode_mapping(mapping))

    def test_malformed_json_is_rejected(self) -> None:
        with pytest.raises(CapabilityMatrixError):
            parse_capability_matrix(b"{not json")

    def test_non_utf8_payload_is_rejected(self) -> None:
        with pytest.raises(CapabilityMatrixError):
            parse_capability_matrix(b"\xff\xfe\x00")

    def test_non_bytes_payload_is_rejected(self) -> None:
        payload: object = "{}"
        with pytest.raises(CapabilityMatrixError):
            parse_capability_matrix(cast("bytes", payload))


class TestStateSemantics:
    def test_missing_spawn_context_marks_process_pool_unsupported(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        def _raise(method: str) -> object:
            raise ValueError("unknown start method")

        monkeypatch.setattr(multiprocessing, "get_context", _raise)
        entry = _entry_of(build_capability_matrix(), SUBORDINATE_PROCESS_POOL_ID)
        assert entry.state is CapabilityState.UNSUPPORTED
        assert entry.reason is not None
        for forbidden in ("/", "\\", "%"):
            assert forbidden not in entry.reason

    def test_unavailable_process_pool_detector_is_reported_without_verbatim_message(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(
            capability_matrix,
            "detect_process_pool_capability",
            lambda: _unavailable_pool(SUBORDINATE_PROCESS_POOL_ID),
        )
        entry = _entry_of(build_capability_matrix(), SUBORDINATE_PROCESS_POOL_ID)
        assert entry.state is CapabilityState.UNAVAILABLE
        assert entry.reason is not None
        assert "simulated" not in entry.reason

    def test_missing_interpreter_executor_marks_interpreter_pool_unsupported(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(capability_matrix, "_interpreter_executor_exists", lambda: False)
        entry = _entry_of(build_capability_matrix(), SUBORDINATE_INTERPRETER_POOL_ID)
        assert entry.state is CapabilityState.UNSUPPORTED
        assert entry.reason is not None

    def test_failing_interpreter_probe_marks_interpreter_pool_unavailable(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(capability_matrix, "_interpreter_executor_exists", lambda: True)
        monkeypatch.setattr(
            capability_matrix,
            "detect_interpreter_capability",
            lambda: _unavailable_pool(SUBORDINATE_INTERPRETER_POOL_ID),
        )
        entry = _entry_of(build_capability_matrix(), SUBORDINATE_INTERPRETER_POOL_ID)
        assert entry.state is CapabilityState.UNAVAILABLE
        assert entry.reason is not None
        assert "simulated" not in entry.reason

    def test_unavailable_threaded_capability_is_reported_never_substituted(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(capability_matrix, "detect_threaded_capability", lambda: False)
        document = build_capability_matrix()
        states = {entry.profile_id: entry.state for entry in document.profiles}
        assert states["threaded"] is CapabilityState.UNAVAILABLE
        assert states["sequential"] is CapabilityState.AVAILABLE
        assert states["asyncio"] is CapabilityState.AVAILABLE
        threaded = _entry_of(document, "threaded")
        assert threaded.kind is ProfileKind.FULL_PLAN
        reason = threaded.reason
        assert reason is not None
        assert reason.strip() == reason

    def test_document_carries_no_field_that_implies_substitution(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(capability_matrix, "detect_threaded_capability", lambda: False)
        mapping = _canonical_mapping()
        assert frozenset(mapping) == DOCUMENT_FIELDS
        assert frozenset(_platform_fields(mapping)) == PLATFORM_FIELDS
        for entry in _profile_entries(mapping):
            assert frozenset(entry) == ENTRY_FIELDS
            assert entry["profile_id"] in KNOWN_PROFILE_IDS

    def test_unavailable_reasons_use_the_locked_closed_templates(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        def _raise(method: str) -> object:
            raise ValueError("unknown start method")

        monkeypatch.setattr(capability_matrix, "detect_threaded_capability", lambda: False)
        monkeypatch.setattr(capability_matrix, "detect_asyncio_capability", lambda: False)
        monkeypatch.setattr(multiprocessing, "get_context", _raise)
        monkeypatch.setattr(capability_matrix, "_interpreter_executor_exists", lambda: False)
        reasons = {entry.profile_id: entry.reason for entry in build_capability_matrix().profiles}
        assert reasons["threaded"] == "threaded detector reported unavailable"
        assert reasons["asyncio"] == "asyncio detector reported unavailable"
        assert reasons[SUBORDINATE_PROCESS_POOL_ID] == (
            "spawn start method is not provided by this runtime"
        )
        assert reasons[SUBORDINATE_INTERPRETER_POOL_ID] == (
            "InterpreterPoolExecutor is not provided by this runtime"
        )


class TestDeterminism:
    def test_consecutive_productions_are_byte_identical(self) -> None:
        first = capability_matrix_bytes()
        second = capability_matrix_bytes()
        assert first == second
        assert first.decode("ascii").startswith("{")
        assert first.decode("ascii") == second.decode("ascii")


class TestSanitizeReason:
    def test_exception_class_only_detail_is_appended(self) -> None:
        reason = _sanitize_reason("interpreter pool detector reported unavailable", "RuntimeError")
        assert reason == "interpreter pool detector reported unavailable: RuntimeError"

    def test_overlong_detail_is_truncated_to_the_bound(self) -> None:
        reason = _sanitize_reason("probe failed", "E" * 400)
        assert len(reason) == MAX_REASON_LENGTH
        assert reason.startswith("probe failed: ")
        assert reason.endswith("E")

    def test_template_alone_passes_through_validated(self) -> None:
        assert _sanitize_reason("spawn start method is not provided by this runtime") == (
            "spawn start method is not provided by this runtime"
        )

    def test_hostile_templates_are_rejected(self) -> None:
        for template in (
            "../relative/path",
            "back\\slash",
            "percent % sign",
            "line\nbreak",
            " padded",
            "trailing ",
            "caf\u00e9 non-ascii",
            "",
        ):
            with pytest.raises(CapabilityMatrixError):
                _sanitize_reason(template)

    def test_hostile_details_are_rejected(self) -> None:
        for detail in (
            "/absolute/path",
            "back\\slash",
            "line\nbreak",
            " padded",
            "caf\u00e9 non-ascii",
            "",
        ):
            with pytest.raises(CapabilityMatrixError):
                _sanitize_reason("probe failed", detail)

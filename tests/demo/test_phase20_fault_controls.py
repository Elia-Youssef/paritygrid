"""Closed, versioned fault-control catalog tests (Phase 20)."""

import json
from typing import cast

import pytest

from paritygrid.demo.datasets import WireValue, canonical_json_bytes
from paritygrid.demo.demo_app import options_fault_selection
from paritygrid.demo.fault_controls import (
    CANONICAL_FAULT_SELECTION,
    FAULT_CONTROL_FORMAT,
    FAULT_CONTROL_VERSION,
    RATE_LIMIT_CONTROL_NAME,
    WAREHOUSE_TRANSIENT_CONTROL_NAME,
    UnknownFaultControlError,
    fault_control_catalog_bytes,
    fault_controls,
    resolve_fault_control,
    resolve_fault_controls,
)

_EXPECTED_IDENTITIES = (
    f"{FAULT_CONTROL_FORMAT}/{RATE_LIMIT_CONTROL_NAME}/v1",
    f"{FAULT_CONTROL_FORMAT}/{WAREHOUSE_TRANSIENT_CONTROL_NAME}/v1",
)

_DOCUMENT_FIELDS = (
    "activation_point",
    "expected_consequence",
    "failure_behavior",
    "identity",
    "name",
    "observable_evidence",
    "recovery_behavior",
    "reset_behavior",
    "title",
    "version",
)

_REJECTED_SELECTIONS = [
    "none",
    "",
    "canonical.rate_limit",
    "CANONICAL",
    "canonical; rm -rf",
    "https://example.invalid/fault",
    "'; DROP TABLE controls; --",
]


class TestCatalogShape:
    def test_catalog_is_exactly_the_two_canonical_controls(self) -> None:
        controls = fault_controls()

        assert tuple(control.name for control in controls) == (
            RATE_LIMIT_CONTROL_NAME,
            WAREHOUSE_TRANSIENT_CONTROL_NAME,
        )
        assert tuple(control.version for control in controls) == (1, 1)

    def test_identities_are_stable_and_versioned(self) -> None:
        assert tuple(control.identity for control in fault_controls()) == _EXPECTED_IDENTITIES

    def test_every_control_document_carries_all_bounded_fields(self) -> None:
        for control in fault_controls():
            document = control.document()
            assert sorted(document) == sorted(_DOCUMENT_FIELDS)
            for field in _DOCUMENT_FIELDS:
                value = document[field]
                if field == "version":
                    assert value == control.version
                    continue
                assert isinstance(value, str)
                assert value != ""
            assert document["identity"] == control.identity


class TestSelectionResolution:
    def test_canonical_selection_resolves_to_the_whole_catalog(self) -> None:
        assert resolve_fault_controls(CANONICAL_FAULT_SELECTION) == fault_controls()

    def test_demo_options_fault_selection_resolves_to_catalog_identities(self) -> None:
        selection = options_fault_selection()

        assert selection == "canonical"
        assert tuple(control.identity for control in resolve_fault_controls(selection)) == (
            _EXPECTED_IDENTITIES
        )

    @pytest.mark.parametrize("selection", _REJECTED_SELECTIONS)
    def test_every_non_canonical_selection_is_rejected(self, selection: str) -> None:
        with pytest.raises(UnknownFaultControlError):
            resolve_fault_controls(selection)


class TestNameResolution:
    def test_exact_names_resolve_to_single_controls(self) -> None:
        assert resolve_fault_control(RATE_LIMIT_CONTROL_NAME).identity == _EXPECTED_IDENTITIES[0]
        assert (
            resolve_fault_control(WAREHOUSE_TRANSIENT_CONTROL_NAME).identity
            == _EXPECTED_IDENTITIES[1]
        )

    @pytest.mark.parametrize(
        "name",
        ["canonical.rate-limit", "rate_limit", "", "canonical.rate_limit/v1", "canonical"],
    )
    def test_unknown_names_are_rejected(self, name: str) -> None:
        with pytest.raises(UnknownFaultControlError):
            resolve_fault_control(name)


class TestCatalogBytes:
    def test_catalog_bytes_are_byte_stable(self) -> None:
        assert fault_control_catalog_bytes() == fault_control_catalog_bytes()

    def test_catalog_bytes_are_canonical_json_with_sorted_keys(self) -> None:
        payload = fault_control_catalog_bytes()

        document: object = json.loads(payload.decode("ascii"))
        assert isinstance(document, dict)
        parsed = cast("dict[str, WireValue]", document)
        assert parsed["format"] == FAULT_CONTROL_FORMAT
        assert parsed["version"] == FAULT_CONTROL_VERSION
        assert parsed["selections"] == [CANONICAL_FAULT_SELECTION]
        controls = parsed["controls"]
        assert isinstance(controls, list)
        assert len(controls) == 2
        # Re-encoding the parsed document reproduces the exact published bytes.
        assert canonical_json_bytes(parsed) == payload

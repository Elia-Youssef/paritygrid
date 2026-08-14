"""Frozen connector capability and non-secret snapshot contracts."""

from __future__ import annotations

from dataclasses import FrozenInstanceError
from pathlib import Path
from typing import Any, cast

import pytest

from paritygrid.application.planner import (
    MAX_CONNECTOR_KIND_LENGTH,
    MAX_CONNECTOR_SNAPSHOT_REFERENCES,
    ConnectorBindingSnapshot,
    ConnectorCapability,
    ConnectorCapabilitySet,
    ConnectorReferenceSnapshot,
    ConnectorValidationError,
    InvalidConnectorSnapshotError,
)
from paritygrid.application.planner import connectors as contract
from paritygrid.application.ports import ConfigurationDocument
from paritygrid.domain.models import ConnectorId


def _document(**values: object) -> ConfigurationDocument:
    return ConfigurationDocument.from_mapping(values)


def _reference(index: int = 0) -> ConnectorReferenceSnapshot:
    return ConnectorReferenceSnapshot(f"token.{index}", f"CONNECTOR_TOKEN_{index}")


def _snapshot(**overrides: object) -> ConnectorBindingSnapshot:
    values: dict[str, object] = {
        "connector_id": ConnectorId("con_inventory"),
        "kind": "inventory-http",
        "revision": 2,
        "configuration": _document(endpoint="https://inventory.invalid"),
        "capabilities": ConnectorCapabilitySet(
            (ConnectorCapability.WRITE, ConnectorCapability.READ)
        ),
        "schema_discovery": _document(version=1),
        "secret_references": (_reference(),),
    }
    values.update(overrides)
    return ConnectorBindingSnapshot(**cast(Any, values))


def test_connector_contract_is_dependency_neutral_and_bounded() -> None:
    assert MAX_CONNECTOR_KIND_LENGTH == 96
    assert MAX_CONNECTOR_SNAPSHOT_REFERENCES == 64
    source = Path(contract.__file__).read_text(encoding="utf-8")
    assert "sqlalchemy" not in source
    assert "fastapi" not in source
    assert "pydantic" not in source


def test_closed_connector_capabilities_are_frozen() -> None:
    assert tuple(capability.value for capability in ConnectorCapability) == (
        "read",
        "write",
        "async_io",
        "blocking_io",
        "idempotency",
        "schema_discovery",
    )


def test_capability_set_is_canonical_immutable_and_total_when_encoded() -> None:
    values = ConnectorCapabilitySet((ConnectorCapability.WRITE, ConnectorCapability.READ))
    assert values.values == (ConnectorCapability.READ, ConnectorCapability.WRITE)
    assert values.supports(ConnectorCapability.READ)
    assert not values.supports(ConnectorCapability.IDEMPOTENCY)
    assert values.to_mapping() == {
        "async_io": False,
        "blocking_io": False,
        "idempotency": False,
        "read": True,
        "schema_discovery": False,
        "write": True,
    }
    assert repr(values) == "ConnectorCapabilitySet(enabled=2)"
    with pytest.raises(FrozenInstanceError):
        values.values = ()  # type: ignore[misc]
    with pytest.raises(TypeError, match="ConnectorCapability"):
        values.supports(cast(Any, "read"))


def test_capability_set_rejects_wrong_collections_duplicates_and_items() -> None:
    with pytest.raises(TypeError, match="tuple"):
        ConnectorCapabilitySet(cast(Any, []))
    with pytest.raises(TypeError, match="invalid"):
        ConnectorCapabilitySet(cast(Any, ("read",)))
    with pytest.raises(ConnectorValidationError, match="unique"):
        ConnectorCapabilitySet((ConnectorCapability.READ, ConnectorCapability.READ))


def test_reference_snapshot_is_canonical_and_never_displays_names() -> None:
    reference = _reference()
    assert reference.to_mapping() == {
        "environment_variable_name": "CONNECTOR_TOKEN_0",
        "reference_name": "token.0",
    }
    assert repr(reference) == "ConnectorReferenceSnapshot(redacted=True)"
    for value in ("", "Token", "token..name", "t\u00f6ken", "x" * 129):
        with pytest.raises(InvalidConnectorSnapshotError):
            ConnectorReferenceSnapshot(value, "CONNECTOR_TOKEN")
    for value in ("", "connector_token", "TOKEN-NAME", "T\u00d6KEN", "X" * 129):
        with pytest.raises(InvalidConnectorSnapshotError):
            ConnectorReferenceSnapshot("token", value)
    with pytest.raises(TypeError, match=r"reference name.*text"):
        ConnectorReferenceSnapshot(cast(Any, 1), "TOKEN")
    with pytest.raises(TypeError, match=r"environment variable name.*text"):
        ConnectorReferenceSnapshot("token", cast(Any, 1))


def test_binding_snapshot_is_exact_canonical_non_secret_and_redacted() -> None:
    snapshot = _snapshot(secret_references=(_reference(1), _reference(0)))
    assert snapshot.secret_references == (_reference(0), _reference(1))
    assert snapshot.to_mapping() == {
        "capabilities": {
            "async_io": False,
            "blocking_io": False,
            "idempotency": False,
            "read": True,
            "schema_discovery": False,
            "write": True,
        },
        "configuration": {"endpoint": "https://inventory.invalid"},
        "connector_id": "con_inventory",
        "kind": "inventory-http",
        "revision": 2,
        "schema_discovery": {"version": 1},
        "secret_references": [
            {
                "environment_variable_name": "CONNECTOR_TOKEN_0",
                "reference_name": "token.0",
            },
            {
                "environment_variable_name": "CONNECTOR_TOKEN_1",
                "reference_name": "token.1",
            },
        ],
    }
    rendered = repr(snapshot)
    assert "inventory.invalid" not in rendered
    assert "CONNECTOR_TOKEN" not in rendered
    assert "<redacted>" in rendered
    with pytest.raises(FrozenInstanceError):
        snapshot.revision = 3  # type: ignore[misc]
    assert _snapshot(schema_discovery=None).to_mapping()["schema_discovery"] is None


@pytest.mark.parametrize(
    ("overrides", "error", "message"),
    [
        ({"connector_id": "con_inventory"}, TypeError, "ConnectorId"),
        ({"kind": 1}, TypeError, "kind.*text"),
        ({"kind": ""}, InvalidConnectorSnapshotError, "kind.*size"),
        ({"kind": "Inventory HTTP"}, InvalidConnectorSnapshotError, "kind.*canonical"),
        (
            {"kind": "x" * (MAX_CONNECTOR_KIND_LENGTH + 1)},
            InvalidConnectorSnapshotError,
            "kind.*size",
        ),
        ({"revision": True}, TypeError, "revision.*integer"),
        ({"revision": 0}, InvalidConnectorSnapshotError, "revision.*outside"),
        ({"configuration": {}}, TypeError, "configuration.*ConfigurationDocument"),
        ({"capabilities": ()}, TypeError, "capabilities.*ConnectorCapabilitySet"),
        ({"schema_discovery": {}}, TypeError, "schema discovery.*ConfigurationDocument"),
        ({"secret_references": []}, TypeError, "references.*tuple"),
        ({"secret_references": (object(),)}, TypeError, "references.*invalid"),
        (
            {
                "secret_references": tuple(
                    _reference(index) for index in range(MAX_CONNECTOR_SNAPSHOT_REFERENCES + 1)
                )
            },
            InvalidConnectorSnapshotError,
            "reference limit",
        ),
        (
            {"secret_references": (_reference(0), _reference(0))},
            InvalidConnectorSnapshotError,
            "reference names.*unique",
        ),
    ],
)
def test_binding_snapshot_rejects_invalid_metadata(
    overrides: dict[str, object],
    error: type[Exception],
    message: str,
) -> None:
    with pytest.raises(error, match=message):
        _snapshot(**overrides)

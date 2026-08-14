"""Missing-capability and immutable binding tests for pipeline connectors."""

from __future__ import annotations

from collections.abc import Mapping
from datetime import UTC, datetime
from typing import Any, cast

import pytest

from paritygrid.application.planner import (
    ConnectorCapability,
    ConnectorValidationError,
    InvalidConnectorSnapshotError,
    MissingConnectorCapabilityError,
    MissingConnectorError,
    PipelineDocument,
    connector_capabilities_from_document,
    required_connector_capabilities,
    validate_connector_capabilities,
)
from paritygrid.application.ports import (
    ConfigurationDocument,
    ConnectorRecord,
    ConnectorSecretReference,
)
from paritygrid.domain.models import ConnectorId, UtcTimestamp
from paritygrid.domain.pipeline import NodeKind


def _configuration(**values: object) -> ConfigurationDocument:
    return ConfigurationDocument.from_mapping(values)


def _record(
    identity: str = "con_inventory",
    *,
    capabilities: Mapping[str, object] | None = None,
    schema_discovery: ConfigurationDocument | None = None,
    archived: bool = False,
    references: tuple[ConnectorSecretReference, ...] = (),
) -> ConnectorRecord:
    timestamp = UtcTimestamp(datetime(2026, 8, 14, tzinfo=UTC))
    return ConnectorRecord(
        connector_id=ConnectorId(identity),
        kind="inventory-http",
        display_name="Inventory",
        configuration=_configuration(endpoint="https://inventory.invalid"),
        capabilities=ConfigurationDocument.from_mapping(capabilities or {}),
        schema_discovery=schema_discovery,
        secret_references=references,
        revision=3,
        created_at=timestamp,
        updated_at=timestamp,
        archived_at=timestamp if archived else None,
        row_version=4,
    )


def _node(
    index: int,
    kind: str,
    connector_id: str | None,
) -> dict[str, object]:
    return {
        "configuration": {},
        "configuration_version": 1,
        "connector_id": connector_id,
        "id": f"nod_connector-{index:03d}",
        "kind": kind,
    }


def _document(*nodes: dict[str, object]) -> PipelineDocument:
    return PipelineDocument.from_mapping(
        {
            "canonical_format_version": 1,
            "edges": [],
            "layout": [],
            "nodes": list(nodes),
            "resource_policy": {},
            "schema_version": 1,
        }
    )


def test_capability_document_parser_is_exact_total_and_closed() -> None:
    parsed = connector_capabilities_from_document(
        _configuration(read=True, write=False, idempotency=True)
    )
    assert parsed.values == (ConnectorCapability.IDEMPOTENCY, ConnectorCapability.READ)
    assert connector_capabilities_from_document(_configuration()).values == ()
    with pytest.raises(TypeError, match="ConfigurationDocument"):
        connector_capabilities_from_document(cast(Any, {}))
    with pytest.raises(InvalidConnectorSnapshotError, match="unknown"):
        connector_capabilities_from_document(_configuration(read=True, execute=True))
    with pytest.raises(InvalidConnectorSnapshotError, match="booleans"):
        connector_capabilities_from_document(_configuration(read=1))


def test_required_capability_matrix_is_frozen_for_every_connector_node() -> None:
    expected = {
        "source.http.async": (ConnectorCapability.ASYNC_IO, ConnectorCapability.READ),
        "source.http.blocking": (ConnectorCapability.BLOCKING_IO, ConnectorCapability.READ),
        "source.csv": (ConnectorCapability.READ,),
        "source.jsonl": (ConnectorCapability.READ,),
        "reconcile.target": (ConnectorCapability.READ,),
        "repair.apply": (ConnectorCapability.IDEMPOTENCY, ConnectorCapability.WRITE),
        "verify.target": (ConnectorCapability.READ,),
        "transform.normalize": (),
    }
    assert {kind: required_connector_capabilities(NodeKind(kind)) for kind in expected} == expected
    with pytest.raises(TypeError, match="NodeKind"):
        required_connector_capabilities(cast(Any, "source.csv"))


def test_valid_bindings_return_sorted_deduplicated_non_secret_snapshots() -> None:
    reference = ConnectorSecretReference("primary.token", "INVENTORY_API_TOKEN")
    document = _document(
        _node(0, "source.csv", "con_inventory"),
        _node(1, "verify.target", "con_inventory"),
        _node(2, "source.jsonl", "con_alpha"),
        _node(3, "export.parquet", None),
    )
    records = (
        _record("con_inventory", capabilities={"read": True}, references=(reference,)),
        _record("con_alpha", capabilities={"read": True}),
    )
    snapshots = validate_connector_capabilities(document, records)
    assert tuple(str(snapshot.connector_id) for snapshot in snapshots) == (
        "con_alpha",
        "con_inventory",
    )
    inventory = snapshots[1].to_mapping()
    assert set(inventory) == {
        "capabilities",
        "configuration",
        "connector_id",
        "kind",
        "revision",
        "schema_discovery",
        "secret_references",
    }
    assert inventory["secret_references"] == [
        {
            "environment_variable_name": "INVENTORY_API_TOKEN",
            "reference_name": "primary.token",
        }
    ]
    assert "Inventory" not in repr(snapshots[1])


@pytest.mark.parametrize(
    ("kind", "capabilities"),
    [
        ("source.http.async", {"read": True}),
        ("source.http.blocking", {"read": True}),
        ("source.csv", {}),
        ("source.jsonl", {}),
        ("reconcile.target", {}),
        ("repair.apply", {"write": True}),
        ("repair.apply", {"idempotency": True}),
        ("verify.target", {}),
    ],
)
def test_each_missing_required_capability_fails_closed(
    kind: str,
    capabilities: dict[str, object],
) -> None:
    document = _document(_node(0, kind, "con_inventory"))
    with pytest.raises(MissingConnectorCapabilityError, match="required capability"):
        validate_connector_capabilities(
            document,
            (_record(capabilities=capabilities),),
        )


def test_async_blocking_and_repair_capability_sets_are_accepted() -> None:
    cases = (
        ("source.http.async", {"async_io": True, "read": True}),
        ("source.http.blocking", {"blocking_io": True, "read": True}),
        ("repair.apply", {"idempotency": True, "write": True}),
    )
    for kind, capabilities in cases:
        assert (
            len(
                validate_connector_capabilities(
                    _document(_node(0, kind, "con_inventory")),
                    (_record(capabilities=capabilities),),
                )
            )
            == 1
        )


def test_binding_presence_archival_and_extraneous_connector_rules_are_strict() -> None:
    with pytest.raises(MissingConnectorError, match="requires"):
        validate_connector_capabilities(_document(_node(0, "source.csv", None)), ())
    with pytest.raises(MissingConnectorError, match="unavailable"):
        validate_connector_capabilities(
            _document(_node(0, "source.csv", "con_missing")),
            (_record(capabilities={"read": True}),),
        )
    with pytest.raises(MissingConnectorError, match="unavailable"):
        validate_connector_capabilities(
            _document(_node(0, "source.csv", "con_inventory")),
            (_record(capabilities={"read": True}, archived=True),),
        )
    with pytest.raises(ConnectorValidationError, match="does not permit"):
        validate_connector_capabilities(
            _document(_node(0, "transform.normalize", "con_inventory")),
            (_record(capabilities={"read": True}),),
        )


def test_duplicate_wrong_record_collections_and_schema_metadata_are_rejected() -> None:
    document = _document(_node(0, "source.csv", "con_inventory"))
    record = _record(capabilities={"read": True})
    with pytest.raises(TypeError, match="tuple"):
        validate_connector_capabilities(document, cast(Any, [record]))
    with pytest.raises(TypeError, match="invalid"):
        validate_connector_capabilities(document, cast(Any, (object(),)))
    with pytest.raises(InvalidConnectorSnapshotError, match="duplicate"):
        validate_connector_capabilities(document, (record, record))
    with pytest.raises(InvalidConnectorSnapshotError, match="schema metadata"):
        validate_connector_capabilities(
            document,
            (
                _record(
                    capabilities={"read": True},
                    schema_discovery=_configuration(columns=[]),
                ),
            ),
        )
    snapshots = validate_connector_capabilities(
        document,
        (
            _record(
                capabilities={"read": True, "schema_discovery": True},
                schema_discovery=_configuration(columns=[]),
            ),
        ),
    )
    assert snapshots[0].schema_discovery == _configuration(columns=[])


def test_connector_validator_requires_exact_document_and_reference_records() -> None:
    with pytest.raises(TypeError, match="PipelineDocument"):
        validate_connector_capabilities(cast(Any, {}), ())
    document = _document(_node(0, "source.csv", "con_inventory"))
    bad_record = _record(
        capabilities={"read": True},
        references=cast(Any, (object(),)),
    )
    with pytest.raises(TypeError, match=r"secret references.*invalid"):
        validate_connector_capabilities(document, (bad_record,))

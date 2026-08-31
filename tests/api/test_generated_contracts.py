"""Deterministic contract generation and drift detection."""

import json
import subprocess
import sys
from pathlib import Path

from paritygrid.api.openapi_export import build_document as build_openapi_document
from paritygrid.api.ts_generation import (
    SchemaEmitter,
)
from paritygrid.api.ts_generation import (
    build_document as build_types_document,
)

PROJECT_ROOT = Path(__file__).resolve().parents[2]

OPENAPI_PATH = PROJECT_ROOT / "docs" / "generated" / "openapi.json"
SCHEMA_PATH = PROJECT_ROOT / "web" / "src" / "api" / "generated" / "schema.d.ts"


def test_openapi_export_is_deterministic() -> None:
    first = build_openapi_document()
    second = build_openapi_document()
    assert first == second


def test_committed_openapi_document_matches_the_factory() -> None:
    committed = OPENAPI_PATH.read_text(encoding="utf-8")
    assert committed == build_openapi_document()


def test_openapi_covers_every_public_route_and_transport() -> None:
    document = json.loads(OPENAPI_PATH.read_text(encoding="utf-8"))
    paths = document["paths"]
    for expected in (
        "/healthz",
        "/readyz",
        "/api/v1/system/capabilities",
        "/api/v1/pipelines",
        "/api/v1/connectors",
        "/api/v1/runs",
        "/api/v1/runs/{run_id}/reconciliation",
        "/api/v1/runs/{run_id}/conflicts",
        "/api/v1/runs/{run_id}/repair-plans",
        "/api/v1/repair-plans/{plan_id}",
        "/api/v1/repair-plans/{plan_id}/approve",
        "/api/v1/repair-plans/{plan_id}/apply",
        "/api/v1/artifacts/{artifact_id}",
        "/api/v1/stream/runs/{run_id}",
    ):
        assert expected in paths, expected
    transports = document["x-paritygrid-live-transports"]
    assert {entry["path"] for entry in transports} == {
        "/api/v1/live/runs/{run_id}",
        "/api/v1/stream/runs/{run_id}",
    }
    telemetry = next(entry for entry in transports if entry["channel"] == "telemetry")
    assert telemetry["advisory"] is True
    assert telemetry["protocol"] == "websocket"


def test_openapi_document_carries_no_nondeterministic_detail() -> None:
    text = OPENAPI_PATH.read_text(encoding="utf-8")
    assert "Users" not in text
    assert "E:\\\\Python" not in text
    assert "file:///" not in text
    import re

    assert re.search(r"20\d\d-\d\d-\d\dT\d\d:", text) is None
    lowered = text.lower()
    for forbidden in ("password", "api_key", "credential", "private_key"):
        assert forbidden not in lowered
    document = json.loads(text)
    assert list(document["paths"]) == sorted(document["paths"])


def test_generated_types_are_deterministic_and_committed() -> None:
    source = json.loads(OPENAPI_PATH.read_text(encoding="utf-8"))
    first = build_types_document(source)
    second = build_types_document(source)
    assert first == second
    committed = SCHEMA_PATH.read_text(encoding="utf-8")
    assert committed == first
    assert "Do not edit by hand" in committed


def test_generated_parameter_arrays_use_a_valid_single_type_argument() -> None:
    source = json.loads(OPENAPI_PATH.read_text(encoding="utf-8"))
    generated = build_types_document(source)
    assert "parameters: Array<{ name:" in generated
    assert ">, { name:" not in generated
    assert " | { name:" in generated


def test_generated_types_cover_the_domain_schemas() -> None:
    text = SCHEMA_PATH.read_text(encoding="utf-8")
    for name in (
        "RunResponse",
        "PipelineResponse",
        "ConnectorResponse",
        "ReconciliationResponse",
        "ConflictPageResponse",
        "RepairPlanResponse",
        "RepairApplyResponse",
        "ObservationSideInput",
    ):
        assert f'export type {name} = components["schemas"]["{name}"];' in text
    assert "export interface operations {" in text
    assert "create_repair_plan_api_v" in text
    assert "approve_repair_plan_api_v" in text
    assert "apply_repair_plan_api_v" in text
    assert "stream_run_events_api_v" in text


def test_emitter_maps_schema_constructs_to_strict_types() -> None:
    emitter = SchemaEmitter()
    assert emitter.type_of({"type": "string"}) == "string"
    assert emitter.type_of({"type": "integer"}) == "number"
    assert emitter.type_of({"type": "boolean"}) == "boolean"
    assert emitter.type_of({"type": "null"}) == "null"
    assert emitter.type_of({"enum": ["a", "b"]}) == '"a" | "b"'
    assert emitter.type_of({"const": 7}) == "7"
    assert emitter.type_of({"type": "array", "items": {"type": "string"}}) == "Array<string>"
    assert emitter.type_of({"anyOf": [{"type": "string"}, {"type": "null"}]}) == "string | null"
    assert emitter.type_of({"allOf": [{"type": "string"}, {"type": "string"}]}) == "string & string"
    assert (
        emitter.type_of({"$ref": "#/components/schemas/RunResponse"})
        == 'components["schemas"]["RunResponse"]'
    )
    assert emitter.type_of({}) == "unknown"
    assert emitter.type_of({"additionalProperties": {"type": "string"}}) == (
        "{\n    [key: string]: string;\n}"
    )
    assert emitter.type_of({"type": "object", "additionalProperties": True}) == (
        "{\n    [key: string]: unknown;\n}"
    )
    assert emitter.type_of({"type": "object"}) == "Record<string, never>"
    assert emitter.type_of({"enum": []}) == "never"


def test_export_scripts_report_drift() -> None:
    original = OPENAPI_PATH.read_text(encoding="utf-8")
    tampered = original.replace("ParityGrid", "Tampered", 1)
    try:
        OPENAPI_PATH.write_text(tampered, encoding="utf-8")
        drift = subprocess.run(
            [sys.executable, str(PROJECT_ROOT / "scripts" / "export_openapi.py"), "--check"],
            capture_output=True,
            text=True,
            check=False,
        )
        assert drift.returncode == 1
        OPENAPI_PATH.write_text(original, encoding="utf-8")
        clean = subprocess.run(
            [sys.executable, str(PROJECT_ROOT / "scripts" / "export_openapi.py"), "--check"],
            capture_output=True,
            text=True,
            check=False,
        )
        assert clean.returncode == 0
        types = subprocess.run(
            [sys.executable, str(PROJECT_ROOT / "scripts" / "generate_api_types.py"), "--check"],
            capture_output=True,
            text=True,
            check=False,
        )
        assert types.returncode == 0
    finally:
        OPENAPI_PATH.write_text(original, encoding="utf-8")

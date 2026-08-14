"""Default, bound, and unknown-field tests for pipeline resource policies."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any, cast

import pytest

from paritygrid.application.planner import (
    DEFAULT_RESOURCE_POLICY,
    PipelineDocument,
    ResourcePolicy,
    ResourcePolicyError,
    parse_resource_policy,
    validate_resource_policy,
)
from paritygrid.application.ports import ConfigurationDocument


def _configuration(**values: object) -> ConfigurationDocument:
    return ConfigurationDocument.from_mapping(values)


def _document(policy: Mapping[str, object]) -> PipelineDocument:
    document: dict[str, object] = {
        "canonical_format_version": 1,
        "edges": [],
        "layout": [],
        "nodes": [
            {
                "configuration": {},
                "configuration_version": 1,
                "connector_id": None,
                "id": "nod_resource-001",
                "kind": "source.csv",
            }
        ],
        "resource_policy": policy,
        "schema_version": 1,
    }
    return PipelineDocument.from_mapping(document)


def test_empty_policy_materializes_frozen_defaults() -> None:
    assert parse_resource_policy(_configuration()) == DEFAULT_RESOURCE_POLICY
    assert validate_resource_policy(_document({})) == DEFAULT_RESOURCE_POLICY


def test_partial_and_total_policies_materialize_exact_values() -> None:
    partial = parse_resource_policy(
        _configuration(max_concurrency=8, max_in_flight=32, queue_capacity=64)
    )
    assert partial == ResourcePolicy(
        max_concurrency=8,
        max_in_flight=32,
        memory_limit_bytes=536_870_912,
        operation_timeout_seconds=60,
        queue_capacity=64,
    )
    total = {
        "max_concurrency": 2,
        "max_in_flight": 4,
        "memory_limit_bytes": 268_435_456,
        "operation_timeout_seconds": 15,
        "queue_capacity": 8,
    }
    assert validate_resource_policy(_document(total)).to_mapping() == total


def test_unknown_fields_and_exact_integer_type_confusion_fail_closed() -> None:
    with pytest.raises(ResourcePolicyError, match="unknown"):
        parse_resource_policy(_configuration(max_workers=4))
    for value in (True, "4", [4]):
        with pytest.raises(TypeError, match="integers"):
            parse_resource_policy(_configuration(max_concurrency=value))


def test_parsed_policies_enforce_individual_and_relational_bounds() -> None:
    for policy in (
        {"max_concurrency": 0},
        {"max_in_flight": 1_025},
        {"memory_limit_bytes": 1},
        {"operation_timeout_seconds": 3_601},
        {"queue_capacity": 0},
        {"max_concurrency": 8, "max_in_flight": 4},
        {"max_in_flight": 32, "queue_capacity": 16},
    ):
        with pytest.raises(ResourcePolicyError):
            parse_resource_policy(ConfigurationDocument.from_mapping(policy))


def test_resource_parsers_require_exact_public_contracts() -> None:
    with pytest.raises(TypeError, match="ConfigurationDocument"):
        parse_resource_policy(cast(Any, {}))
    with pytest.raises(TypeError, match="PipelineDocument"):
        validate_resource_policy(cast(Any, {}))

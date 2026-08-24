"""Versioned concurrency telemetry tests for P7.8: schema, bounds, drops, non-authority."""

from __future__ import annotations

import dataclasses
import threading
from pathlib import Path
from typing import cast

import pytest

import paritygrid.application.execution as execution_package
import paritygrid.application.execution.telemetry as telemetry_module
from paritygrid.application.execution import (
    CHANNEL_KIND_TELEMETRY,
    MAX_COLLECTOR_CAPACITY,
    MAX_LABEL_COUNT,
    MAX_LABEL_LENGTH,
    MAX_METRIC_NAME_LENGTH,
    MAX_METRIC_VALUE,
    MAX_SERIES_PER_RECORD,
    MAX_TELEMETRY_RECORD_BYTES,
    TELEMETRY_SCHEMA_VERSION,
    BoundedChannel,
    CapacitySnapshot,
    ConcurrentScheduler,
    ControlGeneration,
    TelemetryCollector,
    TelemetryError,
    TelemetryMetric,
    TelemetryMetricKind,
    TelemetryRecord,
    TelemetryRedactionError,
    TelemetrySchemaError,
    TelemetryValidationError,
    blocked_writer_record,
    capacity_snapshot_records,
    capacity_wait_record,
    cleanup_state_record,
    dropped_telemetry_record,
    queue_depth_record,
    service_duration_record,
    unresolved_resources_record,
)

FINGERPRINT = "0123456789abcdef" * 4
JOIN_TIMEOUT_SECONDS = 10.0
PER_THREAD = 64
RUN_ID = "run-telemetry"
THREADS = 8

GOLDEN_RECORD = TelemetryRecord(
    schema_version=TELEMETRY_SCHEMA_VERSION,
    observed_at_micros=1_000,
    run_id=RUN_ID,
    metrics=(
        TelemetryMetric(
            name="queue_depth",
            kind=TelemetryMetricKind.QUEUE_DEPTH,
            value=3,
            labels=(("channel", "telemetry"),),
        ),
        TelemetryMetric(
            name="global_active_capacity",
            kind=TelemetryMetricKind.ACTIVE_CAPACITY,
            value=2,
            labels=(("category", "global"), ("limit", "4")),
        ),
    ),
)
GOLDEN_WIRE = (
    b"schema_version=1\n"
    b"observed_at_micros=1000\n"
    b"run_id=run-telemetry\n"
    b"queue_depth|queue_depth|3|channel=telemetry\n"
    b"global_active_capacity|active_capacity|2|category=global;limit=4"
)


def _metric(
    name: str = "queue_depth",
    kind: TelemetryMetricKind = TelemetryMetricKind.QUEUE_DEPTH,
    value: int = 3,
    labels: tuple[tuple[str, str], ...] = (("channel", "telemetry"),),
) -> TelemetryMetric:
    return TelemetryMetric(name=name, kind=kind, value=value, labels=labels)


def _record(
    *metrics: TelemetryMetric,
    schema_version: int = TELEMETRY_SCHEMA_VERSION,
    observed_at_micros: int = 1_000,
    run_id: str = RUN_ID,
) -> TelemetryRecord:
    return TelemetryRecord(
        schema_version=schema_version,
        observed_at_micros=observed_at_micros,
        run_id=run_id,
        metrics=metrics if metrics else (_metric(),),
    )


def _labels(count: int) -> tuple[tuple[str, str], ...]:
    return tuple((f"key{index:02d}", f"value-{index}") for index in range(count))


def _metric_mapping() -> dict[str, object]:
    return {
        "name": "queue_depth",
        "kind": "queue_depth",
        "value": 3,
        "labels": [["channel", "telemetry"]],
    }


def _record_mapping() -> dict[str, object]:
    return {
        "schema_version": TELEMETRY_SCHEMA_VERSION,
        "observed_at_micros": 1_000,
        "run_id": RUN_ID,
        "metrics": [_metric_mapping()],
    }


def _wire_line(index: int, replacement: bytes) -> bytes:
    lines = GOLDEN_WIRE.split(b"\n")
    lines[index] = replacement
    return b"\n".join(lines)


def _snapshot(category: str, limit: int = 4, in_use: int = 2) -> CapacitySnapshot:
    return CapacitySnapshot(
        category=category,
        limit=limit,
        in_use=in_use,
        waiting=0,
        max_observed_in_use=in_use,
    )


def _two_node_scheduler() -> ConcurrentScheduler:
    return ConcurrentScheduler(
        run_id=RUN_ID,
        plan_fingerprint=FINGERPRINT,
        node_order=("extract", "load"),
        edges=(("extract", "load"),),
        partitions_by_node={"extract": ("p1", "p2"), "load": ("p1",)},
        control_generation=ControlGeneration(1),
    )


class TestContractConstants:
    def test_schema_version_is_one(self) -> None:
        assert TELEMETRY_SCHEMA_VERSION == 1

    def test_bounds_match_the_specified_limits(self) -> None:
        assert MAX_METRIC_NAME_LENGTH == 64
        assert MAX_LABEL_COUNT == 8
        assert MAX_LABEL_LENGTH == 64
        assert MAX_SERIES_PER_RECORD == 16
        assert MAX_TELEMETRY_RECORD_BYTES == 4_096
        assert MAX_COLLECTOR_CAPACITY == 65_536

    def test_metric_value_bound_matches_the_runner_contract(self) -> None:
        assert MAX_METRIC_VALUE == 2**31 - 1
        assert MAX_METRIC_VALUE == execution_package.MAX_METRIC_VALUE

    def test_error_hierarchy_is_typed_under_one_base(self) -> None:
        for error in (
            TelemetryValidationError,
            TelemetrySchemaError,
            TelemetryRedactionError,
        ):
            assert issubclass(error, TelemetryError)
            assert issubclass(error, RuntimeError)

    def test_the_eight_required_observation_kinds_are_closed(self) -> None:
        assert {kind.value for kind in TelemetryMetricKind} == {
            "queue_depth",
            "active_capacity",
            "capacity_wait",
            "service_duration",
            "blocked_writer",
            "dropped_telemetry",
            "cleanup_state",
            "unresolved_resources",
        }
        assert len(TelemetryMetricKind) == 8


class TestMetricValidation:
    def test_minimal_metric_without_labels_is_accepted(self) -> None:
        metric = TelemetryMetric(name="queue_depth", kind=TelemetryMetricKind.QUEUE_DEPTH, value=0)
        assert metric.labels == ()

    @pytest.mark.parametrize("kind", list(TelemetryMetricKind))
    def test_each_observation_kind_is_accepted(self, kind: TelemetryMetricKind) -> None:
        metric = TelemetryMetric(name=kind.value, kind=kind, value=1)
        assert metric.kind is kind

    @pytest.mark.parametrize("name", ["q", "a" * MAX_METRIC_NAME_LENGTH, "q1_2"])
    def test_metric_name_boundaries_are_accepted(self, name: str) -> None:
        assert (
            TelemetryMetric(name=name, kind=TelemetryMetricKind.QUEUE_DEPTH, value=0).name == name
        )

    @pytest.mark.parametrize(
        "name",
        ["", "a" * (MAX_METRIC_NAME_LENGTH + 1), "Queue", "1queue", "queue-depth", "sp ace", "q!"],
    )
    def test_invalid_metric_names_are_rejected(self, name: str) -> None:
        with pytest.raises(TelemetryValidationError):
            _metric(name=name)

    @pytest.mark.parametrize("name", [7, b"queue", None, True])
    def test_non_text_metric_names_are_rejected(self, name: object) -> None:
        with pytest.raises(TelemetryValidationError):
            _metric(name=cast(str, name))

    def test_value_boundaries_are_accepted(self) -> None:
        assert _metric(value=0).value == 0
        assert _metric(value=MAX_METRIC_VALUE).value == MAX_METRIC_VALUE

    @pytest.mark.parametrize("value", [-1, MAX_METRIC_VALUE + 1, 2**40])
    def test_values_outside_bounds_are_rejected(self, value: int) -> None:
        with pytest.raises(TelemetryValidationError):
            _metric(value=value)

    @pytest.mark.parametrize("value", [True, False, 1.0, "3", None])
    def test_non_integer_values_are_rejected(self, value: object) -> None:
        with pytest.raises(TelemetryValidationError):
            _metric(value=cast(int, value))

    @pytest.mark.parametrize("kind", ["queue_depth", 7, None, True])
    def test_non_enum_kinds_are_rejected(self, kind: object) -> None:
        with pytest.raises(TelemetryValidationError):
            _metric(kind=cast(TelemetryMetricKind, kind))

    @pytest.mark.parametrize("count", [0, MAX_LABEL_COUNT])
    def test_label_count_boundaries_are_accepted(self, count: int) -> None:
        assert len(_metric(labels=_labels(count)).labels) == count

    def test_label_count_above_the_bound_is_rejected(self) -> None:
        with pytest.raises(TelemetryValidationError):
            _metric(labels=_labels(MAX_LABEL_COUNT + 1))

    def test_label_key_length_boundaries_are_accepted(self) -> None:
        assert _metric(labels=(("k" * 32, "v"),)).labels == (("k" * 32, "v"),)

    def test_label_key_above_the_length_bound_is_rejected(self) -> None:
        with pytest.raises(TelemetryValidationError):
            _metric(labels=(("k" * 33, "v"),))

    @pytest.mark.parametrize("key", ["Key", "1key", "key-x", ""])
    def test_invalid_label_keys_are_rejected(self, key: str) -> None:
        with pytest.raises(TelemetryValidationError):
            _metric(labels=((key, "value"),))

    @pytest.mark.parametrize("value", ["v", "v" * MAX_LABEL_LENGTH])
    def test_label_value_length_boundaries_are_accepted(self, value: str) -> None:
        assert _metric(labels=(("channel", value),)).labels == (("channel", value),)

    @pytest.mark.parametrize("value", ["", "v" * (MAX_LABEL_LENGTH + 1)])
    def test_label_values_outside_length_bounds_are_rejected(self, value: str) -> None:
        with pytest.raises(TelemetryValidationError):
            _metric(labels=(("channel", value),))

    @pytest.mark.parametrize("value", ["bad\x1fvalue", "caf\xc3\xa9", "tab\tvalue"])
    def test_non_printable_label_values_are_rejected(self, value: str) -> None:
        with pytest.raises(TelemetryValidationError):
            _metric(labels=(("channel", value),))

    @pytest.mark.parametrize("pair", [(7, "v"), ("channel", 7), ("channel", None)])
    def test_non_text_label_members_are_rejected(self, pair: tuple[object, object]) -> None:
        with pytest.raises(TelemetryValidationError):
            _metric(labels=(cast("tuple[tuple[str, str], ...]", (pair,))))

    def test_labels_container_must_be_a_tuple(self) -> None:
        with pytest.raises(TelemetryValidationError):
            _metric(labels=cast("tuple[tuple[str, str], ...]", [("channel", "v")]))

    @pytest.mark.parametrize("entry", [("channel", "v", "x"), "channel", ()])
    def test_label_entries_must_be_pairs(self, entry: object) -> None:
        with pytest.raises(TelemetryValidationError):
            _metric(labels=(cast("tuple[tuple[str, str], ...]", (entry,))))

    def test_label_keys_must_be_sorted(self) -> None:
        with pytest.raises(TelemetryValidationError):
            _metric(labels=(("zone", "eu"), ("channel", "result")))

    def test_label_keys_must_be_unique(self) -> None:
        with pytest.raises(TelemetryValidationError):
            _metric(labels=(("channel", "a"), ("channel", "b")))

    def test_metric_is_frozen(self) -> None:
        with pytest.raises(dataclasses.FrozenInstanceError):
            _metric().value = 4  # type: ignore[misc]

    def test_metric_is_slotted(self) -> None:
        assert not hasattr(_metric(), "__dict__")

    def test_metric_repr_is_bounded_and_redacted(self) -> None:
        representation = repr(_metric())
        assert "value=3" in representation
        assert "telemetry" not in representation
        assert "labels=1" in representation


class TestMetricRedaction:
    @pytest.mark.parametrize(
        "value",
        ["password=hunter2", "token:abc123", "bearer xyz", "secret/lease", "TOKEN:x", "PASSWORD=x"],
    )
    def test_secret_markers_are_rejected(self, value: str) -> None:
        with pytest.raises(TelemetryRedactionError):
            _metric(labels=(("note", value),))

    def test_secret_marker_inside_a_longer_value_is_rejected(self) -> None:
        with pytest.raises(TelemetryRedactionError):
            _metric(labels=(("note", "opaque password=x here"),))

    @pytest.mark.parametrize("value", ["/etc/app", "\\windows", "C:\\Users", "..", "logs/../x"])
    def test_path_shapes_are_rejected(self, value: str) -> None:
        with pytest.raises(TelemetryRedactionError):
            _metric(labels=(("note", value),))

    @pytest.mark.parametrize("value", ["worker-1", "run/1", "global", "a=b", "c"])
    def test_safe_values_are_accepted(self, value: str) -> None:
        assert _metric(labels=(("note", value),)).labels == (("note", value),)

    def test_run_id_rejects_secret_markers(self) -> None:
        with pytest.raises(TelemetryRedactionError):
            _record(run_id="run token:abc")

    def test_run_id_rejects_path_shapes(self) -> None:
        with pytest.raises(TelemetryRedactionError):
            _record(run_id="/run/one")


class TestRecordValidation:
    def test_minimal_record_is_accepted(self) -> None:
        record = _record()
        assert record.schema_version == TELEMETRY_SCHEMA_VERSION
        assert record.observed_at_micros == 1_000
        assert record.run_id == RUN_ID
        assert len(record.metrics) == 1

    @pytest.mark.parametrize("schema_version", [0, 2, -1])
    def test_unknown_schema_versions_fail_closed(self, schema_version: int) -> None:
        with pytest.raises(TelemetrySchemaError):
            _record(schema_version=schema_version)

    @pytest.mark.parametrize("schema_version", [True, "1", 1.0, None])
    def test_non_integer_schema_versions_are_rejected(self, schema_version: object) -> None:
        with pytest.raises(TelemetryValidationError):
            _record(schema_version=cast(int, schema_version))

    def test_zero_observation_time_is_accepted(self) -> None:
        assert _record(observed_at_micros=0).observed_at_micros == 0

    def test_negative_observation_time_is_rejected(self) -> None:
        with pytest.raises(TelemetryValidationError):
            _record(observed_at_micros=-1)

    @pytest.mark.parametrize("observed", [True, "0", 1.0, None])
    def test_non_integer_observation_times_are_rejected(self, observed: object) -> None:
        with pytest.raises(TelemetryValidationError):
            _record(observed_at_micros=cast(int, observed))

    def test_run_id_length_boundaries_are_accepted(self) -> None:
        assert _record(run_id="r" * 128).run_id == "r" * 128

    @pytest.mark.parametrize("run_id", ["", "r" * 129])
    def test_run_id_lengths_outside_bounds_are_rejected(self, run_id: str) -> None:
        with pytest.raises(TelemetryValidationError):
            _record(run_id=run_id)

    @pytest.mark.parametrize("run_id", ["bad\x1frun", "run\xc3\xa9", 7, None, True])
    def test_non_printable_or_non_text_run_ids_are_rejected(self, run_id: object) -> None:
        with pytest.raises(TelemetryValidationError):
            _record(run_id=cast(str, run_id))

    def test_series_count_boundaries_are_accepted(self) -> None:
        series = tuple(
            _metric(name=f"m{index:02d}_depth", value=index)
            for index in range(MAX_SERIES_PER_RECORD)
        )
        assert len(_record(*series).metrics) == MAX_SERIES_PER_RECORD

    @pytest.mark.parametrize("series_count", [0, MAX_SERIES_PER_RECORD + 1])
    def test_series_counts_outside_bounds_are_rejected(self, series_count: int) -> None:
        series = tuple(_metric(name=f"m{index:02d}_depth") for index in range(series_count))
        with pytest.raises(TelemetryValidationError):
            TelemetryRecord(
                schema_version=TELEMETRY_SCHEMA_VERSION,
                observed_at_micros=1_000,
                run_id=RUN_ID,
                metrics=series,
            )

    def test_metrics_container_must_be_a_tuple(self) -> None:
        with pytest.raises(TelemetryValidationError):
            TelemetryRecord(
                schema_version=TELEMETRY_SCHEMA_VERSION,
                observed_at_micros=1,
                run_id=RUN_ID,
                metrics=cast(
                    "tuple[TelemetryMetric, ...]",
                    [_metric()],
                ),
            )

    def test_metric_items_must_use_telemetry_metric(self) -> None:
        with pytest.raises(TelemetryValidationError):
            _record(
                _metric(),
                cast(TelemetryMetric, ("queue_depth", TelemetryMetricKind.QUEUE_DEPTH, 1)),
            )

    def test_record_is_frozen(self) -> None:
        with pytest.raises(dataclasses.FrozenInstanceError):
            _record().run_id = "other"  # type: ignore[misc]

    def test_record_is_slotted(self) -> None:
        assert not hasattr(_record(), "__dict__")

    def test_record_repr_is_bounded_and_redacted(self) -> None:
        representation = repr(_record())
        assert "run-telemetry" in representation
        assert "metrics=1" in representation
        assert "channel" not in representation


class TestMappingContract:
    def test_to_mapping_shape_is_exact(self) -> None:
        assert GOLDEN_RECORD.to_mapping() == {
            "schema_version": 1,
            "observed_at_micros": 1_000,
            "run_id": "run-telemetry",
            "metrics": [
                {
                    "name": "queue_depth",
                    "kind": "queue_depth",
                    "value": 3,
                    "labels": [["channel", "telemetry"]],
                },
                {
                    "name": "global_active_capacity",
                    "kind": "active_capacity",
                    "value": 2,
                    "labels": [["category", "global"], ["limit", "4"]],
                },
            ],
        }

    def test_mapping_round_trip_is_equal(self) -> None:
        assert TelemetryRecord.from_mapping(GOLDEN_RECORD.to_mapping()) == GOLDEN_RECORD

    def test_metric_mapping_round_trip_is_equal(self) -> None:
        metric = _metric()
        assert TelemetryMetric.from_mapping(metric.to_mapping()) == metric

    @pytest.mark.parametrize("data", [[], "record", None, 7])
    def test_from_mapping_rejects_non_mappings(self, data: object) -> None:
        with pytest.raises(TelemetryValidationError):
            TelemetryRecord.from_mapping(data)

    def test_from_mapping_rejects_unknown_keys(self) -> None:
        mapping = _record_mapping()
        mapping["extra"] = "nope"
        with pytest.raises(TelemetryValidationError):
            TelemetryRecord.from_mapping(mapping)

    @pytest.mark.parametrize(
        "key",
        ["schema_version", "observed_at_micros", "run_id", "metrics"],
    )
    def test_from_mapping_rejects_missing_keys(self, key: str) -> None:
        mapping = _record_mapping()
        del mapping[key]
        with pytest.raises(TelemetryValidationError):
            TelemetryRecord.from_mapping(mapping)

    @pytest.mark.parametrize("schema_version", [0, 2])
    def test_from_mapping_fails_closed_on_unknown_schema(self, schema_version: int) -> None:
        mapping = _record_mapping()
        mapping["schema_version"] = schema_version
        with pytest.raises(TelemetrySchemaError):
            TelemetryRecord.from_mapping(mapping)

    @pytest.mark.parametrize("schema_version", [True, "1", None])
    def test_from_mapping_rejects_non_integer_schema(self, schema_version: object) -> None:
        mapping = _record_mapping()
        mapping["schema_version"] = schema_version
        with pytest.raises(TelemetryValidationError):
            TelemetryRecord.from_mapping(mapping)

    @pytest.mark.parametrize("observed", [True, "0", None])
    def test_from_mapping_rejects_bad_observation_times(self, observed: object) -> None:
        mapping = _record_mapping()
        mapping["observed_at_micros"] = observed
        with pytest.raises(TelemetryValidationError):
            TelemetryRecord.from_mapping(mapping)

    def test_from_mapping_rejects_negative_observation_time(self) -> None:
        mapping = _record_mapping()
        mapping["observed_at_micros"] = -1
        with pytest.raises(TelemetryValidationError):
            TelemetryRecord.from_mapping(mapping)

    @pytest.mark.parametrize("metrics", [[], ["nope"], None, ()])
    def test_from_mapping_rejects_bad_metrics_containers(self, metrics: object) -> None:
        mapping = _record_mapping()
        mapping["metrics"] = metrics
        with pytest.raises(TelemetryValidationError):
            TelemetryRecord.from_mapping(mapping)

    def test_from_mapping_rejects_too_many_series(self) -> None:
        mapping = _record_mapping()
        mapping["metrics"] = [_metric_mapping()] * (MAX_SERIES_PER_RECORD + 1)
        with pytest.raises(TelemetryValidationError):
            TelemetryRecord.from_mapping(mapping)

    @pytest.mark.parametrize("entry", ["nope", None, 7])
    def test_from_mapping_rejects_non_dict_metric_entries(self, entry: object) -> None:
        mapping = _record_mapping()
        mapping["metrics"] = [entry]
        with pytest.raises(TelemetryValidationError):
            TelemetryRecord.from_mapping(mapping)

    @pytest.mark.parametrize(
        "mutation",
        [
            {"extra": "nope"},
            {"name": 7},
            {"kind": 7},
            {"kind": "not_a_kind"},
            {"value": True},
            {"value": "3"},
            {"labels": "channel"},
            {"labels": None},
            {"labels": [("channel", "v")]},
            {"labels": [["channel"]]},
            {"labels": [["channel", "v", "x"]]},
            {"labels": [["channel", 7]]},
            {"labels": [[7, "v"]]},
            {"labels": [["note", "token:abc"]]},
        ],
    )
    def test_from_mapping_rejects_bad_metric_entries(self, mutation: dict[str, object]) -> None:
        metric_entry = _metric_mapping()
        for key, value in mutation.items():
            metric_entry[key] = value
        with pytest.raises(TelemetryError):
            TelemetryMetric.from_mapping(metric_entry)

    def test_from_mapping_metric_rejects_missing_and_unknown_keys(self) -> None:
        missing = _metric_mapping()
        del missing["value"]
        with pytest.raises(TelemetryValidationError):
            TelemetryMetric.from_mapping(missing)

    def test_from_mapping_propagates_run_id_redaction(self) -> None:
        mapping = _record_mapping()
        mapping["run_id"] = "run token:abc"
        with pytest.raises(TelemetryRedactionError):
            TelemetryRecord.from_mapping(mapping)

    def test_from_mapping_propagates_label_redaction(self) -> None:
        metric_entry = _metric_mapping()
        metric_entry["labels"] = [["note", "password=x"]]
        with pytest.raises(TelemetryRedactionError):
            TelemetryRecord.from_mapping(_record_mapping() | {"metrics": [metric_entry]})


class TestWireContract:
    def test_golden_wire_bytes_are_exact(self) -> None:
        assert GOLDEN_RECORD.wire_bytes() == GOLDEN_WIRE

    def test_wire_round_trip_is_equal(self) -> None:
        assert TelemetryRecord.from_wire_bytes(GOLDEN_WIRE) == GOLDEN_RECORD
        assert TelemetryRecord.from_wire_bytes(GOLDEN_RECORD.wire_bytes()) == GOLDEN_RECORD

    def test_wire_encoding_is_deterministic(self) -> None:
        assert GOLDEN_RECORD.wire_bytes() == GOLDEN_RECORD.wire_bytes()
        reordered = _record(_metric(), _metric(name="blocked_writer", value=0, labels=()))
        assert reordered.wire_bytes() == reordered.wire_bytes()

    def test_wire_escapes_reserved_label_characters(self) -> None:
        record = _record(_metric(name="note_series", labels=(("note", "a|b;c=d\\e"),), value=1))
        decoded = TelemetryRecord.from_wire_bytes(record.wire_bytes())
        assert decoded == record
        assert decoded.metrics[0].labels == (("note", "a|b;c=d\\e"),)

    def test_wire_bytes_rejects_oversized_records(self) -> None:
        labels = tuple(
            (f"label{index:02d}", "v" * MAX_LABEL_LENGTH) for index in range(MAX_LABEL_COUNT)
        )
        series = tuple(
            _metric(name=f"m{index:02d}_depth", labels=labels, value=index)
            for index in range(MAX_SERIES_PER_RECORD)
        )
        with pytest.raises(TelemetryValidationError):
            _record(*series).wire_bytes()

    def test_from_wire_rejects_oversized_payloads(self) -> None:
        with pytest.raises(TelemetryValidationError):
            TelemetryRecord.from_wire_bytes(GOLDEN_WIRE + b"x" * MAX_TELEMETRY_RECORD_BYTES)

    @pytest.mark.parametrize("data", ["wire", bytearray(b"wire"), memoryview(b"wire"), None])
    def test_from_wire_rejects_non_bytes(self, data: object) -> None:
        with pytest.raises(TelemetryValidationError):
            TelemetryRecord.from_wire_bytes(cast(bytes, data))

    def test_from_wire_rejects_empty_payloads(self) -> None:
        with pytest.raises(TelemetryValidationError):
            TelemetryRecord.from_wire_bytes(b"")

    def test_from_wire_rejects_trailing_newlines(self) -> None:
        with pytest.raises(TelemetryValidationError):
            TelemetryRecord.from_wire_bytes(GOLDEN_WIRE + b"\n")

    def test_from_wire_rejects_non_ascii_payloads(self) -> None:
        payload = b"schema_version=1\nobserved_at_micros=1\nrun_id=\xff\nq|queue_depth|0|"
        with pytest.raises(TelemetryValidationError):
            TelemetryRecord.from_wire_bytes(payload)

    @pytest.mark.parametrize(
        "payload",
        [
            GOLDEN_WIRE[:12],
            b"\n".join(GOLDEN_WIRE.split(b"\n")[:3]),
            GOLDEN_WIRE[:-26],
        ],
    )
    def test_from_wire_rejects_truncated_payloads(self, payload: bytes) -> None:
        with pytest.raises(TelemetryValidationError):
            TelemetryRecord.from_wire_bytes(payload)

    def test_from_wire_rejects_too_many_series_lines(self) -> None:
        extra = b"\n".join(b"m|queue_depth|0|" for _ in range(MAX_SERIES_PER_RECORD + 1))
        with pytest.raises(TelemetryValidationError):
            TelemetryRecord.from_wire_bytes(GOLDEN_WIRE + b"\n" + extra)

    @pytest.mark.parametrize("line", [b"schema_version=0", b"schema_version=2"])
    def test_from_wire_fails_closed_on_unknown_schema(self, line: bytes) -> None:
        with pytest.raises(TelemetrySchemaError):
            TelemetryRecord.from_wire_bytes(_wire_line(0, line))

    @pytest.mark.parametrize("line", [b"schema_version=x", b"schema_version="])
    def test_from_wire_rejects_malformed_schema_text(self, line: bytes) -> None:
        with pytest.raises(TelemetryValidationError):
            TelemetryRecord.from_wire_bytes(_wire_line(0, line))

    @pytest.mark.parametrize("line", [b"version=1", b"schemaversion=1"])
    def test_from_wire_rejects_unknown_field_keys(self, line: bytes) -> None:
        with pytest.raises(TelemetryValidationError):
            TelemetryRecord.from_wire_bytes(_wire_line(0, line))

    @pytest.mark.parametrize("line", [b"timestamp=1000", b"observed_at=1000"])
    def test_from_wire_rejects_unknown_observation_keys(self, line: bytes) -> None:
        with pytest.raises(TelemetryValidationError):
            TelemetryRecord.from_wire_bytes(_wire_line(1, line))

    @pytest.mark.parametrize("line", [b"observed_at_micros=0100", b"observed_at_micros=-1"])
    def test_from_wire_rejects_malformed_observation_integers(self, line: bytes) -> None:
        with pytest.raises(TelemetryValidationError):
            TelemetryRecord.from_wire_bytes(_wire_line(1, line))

    @pytest.mark.parametrize("line", [b"run=x", b"identity=run-telemetry"])
    def test_from_wire_rejects_unknown_run_keys(self, line: bytes) -> None:
        with pytest.raises(TelemetryValidationError):
            TelemetryRecord.from_wire_bytes(_wire_line(2, line))

    def test_from_wire_rejects_empty_run_ids(self) -> None:
        with pytest.raises(TelemetryValidationError):
            TelemetryRecord.from_wire_bytes(_wire_line(2, b"run_id="))

    def test_from_wire_propagates_run_id_redaction(self) -> None:
        with pytest.raises(TelemetryRedactionError):
            TelemetryRecord.from_wire_bytes(_wire_line(2, b"run_id=/run/one"))

    @pytest.mark.parametrize(
        "line",
        [
            b"queue_depth|not_a_kind|3|channel=telemetry",
            b"queue_depth|QUEUE_DEPTH|3|channel=telemetry",
        ],
    )
    def test_from_wire_rejects_unknown_kinds(self, line: bytes) -> None:
        with pytest.raises(TelemetryValidationError):
            TelemetryRecord.from_wire_bytes(_wire_line(3, line))

    @pytest.mark.parametrize(
        "line",
        [
            b"queue_depth|queue_depth|three|channel=telemetry",
            b"queue_depth|queue_depth|-1|channel=telemetry",
            b"queue_depth|queue_depth|03|channel=telemetry",
            b"queue_depth|queue_depth||channel=telemetry",
        ],
    )
    def test_from_wire_rejects_malformed_series_integers(self, line: bytes) -> None:
        with pytest.raises(TelemetryValidationError):
            TelemetryRecord.from_wire_bytes(_wire_line(3, line))

    @pytest.mark.parametrize(
        "line",
        [
            b"queue_depth|queue_depth|3",
            b"queue_depth|queue_depth|3|channel=telemetry|extra",
            b"queue_depth queue_depth 3 channel=telemetry",
        ],
    )
    def test_from_wire_rejects_wrong_series_field_counts(self, line: bytes) -> None:
        with pytest.raises(TelemetryValidationError):
            TelemetryRecord.from_wire_bytes(_wire_line(3, line))

    @pytest.mark.parametrize(
        "line",
        [
            b"queue_depth|queue_depth|3|channel=te\\m",
            b"queue_depth|queue_depth|3|channel=telemetry\\",
        ],
    )
    def test_from_wire_rejects_invalid_escapes(self, line: bytes) -> None:
        with pytest.raises(TelemetryValidationError):
            TelemetryRecord.from_wire_bytes(_wire_line(3, line))

    def test_from_wire_rejects_unescaped_reserved_characters(self) -> None:
        with pytest.raises(TelemetryValidationError):
            TelemetryRecord.from_wire_bytes(_wire_line(3, b"queue_depth|queue_depth|3|note=a=b"))

    @pytest.mark.parametrize(
        "line",
        [
            b"queue_depth|queue_depth|3|note",
            b"queue_depth|queue_depth|3|=value",
            b"queue_depth|queue_depth|3|note;other=value",
        ],
    )
    def test_from_wire_rejects_malformed_label_keys(self, line: bytes) -> None:
        with pytest.raises(TelemetryValidationError):
            TelemetryRecord.from_wire_bytes(_wire_line(3, line))

    def test_from_wire_rejects_trailing_label_separators(self) -> None:
        with pytest.raises(TelemetryValidationError):
            TelemetryRecord.from_wire_bytes(
                _wire_line(3, b"queue_depth|queue_depth|3|channel=telemetry;")
            )

    def test_from_wire_propagates_label_redaction(self) -> None:
        with pytest.raises(TelemetryRedactionError):
            TelemetryRecord.from_wire_bytes(_wire_line(3, b"queue_depth|queue_depth|3|n=token:x"))

    def test_from_wire_accepts_series_without_labels(self) -> None:
        record = _record(_metric(name="blocked_writer", labels=(), value=0))
        decoded = TelemetryRecord.from_wire_bytes(record.wire_bytes())
        assert decoded == record
        assert decoded.metrics[0].labels == ()


class TestQueueDepthHelper:
    def test_queue_depth_record_shape_is_exact(self) -> None:
        record = queue_depth_record(RUN_ID, 5_000, "result", 7)
        assert record.schema_version == TELEMETRY_SCHEMA_VERSION
        assert record.observed_at_micros == 5_000
        assert record.metrics == (
            TelemetryMetric(
                name="result_queue_depth",
                kind=TelemetryMetricKind.QUEUE_DEPTH,
                value=7,
                labels=(("channel", "result"),),
            ),
        )

    @pytest.mark.parametrize("channel", ["Result", "1x", "", "a-b", "x" * 33])
    def test_queue_depth_record_rejects_invalid_channels(self, channel: str) -> None:
        with pytest.raises(TelemetryValidationError):
            queue_depth_record(RUN_ID, 5_000, channel, 0)

    @pytest.mark.parametrize("channel", [7, b"result", None, True])
    def test_queue_depth_record_rejects_non_text_channels(self, channel: object) -> None:
        with pytest.raises(TelemetryValidationError):
            queue_depth_record(RUN_ID, 5_000, cast(str, channel), 0)

    @pytest.mark.parametrize("depth", [-1, MAX_METRIC_VALUE + 1, True, "3"])
    def test_queue_depth_record_rejects_bad_depths(self, depth: object) -> None:
        with pytest.raises(TelemetryValidationError):
            queue_depth_record(RUN_ID, 5_000, "result", cast(int, depth))


class TestCapacityHelpers:
    def test_capacity_snapshot_records_shape_is_exact(self) -> None:
        snapshots = (
            _snapshot("global", limit=4, in_use=2),
            _snapshot("connector", limit=8, in_use=0),
        )
        records = capacity_snapshot_records(RUN_ID, 6_000, snapshots)
        assert len(records) == 2
        assert records[0].metrics == (
            TelemetryMetric(
                name="global_active_capacity",
                kind=TelemetryMetricKind.ACTIVE_CAPACITY,
                value=2,
                labels=(("category", "global"), ("limit", "4")),
            ),
        )
        assert records[1].metrics[0].value == 0
        assert records[1].metrics[0].labels == (("category", "connector"), ("limit", "8"))

    def test_capacity_snapshot_records_skip_nothing(self) -> None:
        records = capacity_snapshot_records(
            RUN_ID,
            6_000,
            (_snapshot("node", in_use=0), _snapshot("cpu_pool", limit=8, in_use=5)),
        )
        assert [record.metrics[0].value for record in records] == [0, 5]

    def test_capacity_snapshot_records_accept_an_empty_tuple(self) -> None:
        assert capacity_snapshot_records(RUN_ID, 6_000, ()) == ()

    def test_capacity_snapshot_records_reject_non_tuple_containers(self) -> None:
        with pytest.raises(TelemetryValidationError):
            capacity_snapshot_records(
                RUN_ID,
                6_000,
                cast("tuple[CapacitySnapshot, ...]", [_snapshot("global")]),
            )

    def test_capacity_snapshot_records_reject_non_snapshot_items(self) -> None:
        with pytest.raises(TelemetryValidationError):
            capacity_snapshot_records(
                RUN_ID,
                6_000,
                cast("tuple[CapacitySnapshot, ...]", (_snapshot("global"), "global")),
            )

    @pytest.mark.parametrize(
        "category",
        ["global", "strategy", "node", "connector", "cpu_pool"],
    )
    def test_capacity_wait_accepts_every_known_category(self, category: str) -> None:
        record = capacity_wait_record(RUN_ID, 7_000, category, 250)
        assert record.metrics[0].name == f"{category}_capacity_wait"
        assert record.metrics[0].kind is TelemetryMetricKind.CAPACITY_WAIT
        assert record.metrics[0].labels == (("category", category),)

    @pytest.mark.parametrize("category", ["pool", "GLOBAL", "", "node-1", "x" * 33])
    def test_capacity_wait_rejects_unknown_categories(self, category: str) -> None:
        with pytest.raises(TelemetryValidationError):
            capacity_wait_record(RUN_ID, 7_000, category, 0)

    @pytest.mark.parametrize("waited", [-1, MAX_METRIC_VALUE + 1])
    def test_capacity_wait_rejects_bad_waits(self, waited: int) -> None:
        with pytest.raises(TelemetryValidationError):
            capacity_wait_record(RUN_ID, 7_000, "global", waited)


class TestServiceStateAndCountHelpers:
    def test_service_duration_record_shape_is_exact(self) -> None:
        record = service_duration_record(RUN_ID, 8_000, "writer_flush", 1_250)
        assert record.metrics == (
            TelemetryMetric(
                name="writer_flush_service_duration",
                kind=TelemetryMetricKind.SERVICE_DURATION,
                value=1_250,
                labels=(("operation", "writer_flush"),),
            ),
        )

    @pytest.mark.parametrize("operation", ["Flush", "1x", "", "a-b"])
    def test_service_duration_rejects_invalid_operations(self, operation: str) -> None:
        with pytest.raises(TelemetryValidationError):
            service_duration_record(RUN_ID, 8_000, operation, 0)

    def test_service_duration_rejects_negative_durations(self) -> None:
        with pytest.raises(TelemetryValidationError):
            service_duration_record(RUN_ID, 8_000, "writer_flush", -1)

    def test_blocked_writer_true_maps_to_one(self) -> None:
        record = blocked_writer_record(RUN_ID, 9_000, True)
        assert record.metrics == (
            TelemetryMetric(
                name="blocked_writer",
                kind=TelemetryMetricKind.BLOCKED_WRITER,
                value=1,
                labels=(),
            ),
        )

    def test_blocked_writer_false_maps_to_zero(self) -> None:
        assert blocked_writer_record(RUN_ID, 9_000, False).metrics[0].value == 0

    @pytest.mark.parametrize("blocked", [1, 0, "yes", None, 1.0])
    def test_blocked_writer_rejects_non_booleans(self, blocked: object) -> None:
        with pytest.raises(TelemetryValidationError):
            blocked_writer_record(RUN_ID, 9_000, cast(bool, blocked))

    def test_dropped_telemetry_record_carries_the_count(self) -> None:
        record = dropped_telemetry_record(RUN_ID, 10_000, 5)
        assert record.metrics == (
            TelemetryMetric(
                name="dropped_telemetry",
                kind=TelemetryMetricKind.DROPPED_TELEMETRY,
                value=5,
                labels=(),
            ),
        )

    def test_dropped_telemetry_rejects_negative_counts(self) -> None:
        with pytest.raises(TelemetryValidationError):
            dropped_telemetry_record(RUN_ID, 10_000, -1)

    @pytest.mark.parametrize("state", ["pending", "completed", "failed"])
    def test_cleanup_state_accepts_the_closed_states(self, state: str) -> None:
        record = cleanup_state_record(RUN_ID, 11_000, state)
        assert record.metrics == (
            TelemetryMetric(
                name="cleanup_state",
                kind=TelemetryMetricKind.CLEANUP_STATE,
                value=1,
                labels=(("state", state),),
            ),
        )

    @pytest.mark.parametrize("state", ["running", "", "PENDING", "done"])
    def test_cleanup_state_rejects_unknown_states(self, state: str) -> None:
        with pytest.raises(TelemetryValidationError):
            cleanup_state_record(RUN_ID, 11_000, state)

    def test_cleanup_state_rejects_non_text_states(self) -> None:
        with pytest.raises(TelemetryValidationError):
            cleanup_state_record(RUN_ID, 11_000, cast(str, 7))

    def test_unresolved_resources_record_shape_is_exact(self) -> None:
        record = unresolved_resources_record(RUN_ID, 12_000, 3)
        assert record.metrics == (
            TelemetryMetric(
                name="unresolved_resources",
                kind=TelemetryMetricKind.UNRESOLVED_RESOURCES,
                value=3,
                labels=(),
            ),
        )

    def test_unresolved_resources_rejects_negative_counts(self) -> None:
        with pytest.raises(TelemetryValidationError):
            unresolved_resources_record(RUN_ID, 12_000, -2)

    def test_every_helper_validates_run_id_and_observation_time(self) -> None:
        def queue_depth(run_id: str, observed: int) -> TelemetryRecord:
            return queue_depth_record(run_id, observed, "result", 0)

        def capacity_wait(run_id: str, observed: int) -> TelemetryRecord:
            return capacity_wait_record(run_id, observed, "global", 0)

        def service_duration(run_id: str, observed: int) -> TelemetryRecord:
            return service_duration_record(run_id, observed, "writer_flush", 0)

        def blocked_writer(run_id: str, observed: int) -> TelemetryRecord:
            return blocked_writer_record(run_id, observed, True)

        def dropped(run_id: str, observed: int) -> TelemetryRecord:
            return dropped_telemetry_record(run_id, observed, 0)

        def cleanup(run_id: str, observed: int) -> TelemetryRecord:
            return cleanup_state_record(run_id, observed, "pending")

        def unresolved(run_id: str, observed: int) -> TelemetryRecord:
            return unresolved_resources_record(run_id, observed, 0)

        for build in (
            queue_depth,
            capacity_wait,
            service_duration,
            blocked_writer,
            dropped,
            cleanup,
            unresolved,
        ):
            with pytest.raises(TelemetryValidationError):
                build("", 1_000)
            with pytest.raises(TelemetryValidationError):
                build(RUN_ID, -1)
            with pytest.raises(TelemetryValidationError):
                build(cast(str, 7), 1_000)


class TestCollectorConstruction:
    def test_capacity_boundaries_are_accepted(self) -> None:
        assert TelemetryCollector(run_id=RUN_ID, capacity=1).capacity == 1
        collector = TelemetryCollector(run_id=RUN_ID, capacity=MAX_COLLECTOR_CAPACITY)
        assert collector.capacity == MAX_COLLECTOR_CAPACITY
        assert collector.run_id == RUN_ID

    @pytest.mark.parametrize("capacity", [0, -1, MAX_COLLECTOR_CAPACITY + 1])
    def test_capacities_outside_bounds_are_rejected(self, capacity: int) -> None:
        with pytest.raises(TelemetryValidationError):
            TelemetryCollector(run_id=RUN_ID, capacity=capacity)

    @pytest.mark.parametrize("capacity", [True, "8", 8.0, None])
    def test_non_integer_capacities_are_rejected(self, capacity: object) -> None:
        with pytest.raises(TelemetryValidationError):
            TelemetryCollector(run_id=RUN_ID, capacity=cast(int, capacity))

    @pytest.mark.parametrize("run_id", ["", "r" * 129, "bad\x1f", "/run", "token:x"])
    def test_invalid_run_ids_are_rejected(self, run_id: str) -> None:
        with pytest.raises(TelemetryError):
            TelemetryCollector(run_id=run_id, capacity=4)

    @pytest.mark.parametrize("record", ["record", None, _metric()])
    def test_emit_rejects_non_records(self, record: object) -> None:
        collector = TelemetryCollector(run_id=RUN_ID, capacity=2)
        with pytest.raises(TelemetryValidationError):
            collector.emit(cast(TelemetryRecord, record))
        assert collector.snapshot() == (0, 0)


class TestCollectorBehavior:
    def test_drain_returns_records_oldest_first(self) -> None:
        collector = TelemetryCollector(run_id=RUN_ID, capacity=8)
        for index in range(3):
            collector.emit(dropped_telemetry_record(RUN_ID, index, index))
        drained = collector.drain()
        assert [record.observed_at_micros for record in drained] == [0, 1, 2]

    def test_repeated_drain_empties_the_buffer(self) -> None:
        collector = TelemetryCollector(run_id=RUN_ID, capacity=8)
        collector.emit(dropped_telemetry_record(RUN_ID, 0, 0))
        assert collector.drain() != ()
        assert collector.drain() == ()
        assert collector.snapshot() == (0, 0)

    def test_emit_below_capacity_drops_nothing(self) -> None:
        collector = TelemetryCollector(run_id=RUN_ID, capacity=4)
        for index in range(4):
            collector.emit(dropped_telemetry_record(RUN_ID, index, index))
        assert collector.snapshot() == (4, 0)
        assert collector.dropped_count() == 0
        assert collector.dropped_record(999) is None

    def test_overflow_drops_the_oldest_and_counts_every_drop(self) -> None:
        capacity = 8
        collector = TelemetryCollector(run_id=RUN_ID, capacity=capacity)
        for index in range(capacity + 5):
            collector.emit(dropped_telemetry_record(RUN_ID, index, index))
        buffered, dropped = collector.snapshot()
        assert buffered == capacity
        assert dropped == 5
        assert collector.dropped_count() == 5
        drained = collector.drain()
        assert [record.observed_at_micros for record in drained] == list(range(5, 13))

    def test_drop_count_accumulates_across_drains(self) -> None:
        collector = TelemetryCollector(run_id=RUN_ID, capacity=2)
        for index in range(4):
            collector.emit(dropped_telemetry_record(RUN_ID, index, index))
        assert collector.dropped_count() == 2
        collector.drain()
        for index in range(4, 8):
            collector.emit(dropped_telemetry_record(RUN_ID, index, index))
        assert collector.dropped_count() == 4
        report = collector.dropped_record(100)
        assert report is not None
        assert report.metrics[0].value == 4
        assert report.observed_at_micros == 100
        assert report.metrics[0].kind is TelemetryMetricKind.DROPPED_TELEMETRY

    def test_snapshot_stays_bounded_under_repeated_overflow(self) -> None:
        capacity = 16
        collector = TelemetryCollector(run_id=RUN_ID, capacity=capacity)
        for index in range(capacity * 4):
            collector.emit(dropped_telemetry_record(RUN_ID, index, index))
            buffered, _ = collector.snapshot()
            assert buffered <= capacity

    def test_repr_is_bounded(self) -> None:
        collector = TelemetryCollector(run_id=RUN_ID, capacity=2)
        assert "run-telemetry" in repr(collector)
        assert "dropped=0" in repr(collector)

    def test_collector_exposes_no_authority_surface(self) -> None:
        collector = TelemetryCollector(run_id=RUN_ID, capacity=2)
        for authority in ("commit", "release", "acknowledge", "admit", "advance", "publish"):
            assert not hasattr(collector, authority)


class TestCollectorConcurrency:
    def test_concurrent_emit_conserves_every_record(self) -> None:
        capacity = 100
        collector = TelemetryCollector(run_id=RUN_ID, capacity=capacity)
        start = threading.Barrier(THREADS + 1)
        lock = threading.Lock()
        errors: list[BaseException] = []

        def produce(thread_index: int) -> None:
            try:
                start.wait(timeout=JOIN_TIMEOUT_SECONDS)
                for index in range(PER_THREAD):
                    collector.emit(
                        service_duration_record(
                            run_id=RUN_ID,
                            observed_at_micros=thread_index * PER_THREAD + index,
                            operation="writer_flush",
                            duration_micros=index,
                        )
                    )
            except BaseException as error:
                with lock:
                    errors.append(error)

        threads = [
            threading.Thread(target=produce, args=(thread_index,))
            for thread_index in range(THREADS)
        ]
        for thread in threads:
            thread.start()
        start.wait(timeout=JOIN_TIMEOUT_SECONDS)
        for thread in threads:
            thread.join(timeout=JOIN_TIMEOUT_SECONDS)
            assert not thread.is_alive()
        assert errors == []

        emitted = THREADS * PER_THREAD
        drained = collector.drain()
        buffered, dropped = collector.snapshot()
        assert buffered == 0
        assert dropped == emitted - capacity
        assert len(drained) + dropped == emitted
        assert len(drained) == capacity
        assert len(set(drained)) == len(drained)

    def test_concurrent_emit_and_drain_conserves_every_record(self) -> None:
        capacity = 32
        collector = TelemetryCollector(run_id=RUN_ID, capacity=capacity)
        start = threading.Barrier(THREADS + 2)
        stop = threading.Event()
        lock = threading.Lock()
        errors: list[BaseException] = []
        drained_total = 0

        def produce(thread_index: int) -> None:
            try:
                start.wait(timeout=JOIN_TIMEOUT_SECONDS)
                for index in range(PER_THREAD):
                    collector.emit(
                        dropped_telemetry_record(RUN_ID, thread_index * PER_THREAD + index, index)
                    )
            except BaseException as error:
                with lock:
                    errors.append(error)

        def consume() -> None:
            nonlocal drained_total
            try:
                start.wait(timeout=JOIN_TIMEOUT_SECONDS)
                while not stop.is_set():
                    drained_total += len(collector.drain())
            except BaseException as error:
                with lock:
                    errors.append(error)

        threads = [
            threading.Thread(target=produce, args=(thread_index,))
            for thread_index in range(THREADS)
        ]
        consumer = threading.Thread(target=consume)
        for thread in threads:
            thread.start()
        consumer.start()
        start.wait(timeout=JOIN_TIMEOUT_SECONDS)
        for thread in threads:
            thread.join(timeout=JOIN_TIMEOUT_SECONDS)
            assert not thread.is_alive()
        stop.set()
        consumer.join(timeout=JOIN_TIMEOUT_SECONDS)
        assert not consumer.is_alive()
        assert errors == []

        drained_total += len(collector.drain())
        emitted = THREADS * PER_THREAD
        assert drained_total + collector.dropped_count() == emitted


class TestTelemetryIsNonAuthoritative:
    def test_telemetry_through_the_bounded_channel_leaves_the_frontier_identical(
        self,
    ) -> None:
        scheduler = _two_node_scheduler()
        first = scheduler.next_ready(1)[0]
        scheduler.register_admission(first, 1)
        scheduler.commit_result(first, "succeeded")
        frontier_before = scheduler.frontier

        channel = BoundedChannel(kind=CHANNEL_KIND_TELEMETRY, capacity=4)
        dropped_sends = 0
        for index in range(12):
            record = queue_depth_record(RUN_ID, 10_000 + index, "telemetry", index)
            if not channel.try_send(record):
                dropped_sends += 1
        assert channel.queued == 4
        assert dropped_sends == 8
        assert channel.accepted_count == 4
        drained = channel.drain()
        assert all(type(message) is TelemetryRecord for message in drained)
        channel.close()

        collector = TelemetryCollector(run_id=RUN_ID, capacity=8)
        for record in capacity_snapshot_records(
            RUN_ID, 20_000, (_snapshot("global", limit=4, in_use=2),)
        ):
            collector.emit(record)
        for index in range(12):
            collector.emit(dropped_telemetry_record(RUN_ID, 30_000 + index, index))
        assert collector.dropped_count() == 5
        report = collector.dropped_record(40_000)
        assert report is not None
        assert report.metrics[0].value == 5
        collector.emit(report)
        assert collector.drain() != ()

        frontier_after = scheduler.frontier
        assert frontier_after == frontier_before
        assert frontier_after.to_mapping() == frontier_before.to_mapping()

    def test_telemetry_burst_leaves_an_in_flight_frontier_identical(self) -> None:
        scheduler = _two_node_scheduler()
        first, second = scheduler.next_ready(2)
        scheduler.register_admission(first, 1)
        scheduler.register_admission(second, 2)
        scheduler.mark_result_received(first)
        frontier_before = scheduler.frontier

        collector = TelemetryCollector(run_id=RUN_ID, capacity=4)
        for index in range(20):
            collector.emit(service_duration_record(RUN_ID, index, "writer_flush", index))
        assert collector.dropped_count() == 16
        for record in collector.drain():
            channel_record = queue_depth_record(RUN_ID, record.observed_at_micros, "writer", 1)
            assert TelemetryRecord.from_wire_bytes(channel_record.wire_bytes()) == channel_record

        frontier_after = scheduler.frontier
        assert frontier_after == frontier_before
        assert frontier_after.to_mapping() == frontier_before.to_mapping()

    def test_records_expose_no_authority_methods(self) -> None:
        record = _record()
        metric = record.metrics[0]
        for authority in ("commit", "release", "acknowledge", "advance", "publish", "resolve"):
            assert not hasattr(record, authority)
            assert not hasattr(metric, authority)

    def test_source_module_imports_no_authority_module(self) -> None:
        source = Path(telemetry_module.__file__).read_text(encoding="utf-8")
        for forbidden in (
            "from paritygrid.adapters",
            "sqlite",
            "SchedulerFrontier",
            "WorkLease",
            "checkpoint",
            "concurrent_scheduler",
            "result_sink",
            "repository",
            "scheduler",
            "leasing",
        ):
            assert forbidden not in source
        assert "from paritygrid.application.execution.capacity import" in source


class TestModuleContract:
    def test_module_exports_exactly_the_deliberate_public_names(self) -> None:
        assert set(telemetry_module.__all__) == {
            "MAX_COLLECTOR_CAPACITY",
            "MAX_LABEL_COUNT",
            "MAX_LABEL_LENGTH",
            "MAX_METRIC_NAME_LENGTH",
            "MAX_METRIC_VALUE",
            "MAX_SERIES_PER_RECORD",
            "MAX_TELEMETRY_RECORD_BYTES",
            "TELEMETRY_SCHEMA_VERSION",
            "TelemetryCollector",
            "TelemetryError",
            "TelemetryMetric",
            "TelemetryMetricKind",
            "TelemetryRecord",
            "TelemetryRedactionError",
            "TelemetrySchemaError",
            "TelemetryValidationError",
            "blocked_writer_record",
            "capacity_snapshot_records",
            "capacity_wait_record",
            "cleanup_state_record",
            "dropped_telemetry_record",
            "queue_depth_record",
            "service_duration_record",
            "unresolved_resources_record",
        }

    def test_execution_package_re_exports_the_telemetry_contract(self) -> None:
        for name in telemetry_module.__all__:
            assert name in execution_package.__all__, name
            module_value = getattr(telemetry_module, name)
            package_value = getattr(execution_package, name)
            if type(module_value) is int:
                assert package_value == module_value
            else:
                assert package_value is module_value

    def test_module_documentation_states_the_passive_contract(self) -> None:
        documentation = telemetry_module.__doc__ or ""
        for phrase in ("bounded", "drops the oldest", "never reads a clock", "no authority"):
            assert phrase in documentation

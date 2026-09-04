"""Performance harness contract, percentile, and gated-measurement tests (P21.4)."""

# pyright: reportPrivateUsage=false

import json
from pathlib import Path
from typing import cast

import pytest

from paritygrid.quality.performance_harness import (
    ACCEPTED_SHOWCASE_RUN_MANIFEST_SHA256,
    PERFORMANCE_REPORT_FORMAT,
    PERFORMANCE_REPORT_VERSION,
    PerformanceConfig,
    PerformanceCorrectnessError,
    PerformanceHarnessError,
    PerformanceReportError,
    _latency_percentiles,
    _verify_executed_manifest,
    build_performance_report,
    nearest_rank_percentile,
    parse_performance_report,
    performance_report_bytes,
)


def _as_object(value: object) -> dict[str, object]:
    """Narrow a parsed report member to a JSON object for assertions."""
    assert isinstance(value, dict)
    return cast("dict[str, object]", value)


def _as_list(value: object) -> list[object]:
    """Narrow a parsed report member to a JSON array for assertions."""
    assert isinstance(value, list)
    return cast("list[object]", value)


def _obj_at(document: dict[str, object], key: str) -> dict[str, object]:
    """Narrow one named member of a parsed object to a JSON object."""
    return _as_object(document[key])


def _list_at(document: dict[str, object], key: str) -> list[object]:
    """Narrow one named member of a parsed object to a JSON array."""
    return _as_list(document[key])


def _minimal_accepted_document() -> dict[str, object]:
    """A smallest valid accepted report used by parser mutation tests."""
    runner_observation = {
        "repetition": 0,
        "total_duration_seconds": 1.0,
        "admitted_count": 1,
        "committed_count": 1,
        "retry_count": 1,
        "queue_wait_seconds_total": 0.0,
        "queue_wait_seconds_mean": 0.0,
        "queue_waits_observed": 1,
        "service_time_seconds_total": 1.0,
        "service_time_seconds_mean": 1.0,
        "peak_in_flight_work": 1,
        "peak_concurrent_service": 1,
        "sqlite_commit_count": 1,
        "sqlite_commit_seconds_total": 1.0,
        "sqlite_commit_seconds_mean": 1.0,
        "duckdb_query_seconds": 1.0,
        "channel_high_water": {"assignment": 1, "result": 1, "telemetry": 1, "writer": 1},
    }
    runners: dict[str, object] = {
        strategy: {
            "warmup_durations_seconds": [],
            "observations": [dict(runner_observation)],
            "latency_p50_seconds": 1.0,
            "latency_p95_seconds": 1.0,
            "latency_p99_seconds": 1.0,
            "unavailable": [],
        }
        for strategy in ("sequential", "threaded", "asyncio")
    }
    return {
        "format": PERFORMANCE_REPORT_FORMAT,
        "version": PERFORMANCE_REPORT_VERSION,
        "package_version": "0.1.0",
        "environment": {
            "system": "windows",
            "machine": "x86_64",
            "processor": "cpu",
            "python_implementation": "CPython",
            "python_version": "3.14.4",
            "sqlite_version": "3.50.4",
            "cpu_count": 1,
            "total_memory_bytes": 1,
            "total_memory_source": "test",
            "unavailable": [],
        },
        "scenario": {
            "format_name": "scenario",
            "format_version": 1,
            "scenario_version": 1,
            "profile_id": "showcase",
            "profile_identity": "profile",
            "seed": 19,
            "record_count": 700,
            "expected_counts": {
                "total_input_rows": 700,
                "accepted_rows": 1,
                "quarantined_rows": 1,
                "planned_repairs": 1,
                "applied_repairs": 1,
            },
            "plan_fingerprint": "a" * 64,
            "derivation_manifest_sha256": "b" * 64,
            "executed_manifest_sha256": "c" * 64,
        },
        "correctness": {
            "accepted": True,
            "checks": [{"name": "gate", "passed": True, "detail": None}],
            "cross_runner_evidence_equal": True,
        },
        "method": {
            "clock": "time.perf_counter (monotonic)",
            "percentile_method": "nearest-rank",
            "story_warmup_runs": 0,
            "story_measured_runs": 1,
            "runner_warmup_runs": 0,
            "runner_measured_runs": 1,
            "runners": ["sequential", "threaded", "asyncio"],
            "metric_definitions": [{"name": "duration", "unit": "seconds", "definition": "x"}],
            "disclaimers": ["diagnostic only"],
        },
        "story": {
            "warmup_durations_seconds": [],
            "observations": [
                {
                    "repetition": 0,
                    "total_duration_seconds": 1.0,
                    "records_per_second": 700.0,
                    "retry_count": 1,
                    "peak_process_rss_bytes": 1,
                    "peak_python_heap_bytes": 1,
                    "manifest_sha256": "c" * 64,
                }
            ],
            "latency_p50_seconds": 1.0,
            "latency_p95_seconds": 1.0,
            "latency_p99_seconds": 1.0,
            "records_per_second_mean": 700.0,
            "total_duration_seconds_mean": 1.0,
            "unavailable": [],
        },
        "runners": runners,
        "cleanup": {
            "retired_within_deadline": True,
            "child_process_count": 0,
            "owned_thread_names": [],
            "zero_orphan_children": True,
            "harness_roots_removed": True,
        },
    }


class TestPercentileDefinition:
    def test_single_observation_is_every_percentile(self) -> None:
        assert nearest_rank_percentile([2.5], 0) == 2.5
        assert nearest_rank_percentile([2.5], 50) == 2.5
        assert nearest_rank_percentile([2.5], 100) == 2.5

    def test_nearest_rank_returns_observed_samples(self) -> None:
        values = [1.0, 2.0, 3.0, 4.0]
        assert nearest_rank_percentile(values, 50) == 2.0
        assert nearest_rank_percentile(values, 75) == 3.0
        assert nearest_rank_percentile(values, 100) == 4.0

    def test_zeroth_percentile_clamps_to_the_first_sample(self) -> None:
        assert nearest_rank_percentile([5.0, 7.0, 9.0], 0) == 5.0

    def test_boundary_ranks_hit_exact_samples(self) -> None:
        values = [10.0, 20.0]
        assert nearest_rank_percentile(values, 99) == 20.0
        assert nearest_rank_percentile(values, 50) == 10.0

    def test_report_percentiles_sort_chronological_measurements(self) -> None:
        assert _latency_percentiles([3.0, 1.0, 2.0]) == (2.0, 3.0, 3.0)

    def test_rejects_unsorted_non_finite_and_empty_inputs(self) -> None:
        with pytest.raises(PerformanceReportError):
            nearest_rank_percentile([], 50)
        with pytest.raises(PerformanceReportError):
            nearest_rank_percentile([2.0, 1.0], 50)
        with pytest.raises(PerformanceReportError):
            nearest_rank_percentile([float("nan")], 50)
        with pytest.raises(PerformanceReportError):
            nearest_rank_percentile([float("inf")], 50)
        with pytest.raises(PerformanceReportError):
            nearest_rank_percentile([-1.0], 50)
        with pytest.raises(PerformanceReportError):
            nearest_rank_percentile([1.0], 150)
        with pytest.raises(PerformanceReportError):
            nearest_rank_percentile([1.0], -5)


class TestPerformanceConfig:
    def test_defaults_are_within_the_bounded_plan(self) -> None:
        config = PerformanceConfig()
        assert config.story_warmup_runs == 1
        assert config.story_measured_runs == 3
        assert config.runner_warmup_runs == 1
        assert config.runner_measured_runs == 3

    @pytest.mark.parametrize("field", ["story_warmup_runs", "runner_warmup_runs"])
    def test_warmup_counts_reject_out_of_bound_values(self, field: str) -> None:
        with pytest.raises(PerformanceHarnessError):
            PerformanceConfig(**{field: -1})
        with pytest.raises(PerformanceHarnessError):
            PerformanceConfig(**{field: 6})
        with pytest.raises(PerformanceHarnessError):
            PerformanceConfig(**{field: True})

    @pytest.mark.parametrize("field", ["story_measured_runs", "runner_measured_runs"])
    def test_measured_counts_reject_out_of_bound_values(self, field: str) -> None:
        with pytest.raises(PerformanceHarnessError):
            PerformanceConfig(**{field: 0})
        with pytest.raises(PerformanceHarnessError):
            PerformanceConfig(**{field: 21})


class TestExecutedManifestGate:
    def test_accepted_manifest_passes_the_gate(self) -> None:
        import hashlib

        # Rebuild the exact golden bytes is impractical here; instead prove
        # the gate on a known-good hash by patching the accepted constant.
        payload = b"manifest"
        digest = hashlib.sha256(payload).hexdigest()
        original = ACCEPTED_SHOWCASE_RUN_MANIFEST_SHA256
        try:
            _patch_accepted(digest)
            check = _verify_executed_manifest(payload, "gate")
        finally:
            _patch_accepted(original)
        assert check["passed"] is True

    def test_wrong_manifest_aborts_before_measurement(self) -> None:
        original = ACCEPTED_SHOWCASE_RUN_MANIFEST_SHA256
        try:
            _patch_accepted("0" * 64)
            with pytest.raises(PerformanceCorrectnessError):
                _verify_executed_manifest(b"not-the-canonical-manifest", "gate")
        finally:
            _patch_accepted(original)


def _patch_accepted(value: str) -> None:
    import paritygrid.quality.performance_harness as harness_module

    harness_module.ACCEPTED_SHOWCASE_RUN_MANIFEST_SHA256 = value


class TestReportContract:
    def test_report_bytes_are_canonical_and_deterministic(self) -> None:
        document = {
            "format": PERFORMANCE_REPORT_FORMAT,
            "version": PERFORMANCE_REPORT_VERSION,
            "zeta": 1,
            "alpha": 2.5,
        }
        first = performance_report_bytes(document)
        second = performance_report_bytes(document)
        assert first == second
        assert b'"alpha":2.5' in first
        assert first.index(b'"alpha"') < first.index(b'"zeta"')

    def test_parser_round_trips_a_minimal_accepted_document(self) -> None:
        document = _minimal_accepted_document()
        parsed = parse_performance_report(performance_report_bytes(document))
        assert parsed == document

    def test_negative_integers_fail_closed(self) -> None:
        document = _minimal_accepted_document()
        document["cleanup"] = {"zero_orphan_children": True, "child_process_count": -1}
        with pytest.raises(PerformanceReportError):
            parse_performance_report(performance_report_bytes(document))

    def test_empty_sections_fail_closed(self) -> None:
        document = _minimal_accepted_document()
        document["environment"] = {}
        with pytest.raises(PerformanceReportError):
            parse_performance_report(performance_report_bytes(document))

    def test_missing_runner_strategy_fails_closed(self) -> None:
        document = _minimal_accepted_document()
        document["runners"] = {"sequential": {"observations": []}}
        with pytest.raises(PerformanceReportError):
            parse_performance_report(performance_report_bytes(document))

    def test_skeletal_nested_acceptance_evidence_fails_closed(self) -> None:
        document = _minimal_accepted_document()
        document["cleanup"] = {"zero_orphan_children": True}
        with pytest.raises(PerformanceReportError):
            parse_performance_report(performance_report_bytes(document))

        document = _minimal_accepted_document()
        runners = _obj_at(document, "runners")
        runners["sequential"] = {"observations": []}
        with pytest.raises(PerformanceReportError):
            parse_performance_report(performance_report_bytes(document))

    def test_unknown_version_fails_closed(self) -> None:
        document = _minimal_accepted_document()
        document["version"] = PERFORMANCE_REPORT_VERSION + 1
        with pytest.raises(PerformanceReportError):
            parse_performance_report(performance_report_bytes(document))

    def test_missing_section_fails_closed(self) -> None:
        document = _minimal_accepted_document()
        del document["cleanup"]
        with pytest.raises(PerformanceReportError):
            parse_performance_report(performance_report_bytes(document))

    def test_unaccepted_or_unequal_correctness_fails_closed(self) -> None:
        base = _minimal_accepted_document()
        rejected = dict(base, correctness={"accepted": False, "cross_runner_evidence_equal": True})
        with pytest.raises(PerformanceReportError):
            parse_performance_report(performance_report_bytes(rejected))
        unequal = dict(base, correctness={"accepted": True, "cross_runner_evidence_equal": False})
        with pytest.raises(PerformanceReportError):
            parse_performance_report(performance_report_bytes(unequal))

    def test_malformed_json_and_non_finite_numbers_fail_closed(self) -> None:
        with pytest.raises(PerformanceReportError):
            parse_performance_report(b"{not json")
        document = _minimal_accepted_document()
        document["story"] = {"latency": float("inf")}
        literal = json.dumps(document).encode("ascii")
        with pytest.raises(PerformanceReportError):
            parse_performance_report(literal)


class TestGatedMeasurement:
    def test_complete_gated_report_is_accepted_and_clean(self, tmp_path: Path) -> None:
        config = PerformanceConfig(
            story_warmup_runs=0,
            story_measured_runs=1,
            runner_warmup_runs=0,
            runner_measured_runs=1,
        )
        document = build_performance_report(tmp_path, config)
        payload = performance_report_bytes(document)
        parsed = parse_performance_report(payload)

        correctness = _obj_at(parsed, "correctness")
        assert correctness["accepted"] is True
        assert correctness["cross_runner_evidence_equal"] is True
        checks = _list_at(correctness, "checks")
        check_names: list[str] = []
        for check in checks:
            check_name = _as_object(check)["name"]
            assert isinstance(check_name, str)
            check_names.append(check_name)
        assert "derivation_manifest_golden" in check_names
        assert any(name.startswith("executed_manifest_golden") for name in check_names)

        scenario = _obj_at(parsed, "scenario")
        assert scenario["profile_id"] == "showcase"
        assert scenario["seed"] == 19
        assert scenario["record_count"] == 700

        story = _obj_at(parsed, "story")
        observations = _list_at(story, "observations")
        assert len(observations) == 1
        first = _as_object(observations[0])
        duration = first["total_duration_seconds"]
        assert isinstance(duration, float)
        assert duration > 0.0
        records_per_second = first["records_per_second"]
        assert isinstance(records_per_second, float)
        assert records_per_second > 0.0
        assert first["retry_count"] == 1
        assert first["manifest_sha256"] == ACCEPTED_SHOWCASE_RUN_MANIFEST_SHA256
        # The heap peak is a real measurement or explicitly unavailable,
        # never a fabricated zero.
        heap_peak = first["peak_python_heap_bytes"]
        assert heap_peak is None or (isinstance(heap_peak, int) and heap_peak > 0)
        rss_peak = first["peak_process_rss_bytes"]
        assert rss_peak is None or (isinstance(rss_peak, int) and rss_peak > 0)

        runners = _obj_at(parsed, "runners")
        assert frozenset(runners) == {"sequential", "threaded", "asyncio"}
        for runner_id, runner_section_value in runners.items():
            section = _as_object(runner_section_value)
            runner_observations = _as_list(section["observations"])
            assert len(runner_observations) == 1
            runner_observation = _as_object(runner_observations[0])
            assert runner_observation["retry_count"] == 1
            for counter in (
                "peak_in_flight_work",
                "peak_concurrent_service",
                "sqlite_commit_count",
            ):
                observed = runner_observation[counter]
                assert isinstance(observed, int)
                assert observed >= 1, (runner_id, counter)
            duckdb_seconds = runner_observation["duckdb_query_seconds"]
            assert isinstance(duckdb_seconds, float)
            assert duckdb_seconds >= 0.0
            high_water = _as_object(runner_observation["channel_high_water"])
            assert len(high_water) == 4
            for channel_kind, mark in high_water.items():
                assert isinstance(mark, int)
                assert mark >= 0, (runner_id, channel_kind)
            unavailable = _list_at(section, "unavailable")
            assert unavailable

        cleanup = _obj_at(parsed, "cleanup")
        assert cleanup["zero_orphan_children"] is True
        assert cleanup["retired_within_deadline"] is True
        assert cleanup["harness_roots_removed"] is True

        method = _obj_at(parsed, "method")
        assert method["clock"] == "time.perf_counter (monotonic)"
        definitions = _list_at(method, "metric_definitions")
        assert len(definitions) >= 10

        text = payload.decode("ascii")
        assert str(tmp_path) not in text
        assert "\\\\" not in text

    def test_derivation_drift_aborts_without_measurements(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        import paritygrid.quality.performance_harness as harness_module

        monkeypatch.setattr(
            harness_module, "ACCEPTED_SHOWCASE_DERIVATION_MANIFEST_SHA256", "0" * 64
        )
        with pytest.raises(PerformanceCorrectnessError):
            build_performance_report(tmp_path, PerformanceConfig(story_measured_runs=1))
        # No measurement roots were created: the abort happened at the gate.
        assert not (tmp_path / "story").exists()

    def test_missing_root_is_refused(self, tmp_path: Path) -> None:
        with pytest.raises(PerformanceHarnessError):
            build_performance_report(tmp_path / "absent", PerformanceConfig(story_measured_runs=1))

    def test_existing_content_is_refused_without_mutation(self, tmp_path: Path) -> None:
        sentinel = tmp_path / "story" / "sentinel.txt"
        sentinel.parent.mkdir()
        sentinel.write_text("must survive", encoding="utf-8")

        with pytest.raises(PerformanceHarnessError, match="must be empty"):
            build_performance_report(tmp_path, PerformanceConfig(story_measured_runs=1))

        assert sentinel.read_text(encoding="utf-8") == "must survive"

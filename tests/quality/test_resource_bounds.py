"""Resource bounds exercise tests: saturation, cancellation, growth, cleanup (P21.5)."""

# pyright: reportPrivateUsage=false

import threading
from pathlib import Path
from typing import cast

import pytest

from paritygrid.application.execution.channels import BoundedChannel, ChannelTimeoutError
from paritygrid.application.execution.concurrency_settings import CapturedConcurrencySettings
from paritygrid.quality.resource_bounds import (
    MAX_REPETITIONS,
    MIN_REPETITIONS,
    ResourceBoundsError,
    _evaluate_rss_growth,
    _partial_startup_cell,
    _saturation_cell,
    parse_resource_bounds_report,
    run_resource_bounds_exercise,
)
from paritygrid.quality.resource_profile import (
    assert_within_capacity,
    bounded_growth_within,
)


class TestConfigurationBounds:
    def test_repetition_bounds_are_enforced(self, tmp_path: Path) -> None:
        assert MIN_REPETITIONS == 1
        assert MAX_REPETITIONS <= 10
        with pytest.raises(ResourceBoundsError):
            run_resource_bounds_exercise(tmp_path, repetitions=0)
        with pytest.raises(ResourceBoundsError):
            run_resource_bounds_exercise(tmp_path, repetitions=MAX_REPETITIONS + 1)
        with pytest.raises(ResourceBoundsError):
            run_resource_bounds_exercise(tmp_path, repetitions=True)  # type: ignore[arg-type]

    def test_missing_root_is_refused(self, tmp_path: Path) -> None:
        with pytest.raises(ResourceBoundsError):
            run_resource_bounds_exercise(tmp_path / "absent", repetitions=1)


class TestSaturationCell:
    def test_channel_saturates_exactly_at_capacity_and_blocks(self) -> None:
        settings = CapturedConcurrencySettings()
        cell = _saturation_cell(settings)

        assert cell["capacity"] == settings.result_channel_capacity
        assert cell["accepted_before_backpressure"] == settings.result_channel_capacity
        assert cell["max_observed_queued"] == settings.result_channel_capacity
        assert cell["backpressure_blocked_full_send"] is True
        assert cell["drained_after_close"] == settings.result_channel_capacity

    def test_bounded_channel_never_accepts_beyond_capacity(self) -> None:
        channel = BoundedChannel(kind="result", capacity=2)
        assert channel.try_send("a") is True
        assert channel.try_send("b") is True
        assert channel.try_send("c") is False
        with pytest.raises(ChannelTimeoutError):
            channel.send("c", timeout=0.02)
        assert channel.max_observed_queued == 2


class TestPartialStartupCell:
    def test_rollback_shuts_down_started_strategies_in_reverse(self) -> None:
        cell = _partial_startup_cell()
        assert cell["rollback_error"] == "RuntimeError"
        assert cell["rollback_shutdowns"] == ["shutdown:sequential"]
        assert cell["callback_order"] == [
            "start:sequential",
            "start:threaded",
            "shutdown:sequential",
        ]


class TestBoundedGrowthMethod:
    def test_growth_bound_holds_within_ratio_and_slack(self) -> None:
        assert bounded_growth_within(100, 120, max_growth_ratio=1.5, max_growth_bytes=16)
        assert not bounded_growth_within(100, 200, max_growth_ratio=1.5, max_growth_bytes=16)

    def test_missing_run_observation_never_claims_the_configured_bound(self) -> None:
        first, last, holds = _evaluate_rss_growth([100, None, 110], repetitions=3)
        assert (first, last, holds) == (100, 110, None)

        first, last, holds = _evaluate_rss_growth([100, 110], repetitions=3)
        assert (first, last, holds) == (100, 110, None)
        assert bounded_growth_within(100, 200, max_growth_ratio=1.5, max_growth_bytes=64)
        with pytest.raises(ValueError, match="requires observed peaks"):
            bounded_growth_within(None, 100, max_growth_ratio=1.5, max_growth_bytes=16)
        with pytest.raises(ValueError, match="at least 1"):
            bounded_growth_within(100, 100, max_growth_ratio=0.5, max_growth_bytes=16)


class TestCapacityAssertion:
    def test_capacity_at_boundary_passes_and_exceeding_fails(self) -> None:
        assert_within_capacity(8, 8, "channel")
        with pytest.raises(ValueError, match="exceeds capacity"):
            assert_within_capacity(9, 8, "channel")


class TestCancellationCoordination:
    def test_cancellation_event_coordinates_without_sleeps(self) -> None:
        started = threading.Event()
        released = threading.Event()

        def blocker() -> None:
            started.set()
            assert released.wait(5.0)

        worker = threading.Thread(target=blocker, daemon=True)
        worker.start()
        assert started.wait(5.0)
        released.set()
        worker.join(5.0)
        assert not worker.is_alive()


def _cell(cells: object, key: str) -> dict[str, object]:
    """Narrow one exercise cell to a JSON object for assertions."""
    assert isinstance(cells, dict)
    cell = cast("dict[str, object]", cells)[key]
    assert isinstance(cell, dict)
    return cast("dict[str, object]", cell)


class TestFullExercise:
    def test_every_cell_holds_with_zero_orphans(self, tmp_path: Path) -> None:
        document = run_resource_bounds_exercise(tmp_path, repetitions=2)

        assert document["format"] == "paritygrid-resource-bounds-report"
        assert document["version"] == 1
        cells = cast("dict[str, object]", document["cells"])

        steady = _cell(cells, "steady_state")
        assert steady["terminal_status"] == "completed"
        assert steady["retry_count"] == 1
        high_water = steady["channel_high_water"]
        assert isinstance(high_water, dict)
        for mark in cast("dict[str, object]", high_water).values():
            assert isinstance(mark, int)
            assert mark >= 0

        repeated = _cell(cells, "repeated_executions")
        assert repeated["runs"] == 2
        assert repeated["bounded_growth_holds"] is True

        saturation = _cell(cells, "queue_saturation_backpressure")
        assert saturation["backpressure_blocked_full_send"] is True

        cancellation = _cell(cells, "cancellation_cleanup")
        assert cancellation["terminal_status"] == "cancelled"
        assert cancellation["remaining_owned_threads"] == []

        assert _cell(cells, "partial_startup_rollback")["rollback_shutdowns"] == [
            "shutdown:sequential"
        ]
        shutdown = _cell(cells, "repeated_idempotent_shutdown")
        assert shutdown["writer_first_close_drained"] is True
        assert shutdown["writer_second_close_drained"] is True

        interruption = _cell(cells, "interruption_and_restart")
        assert interruption["failpoint"] == "repair.approved"
        assert isinstance(interruption["checks_passed"], int)
        assert interruption["checks_passed"] >= 1

        cleanup = document["cleanup"]
        assert isinstance(cleanup, dict)
        assert cleanup["zero_orphan_children"] is True
        assert cleanup["zero_owned_threads"] is True

    def test_existing_content_is_refused_without_mutation(self, tmp_path: Path) -> None:
        sentinel = tmp_path / "engine" / "sentinel.txt"
        sentinel.parent.mkdir()
        sentinel.write_text("must survive", encoding="utf-8")

        with pytest.raises(ResourceBoundsError, match="must be empty"):
            run_resource_bounds_exercise(tmp_path, repetitions=1)

        assert sentinel.read_text(encoding="utf-8") == "must survive"


class TestReportContract:
    def test_report_round_trips_through_the_strict_parser(self, tmp_path: Path) -> None:
        import json

        document = run_resource_bounds_exercise(tmp_path, repetitions=1)
        payload = json.dumps(
            document, ensure_ascii=True, separators=(",", ":"), sort_keys=True
        ).encode("ascii")
        assert parse_resource_bounds_report(payload) == document

    def test_parser_rejects_unknown_version_and_missing_cells(self, tmp_path: Path) -> None:
        import json

        from paritygrid.quality.resource_bounds import (
            ResourceBoundsReportError,
            run_resource_bounds_exercise,
        )

        (tmp_path / "again").mkdir()
        document = run_resource_bounds_exercise(tmp_path / "again", repetitions=1)
        mutated = dict(document)
        mutated["version"] = 99
        with pytest.raises(ResourceBoundsReportError):
            parse_resource_bounds_report(json.dumps(mutated).encode("ascii"))
        missing = dict(document)
        cells_document = _cell(document, "cells") if isinstance(document["cells"], dict) else {}
        cells = dict(cells_document)
        del cells["steady_state"]
        missing["cells"] = cells
        with pytest.raises(ResourceBoundsReportError):
            parse_resource_bounds_report(json.dumps(missing).encode("ascii"))

    def test_parser_rejects_negative_numbers(self, tmp_path: Path) -> None:
        import json

        from paritygrid.quality.resource_bounds import (
            ResourceBoundsReportError,
            run_resource_bounds_exercise,
        )

        (tmp_path / "negative").mkdir()
        document = run_resource_bounds_exercise(tmp_path / "negative", repetitions=1)
        mutated = dict(document)
        mutated["version"] = document["version"]
        cleanup_document = (
            _cell(document, "cleanup") if isinstance(document["cleanup"], dict) else {}
        )
        cleanup = dict(cleanup_document)
        cleanup["child_process_count"] = -1
        mutated["cleanup"] = cleanup
        with pytest.raises(ResourceBoundsReportError):
            parse_resource_bounds_report(json.dumps(mutated).encode("ascii"))

    def test_parser_rejects_forged_nested_cell(self, tmp_path: Path) -> None:
        import json

        from paritygrid.quality.resource_bounds import ResourceBoundsReportError

        document = run_resource_bounds_exercise(tmp_path, repetitions=1)
        cells = dict(cast("dict[str, object]", document["cells"]))
        cells["steady_state"] = {"terminal_status": "completed"}
        document["cells"] = cells
        with pytest.raises(ResourceBoundsReportError):
            parse_resource_bounds_report(json.dumps(document).encode("ascii"))

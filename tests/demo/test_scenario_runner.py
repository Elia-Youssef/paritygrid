"""Complete canonical scenario story tests (Phase 19).

The fast profile runs the entire product story in temporary isolated roots:
deterministic datasets, three loopback simulators, the four source connector
kinds with exactly one durable retry, a transient post-commit connection loss
resolved by the accepted ambiguous replay, the complete reconciliation and
repair workflow, and independent verified parity.  The showcase profile proves
the same story at scale with its own golden manifest lock.
"""

# pyright: reportPrivateUsage=false

import asyncio
from hashlib import sha256
from pathlib import Path
from threading import Barrier

import pytest
from sqlalchemy import select

from paritygrid.adapters.persistence.schema import repair_actions, work_attempts
from paritygrid.adapters.persistence.sqlite import (
    SQLiteDatabase,
    SQLiteDatabaseConfig,
)
from paritygrid.demo.scenario_runner import (
    MANIFEST_FILENAME,
    CanonicalScenarioResult,
    run_canonical_scenario,
)
from paritygrid.demo.scenarios import (
    CANONICAL_SCENARIO_SEED,
    CANONICAL_SCENARIO_VERSION,
    FAST_PROFILE,
    SCENARIO_FORMAT_VERSION,
    SHOWCASE_PROFILE,
    build_manifest,
    derive_scenario,
)
from paritygrid.domain.models import RunId

GOLDEN_FAST_RUN_MANIFEST_SHA256 = "02d791ad3975e2ee7156dca17a39db8c08ee64ccbade205d79a6db085b4fb6cd"
GOLDEN_SHOWCASE_RUN_MANIFEST_SHA256 = (
    "a60df038b0d933732877175fca637a7dbe31634c754d4db75d0fa70c20480f4e"
)


def _run_fast(root: Path) -> CanonicalScenarioResult:
    return asyncio.run(run_canonical_scenario(FAST_PROFILE, root))


def _sha256(payload: bytes) -> str:
    return sha256(payload).hexdigest()


def test_independent_source_reads_start_concurrently(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from paritygrid.demo import scenario_runner

    evidence = derive_scenario(FAST_PROFILE)
    root = scenario_runner.open_scenario_root(tmp_path / "scenario")
    rendezvous = Barrier(4, timeout=5.0)

    async def async_read(*_arguments: object) -> scenario_runner.SourceRead:
        await asyncio.to_thread(rendezvous.wait)
        return scenario_runner._successful_read("async_http", evidence, [], 0, 0)

    def blocking_read(*_arguments: object) -> tuple[list[object], int, int]:
        rendezvous.wait()
        return [], 0, 0

    monkeypatch.setattr(scenario_runner, "_read_async_source", async_read)
    monkeypatch.setattr(scenario_runner, "_read_blocking_source", blocking_read)
    monkeypatch.setattr(scenario_runner, "_read_bounded", blocking_read)

    class Endpoint:
        base_url = "http://127.0.0.1:1"

    reads = asyncio.run(
        scenario_runner._read_all_sources(
            evidence,
            root,
            scenario_runner.ScenarioClock.create(),
            RunId("run_concurrency-test"),
            Endpoint(),  # type: ignore[arg-type]
            Endpoint(),  # type: ignore[arg-type]
        )
    )
    assert tuple(reads) == ("async_http", "blocking_http", "csv", "jsonl")


class TestFastCanonicalRun:
    def test_complete_story_reproduces_the_golden_manifest(self, tmp_path: Path) -> None:
        result = _run_fast(tmp_path / "a" / "fast")
        published = (tmp_path / "a" / "fast" / MANIFEST_FILENAME).read_bytes()
        assert result.manifest_bytes == published
        assert _sha256(result.manifest_bytes) == GOLDEN_FAST_RUN_MANIFEST_SHA256

    def test_different_roots_produce_identical_canonical_bytes(self, tmp_path: Path) -> None:
        first = _run_fast(tmp_path / "one" / "fast")
        second = _run_fast(tmp_path / "two" / "completely-different-root")
        assert first.manifest_bytes == second.manifest_bytes

    def test_manifest_matches_the_pure_derivation(self, tmp_path: Path) -> None:
        result = _run_fast(tmp_path / "s" / "fast")
        expected = build_manifest(
            derive_scenario(FAST_PROFILE),
            execution_evidence_fingerprint=result.execution_evidence_fingerprint,
            verification_result="parity_holding",
        )
        assert result.manifest.canonical_bytes() == expected.canonical_bytes()

    def test_every_locked_fact_is_carried_by_the_manifest(self, tmp_path: Path) -> None:
        result = _run_fast(tmp_path / "s" / "fast")
        manifest = result.manifest
        counts = manifest.counts.as_mapping()
        assert manifest.seed == CANONICAL_SCENARIO_SEED == 19
        assert manifest.scenario_version == CANONICAL_SCENARIO_VERSION == 1
        assert SCENARIO_FORMAT_VERSION == 1
        assert counts["total_input_rows"] == 48
        assert counts["accepted_rows"] == 44
        assert counts["rejected_rows"] == 4
        assert counts["quarantined_rows"] == 4
        assert counts["boundary_rows"] == 3
        assert counts["duplicate_rows"] == 5
        assert counts["duplicate_groups"] == 8
        assert counts["canonical_keys"] == 43
        assert counts["match"] == 23
        assert counts["missing_from_target"] == 6
        assert counts["missing_from_source"] == 4
        assert counts["field_mismatch"] == 3
        assert counts["duplicate_source"] == 4
        assert counts["duplicate_target"] == 2
        assert counts["duplicate_both"] == 1
        assert counts["planned_repairs"] == 9
        assert counts["applied_repairs"] == 9
        assert counts["review_only_repairs"] == 11
        assert counts["rate_limit_retries"] == 1
        assert counts["transient_connection_failures"] == 1
        assert counts["ambiguous_replays_resolved"] == 1
        assert counts["artifacts"] == 3
        assert result.manifest.artifact_identities == (
            "fixture:canonical-source.csv",
            "fixture:canonical-source.jsonl",
            "art_canonical-conflicts",
        )
        assert manifest.verification_result == "parity_holding"
        assert manifest.execution_evidence_fingerprint is not None
        assert manifest.expected_target_fingerprint == result.observed_target_fingerprint

    def test_the_single_retry_is_visible_in_durable_attempt_history(self, tmp_path: Path) -> None:
        root = tmp_path / "s" / "fast"
        _run_fast(root)
        database = SQLiteDatabase.open(SQLiteDatabaseConfig(root / "canonical.db"))
        try:
            with database.transaction() as session:
                rows = session.execute(
                    select(
                        work_attempts.c.attempt_number,
                        work_attempts.c.outcome,
                    )
                    .where(work_attempts.c.work_item_id == "wrk_can-async-src")
                    .order_by(work_attempts.c.attempt_number)
                ).all()
            assert [(int(row[0]), str(row[1])) for row in rows] == [
                (1, "retry_scheduled"),
                (2, "succeeded"),
            ]
        finally:
            database.close()

    def test_only_non_destructive_action_kinds_are_durable(self, tmp_path: Path) -> None:
        root = tmp_path / "s" / "fast"
        _run_fast(root)
        database = SQLiteDatabase.open(SQLiteDatabaseConfig(root / "canonical.db"))
        try:
            with database.transaction() as session:
                kinds = set(session.execute(select(repair_actions.c.action_kind)).scalars().all())
            assert kinds == {"create_target", "update_target"}
        finally:
            database.close()

    def test_repair_replay_is_an_idempotent_no_op(self, tmp_path: Path) -> None:
        result = _run_fast(tmp_path / "s" / "fast")
        assert result.repair_replay_disposition == "already_applied"
        assert result.total_target_requests == 50


class TestShowcaseCanonicalRun:
    def test_showcase_scale_story_reproduces_its_golden_manifest(self, tmp_path: Path) -> None:
        result = asyncio.run(run_canonical_scenario(SHOWCASE_PROFILE, tmp_path / "s" / "showcase"))
        assert _sha256(result.manifest_bytes) == GOLDEN_SHOWCASE_RUN_MANIFEST_SHA256
        counts = result.manifest.counts.as_mapping()
        assert counts["total_input_rows"] == 700
        assert counts["accepted_rows"] == 690
        assert counts["rejected_rows"] == 10
        assert counts["quarantined_rows"] == 10
        assert counts["planned_repairs"] == 126
        assert counts["applied_repairs"] == 126
        assert counts["rate_limit_retries"] == 1
        assert counts["transient_connection_failures"] == 1
        assert result.manifest.verification_result == "parity_holding"

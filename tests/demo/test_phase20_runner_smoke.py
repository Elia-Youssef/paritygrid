"""Phase 20 demo runner smoke and cross-runner verification tests.

The module fixture drives three complete headless demo CLI invocations —
sequential, threaded, and asyncio — over one shared demo root through the
public Python API (``run_demo_command``), exactly like the ``paritygrid demo``
command does.  Each runner owns one stable engine run identity, so the three
durable engine runs can be compared for execution-evidence equality and the
cross-runner manifest can be collected from the same root.
"""

# pyright: reportPrivateUsage=false

import json
import threading
import time
from collections.abc import Iterator
from pathlib import Path
from typing import cast

import pytest
from sqlalchemy import select

from paritygrid.adapters.persistence import (
    SQLiteDatabase,
    SQLiteDatabaseConfig,
    SQLiteTransactionalWriter,
    create_session_factory,
)
from paritygrid.adapters.persistence.schema import runs as runs_table
from paritygrid.application.ports.writer import WriterSettings
from paritygrid.demo.demo_app import run_demo_command
from paritygrid.demo.engine_runner import (
    ENGINE_RUN_OFFSETS,
    ENGINE_RUN_PREFIX,
    ENGINE_STRATEGIES,
    DemoEngineError,
    collect_cross_runner_manifest,
    run_demo_engine_strategy,
)
from paritygrid.demo.orchestration import DemoOptions
from paritygrid.demo.scenario_runner import DATABASE_FILENAME
from paritygrid.demo.scenarios import CANONICAL_PIPELINE_VERSION
from paritygrid.demo.verification import (
    NON_EQUIVALENCE_DISCLAIMERS,
    ConcurrentScenarioHarness,
    CrossRunnerVerificationManifest,
)
from paritygrid.domain.models import PipelineVersion, RunId

_THREAD_RETIREMENT_TIMEOUT_SECONDS = 10.0
_THREAD_POLL_SECONDS = 0.1
_MANIFEST_EVIDENCE_VERSION = 2
_CLOSED_FORBIDDEN_CLAIMS = (
    "reconciliation_equal",
    "repair_plan_equal",
    "repair_effect_equal",
    "target_state_equal",
)


def _alive_launcher_thread_names() -> list[str]:
    return [
        thread.name
        for thread in threading.enumerate()
        if thread.name.startswith("paritygrid-run-") and thread.is_alive()
    ]


def _await_launcher_thread_retirement() -> None:
    """Require every launcher engine-owner thread to retire after one run."""
    deadline = time.monotonic() + _THREAD_RETIREMENT_TIMEOUT_SECONDS
    while time.monotonic() < deadline:
        if not _alive_launcher_thread_names():
            return
        time.sleep(_THREAD_POLL_SECONDS)
    leaked = _alive_launcher_thread_names()
    assert not leaked, f"launcher engine-owner threads leaked past terminal: {leaked}"


def _open_demo_database(root: Path) -> SQLiteDatabase:
    scenario_path = root / "scenario"
    return SQLiteDatabase.open(SQLiteDatabaseConfig((scenario_path / DATABASE_FILENAME).resolve()))


@pytest.fixture(scope="module")
def demo_root(tmp_path_factory: pytest.TempPathFactory) -> Iterator[Path]:
    """Run all three demo runner profiles over one shared root, leak-free."""
    root = tmp_path_factory.mktemp("phase20-demo-root") / "demo-root"
    try:
        for runner in ENGINE_STRATEGIES:
            exit_code = run_demo_command(DemoOptions(root=root, runner=runner, headless=True))
            assert exit_code == 0, f"the {runner} demo run exited with code {exit_code}"
            _await_launcher_thread_retirement()
        yield root
    finally:
        leaked = _alive_launcher_thread_names()
        assert not leaked, f"launcher engine-owner threads leaked after all runs: {leaked}"


def test_engine_run_offsets_cover_exactly_the_strategy_set() -> None:
    """The runner-to-run-identity lock is total: every strategy, offsets 1..3."""
    assert set(ENGINE_RUN_OFFSETS) == set(ENGINE_STRATEGIES)
    assert sorted(ENGINE_RUN_OFFSETS.values()) == [1, 2, 3]
    assert set(ENGINE_STRATEGIES) == {"sequential", "threaded", "asyncio"}


def test_each_runner_wrote_its_own_terminal_engine_run(demo_root: Path) -> None:
    """Every runner profile owns exactly its offset identity, terminal and equal."""
    database = _open_demo_database(demo_root)
    try:
        with database.transaction() as session:
            rows = session.execute(
                select(
                    runs_table.c.run_id,
                    runs_table.c.state,
                    runs_table.c.execution_evidence_fingerprint,
                )
                .where(runs_table.c.run_id.like(f"{ENGINE_RUN_PREFIX}-%"))
                .order_by(runs_table.c.run_id)
            ).all()
    finally:
        database.close()
    by_run_id = {str(row.run_id): row for row in rows}
    fingerprints: list[str] = []
    for offset in (1, 2, 3):
        run_id = f"{ENGINE_RUN_PREFIX}-{offset:04d}"
        row = by_run_id.get(run_id)
        assert row is not None, f"the {run_id} engine run is absent from the demo root"
        assert str(row.state) == "succeeded", f"{run_id} is not durably terminal"
        assert row.execution_evidence_fingerprint is not None, f"{run_id} lacks its fingerprint"
        fingerprints.append(str(row.execution_evidence_fingerprint))
    assert len(by_run_id) == 3, f"unexpected extra engine runs: {sorted(by_run_id)}"
    assert len(set(fingerprints)) == 1, (
        "the three runners produced unequal execution-evidence fingerprints"
    )


def test_cross_runner_manifest_proves_execution_evidence_equality(demo_root: Path) -> None:
    """The collected manifest is equal, execution-evidence-only, and untimed."""
    database = _open_demo_database(demo_root)
    writer = SQLiteTransactionalWriter(
        create_session_factory(database.engine),
        WriterSettings(contention_delay_seconds=0.0),
    )
    writer.start()
    try:
        manifest = collect_cross_runner_manifest(
            database,
            writer,
            demo_root / "scenario" / "artifacts",
            PipelineVersion(CANONICAL_PIPELINE_VERSION),
        )
    finally:
        writer.close(timeout_seconds=5.0)
        database.close()
    assert isinstance(manifest, CrossRunnerVerificationManifest)
    assert manifest.equal is True
    assert manifest.differences == ()
    assert manifest.evidence_kind == "execution-evidence"
    assert manifest.evidence_version == _MANIFEST_EVIDENCE_VERSION
    assert manifest.timings_recorded is False


def test_manifest_never_claims_more_than_execution_evidence(demo_root: Path) -> None:
    """The manifest bytes carry every disclaimer and no equivalence beyond it."""
    database = _open_demo_database(demo_root)
    writer = SQLiteTransactionalWriter(
        create_session_factory(database.engine),
        WriterSettings(contention_delay_seconds=0.0),
    )
    writer.start()
    try:
        manifest = collect_cross_runner_manifest(
            database,
            writer,
            demo_root / "scenario" / "artifacts",
            PipelineVersion(CANONICAL_PIPELINE_VERSION),
        )
    finally:
        writer.close(timeout_seconds=5.0)
        database.close()
    payload = json.loads(manifest.canonical_bytes())
    assert list(payload["non_equivalence_disclaimers"]) == list(NON_EQUIVALENCE_DISCLAIMERS)
    for disclaimer in NON_EQUIVALENCE_DISCLAIMERS:
        assert disclaimer.encode("ascii") in manifest.canonical_bytes()
    assert set(payload) == {
        "comparisons",
        "comparison_version",
        "evidence_kind",
        "evidence_version",
        "format",
        "manifest_version",
        "non_equivalence_disclaimers",
        "plan_fingerprint",
        "records",
        "scenario_version",
        "timings_recorded",
    }
    for forbidden in _CLOSED_FORBIDDEN_CLAIMS:
        assert forbidden not in payload, f"the manifest claims {forbidden} beyond its evidence"


@pytest.mark.parametrize("strategy_id", ["", "gpu", "sequential ", "SEQUENTIAL", "mp"])
def test_unknown_engine_strategy_raises_the_closed_set_error(strategy_id: str) -> None:
    """An unknown runner is never silently substituted by a known one."""
    with pytest.raises(DemoEngineError) as expected:
        run_demo_engine_strategy(
            cast("ConcurrentScenarioHarness", object()),
            strategy_id,
            RunId("run_can-engine-0001"),
            analytics_path=Path("unused.duckdb"),
            runner_configuration={},
        )
    message = str(expected.value)
    assert strategy_id in message or repr(strategy_id) in message
    for known in ENGINE_STRATEGIES:
        assert known in message, f"the closed set message omits {known}"

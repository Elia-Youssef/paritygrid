"""Demo-side canonical story: publication, run bootstrap, and resume.

The demo publishes the canonical pipeline through the ordinary application
services the API layer itself uses, and drives the Phase 19 story against the
runtime's owned database and writer so every fact is visible through the
product boundary.  Every stage tolerates durable evidence an earlier process
already committed: publication converges, the run creation resumes, and the
story replays idempotently.  In-memory objects are never recovery authority.
"""

from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

from sqlalchemy import select

from paritygrid.adapters.persistence.repositories import SqlAlchemyRunRepository
from paritygrid.adapters.persistence.schema import work_items as work_items_table
from paritygrid.adapters.persistence.sqlite import SQLiteDatabase
from paritygrid.application.planner.execution_plan import compile_execution_plan
from paritygrid.application.planner.plan_fingerprint import fingerprint_execution_plan
from paritygrid.application.planner.publication import PublishedPipelineSpecification
from paritygrid.demo.fixtures import write_csv_fixture, write_jsonl_fixture
from paritygrid.demo.scenario_runner import (
    CSV_FIXTURE_NAME,
    JSONL_FIXTURE_NAME,
    ScenarioClock,
    ScenarioRoot,
    # The private helper is the one accepted run-bootstrap path shared with
    # the Phase 19 scenario runner; no public wrapper exists by design.
    _create_run,  # pyright: ignore[reportPrivateUsage]
    execute_canonical_story,
)
from paritygrid.demo.scenarios import (
    CANONICAL_PIPELINE_ID,
    CANONICAL_PIPELINE_VERSION,
    CANONICAL_RUN_ID,
    CanonicalScenarioProfile,
    ScenarioExpectedEvidence,
    canonical_connector_definitions,
    canonical_pipeline_document,
    canonical_plan_fingerprint,
)
from paritygrid.demo.simulators.async_source import AsyncInventorySource
from paritygrid.demo.simulators.blocking_source import BlockingInventorySource
from paritygrid.demo.simulators.warehouse import SimulatedWarehouse
from paritygrid.domain.execution import RunState
from paritygrid.domain.models import RunId
from paritygrid.runtime.composition import RuntimeContainer


class DemoStoryError(RuntimeError):
    """Raised when the demo story cannot be published, resumed, or executed."""


def _real_now() -> datetime:
    from datetime import UTC, datetime

    return datetime.now(UTC)


_STORY_RUN_ABSENT = object()


def _story_clock_anchor(database: SQLiteDatabase, run_id: RunId) -> datetime:
    """Return the story clock anchor: durable time for a resume, now for fresh.

    The injected story clock advances one second per read, so a committed
    story's durable timestamps can sit ahead of wall time.  A resumed story
    must therefore continue after the newest durable fact of the run; a
    fresh run simply anchors at the real current instant so run creation
    follows publication.
    """
    from datetime import timedelta

    durable_floor = _durable_timestamp_floor(database, run_id)
    anchor = _real_now()
    if durable_floor is not None:
        anchor = max(anchor, durable_floor + timedelta(seconds=1))
    return anchor


def _durable_timestamp_floor(database: SQLiteDatabase, run_id: RunId) -> datetime | None:
    """Return the newest durable timestamp of one run, if any fact exists.

    Every column queried stores the fixed 27-character UTC text format, so
    lexicographic MAX is a correct chronological maximum.  The query covers
    every timestamped fact the story can commit — the run row, its durable
    events, attempts, checkpoints, artifacts, reconciliation summary,
    verification observations, repair plans, actions, and approvals — so a
    resumed clock can never land before a monotonicity-checked instant even
    if a future command stops writing a companion event.
    """
    from datetime import datetime

    from sqlalchemy import text

    statement = text(
        """
        SELECT MAX(candidate) FROM (
            SELECT MAX(created_at) AS candidate FROM runs
                WHERE run_id = :run_id
            UNION ALL
            SELECT MAX(started_at) FROM runs
                WHERE run_id = :run_id
            UNION ALL
            SELECT MAX(finished_at) FROM runs
                WHERE run_id = :run_id
            UNION ALL
            SELECT MAX(occurred_at) FROM execution_events
                WHERE run_id = :run_id
            UNION ALL
            SELECT MAX(created_at) FROM artifact_manifests
                WHERE run_id = :run_id
            UNION ALL
            SELECT MAX(committed_at) FROM checkpoints
                WHERE run_id = :run_id
            UNION ALL
            SELECT MAX(created_at) FROM reconciliation_summaries
                WHERE run_id = :run_id
            UNION ALL
            SELECT MAX(observed_at) FROM target_state_verifications
                WHERE run_id = :run_id
            UNION ALL
            SELECT MAX(created_at) FROM repair_plans
                WHERE run_id = :run_id
            UNION ALL
            SELECT MAX(applying_at) FROM repair_plans
                WHERE run_id = :run_id
            UNION ALL
            SELECT MAX(applied_at) FROM repair_plans
                WHERE run_id = :run_id
            UNION ALL
            SELECT MAX(applied_at) FROM repair_actions
                WHERE run_id = :run_id
            UNION ALL
            SELECT MAX(ra.started_at) FROM work_attempts ra
                JOIN work_items wi ON ra.work_item_id = wi.work_item_id
                WHERE wi.run_id = :run_id
            UNION ALL
            SELECT MAX(ra2.finished_at) FROM work_attempts ra2
                JOIN work_items wi2 ON ra2.work_item_id = wi2.work_item_id
                WHERE wi2.run_id = :run_id
            UNION ALL
            SELECT MAX(pa.approved_at) FROM repair_approvals pa
                WHERE pa.repair_plan_id IN (
                    SELECT rp.repair_plan_id FROM repair_plans rp
                        WHERE rp.run_id = :run_id
                )
        )
        """
    )
    with database.engine.connect() as connection:
        row = connection.execute(statement, {"run_id": run_id.value}).first()
    if row is None or row[0] is None:
        return None
    return datetime.strptime(str(row[0]), "%Y-%m-%dT%H:%M:%S.%fZ").replace(tzinfo=UTC)


@dataclass(frozen=True, slots=True)
class PublicationFacts:
    """The durable publication facts of the canonical pipeline."""

    version: int
    plan_fingerprint: str


def publish_canonical_pipeline(container: RuntimeContainer) -> PublicationFacts:
    """Register the canonical connectors and publish the pipeline publicly.

    Registration and publication converge on exact duplicates so a resumed
    demo root replays the committed publication instead of duplicating it;
    any divergence from the locked plan fingerprint fails closed.
    """
    connectors = container.services.connectors
    for definition in canonical_connector_definitions():
        connectors.register(
            connector_id=str(definition.connector_id),
            kind=definition.kind,
            display_name=definition.display_name,
            configuration=definition.configuration.to_mapping(),
            capabilities=definition.capabilities.to_mapping(),
            schema_discovery=None,
            secret_references=(),
            converge_on_duplicate=True,
        )
    pipelines = container.services.pipelines
    pipelines.create(
        pipeline_id=CANONICAL_PIPELINE_ID,
        display_name="Canonical demonstration pipeline",
        description=None,
        converge_on_duplicate=True,
    )
    published = pipelines.publish(
        pipeline_id=CANONICAL_PIPELINE_ID,
        document=canonical_pipeline_document().to_mapping(),
        expected_latest_version=None,
        converge_on_duplicate=True,
    )
    if published.version.number != CANONICAL_PIPELINE_VERSION:
        raise DemoStoryError("the public boundary published an unexpected pipeline version")
    specification = PublishedPipelineSpecification.from_configuration_document(
        published.specification
    )
    actual = fingerprint_execution_plan(compile_execution_plan(specification)).value
    if actual != canonical_plan_fingerprint():
        raise DemoStoryError("the published pipeline diverges from the locked plan fingerprint")
    return PublicationFacts(
        version=published.version.number,
        plan_fingerprint=actual,
    )


@dataclass(frozen=True, slots=True)
class StoryOutcome:
    """The verified outcome of one demo story execution."""

    manifest_bytes: bytes
    execution_evidence_fingerprint: str
    observed_target_fingerprint: str
    reconciliation_fingerprint: str
    run_id: str
    total_target_requests: int
    repair_replay_disposition: str


async def execute_demo_story(
    container: RuntimeContainer,
    scenario_path: Path,
    evidence: ScenarioExpectedEvidence,
    profile: CanonicalScenarioProfile,
    *,
    async_source: AsyncInventorySource,
    blocking_source: BlockingInventorySource,
    warehouse: SimulatedWarehouse,
    failpoint: Callable[[str], None] | None = None,
) -> StoryOutcome:
    """Run the resume-tolerant canonical story against the runtime resources."""
    _write_story_fixtures(scenario_path, evidence)
    scenario_root = ScenarioRoot(path=scenario_path)
    run_id = RunId(CANONICAL_RUN_ID)
    # The demo publishes through the runtime's real-time services, so a fresh
    # story anchors its clock at the current instant: run creation must follow
    # publication.  A resumed story anchors after the maximum durable
    # timestamp instead — the interrupted process's injected clock ran ahead
    # of wall time, so recovery reconstructs its time source from the durable
    # evidence to keep every later write monotonic.  Timestamps never enter
    # any canonical fact, identity, or fingerprint, so anchoring cannot
    # change the locked evidence.
    clock = ScenarioClock(_story_clock_anchor(container.database, run_id))
    _create_or_resume_story_run(container, run_id, profile=profile, clock=clock)
    result = await execute_canonical_story(
        scenario_root=scenario_root,
        database=container.database,
        writer=container.writer,
        clock=clock,
        run_id=run_id,
        evidence=evidence,
        profile=profile,
        async_source=async_source,
        blocking_source=blocking_source,
        warehouse=warehouse,
        resume_enabled=True,
        failpoint=failpoint,
    )
    return StoryOutcome(
        manifest_bytes=result.manifest_bytes,
        execution_evidence_fingerprint=result.execution_evidence_fingerprint,
        observed_target_fingerprint=result.observed_target_fingerprint,
        reconciliation_fingerprint=result.reconciliation_fingerprint,
        run_id=result.run_id,
        total_target_requests=result.total_target_requests,
        repair_replay_disposition=result.repair_replay_disposition,
    )


def _write_story_fixtures(scenario_path: Path, evidence: ScenarioExpectedEvidence) -> None:
    csv_manifest = write_csv_fixture(
        evidence.slice_for("csv").dataset,
        scenario_path / "fixtures" / CSV_FIXTURE_NAME,
    )
    jsonl_manifest = write_jsonl_fixture(
        evidence.slice_for("jsonl").dataset,
        scenario_path / "fixtures" / JSONL_FIXTURE_NAME,
    )
    if csv_manifest.byte_size != len(evidence.csv_fixture_bytes):
        raise DemoStoryError("the written csv fixture diverges from the derivation")
    if jsonl_manifest.byte_size != len(evidence.jsonl_fixture_bytes):
        raise DemoStoryError("the written jsonl fixture diverges from the derivation")


def _create_or_resume_story_run(
    container: RuntimeContainer,
    run_id: RunId,
    *,
    profile: CanonicalScenarioProfile,
    clock: ScenarioClock,
) -> str:
    """Create the canonical story run, or resume the durable one.

    An absent run is created exactly like the accepted single-shot story.  An
    existing run must match the canonical identity; a started run keeps its
    committed evidence.  Durable state — never in-memory objects — decides
    which path applies.
    """
    with container.database.transaction() as session:
        existing = SqlAlchemyRunRepository(session).get(run_id)
    if existing is None:
        _create_run(container.writer, container.database, profile, clock, run_id)
        return "created"
    if existing.pipeline_id.value != CANONICAL_PIPELINE_ID:
        raise DemoStoryError("the durable story run references an unexpected pipeline")
    if existing.state not in (RunState.RUNNING, RunState.SUCCEEDED):
        raise DemoStoryError("the durable story run is not resumable; the demo root requires reset")
    with container.database.transaction() as session:
        bootstrapped = session.execute(
            select(work_items_table.c.work_item_id).where(work_items_table.c.run_id == run_id.value)
        ).first()
    if bootstrapped is None:
        raise DemoStoryError("a started story run without bootstrapped work is not resumable")
    return "resumed"

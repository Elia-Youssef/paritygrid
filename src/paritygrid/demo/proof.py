"""Headless smoke proof: verify every required fact before reporting success.

A clean shutdown is not a successful smoke run.  The proof re-derives every
required fact from durable state — the migrated schema revision, the run's
terminal row, the attempt history, artifact manifests and file integrity, the
approval and application facts, the independent target verification, and the
engine-plane execution evidence — and only then emits the deterministic
machine-readable result.  Wall-clock timings stay out of the result: they are
diagnostic human output and never canonical correctness bytes.
"""

from dataclasses import dataclass
from hashlib import sha256

from sqlalchemy import select

from paritygrid.adapters.persistence.migration import HEAD_REVISION
from paritygrid.adapters.persistence.sqlite import SQLiteDatabase
from paritygrid.demo.datasets import WireValue, canonical_json_bytes
from paritygrid.demo.engine_runner import ENGINE_STRATEGIES
from paritygrid.demo.fault_controls import fault_controls
from paritygrid.demo.ownership import OWNERSHIP_MARKER_NAME, DemoRoot
from paritygrid.demo.scenario_runner import ARTIFACTS_DIRNAME, DATABASE_FILENAME, MANIFEST_FILENAME
from paritygrid.demo.scenarios import (
    CANONICAL_PIPELINE_ID,
    CANONICAL_PIPELINE_VERSION,
    CANONICAL_SCENARIO_SEED,
    CANONICAL_SCENARIO_VERSION,
    ScenarioExpectedEvidence,
)
from paritygrid.demo.story import PublicationFacts, StoryOutcome
from paritygrid.demo.verification import RunnerExecutionRecord
from paritygrid.domain.models import RunId

DEMO_RESULT_FORMAT = "paritygrid.demo.result"
DEMO_RESULT_VERSION = 1
EXECUTION_EVIDENCE_KIND = "execution-evidence"
EXECUTION_EVIDENCE_VERSION = 2
RECONCILIATION_KIND = "reconciliation"
RECONCILIATION_VERSION = 1
TARGET_STATE_KIND = "target_state"
TARGET_STATE_VERSION = 1
PLAN_KIND = "plan"
PLAN_VERSION = 1

# Stable, documented demo exit codes.
EXIT_SUCCESS = 0
EXIT_FAILURE = 1
EXIT_USAGE = 2
EXIT_TIMEOUT = 3
EXIT_CANCELLED = 4


class ProofError(RuntimeError):
    """Raised when a required smoke fact does not hold."""

    def __init__(self, code: str, detail: str) -> None:
        super().__init__(detail)
        self.code = code


@dataclass(frozen=True, slots=True)
class DemoResult:
    """The deterministic, bounded, machine-readable smoke result."""

    document: dict[str, WireValue]

    def canonical_bytes(self) -> bytes:
        """Return the byte-stable result document."""
        return canonical_json_bytes(self.document)


def build_demo_result(
    *,
    runner: str,
    migration_revision: str,
    publication: PublicationFacts,
    story: StoryOutcome,
    engine_record: RunnerExecutionRecord,
    evidence: ScenarioExpectedEvidence,
) -> DemoResult:
    """Verify the in-process smoke facts and emit the deterministic result."""
    _verify_runner(runner)
    _verify_migration_revision(migration_revision)
    _verify_publication(publication, evidence)
    _verify_engine_record(engine_record, runner, evidence)
    document: dict[str, WireValue] = {
        "engine": {
            "attempt_outcome_counts": dict(sorted(engine_record.attempt_outcome_counts.items())),
            "checkpoint_count": engine_record.checkpoint_count,
            "evidence_kind": engine_record.evidence.evidence_kind,
            "evidence_version": engine_record.evidence.evidence_version,
            "execution_evidence_fingerprint": (
                engine_record.evidence.execution_evidence_fingerprint
            ),
            "run_id": engine_record.run_id,
            "strategy_id": engine_record.strategy_id,
        },
        "format": DEMO_RESULT_FORMAT,
        "fault_controls": [control.identity for control in fault_controls()],
        "fingerprints": _fingerprint_kinds(story),
        "migrations": {"revision": migration_revision},
        "publication": {
            "pipeline_id": CANONICAL_PIPELINE_ID,
            "plan_fingerprint": publication.plan_fingerprint,
            "version": publication.version,
        },
        "result_version": DEMO_RESULT_VERSION,
        "runner": runner,
        "scenario_version": CANONICAL_SCENARIO_VERSION,
        "seed": CANONICAL_SCENARIO_SEED,
        "story": {
            "counts": dict(sorted(evidence.counts.as_mapping().items())),
            "execution_evidence_fingerprint": story.execution_evidence_fingerprint,
            "observed_target_fingerprint": story.observed_target_fingerprint,
            "reconciliation_fingerprint": story.reconciliation_fingerprint,
            "repair_replay_disposition": story.repair_replay_disposition,
            "run_id": story.run_id,
            "total_target_requests": story.total_target_requests,
        },
    }
    return DemoResult(document=document)


def verify_durable_facts(
    database: SQLiteDatabase,
    demo_root: DemoRoot,
    story: StoryOutcome,
    engine_record: RunnerExecutionRecord,
    evidence: ScenarioExpectedEvidence,
) -> list[str]:
    """Verify the durable-state facts of the smoke run; return check names."""
    checks: list[str] = []
    _require_run_terminal(database, RunId(story.run_id))
    checks.append("story_run_terminal")
    _require_story_attempts(database, RunId(story.run_id))
    checks.append("story_retry_and_attempts")
    _require_story_artifacts(database, demo_root, RunId(story.run_id))
    checks.append("artifact_identities_and_integrity")
    _require_reconciliation_summary(database, RunId(story.run_id), story, evidence)
    checks.append("reconciliation_summary")
    _require_repair_facts(database, RunId(story.run_id))
    checks.append("repair_approval_application_idempotent")
    _require_target_verification(database, RunId(story.run_id), story)
    checks.append("independent_target_parity")
    _require_engine_terminal(database, engine_record)
    checks.append("engine_run_terminal")
    _require_root_coherent(demo_root)
    checks.append("demo_root_coherent")
    del evidence
    return checks


def _verify_runner(runner: str) -> None:
    if runner not in ENGINE_STRATEGIES:
        raise ProofError("runner_mismatch", f"runner {runner!r} is not a full-plan runner")


def _verify_migration_revision(revision: str) -> None:
    if revision != HEAD_REVISION:
        raise ProofError("migration_incomplete", "the schema did not reach the head revision")


def _verify_publication(publication: PublicationFacts, evidence: ScenarioExpectedEvidence) -> None:
    if publication.version != CANONICAL_PIPELINE_VERSION:
        raise ProofError(
            "publication_version", "the canonical pipeline version is not the locked version"
        )
    if publication.plan_fingerprint != evidence.plan_fingerprint:
        raise ProofError(
            "plan_fingerprint", "the published plan fingerprint diverges from the locked value"
        )


def _verify_engine_record(
    record: RunnerExecutionRecord, runner: str, evidence: ScenarioExpectedEvidence
) -> None:
    if record.strategy_id != runner:
        raise ProofError(
            "runner_mismatch",
            "the executed engine strategy differs from the requested runner",
        )
    if record.evidence.evidence_kind != EXECUTION_EVIDENCE_KIND:
        raise ProofError("fingerprint_kind", "the engine evidence kind is not execution-evidence")
    if record.evidence.evidence_version != EXECUTION_EVIDENCE_VERSION:
        raise ProofError("fingerprint_version", "the engine evidence version is not 2")
    if record.evidence.plan_fingerprint != evidence.plan_fingerprint:
        raise ProofError("plan_fingerprint", "the engine run planned a different pipeline")
    counts = record.attempt_outcome_counts
    if counts.get("retry_scheduled", 0) != 1:
        raise ProofError(
            "engine_retry_facts",
            "the engine run must carry exactly one durably scheduled retry",
        )
    if set(counts) - {"retry_scheduled", "succeeded"}:
        raise ProofError(
            "engine_retry_facts",
            "the canonical engine script admits only successes and the one retry",
        )


def _fingerprint_kinds(story: StoryOutcome) -> dict[str, WireValue]:
    return {
        PLAN_KIND: {"kind": PLAN_KIND, "version": PLAN_VERSION},
        "execution_evidence": {
            "kind": EXECUTION_EVIDENCE_KIND,
            "version": EXECUTION_EVIDENCE_VERSION,
        },
        RECONCILIATION_KIND: {
            "fingerprint": story.reconciliation_fingerprint,
            "kind": RECONCILIATION_KIND,
            "version": RECONCILIATION_VERSION,
        },
        TARGET_STATE_KIND: {
            "fingerprint": story.observed_target_fingerprint,
            "kind": TARGET_STATE_KIND,
            "version": TARGET_STATE_VERSION,
        },
    }


def _require_run_terminal(database: SQLiteDatabase, run_id: RunId) -> None:
    from paritygrid.adapters.persistence.schema import runs as runs_table

    with database.transaction() as session:
        row = session.execute(
            select(runs_table.c.state).where(runs_table.c.run_id == run_id.value)
        ).first()
    if row is None or str(row.state) != "succeeded":
        raise ProofError("story_run_state", "the canonical story run is not durably succeeded")


def _require_story_attempts(database: SQLiteDatabase, run_id: RunId) -> None:
    from paritygrid.adapters.persistence.schema import work_attempts, work_items

    with database.transaction() as session:
        rows = session.execute(
            select(work_attempts.c.attempt_number, work_attempts.c.failure_classification)
            .join(work_items, work_attempts.c.work_item_id == work_items.c.work_item_id)
            .where(work_items.c.run_id == run_id.value)
        ).all()
    numbers = sorted(int(row.attempt_number) for row in rows)
    if numbers != [1, 2]:
        raise ProofError(
            "story_attempts", "the canonical story must record exactly two durable attempts"
        )
    failed = [str(row.failure_classification) for row in rows if row.failure_classification]
    if failed != ["http_429"]:
        raise ProofError(
            "story_retry_classification",
            "the canonical story must classify exactly one http_429 attempt",
        )


def _require_story_artifacts(database: SQLiteDatabase, demo_root: DemoRoot, run_id: RunId) -> None:
    from paritygrid.adapters.persistence.schema import artifact_manifests

    with database.transaction() as session:
        rows = session.execute(
            select(
                artifact_manifests.c.artifact_id,
                artifact_manifests.c.relative_path,
                artifact_manifests.c.sha256,
            ).where(artifact_manifests.c.run_id == run_id.value)
        ).all()
    if len(rows) != 1:
        raise ProofError(
            "artifact_registry", "the story run must register exactly the conflict artifact"
        )
    row = rows[0]
    artifact_path = demo_root.scenario_path / ARTIFACTS_DIRNAME / str(row.relative_path)
    digest = sha256(artifact_path.read_bytes()).hexdigest()
    if digest != str(row.sha256):
        raise ProofError(
            "artifact_integrity", "the conflict artifact file diverges from its manifest"
        )


def _require_reconciliation_summary(
    database: SQLiteDatabase,
    run_id: RunId,
    story: StoryOutcome,
    evidence: ScenarioExpectedEvidence,
) -> None:
    from paritygrid.adapters.persistence.schema import reconciliation_summaries

    with database.transaction() as session:
        row = session.execute(
            select(reconciliation_summaries.c.reconciliation_fingerprint).where(
                reconciliation_summaries.c.run_id == run_id.value
            )
        ).first()
    if row is None:
        raise ProofError("reconciliation_missing", "the durable reconciliation summary is absent")
    if str(row.reconciliation_fingerprint) != story.reconciliation_fingerprint:
        raise ProofError(
            "reconciliation_fingerprint",
            "the durable reconciliation fingerprint diverges from the story fact",
        )
    if story.reconciliation_fingerprint != evidence.reconciliation_fingerprint:
        raise ProofError(
            "reconciliation_expected",
            "the reconciliation fingerprint diverges from the locked derivation",
        )


def _require_repair_facts(database: SQLiteDatabase, run_id: RunId) -> None:
    from paritygrid.adapters.persistence.schema import (
        repair_actions,
        repair_approvals,
        repair_plans,
    )

    with database.transaction() as session:
        plans = session.execute(
            select(repair_plans.c.repair_plan_id, repair_plans.c.status).where(
                repair_plans.c.run_id == run_id.value
            )
        ).all()
        plan_ids = [str(row.repair_plan_id) for row in plans]
        approvals = session.execute(
            select(repair_approvals.c.repair_plan_id).where(
                repair_approvals.c.repair_plan_id.in_(plan_ids)
            )
        ).all()
        actions = session.execute(
            select(repair_actions.c.applied_at).where(repair_actions.c.run_id == run_id.value)
        ).all()
    if not plans:
        raise ProofError("plan_missing", "the canonical repair plan is not durable")
    if not plans or any(str(plan.status) != "applied" for plan in plans):
        raise ProofError("plan_status", "the canonical repair plan is not durably applied")
    if not approvals:
        raise ProofError(
            "approval_missing",
            "no durable approval fact exists for the canonical repair plan",
        )
    if not actions or any(row.applied_at is None for row in actions):
        raise ProofError("repair_actions", "every canonical repair action must be durably applied")


def _require_target_verification(
    database: SQLiteDatabase, run_id: RunId, story: StoryOutcome
) -> None:
    from paritygrid.adapters.persistence.schema import target_state_verifications

    with database.transaction() as session:
        rows = session.execute(
            select(
                target_state_verifications.c.verdict,
                target_state_verifications.c.observed_fingerprint,
            ).where(target_state_verifications.c.run_id == run_id.value)
        ).all()
    if not rows:
        raise ProofError(
            "verification_missing", "no independent target verification fact is durable"
        )
    for row in rows:
        if str(row.verdict) != "parity_holding":
            raise ProofError("verification_verdict", "the target verification is not parity")
        if str(row.observed_fingerprint) != story.observed_target_fingerprint:
            raise ProofError(
                "verification_fingerprint",
                "the verified target fingerprint diverges from the story fact",
            )


def _require_engine_terminal(
    database: SQLiteDatabase, engine_record: RunnerExecutionRecord
) -> None:
    from paritygrid.adapters.persistence.schema import runs as runs_table

    with database.transaction() as session:
        row = session.execute(
            select(runs_table.c.state).where(runs_table.c.run_id == engine_record.run_id)
        ).first()
    if row is None or str(row.state) != "succeeded":
        raise ProofError("engine_run_state", "the engine run is not durably succeeded")


def _require_root_coherent(demo_root: DemoRoot) -> None:
    scenario_path = demo_root.scenario_path
    if not (scenario_path / MANIFEST_FILENAME).is_file():
        raise ProofError(
            "manifest_missing", "the verified story did not publish its canonical manifest"
        )
    if not (demo_root.path / OWNERSHIP_MARKER_NAME).is_file():
        raise ProofError("ownership_missing", "the demo root lost its ownership marker")
    if not (scenario_path / DATABASE_FILENAME).is_file():
        raise ProofError("database_missing", "the demo root lost its operational database")

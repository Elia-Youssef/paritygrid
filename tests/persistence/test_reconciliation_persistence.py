"""Persistence tests for reconciliation results, verifications, and migration 0003."""

from collections.abc import Iterator
from datetime import UTC, datetime
from pathlib import Path

import pytest
from sqlalchemy import func, insert, select

from paritygrid.adapters.persistence import SQLiteDatabase, SQLiteDatabaseConfig
from paritygrid.adapters.persistence.migration import (
    HEAD_REVISION,
    MigrationReport,
    upgrade_to_head,
)
from paritygrid.adapters.persistence.repositories import (
    SqlAlchemyReconciliationResultRepository,
    SqlAlchemyRunRepository,
    SqlAlchemyTargetVerificationRepository,
)
from paritygrid.adapters.persistence.schema import (
    reconciliation_conflicts,
    reconciliation_summaries,
    target_state_verifications,
)
from paritygrid.application.ports.consistency import RedactedDocument
from paritygrid.application.ports.reconciliation_persistence import (
    ReconciliationInvalidRequestError,
    TargetVerificationConflictError,
    TargetVerificationRecord,
    TargetVerificationVerdict,
)
from paritygrid.domain.execution import RunState
from paritygrid.domain.models import (
    ConflictId,
    RunId,
    StateFingerprint,
    TargetVerificationId,
    UtcTimestamp,
)

FINGERPRINT = StateFingerprint("4" * 64)
EVIDENCE = StateFingerprint("5" * 64)
RUN_ID = RunId("run_reconciliation-persistence")
VERIFICATION_ID = TargetVerificationId("tgv_reconciliation-persistence")
MOMENT = UtcTimestamp(datetime(2026, 8, 27, 10, 0, 0, tzinfo=UTC))


@pytest.fixture
def database(tmp_path: Path) -> Iterator[SQLiteDatabase]:
    database = SQLiteDatabase.open(SQLiteDatabaseConfig(tmp_path / "recon %25.db"))
    with database.engine.connect() as connection:
        upgrade_to_head(connection)
    try:
        yield database
    finally:
        database.close()


def seed_run(database: SQLiteDatabase) -> None:
    from paritygrid.adapters.persistence.repositories import (
        SqlAlchemyPipelineRepository,
    )
    from paritygrid.application.ports.configuration import ConfigurationDocument
    from paritygrid.domain.models import NodeId, PipelineId, PipelineVersion

    with database.transaction() as session:
        pipelines = SqlAlchemyPipelineRepository(session)
        pipelines.create(
            pipeline_id=PipelineId("pip_reconciliation-persistence"),
            display_name="Persistence pipeline",
            description=None,
            created_at=MOMENT,
        )
        pipelines.publish_version(
            pipeline_id=PipelineId("pip_reconciliation-persistence"),
            expected_latest_version=None,
            specification=ConfigurationDocument.from_mapping({"nodes": []}),
            planner_format_version=1,
            published_at=MOMENT,
        )
        repository = SqlAlchemyRunRepository(session)
        repository.create(
            run_id=RUN_ID,
            pipeline_id=PipelineId("pip_reconciliation-persistence"),
            pipeline_version=PipelineVersion(1),
            runner_kind="sequential",
            runner_configuration=ConfigurationDocument.from_mapping({}),
            scenario_seed=None,
            node_ids=(NodeId("nod_persistence"),),
            created_at=MOMENT,
        )
        repository.transition(
            RUN_ID,
            expected_row_version=1,
            target_state=RunState.RUNNING,
            transitioned_at=MOMENT,
        )
        repository.transition(
            RUN_ID,
            expected_row_version=2,
            target_state=RunState.SUCCEEDED,
            transitioned_at=MOMENT,
            execution_evidence_fingerprint=EVIDENCE,
            execution_evidence_fingerprint_version=2,
        )


def seed_summary(database: SQLiteDatabase) -> None:
    with database.transaction() as session:
        session.execute(
            insert(reconciliation_summaries).values(
                run_id=RUN_ID.value,
                match_count=1,
                missing_from_target_count=1,
                missing_from_source_count=0,
                field_mismatch_count=0,
                duplicate_source_count=0,
                duplicate_target_count=0,
                duplicate_both_count=0,
                total_count=2,
                source_fingerprint="1" * 64,
                target_fingerprint="2" * 64,
                reconciliation_fingerprint=FINGERPRINT.value,
                analytical_query_version=1,
                created_at=str(MOMENT),
            )
        )
        session.execute(
            insert(reconciliation_conflicts).values(
                conflict_id=ConflictId("cnf_reconciliation-persistence-grid-0001").value,
                run_id=RUN_ID.value,
                canonical_key="GRID-0001",
                classification="missing_from_target",
                source_references_json="[]",
                target_reference_json=None,
                field_differences_json="[]",
                suggested_resolution="create_target",
                created_at=str(MOMENT),
            )
        )


def verification(
    *, verdict: TargetVerificationVerdict = TargetVerificationVerdict.PARITY_HOLDING
) -> TargetVerificationRecord:
    return TargetVerificationRecord(
        verification_id=VERIFICATION_ID,
        run_id=RUN_ID,
        repair_plan_id=None,
        reconciliation_fingerprint=FINGERPRINT,
        plan_content_fingerprint=None,
        observed_fingerprint=StateFingerprint("6" * 64),
        observed_fingerprint_version=1,
        expected_fingerprint=StateFingerprint("7" * 64),
        verdict=verdict,
        observed_record_count=3,
        expected_record_count=3,
        observed_target_version=3,
        observed_at=MOMENT,
        detail=RedactedDocument.from_mapping({"divergences": {}}),
    )


class TestMigration0003:
    def test_empty_database_upgrades_to_the_new_head(self, tmp_path: Path) -> None:
        import sqlalchemy

        engine = sqlalchemy.create_engine(f"sqlite:///{tmp_path / 'empty.db'}")
        with engine.connect() as connection:
            report = upgrade_to_head(connection)
            assert report == MigrationReport(None, HEAD_REVISION, HEAD_REVISION)
            tables = connection.exec_driver_sql(
                "SELECT name FROM sqlite_master "
                "WHERE type = 'table' AND name = 'target_state_verifications'"
            ).scalar_one_or_none()
            assert tables == "target_state_verifications"
            triggers = connection.exec_driver_sql(
                "SELECT COUNT(*) FROM sqlite_master "
                "WHERE type = 'trigger' AND tbl_name = 'target_state_verifications'"
            ).scalar_one()
            assert triggers == 2
        engine.dispose()

    def test_previous_released_fixture_upgrades_and_preserves_rows(self, tmp_path: Path) -> None:
        import sqlite3

        import sqlalchemy

        from paritygrid.quality.frozen_schema import reconstruct_fixture
        from tests.persistence.test_frozen_schema_fixture import SCHEMA_PATH, SEED_PATH

        raw = sqlite3.connect(tmp_path / "fixture.db")
        reconstruct_fixture(raw, SCHEMA_PATH.read_bytes(), SEED_PATH.read_bytes())
        raw.close()
        engine = sqlalchemy.create_engine(f"sqlite:///{tmp_path / 'fixture.db'}")
        with engine.connect() as connection:
            before = connection.exec_driver_sql("SELECT COUNT(*) FROM runs").scalar_one()
            connection.rollback()
            report = upgrade_to_head(connection)
            assert report.previous_revision == "0001_operational"
            assert report.current_revision == HEAD_REVISION
            after = connection.exec_driver_sql("SELECT COUNT(*) FROM runs").scalar_one()
            assert after == before
        engine.dispose()

    def test_downgrade_is_irreversible_by_design(self, tmp_path: Path) -> None:
        import sqlalchemy
        from alembic import command

        from paritygrid.adapters.persistence.migration import (
            _migration_config,  # pyright: ignore[reportPrivateUsage]
        )

        engine = sqlalchemy.create_engine(f"sqlite:///{tmp_path / 'downgrade.db'}")
        with engine.connect() as connection:
            upgrade_to_head(connection)
            config = _migration_config(connection)
            with pytest.raises(RuntimeError, match="restore from backup"):
                command.downgrade(config, "0002_execution_evidence")
        engine.dispose()

    def test_verification_rows_are_immutable(self, database: SQLiteDatabase) -> None:
        seed_run(database)
        seed_summary(database)
        with database.transaction() as session:
            SqlAlchemyTargetVerificationRepository(session).record(verification())
        with (
            database.transaction() as session,
            pytest.raises(Exception, match="does not permit"),
        ):
            session.execute(
                target_state_verifications.delete().where(
                    target_state_verifications.c.verification_id == VERIFICATION_ID.value
                )
            )
        with (
            database.transaction() as session,
            pytest.raises(Exception, match="does not permit"),
        ):
            session.execute(
                target_state_verifications.update()
                .where(target_state_verifications.c.verification_id == VERIFICATION_ID.value)
                .values(verdict="parity_divergent")
            )


class TestTargetVerificationRepository:
    def test_record_is_idempotent_and_rejects_divergence(self, database: SQLiteDatabase) -> None:
        seed_run(database)
        seed_summary(database)
        with database.transaction() as session:
            repository = SqlAlchemyTargetVerificationRepository(session)
            first = repository.record(verification())
            replay = repository.record(verification())
            assert replay == first
        divergent = TargetVerificationRecord(
            verification_id=VERIFICATION_ID,
            run_id=RUN_ID,
            repair_plan_id=None,
            reconciliation_fingerprint=FINGERPRINT,
            plan_content_fingerprint=None,
            observed_fingerprint=StateFingerprint("8" * 64),
            observed_fingerprint_version=1,
            expected_fingerprint=StateFingerprint("7" * 64),
            verdict=TargetVerificationVerdict.PARITY_HOLDING,
            observed_record_count=3,
            expected_record_count=3,
            observed_target_version=3,
            observed_at=MOMENT,
            detail=RedactedDocument.from_mapping({"divergences": {}}),
        )
        with (
            database.transaction() as session,
            pytest.raises(TargetVerificationConflictError),
        ):
            SqlAlchemyTargetVerificationRepository(session).record(divergent)
        with database.transaction() as session:
            assert session.scalar(select(func.count()).select_from(target_state_verifications)) == 1

    def test_record_requires_the_reconciliation_parent(self, database: SQLiteDatabase) -> None:
        seed_run(database)
        with database.transaction() as session:
            from paritygrid.application.ports.reconciliation_persistence import (
                TargetVerificationStorageError,
            )

            with pytest.raises(TargetVerificationStorageError):
                SqlAlchemyTargetVerificationRepository(session).record(verification())

    def test_latest_for_run_orders_by_observation_time(self, database: SQLiteDatabase) -> None:
        seed_run(database)
        seed_summary(database)
        later = TargetVerificationRecord(
            verification_id=TargetVerificationId("tgv_reconciliation-persistence-later"),
            run_id=RUN_ID,
            repair_plan_id=None,
            reconciliation_fingerprint=FINGERPRINT,
            plan_content_fingerprint=None,
            observed_fingerprint=StateFingerprint("9" * 64),
            observed_fingerprint_version=1,
            expected_fingerprint=StateFingerprint("7" * 64),
            verdict=TargetVerificationVerdict.PARITY_DIVERGENT,
            observed_record_count=3,
            expected_record_count=3,
            observed_target_version=4,
            observed_at=UtcTimestamp(datetime(2026, 8, 27, 11, 0, 0, tzinfo=UTC)),
            detail=RedactedDocument.from_mapping({"divergences": {}}),
        )
        with database.transaction() as session:
            repository = SqlAlchemyTargetVerificationRepository(session)
            repository.record(verification())
            repository.record(later)
        with database.transaction() as session:
            latest = SqlAlchemyTargetVerificationRepository(session).latest_for_run(RUN_ID)
            assert latest is not None
            assert latest.verification_id == later.verification_id


class TestReconciliationResultRepository:
    def test_get_summary_and_result_round_trip(self, database: SQLiteDatabase) -> None:
        seed_run(database)
        seed_summary(database)
        with database.transaction() as session:
            repository = SqlAlchemyReconciliationResultRepository(session)
            summary = repository.get_summary(RUN_ID)
            result = repository.get_result(RUN_ID)
        assert summary is not None
        assert result is not None
        assert summary.reconciliation_fingerprint == FINGERPRINT
        assert result.summary.reconciliation_fingerprint == FINGERPRINT
        assert len(result.conflicts) == 1
        assert result.conflicts[0].canonical_key == "GRID-0001"

    def test_missing_run_returns_none(self, database: SQLiteDatabase) -> None:
        with database.transaction() as session:
            repository = SqlAlchemyReconciliationResultRepository(session)
            assert repository.get_summary(RunId("run_missing")) is None
            assert repository.get_result(RunId("run_missing")) is None

    def test_repositories_refuse_to_run_without_a_transaction(
        self, database: SQLiteDatabase
    ) -> None:
        from paritygrid.adapters.persistence.sqlite import create_session_factory

        session_factory = create_session_factory(database.engine)
        session = session_factory()
        try:
            repository = SqlAlchemyReconciliationResultRepository(session)
            with pytest.raises(ReconciliationInvalidRequestError):
                repository.get_summary(RUN_ID)
        finally:
            session.close()

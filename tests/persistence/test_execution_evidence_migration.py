"""P7.1 execution-evidence fingerprint storage migration contract."""

from __future__ import annotations

from collections.abc import Iterator
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING, cast

if TYPE_CHECKING:
    from paritygrid.application.ports.configuration import ConfigurationDocument

import pytest
from alembic import command
from sqlalchemy import create_engine
from sqlalchemy.engine import Connection, Engine, RowMapping
from sqlalchemy.orm import Session

from paritygrid.adapters.persistence.migration import (
    HEAD_REVISION,
    _migration_config,  # pyright: ignore[reportPrivateUsage]
    upgrade_to_head,
)
from paritygrid.adapters.persistence.repositories import execution_mapping as mapping
from paritygrid.adapters.persistence.repositories.runs import SqlAlchemyRunRepository
from paritygrid.adapters.persistence.schema import metadata, runs
from paritygrid.application.ports.execution import ExecutionCorruptionError
from paritygrid.domain.execution import RunState
from paritygrid.domain.models import (
    NodeId,
    PipelineId,
    PipelineVersion,
    RunId,
    StateFingerprint,
    UtcTimestamp,
)

DIGEST_A = "1" * 64
DIGEST_B = "2" * 64
CREATED_AT = "2026-08-21T12:00:00.000000Z"
STARTED_AT = "2026-08-21T12:01:00.000000Z"
FINISHED_AT = "2026-08-21T12:02:00.000000Z"


def _v0001_row(
    run_id: str,
    *,
    state: str,
    fingerprint: str | None,
    finished_at: str | None,
    row_version: int = 1,
) -> dict[str, object]:
    return {
        "run_id": run_id,
        "pipeline_id": "pip_migration-p7-1",
        "pipeline_version_number": 1,
        "runner_kind": "sequential",
        "runner_configuration_json": '{"concurrency":1}',
        "state": state,
        "row_version": row_version,
        "scenario_seed": 7,
        "created_at": CREATED_AT,
        "started_at": STARTED_AT,
        "finished_at": finished_at,
        "cancellation_requested_at": None,
        "recovery_started_at": None,
        "recovered_at": None,
        "final_reconciliation_fingerprint": fingerprint,
    }


@pytest.fixture
def v0001_engine(tmp_path: Path) -> Iterator[Engine]:
    engine = create_engine(f"sqlite+pysqlite:///{tmp_path / 'v0001.db'}")
    with engine.connect() as connection:
        config = _migration_config(connection)
        connection.exec_driver_sql("PRAGMA foreign_keys = OFF")
        command.upgrade(config, "0001_operational")
        connection.commit()
    yield engine
    engine.dispose()


def _seed_pipeline(engine: Engine) -> None:
    from paritygrid.adapters.persistence.repositories.pipelines import (
        SqlAlchemyPipelineRepository,
    )
    from paritygrid.application.ports.configuration import ConfigurationDocument

    with Session(engine) as session, session.begin():
        repository = SqlAlchemyPipelineRepository(session)
        repository.create(
            pipeline_id=PipelineId("pip_migration-p7-1"),
            display_name="Migration P7.1",
            description=None,
            created_at=UtcTimestamp(datetime(2026, 8, 21, 12, 0, tzinfo=UTC)),
        )
        repository.publish_version(
            pipeline_id=PipelineId("pip_migration-p7-1"),
            expected_latest_version=None,
            specification=ConfigurationDocument.from_mapping({"nodes": []}),
            planner_format_version=1,
            published_at=UtcTimestamp(datetime(2026, 8, 21, 12, 0, tzinfo=UTC)),
        )


def _seed_v0001_runs(
    engine: Engine,
    rows: tuple[dict[str, object], ...],
) -> None:
    with engine.begin() as connection:
        for row in rows:
            columns = ", ".join(f'"{name}"' for name in row)
            parameters = ", ".join(f":{name}" for name in row)
            connection.exec_driver_sql(
                f'INSERT INTO "runs" ({columns}) VALUES ({parameters})',
                dict(row),
            )


def _runs_rows(connection: Connection) -> dict[str, tuple[object, ...]]:
    rows = connection.exec_driver_sql(
        "SELECT run_id, state, execution_evidence_fingerprint, "
        "execution_evidence_fingerprint_version FROM runs"
    ).all()
    return {str(row[0]): tuple(row[1:]) for row in rows}


def test_mixed_v0001_values_are_preserved_and_backfilled_as_version_2(
    v0001_engine: Engine,
) -> None:
    _seed_pipeline(v0001_engine)
    _seed_v0001_runs(
        v0001_engine,
        (
            _v0001_row("run_migration-null", state="running", fingerprint=None, finished_at=None),
            _v0001_row(
                "run_migration-kept",
                state="succeeded",
                fingerprint=DIGEST_A,
                finished_at=FINISHED_AT,
            ),
            _v0001_row(
                "run_migration-kept-two",
                state="partially_succeeded",
                fingerprint=DIGEST_B,
                finished_at=FINISHED_AT,
            ),
        ),
    )
    with v0001_engine.connect() as connection:
        report = upgrade_to_head(connection)
        rows = _runs_rows(connection)
        connection.rollback()

    assert report.previous_revision == "0001_operational"
    assert report.current_revision == HEAD_REVISION
    assert rows["run_migration-null"] == ("running", None, None)
    assert rows["run_migration-kept"] == ("succeeded", DIGEST_A, 2)
    assert rows["run_migration-kept-two"] == ("partially_succeeded", DIGEST_B, 2)


def test_upgraded_storage_rejects_unpaired_fingerprint_and_version(
    v0001_engine: Engine,
) -> None:
    _seed_pipeline(v0001_engine)
    _seed_v0001_runs(
        v0001_engine,
        (_v0001_row("run_migration-host", state="running", fingerprint=None, finished_at=None),),
    )
    with v0001_engine.connect() as connection:
        upgrade_to_head(connection)
        connection.rollback()
    with pytest.raises(Exception, match="pairing"), v0001_engine.begin() as connection:
        connection.exec_driver_sql(
            "UPDATE runs SET execution_evidence_fingerprint_version = 2 "
            "WHERE run_id = 'run_migration-host'"
        )
    with pytest.raises(Exception, match="pairing"), v0001_engine.begin() as connection:
        connection.exec_driver_sql(
            "UPDATE runs SET execution_evidence_fingerprint = :digest "
            "WHERE run_id = 'run_migration-host'",
            {"digest": DIGEST_A},
        )
    with pytest.raises(Exception, match="terminal"), v0001_engine.begin() as connection:
        connection.exec_driver_sql(
            "UPDATE runs SET execution_evidence_fingerprint = :digest, "
            "execution_evidence_fingerprint_version = 2 "
            "WHERE run_id = 'run_migration-host'",
            {"digest": DIGEST_A},
        )


def test_new_writes_use_the_new_name_with_explicit_version(tmp_path: Path) -> None:
    engine = create_engine(f"sqlite+pysqlite:///{tmp_path / 'new-writes.db'}")
    try:
        with engine.connect() as connection:
            upgrade_to_head(connection)
            connection.rollback()
        _seed_pipeline(engine)
        with Session(engine) as session, session.begin():
            repository = SqlAlchemyRunRepository(session)
            repository.create(
                run_id=RunId("run_migration-new"),
                pipeline_id=PipelineId("pip_migration-p7-1"),
                pipeline_version=PipelineVersion(1),
                runner_kind="sequential",
                runner_configuration=mapping_null_document(),
                scenario_seed=None,
                node_ids=(NodeId("nod_migration-new"),),
                created_at=UtcTimestamp(datetime(2026, 8, 21, 12, 0, tzinfo=UTC)),
            )
            repository.transition(
                RunId("run_migration-new"),
                expected_row_version=1,
                target_state=RunState.RUNNING,
                transitioned_at=UtcTimestamp(datetime(2026, 8, 21, 12, 1, tzinfo=UTC)),
            )
            finalized = repository.transition(
                RunId("run_migration-new"),
                expected_row_version=2,
                target_state=RunState.SUCCEEDED,
                transitioned_at=UtcTimestamp(datetime(2026, 8, 21, 12, 2, tzinfo=UTC)),
                execution_evidence_fingerprint=StateFingerprint(DIGEST_A),
                execution_evidence_fingerprint_version=2,
            )
        assert finalized.execution_evidence_fingerprint == StateFingerprint(DIGEST_A)
        assert finalized.execution_evidence_fingerprint_version == 2
        with engine.connect() as connection:
            stored = _runs_rows(connection)["run_migration-new"]
            connection.rollback()
        assert stored == ("succeeded", DIGEST_A, 2)
    finally:
        engine.dispose()


def mapping_null_document() -> ConfigurationDocument:
    from paritygrid.application.ports.configuration import ConfigurationDocument as Document

    return Document.from_mapping({})


def _row_mapping(values: dict[str, object]) -> RowMapping:
    return cast(RowMapping, _LightRowMapping(values))


def _mutable_row(values: dict[str, object]) -> RowMapping:
    return cast(RowMapping, values)


class _LightRowMapping(dict[str, object]):
    """Mapping with SQL-row key membership used by the compatibility read."""


def test_compatibility_read_infers_version_2_for_former_storage_name() -> None:
    row = _row_mapping(
        dict(
            _v0001_row(
                "run_migration-window",
                state="succeeded",
                fingerprint=DIGEST_A,
                finished_at=FINISHED_AT,
            )
        )
    )
    record = mapping.run_from_row(row)
    assert record.execution_evidence_fingerprint == StateFingerprint(DIGEST_A)
    assert record.execution_evidence_fingerprint_version == 2

    null_row = _row_mapping(
        dict(
            _v0001_row(
                "run_migration-window-null",
                state="running",
                fingerprint=None,
                finished_at=None,
            )
        )
    )
    null_record = mapping.run_from_row(null_row)
    assert null_record.execution_evidence_fingerprint is None
    assert null_record.execution_evidence_fingerprint_version is None


@pytest.mark.parametrize("version", [1, 3, 99, 0, -2])
def test_unknown_or_mismatched_versions_fail_closed(version: int) -> None:
    row = _row_mapping(
        dict(
            _v0001_row(
                "run_migration-unknown",
                state="succeeded",
                fingerprint=DIGEST_A,
                finished_at=FINISHED_AT,
            )
        )
    )
    values = dict(row)
    values["execution_evidence_fingerprint"] = DIGEST_A
    values["execution_evidence_fingerprint_version"] = version
    row = _mutable_row(values)
    with pytest.raises(ExecutionCorruptionError, match=r"unsupported|corrupt"):
        mapping.run_from_row(row)


def test_migration_is_idempotent_and_downgrade_policy_is_documented(
    v0001_engine: Engine,
) -> None:
    _seed_pipeline(v0001_engine)
    _seed_v0001_runs(
        v0001_engine,
        (
            _v0001_row(
                "run_migration-repeat",
                state="succeeded",
                fingerprint=DIGEST_A,
                finished_at=FINISHED_AT,
            ),
        ),
    )
    with v0001_engine.connect() as connection:
        first = upgrade_to_head(connection)
        after_first = _runs_rows(connection)
        connection.rollback()
        second = upgrade_to_head(connection)
        after_second = _runs_rows(connection)
        connection.rollback()
    assert first.previous_revision == "0001_operational"
    assert second.previous_revision == HEAD_REVISION
    assert after_first == after_second
    assert after_first["run_migration-repeat"] == ("succeeded", DIGEST_A, 2)

    from alembic import command as alembic_command

    v0001_engine.dispose()
    with v0001_engine.connect() as connection:
        config = _migration_config(connection)
        with pytest.raises(RuntimeError, match="restore from backup"):
            alembic_command.downgrade(config, "0001_operational")
        connection.rollback()


def test_current_metadata_matches_upgraded_v0001_storage(
    v0001_engine: Engine,
) -> None:
    _seed_pipeline(v0001_engine)
    _seed_v0001_runs(
        v0001_engine,
        (_v0001_row("run_migration-meta", state="running", fingerprint=None, finished_at=None),),
    )
    with v0001_engine.connect() as connection:
        upgrade_to_head(connection)
        upgraded = connection.exec_driver_sql(
            "SELECT sql FROM sqlite_master WHERE name = 'runs'"
        ).scalar_one()
        connection.rollback()
    reference = create_engine("sqlite://")
    try:
        with reference.begin() as connection:
            metadata.create_all(connection, tables=[runs.metadata.tables["runs"]])
        with reference.connect() as connection:
            expected = connection.exec_driver_sql(
                "SELECT sql FROM sqlite_master WHERE name = 'runs'"
            ).scalar_one()
            connection.rollback()
    finally:
        reference.dispose()
    assert upgraded == expected

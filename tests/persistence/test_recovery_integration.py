# pyright: reportPrivateUsage=false
"""Real SQLite, artifact, and writer integration for startup recovery."""

from __future__ import annotations

import gc
import threading
import warnings
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, cast

import pytest

from paritygrid.adapters.artifacts.manifests import FileSystemArtifactManifestRepository
from paritygrid.adapters.artifacts.writer import (
    ArtifactWriteReceipt,
    FileSystemArtifactWriter,
)
from paritygrid.adapters.persistence import (
    SqlAlchemyIdempotencyRepository,
    SqlAlchemyPipelineRepository,
    SQLiteDatabase,
    SQLiteDatabaseConfig,
    SQLiteRecoveryStateReader,
    SQLiteTransactionalWriter,
    create_session_factory,
    upgrade_to_head,
)
from paritygrid.application.execution import (
    AcquireWorkLeaseRequest,
    RecoveryAmbiguousError,
    RecoveryFindingKind,
    RecoverySettings,
    RecoveryStatus,
    StartupRecoveryScanner,
    WorkLeaseService,
    WorkLeaseSettings,
)
from paritygrid.application.ports.artifacts import ArtifactRelativePath
from paritygrid.application.ports.configuration import ConfigurationDocument
from paritygrid.application.ports.consistency import EventSequence
from paritygrid.application.ports.execution import (
    WorkItemState,
)
from paritygrid.application.ports.writer import (
    EventAppendRequest,
    WriterCommand,
    WriterSettings,
    WriterSubmissionId,
)
from paritygrid.application.writes import (
    BootstrapWork,
    CreateCapturedRun,
    TransitionRun,
)
from paritygrid.domain.execution import RunState
from paritygrid.domain.models import (
    AttemptNumber,
    Duration,
    NodeId,
    PipelineId,
    PipelineVersion,
    RunId,
    UtcTimestamp,
    WorkItemId,
)
from paritygrid.domain.pipeline import PartitionKey

RUN_ID = RunId("run_recov-real")
PIPELINE_ID = PipelineId("pip_recov-real")
NODE_A = NodeId("nod_recov-a")
WORK_A = WorkItemId("wrk_recov-a")
WORK_B = WorkItemId("wrk_recov-b")
RUNNER_KIND = "sequential"


class _Clock:
    def __init__(self, value: UtcTimestamp) -> None:
        self.value = value

    def now(self) -> UtcTimestamp:
        return self.value


def _time(second: int) -> UtcTimestamp:
    return UtcTimestamp(datetime(2026, 8, 17, 10, 0, tzinfo=UTC) + timedelta(seconds=second))


def _event(
    sequence: int,
    kind: str,
    subject: RunId | WorkItemId,
    *,
    second: int = 3,
) -> EventAppendRequest:
    from paritygrid.application.ports.consistency import (
        EventSubjectKind,
        PendingExecutionEvent,
        RedactedDocument,
    )

    subject_kind = EventSubjectKind.RUN if type(subject) is RunId else EventSubjectKind.WORK_ITEM
    return EventAppendRequest(
        EventSequence(sequence),
        sequence,
        PendingExecutionEvent(
            kind,
            _time(second),
            subject_kind,
            subject,
            None,
            1,
            RedactedDocument.from_mapping({"kind": kind}),
        ),
    )


def _submit(writer: SQLiteTransactionalWriter, command: WriterCommand) -> Any:
    return writer.submit(command, timeout_seconds=5.0).result(timeout_seconds=5.0)


def _open_database(path: Path) -> SQLiteDatabase:
    database = SQLiteDatabase.open(SQLiteDatabaseConfig(path))
    with database.engine.connect() as connection:
        upgrade_to_head(connection)
    with database.transaction() as session:
        pipelines = SqlAlchemyPipelineRepository(session)
        pipelines.create(
            pipeline_id=PIPELINE_ID,
            display_name="Recovery pipeline",
            description=None,
            created_at=_time(0),
        )
        pipelines.publish_version(
            pipeline_id=PIPELINE_ID,
            expected_latest_version=None,
            specification=ConfigurationDocument.from_mapping({"nodes": []}),
            planner_format_version=1,
            published_at=_time(0),
        )
    return database


def _writer_for(database: SQLiteDatabase) -> SQLiteTransactionalWriter:
    return SQLiteTransactionalWriter(
        create_session_factory(database.engine),
        settings=WriterSettings(contention_delay_seconds=0.0),
    )


def _start_run(writer: SQLiteTransactionalWriter) -> None:
    _submit(
        writer,
        CreateCapturedRun(
            run_id=RUN_ID,
            pipeline_id=PIPELINE_ID,
            pipeline_version=PipelineVersion(1),
            runner_kind=RUNNER_KIND,
            runner_configuration=ConfigurationDocument(()),
            scenario_seed=None,
            node_ids=(NODE_A,),
            created_at=_time(1),
            event=_event(1, "run_created", RUN_ID),
        ),
    )
    _submit(
        writer,
        TransitionRun(
            run_id=RUN_ID,
            expected_run_row_version=1,
            target_state=RunState.RUNNING,
            transitioned_at=_time(2),
            execution_evidence_fingerprint=None,
            execution_evidence_fingerprint_version=None,
            event=_event(2, "run_started", RUN_ID, second=2),
        ),
    )
    _submit(
        writer,
        BootstrapWork(
            run_id=RUN_ID,
            node_id=NODE_A,
            work_item_id=WORK_A,
            partition_key=PartitionKey("part-recov"),
            input_reference=None,
            created_at=_time(3),
            expected_node_row_version=1,
            expected_run_row_version=2,
            event=_event(3, "work_created", WORK_A),
        ),
    )


def _scanner(
    writer: SQLiteTransactionalWriter,
    database: SQLiteDatabase,
    artifact_root: Path,
    observed: UtcTimestamp,
) -> StartupRecoveryScanner:
    clock = _Clock(observed)
    return StartupRecoveryScanner(
        writer,
        SQLiteRecoveryStateReader(database, artifact_root),
        clock,
        settings=RecoverySettings(5.0, 5.0),
    )


def _commit_artifact(
    database: SQLiteDatabase,
    artifact_root: Path,
    *,
    relative: str = "runs/run/artifact.parquet",
) -> ArtifactWriteReceipt:
    writer = FileSystemArtifactWriter(artifact_root, maximum_bytes=1_048_576)
    receipt = writer.write(ArtifactRelativePath(relative), [b"recovery artifact bytes"])
    from paritygrid.domain.models import ArtifactId

    with database.transaction() as session:
        FileSystemArtifactManifestRepository(session, artifact_root).register(
            artifact_id=ArtifactId("art_recov-1"),
            run_id=RUN_ID,
            node_id=NODE_A,
            partition_key=PartitionKey("part-recov"),
            write_receipt=receipt,
            media_type="application/octet-stream",
            schema_version=1,
            row_count=1,
            created_at=_time(5),
        )
    return receipt


def test_real_expired_lease_recovery_reopens_and_repeats_noop(tmp_path: Path) -> None:
    database = _open_database(tmp_path / "recovery expired %.db")
    artifact_root = tmp_path / "artifacts"
    artifact_root.mkdir()
    writer: SQLiteTransactionalWriter | None = None
    threads_before = threading.active_count()
    try:
        writer = _writer_for(database)
        writer.start()
        _start_run(writer)
        clock = _Clock(_time(10))
        service = WorkLeaseService(
            writer,
            clock,
            settings=WorkLeaseSettings(
                lease_duration=Duration(1_000_000),
                admission_timeout_seconds=5.0,
                result_timeout_seconds=5.0,
            ),
        )
        lease = service.acquire(
            AcquireWorkLeaseRequest(
                run_id=RUN_ID,
                node_id=NODE_A,
                work_item_id=WORK_A,
                expected_attempt_number=AttemptNumber(1),
                expected_work_row_version=1,
                expected_node_row_version=2,
                expected_run_row_version=3,
                lease_owner="recov-owner",
                runner_kind=RUNNER_KIND,
                worker_identity="recov-worker",
                event=_event(4, "work_claimed", WORK_A),
            )
        )
        del lease

        scanner = _scanner(writer, database, artifact_root, _time(30))
        before = scanner.scan(RUN_ID)
        assert before.status is RecoveryStatus.RECOVERABLE
        assert RecoveryFindingKind.WORK_EXPIRED_NO_EFFECT in {f.kind for f in before.findings}
        report = scanner.recover(RUN_ID, correlation_id="recov:real-1")
        assert report.applied == 1
        assert report.after.status is RecoveryStatus.HEALTHY
        assert report.submission_ids == (WriterSubmissionId(5),)

        with database.transaction() as session:
            from paritygrid.adapters.persistence import SqlAlchemyWorkItemRepository

            work = SqlAlchemyWorkItemRepository(session).get(WORK_A)
            assert work is not None
            assert work.state is WorkItemState.RETRY_WAIT
            assert work.lease_owner is None

        repeat = scanner.recover(RUN_ID)
        assert repeat.applied == 0
        assert repeat.before.status is RecoveryStatus.HEALTHY
        closed = writer.close(timeout_seconds=5.0)
        assert closed.drained
        writer = None
    finally:
        if writer is not None:
            writer.close(timeout_seconds=5.0)
        database.close()
    assert threading.active_count() == threads_before

    reopened = SQLiteDatabase.open(SQLiteDatabaseConfig(tmp_path / "recovery expired %.db"))
    try:
        with reopened.engine.connect() as connection:
            assert connection.exec_driver_sql("PRAGMA quick_check").scalar_one() == "ok"
            assert connection.exec_driver_sql("PRAGMA foreign_key_check").first() is None
        second_writer = _writer_for(reopened)
        second_writer.start()
        try:
            second = _scanner(second_writer, reopened, artifact_root, _time(31))
            repeat_scan = second.scan(RUN_ID)
            assert repeat_scan.status is RecoveryStatus.HEALTHY
            assert second.recover(RUN_ID).applied == 0
        finally:
            second_writer.close(timeout_seconds=5.0)
    finally:
        reopened.close()


def test_real_recovery_advances_same_node_frontier_between_expired_work(tmp_path: Path) -> None:
    database = _open_database(tmp_path / "recovery same node.db")
    artifact_root = tmp_path / "artifacts"
    artifact_root.mkdir()
    writer: SQLiteTransactionalWriter | None = None
    try:
        writer = _writer_for(database)
        writer.start()
        _start_run(writer)
        _submit(
            writer,
            BootstrapWork(
                run_id=RUN_ID,
                node_id=NODE_A,
                work_item_id=WORK_B,
                partition_key=PartitionKey("part-recov-b"),
                input_reference=None,
                created_at=_time(4),
                expected_node_row_version=2,
                expected_run_row_version=3,
                event=_event(4, "work_created", WORK_B, second=4),
            ),
        )
        service = WorkLeaseService(
            writer,
            _Clock(_time(10)),
            settings=WorkLeaseSettings(
                lease_duration=Duration(1_000_000),
                admission_timeout_seconds=5.0,
                result_timeout_seconds=5.0,
            ),
        )
        service.acquire(
            AcquireWorkLeaseRequest(
                run_id=RUN_ID,
                node_id=NODE_A,
                work_item_id=WORK_A,
                expected_attempt_number=AttemptNumber(1),
                expected_work_row_version=1,
                expected_node_row_version=3,
                expected_run_row_version=4,
                lease_owner="recov-owner-a",
                runner_kind=RUNNER_KIND,
                worker_identity="recov-worker-a",
                event=_event(5, "work_claimed", WORK_A, second=10),
            )
        )
        service.acquire(
            AcquireWorkLeaseRequest(
                run_id=RUN_ID,
                node_id=NODE_A,
                work_item_id=WORK_B,
                expected_attempt_number=AttemptNumber(1),
                expected_work_row_version=1,
                expected_node_row_version=4,
                expected_run_row_version=5,
                lease_owner="recov-owner-b",
                runner_kind=RUNNER_KIND,
                worker_identity="recov-worker-b",
                event=_event(6, "work_claimed", WORK_B, second=10),
            )
        )

        scanner = _scanner(writer, database, artifact_root, _time(30))
        report = scanner.recover(RUN_ID, correlation_id="recov:same-node")

        assert report.applied == 2
        assert report.after.status is RecoveryStatus.HEALTHY
        with database.transaction() as session:
            from paritygrid.adapters.persistence import SqlAlchemyWorkItemRepository

            repository = SqlAlchemyWorkItemRepository(session)
            recovered_a = repository.get(WORK_A)
            recovered_b = repository.get(WORK_B)
            assert recovered_a is not None
            assert recovered_b is not None
            assert recovered_a.state is WorkItemState.RETRY_WAIT
            assert recovered_b.state is WorkItemState.RETRY_WAIT
    finally:
        if writer is not None:
            writer.close(timeout_seconds=5.0)
        database.close()


def test_real_committed_artifact_without_checkpoint_fails_closed(tmp_path: Path) -> None:
    database = _open_database(tmp_path / "recovery artifact.db")
    artifact_root = tmp_path / "artifacts"
    artifact_root.mkdir()
    writer: SQLiteTransactionalWriter | None = None
    try:
        writer = _writer_for(database)
        writer.start()
        _start_run(writer)
        clock = _Clock(_time(10))
        service = WorkLeaseService(
            writer,
            clock,
            settings=WorkLeaseSettings(
                lease_duration=Duration(1_000_000),
                admission_timeout_seconds=5.0,
                result_timeout_seconds=5.0,
            ),
        )
        service.acquire(
            AcquireWorkLeaseRequest(
                run_id=RUN_ID,
                node_id=NODE_A,
                work_item_id=WORK_A,
                expected_attempt_number=AttemptNumber(1),
                expected_work_row_version=1,
                expected_node_row_version=2,
                expected_run_row_version=3,
                lease_owner="recov-owner",
                runner_kind=RUNNER_KIND,
                worker_identity="recov-worker",
                event=_event(4, "work_claimed", WORK_A),
            )
        )
        _commit_artifact(database, artifact_root)
        scanner = _scanner(writer, database, artifact_root, _time(30))
        scan = scanner.scan(RUN_ID)
        assert scan.status is RecoveryStatus.AMBIGUOUS
        finding = next(
            f
            for f in scan.findings
            if f.kind is RecoveryFindingKind.WORK_EXPIRED_WITH_COMMITTED_ARTIFACT
        )
        assert finding.work_item_id == WORK_A
        assert scan.recoverable_findings == ()
        with pytest.raises(RecoveryAmbiguousError):
            scanner.recover(RUN_ID)
        with database.transaction() as session:
            from paritygrid.adapters.persistence import SqlAlchemyWorkItemRepository

            recovered = SqlAlchemyWorkItemRepository(session).get(WORK_A)
            assert recovered is not None
            assert recovered.state is WorkItemState.RUNNING
        closed = writer.close(timeout_seconds=5.0)
        assert closed.drained
        writer = None
    finally:
        if writer is not None:
            writer.close(timeout_seconds=5.0)
        database.close()


def test_real_changed_artifact_fails_closed_and_is_preserved(tmp_path: Path) -> None:
    database = _open_database(tmp_path / "recovery changed ✓.db")
    artifact_root = tmp_path / "artifacts"
    artifact_root.mkdir()
    writer: SQLiteTransactionalWriter | None = None
    try:
        writer = _writer_for(database)
        writer.start()
        _start_run(writer)
        receipt = _commit_artifact(database, artifact_root)
        target = artifact_root / receipt.relative_path.value
        target.write_bytes(b"tampered content")
        scanner = _scanner(writer, database, artifact_root, _time(30))
        scan = scanner.scan(RUN_ID)
        assert scan.status is RecoveryStatus.AMBIGUOUS
        assert RecoveryFindingKind.INTEGRITY_CHANGED_FILE in {f.kind for f in scan.findings}
        with pytest.raises(RecoveryAmbiguousError):
            scanner.recover(RUN_ID)
        assert target.exists()
        assert target.read_bytes() == b"tampered content"
        closed = writer.close(timeout_seconds=5.0)
        assert closed.drained
        writer = None
    finally:
        if writer is not None:
            writer.close(timeout_seconds=5.0)
        database.close()


def test_real_orphan_artifact_is_reported_not_deleted(tmp_path: Path) -> None:
    database = _open_database(tmp_path / "recovery orphan.db")
    artifact_root = tmp_path / "artifacts"
    artifact_root.mkdir()
    writer: SQLiteTransactionalWriter | None = None
    try:
        writer = _writer_for(database)
        writer.start()
        _start_run(writer)
        orphan = artifact_root / "runs" / "orphan-dir" / "orphan.parquet"
        orphan.parent.mkdir(parents=True)
        orphan.write_bytes(b"orphan")
        scanner = _scanner(writer, database, artifact_root, _time(30))
        scan = scanner.scan(RUN_ID)
        assert scan.status is RecoveryStatus.AMBIGUOUS
        assert RecoveryFindingKind.INTEGRITY_ORPHAN_FILE in {f.kind for f in scan.findings}
        assert orphan.exists()
        closed = writer.close(timeout_seconds=5.0)
        assert closed.drained
        writer = None
    finally:
        if writer is not None:
            writer.close(timeout_seconds=5.0)
        database.close()


def test_real_missing_artifact_fails_closed(tmp_path: Path) -> None:
    database = _open_database(tmp_path / "recovery missing.db")
    artifact_root = tmp_path / "artifacts"
    artifact_root.mkdir()
    writer: SQLiteTransactionalWriter | None = None
    try:
        writer = _writer_for(database)
        writer.start()
        _start_run(writer)
        receipt = _commit_artifact(database, artifact_root)
        (artifact_root / receipt.relative_path.value).unlink()
        scanner = _scanner(writer, database, artifact_root, _time(30))
        scan = scanner.scan(RUN_ID)
        assert scan.status is RecoveryStatus.AMBIGUOUS
        assert RecoveryFindingKind.INTEGRITY_MISSING_FILE in {f.kind for f in scan.findings}
        closed = writer.close(timeout_seconds=5.0)
        assert closed.drained
        writer = None
    finally:
        if writer is not None:
            writer.close(timeout_seconds=5.0)
        database.close()


def test_real_stranded_idempotency_is_reported(tmp_path: Path) -> None:
    database = _open_database(tmp_path / "recovery idempotency.db")
    artifact_root = tmp_path / "artifacts"
    artifact_root.mkdir()
    writer: SQLiteTransactionalWriter | None = None
    try:
        writer = _writer_for(database)
        writer.start()
        _start_run(writer)
        with database.transaction() as session:
            SqlAlchemyIdempotencyRepository(session).begin(
                scope="recovery",
                key="effect-1",
                request=ConfigurationDocument.from_mapping({"effect": 1}),
                started_at=_time(4),
            )
        scanner = _scanner(writer, database, artifact_root, _time(30))
        scan = scanner.scan(RUN_ID)
        assert scan.status is RecoveryStatus.HEALTHY
        finding = next(
            f for f in scan.findings if f.kind is RecoveryFindingKind.STRANDED_IDEMPOTENCY
        )
        assert finding.detail is not None
        closed = writer.close(timeout_seconds=5.0)
        assert closed.drained
        writer = None
    finally:
        if writer is not None:
            writer.close(timeout_seconds=5.0)
        database.close()


def test_real_foreign_key_violation_fails_closed(tmp_path: Path) -> None:
    database = _open_database(tmp_path / "recovery fk.db")
    artifact_root = tmp_path / "artifacts"
    artifact_root.mkdir()
    writer: SQLiteTransactionalWriter | None = None
    try:
        writer = _writer_for(database)
        writer.start()
        _start_run(writer)
        with database.engine.connect() as connection:
            connection.exec_driver_sql("PRAGMA foreign_keys = OFF")
            connection.exec_driver_sql('DROP TRIGGER "trg_work_items_prohibit_delete"')
            connection.exec_driver_sql(
                "DELETE FROM work_items WHERE work_item_id = ?", (str(WORK_A),)
            )
            connection.commit()
        reader = SQLiteRecoveryStateReader(database, artifact_root)
        with pytest.raises(Exception, match="foreign_key_check"):
            reader.read(RUN_ID)
        closed = writer.close(timeout_seconds=5.0)
        assert closed.drained
        writer = None
    finally:
        if writer is not None:
            writer.close(timeout_seconds=5.0)
        database.close()


def test_real_reader_validates_identity_and_types(tmp_path: Path) -> None:
    database = _open_database(tmp_path / "recovery reader.db")
    artifact_root = tmp_path / "artifacts"
    artifact_root.mkdir()
    try:
        reader = SQLiteRecoveryStateReader(database, artifact_root)
        with pytest.raises(TypeError, match="RunId"):
            reader.read(cast(Any, "run_recov-real"))
        with pytest.raises(TypeError, match="SQLiteDatabase"):
            SQLiteRecoveryStateReader(cast(Any, object()), artifact_root)
        with pytest.raises(TypeError, match="Path"):
            SQLiteRecoveryStateReader(database, cast(Any, "not a path"))
    finally:
        database.close()


def test_real_quick_check_failure_fails_closed(tmp_path: Path) -> None:
    database_path = tmp_path / "recovery corrupt.db"
    database = _open_database(database_path)
    artifact_root = tmp_path / "artifacts"
    artifact_root.mkdir()
    writer: SQLiteTransactionalWriter | None = None
    try:
        writer = _writer_for(database)
        writer.start()
        _start_run(writer)
        closed = writer.close(timeout_seconds=5.0)
        assert closed.drained
        writer = None
    finally:
        if writer is not None:
            writer.close(timeout_seconds=5.0)
        database.close()

    with database_path.open("r+b") as handle:
        size = handle.seek(0, 2)
        # Corrupt every page in the back half so the damage cannot land in
        # layout-dependent free space left by the runs rebuild migration.
        for offset in range(size // 2, size - 32, 4096):
            handle.seek(offset)
            handle.write(b"\x00corrupted-page-bytes\x00")
    from paritygrid.adapters.persistence.errors import SQLiteCapabilityError

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always", ResourceWarning)
        with pytest.raises(SQLiteCapabilityError):
            SQLiteDatabase.open(SQLiteDatabaseConfig(database_path))
        gc.collect()
    assert not [warning for warning in caught if warning.category is ResourceWarning]


def test_real_event_gap_is_a_typed_corruption(tmp_path: Path) -> None:
    from paritygrid.adapters.persistence import SQLiteFinalizationStateReader
    from paritygrid.application.ports.consistency import ConsistencyCorruptionError

    database = _open_database(tmp_path / "recovery gap.db")
    artifact_root = tmp_path / "artifacts"
    artifact_root.mkdir()
    writer: SQLiteTransactionalWriter | None = None
    try:
        writer = _writer_for(database)
        writer.start()
        _start_run(writer)
        with database.engine.connect() as connection:
            connection.exec_driver_sql("PRAGMA foreign_keys = OFF")
            connection.exec_driver_sql('DROP TRIGGER "trg_execution_events_prohibit_delete"')
            connection.exec_driver_sql(
                "DELETE FROM execution_events WHERE run_id = ? AND sequence_number = 2",
                (str(RUN_ID),),
            )
            connection.commit()
        reader = SQLiteFinalizationStateReader(database)
        with pytest.raises(ConsistencyCorruptionError, match="contiguous"):
            reader.read(RUN_ID)
        recovery_reader = SQLiteRecoveryStateReader(database, artifact_root)
        with pytest.raises(ConsistencyCorruptionError, match="contiguous"):
            recovery_reader.read(RUN_ID)
        scanner = _scanner(writer, database, artifact_root, _time(30))
        with pytest.raises(Exception, match=r"contiguous|corrupt"):
            scanner.scan(RUN_ID)
        closed = writer.close(timeout_seconds=5.0)
        assert closed.drained
        writer = None
    finally:
        if writer is not None:
            writer.close(timeout_seconds=5.0)
        database.close()


def test_real_integrity_storage_failure_fails_closed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from paritygrid.adapters.artifacts.integrity import FileSystemArtifactIntegrityScanner

    database = _open_database(tmp_path / "recovery storage.db")
    artifact_root = tmp_path / "artifacts"
    artifact_root.mkdir()
    writer: SQLiteTransactionalWriter | None = None
    try:
        writer = _writer_for(database)
        writer.start()
        _start_run(writer)

        def _failing_scan(self: object) -> Any:
            from paritygrid.application.ports.artifact_integrity import (
                ArtifactIntegrityScanStorageError,
            )

            raise ArtifactIntegrityScanStorageError("storage unavailable")

        monkeypatch.setattr(FileSystemArtifactIntegrityScanner, "scan", _failing_scan)
        reader = SQLiteRecoveryStateReader(database, artifact_root)
        with pytest.raises(Exception, match="artifact integrity storage failed"):
            reader.read(RUN_ID)
        monkeypatch.undo()
        assert reader.read(RUN_ID).integrity_issues == ()
        closed = writer.close(timeout_seconds=5.0)
        assert closed.drained
        writer = None
    finally:
        if writer is not None:
            writer.close(timeout_seconds=5.0)
        database.close()


def test_real_reader_pages_manifests_and_idempotency(tmp_path: Path) -> None:
    from paritygrid.domain.models import ArtifactId

    database = _open_database(tmp_path / "recovery paging %.db")
    artifact_root = tmp_path / "artifacts"
    artifact_root.mkdir()
    writer: SQLiteTransactionalWriter | None = None
    try:
        writer = _writer_for(database)
        writer.start()
        _start_run(writer)
        artifact_writer = FileSystemArtifactWriter(artifact_root, maximum_bytes=1_048_576)
        for index in range(101):
            relative = f"runs/run/part-{index:03d}.parquet"
            receipt = artifact_writer.write(
                ArtifactRelativePath(relative), [f"artifact {index}".encode()]
            )
            with database.transaction() as session:
                FileSystemArtifactManifestRepository(session, artifact_root).register(
                    artifact_id=ArtifactId(f"art_recov-{index:03d}"),
                    run_id=RUN_ID,
                    node_id=NODE_A,
                    partition_key=PartitionKey(f"part-{index:03d}"),
                    write_receipt=receipt,
                    media_type="application/octet-stream",
                    schema_version=1,
                    row_count=1,
                    created_at=_time(5),
                )
        with database.transaction() as session:
            idempotency = SqlAlchemyIdempotencyRepository(session)
            for index in range(101):
                idempotency.begin(
                    scope="recovery-paging",
                    key=f"effect-{index:03d}",
                    request=ConfigurationDocument.from_mapping({"effect": index}),
                    started_at=_time(4),
                )
        reader = SQLiteRecoveryStateReader(database, artifact_root)
        evidence = reader.read(RUN_ID)
        assert len(evidence.artifacts) == 101
        assert len(evidence.idempotency_in_progress) == 101
        scanner = _scanner(writer, database, artifact_root, _time(30))
        scan = scanner.scan(RUN_ID)
        assert scan.status is RecoveryStatus.HEALTHY
        stranded = [f for f in scan.findings if f.kind is RecoveryFindingKind.STRANDED_IDEMPOTENCY]
        assert len(stranded) == 101
        closed = writer.close(timeout_seconds=5.0)
        assert closed.drained
        writer = None
    finally:
        if writer is not None:
            writer.close(timeout_seconds=5.0)
        database.close()


def test_real_data_page_corruption_fails_quick_check(tmp_path: Path) -> None:
    database_path = tmp_path / "recovery datapage.db"
    database = _open_database(database_path)
    artifact_root = tmp_path / "artifacts"
    artifact_root.mkdir()
    writer: SQLiteTransactionalWriter | None = None
    try:
        writer = _writer_for(database)
        writer.start()
        _start_run(writer)
        closed = writer.close(timeout_seconds=5.0)
        assert closed.drained
        writer = None
    finally:
        if writer is not None:
            writer.close(timeout_seconds=5.0)
        database.close()

    # Corrupt a leaf b-tree page (page 2) so connection pragmas still succeed
    # while quick_check detects the structural damage.
    with database_path.open("r+b") as handle:
        handle.seek(4096)
        handle.write(bytes(range(1, 33)) * 2)
    reopened = SQLiteDatabase.open(SQLiteDatabaseConfig(database_path))
    try:
        reader = SQLiteRecoveryStateReader(reopened, artifact_root)
        with pytest.raises(Exception, match="quick_check"):
            reader.read(RUN_ID)
    finally:
        reopened.close()

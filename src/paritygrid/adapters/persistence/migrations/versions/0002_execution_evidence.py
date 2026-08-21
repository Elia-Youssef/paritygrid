# ruff: noqa: E501
"""Rename the runs execution-evidence fingerprint storage with an explicit version.

Revision 0002 renames the misleading ``final_reconciliation_fingerprint``
storage on ``runs`` to ``execution_evidence_fingerprint``, adds
``execution_evidence_fingerprint_version``, preserves every existing digest
byte-for-byte, backfills the preserved digests as version 2, and leaves null
fingerprints null without a version. The rename never recomputes a stored
digest.
"""

from alembic import op

revision: str = "0002_execution_evidence"
down_revision: str | None = "0001_operational"
branch_labels = None
depends_on = None

_RUNS_TABLE = "CREATE TABLE runs (\n\trun_id VARCHAR(68) NOT NULL, \n\tpipeline_id VARCHAR(68) NOT NULL, \n\tpipeline_version_number INTEGER NOT NULL, \n\trunner_kind VARCHAR(32) NOT NULL, \n\trunner_configuration_json TEXT NOT NULL, \n\tstate VARCHAR(32) NOT NULL, \n\trow_version INTEGER DEFAULT '1' NOT NULL, \n\tscenario_seed INTEGER, \n\tcreated_at VARCHAR(27) NOT NULL, \n\tstarted_at VARCHAR(27), \n\tfinished_at VARCHAR(27), \n\tcancellation_requested_at VARCHAR(27), \n\trecovery_started_at VARCHAR(27), \n\trecovered_at VARCHAR(27), \n\texecution_evidence_fingerprint VARCHAR(64), \n\texecution_evidence_fingerprint_version INTEGER, \n\tCONSTRAINT pk_runs PRIMARY KEY (run_id), \n\tCONSTRAINT fk_runs_pipeline_id_pipeline_version_number_pipeline_versions FOREIGN KEY(pipeline_id, pipeline_version_number) REFERENCES pipeline_versions (pipeline_id, version_number), \n\tCONSTRAINT ck_runs_run_id_shape CHECK (typeof(run_id) = 'text' AND length(run_id) BETWEEN 7 AND 68 AND substr(run_id, 1, 4) = 'run_' AND substr(run_id, 5) NOT GLOB '*[^a-z0-9-]*' AND substr(run_id, 5) NOT LIKE '-%' AND substr(run_id, -1) <> '-' AND run_id NOT LIKE '%--%'), \n\tCONSTRAINT ck_runs_pipeline_version_number_range CHECK (typeof(pipeline_version_number) = 'integer' AND pipeline_version_number BETWEEN 1 AND 2147483647), \n\tCONSTRAINT ck_runs_runner_kind_size CHECK (typeof(runner_kind) = 'text' AND length(runner_kind) BETWEEN 1 AND 32), \n\tCONSTRAINT ck_runs_runner_configuration_json_object CHECK (typeof(runner_configuration_json) = 'text' AND json_valid(runner_configuration_json) AND json_type(runner_configuration_json) = 'object'), \n\tCONSTRAINT ck_runs_state_values CHECK (state IN ('queued', 'running', 'pausing', 'paused', 'resuming', 'succeeded', 'partially_succeeded', 'failed', 'cancelling', 'cancelled')), \n\tCONSTRAINT ck_runs_row_version_range CHECK (typeof(row_version) = 'integer' AND row_version BETWEEN 1 AND 2147483647), \n\tCONSTRAINT ck_runs_scenario_seed_storage CHECK (scenario_seed IS NULL OR typeof(scenario_seed) = 'integer'), \n\tCONSTRAINT ck_runs_created_at_utc CHECK (typeof(created_at) = 'text' AND length(created_at) = 27 AND substr(created_at, 5, 1) = '-' AND substr(created_at, 8, 1) = '-' AND substr(created_at, 11, 1) = 'T' AND substr(created_at, 14, 1) = ':' AND substr(created_at, 17, 1) = ':' AND substr(created_at, 20, 1) = '.' AND substr(created_at, 27, 1) = 'Z' AND substr(created_at, 1, 4) NOT GLOB '*[^0-9]*' AND substr(created_at, 6, 2) NOT GLOB '*[^0-9]*' AND substr(created_at, 9, 2) NOT GLOB '*[^0-9]*' AND substr(created_at, 12, 2) NOT GLOB '*[^0-9]*' AND substr(created_at, 15, 2) NOT GLOB '*[^0-9]*' AND substr(created_at, 18, 2) NOT GLOB '*[^0-9]*' AND substr(created_at, 21, 6) NOT GLOB '*[^0-9]*' AND substr(created_at, 1, 4) BETWEEN '0001' AND '9999' AND substr(created_at, 6, 2) BETWEEN '01' AND '12' AND substr(created_at, 9, 2) BETWEEN '01' AND '31' AND substr(created_at, 12, 2) BETWEEN '00' AND '23' AND substr(created_at, 15, 2) BETWEEN '00' AND '59' AND substr(created_at, 18, 2) BETWEEN '00' AND '59'), \n\tCONSTRAINT ck_runs_started_at_utc CHECK (started_at IS NULL OR (typeof(started_at) = 'text' AND length(started_at) = 27 AND substr(started_at, 5, 1) = '-' AND substr(started_at, 8, 1) = '-' AND substr(started_at, 11, 1) = 'T' AND substr(started_at, 14, 1) = ':' AND substr(started_at, 17, 1) = ':' AND substr(started_at, 20, 1) = '.' AND substr(started_at, 27, 1) = 'Z' AND substr(started_at, 1, 4) NOT GLOB '*[^0-9]*' AND substr(started_at, 6, 2) NOT GLOB '*[^0-9]*' AND substr(started_at, 9, 2) NOT GLOB '*[^0-9]*' AND substr(started_at, 12, 2) NOT GLOB '*[^0-9]*' AND substr(started_at, 15, 2) NOT GLOB '*[^0-9]*' AND substr(started_at, 18, 2) NOT GLOB '*[^0-9]*' AND substr(started_at, 21, 6) NOT GLOB '*[^0-9]*' AND substr(started_at, 1, 4) BETWEEN '0001' AND '9999' AND substr(started_at, 6, 2) BETWEEN '01' AND '12' AND substr(started_at, 9, 2) BETWEEN '01' AND '31' AND substr(started_at, 12, 2) BETWEEN '00' AND '23' AND substr(started_at, 15, 2) BETWEEN '00' AND '59' AND substr(started_at, 18, 2) BETWEEN '00' AND '59')), \n\tCONSTRAINT ck_runs_finished_at_utc CHECK (finished_at IS NULL OR (typeof(finished_at) = 'text' AND length(finished_at) = 27 AND substr(finished_at, 5, 1) = '-' AND substr(finished_at, 8, 1) = '-' AND substr(finished_at, 11, 1) = 'T' AND substr(finished_at, 14, 1) = ':' AND substr(finished_at, 17, 1) = ':' AND substr(finished_at, 20, 1) = '.' AND substr(finished_at, 27, 1) = 'Z' AND substr(finished_at, 1, 4) NOT GLOB '*[^0-9]*' AND substr(finished_at, 6, 2) NOT GLOB '*[^0-9]*' AND substr(finished_at, 9, 2) NOT GLOB '*[^0-9]*' AND substr(finished_at, 12, 2) NOT GLOB '*[^0-9]*' AND substr(finished_at, 15, 2) NOT GLOB '*[^0-9]*' AND substr(finished_at, 18, 2) NOT GLOB '*[^0-9]*' AND substr(finished_at, 21, 6) NOT GLOB '*[^0-9]*' AND substr(finished_at, 1, 4) BETWEEN '0001' AND '9999' AND substr(finished_at, 6, 2) BETWEEN '01' AND '12' AND substr(finished_at, 9, 2) BETWEEN '01' AND '31' AND substr(finished_at, 12, 2) BETWEEN '00' AND '23' AND substr(finished_at, 15, 2) BETWEEN '00' AND '59' AND substr(finished_at, 18, 2) BETWEEN '00' AND '59')), \n\tCONSTRAINT ck_runs_cancellation_requested_at_utc CHECK (cancellation_requested_at IS NULL OR (typeof(cancellation_requested_at) = 'text' AND length(cancellation_requested_at) = 27 AND substr(cancellation_requested_at, 5, 1) = '-' AND substr(cancellation_requested_at, 8, 1) = '-' AND substr(cancellation_requested_at, 11, 1) = 'T' AND substr(cancellation_requested_at, 14, 1) = ':' AND substr(cancellation_requested_at, 17, 1) = ':' AND substr(cancellation_requested_at, 20, 1) = '.' AND substr(cancellation_requested_at, 27, 1) = 'Z' AND substr(cancellation_requested_at, 1, 4) NOT GLOB '*[^0-9]*' AND substr(cancellation_requested_at, 6, 2) NOT GLOB '*[^0-9]*' AND substr(cancellation_requested_at, 9, 2) NOT GLOB '*[^0-9]*' AND substr(cancellation_requested_at, 12, 2) NOT GLOB '*[^0-9]*' AND substr(cancellation_requested_at, 15, 2) NOT GLOB '*[^0-9]*' AND substr(cancellation_requested_at, 18, 2) NOT GLOB '*[^0-9]*' AND substr(cancellation_requested_at, 21, 6) NOT GLOB '*[^0-9]*' AND substr(cancellation_requested_at, 1, 4) BETWEEN '0001' AND '9999' AND substr(cancellation_requested_at, 6, 2) BETWEEN '01' AND '12' AND substr(cancellation_requested_at, 9, 2) BETWEEN '01' AND '31' AND substr(cancellation_requested_at, 12, 2) BETWEEN '00' AND '23' AND substr(cancellation_requested_at, 15, 2) BETWEEN '00' AND '59' AND substr(cancellation_requested_at, 18, 2) BETWEEN '00' AND '59')), \n\tCONSTRAINT ck_runs_recovery_started_at_utc CHECK (recovery_started_at IS NULL OR (typeof(recovery_started_at) = 'text' AND length(recovery_started_at) = 27 AND substr(recovery_started_at, 5, 1) = '-' AND substr(recovery_started_at, 8, 1) = '-' AND substr(recovery_started_at, 11, 1) = 'T' AND substr(recovery_started_at, 14, 1) = ':' AND substr(recovery_started_at, 17, 1) = ':' AND substr(recovery_started_at, 20, 1) = '.' AND substr(recovery_started_at, 27, 1) = 'Z' AND substr(recovery_started_at, 1, 4) NOT GLOB '*[^0-9]*' AND substr(recovery_started_at, 6, 2) NOT GLOB '*[^0-9]*' AND substr(recovery_started_at, 9, 2) NOT GLOB '*[^0-9]*' AND substr(recovery_started_at, 12, 2) NOT GLOB '*[^0-9]*' AND substr(recovery_started_at, 15, 2) NOT GLOB '*[^0-9]*' AND substr(recovery_started_at, 18, 2) NOT GLOB '*[^0-9]*' AND substr(recovery_started_at, 21, 6) NOT GLOB '*[^0-9]*' AND substr(recovery_started_at, 1, 4) BETWEEN '0001' AND '9999' AND substr(recovery_started_at, 6, 2) BETWEEN '01' AND '12' AND substr(recovery_started_at, 9, 2) BETWEEN '01' AND '31' AND substr(recovery_started_at, 12, 2) BETWEEN '00' AND '23' AND substr(recovery_started_at, 15, 2) BETWEEN '00' AND '59' AND substr(recovery_started_at, 18, 2) BETWEEN '00' AND '59')), \n\tCONSTRAINT ck_runs_recovered_at_utc CHECK (recovered_at IS NULL OR (typeof(recovered_at) = 'text' AND length(recovered_at) = 27 AND substr(recovered_at, 5, 1) = '-' AND substr(recovered_at, 8, 1) = '-' AND substr(recovered_at, 11, 1) = 'T' AND substr(recovered_at, 14, 1) = ':' AND substr(recovered_at, 17, 1) = ':' AND substr(recovered_at, 20, 1) = '.' AND substr(recovered_at, 27, 1) = 'Z' AND substr(recovered_at, 1, 4) NOT GLOB '*[^0-9]*' AND substr(recovered_at, 6, 2) NOT GLOB '*[^0-9]*' AND substr(recovered_at, 9, 2) NOT GLOB '*[^0-9]*' AND substr(recovered_at, 12, 2) NOT GLOB '*[^0-9]*' AND substr(recovered_at, 15, 2) NOT GLOB '*[^0-9]*' AND substr(recovered_at, 18, 2) NOT GLOB '*[^0-9]*' AND substr(recovered_at, 21, 6) NOT GLOB '*[^0-9]*' AND substr(recovered_at, 1, 4) BETWEEN '0001' AND '9999' AND substr(recovered_at, 6, 2) BETWEEN '01' AND '12' AND substr(recovered_at, 9, 2) BETWEEN '01' AND '31' AND substr(recovered_at, 12, 2) BETWEEN '00' AND '23' AND substr(recovered_at, 15, 2) BETWEEN '00' AND '59' AND substr(recovered_at, 18, 2) BETWEEN '00' AND '59')), \n\tCONSTRAINT ck_runs_execution_evidence_fingerprint_shape CHECK (execution_evidence_fingerprint IS NULL OR (typeof(execution_evidence_fingerprint) = 'text' AND length(execution_evidence_fingerprint) = 64 AND execution_evidence_fingerprint NOT GLOB '*[^0-9a-f]*')), \n\tCONSTRAINT ck_runs_execution_evidence_fingerprint_version_range CHECK (execution_evidence_fingerprint_version IS NULL OR (typeof(execution_evidence_fingerprint_version) = 'integer' AND execution_evidence_fingerprint_version BETWEEN 1 AND 2147483647)), \n\tCONSTRAINT ck_runs_execution_evidence_fingerprint_pairing CHECK ((execution_evidence_fingerprint IS NULL AND execution_evidence_fingerprint_version IS NULL) OR (execution_evidence_fingerprint IS NOT NULL AND execution_evidence_fingerprint_version IS NOT NULL)), \n\tCONSTRAINT ck_runs_terminal_finish CHECK (state NOT IN ('succeeded','partially_succeeded','failed','cancelled') OR finished_at IS NOT NULL), \n\tCONSTRAINT ck_runs_execution_evidence_fingerprint_terminal CHECK (execution_evidence_fingerprint IS NULL OR state IN ('succeeded','partially_succeeded')), \n\tCONSTRAINT ck_runs_started_at_order CHECK (started_at IS NULL OR started_at >= created_at), \n\tCONSTRAINT ck_runs_finished_at_order CHECK (finished_at IS NULL OR finished_at >= created_at)\n)"
_RUNS_INDEXES: tuple[str, ...] = (
    "CREATE INDEX ix_runs_created_at ON runs (created_at)",
    "CREATE INDEX ix_runs_pipeline_id_pipeline_version_number ON runs (pipeline_id, pipeline_version_number)",
    "CREATE INDEX ix_runs_state_created_at ON runs (state, created_at)",
)
_RUNS_TRIGGERS: tuple[str, ...] = (
    'CREATE TRIGGER "trg_runs_prohibit_delete" BEFORE DELETE ON "runs" BEGIN SELECT RAISE(ABORT, \'runs does not permit delete\'); END',
    'CREATE TRIGGER "trg_runs_protect_immutable_columns" BEFORE UPDATE ON "runs" WHEN NEW."run_id" IS NOT OLD."run_id" OR NEW."pipeline_id" IS NOT OLD."pipeline_id" OR NEW."pipeline_version_number" IS NOT OLD."pipeline_version_number" OR NEW."runner_kind" IS NOT OLD."runner_kind" OR NEW."runner_configuration_json" IS NOT OLD."runner_configuration_json" OR NEW."scenario_seed" IS NOT OLD."scenario_seed" OR NEW."created_at" IS NOT OLD."created_at" BEGIN SELECT RAISE(ABORT, \'runs immutable columns cannot change\'); END',
)
_STAGING_NAME = "runs_v0002_staging"
_STAGING_PREFIX = f"CREATE TABLE {_STAGING_NAME} ("
_COLUMNS = (
    "run_id, pipeline_id, pipeline_version_number, runner_kind, "
    "runner_configuration_json, state, row_version, scenario_seed, created_at, "
    "started_at, finished_at, cancellation_requested_at, recovery_started_at, "
    "recovered_at, execution_evidence_fingerprint, "
    "execution_evidence_fingerprint_version"
)
_SELECT = (
    "SELECT run_id, pipeline_id, pipeline_version_number, runner_kind, "
    "runner_configuration_json, state, row_version, scenario_seed, created_at, "
    "started_at, finished_at, cancellation_requested_at, recovery_started_at, "
    "recovered_at, final_reconciliation_fingerprint, "
    "CASE WHEN final_reconciliation_fingerprint IS NOT NULL THEN 2 ELSE NULL END "
    'FROM "runs"'
)
_COPY_TO_STAGING = f'INSERT INTO "{_STAGING_NAME}" ({_COLUMNS}) {_SELECT}'
_COPY_FROM_STAGING = f'INSERT INTO "runs" ({_COLUMNS}) SELECT {_COLUMNS} FROM "{_STAGING_NAME}"'


def upgrade() -> None:
    """Rebuild ``runs`` with the execution-evidence storage name and version.

    Rows wait in a staging table while ``runs`` is dropped and recreated from
    the packaged definition, so child foreign keys keep referencing ``runs``
    and the installed table text stays byte-identical to current metadata.
    Digests are copied unchanged, preserved digests are backfilled as version
    2, null digests stay null without a version, and no value is recomputed.
    """
    op.execute(_RUNS_TABLE.replace("CREATE TABLE runs (", _STAGING_PREFIX, 1))
    op.execute(_COPY_TO_STAGING)
    op.execute('DROP TABLE "runs"')
    op.execute(_RUNS_TABLE)
    op.execute(_COPY_FROM_STAGING)
    op.execute(f'DROP TABLE "{_STAGING_NAME}"')
    for statement in (*_RUNS_INDEXES, *_RUNS_TRIGGERS):
        op.execute(statement)


def downgrade() -> None:
    """Reject destructive downgrade before executing schema changes."""
    raise RuntimeError(
        "Downgrading below 0002 would drop execution-evidence fingerprint "
        "versions and cannot preserve accepted evidence; restore from backup."
    )

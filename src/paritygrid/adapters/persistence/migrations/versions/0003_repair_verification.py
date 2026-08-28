# ruff: noqa: E501
"""Add the immutable target-state verification fact table for Phase 11.

One row records one independently observed target state after repair
effects: its own fingerprint kind and version, the expected identity it was
compared against, the verdict, and bounded redacted evidence. Rows are
append-only; triggers installed by this revision reject delete and update.
"""

from alembic import op

revision: str = "0003_repair_verification"
down_revision: str | None = "0002_execution_evidence"
branch_labels = None
depends_on = None

_TABLE = "CREATE TABLE target_state_verifications (\n\tverification_id VARCHAR(68) NOT NULL, \n\trun_id VARCHAR(68) NOT NULL, \n\trepair_plan_id VARCHAR(68), \n\treconciliation_fingerprint VARCHAR(64) NOT NULL, \n\tplan_content_fingerprint VARCHAR(64), \n\tobserved_fingerprint VARCHAR(64) NOT NULL, \n\tobserved_fingerprint_version INTEGER NOT NULL, \n\texpected_fingerprint VARCHAR(64) NOT NULL, \n\tverdict VARCHAR(32) NOT NULL, \n\tobserved_record_count INTEGER NOT NULL, \n\texpected_record_count INTEGER NOT NULL, \n\tobserved_target_version INTEGER NOT NULL, \n\tobserved_at VARCHAR(27) NOT NULL, \n\tdetail_json TEXT NOT NULL, \n\tCONSTRAINT pk_target_state_verifications PRIMARY KEY (verification_id), \n\tCONSTRAINT fk_target_state_verifications_run_id_runs FOREIGN KEY(run_id) REFERENCES runs (run_id), \n\tCONSTRAINT fk_target_state_verifications_run_id_reconciliation_fingerprint_reconciliation_summaries FOREIGN KEY(run_id, reconciliation_fingerprint) REFERENCES reconciliation_summaries (run_id, reconciliation_fingerprint), \n\tCONSTRAINT fk_target_state_verifications_repair_plan_id_repair_plans FOREIGN KEY(repair_plan_id) REFERENCES repair_plans (repair_plan_id), \n\tCONSTRAINT ck_target_state_verifications_verification_id_shape CHECK (typeof(verification_id) = 'text' AND length(verification_id) BETWEEN 7 AND 68 AND substr(verification_id, 1, 4) = 'tgv_' AND substr(verification_id, 5) NOT GLOB '*[^a-z0-9-]*' AND substr(verification_id, 5) NOT LIKE '-%' AND substr(verification_id, -1) <> '-' AND verification_id NOT LIKE '%--%'), \n\tCONSTRAINT ck_target_state_verifications_repair_plan_id_shape CHECK (repair_plan_id IS NULL OR (typeof(repair_plan_id) = 'text' AND length(repair_plan_id) BETWEEN 7 AND 68 AND substr(repair_plan_id, 1, 4) = 'rpl_' AND substr(repair_plan_id, 5) NOT GLOB '*[^a-z0-9-]*' AND substr(repair_plan_id, 5) NOT LIKE '-%' AND substr(repair_plan_id, -1) <> '-' AND repair_plan_id NOT LIKE '%--%')), \n\tCONSTRAINT ck_target_state_verifications_run_id_size CHECK (typeof(run_id) = 'text' AND length(run_id) BETWEEN 1 AND 68), \n\tCONSTRAINT ck_target_state_verifications_reconciliation_fingerprint_shape CHECK (typeof(reconciliation_fingerprint) = 'text' AND length(reconciliation_fingerprint) = 64 AND reconciliation_fingerprint NOT GLOB '*[^0-9a-f]*'), \n\tCONSTRAINT ck_target_state_verifications_plan_content_fingerprint_shape CHECK (plan_content_fingerprint IS NULL OR (typeof(plan_content_fingerprint) = 'text' AND length(plan_content_fingerprint) = 64 AND plan_content_fingerprint NOT GLOB '*[^0-9a-f]*')), \n\tCONSTRAINT ck_target_state_verifications_observed_fingerprint_shape CHECK (typeof(observed_fingerprint) = 'text' AND length(observed_fingerprint) = 64 AND observed_fingerprint NOT GLOB '*[^0-9a-f]*'), \n\tCONSTRAINT ck_target_state_verifications_expected_fingerprint_shape CHECK (typeof(expected_fingerprint) = 'text' AND length(expected_fingerprint) = 64 AND expected_fingerprint NOT GLOB '*[^0-9a-f]*'), \n\tCONSTRAINT ck_target_state_verifications_observed_fingerprint_version_range CHECK (typeof(observed_fingerprint_version) = 'integer' AND observed_fingerprint_version BETWEEN 1 AND 2147483647), \n\tCONSTRAINT ck_target_state_verifications_verdict_values CHECK (verdict IN ('parity_holding', 'parity_divergent', 'observation_failed')), \n\tCONSTRAINT ck_target_state_verifications_observed_record_count_range CHECK (typeof(observed_record_count) = 'integer' AND observed_record_count >= 0), \n\tCONSTRAINT ck_target_state_verifications_expected_record_count_range CHECK (typeof(expected_record_count) = 'integer' AND expected_record_count >= 0), \n\tCONSTRAINT ck_target_state_verifications_observed_target_version_range CHECK (typeof(observed_target_version) = 'integer' AND observed_target_version >= 0), \n\tCONSTRAINT ck_target_state_verifications_observed_at_utc CHECK (typeof(observed_at) = 'text' AND length(observed_at) = 27 AND substr(observed_at, 5, 1) = '-' AND substr(observed_at, 8, 1) = '-' AND substr(observed_at, 11, 1) = 'T' AND substr(observed_at, 14, 1) = ':' AND substr(observed_at, 17, 1) = ':' AND substr(observed_at, 20, 1) = '.' AND substr(observed_at, 27, 1) = 'Z' AND substr(observed_at, 1, 4) NOT GLOB '*[^0-9]*' AND substr(observed_at, 6, 2) NOT GLOB '*[^0-9]*' AND substr(observed_at, 9, 2) NOT GLOB '*[^0-9]*' AND substr(observed_at, 12, 2) NOT GLOB '*[^0-9]*' AND substr(observed_at, 15, 2) NOT GLOB '*[^0-9]*' AND substr(observed_at, 18, 2) NOT GLOB '*[^0-9]*' AND substr(observed_at, 21, 6) NOT GLOB '*[^0-9]*' AND substr(observed_at, 1, 4) BETWEEN '0001' AND '9999' AND substr(observed_at, 6, 2) BETWEEN '01' AND '12' AND substr(observed_at, 9, 2) BETWEEN '01' AND '31' AND substr(observed_at, 12, 2) BETWEEN '00' AND '23' AND substr(observed_at, 15, 2) BETWEEN '00' AND '59' AND substr(observed_at, 18, 2) BETWEEN '00' AND '59'), \n\tCONSTRAINT ck_target_state_verifications_detail_json_object CHECK (typeof(detail_json) = 'text' AND json_valid(detail_json) AND json_type(detail_json) = 'object')\n)"
_INDEXES: tuple[str, ...] = (
    "CREATE INDEX ix_target_state_verifications_run_id_observed_at ON target_state_verifications (run_id, observed_at)",
    "CREATE INDEX ix_target_state_verifications_run_id_reconciliation_fingerprint ON target_state_verifications (run_id, reconciliation_fingerprint)",
    "CREATE INDEX ix_target_state_verifications_repair_plan_id ON target_state_verifications (repair_plan_id)",
)
_TRIGGERS: tuple[str, ...] = (
    'CREATE TRIGGER "trg_target_state_verifications_prohibit_delete" BEFORE DELETE ON "target_state_verifications" BEGIN SELECT RAISE(ABORT, \'target_state_verifications does not permit delete\'); END',
    'CREATE TRIGGER "trg_target_state_verifications_prohibit_update" BEFORE UPDATE ON "target_state_verifications" BEGIN SELECT RAISE(ABORT, \'target_state_verifications does not permit update\'); END',
)

# This is intentionally frozen rather than compiled from current metadata.
# Migrations must remain runnable from a packaged historical revision even if
# application schema definitions are later changed or cannot be imported.
_REPAIR_PLANS_TABLE = "\nCREATE TABLE repair_plans (\n\trepair_plan_id VARCHAR(68) NOT NULL, \n\trun_id VARCHAR(68) NOT NULL, \n\treconciliation_fingerprint VARCHAR(64) NOT NULL, \n\tcontent_fingerprint VARCHAR(64) NOT NULL, \n\tsource_input_identity VARCHAR(64) NOT NULL, \n\ttarget_input_identity VARCHAR(64) NOT NULL, \n\tpolicy_version INTEGER NOT NULL, \n\tgeneration_version INTEGER NOT NULL, \n\trules_version INTEGER NOT NULL, \n\tanalysis_version INTEGER NOT NULL, \n\tanalytical_query_version INTEGER NOT NULL, \n\taction_count INTEGER NOT NULL, \n\tstatus VARCHAR(32) NOT NULL, \n\trow_version INTEGER DEFAULT '1' NOT NULL, \n\tcreated_at VARCHAR(27) NOT NULL, \n\tapplying_at VARCHAR(27), \n\tapplied_at VARCHAR(27), \n\trejected_at VARCHAR(27), \n\tfailed_at VARCHAR(27), \n\tfailure_detail TEXT, \n\tCONSTRAINT pk_repair_plans PRIMARY KEY (repair_plan_id), \n\tCONSTRAINT uq_repair_plans_repair_plan_id_run_id UNIQUE (repair_plan_id, run_id), \n\tCONSTRAINT uq_repair_plans_repair_plan_id_reconciliation_fingerprint UNIQUE (repair_plan_id, reconciliation_fingerprint), \n\tCONSTRAINT uq_repair_plans_run_id_content_fingerprint UNIQUE (run_id, content_fingerprint), \n\tCONSTRAINT fk_repair_plans_run_id_reconciliation_fingerprint_reconciliation_summaries FOREIGN KEY(run_id, reconciliation_fingerprint) REFERENCES reconciliation_summaries (run_id, reconciliation_fingerprint), \n\tCONSTRAINT ck_repair_plans_repair_plan_id_shape CHECK (typeof(repair_plan_id) = 'text' AND length(repair_plan_id) BETWEEN 7 AND 68 AND substr(repair_plan_id, 1, 4) = 'rpl_' AND substr(repair_plan_id, 5) NOT GLOB '*[^a-z0-9-]*' AND substr(repair_plan_id, 5) NOT LIKE '-%' AND substr(repair_plan_id, -1) <> '-' AND repair_plan_id NOT LIKE '%--%'), \n\tCONSTRAINT ck_repair_plans_reconciliation_fingerprint_shape CHECK (typeof(reconciliation_fingerprint) = 'text' AND length(reconciliation_fingerprint) = 64 AND reconciliation_fingerprint NOT GLOB '*[^0-9a-f]*'), \n\tCONSTRAINT ck_repair_plans_content_fingerprint_shape CHECK (typeof(content_fingerprint) = 'text' AND length(content_fingerprint) = 64 AND content_fingerprint NOT GLOB '*[^0-9a-f]*'), \n\tCONSTRAINT ck_repair_plans_source_input_identity_shape CHECK (typeof(source_input_identity) = 'text' AND length(source_input_identity) = 64 AND source_input_identity NOT GLOB '*[^0-9a-f]*'), \n\tCONSTRAINT ck_repair_plans_target_input_identity_shape CHECK (typeof(target_input_identity) = 'text' AND length(target_input_identity) = 64 AND target_input_identity NOT GLOB '*[^0-9a-f]*'), \n\tCONSTRAINT ck_repair_plans_policy_version_range CHECK (typeof(policy_version) = 'integer' AND policy_version BETWEEN 1 AND 2147483647), \n\tCONSTRAINT ck_repair_plans_generation_version_range CHECK (typeof(generation_version) = 'integer' AND generation_version BETWEEN 1 AND 2147483647), \n\tCONSTRAINT ck_repair_plans_rules_version_range CHECK (typeof(rules_version) = 'integer' AND rules_version BETWEEN 1 AND 2147483647), \n\tCONSTRAINT ck_repair_plans_analysis_version_range CHECK (typeof(analysis_version) = 'integer' AND analysis_version BETWEEN 1 AND 2147483647), \n\tCONSTRAINT ck_repair_plans_analytical_query_version_range CHECK (typeof(analytical_query_version) = 'integer' AND analytical_query_version BETWEEN 1 AND 2147483647), \n\tCONSTRAINT ck_repair_plans_action_count_range CHECK (typeof(action_count) = 'integer' AND action_count >= 0), \n\tCONSTRAINT ck_repair_plans_status_values CHECK (status IN ('proposed', 'approved', 'applying', 'applied', 'rejected', 'failed')), \n\tCONSTRAINT ck_repair_plans_row_version_range CHECK (typeof(row_version) = 'integer' AND row_version BETWEEN 1 AND 2147483647), \n\tCONSTRAINT ck_repair_plans_created_at_utc CHECK (typeof(created_at) = 'text' AND length(created_at) = 27 AND substr(created_at, 5, 1) = '-' AND substr(created_at, 8, 1) = '-' AND substr(created_at, 11, 1) = 'T' AND substr(created_at, 14, 1) = ':' AND substr(created_at, 17, 1) = ':' AND substr(created_at, 20, 1) = '.' AND substr(created_at, 27, 1) = 'Z' AND substr(created_at, 1, 4) NOT GLOB '*[^0-9]*' AND substr(created_at, 6, 2) NOT GLOB '*[^0-9]*' AND substr(created_at, 9, 2) NOT GLOB '*[^0-9]*' AND substr(created_at, 12, 2) NOT GLOB '*[^0-9]*' AND substr(created_at, 15, 2) NOT GLOB '*[^0-9]*' AND substr(created_at, 18, 2) NOT GLOB '*[^0-9]*' AND substr(created_at, 21, 6) NOT GLOB '*[^0-9]*' AND substr(created_at, 1, 4) BETWEEN '0001' AND '9999' AND substr(created_at, 6, 2) BETWEEN '01' AND '12' AND substr(created_at, 9, 2) BETWEEN '01' AND '31' AND substr(created_at, 12, 2) BETWEEN '00' AND '23' AND substr(created_at, 15, 2) BETWEEN '00' AND '59' AND substr(created_at, 18, 2) BETWEEN '00' AND '59'), \n\tCONSTRAINT ck_repair_plans_applying_at_utc CHECK (applying_at IS NULL OR (typeof(applying_at) = 'text' AND length(applying_at) = 27 AND substr(applying_at, 5, 1) = '-' AND substr(applying_at, 8, 1) = '-' AND substr(applying_at, 11, 1) = 'T' AND substr(applying_at, 14, 1) = ':' AND substr(applying_at, 17, 1) = ':' AND substr(applying_at, 20, 1) = '.' AND substr(applying_at, 27, 1) = 'Z' AND substr(applying_at, 1, 4) NOT GLOB '*[^0-9]*' AND substr(applying_at, 6, 2) NOT GLOB '*[^0-9]*' AND substr(applying_at, 9, 2) NOT GLOB '*[^0-9]*' AND substr(applying_at, 12, 2) NOT GLOB '*[^0-9]*' AND substr(applying_at, 15, 2) NOT GLOB '*[^0-9]*' AND substr(applying_at, 18, 2) NOT GLOB '*[^0-9]*' AND substr(applying_at, 21, 6) NOT GLOB '*[^0-9]*' AND substr(applying_at, 1, 4) BETWEEN '0001' AND '9999' AND substr(applying_at, 6, 2) BETWEEN '01' AND '12' AND substr(applying_at, 9, 2) BETWEEN '01' AND '31' AND substr(applying_at, 12, 2) BETWEEN '00' AND '23' AND substr(applying_at, 15, 2) BETWEEN '00' AND '59' AND substr(applying_at, 18, 2) BETWEEN '00' AND '59')), \n\tCONSTRAINT ck_repair_plans_applied_at_utc CHECK (applied_at IS NULL OR (typeof(applied_at) = 'text' AND length(applied_at) = 27 AND substr(applied_at, 5, 1) = '-' AND substr(applied_at, 8, 1) = '-' AND substr(applied_at, 11, 1) = 'T' AND substr(applied_at, 14, 1) = ':' AND substr(applied_at, 17, 1) = ':' AND substr(applied_at, 20, 1) = '.' AND substr(applied_at, 27, 1) = 'Z' AND substr(applied_at, 1, 4) NOT GLOB '*[^0-9]*' AND substr(applied_at, 6, 2) NOT GLOB '*[^0-9]*' AND substr(applied_at, 9, 2) NOT GLOB '*[^0-9]*' AND substr(applied_at, 12, 2) NOT GLOB '*[^0-9]*' AND substr(applied_at, 15, 2) NOT GLOB '*[^0-9]*' AND substr(applied_at, 18, 2) NOT GLOB '*[^0-9]*' AND substr(applied_at, 21, 6) NOT GLOB '*[^0-9]*' AND substr(applied_at, 1, 4) BETWEEN '0001' AND '9999' AND substr(applied_at, 6, 2) BETWEEN '01' AND '12' AND substr(applied_at, 9, 2) BETWEEN '01' AND '31' AND substr(applied_at, 12, 2) BETWEEN '00' AND '23' AND substr(applied_at, 15, 2) BETWEEN '00' AND '59' AND substr(applied_at, 18, 2) BETWEEN '00' AND '59')), \n\tCONSTRAINT ck_repair_plans_rejected_at_utc CHECK (rejected_at IS NULL OR (typeof(rejected_at) = 'text' AND length(rejected_at) = 27 AND substr(rejected_at, 5, 1) = '-' AND substr(rejected_at, 8, 1) = '-' AND substr(rejected_at, 11, 1) = 'T' AND substr(rejected_at, 14, 1) = ':' AND substr(rejected_at, 17, 1) = ':' AND substr(rejected_at, 20, 1) = '.' AND substr(rejected_at, 27, 1) = 'Z' AND substr(rejected_at, 1, 4) NOT GLOB '*[^0-9]*' AND substr(rejected_at, 6, 2) NOT GLOB '*[^0-9]*' AND substr(rejected_at, 9, 2) NOT GLOB '*[^0-9]*' AND substr(rejected_at, 12, 2) NOT GLOB '*[^0-9]*' AND substr(rejected_at, 15, 2) NOT GLOB '*[^0-9]*' AND substr(rejected_at, 18, 2) NOT GLOB '*[^0-9]*' AND substr(rejected_at, 21, 6) NOT GLOB '*[^0-9]*' AND substr(rejected_at, 1, 4) BETWEEN '0001' AND '9999' AND substr(rejected_at, 6, 2) BETWEEN '01' AND '12' AND substr(rejected_at, 9, 2) BETWEEN '01' AND '31' AND substr(rejected_at, 12, 2) BETWEEN '00' AND '23' AND substr(rejected_at, 15, 2) BETWEEN '00' AND '59' AND substr(rejected_at, 18, 2) BETWEEN '00' AND '59')), \n\tCONSTRAINT ck_repair_plans_failed_at_utc CHECK (failed_at IS NULL OR (typeof(failed_at) = 'text' AND length(failed_at) = 27 AND substr(failed_at, 5, 1) = '-' AND substr(failed_at, 8, 1) = '-' AND substr(failed_at, 11, 1) = 'T' AND substr(failed_at, 14, 1) = ':' AND substr(failed_at, 17, 1) = ':' AND substr(failed_at, 20, 1) = '.' AND substr(failed_at, 27, 1) = 'Z' AND substr(failed_at, 1, 4) NOT GLOB '*[^0-9]*' AND substr(failed_at, 6, 2) NOT GLOB '*[^0-9]*' AND substr(failed_at, 9, 2) NOT GLOB '*[^0-9]*' AND substr(failed_at, 12, 2) NOT GLOB '*[^0-9]*' AND substr(failed_at, 15, 2) NOT GLOB '*[^0-9]*' AND substr(failed_at, 18, 2) NOT GLOB '*[^0-9]*' AND substr(failed_at, 21, 6) NOT GLOB '*[^0-9]*' AND substr(failed_at, 1, 4) BETWEEN '0001' AND '9999' AND substr(failed_at, 6, 2) BETWEEN '01' AND '12' AND substr(failed_at, 9, 2) BETWEEN '01' AND '31' AND substr(failed_at, 12, 2) BETWEEN '00' AND '23' AND substr(failed_at, 15, 2) BETWEEN '00' AND '59' AND substr(failed_at, 18, 2) BETWEEN '00' AND '59')), \n\tCONSTRAINT ck_repair_plans_applied_time CHECK (status <> 'applied' OR applied_at IS NOT NULL), \n\tCONSTRAINT ck_repair_plans_rejected_time CHECK (status <> 'rejected' OR rejected_at IS NOT NULL), \n\tCONSTRAINT ck_repair_plans_failed_time CHECK (status <> 'failed' OR failed_at IS NOT NULL), \n\tCONSTRAINT ck_repair_plans_failure_detail_size CHECK (failure_detail IS NULL OR (typeof(failure_detail) = 'text' AND length(failure_detail) BETWEEN 1 AND 4096))\n)\n\n"
_REPAIR_PLANS_STAGING_NAME = "repair_plans_phase11"
_REPAIR_PLANS_STAGING_PREFIX = f"CREATE TABLE {_REPAIR_PLANS_STAGING_NAME} ("
_REPAIR_PLANS_COLUMNS = (
    "repair_plan_id, run_id, reconciliation_fingerprint, content_fingerprint, "
    "source_input_identity, target_input_identity, policy_version, generation_version, "
    "rules_version, analysis_version, analytical_query_version, action_count, status, "
    "row_version, created_at, applying_at, applied_at, rejected_at, failed_at, failure_detail"
)
_REPAIR_PLAN_ADDED_COLUMNS: tuple[str, ...] = (
    "ALTER TABLE repair_plans ADD COLUMN source_input_identity VARCHAR(64)",
    "ALTER TABLE repair_plans ADD COLUMN target_input_identity VARCHAR(64)",
    "ALTER TABLE repair_plans ADD COLUMN policy_version INTEGER",
    "ALTER TABLE repair_plans ADD COLUMN generation_version INTEGER",
    "ALTER TABLE repair_plans ADD COLUMN rules_version INTEGER",
    "ALTER TABLE repair_plans ADD COLUMN analysis_version INTEGER",
    "ALTER TABLE repair_plans ADD COLUMN analytical_query_version INTEGER",
    "ALTER TABLE repair_plans ADD COLUMN action_count INTEGER",
)
_REPAIR_PLAN_BACKFILL = (
    "UPDATE repair_plans AS p SET "
    "source_input_identity=(SELECT source_fingerprint FROM reconciliation_summaries s "
    "WHERE s.run_id=p.run_id AND s.reconciliation_fingerprint=p.reconciliation_fingerprint), "
    "target_input_identity=(SELECT target_fingerprint FROM reconciliation_summaries s "
    "WHERE s.run_id=p.run_id AND s.reconciliation_fingerprint=p.reconciliation_fingerprint), "
    "policy_version=1, generation_version=1, rules_version=1, analysis_version=1, "
    "action_count=(SELECT COUNT(*) FROM repair_actions a WHERE a.repair_plan_id=p.repair_plan_id), "
    "analytical_query_version=(SELECT analytical_query_version FROM reconciliation_summaries s "
    "WHERE s.run_id=p.run_id AND s.reconciliation_fingerprint=p.reconciliation_fingerprint)"
)
_COPY_REPAIR_PLANS_TO_STAGING = (
    f"INSERT INTO {_REPAIR_PLANS_STAGING_NAME} ({_REPAIR_PLANS_COLUMNS}) "
    f"SELECT {_REPAIR_PLANS_COLUMNS} FROM repair_plans"
)
_COPY_REPAIR_PLANS_FROM_STAGING = (
    f"INSERT INTO repair_plans ({_REPAIR_PLANS_COLUMNS}) "
    f"SELECT {_REPAIR_PLANS_COLUMNS} FROM {_REPAIR_PLANS_STAGING_NAME}"
)
_REPAIR_PLANS_INDEX = (
    "CREATE INDEX ix_repair_plans_run_id_reconciliation_fingerprint "
    "ON repair_plans (run_id, reconciliation_fingerprint)"
)
_REPAIR_PLANS_TRIGGERS: tuple[str, ...] = (
    'CREATE TRIGGER "trg_repair_plans_prohibit_delete" BEFORE DELETE ON "repair_plans" '
    "BEGIN SELECT RAISE(ABORT, 'repair_plans does not permit delete'); END",
    'CREATE TRIGGER "trg_repair_plans_protect_immutable_columns" BEFORE UPDATE ON "repair_plans" '
    'WHEN NEW."repair_plan_id" IS NOT OLD."repair_plan_id" OR NEW."run_id" IS NOT OLD."run_id" '
    'OR NEW."reconciliation_fingerprint" IS NOT OLD."reconciliation_fingerprint" '
    'OR NEW."content_fingerprint" IS NOT OLD."content_fingerprint" '
    'OR NEW."source_input_identity" IS NOT OLD."source_input_identity" '
    'OR NEW."target_input_identity" IS NOT OLD."target_input_identity" '
    'OR NEW."policy_version" IS NOT OLD."policy_version" '
    'OR NEW."generation_version" IS NOT OLD."generation_version" '
    'OR NEW."rules_version" IS NOT OLD."rules_version" '
    'OR NEW."analysis_version" IS NOT OLD."analysis_version" '
    'OR NEW."analytical_query_version" IS NOT OLD."analytical_query_version" '
    'OR NEW."action_count" IS NOT OLD."action_count" '
    'OR NEW."created_at" IS NOT OLD."created_at" '
    "BEGIN SELECT RAISE(ABORT, 'repair_plans immutable columns cannot change'); END",
    'CREATE TRIGGER "trg_repair_plans_protect_terminal_status" BEFORE UPDATE ON "repair_plans" '
    "WHEN OLD.\"status\" IN ('applied', 'rejected', 'failed') "
    "BEGIN SELECT RAISE(ABORT, 'repair_plans terminal rows cannot change'); END",
)


def upgrade() -> None:
    """Rebuild repair plans and create append-only verification evidence."""
    # Phase 3's historical repair rows predate explicit generation binding.
    # Backfill their immutable reconciliation parents before new Phase 11
    # writes make every binding field mandatory at the repository boundary.
    for statement in _REPAIR_PLAN_ADDED_COLUMNS:
        op.execute(statement)
    op.execute('DROP TRIGGER IF EXISTS "trg_repair_plans_protect_terminal_status"')
    op.execute(_REPAIR_PLAN_BACKFILL)
    op.execute(
        _REPAIR_PLANS_TABLE.replace("CREATE TABLE repair_plans (", _REPAIR_PLANS_STAGING_PREFIX, 1)
    )
    op.execute(_COPY_REPAIR_PLANS_TO_STAGING)
    op.execute("DROP TABLE repair_plans")
    op.execute(_REPAIR_PLANS_TABLE)
    op.execute(_COPY_REPAIR_PLANS_FROM_STAGING)
    op.execute(f"DROP TABLE {_REPAIR_PLANS_STAGING_NAME}")
    op.execute(_REPAIR_PLANS_INDEX)
    for statement in _REPAIR_PLANS_TRIGGERS:
        op.execute(statement)
    op.execute(_TABLE)
    for statement in (*_INDEXES, *_TRIGGERS):
        op.execute(statement)


def downgrade() -> None:
    """Reject destructive downgrade before executing schema changes."""
    raise RuntimeError(
        "Downgrading below 0003 would drop immutable target-state "
        "verification evidence; restore from backup."
    )

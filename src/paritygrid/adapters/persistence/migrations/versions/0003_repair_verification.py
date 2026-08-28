# ruff: noqa: E501
"""Add the immutable target-state verification fact table for Phase 11.

One row records one independently observed target state after repair
effects: its own fingerprint kind and version, the expected identity it was
compared against, the verdict, and bounded redacted evidence. Rows are
append-only; triggers installed by this revision reject delete and update.
"""

import sqlalchemy as sa
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


def upgrade() -> None:
    """Create the append-only verification table with its guards."""
    # Phase 3's historical repair rows predate explicit generation binding.
    # Backfill their immutable reconciliation parents before new Phase 11
    # writes make every binding field mandatory at the repository boundary.
    for name, type_ in (
        ("source_input_identity", sa.String(length=64)),
        ("target_input_identity", sa.String(length=64)),
        ("policy_version", sa.Integer()),
        ("generation_version", sa.Integer()),
        ("rules_version", sa.Integer()),
        ("analysis_version", sa.Integer()),
        ("analytical_query_version", sa.Integer()),
        ("action_count", sa.Integer()),
    ):
        op.add_column("repair_plans", sa.Column(name, type_, nullable=True))
    op.execute(
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
    op.execute('DROP TRIGGER IF EXISTS "trg_repair_plans_protect_immutable_columns"')
    op.execute(
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
        "BEGIN SELECT RAISE(ABORT, 'repair_plans immutable columns cannot change'); END"
    )
    op.execute(_TABLE)
    for statement in (*_INDEXES, *_TRIGGERS):
        op.execute(statement)


def downgrade() -> None:
    """Reject destructive downgrade before executing schema changes."""
    raise RuntimeError(
        "Downgrading below 0003 would drop immutable target-state "
        "verification evidence; restore from backup."
    )

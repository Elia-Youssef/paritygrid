"""Atomic, connection-owned Alembic migration execution."""

import hashlib
from dataclasses import dataclass
from typing import cast

from alembic import command
from alembic.config import Config
from alembic.migration import MigrationContext
from alembic.script import ScriptDirectory
from alembic.util.exc import CommandError
from sqlalchemy.engine import Connection
from sqlalchemy.exc import SQLAlchemyError

from paritygrid.adapters.persistence.errors import (
    MigrationConfigurationError,
    MigrationExecutionError,
    MigrationIntegrityError,
)

HEAD_REVISION = "0001_operational"
_SCRIPT_LOCATION = "paritygrid.adapters.persistence:migrations"
_EXPECTED_TABLE_NAMES = frozenset(
    {
        "artifact_manifests",
        "audit_entries",
        "checkpoint_heads",
        "checkpoints",
        "connector_secret_references",
        "connectors",
        "execution_events",
        "idempotency_records",
        "pipeline_versions",
        "pipelines",
        "reconciliation_conflicts",
        "reconciliation_summaries",
        "repair_actions",
        "repair_approvals",
        "repair_plans",
        "run_event_counters",
        "run_nodes",
        "runs",
        "system_metadata",
        "work_attempts",
        "work_items",
    }
)
_EXPECTED_SCHEMA_HASHES: dict[tuple[str, str, str], str] = {
    (
        "index",
        "ix_artifact_manifests_run_id_node_id",
        "artifact_manifests",
    ): "a1f99c7290e27d9431c47ead4e027ee69dc643051935d2dc352edb1e72cb7ec7",
    (
        "index",
        "ix_artifact_manifests_sha256",
        "artifact_manifests",
    ): "ecf401d4b184c62d10ef262589a50f677778c39fe85ce9368f3187ddc86c3556",
    (
        "index",
        "ix_audit_entries_correlation_id",
        "audit_entries",
    ): "8b5b928384ec65142ce5deee6a7e7719fd79065f86fac7c3d4cc1fb205bc09aa",
    (
        "index",
        "ix_audit_entries_object_kind_object_id",
        "audit_entries",
    ): "a47b71575cf1aeda2973557542d1d8d9ff141b3df742c2645ffc83f70ca003b8",
    (
        "index",
        "ix_audit_entries_occurred_at",
        "audit_entries",
    ): "82e8fb08570cdc3f2520746871338808c4c6fb48238499e274f0d50be4d87e2d",
    (
        "index",
        "ix_checkpoints_artifact_id_run_id_node_id_partition_key",
        "checkpoints",
    ): "6765f65e9301eb77fc4d4e0f64a36b8c2f36d1c530fb35fa65705cfc2599a53e",
    (
        "index",
        "ix_connectors_archived_at",
        "connectors",
    ): "bf26bbc1b391d09e92fcaece2b4118a4f5c69a6cc1657c01e6a3d1b92cdb3d4d",
    (
        "index",
        "ix_connectors_kind",
        "connectors",
    ): "999a106013acb12c225127183b09399bfa0abd0dd8762adbde60327395253883",
    (
        "index",
        "ix_execution_events_correlation_id",
        "execution_events",
    ): "b906632af7ff978cc257e33714c5fa418f7ab2f3d73dde086790c9ca541c0a45",
    (
        "index",
        "ix_execution_events_occurred_at",
        "execution_events",
    ): "bf84ff780c9cc89362dce7f1b33a965e801c7e0f596639946b38c79dc74a906d",
    (
        "index",
        "ix_idempotency_records_status_created_at",
        "idempotency_records",
    ): "7548e25ea7bdedb4320a8753de4ed0b8f42088afce06323d34d354e408cddd43",
    (
        "index",
        "ix_pipeline_versions_published_at",
        "pipeline_versions",
    ): "9fcd0c61821e5d2a97df1ffba20ff8e673343525ac71250ea3da26d47e8d1aeb",
    (
        "index",
        "ix_pipelines_archived_at",
        "pipelines",
    ): "969d74e687aecdc84034b56ec970565611bf06f06ca61edf45f3bdb20b274a8b",
    (
        "index",
        "ix_pipelines_display_name",
        "pipelines",
    ): "3c5a15311bb3e42d6c9cd0b6382ab85758fc1881af946a52f92d59495e6fc38a",
    (
        "index",
        "ix_reconciliation_conflicts_run_id_classification_canonical_key",
        "reconciliation_conflicts",
    ): "dcd99e4d7c99709b8a6bb605510d8d52ddeb95c5460e926054e522e5bbe1ccba",
    (
        "index",
        "ix_repair_actions_conflict_id_run_id_canonical_key",
        "repair_actions",
    ): "2839277d069439c832def98d00d50ef5fc8f976410be740cfd2ee61fe8afca3c",
    (
        "index",
        "ix_repair_actions_repair_plan_id_run_id",
        "repair_actions",
    ): "116f1f483303ac8ce4738e31bc3544a74c8689b8b470c0f77c05ed81fca7a81c",
    (
        "index",
        "ix_repair_approvals_repair_plan_id_reconciliation_fingerprint",
        "repair_approvals",
    ): "bf8583ada4d70bb31caf6f81978911f917f11f7b576716074c1629bfb65a4d90",
    (
        "index",
        "ix_repair_plans_run_id_reconciliation_fingerprint",
        "repair_plans",
    ): "4eba11830a70dfe9614400920678f4d51d306a60d31213def1b505654f145d05",
    (
        "index",
        "ix_run_nodes_run_id_state",
        "run_nodes",
    ): "4105aeafcbd58e13ca87f134b5e7cd74c1ab014b5ef831d08fb6e91b3d0d2c72",
    (
        "index",
        "ix_runs_created_at",
        "runs",
    ): "38875ccc7849561e9e0a25a6b3e747a23adfd1c1ad3e1af0aa881990ac432089",
    (
        "index",
        "ix_runs_pipeline_id_pipeline_version_number",
        "runs",
    ): "2c48c048648872526fe4e743c350d2b13585fb8d1f765e089c9b1e5a6f16efd9",
    (
        "index",
        "ix_runs_state_created_at",
        "runs",
    ): "2f8eba1c6b9f588c4eb60b03b5d7348facc253c604d5ee80769f9f8cfc04742d",
    (
        "index",
        "ix_work_attempts_failure_classification",
        "work_attempts",
    ): "f9c393b6806bed113703a7dff0a1d826ef8cd846eaf2e127d2c580a1ba0d60fe",
    (
        "index",
        "ix_work_attempts_finished_at",
        "work_attempts",
    ): "7f3c8fd29fea2d52fbcd624f73fe9b2ad6b59e352a57362b62bd26ad4ebb0059",
    (
        "index",
        "ix_work_items_lease_expires_at",
        "work_items",
    ): "93de29a5cfa0e3839c25d79ecf68100db97172358b97a336bcf808191d4152c5",
    (
        "index",
        "ix_work_items_run_id_state_retry_available_at",
        "work_items",
    ): "c38fd8544361739199a7a5e232963a4a4e5cfe11be5319c3b01b1efd08b0456e",
    (
        "table",
        "artifact_manifests",
        "artifact_manifests",
    ): "8e4b7cf48bde93ad666fc3d3ffad3f774fcaf8b274ee9806f45e90b65a1916a9",
    (
        "table",
        "audit_entries",
        "audit_entries",
    ): "29c07b23cf752c1e3c65a4a53d9310604065d84347433930464b84ec560ec762",
    (
        "table",
        "checkpoint_heads",
        "checkpoint_heads",
    ): "ee16c606a8f27dbfb7bbbf34189365140c9c10e76106e8cad108083b72fa1471",
    (
        "table",
        "checkpoints",
        "checkpoints",
    ): "8756bbcf190715e69806a4fa7caed5652bb8f7dfd6feff65be26684ce40acbad",
    (
        "table",
        "connector_secret_references",
        "connector_secret_references",
    ): "c7b8d260a832cb6b0bef8664f4132b38e88c24d762507bbfcaf39d8f49c37c0e",
    (
        "table",
        "connectors",
        "connectors",
    ): "e2f38b7c6397cc96dd372a939fccaf700ee9b7c8d086184271c8fb30bc6d1bbe",
    (
        "table",
        "execution_events",
        "execution_events",
    ): "38cd6af8667eb22daec51b0a6b4f226addf6cfb29c7107fb1e5484a075eaf41b",
    (
        "table",
        "idempotency_records",
        "idempotency_records",
    ): "214652216f3d8a7a87e53388482f62e80d7de3472936601c62de9913066ee37a",
    (
        "table",
        "pipeline_versions",
        "pipeline_versions",
    ): "6ad3a98ad204da611512536f60b2421f38834a760e03a094a0afd98cbee3c28e",
    (
        "table",
        "pipelines",
        "pipelines",
    ): "1d01b42be54bdcd9f5099f624c59a064842a81b382272d6bc8b3b3eed9eb6ec3",
    (
        "table",
        "reconciliation_conflicts",
        "reconciliation_conflicts",
    ): "0fd3a54423f5019c7d98259526e28b389983a284199a640830665f73b7f3a05a",
    (
        "table",
        "reconciliation_summaries",
        "reconciliation_summaries",
    ): "c7cefc03d98d97e8987a4f1b278ced2ad2dd93dbfaafd43947d8fca6b93f2e4c",
    (
        "table",
        "repair_actions",
        "repair_actions",
    ): "eeba8001008e8067eeef991bef345d64a2bdf24270375ae56f3d1156ad334bc5",
    (
        "table",
        "repair_approvals",
        "repair_approvals",
    ): "a56b5ec4a97aa6d62a3a3fee700a615d00b7f2d0e3e1a9c7a88c2806a3a91b1d",
    (
        "table",
        "repair_plans",
        "repair_plans",
    ): "17b1de1e714e0c99707ade4d9b5b78587e2c119fb957841b45bcef3e560955ae",
    (
        "table",
        "run_event_counters",
        "run_event_counters",
    ): "d69992248ff8fda213ccb0a6f994420a899a4456af9e4b9a39c9ecf0cf03845e",
    (
        "table",
        "run_nodes",
        "run_nodes",
    ): "37990cbaa5f53c9bed65af0096cec9e5a26f7551ae3dbe5972e50ff43a378737",
    ("table", "runs", "runs"): "e1386f26ca6df409bafb594300b33ed2d3abc3aead8bc9f1e56186a8af8a4b64",
    (
        "table",
        "system_metadata",
        "system_metadata",
    ): "4595b61ade66b3d6156c1f655fb17f3eeae02f9c0782987f3cd1a7e199cd5b7e",
    (
        "table",
        "work_attempts",
        "work_attempts",
    ): "6eded11ff01a18e53b43f7f377fc33219d9fface198a7dc7b92dc43c61843d6f",
    (
        "table",
        "work_items",
        "work_items",
    ): "013050e1128b646ed53e37d75f779beb43160e91051d8d2939afd825d5805abb",
    (
        "trigger",
        "trg_artifact_manifests_prohibit_delete",
        "artifact_manifests",
    ): "a2c413960a0038e534d7439d00ba7e28886cfb7bc17a826a23b548673edd8b34",
    (
        "trigger",
        "trg_artifact_manifests_prohibit_update",
        "artifact_manifests",
    ): "9a4f5babb85c1e2de77b2af17ce66fd0b5f895030b03a5f7d74678f30ac0c97b",
    (
        "trigger",
        "trg_audit_entries_prohibit_delete",
        "audit_entries",
    ): "4bc7a0b664456fe02ace32712a7e38036e941f6c227e6fa19b95ce7b63d7e400",
    (
        "trigger",
        "trg_audit_entries_prohibit_update",
        "audit_entries",
    ): "2659206f60a5d646b785b8c0731abc7dabcda684a5257118c37e9488235a6207",
    (
        "trigger",
        "trg_checkpoint_heads_current_version_must_increase",
        "checkpoint_heads",
    ): "edd258eddc80e5950c26d5c6e0ec56e74b2a5d42800896a054b2b929206b2d61",
    (
        "trigger",
        "trg_checkpoint_heads_prohibit_delete",
        "checkpoint_heads",
    ): "3e44222a529261958222993912e84f71506925f3ab8d57a01d6e908145a1fb55",
    (
        "trigger",
        "trg_checkpoint_heads_protect_immutable_columns",
        "checkpoint_heads",
    ): "ac511db102662c9fc6b13c049c6ee1420defee56c0841808ce24d2001db7c364",
    (
        "trigger",
        "trg_checkpoints_prohibit_delete",
        "checkpoints",
    ): "26511afa6579ac3d30e989edef9eee73518ede7990d6017d9f6c6de7c527321c",
    (
        "trigger",
        "trg_checkpoints_prohibit_update",
        "checkpoints",
    ): "a2029791f01a9fef2103e4b49e3ccf8240f0523d5cebe3ee5a67c7044cdb82f6",
    (
        "trigger",
        "trg_connector_secret_references_prohibit_delete",
        "connector_secret_references",
    ): "cb40b0c49a6795b4ae18a69084b6469e065b51e40d8ae0ad7a6d88c84c0a97c7",
    (
        "trigger",
        "trg_connector_secret_references_prohibit_update",
        "connector_secret_references",
    ): "827c664151c4fd9c3f5d34885a0327bb48db49b30f477ed67d0620438a6af240",
    (
        "trigger",
        "trg_connectors_prohibit_delete",
        "connectors",
    ): "185e9712f2c92adafe84144f0b40291a9bd1ff97e2c15845a85e14a56a939db5",
    (
        "trigger",
        "trg_connectors_protect_immutable_columns",
        "connectors",
    ): "69f8a531761d622a4cecc510bf6b4e65464e8883c41c61bcb1bdb36f9cbd2af8",
    (
        "trigger",
        "trg_execution_events_prohibit_delete",
        "execution_events",
    ): "f26fb75d558f84c8655465e050031c3b5fb6a847db97a71784bcc3d2d43bebe7",
    (
        "trigger",
        "trg_execution_events_prohibit_update",
        "execution_events",
    ): "24a480a03701c0b9adc2a28351a62c20791c72660eeb939b4ad0bf9034553d1d",
    (
        "trigger",
        "trg_idempotency_records_prohibit_delete",
        "idempotency_records",
    ): "1178263ae133907fa02f50d73ed85a7c9e3eae6acf16286badd9d0a347afd333",
    (
        "trigger",
        "trg_idempotency_records_protect_immutable_columns",
        "idempotency_records",
    ): "7f00cfdb909066b98fefd887aaa5e170e49fd10d4fd0f87761f0c33ed9e91808",
    (
        "trigger",
        "trg_idempotency_records_protect_terminal_status",
        "idempotency_records",
    ): "129f460a061ddd5f3c91c26b42b5916975759cbda3cb5cc96afd41f188aa534e",
    (
        "trigger",
        "trg_pipeline_versions_prohibit_delete",
        "pipeline_versions",
    ): "8950ad740456ed56f185ce4b7a88f393629c4f9a1176445569075de3d73326ab",
    (
        "trigger",
        "trg_pipeline_versions_prohibit_update",
        "pipeline_versions",
    ): "5d4074bf814d7ea1bd75e40a6d110da0c54c5cd7d6a5a411f4b35f53204faef6",
    (
        "trigger",
        "trg_pipelines_prohibit_delete",
        "pipelines",
    ): "5eb10ae08b4d608449abe1f22c3deceed5bcb65f1e68f36800065399d06ddb39",
    (
        "trigger",
        "trg_pipelines_protect_immutable_columns",
        "pipelines",
    ): "0abed1c98f563705279f811b957ba6ffd67660d438aa6a513278f2a9e26b16e2",
    (
        "trigger",
        "trg_reconciliation_conflicts_prohibit_delete",
        "reconciliation_conflicts",
    ): "a4c539fe4556099edf59b8ec58263d4add06dba3386c6b03c456c4a329a4c2d5",
    (
        "trigger",
        "trg_reconciliation_conflicts_prohibit_update",
        "reconciliation_conflicts",
    ): "d87b39651acfc54728e0a731abae4010732bd561e9d0a73ca07fe7ec86c0a49e",
    (
        "trigger",
        "trg_reconciliation_summaries_prohibit_delete",
        "reconciliation_summaries",
    ): "559237ae8e61cd8c09238d4513541287a79013795a1d0c6b9a0cea0183279301",
    (
        "trigger",
        "trg_reconciliation_summaries_prohibit_update",
        "reconciliation_summaries",
    ): "9e50e09f706376076eaf25fc338032c3fd63eaa2c9e925b4030bba375a65dca6",
    (
        "trigger",
        "trg_repair_actions_prohibit_delete",
        "repair_actions",
    ): "4e0c3e3de791e1b2160a8de8c4c0c41cc2015a21fdbc4868ee3655c5aa9953ab",
    (
        "trigger",
        "trg_repair_actions_protect_immutable_columns",
        "repair_actions",
    ): "4aed219c3413faf63fbf9d07694fc49a8e96075ddfd15913705f146d60262565",
    (
        "trigger",
        "trg_repair_actions_protect_terminal_application_status",
        "repair_actions",
    ): "ce91aea72b4f9024f0b10e86a7e91a59a2ecc9864a6a5088a14ed487e41a1d87",
    (
        "trigger",
        "trg_repair_approvals_prohibit_delete",
        "repair_approvals",
    ): "ab25267fdf49aeaacc4b7d6edb55681272465842847533ab96782846c5642b29",
    (
        "trigger",
        "trg_repair_approvals_prohibit_update",
        "repair_approvals",
    ): "90f14e550dabba911258a2ba8ba6a11c348949a2c2183e1ba63523aa4f83b71c",
    (
        "trigger",
        "trg_repair_plans_prohibit_delete",
        "repair_plans",
    ): "e9b43e9e05fdcaeedfc9a467e8325ec1ef54533dde0dd2098e034be367287531",
    (
        "trigger",
        "trg_repair_plans_protect_immutable_columns",
        "repair_plans",
    ): "1a30f1cb5124f6bc7b20968d8ea7e6406f5507a3e1b53a68aabec246524741a9",
    (
        "trigger",
        "trg_repair_plans_protect_terminal_status",
        "repair_plans",
    ): "6aea93157b5a970941ec15674f2c14ce4291cd2f8798a6b2b20d7cdc7d666faf",
    (
        "trigger",
        "trg_run_event_counters_next_sequence_number_must_increase",
        "run_event_counters",
    ): "1bf1bb00e98d674d94ec6b3742f1ab972efa2a5208489f7fea90ba0e96858b21",
    (
        "trigger",
        "trg_run_event_counters_prohibit_delete",
        "run_event_counters",
    ): "8caeaa4dce5645f6866f719bbaebfd49e3c4365280e72731515c8e5624e0fa92",
    (
        "trigger",
        "trg_run_event_counters_protect_immutable_columns",
        "run_event_counters",
    ): "d235ce282f055de5e24dab32160f6b2170add3b237e9ddf9091dd084902253f3",
    (
        "trigger",
        "trg_run_nodes_prohibit_delete",
        "run_nodes",
    ): "a3315afab5ec5515a301a1c0268408251216761b36e95afe7f69602f580ed627",
    (
        "trigger",
        "trg_run_nodes_protect_immutable_columns",
        "run_nodes",
    ): "ad457eff7efe25a12a2b2d9cec69de9f4341c21e92d6218fc22c0fbc413676a0",
    (
        "trigger",
        "trg_runs_prohibit_delete",
        "runs",
    ): "956d194764aa26ee724f8a1b56ca493f51213ed1f2d57f7893c745ef19df3d53",
    (
        "trigger",
        "trg_runs_protect_immutable_columns",
        "runs",
    ): "6bde03372ce8b1da348aaf298d38a4a0b4b6f5992a1b3536719e72a0ab0f249b",
    (
        "trigger",
        "trg_system_metadata_prohibit_delete",
        "system_metadata",
    ): "9492d436a28961f8fca31a9bf62194b906bf210a54b723b273177dfc2843d7af",
    (
        "trigger",
        "trg_system_metadata_protect_immutable_columns",
        "system_metadata",
    ): "8be89fa205c2736736886565d1c6a38a91b7a3dc60841e10136bfff8f6cfc1c1",
    (
        "trigger",
        "trg_work_attempts_prohibit_delete",
        "work_attempts",
    ): "e1ec0ec483b5a815f81c0754afaecbcb28b13844b160f1e1c4de61e5d5617811",
    (
        "trigger",
        "trg_work_attempts_prohibit_update",
        "work_attempts",
    ): "5efdde5fe26a9e003e587d12c4768d82604c611f0e90d558a705cc901854a164",
    (
        "trigger",
        "trg_work_items_prohibit_delete",
        "work_items",
    ): "c5a546e7e513cc373842c44fcb42d01872c6f4fdd8e919c2ee52ccc3969784e8",
    (
        "trigger",
        "trg_work_items_protect_immutable_columns",
        "work_items",
    ): "cf1a4f19885d1be584270daed514ff2cfc4a35be00f906001c3c98cbc015d7a2",
}


@dataclass(frozen=True, slots=True)
class MigrationReport:
    """One completed migration attempt against a caller-owned connection."""

    previous_revision: str | None
    current_revision: str
    target_revision: str


def _require_connection(connection: Connection) -> None:
    connection_value = cast(object, connection)
    if not isinstance(connection_value, Connection):
        raise MigrationConfigurationError("Migrations require a SQLAlchemy Connection.")
    if connection.closed:
        raise MigrationConfigurationError("Migrations require an open database connection.")
    if connection.dialect.name != "sqlite":
        raise MigrationConfigurationError("Operational migrations support only SQLite.")
    if connection.in_transaction():
        raise MigrationConfigurationError("Migrations require an idle caller-owned connection.")


def _migration_config(connection: Connection) -> Config:
    config = Config()
    config.set_main_option("script_location", _SCRIPT_LOCATION)
    config.attributes["connection"] = connection
    return config


def _configured_head(config: Config) -> str:
    heads = ScriptDirectory.from_config(config).get_heads()
    if heads != [HEAD_REVISION]:
        raise MigrationConfigurationError("The packaged migration history has an unexpected head.")
    return heads[0]


def _current_revision(connection: Connection) -> str | None:
    revisions = MigrationContext.configure(connection).get_current_heads()
    if len(revisions) > 1:
        raise MigrationIntegrityError("The operational database contains multiple migration heads.")
    return revisions[0] if revisions else None


def _validate_revision_state(connection: Connection, revision: str) -> None:
    """Reject schema drift even when the revision stamp claims the packaged head."""
    if revision != HEAD_REVISION:
        raise MigrationIntegrityError("The operational database is not at the expected revision.")
    table_names = frozenset(
        str(row[0])
        for row in connection.exec_driver_sql(
            "SELECT name FROM sqlite_master "
            "WHERE type = 'table' AND name NOT LIKE 'sqlite_%' AND name <> 'alembic_version'"
        )
    )
    if table_names != _EXPECTED_TABLE_NAMES:
        raise MigrationIntegrityError("The installed operational table inventory is incomplete.")
    trigger_count = connection.exec_driver_sql(
        "SELECT COUNT(*) FROM sqlite_master WHERE type = 'trigger'"
    ).scalar_one()
    if type(trigger_count) is not int or trigger_count != 47:
        raise MigrationIntegrityError("The installed operational trigger inventory is incomplete.")
    rows = connection.exec_driver_sql(
        "SELECT type, name, tbl_name, sql FROM sqlite_master "
        "WHERE sql IS NOT NULL AND name NOT LIKE 'sqlite_%' "
        "AND name <> 'alembic_version'"
    ).all()
    observed_hashes: dict[tuple[str, str, str], str] = {}
    for kind, name, table_name, sql in rows:
        if not all(isinstance(value, str) for value in (kind, name, table_name, sql)):
            raise MigrationIntegrityError("SQLite reported malformed schema metadata.")
        observed_hashes[(kind, name, table_name)] = hashlib.sha256(sql.encode("utf-8")).hexdigest()
    if observed_hashes != _EXPECTED_SCHEMA_HASHES:
        raise MigrationIntegrityError(
            "The installed operational schema does not match the packaged revision."
        )


def _foreign_keys_enabled(connection: Connection) -> bool:
    value = connection.exec_driver_sql("PRAGMA foreign_keys").scalar_one()
    return type(value) is int and value == 1


def _restore_connection_postconditions(connection: Connection) -> None:
    if connection.in_transaction():
        connection.rollback()
    connection.exec_driver_sql("PRAGMA foreign_keys = ON")
    if not _foreign_keys_enabled(connection):
        connection.rollback()
        raise MigrationIntegrityError("SQLite foreign-key enforcement could not be restored.")
    connection.rollback()


def upgrade_to_head(connection: Connection) -> MigrationReport:
    """Atomically upgrade one exact, caller-owned SQLite connection to the packaged head."""
    _require_connection(connection)
    operation_error: BaseException | None = None
    report: MigrationReport | None = None
    try:
        config = _migration_config(connection)
        target_revision = _configured_head(config)
        connection.exec_driver_sql("BEGIN IMMEDIATE")
        previous_revision = _current_revision(connection)
        command.upgrade(config, target_revision)
        current_revision = _current_revision(connection)
        if current_revision is None:
            raise MigrationIntegrityError("Alembic did not install a database revision.")
        _validate_revision_state(connection, current_revision)
        connection.commit()
        report = MigrationReport(
            previous_revision=previous_revision,
            current_revision=current_revision,
            target_revision=target_revision,
        )
    except BaseException as error:
        operation_error = error
        connection.rollback()

    try:
        _restore_connection_postconditions(connection)
    except BaseException as postcondition_error:
        if operation_error is not None:
            postcondition_error.add_note(
                "The migration also failed with "
                f"{type(operation_error).__name__}: {operation_error}"
            )
        raise

    if operation_error is not None:
        if isinstance(
            operation_error,
            (MigrationConfigurationError, MigrationExecutionError, MigrationIntegrityError),
        ):
            raise operation_error
        if isinstance(operation_error, SQLAlchemyError):
            raise MigrationExecutionError(
                "The operational migration failed atomically."
            ) from operation_error
        if isinstance(operation_error, CommandError):
            raise MigrationConfigurationError(
                "The packaged migration history or database revision is invalid."
            ) from operation_error
        raise operation_error
    return cast(MigrationReport, report)

PRAGMA foreign_keys = ON;
BEGIN IMMEDIATE;
CREATE TABLE audit_entries (
	sequence_number INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT, 
	actor VARCHAR(128) NOT NULL, 
	operation VARCHAR(96) NOT NULL, 
	object_kind VARCHAR(48) NOT NULL, 
	object_id VARCHAR(128), 
	correlation_id VARCHAR(96) NOT NULL, 
	occurred_at VARCHAR(27) NOT NULL, 
	detail_schema_version INTEGER NOT NULL, 
	detail_json TEXT NOT NULL, 
	CONSTRAINT ck_audit_entries_sequence_number_range CHECK (typeof(sequence_number) = 'integer' AND sequence_number BETWEEN 1 AND 2147483647), 
	CONSTRAINT ck_audit_entries_actor_size CHECK (typeof(actor) = 'text' AND length(actor) BETWEEN 1 AND 128), 
	CONSTRAINT ck_audit_entries_operation_size CHECK (typeof(operation) = 'text' AND length(operation) BETWEEN 1 AND 96), 
	CONSTRAINT ck_audit_entries_object_kind_size CHECK (typeof(object_kind) = 'text' AND length(object_kind) BETWEEN 1 AND 48), 
	CONSTRAINT ck_audit_entries_correlation_id_size CHECK (typeof(correlation_id) = 'text' AND length(correlation_id) BETWEEN 1 AND 96), 
	CONSTRAINT ck_audit_entries_occurred_at_utc CHECK (typeof(occurred_at) = 'text' AND length(occurred_at) = 27 AND substr(occurred_at, 5, 1) = '-' AND substr(occurred_at, 8, 1) = '-' AND substr(occurred_at, 11, 1) = 'T' AND substr(occurred_at, 14, 1) = ':' AND substr(occurred_at, 17, 1) = ':' AND substr(occurred_at, 20, 1) = '.' AND substr(occurred_at, 27, 1) = 'Z' AND substr(occurred_at, 1, 4) NOT GLOB '*[^0-9]*' AND substr(occurred_at, 6, 2) NOT GLOB '*[^0-9]*' AND substr(occurred_at, 9, 2) NOT GLOB '*[^0-9]*' AND substr(occurred_at, 12, 2) NOT GLOB '*[^0-9]*' AND substr(occurred_at, 15, 2) NOT GLOB '*[^0-9]*' AND substr(occurred_at, 18, 2) NOT GLOB '*[^0-9]*' AND substr(occurred_at, 21, 6) NOT GLOB '*[^0-9]*' AND substr(occurred_at, 1, 4) BETWEEN '0001' AND '9999' AND substr(occurred_at, 6, 2) BETWEEN '01' AND '12' AND substr(occurred_at, 9, 2) BETWEEN '01' AND '31' AND substr(occurred_at, 12, 2) BETWEEN '00' AND '23' AND substr(occurred_at, 15, 2) BETWEEN '00' AND '59' AND substr(occurred_at, 18, 2) BETWEEN '00' AND '59'), 
	CONSTRAINT ck_audit_entries_detail_schema_version_range CHECK (typeof(detail_schema_version) = 'integer' AND detail_schema_version BETWEEN 1 AND 2147483647), 
	CONSTRAINT ck_audit_entries_detail_json_object CHECK (typeof(detail_json) = 'text' AND json_valid(detail_json) AND json_type(detail_json) = 'object'), 
	CONSTRAINT ck_audit_entries_object_id_size CHECK (object_id IS NULL OR (typeof(object_id) = 'text' AND length(object_id) BETWEEN 1 AND 128))
);
CREATE TABLE connectors (
	connector_id VARCHAR(68) NOT NULL, 
	kind VARCHAR(96) NOT NULL, 
	display_name VARCHAR(160) NOT NULL, 
	configuration_json TEXT NOT NULL, 
	capabilities_json TEXT NOT NULL, 
	schema_discovery_json TEXT, 
	revision INTEGER DEFAULT '1' NOT NULL, 
	created_at VARCHAR(27) NOT NULL, 
	updated_at VARCHAR(27) NOT NULL, 
	archived_at VARCHAR(27), 
	row_version INTEGER DEFAULT '1' NOT NULL, 
	CONSTRAINT pk_connectors PRIMARY KEY (connector_id), 
	CONSTRAINT ck_connectors_connector_id_shape CHECK (typeof(connector_id) = 'text' AND length(connector_id) BETWEEN 7 AND 68 AND substr(connector_id, 1, 4) = 'con_' AND substr(connector_id, 5) NOT GLOB '*[^a-z0-9-]*' AND substr(connector_id, 5) NOT LIKE '-%' AND substr(connector_id, -1) <> '-' AND connector_id NOT LIKE '%--%'), 
	CONSTRAINT ck_connectors_kind_size CHECK (typeof(kind) = 'text' AND length(kind) BETWEEN 1 AND 96), 
	CONSTRAINT ck_connectors_display_name_size CHECK (typeof(display_name) = 'text' AND length(display_name) BETWEEN 1 AND 160), 
	CONSTRAINT ck_connectors_configuration_json_object CHECK (typeof(configuration_json) = 'text' AND json_valid(configuration_json) AND json_type(configuration_json) = 'object'), 
	CONSTRAINT ck_connectors_capabilities_json_object CHECK (typeof(capabilities_json) = 'text' AND json_valid(capabilities_json) AND json_type(capabilities_json) = 'object'), 
	CONSTRAINT ck_connectors_schema_discovery_json_object CHECK (schema_discovery_json IS NULL OR (typeof(schema_discovery_json) = 'text' AND json_valid(schema_discovery_json) AND json_type(schema_discovery_json) = 'object')), 
	CONSTRAINT ck_connectors_revision_range CHECK (typeof(revision) = 'integer' AND revision BETWEEN 1 AND 2147483647), 
	CONSTRAINT ck_connectors_created_at_utc CHECK (typeof(created_at) = 'text' AND length(created_at) = 27 AND substr(created_at, 5, 1) = '-' AND substr(created_at, 8, 1) = '-' AND substr(created_at, 11, 1) = 'T' AND substr(created_at, 14, 1) = ':' AND substr(created_at, 17, 1) = ':' AND substr(created_at, 20, 1) = '.' AND substr(created_at, 27, 1) = 'Z' AND substr(created_at, 1, 4) NOT GLOB '*[^0-9]*' AND substr(created_at, 6, 2) NOT GLOB '*[^0-9]*' AND substr(created_at, 9, 2) NOT GLOB '*[^0-9]*' AND substr(created_at, 12, 2) NOT GLOB '*[^0-9]*' AND substr(created_at, 15, 2) NOT GLOB '*[^0-9]*' AND substr(created_at, 18, 2) NOT GLOB '*[^0-9]*' AND substr(created_at, 21, 6) NOT GLOB '*[^0-9]*' AND substr(created_at, 1, 4) BETWEEN '0001' AND '9999' AND substr(created_at, 6, 2) BETWEEN '01' AND '12' AND substr(created_at, 9, 2) BETWEEN '01' AND '31' AND substr(created_at, 12, 2) BETWEEN '00' AND '23' AND substr(created_at, 15, 2) BETWEEN '00' AND '59' AND substr(created_at, 18, 2) BETWEEN '00' AND '59'), 
	CONSTRAINT ck_connectors_updated_at_utc CHECK (typeof(updated_at) = 'text' AND length(updated_at) = 27 AND substr(updated_at, 5, 1) = '-' AND substr(updated_at, 8, 1) = '-' AND substr(updated_at, 11, 1) = 'T' AND substr(updated_at, 14, 1) = ':' AND substr(updated_at, 17, 1) = ':' AND substr(updated_at, 20, 1) = '.' AND substr(updated_at, 27, 1) = 'Z' AND substr(updated_at, 1, 4) NOT GLOB '*[^0-9]*' AND substr(updated_at, 6, 2) NOT GLOB '*[^0-9]*' AND substr(updated_at, 9, 2) NOT GLOB '*[^0-9]*' AND substr(updated_at, 12, 2) NOT GLOB '*[^0-9]*' AND substr(updated_at, 15, 2) NOT GLOB '*[^0-9]*' AND substr(updated_at, 18, 2) NOT GLOB '*[^0-9]*' AND substr(updated_at, 21, 6) NOT GLOB '*[^0-9]*' AND substr(updated_at, 1, 4) BETWEEN '0001' AND '9999' AND substr(updated_at, 6, 2) BETWEEN '01' AND '12' AND substr(updated_at, 9, 2) BETWEEN '01' AND '31' AND substr(updated_at, 12, 2) BETWEEN '00' AND '23' AND substr(updated_at, 15, 2) BETWEEN '00' AND '59' AND substr(updated_at, 18, 2) BETWEEN '00' AND '59'), 
	CONSTRAINT ck_connectors_archived_at_utc CHECK (archived_at IS NULL OR (typeof(archived_at) = 'text' AND length(archived_at) = 27 AND substr(archived_at, 5, 1) = '-' AND substr(archived_at, 8, 1) = '-' AND substr(archived_at, 11, 1) = 'T' AND substr(archived_at, 14, 1) = ':' AND substr(archived_at, 17, 1) = ':' AND substr(archived_at, 20, 1) = '.' AND substr(archived_at, 27, 1) = 'Z' AND substr(archived_at, 1, 4) NOT GLOB '*[^0-9]*' AND substr(archived_at, 6, 2) NOT GLOB '*[^0-9]*' AND substr(archived_at, 9, 2) NOT GLOB '*[^0-9]*' AND substr(archived_at, 12, 2) NOT GLOB '*[^0-9]*' AND substr(archived_at, 15, 2) NOT GLOB '*[^0-9]*' AND substr(archived_at, 18, 2) NOT GLOB '*[^0-9]*' AND substr(archived_at, 21, 6) NOT GLOB '*[^0-9]*' AND substr(archived_at, 1, 4) BETWEEN '0001' AND '9999' AND substr(archived_at, 6, 2) BETWEEN '01' AND '12' AND substr(archived_at, 9, 2) BETWEEN '01' AND '31' AND substr(archived_at, 12, 2) BETWEEN '00' AND '23' AND substr(archived_at, 15, 2) BETWEEN '00' AND '59' AND substr(archived_at, 18, 2) BETWEEN '00' AND '59')), 
	CONSTRAINT ck_connectors_row_version_range CHECK (typeof(row_version) = 'integer' AND row_version BETWEEN 1 AND 2147483647), 
	CONSTRAINT ck_connectors_updated_at_order CHECK (updated_at >= created_at), 
	CONSTRAINT ck_connectors_archive_order CHECK (archived_at IS NULL OR archived_at >= created_at)
);
CREATE TABLE idempotency_records (
	scope VARCHAR(96) NOT NULL, 
	idempotency_key VARCHAR(128) NOT NULL, 
	request_sha256 VARCHAR(64) NOT NULL, 
	status VARCHAR(32) NOT NULL, 
	response_schema_version INTEGER, 
	response_json TEXT, 
	created_at VARCHAR(27) NOT NULL, 
	updated_at VARCHAR(27) NOT NULL, 
	completed_at VARCHAR(27), 
	CONSTRAINT pk_idempotency_records PRIMARY KEY (scope, idempotency_key), 
	CONSTRAINT ck_idempotency_records_scope_size CHECK (typeof(scope) = 'text' AND length(scope) BETWEEN 1 AND 96), 
	CONSTRAINT ck_idempotency_records_idempotency_key_size CHECK (typeof(idempotency_key) = 'text' AND length(idempotency_key) BETWEEN 1 AND 128), 
	CONSTRAINT ck_idempotency_records_request_sha256_shape CHECK (typeof(request_sha256) = 'text' AND length(request_sha256) = 64 AND request_sha256 NOT GLOB '*[^0-9a-f]*'), 
	CONSTRAINT ck_idempotency_records_status_values CHECK (status IN ('in_progress', 'completed', 'failed')), 
	CONSTRAINT ck_idempotency_records_response_json_object CHECK (response_json IS NULL OR (typeof(response_json) = 'text' AND json_valid(response_json) AND json_type(response_json) = 'object')), 
	CONSTRAINT ck_idempotency_records_created_at_utc CHECK (typeof(created_at) = 'text' AND length(created_at) = 27 AND substr(created_at, 5, 1) = '-' AND substr(created_at, 8, 1) = '-' AND substr(created_at, 11, 1) = 'T' AND substr(created_at, 14, 1) = ':' AND substr(created_at, 17, 1) = ':' AND substr(created_at, 20, 1) = '.' AND substr(created_at, 27, 1) = 'Z' AND substr(created_at, 1, 4) NOT GLOB '*[^0-9]*' AND substr(created_at, 6, 2) NOT GLOB '*[^0-9]*' AND substr(created_at, 9, 2) NOT GLOB '*[^0-9]*' AND substr(created_at, 12, 2) NOT GLOB '*[^0-9]*' AND substr(created_at, 15, 2) NOT GLOB '*[^0-9]*' AND substr(created_at, 18, 2) NOT GLOB '*[^0-9]*' AND substr(created_at, 21, 6) NOT GLOB '*[^0-9]*' AND substr(created_at, 1, 4) BETWEEN '0001' AND '9999' AND substr(created_at, 6, 2) BETWEEN '01' AND '12' AND substr(created_at, 9, 2) BETWEEN '01' AND '31' AND substr(created_at, 12, 2) BETWEEN '00' AND '23' AND substr(created_at, 15, 2) BETWEEN '00' AND '59' AND substr(created_at, 18, 2) BETWEEN '00' AND '59'), 
	CONSTRAINT ck_idempotency_records_updated_at_utc CHECK (typeof(updated_at) = 'text' AND length(updated_at) = 27 AND substr(updated_at, 5, 1) = '-' AND substr(updated_at, 8, 1) = '-' AND substr(updated_at, 11, 1) = 'T' AND substr(updated_at, 14, 1) = ':' AND substr(updated_at, 17, 1) = ':' AND substr(updated_at, 20, 1) = '.' AND substr(updated_at, 27, 1) = 'Z' AND substr(updated_at, 1, 4) NOT GLOB '*[^0-9]*' AND substr(updated_at, 6, 2) NOT GLOB '*[^0-9]*' AND substr(updated_at, 9, 2) NOT GLOB '*[^0-9]*' AND substr(updated_at, 12, 2) NOT GLOB '*[^0-9]*' AND substr(updated_at, 15, 2) NOT GLOB '*[^0-9]*' AND substr(updated_at, 18, 2) NOT GLOB '*[^0-9]*' AND substr(updated_at, 21, 6) NOT GLOB '*[^0-9]*' AND substr(updated_at, 1, 4) BETWEEN '0001' AND '9999' AND substr(updated_at, 6, 2) BETWEEN '01' AND '12' AND substr(updated_at, 9, 2) BETWEEN '01' AND '31' AND substr(updated_at, 12, 2) BETWEEN '00' AND '23' AND substr(updated_at, 15, 2) BETWEEN '00' AND '59' AND substr(updated_at, 18, 2) BETWEEN '00' AND '59'), 
	CONSTRAINT ck_idempotency_records_completed_at_utc CHECK (completed_at IS NULL OR (typeof(completed_at) = 'text' AND length(completed_at) = 27 AND substr(completed_at, 5, 1) = '-' AND substr(completed_at, 8, 1) = '-' AND substr(completed_at, 11, 1) = 'T' AND substr(completed_at, 14, 1) = ':' AND substr(completed_at, 17, 1) = ':' AND substr(completed_at, 20, 1) = '.' AND substr(completed_at, 27, 1) = 'Z' AND substr(completed_at, 1, 4) NOT GLOB '*[^0-9]*' AND substr(completed_at, 6, 2) NOT GLOB '*[^0-9]*' AND substr(completed_at, 9, 2) NOT GLOB '*[^0-9]*' AND substr(completed_at, 12, 2) NOT GLOB '*[^0-9]*' AND substr(completed_at, 15, 2) NOT GLOB '*[^0-9]*' AND substr(completed_at, 18, 2) NOT GLOB '*[^0-9]*' AND substr(completed_at, 21, 6) NOT GLOB '*[^0-9]*' AND substr(completed_at, 1, 4) BETWEEN '0001' AND '9999' AND substr(completed_at, 6, 2) BETWEEN '01' AND '12' AND substr(completed_at, 9, 2) BETWEEN '01' AND '31' AND substr(completed_at, 12, 2) BETWEEN '00' AND '23' AND substr(completed_at, 15, 2) BETWEEN '00' AND '59' AND substr(completed_at, 18, 2) BETWEEN '00' AND '59')), 
	CONSTRAINT ck_idempotency_records_response_schema_version_range CHECK (response_schema_version IS NULL OR (typeof(response_schema_version) = 'integer' AND response_schema_version BETWEEN 1 AND 2147483647)), 
	CONSTRAINT ck_idempotency_records_updated_at_order CHECK (updated_at >= created_at), 
	CONSTRAINT ck_idempotency_records_completed_at_order CHECK (completed_at IS NULL OR completed_at >= created_at), 
	CONSTRAINT ck_idempotency_records_response_coherence CHECK ((status = 'in_progress' AND response_schema_version IS NULL AND response_json IS NULL AND completed_at IS NULL) OR (status IN ('completed','failed') AND response_schema_version IS NOT NULL AND response_json IS NOT NULL AND completed_at IS NOT NULL))
);
CREATE TABLE pipelines (
	pipeline_id VARCHAR(68) NOT NULL, 
	display_name VARCHAR(160) NOT NULL, 
	description TEXT, 
	created_at VARCHAR(27) NOT NULL, 
	archived_at VARCHAR(27), 
	row_version INTEGER DEFAULT '1' NOT NULL, 
	CONSTRAINT pk_pipelines PRIMARY KEY (pipeline_id), 
	CONSTRAINT ck_pipelines_pipeline_id_shape CHECK (typeof(pipeline_id) = 'text' AND length(pipeline_id) BETWEEN 7 AND 68 AND substr(pipeline_id, 1, 4) = 'pip_' AND substr(pipeline_id, 5) NOT GLOB '*[^a-z0-9-]*' AND substr(pipeline_id, 5) NOT LIKE '-%' AND substr(pipeline_id, -1) <> '-' AND pipeline_id NOT LIKE '%--%'), 
	CONSTRAINT ck_pipelines_display_name_size CHECK (typeof(display_name) = 'text' AND length(display_name) BETWEEN 1 AND 160), 
	CONSTRAINT ck_pipelines_created_at_utc CHECK (typeof(created_at) = 'text' AND length(created_at) = 27 AND substr(created_at, 5, 1) = '-' AND substr(created_at, 8, 1) = '-' AND substr(created_at, 11, 1) = 'T' AND substr(created_at, 14, 1) = ':' AND substr(created_at, 17, 1) = ':' AND substr(created_at, 20, 1) = '.' AND substr(created_at, 27, 1) = 'Z' AND substr(created_at, 1, 4) NOT GLOB '*[^0-9]*' AND substr(created_at, 6, 2) NOT GLOB '*[^0-9]*' AND substr(created_at, 9, 2) NOT GLOB '*[^0-9]*' AND substr(created_at, 12, 2) NOT GLOB '*[^0-9]*' AND substr(created_at, 15, 2) NOT GLOB '*[^0-9]*' AND substr(created_at, 18, 2) NOT GLOB '*[^0-9]*' AND substr(created_at, 21, 6) NOT GLOB '*[^0-9]*' AND substr(created_at, 1, 4) BETWEEN '0001' AND '9999' AND substr(created_at, 6, 2) BETWEEN '01' AND '12' AND substr(created_at, 9, 2) BETWEEN '01' AND '31' AND substr(created_at, 12, 2) BETWEEN '00' AND '23' AND substr(created_at, 15, 2) BETWEEN '00' AND '59' AND substr(created_at, 18, 2) BETWEEN '00' AND '59'), 
	CONSTRAINT ck_pipelines_archived_at_utc CHECK (archived_at IS NULL OR (typeof(archived_at) = 'text' AND length(archived_at) = 27 AND substr(archived_at, 5, 1) = '-' AND substr(archived_at, 8, 1) = '-' AND substr(archived_at, 11, 1) = 'T' AND substr(archived_at, 14, 1) = ':' AND substr(archived_at, 17, 1) = ':' AND substr(archived_at, 20, 1) = '.' AND substr(archived_at, 27, 1) = 'Z' AND substr(archived_at, 1, 4) NOT GLOB '*[^0-9]*' AND substr(archived_at, 6, 2) NOT GLOB '*[^0-9]*' AND substr(archived_at, 9, 2) NOT GLOB '*[^0-9]*' AND substr(archived_at, 12, 2) NOT GLOB '*[^0-9]*' AND substr(archived_at, 15, 2) NOT GLOB '*[^0-9]*' AND substr(archived_at, 18, 2) NOT GLOB '*[^0-9]*' AND substr(archived_at, 21, 6) NOT GLOB '*[^0-9]*' AND substr(archived_at, 1, 4) BETWEEN '0001' AND '9999' AND substr(archived_at, 6, 2) BETWEEN '01' AND '12' AND substr(archived_at, 9, 2) BETWEEN '01' AND '31' AND substr(archived_at, 12, 2) BETWEEN '00' AND '23' AND substr(archived_at, 15, 2) BETWEEN '00' AND '59' AND substr(archived_at, 18, 2) BETWEEN '00' AND '59')), 
	CONSTRAINT ck_pipelines_row_version_range CHECK (typeof(row_version) = 'integer' AND row_version BETWEEN 1 AND 2147483647), 
	CONSTRAINT ck_pipelines_archive_order CHECK (archived_at IS NULL OR archived_at >= created_at)
);
CREATE TABLE system_metadata (
	"key" VARCHAR(96) NOT NULL, 
	value TEXT NOT NULL, 
	updated_at VARCHAR(27) NOT NULL, 
	CONSTRAINT pk_system_metadata PRIMARY KEY ("key"), 
	CONSTRAINT ck_system_metadata_key_shape CHECK (typeof(key) = 'text' AND length(key) BETWEEN 1 AND 96 AND key GLOB '[a-z]*' AND key NOT GLOB '*[^a-z0-9_.-]*' AND key NOT GLOB '*[._-][._-]*' AND substr(key,-1) NOT IN ('.','-','_')), 
	CONSTRAINT ck_system_metadata_value_size CHECK (typeof(value) = 'text' AND length(value) BETWEEN 1 AND 4096), 
	CONSTRAINT ck_system_metadata_updated_at_utc CHECK (typeof(updated_at) = 'text' AND length(updated_at) = 27 AND substr(updated_at, 5, 1) = '-' AND substr(updated_at, 8, 1) = '-' AND substr(updated_at, 11, 1) = 'T' AND substr(updated_at, 14, 1) = ':' AND substr(updated_at, 17, 1) = ':' AND substr(updated_at, 20, 1) = '.' AND substr(updated_at, 27, 1) = 'Z' AND substr(updated_at, 1, 4) NOT GLOB '*[^0-9]*' AND substr(updated_at, 6, 2) NOT GLOB '*[^0-9]*' AND substr(updated_at, 9, 2) NOT GLOB '*[^0-9]*' AND substr(updated_at, 12, 2) NOT GLOB '*[^0-9]*' AND substr(updated_at, 15, 2) NOT GLOB '*[^0-9]*' AND substr(updated_at, 18, 2) NOT GLOB '*[^0-9]*' AND substr(updated_at, 21, 6) NOT GLOB '*[^0-9]*' AND substr(updated_at, 1, 4) BETWEEN '0001' AND '9999' AND substr(updated_at, 6, 2) BETWEEN '01' AND '12' AND substr(updated_at, 9, 2) BETWEEN '01' AND '31' AND substr(updated_at, 12, 2) BETWEEN '00' AND '23' AND substr(updated_at, 15, 2) BETWEEN '00' AND '59' AND substr(updated_at, 18, 2) BETWEEN '00' AND '59')
);
CREATE TABLE connector_secret_references (
	connector_id VARCHAR(68) NOT NULL, 
	reference_name VARCHAR(64) NOT NULL, 
	environment_variable_name VARCHAR(128) NOT NULL, 
	created_at VARCHAR(27) NOT NULL, 
	CONSTRAINT pk_connector_secret_references PRIMARY KEY (connector_id, reference_name), 
	CONSTRAINT fk_connector_secret_references_connector_id_connectors FOREIGN KEY(connector_id) REFERENCES connectors (connector_id), 
	CONSTRAINT ck_connector_secret_references_reference_name_shape CHECK (typeof(reference_name) = 'text' AND length(reference_name) BETWEEN 1 AND 64 AND reference_name GLOB '[a-z]*' AND reference_name NOT GLOB '*[^a-z0-9_.-]*' AND reference_name NOT GLOB '*[._-][._-]*' AND substr(reference_name,-1) NOT IN ('.','-','_')), 
	CONSTRAINT ck_connector_secret_references_environment_variable_name_shape CHECK (typeof(environment_variable_name) = 'text' AND length(environment_variable_name) BETWEEN 1 AND 128 AND environment_variable_name GLOB '[A-Z_]*' AND environment_variable_name NOT GLOB '*[^A-Z0-9_]*'), 
	CONSTRAINT ck_connector_secret_references_created_at_utc CHECK (typeof(created_at) = 'text' AND length(created_at) = 27 AND substr(created_at, 5, 1) = '-' AND substr(created_at, 8, 1) = '-' AND substr(created_at, 11, 1) = 'T' AND substr(created_at, 14, 1) = ':' AND substr(created_at, 17, 1) = ':' AND substr(created_at, 20, 1) = '.' AND substr(created_at, 27, 1) = 'Z' AND substr(created_at, 1, 4) NOT GLOB '*[^0-9]*' AND substr(created_at, 6, 2) NOT GLOB '*[^0-9]*' AND substr(created_at, 9, 2) NOT GLOB '*[^0-9]*' AND substr(created_at, 12, 2) NOT GLOB '*[^0-9]*' AND substr(created_at, 15, 2) NOT GLOB '*[^0-9]*' AND substr(created_at, 18, 2) NOT GLOB '*[^0-9]*' AND substr(created_at, 21, 6) NOT GLOB '*[^0-9]*' AND substr(created_at, 1, 4) BETWEEN '0001' AND '9999' AND substr(created_at, 6, 2) BETWEEN '01' AND '12' AND substr(created_at, 9, 2) BETWEEN '01' AND '31' AND substr(created_at, 12, 2) BETWEEN '00' AND '23' AND substr(created_at, 15, 2) BETWEEN '00' AND '59' AND substr(created_at, 18, 2) BETWEEN '00' AND '59')
);
CREATE TABLE pipeline_versions (
	pipeline_id VARCHAR(68) NOT NULL, 
	version_number INTEGER NOT NULL, 
	specification_json TEXT NOT NULL, 
	specification_sha256 VARCHAR(64) NOT NULL, 
	planner_format_version INTEGER NOT NULL, 
	published_at VARCHAR(27) NOT NULL, 
	CONSTRAINT pk_pipeline_versions PRIMARY KEY (pipeline_id, version_number), 
	CONSTRAINT fk_pipeline_versions_pipeline_id_pipelines FOREIGN KEY(pipeline_id) REFERENCES pipelines (pipeline_id), 
	CONSTRAINT ck_pipeline_versions_version_number_range CHECK (typeof(version_number) = 'integer' AND version_number BETWEEN 1 AND 2147483647), 
	CONSTRAINT ck_pipeline_versions_specification_json_object CHECK (typeof(specification_json) = 'text' AND json_valid(specification_json) AND json_type(specification_json) = 'object'), 
	CONSTRAINT ck_pipeline_versions_specification_sha256_shape CHECK (typeof(specification_sha256) = 'text' AND length(specification_sha256) = 64 AND specification_sha256 NOT GLOB '*[^0-9a-f]*'), 
	CONSTRAINT ck_pipeline_versions_planner_format_version_range CHECK (typeof(planner_format_version) = 'integer' AND planner_format_version BETWEEN 1 AND 2147483647), 
	CONSTRAINT ck_pipeline_versions_published_at_utc CHECK (typeof(published_at) = 'text' AND length(published_at) = 27 AND substr(published_at, 5, 1) = '-' AND substr(published_at, 8, 1) = '-' AND substr(published_at, 11, 1) = 'T' AND substr(published_at, 14, 1) = ':' AND substr(published_at, 17, 1) = ':' AND substr(published_at, 20, 1) = '.' AND substr(published_at, 27, 1) = 'Z' AND substr(published_at, 1, 4) NOT GLOB '*[^0-9]*' AND substr(published_at, 6, 2) NOT GLOB '*[^0-9]*' AND substr(published_at, 9, 2) NOT GLOB '*[^0-9]*' AND substr(published_at, 12, 2) NOT GLOB '*[^0-9]*' AND substr(published_at, 15, 2) NOT GLOB '*[^0-9]*' AND substr(published_at, 18, 2) NOT GLOB '*[^0-9]*' AND substr(published_at, 21, 6) NOT GLOB '*[^0-9]*' AND substr(published_at, 1, 4) BETWEEN '0001' AND '9999' AND substr(published_at, 6, 2) BETWEEN '01' AND '12' AND substr(published_at, 9, 2) BETWEEN '01' AND '31' AND substr(published_at, 12, 2) BETWEEN '00' AND '23' AND substr(published_at, 15, 2) BETWEEN '00' AND '59' AND substr(published_at, 18, 2) BETWEEN '00' AND '59')
);
CREATE TABLE runs (
	run_id VARCHAR(68) NOT NULL, 
	pipeline_id VARCHAR(68) NOT NULL, 
	pipeline_version_number INTEGER NOT NULL, 
	runner_kind VARCHAR(32) NOT NULL, 
	runner_configuration_json TEXT NOT NULL, 
	state VARCHAR(32) NOT NULL, 
	row_version INTEGER DEFAULT '1' NOT NULL, 
	scenario_seed INTEGER, 
	created_at VARCHAR(27) NOT NULL, 
	started_at VARCHAR(27), 
	finished_at VARCHAR(27), 
	cancellation_requested_at VARCHAR(27), 
	recovery_started_at VARCHAR(27), 
	recovered_at VARCHAR(27), 
	final_reconciliation_fingerprint VARCHAR(64), 
	CONSTRAINT pk_runs PRIMARY KEY (run_id), 
	CONSTRAINT fk_runs_pipeline_id_pipeline_version_number_pipeline_versions FOREIGN KEY(pipeline_id, pipeline_version_number) REFERENCES pipeline_versions (pipeline_id, version_number), 
	CONSTRAINT ck_runs_run_id_shape CHECK (typeof(run_id) = 'text' AND length(run_id) BETWEEN 7 AND 68 AND substr(run_id, 1, 4) = 'run_' AND substr(run_id, 5) NOT GLOB '*[^a-z0-9-]*' AND substr(run_id, 5) NOT LIKE '-%' AND substr(run_id, -1) <> '-' AND run_id NOT LIKE '%--%'), 
	CONSTRAINT ck_runs_pipeline_version_number_range CHECK (typeof(pipeline_version_number) = 'integer' AND pipeline_version_number BETWEEN 1 AND 2147483647), 
	CONSTRAINT ck_runs_runner_kind_size CHECK (typeof(runner_kind) = 'text' AND length(runner_kind) BETWEEN 1 AND 32), 
	CONSTRAINT ck_runs_runner_configuration_json_object CHECK (typeof(runner_configuration_json) = 'text' AND json_valid(runner_configuration_json) AND json_type(runner_configuration_json) = 'object'), 
	CONSTRAINT ck_runs_state_values CHECK (state IN ('queued', 'running', 'pausing', 'paused', 'resuming', 'succeeded', 'partially_succeeded', 'failed', 'cancelling', 'cancelled')), 
	CONSTRAINT ck_runs_row_version_range CHECK (typeof(row_version) = 'integer' AND row_version BETWEEN 1 AND 2147483647), 
	CONSTRAINT ck_runs_scenario_seed_storage CHECK (scenario_seed IS NULL OR typeof(scenario_seed) = 'integer'), 
	CONSTRAINT ck_runs_created_at_utc CHECK (typeof(created_at) = 'text' AND length(created_at) = 27 AND substr(created_at, 5, 1) = '-' AND substr(created_at, 8, 1) = '-' AND substr(created_at, 11, 1) = 'T' AND substr(created_at, 14, 1) = ':' AND substr(created_at, 17, 1) = ':' AND substr(created_at, 20, 1) = '.' AND substr(created_at, 27, 1) = 'Z' AND substr(created_at, 1, 4) NOT GLOB '*[^0-9]*' AND substr(created_at, 6, 2) NOT GLOB '*[^0-9]*' AND substr(created_at, 9, 2) NOT GLOB '*[^0-9]*' AND substr(created_at, 12, 2) NOT GLOB '*[^0-9]*' AND substr(created_at, 15, 2) NOT GLOB '*[^0-9]*' AND substr(created_at, 18, 2) NOT GLOB '*[^0-9]*' AND substr(created_at, 21, 6) NOT GLOB '*[^0-9]*' AND substr(created_at, 1, 4) BETWEEN '0001' AND '9999' AND substr(created_at, 6, 2) BETWEEN '01' AND '12' AND substr(created_at, 9, 2) BETWEEN '01' AND '31' AND substr(created_at, 12, 2) BETWEEN '00' AND '23' AND substr(created_at, 15, 2) BETWEEN '00' AND '59' AND substr(created_at, 18, 2) BETWEEN '00' AND '59'), 
	CONSTRAINT ck_runs_started_at_utc CHECK (started_at IS NULL OR (typeof(started_at) = 'text' AND length(started_at) = 27 AND substr(started_at, 5, 1) = '-' AND substr(started_at, 8, 1) = '-' AND substr(started_at, 11, 1) = 'T' AND substr(started_at, 14, 1) = ':' AND substr(started_at, 17, 1) = ':' AND substr(started_at, 20, 1) = '.' AND substr(started_at, 27, 1) = 'Z' AND substr(started_at, 1, 4) NOT GLOB '*[^0-9]*' AND substr(started_at, 6, 2) NOT GLOB '*[^0-9]*' AND substr(started_at, 9, 2) NOT GLOB '*[^0-9]*' AND substr(started_at, 12, 2) NOT GLOB '*[^0-9]*' AND substr(started_at, 15, 2) NOT GLOB '*[^0-9]*' AND substr(started_at, 18, 2) NOT GLOB '*[^0-9]*' AND substr(started_at, 21, 6) NOT GLOB '*[^0-9]*' AND substr(started_at, 1, 4) BETWEEN '0001' AND '9999' AND substr(started_at, 6, 2) BETWEEN '01' AND '12' AND substr(started_at, 9, 2) BETWEEN '01' AND '31' AND substr(started_at, 12, 2) BETWEEN '00' AND '23' AND substr(started_at, 15, 2) BETWEEN '00' AND '59' AND substr(started_at, 18, 2) BETWEEN '00' AND '59')), 
	CONSTRAINT ck_runs_finished_at_utc CHECK (finished_at IS NULL OR (typeof(finished_at) = 'text' AND length(finished_at) = 27 AND substr(finished_at, 5, 1) = '-' AND substr(finished_at, 8, 1) = '-' AND substr(finished_at, 11, 1) = 'T' AND substr(finished_at, 14, 1) = ':' AND substr(finished_at, 17, 1) = ':' AND substr(finished_at, 20, 1) = '.' AND substr(finished_at, 27, 1) = 'Z' AND substr(finished_at, 1, 4) NOT GLOB '*[^0-9]*' AND substr(finished_at, 6, 2) NOT GLOB '*[^0-9]*' AND substr(finished_at, 9, 2) NOT GLOB '*[^0-9]*' AND substr(finished_at, 12, 2) NOT GLOB '*[^0-9]*' AND substr(finished_at, 15, 2) NOT GLOB '*[^0-9]*' AND substr(finished_at, 18, 2) NOT GLOB '*[^0-9]*' AND substr(finished_at, 21, 6) NOT GLOB '*[^0-9]*' AND substr(finished_at, 1, 4) BETWEEN '0001' AND '9999' AND substr(finished_at, 6, 2) BETWEEN '01' AND '12' AND substr(finished_at, 9, 2) BETWEEN '01' AND '31' AND substr(finished_at, 12, 2) BETWEEN '00' AND '23' AND substr(finished_at, 15, 2) BETWEEN '00' AND '59' AND substr(finished_at, 18, 2) BETWEEN '00' AND '59')), 
	CONSTRAINT ck_runs_cancellation_requested_at_utc CHECK (cancellation_requested_at IS NULL OR (typeof(cancellation_requested_at) = 'text' AND length(cancellation_requested_at) = 27 AND substr(cancellation_requested_at, 5, 1) = '-' AND substr(cancellation_requested_at, 8, 1) = '-' AND substr(cancellation_requested_at, 11, 1) = 'T' AND substr(cancellation_requested_at, 14, 1) = ':' AND substr(cancellation_requested_at, 17, 1) = ':' AND substr(cancellation_requested_at, 20, 1) = '.' AND substr(cancellation_requested_at, 27, 1) = 'Z' AND substr(cancellation_requested_at, 1, 4) NOT GLOB '*[^0-9]*' AND substr(cancellation_requested_at, 6, 2) NOT GLOB '*[^0-9]*' AND substr(cancellation_requested_at, 9, 2) NOT GLOB '*[^0-9]*' AND substr(cancellation_requested_at, 12, 2) NOT GLOB '*[^0-9]*' AND substr(cancellation_requested_at, 15, 2) NOT GLOB '*[^0-9]*' AND substr(cancellation_requested_at, 18, 2) NOT GLOB '*[^0-9]*' AND substr(cancellation_requested_at, 21, 6) NOT GLOB '*[^0-9]*' AND substr(cancellation_requested_at, 1, 4) BETWEEN '0001' AND '9999' AND substr(cancellation_requested_at, 6, 2) BETWEEN '01' AND '12' AND substr(cancellation_requested_at, 9, 2) BETWEEN '01' AND '31' AND substr(cancellation_requested_at, 12, 2) BETWEEN '00' AND '23' AND substr(cancellation_requested_at, 15, 2) BETWEEN '00' AND '59' AND substr(cancellation_requested_at, 18, 2) BETWEEN '00' AND '59')), 
	CONSTRAINT ck_runs_recovery_started_at_utc CHECK (recovery_started_at IS NULL OR (typeof(recovery_started_at) = 'text' AND length(recovery_started_at) = 27 AND substr(recovery_started_at, 5, 1) = '-' AND substr(recovery_started_at, 8, 1) = '-' AND substr(recovery_started_at, 11, 1) = 'T' AND substr(recovery_started_at, 14, 1) = ':' AND substr(recovery_started_at, 17, 1) = ':' AND substr(recovery_started_at, 20, 1) = '.' AND substr(recovery_started_at, 27, 1) = 'Z' AND substr(recovery_started_at, 1, 4) NOT GLOB '*[^0-9]*' AND substr(recovery_started_at, 6, 2) NOT GLOB '*[^0-9]*' AND substr(recovery_started_at, 9, 2) NOT GLOB '*[^0-9]*' AND substr(recovery_started_at, 12, 2) NOT GLOB '*[^0-9]*' AND substr(recovery_started_at, 15, 2) NOT GLOB '*[^0-9]*' AND substr(recovery_started_at, 18, 2) NOT GLOB '*[^0-9]*' AND substr(recovery_started_at, 21, 6) NOT GLOB '*[^0-9]*' AND substr(recovery_started_at, 1, 4) BETWEEN '0001' AND '9999' AND substr(recovery_started_at, 6, 2) BETWEEN '01' AND '12' AND substr(recovery_started_at, 9, 2) BETWEEN '01' AND '31' AND substr(recovery_started_at, 12, 2) BETWEEN '00' AND '23' AND substr(recovery_started_at, 15, 2) BETWEEN '00' AND '59' AND substr(recovery_started_at, 18, 2) BETWEEN '00' AND '59')), 
	CONSTRAINT ck_runs_recovered_at_utc CHECK (recovered_at IS NULL OR (typeof(recovered_at) = 'text' AND length(recovered_at) = 27 AND substr(recovered_at, 5, 1) = '-' AND substr(recovered_at, 8, 1) = '-' AND substr(recovered_at, 11, 1) = 'T' AND substr(recovered_at, 14, 1) = ':' AND substr(recovered_at, 17, 1) = ':' AND substr(recovered_at, 20, 1) = '.' AND substr(recovered_at, 27, 1) = 'Z' AND substr(recovered_at, 1, 4) NOT GLOB '*[^0-9]*' AND substr(recovered_at, 6, 2) NOT GLOB '*[^0-9]*' AND substr(recovered_at, 9, 2) NOT GLOB '*[^0-9]*' AND substr(recovered_at, 12, 2) NOT GLOB '*[^0-9]*' AND substr(recovered_at, 15, 2) NOT GLOB '*[^0-9]*' AND substr(recovered_at, 18, 2) NOT GLOB '*[^0-9]*' AND substr(recovered_at, 21, 6) NOT GLOB '*[^0-9]*' AND substr(recovered_at, 1, 4) BETWEEN '0001' AND '9999' AND substr(recovered_at, 6, 2) BETWEEN '01' AND '12' AND substr(recovered_at, 9, 2) BETWEEN '01' AND '31' AND substr(recovered_at, 12, 2) BETWEEN '00' AND '23' AND substr(recovered_at, 15, 2) BETWEEN '00' AND '59' AND substr(recovered_at, 18, 2) BETWEEN '00' AND '59')), 
	CONSTRAINT ck_runs_final_fingerprint_shape CHECK (final_reconciliation_fingerprint IS NULL OR (typeof(final_reconciliation_fingerprint) = 'text' AND length(final_reconciliation_fingerprint) = 64 AND final_reconciliation_fingerprint NOT GLOB '*[^0-9a-f]*')), 
	CONSTRAINT ck_runs_terminal_finish CHECK (state NOT IN ('succeeded','partially_succeeded','failed','cancelled') OR finished_at IS NOT NULL), 
	CONSTRAINT ck_runs_final_fingerprint_terminal CHECK (final_reconciliation_fingerprint IS NULL OR state IN ('succeeded','partially_succeeded')), 
	CONSTRAINT ck_runs_started_at_order CHECK (started_at IS NULL OR started_at >= created_at), 
	CONSTRAINT ck_runs_finished_at_order CHECK (finished_at IS NULL OR finished_at >= created_at)
);
CREATE TABLE execution_events (
	run_id VARCHAR(68) NOT NULL, 
	sequence_number INTEGER NOT NULL, 
	event_kind VARCHAR(96) NOT NULL, 
	occurred_at VARCHAR(27) NOT NULL, 
	subject_kind VARCHAR(48) NOT NULL, 
	subject_id VARCHAR(128), 
	correlation_id VARCHAR(96), 
	payload_schema_version INTEGER NOT NULL, 
	payload_json TEXT NOT NULL, 
	CONSTRAINT pk_execution_events PRIMARY KEY (run_id, sequence_number), 
	CONSTRAINT fk_execution_events_run_id_runs FOREIGN KEY(run_id) REFERENCES runs (run_id), 
	CONSTRAINT ck_execution_events_sequence_number_range CHECK (typeof(sequence_number) = 'integer' AND sequence_number BETWEEN 1 AND 2147483647), 
	CONSTRAINT ck_execution_events_event_kind_size CHECK (typeof(event_kind) = 'text' AND length(event_kind) BETWEEN 1 AND 96), 
	CONSTRAINT ck_execution_events_occurred_at_utc CHECK (typeof(occurred_at) = 'text' AND length(occurred_at) = 27 AND substr(occurred_at, 5, 1) = '-' AND substr(occurred_at, 8, 1) = '-' AND substr(occurred_at, 11, 1) = 'T' AND substr(occurred_at, 14, 1) = ':' AND substr(occurred_at, 17, 1) = ':' AND substr(occurred_at, 20, 1) = '.' AND substr(occurred_at, 27, 1) = 'Z' AND substr(occurred_at, 1, 4) NOT GLOB '*[^0-9]*' AND substr(occurred_at, 6, 2) NOT GLOB '*[^0-9]*' AND substr(occurred_at, 9, 2) NOT GLOB '*[^0-9]*' AND substr(occurred_at, 12, 2) NOT GLOB '*[^0-9]*' AND substr(occurred_at, 15, 2) NOT GLOB '*[^0-9]*' AND substr(occurred_at, 18, 2) NOT GLOB '*[^0-9]*' AND substr(occurred_at, 21, 6) NOT GLOB '*[^0-9]*' AND substr(occurred_at, 1, 4) BETWEEN '0001' AND '9999' AND substr(occurred_at, 6, 2) BETWEEN '01' AND '12' AND substr(occurred_at, 9, 2) BETWEEN '01' AND '31' AND substr(occurred_at, 12, 2) BETWEEN '00' AND '23' AND substr(occurred_at, 15, 2) BETWEEN '00' AND '59' AND substr(occurred_at, 18, 2) BETWEEN '00' AND '59'), 
	CONSTRAINT ck_execution_events_subject_kind_size CHECK (typeof(subject_kind) = 'text' AND length(subject_kind) BETWEEN 1 AND 48), 
	CONSTRAINT ck_execution_events_payload_schema_version_range CHECK (typeof(payload_schema_version) = 'integer' AND payload_schema_version BETWEEN 1 AND 2147483647), 
	CONSTRAINT ck_execution_events_payload_json_object CHECK (typeof(payload_json) = 'text' AND json_valid(payload_json) AND json_type(payload_json) = 'object'), 
	CONSTRAINT ck_execution_events_subject_id_size CHECK (subject_id IS NULL OR (typeof(subject_id) = 'text' AND length(subject_id) BETWEEN 1 AND 128)), 
	CONSTRAINT ck_execution_events_correlation_id_size CHECK (correlation_id IS NULL OR (typeof(correlation_id) = 'text' AND length(correlation_id) BETWEEN 1 AND 96))
);
CREATE TABLE reconciliation_conflicts (
	conflict_id VARCHAR(68) NOT NULL, 
	run_id VARCHAR(68) NOT NULL, 
	canonical_key VARCHAR(64) NOT NULL, 
	classification VARCHAR(32) NOT NULL, 
	source_references_json TEXT NOT NULL, 
	target_reference_json TEXT, 
	field_differences_json TEXT NOT NULL, 
	suggested_resolution VARCHAR(32), 
	created_at VARCHAR(27) NOT NULL, 
	CONSTRAINT pk_reconciliation_conflicts PRIMARY KEY (conflict_id), 
	CONSTRAINT uq_reconciliation_conflicts_run_id_canonical_key UNIQUE (run_id, canonical_key), 
	CONSTRAINT uq_reconciliation_conflicts_conflict_id_run_id_canonical_key UNIQUE (conflict_id, run_id, canonical_key), 
	CONSTRAINT fk_reconciliation_conflicts_run_id_runs FOREIGN KEY(run_id) REFERENCES runs (run_id), 
	CONSTRAINT ck_reconciliation_conflicts_conflict_id_shape CHECK (typeof(conflict_id) = 'text' AND length(conflict_id) BETWEEN 7 AND 68 AND substr(conflict_id, 1, 4) = 'cnf_' AND substr(conflict_id, 5) NOT GLOB '*[^a-z0-9-]*' AND substr(conflict_id, 5) NOT LIKE '-%' AND substr(conflict_id, -1) <> '-' AND conflict_id NOT LIKE '%--%'), 
	CONSTRAINT ck_reconciliation_conflicts_canonical_key_size CHECK (typeof(canonical_key) = 'text' AND length(canonical_key) BETWEEN 1 AND 64), 
	CONSTRAINT ck_reconciliation_conflicts_classification_values CHECK (classification IN ('missing_from_target', 'missing_from_source', 'field_mismatch', 'duplicate_source', 'duplicate_target', 'duplicate_both')), 
	CONSTRAINT ck_reconciliation_conflicts_source_references_json_array CHECK (typeof(source_references_json) = 'text' AND json_valid(source_references_json) AND json_type(source_references_json) = 'array'), 
	CONSTRAINT ck_reconciliation_conflicts_target_reference_json_object CHECK (target_reference_json IS NULL OR (typeof(target_reference_json) = 'text' AND json_valid(target_reference_json) AND json_type(target_reference_json) = 'object')), 
	CONSTRAINT ck_reconciliation_conflicts_field_differences_json_array CHECK (typeof(field_differences_json) = 'text' AND json_valid(field_differences_json) AND json_type(field_differences_json) = 'array'), 
	CONSTRAINT ck_reconciliation_conflicts_suggested_resolution_size CHECK (suggested_resolution IS NULL OR (typeof(suggested_resolution) = 'text' AND length(suggested_resolution) BETWEEN 1 AND 32)), 
	CONSTRAINT ck_reconciliation_conflicts_created_at_utc CHECK (typeof(created_at) = 'text' AND length(created_at) = 27 AND substr(created_at, 5, 1) = '-' AND substr(created_at, 8, 1) = '-' AND substr(created_at, 11, 1) = 'T' AND substr(created_at, 14, 1) = ':' AND substr(created_at, 17, 1) = ':' AND substr(created_at, 20, 1) = '.' AND substr(created_at, 27, 1) = 'Z' AND substr(created_at, 1, 4) NOT GLOB '*[^0-9]*' AND substr(created_at, 6, 2) NOT GLOB '*[^0-9]*' AND substr(created_at, 9, 2) NOT GLOB '*[^0-9]*' AND substr(created_at, 12, 2) NOT GLOB '*[^0-9]*' AND substr(created_at, 15, 2) NOT GLOB '*[^0-9]*' AND substr(created_at, 18, 2) NOT GLOB '*[^0-9]*' AND substr(created_at, 21, 6) NOT GLOB '*[^0-9]*' AND substr(created_at, 1, 4) BETWEEN '0001' AND '9999' AND substr(created_at, 6, 2) BETWEEN '01' AND '12' AND substr(created_at, 9, 2) BETWEEN '01' AND '31' AND substr(created_at, 12, 2) BETWEEN '00' AND '23' AND substr(created_at, 15, 2) BETWEEN '00' AND '59' AND substr(created_at, 18, 2) BETWEEN '00' AND '59')
);
CREATE TABLE reconciliation_summaries (
	run_id VARCHAR(68) NOT NULL, 
	match_count INTEGER DEFAULT '0' NOT NULL, 
	missing_from_target_count INTEGER DEFAULT '0' NOT NULL, 
	missing_from_source_count INTEGER DEFAULT '0' NOT NULL, 
	field_mismatch_count INTEGER DEFAULT '0' NOT NULL, 
	duplicate_source_count INTEGER DEFAULT '0' NOT NULL, 
	duplicate_target_count INTEGER DEFAULT '0' NOT NULL, 
	duplicate_both_count INTEGER DEFAULT '0' NOT NULL, 
	total_count INTEGER NOT NULL, 
	source_fingerprint VARCHAR(64) NOT NULL, 
	target_fingerprint VARCHAR(64) NOT NULL, 
	reconciliation_fingerprint VARCHAR(64) NOT NULL, 
	analytical_query_version INTEGER NOT NULL, 
	created_at VARCHAR(27) NOT NULL, 
	CONSTRAINT pk_reconciliation_summaries PRIMARY KEY (run_id), 
	CONSTRAINT uq_reconciliation_summaries_run_id_reconciliation_fingerprint UNIQUE (run_id, reconciliation_fingerprint), 
	CONSTRAINT fk_reconciliation_summaries_run_id_runs FOREIGN KEY(run_id) REFERENCES runs (run_id), 
	CONSTRAINT ck_reconciliation_summaries_match_count_range CHECK (typeof(match_count) = 'integer' AND match_count >= 0), 
	CONSTRAINT ck_reconciliation_summaries_missing_from_target_count_range CHECK (typeof(missing_from_target_count) = 'integer' AND missing_from_target_count >= 0), 
	CONSTRAINT ck_reconciliation_summaries_missing_from_source_count_range CHECK (typeof(missing_from_source_count) = 'integer' AND missing_from_source_count >= 0), 
	CONSTRAINT ck_reconciliation_summaries_field_mismatch_count_range CHECK (typeof(field_mismatch_count) = 'integer' AND field_mismatch_count >= 0), 
	CONSTRAINT ck_reconciliation_summaries_duplicate_source_count_range CHECK (typeof(duplicate_source_count) = 'integer' AND duplicate_source_count >= 0), 
	CONSTRAINT ck_reconciliation_summaries_duplicate_target_count_range CHECK (typeof(duplicate_target_count) = 'integer' AND duplicate_target_count >= 0), 
	CONSTRAINT ck_reconciliation_summaries_duplicate_both_count_range CHECK (typeof(duplicate_both_count) = 'integer' AND duplicate_both_count >= 0), 
	CONSTRAINT ck_reconciliation_summaries_total_count_range CHECK (typeof(total_count) = 'integer' AND total_count >= 0), 
	CONSTRAINT ck_reconciliation_summaries_total_count_sum CHECK (total_count = match_count + missing_from_target_count + missing_from_source_count + field_mismatch_count + duplicate_source_count + duplicate_target_count + duplicate_both_count), 
	CONSTRAINT ck_reconciliation_summaries_source_fingerprint_shape CHECK (typeof(source_fingerprint) = 'text' AND length(source_fingerprint) = 64 AND source_fingerprint NOT GLOB '*[^0-9a-f]*'), 
	CONSTRAINT ck_reconciliation_summaries_target_fingerprint_shape CHECK (typeof(target_fingerprint) = 'text' AND length(target_fingerprint) = 64 AND target_fingerprint NOT GLOB '*[^0-9a-f]*'), 
	CONSTRAINT ck_reconciliation_summaries_reconciliation_fingerprint_shape CHECK (typeof(reconciliation_fingerprint) = 'text' AND length(reconciliation_fingerprint) = 64 AND reconciliation_fingerprint NOT GLOB '*[^0-9a-f]*'), 
	CONSTRAINT ck_reconciliation_summaries_analytical_query_version_range CHECK (typeof(analytical_query_version) = 'integer' AND analytical_query_version BETWEEN 1 AND 2147483647), 
	CONSTRAINT ck_reconciliation_summaries_created_at_utc CHECK (typeof(created_at) = 'text' AND length(created_at) = 27 AND substr(created_at, 5, 1) = '-' AND substr(created_at, 8, 1) = '-' AND substr(created_at, 11, 1) = 'T' AND substr(created_at, 14, 1) = ':' AND substr(created_at, 17, 1) = ':' AND substr(created_at, 20, 1) = '.' AND substr(created_at, 27, 1) = 'Z' AND substr(created_at, 1, 4) NOT GLOB '*[^0-9]*' AND substr(created_at, 6, 2) NOT GLOB '*[^0-9]*' AND substr(created_at, 9, 2) NOT GLOB '*[^0-9]*' AND substr(created_at, 12, 2) NOT GLOB '*[^0-9]*' AND substr(created_at, 15, 2) NOT GLOB '*[^0-9]*' AND substr(created_at, 18, 2) NOT GLOB '*[^0-9]*' AND substr(created_at, 21, 6) NOT GLOB '*[^0-9]*' AND substr(created_at, 1, 4) BETWEEN '0001' AND '9999' AND substr(created_at, 6, 2) BETWEEN '01' AND '12' AND substr(created_at, 9, 2) BETWEEN '01' AND '31' AND substr(created_at, 12, 2) BETWEEN '00' AND '23' AND substr(created_at, 15, 2) BETWEEN '00' AND '59' AND substr(created_at, 18, 2) BETWEEN '00' AND '59')
);
CREATE TABLE run_event_counters (
	run_id VARCHAR(68) NOT NULL, 
	next_sequence_number INTEGER DEFAULT '1' NOT NULL, 
	row_version INTEGER DEFAULT '1' NOT NULL, 
	CONSTRAINT pk_run_event_counters PRIMARY KEY (run_id), 
	CONSTRAINT fk_run_event_counters_run_id_runs FOREIGN KEY(run_id) REFERENCES runs (run_id), 
	CONSTRAINT ck_run_event_counters_next_sequence_number_range CHECK (typeof(next_sequence_number) = 'integer' AND next_sequence_number BETWEEN 1 AND 2147483647), 
	CONSTRAINT ck_run_event_counters_row_version_range CHECK (typeof(row_version) = 'integer' AND row_version BETWEEN 1 AND 2147483647)
);
CREATE TABLE run_nodes (
	run_id VARCHAR(68) NOT NULL, 
	node_id VARCHAR(68) NOT NULL, 
	state VARCHAR(32) NOT NULL, 
	row_version INTEGER DEFAULT '1' NOT NULL, 
	work_total INTEGER DEFAULT '0' NOT NULL, 
	work_pending INTEGER DEFAULT '0' NOT NULL, 
	work_running INTEGER DEFAULT '0' NOT NULL, 
	work_succeeded INTEGER DEFAULT '0' NOT NULL, 
	work_quarantined INTEGER DEFAULT '0' NOT NULL, 
	work_failed INTEGER DEFAULT '0' NOT NULL, 
	work_cancelled INTEGER DEFAULT '0' NOT NULL, 
	records_read INTEGER DEFAULT '0' NOT NULL, 
	records_written INTEGER DEFAULT '0' NOT NULL, 
	records_quarantined INTEGER DEFAULT '0' NOT NULL, 
	bytes_read INTEGER DEFAULT '0' NOT NULL, 
	bytes_written INTEGER DEFAULT '0' NOT NULL, 
	retry_count INTEGER DEFAULT '0' NOT NULL, 
	duration_microseconds INTEGER DEFAULT '0' NOT NULL, 
	started_at VARCHAR(27), 
	finished_at VARCHAR(27), 
	CONSTRAINT pk_run_nodes PRIMARY KEY (run_id, node_id), 
	CONSTRAINT fk_run_nodes_run_id_runs FOREIGN KEY(run_id) REFERENCES runs (run_id), 
	CONSTRAINT ck_run_nodes_node_id_shape CHECK (typeof(node_id) = 'text' AND length(node_id) BETWEEN 7 AND 68 AND substr(node_id, 1, 4) = 'nod_' AND substr(node_id, 5) NOT GLOB '*[^a-z0-9-]*' AND substr(node_id, 5) NOT LIKE '-%' AND substr(node_id, -1) <> '-' AND node_id NOT LIKE '%--%'), 
	CONSTRAINT ck_run_nodes_state_values CHECK (state IN ('pending', 'running', 'succeeded', 'partially_succeeded', 'failed', 'cancelled')), 
	CONSTRAINT ck_run_nodes_row_version_range CHECK (typeof(row_version) = 'integer' AND row_version BETWEEN 1 AND 2147483647), 
	CONSTRAINT ck_run_nodes_work_total_range CHECK (typeof(work_total) = 'integer' AND work_total >= 0), 
	CONSTRAINT ck_run_nodes_work_pending_range CHECK (typeof(work_pending) = 'integer' AND work_pending >= 0), 
	CONSTRAINT ck_run_nodes_work_running_range CHECK (typeof(work_running) = 'integer' AND work_running >= 0), 
	CONSTRAINT ck_run_nodes_work_succeeded_range CHECK (typeof(work_succeeded) = 'integer' AND work_succeeded >= 0), 
	CONSTRAINT ck_run_nodes_work_quarantined_range CHECK (typeof(work_quarantined) = 'integer' AND work_quarantined >= 0), 
	CONSTRAINT ck_run_nodes_work_failed_range CHECK (typeof(work_failed) = 'integer' AND work_failed >= 0), 
	CONSTRAINT ck_run_nodes_work_cancelled_range CHECK (typeof(work_cancelled) = 'integer' AND work_cancelled >= 0), 
	CONSTRAINT ck_run_nodes_records_read_range CHECK (typeof(records_read) = 'integer' AND records_read >= 0), 
	CONSTRAINT ck_run_nodes_records_written_range CHECK (typeof(records_written) = 'integer' AND records_written >= 0), 
	CONSTRAINT ck_run_nodes_records_quarantined_range CHECK (typeof(records_quarantined) = 'integer' AND records_quarantined >= 0), 
	CONSTRAINT ck_run_nodes_bytes_read_range CHECK (typeof(bytes_read) = 'integer' AND bytes_read >= 0), 
	CONSTRAINT ck_run_nodes_bytes_written_range CHECK (typeof(bytes_written) = 'integer' AND bytes_written >= 0), 
	CONSTRAINT ck_run_nodes_retry_count_range CHECK (typeof(retry_count) = 'integer' AND retry_count >= 0), 
	CONSTRAINT ck_run_nodes_duration_range CHECK (typeof(duration_microseconds) = 'integer' AND duration_microseconds >= 0 AND duration_microseconds <= 31536000000000), 
	CONSTRAINT ck_run_nodes_started_at_utc CHECK (started_at IS NULL OR (typeof(started_at) = 'text' AND length(started_at) = 27 AND substr(started_at, 5, 1) = '-' AND substr(started_at, 8, 1) = '-' AND substr(started_at, 11, 1) = 'T' AND substr(started_at, 14, 1) = ':' AND substr(started_at, 17, 1) = ':' AND substr(started_at, 20, 1) = '.' AND substr(started_at, 27, 1) = 'Z' AND substr(started_at, 1, 4) NOT GLOB '*[^0-9]*' AND substr(started_at, 6, 2) NOT GLOB '*[^0-9]*' AND substr(started_at, 9, 2) NOT GLOB '*[^0-9]*' AND substr(started_at, 12, 2) NOT GLOB '*[^0-9]*' AND substr(started_at, 15, 2) NOT GLOB '*[^0-9]*' AND substr(started_at, 18, 2) NOT GLOB '*[^0-9]*' AND substr(started_at, 21, 6) NOT GLOB '*[^0-9]*' AND substr(started_at, 1, 4) BETWEEN '0001' AND '9999' AND substr(started_at, 6, 2) BETWEEN '01' AND '12' AND substr(started_at, 9, 2) BETWEEN '01' AND '31' AND substr(started_at, 12, 2) BETWEEN '00' AND '23' AND substr(started_at, 15, 2) BETWEEN '00' AND '59' AND substr(started_at, 18, 2) BETWEEN '00' AND '59')), 
	CONSTRAINT ck_run_nodes_finished_at_utc CHECK (finished_at IS NULL OR (typeof(finished_at) = 'text' AND length(finished_at) = 27 AND substr(finished_at, 5, 1) = '-' AND substr(finished_at, 8, 1) = '-' AND substr(finished_at, 11, 1) = 'T' AND substr(finished_at, 14, 1) = ':' AND substr(finished_at, 17, 1) = ':' AND substr(finished_at, 20, 1) = '.' AND substr(finished_at, 27, 1) = 'Z' AND substr(finished_at, 1, 4) NOT GLOB '*[^0-9]*' AND substr(finished_at, 6, 2) NOT GLOB '*[^0-9]*' AND substr(finished_at, 9, 2) NOT GLOB '*[^0-9]*' AND substr(finished_at, 12, 2) NOT GLOB '*[^0-9]*' AND substr(finished_at, 15, 2) NOT GLOB '*[^0-9]*' AND substr(finished_at, 18, 2) NOT GLOB '*[^0-9]*' AND substr(finished_at, 21, 6) NOT GLOB '*[^0-9]*' AND substr(finished_at, 1, 4) BETWEEN '0001' AND '9999' AND substr(finished_at, 6, 2) BETWEEN '01' AND '12' AND substr(finished_at, 9, 2) BETWEEN '01' AND '31' AND substr(finished_at, 12, 2) BETWEEN '00' AND '23' AND substr(finished_at, 15, 2) BETWEEN '00' AND '59' AND substr(finished_at, 18, 2) BETWEEN '00' AND '59')), 
	CONSTRAINT ck_run_nodes_work_count_sum CHECK (work_pending + work_running + work_succeeded + work_quarantined + work_failed + work_cancelled <= work_total)
);
CREATE TABLE artifact_manifests (
	artifact_id VARCHAR(68) NOT NULL, 
	run_id VARCHAR(68) NOT NULL, 
	node_id VARCHAR(68) NOT NULL, 
	partition_key VARCHAR(128) NOT NULL, 
	relative_path TEXT NOT NULL, 
	media_type VARCHAR(127) NOT NULL, 
	schema_version INTEGER NOT NULL, 
	byte_size INTEGER NOT NULL, 
	row_count INTEGER NOT NULL, 
	sha256 VARCHAR(64) NOT NULL, 
	created_at VARCHAR(27) NOT NULL, 
	CONSTRAINT pk_artifact_manifests PRIMARY KEY (artifact_id), 
	CONSTRAINT fk_artifact_manifests_run_id_node_id_run_nodes FOREIGN KEY(run_id, node_id) REFERENCES run_nodes (run_id, node_id), 
	CONSTRAINT uq_artifact_manifests_relative_path UNIQUE (relative_path), 
	CONSTRAINT uq_artifact_manifests_artifact_id_run_id_node_id_partition_key UNIQUE (artifact_id, run_id, node_id, partition_key), 
	CONSTRAINT ck_artifact_manifests_artifact_id_shape CHECK (typeof(artifact_id) = 'text' AND length(artifact_id) BETWEEN 7 AND 68 AND substr(artifact_id, 1, 4) = 'art_' AND substr(artifact_id, 5) NOT GLOB '*[^a-z0-9-]*' AND substr(artifact_id, 5) NOT LIKE '-%' AND substr(artifact_id, -1) <> '-' AND artifact_id NOT LIKE '%--%'), 
	CONSTRAINT ck_artifact_manifests_partition_key_size CHECK (typeof(partition_key) = 'text' AND length(partition_key) BETWEEN 1 AND 128), 
	CONSTRAINT ck_artifact_manifests_relative_path_basic_shape CHECK (typeof(relative_path) = 'text' AND length(relative_path) BETWEEN 1 AND 1024 AND substr(relative_path,1,1) NOT IN ('/','\') AND instr(relative_path, char(0)) = 0), 
	CONSTRAINT ck_artifact_manifests_media_type_size CHECK (typeof(media_type) = 'text' AND length(media_type) BETWEEN 1 AND 127), 
	CONSTRAINT ck_artifact_manifests_schema_version_range CHECK (typeof(schema_version) = 'integer' AND schema_version BETWEEN 1 AND 2147483647), 
	CONSTRAINT ck_artifact_manifests_byte_size_range CHECK (typeof(byte_size) = 'integer' AND byte_size >= 0), 
	CONSTRAINT ck_artifact_manifests_row_count_range CHECK (typeof(row_count) = 'integer' AND row_count >= 0), 
	CONSTRAINT ck_artifact_manifests_sha256_shape CHECK (typeof(sha256) = 'text' AND length(sha256) = 64 AND sha256 NOT GLOB '*[^0-9a-f]*'), 
	CONSTRAINT ck_artifact_manifests_created_at_utc CHECK (typeof(created_at) = 'text' AND length(created_at) = 27 AND substr(created_at, 5, 1) = '-' AND substr(created_at, 8, 1) = '-' AND substr(created_at, 11, 1) = 'T' AND substr(created_at, 14, 1) = ':' AND substr(created_at, 17, 1) = ':' AND substr(created_at, 20, 1) = '.' AND substr(created_at, 27, 1) = 'Z' AND substr(created_at, 1, 4) NOT GLOB '*[^0-9]*' AND substr(created_at, 6, 2) NOT GLOB '*[^0-9]*' AND substr(created_at, 9, 2) NOT GLOB '*[^0-9]*' AND substr(created_at, 12, 2) NOT GLOB '*[^0-9]*' AND substr(created_at, 15, 2) NOT GLOB '*[^0-9]*' AND substr(created_at, 18, 2) NOT GLOB '*[^0-9]*' AND substr(created_at, 21, 6) NOT GLOB '*[^0-9]*' AND substr(created_at, 1, 4) BETWEEN '0001' AND '9999' AND substr(created_at, 6, 2) BETWEEN '01' AND '12' AND substr(created_at, 9, 2) BETWEEN '01' AND '31' AND substr(created_at, 12, 2) BETWEEN '00' AND '23' AND substr(created_at, 15, 2) BETWEEN '00' AND '59' AND substr(created_at, 18, 2) BETWEEN '00' AND '59')
);
CREATE TABLE repair_plans (
	repair_plan_id VARCHAR(68) NOT NULL, 
	run_id VARCHAR(68) NOT NULL, 
	reconciliation_fingerprint VARCHAR(64) NOT NULL, 
	content_fingerprint VARCHAR(64) NOT NULL, 
	status VARCHAR(32) NOT NULL, 
	row_version INTEGER DEFAULT '1' NOT NULL, 
	created_at VARCHAR(27) NOT NULL, 
	applying_at VARCHAR(27), 
	applied_at VARCHAR(27), 
	rejected_at VARCHAR(27), 
	failed_at VARCHAR(27), 
	failure_detail TEXT, 
	CONSTRAINT pk_repair_plans PRIMARY KEY (repair_plan_id), 
	CONSTRAINT uq_repair_plans_repair_plan_id_run_id UNIQUE (repair_plan_id, run_id), 
	CONSTRAINT uq_repair_plans_repair_plan_id_reconciliation_fingerprint UNIQUE (repair_plan_id, reconciliation_fingerprint), 
	CONSTRAINT uq_repair_plans_run_id_content_fingerprint UNIQUE (run_id, content_fingerprint), 
	CONSTRAINT fk_repair_plans_run_id_reconciliation_fingerprint_reconciliation_summaries FOREIGN KEY(run_id, reconciliation_fingerprint) REFERENCES reconciliation_summaries (run_id, reconciliation_fingerprint), 
	CONSTRAINT ck_repair_plans_repair_plan_id_shape CHECK (typeof(repair_plan_id) = 'text' AND length(repair_plan_id) BETWEEN 7 AND 68 AND substr(repair_plan_id, 1, 4) = 'rpl_' AND substr(repair_plan_id, 5) NOT GLOB '*[^a-z0-9-]*' AND substr(repair_plan_id, 5) NOT LIKE '-%' AND substr(repair_plan_id, -1) <> '-' AND repair_plan_id NOT LIKE '%--%'), 
	CONSTRAINT ck_repair_plans_reconciliation_fingerprint_shape CHECK (typeof(reconciliation_fingerprint) = 'text' AND length(reconciliation_fingerprint) = 64 AND reconciliation_fingerprint NOT GLOB '*[^0-9a-f]*'), 
	CONSTRAINT ck_repair_plans_content_fingerprint_shape CHECK (typeof(content_fingerprint) = 'text' AND length(content_fingerprint) = 64 AND content_fingerprint NOT GLOB '*[^0-9a-f]*'), 
	CONSTRAINT ck_repair_plans_status_values CHECK (status IN ('proposed', 'approved', 'applying', 'applied', 'rejected', 'failed')), 
	CONSTRAINT ck_repair_plans_row_version_range CHECK (typeof(row_version) = 'integer' AND row_version BETWEEN 1 AND 2147483647), 
	CONSTRAINT ck_repair_plans_created_at_utc CHECK (typeof(created_at) = 'text' AND length(created_at) = 27 AND substr(created_at, 5, 1) = '-' AND substr(created_at, 8, 1) = '-' AND substr(created_at, 11, 1) = 'T' AND substr(created_at, 14, 1) = ':' AND substr(created_at, 17, 1) = ':' AND substr(created_at, 20, 1) = '.' AND substr(created_at, 27, 1) = 'Z' AND substr(created_at, 1, 4) NOT GLOB '*[^0-9]*' AND substr(created_at, 6, 2) NOT GLOB '*[^0-9]*' AND substr(created_at, 9, 2) NOT GLOB '*[^0-9]*' AND substr(created_at, 12, 2) NOT GLOB '*[^0-9]*' AND substr(created_at, 15, 2) NOT GLOB '*[^0-9]*' AND substr(created_at, 18, 2) NOT GLOB '*[^0-9]*' AND substr(created_at, 21, 6) NOT GLOB '*[^0-9]*' AND substr(created_at, 1, 4) BETWEEN '0001' AND '9999' AND substr(created_at, 6, 2) BETWEEN '01' AND '12' AND substr(created_at, 9, 2) BETWEEN '01' AND '31' AND substr(created_at, 12, 2) BETWEEN '00' AND '23' AND substr(created_at, 15, 2) BETWEEN '00' AND '59' AND substr(created_at, 18, 2) BETWEEN '00' AND '59'), 
	CONSTRAINT ck_repair_plans_applying_at_utc CHECK (applying_at IS NULL OR (typeof(applying_at) = 'text' AND length(applying_at) = 27 AND substr(applying_at, 5, 1) = '-' AND substr(applying_at, 8, 1) = '-' AND substr(applying_at, 11, 1) = 'T' AND substr(applying_at, 14, 1) = ':' AND substr(applying_at, 17, 1) = ':' AND substr(applying_at, 20, 1) = '.' AND substr(applying_at, 27, 1) = 'Z' AND substr(applying_at, 1, 4) NOT GLOB '*[^0-9]*' AND substr(applying_at, 6, 2) NOT GLOB '*[^0-9]*' AND substr(applying_at, 9, 2) NOT GLOB '*[^0-9]*' AND substr(applying_at, 12, 2) NOT GLOB '*[^0-9]*' AND substr(applying_at, 15, 2) NOT GLOB '*[^0-9]*' AND substr(applying_at, 18, 2) NOT GLOB '*[^0-9]*' AND substr(applying_at, 21, 6) NOT GLOB '*[^0-9]*' AND substr(applying_at, 1, 4) BETWEEN '0001' AND '9999' AND substr(applying_at, 6, 2) BETWEEN '01' AND '12' AND substr(applying_at, 9, 2) BETWEEN '01' AND '31' AND substr(applying_at, 12, 2) BETWEEN '00' AND '23' AND substr(applying_at, 15, 2) BETWEEN '00' AND '59' AND substr(applying_at, 18, 2) BETWEEN '00' AND '59')), 
	CONSTRAINT ck_repair_plans_applied_at_utc CHECK (applied_at IS NULL OR (typeof(applied_at) = 'text' AND length(applied_at) = 27 AND substr(applied_at, 5, 1) = '-' AND substr(applied_at, 8, 1) = '-' AND substr(applied_at, 11, 1) = 'T' AND substr(applied_at, 14, 1) = ':' AND substr(applied_at, 17, 1) = ':' AND substr(applied_at, 20, 1) = '.' AND substr(applied_at, 27, 1) = 'Z' AND substr(applied_at, 1, 4) NOT GLOB '*[^0-9]*' AND substr(applied_at, 6, 2) NOT GLOB '*[^0-9]*' AND substr(applied_at, 9, 2) NOT GLOB '*[^0-9]*' AND substr(applied_at, 12, 2) NOT GLOB '*[^0-9]*' AND substr(applied_at, 15, 2) NOT GLOB '*[^0-9]*' AND substr(applied_at, 18, 2) NOT GLOB '*[^0-9]*' AND substr(applied_at, 21, 6) NOT GLOB '*[^0-9]*' AND substr(applied_at, 1, 4) BETWEEN '0001' AND '9999' AND substr(applied_at, 6, 2) BETWEEN '01' AND '12' AND substr(applied_at, 9, 2) BETWEEN '01' AND '31' AND substr(applied_at, 12, 2) BETWEEN '00' AND '23' AND substr(applied_at, 15, 2) BETWEEN '00' AND '59' AND substr(applied_at, 18, 2) BETWEEN '00' AND '59')), 
	CONSTRAINT ck_repair_plans_rejected_at_utc CHECK (rejected_at IS NULL OR (typeof(rejected_at) = 'text' AND length(rejected_at) = 27 AND substr(rejected_at, 5, 1) = '-' AND substr(rejected_at, 8, 1) = '-' AND substr(rejected_at, 11, 1) = 'T' AND substr(rejected_at, 14, 1) = ':' AND substr(rejected_at, 17, 1) = ':' AND substr(rejected_at, 20, 1) = '.' AND substr(rejected_at, 27, 1) = 'Z' AND substr(rejected_at, 1, 4) NOT GLOB '*[^0-9]*' AND substr(rejected_at, 6, 2) NOT GLOB '*[^0-9]*' AND substr(rejected_at, 9, 2) NOT GLOB '*[^0-9]*' AND substr(rejected_at, 12, 2) NOT GLOB '*[^0-9]*' AND substr(rejected_at, 15, 2) NOT GLOB '*[^0-9]*' AND substr(rejected_at, 18, 2) NOT GLOB '*[^0-9]*' AND substr(rejected_at, 21, 6) NOT GLOB '*[^0-9]*' AND substr(rejected_at, 1, 4) BETWEEN '0001' AND '9999' AND substr(rejected_at, 6, 2) BETWEEN '01' AND '12' AND substr(rejected_at, 9, 2) BETWEEN '01' AND '31' AND substr(rejected_at, 12, 2) BETWEEN '00' AND '23' AND substr(rejected_at, 15, 2) BETWEEN '00' AND '59' AND substr(rejected_at, 18, 2) BETWEEN '00' AND '59')), 
	CONSTRAINT ck_repair_plans_failed_at_utc CHECK (failed_at IS NULL OR (typeof(failed_at) = 'text' AND length(failed_at) = 27 AND substr(failed_at, 5, 1) = '-' AND substr(failed_at, 8, 1) = '-' AND substr(failed_at, 11, 1) = 'T' AND substr(failed_at, 14, 1) = ':' AND substr(failed_at, 17, 1) = ':' AND substr(failed_at, 20, 1) = '.' AND substr(failed_at, 27, 1) = 'Z' AND substr(failed_at, 1, 4) NOT GLOB '*[^0-9]*' AND substr(failed_at, 6, 2) NOT GLOB '*[^0-9]*' AND substr(failed_at, 9, 2) NOT GLOB '*[^0-9]*' AND substr(failed_at, 12, 2) NOT GLOB '*[^0-9]*' AND substr(failed_at, 15, 2) NOT GLOB '*[^0-9]*' AND substr(failed_at, 18, 2) NOT GLOB '*[^0-9]*' AND substr(failed_at, 21, 6) NOT GLOB '*[^0-9]*' AND substr(failed_at, 1, 4) BETWEEN '0001' AND '9999' AND substr(failed_at, 6, 2) BETWEEN '01' AND '12' AND substr(failed_at, 9, 2) BETWEEN '01' AND '31' AND substr(failed_at, 12, 2) BETWEEN '00' AND '23' AND substr(failed_at, 15, 2) BETWEEN '00' AND '59' AND substr(failed_at, 18, 2) BETWEEN '00' AND '59')), 
	CONSTRAINT ck_repair_plans_applied_time CHECK (status <> 'applied' OR applied_at IS NOT NULL), 
	CONSTRAINT ck_repair_plans_rejected_time CHECK (status <> 'rejected' OR rejected_at IS NOT NULL), 
	CONSTRAINT ck_repair_plans_failed_time CHECK (status <> 'failed' OR failed_at IS NOT NULL), 
	CONSTRAINT ck_repair_plans_failure_detail_size CHECK (failure_detail IS NULL OR (typeof(failure_detail) = 'text' AND length(failure_detail) BETWEEN 1 AND 4096))
);
CREATE TABLE work_items (
	work_item_id VARCHAR(68) NOT NULL, 
	run_id VARCHAR(68) NOT NULL, 
	node_id VARCHAR(68) NOT NULL, 
	partition_key VARCHAR(128) NOT NULL, 
	state VARCHAR(32) NOT NULL, 
	row_version INTEGER DEFAULT '1' NOT NULL, 
	completed_attempt_count INTEGER DEFAULT '0' NOT NULL, 
	expected_checkpoint_version INTEGER DEFAULT '0' NOT NULL, 
	input_reference_json TEXT, 
	retry_available_at VARCHAR(27), 
	lease_owner VARCHAR(128), 
	lease_expires_at VARCHAR(27), 
	active_attempt_number INTEGER, 
	active_attempt_started_at VARCHAR(27), 
	active_runner_kind VARCHAR(32), 
	active_worker_identity VARCHAR(128), 
	created_at VARCHAR(27) NOT NULL, 
	updated_at VARCHAR(27) NOT NULL, 
	CONSTRAINT pk_work_items PRIMARY KEY (work_item_id), 
	CONSTRAINT uq_work_items_run_id_node_id_partition_key UNIQUE (run_id, node_id, partition_key), 
	CONSTRAINT fk_work_items_run_id_node_id_run_nodes FOREIGN KEY(run_id, node_id) REFERENCES run_nodes (run_id, node_id), 
	CONSTRAINT ck_work_items_work_item_id_shape CHECK (typeof(work_item_id) = 'text' AND length(work_item_id) BETWEEN 7 AND 68 AND substr(work_item_id, 1, 4) = 'wrk_' AND substr(work_item_id, 5) NOT GLOB '*[^a-z0-9-]*' AND substr(work_item_id, 5) NOT LIKE '-%' AND substr(work_item_id, -1) <> '-' AND work_item_id NOT LIKE '%--%'), 
	CONSTRAINT ck_work_items_partition_key_size CHECK (typeof(partition_key) = 'text' AND length(partition_key) BETWEEN 1 AND 128), 
	CONSTRAINT ck_work_items_state_values CHECK (state IN ('pending', 'running', 'succeeded', 'retry_wait', 'quarantined', 'failed', 'cancelled')), 
	CONSTRAINT ck_work_items_row_version_range CHECK (typeof(row_version) = 'integer' AND row_version BETWEEN 1 AND 2147483647), 
	CONSTRAINT ck_work_items_completed_attempt_count_range CHECK (typeof(completed_attempt_count) = 'integer' AND completed_attempt_count >= 0 AND completed_attempt_count <= 2147483647), 
	CONSTRAINT ck_work_items_expected_checkpoint_version_range CHECK (typeof(expected_checkpoint_version) = 'integer' AND expected_checkpoint_version >= 0 AND expected_checkpoint_version <= 2147483647), 
	CONSTRAINT ck_work_items_input_reference_json_object CHECK (input_reference_json IS NULL OR (typeof(input_reference_json) = 'text' AND json_valid(input_reference_json) AND json_type(input_reference_json) = 'object')), 
	CONSTRAINT ck_work_items_retry_available_at_utc CHECK (retry_available_at IS NULL OR (typeof(retry_available_at) = 'text' AND length(retry_available_at) = 27 AND substr(retry_available_at, 5, 1) = '-' AND substr(retry_available_at, 8, 1) = '-' AND substr(retry_available_at, 11, 1) = 'T' AND substr(retry_available_at, 14, 1) = ':' AND substr(retry_available_at, 17, 1) = ':' AND substr(retry_available_at, 20, 1) = '.' AND substr(retry_available_at, 27, 1) = 'Z' AND substr(retry_available_at, 1, 4) NOT GLOB '*[^0-9]*' AND substr(retry_available_at, 6, 2) NOT GLOB '*[^0-9]*' AND substr(retry_available_at, 9, 2) NOT GLOB '*[^0-9]*' AND substr(retry_available_at, 12, 2) NOT GLOB '*[^0-9]*' AND substr(retry_available_at, 15, 2) NOT GLOB '*[^0-9]*' AND substr(retry_available_at, 18, 2) NOT GLOB '*[^0-9]*' AND substr(retry_available_at, 21, 6) NOT GLOB '*[^0-9]*' AND substr(retry_available_at, 1, 4) BETWEEN '0001' AND '9999' AND substr(retry_available_at, 6, 2) BETWEEN '01' AND '12' AND substr(retry_available_at, 9, 2) BETWEEN '01' AND '31' AND substr(retry_available_at, 12, 2) BETWEEN '00' AND '23' AND substr(retry_available_at, 15, 2) BETWEEN '00' AND '59' AND substr(retry_available_at, 18, 2) BETWEEN '00' AND '59')), 
	CONSTRAINT ck_work_items_lease_expires_at_utc CHECK (lease_expires_at IS NULL OR (typeof(lease_expires_at) = 'text' AND length(lease_expires_at) = 27 AND substr(lease_expires_at, 5, 1) = '-' AND substr(lease_expires_at, 8, 1) = '-' AND substr(lease_expires_at, 11, 1) = 'T' AND substr(lease_expires_at, 14, 1) = ':' AND substr(lease_expires_at, 17, 1) = ':' AND substr(lease_expires_at, 20, 1) = '.' AND substr(lease_expires_at, 27, 1) = 'Z' AND substr(lease_expires_at, 1, 4) NOT GLOB '*[^0-9]*' AND substr(lease_expires_at, 6, 2) NOT GLOB '*[^0-9]*' AND substr(lease_expires_at, 9, 2) NOT GLOB '*[^0-9]*' AND substr(lease_expires_at, 12, 2) NOT GLOB '*[^0-9]*' AND substr(lease_expires_at, 15, 2) NOT GLOB '*[^0-9]*' AND substr(lease_expires_at, 18, 2) NOT GLOB '*[^0-9]*' AND substr(lease_expires_at, 21, 6) NOT GLOB '*[^0-9]*' AND substr(lease_expires_at, 1, 4) BETWEEN '0001' AND '9999' AND substr(lease_expires_at, 6, 2) BETWEEN '01' AND '12' AND substr(lease_expires_at, 9, 2) BETWEEN '01' AND '31' AND substr(lease_expires_at, 12, 2) BETWEEN '00' AND '23' AND substr(lease_expires_at, 15, 2) BETWEEN '00' AND '59' AND substr(lease_expires_at, 18, 2) BETWEEN '00' AND '59')), 
	CONSTRAINT ck_work_items_active_attempt_started_at_utc CHECK (active_attempt_started_at IS NULL OR (typeof(active_attempt_started_at) = 'text' AND length(active_attempt_started_at) = 27 AND substr(active_attempt_started_at, 5, 1) = '-' AND substr(active_attempt_started_at, 8, 1) = '-' AND substr(active_attempt_started_at, 11, 1) = 'T' AND substr(active_attempt_started_at, 14, 1) = ':' AND substr(active_attempt_started_at, 17, 1) = ':' AND substr(active_attempt_started_at, 20, 1) = '.' AND substr(active_attempt_started_at, 27, 1) = 'Z' AND substr(active_attempt_started_at, 1, 4) NOT GLOB '*[^0-9]*' AND substr(active_attempt_started_at, 6, 2) NOT GLOB '*[^0-9]*' AND substr(active_attempt_started_at, 9, 2) NOT GLOB '*[^0-9]*' AND substr(active_attempt_started_at, 12, 2) NOT GLOB '*[^0-9]*' AND substr(active_attempt_started_at, 15, 2) NOT GLOB '*[^0-9]*' AND substr(active_attempt_started_at, 18, 2) NOT GLOB '*[^0-9]*' AND substr(active_attempt_started_at, 21, 6) NOT GLOB '*[^0-9]*' AND substr(active_attempt_started_at, 1, 4) BETWEEN '0001' AND '9999' AND substr(active_attempt_started_at, 6, 2) BETWEEN '01' AND '12' AND substr(active_attempt_started_at, 9, 2) BETWEEN '01' AND '31' AND substr(active_attempt_started_at, 12, 2) BETWEEN '00' AND '23' AND substr(active_attempt_started_at, 15, 2) BETWEEN '00' AND '59' AND substr(active_attempt_started_at, 18, 2) BETWEEN '00' AND '59')), 
	CONSTRAINT ck_work_items_created_at_utc CHECK (typeof(created_at) = 'text' AND length(created_at) = 27 AND substr(created_at, 5, 1) = '-' AND substr(created_at, 8, 1) = '-' AND substr(created_at, 11, 1) = 'T' AND substr(created_at, 14, 1) = ':' AND substr(created_at, 17, 1) = ':' AND substr(created_at, 20, 1) = '.' AND substr(created_at, 27, 1) = 'Z' AND substr(created_at, 1, 4) NOT GLOB '*[^0-9]*' AND substr(created_at, 6, 2) NOT GLOB '*[^0-9]*' AND substr(created_at, 9, 2) NOT GLOB '*[^0-9]*' AND substr(created_at, 12, 2) NOT GLOB '*[^0-9]*' AND substr(created_at, 15, 2) NOT GLOB '*[^0-9]*' AND substr(created_at, 18, 2) NOT GLOB '*[^0-9]*' AND substr(created_at, 21, 6) NOT GLOB '*[^0-9]*' AND substr(created_at, 1, 4) BETWEEN '0001' AND '9999' AND substr(created_at, 6, 2) BETWEEN '01' AND '12' AND substr(created_at, 9, 2) BETWEEN '01' AND '31' AND substr(created_at, 12, 2) BETWEEN '00' AND '23' AND substr(created_at, 15, 2) BETWEEN '00' AND '59' AND substr(created_at, 18, 2) BETWEEN '00' AND '59'), 
	CONSTRAINT ck_work_items_updated_at_utc CHECK (typeof(updated_at) = 'text' AND length(updated_at) = 27 AND substr(updated_at, 5, 1) = '-' AND substr(updated_at, 8, 1) = '-' AND substr(updated_at, 11, 1) = 'T' AND substr(updated_at, 14, 1) = ':' AND substr(updated_at, 17, 1) = ':' AND substr(updated_at, 20, 1) = '.' AND substr(updated_at, 27, 1) = 'Z' AND substr(updated_at, 1, 4) NOT GLOB '*[^0-9]*' AND substr(updated_at, 6, 2) NOT GLOB '*[^0-9]*' AND substr(updated_at, 9, 2) NOT GLOB '*[^0-9]*' AND substr(updated_at, 12, 2) NOT GLOB '*[^0-9]*' AND substr(updated_at, 15, 2) NOT GLOB '*[^0-9]*' AND substr(updated_at, 18, 2) NOT GLOB '*[^0-9]*' AND substr(updated_at, 21, 6) NOT GLOB '*[^0-9]*' AND substr(updated_at, 1, 4) BETWEEN '0001' AND '9999' AND substr(updated_at, 6, 2) BETWEEN '01' AND '12' AND substr(updated_at, 9, 2) BETWEEN '01' AND '31' AND substr(updated_at, 12, 2) BETWEEN '00' AND '23' AND substr(updated_at, 15, 2) BETWEEN '00' AND '59' AND substr(updated_at, 18, 2) BETWEEN '00' AND '59'), 
	CONSTRAINT ck_work_items_updated_at_order CHECK (updated_at >= created_at), 
	CONSTRAINT ck_work_items_lease_owner_size CHECK (lease_owner IS NULL OR (typeof(lease_owner) = 'text' AND length(lease_owner) BETWEEN 1 AND 128)), 
	CONSTRAINT ck_work_items_active_runner_kind_size CHECK (active_runner_kind IS NULL OR (typeof(active_runner_kind) = 'text' AND length(active_runner_kind) BETWEEN 1 AND 32)), 
	CONSTRAINT ck_work_items_active_worker_identity_size CHECK (active_worker_identity IS NULL OR (typeof(active_worker_identity) = 'text' AND length(active_worker_identity) BETWEEN 1 AND 128)), 
	CONSTRAINT ck_work_items_active_attempt_coherence CHECK ((state = 'running' AND lease_owner IS NOT NULL AND lease_expires_at IS NOT NULL AND active_attempt_number IS NOT NULL AND active_attempt_started_at IS NOT NULL AND active_runner_kind IS NOT NULL AND active_worker_identity IS NOT NULL) OR (state <> 'running' AND lease_owner IS NULL AND lease_expires_at IS NULL AND active_attempt_number IS NULL AND active_attempt_started_at IS NULL AND active_runner_kind IS NULL AND active_worker_identity IS NULL)), 
	CONSTRAINT ck_work_items_active_attempt_number_order CHECK (active_attempt_number IS NULL OR active_attempt_number = completed_attempt_count + 1), 
	CONSTRAINT ck_work_items_lease_time_order CHECK (lease_expires_at IS NULL OR lease_expires_at > active_attempt_started_at), 
	CONSTRAINT ck_work_items_retry_time_coherence CHECK ((state = 'retry_wait' AND retry_available_at IS NOT NULL) OR (state <> 'retry_wait' AND retry_available_at IS NULL))
);
CREATE TABLE checkpoint_heads (
	run_id VARCHAR(68) NOT NULL, 
	node_id VARCHAR(68) NOT NULL, 
	partition_key VARCHAR(128) NOT NULL, 
	current_version INTEGER DEFAULT '0' NOT NULL, 
	updated_at VARCHAR(27) NOT NULL, 
	row_version INTEGER DEFAULT '1' NOT NULL, 
	CONSTRAINT pk_checkpoint_heads PRIMARY KEY (run_id, node_id, partition_key), 
	CONSTRAINT fk_checkpoint_heads_run_id_node_id_partition_key_work_items FOREIGN KEY(run_id, node_id, partition_key) REFERENCES work_items (run_id, node_id, partition_key), 
	CONSTRAINT ck_checkpoint_heads_current_version_range CHECK (typeof(current_version) = 'integer' AND current_version >= 0 AND current_version <= 2147483647), 
	CONSTRAINT ck_checkpoint_heads_updated_at_utc CHECK (typeof(updated_at) = 'text' AND length(updated_at) = 27 AND substr(updated_at, 5, 1) = '-' AND substr(updated_at, 8, 1) = '-' AND substr(updated_at, 11, 1) = 'T' AND substr(updated_at, 14, 1) = ':' AND substr(updated_at, 17, 1) = ':' AND substr(updated_at, 20, 1) = '.' AND substr(updated_at, 27, 1) = 'Z' AND substr(updated_at, 1, 4) NOT GLOB '*[^0-9]*' AND substr(updated_at, 6, 2) NOT GLOB '*[^0-9]*' AND substr(updated_at, 9, 2) NOT GLOB '*[^0-9]*' AND substr(updated_at, 12, 2) NOT GLOB '*[^0-9]*' AND substr(updated_at, 15, 2) NOT GLOB '*[^0-9]*' AND substr(updated_at, 18, 2) NOT GLOB '*[^0-9]*' AND substr(updated_at, 21, 6) NOT GLOB '*[^0-9]*' AND substr(updated_at, 1, 4) BETWEEN '0001' AND '9999' AND substr(updated_at, 6, 2) BETWEEN '01' AND '12' AND substr(updated_at, 9, 2) BETWEEN '01' AND '31' AND substr(updated_at, 12, 2) BETWEEN '00' AND '23' AND substr(updated_at, 15, 2) BETWEEN '00' AND '59' AND substr(updated_at, 18, 2) BETWEEN '00' AND '59'), 
	CONSTRAINT ck_checkpoint_heads_row_version_range CHECK (typeof(row_version) = 'integer' AND row_version BETWEEN 1 AND 2147483647)
);
CREATE TABLE repair_actions (
	repair_action_id VARCHAR(68) NOT NULL, 
	repair_plan_id VARCHAR(68) NOT NULL, 
	run_id VARCHAR(68) NOT NULL, 
	conflict_id VARCHAR(68) NOT NULL, 
	canonical_key VARCHAR(64) NOT NULL, 
	action_kind VARCHAR(32) NOT NULL, 
	external_idempotency_key VARCHAR(128) NOT NULL, 
	before_sha256 VARCHAR(64), 
	proposed_after_sha256 VARCHAR(64) NOT NULL, 
	proposed_record_json TEXT NOT NULL, 
	expected_target_record_json TEXT, 
	mismatch_evidence_json TEXT NOT NULL, 
	application_status VARCHAR(32) DEFAULT '''pending''' NOT NULL, 
	application_result_json TEXT, 
	target_version INTEGER, 
	applied_at VARCHAR(27), 
	failed_at VARCHAR(27), 
	CONSTRAINT pk_repair_actions PRIMARY KEY (repair_action_id), 
	CONSTRAINT uq_repair_actions_external_idempotency_key UNIQUE (external_idempotency_key), 
	CONSTRAINT uq_repair_actions_repair_plan_id_canonical_key_action_kind UNIQUE (repair_plan_id, canonical_key, action_kind), 
	CONSTRAINT fk_repair_actions_repair_plan_id_run_id_repair_plans FOREIGN KEY(repair_plan_id, run_id) REFERENCES repair_plans (repair_plan_id, run_id), 
	CONSTRAINT fk_repair_actions_conflict_id_run_id_canonical_key_reconciliation_conflicts FOREIGN KEY(conflict_id, run_id, canonical_key) REFERENCES reconciliation_conflicts (conflict_id, run_id, canonical_key), 
	CONSTRAINT ck_repair_actions_repair_action_id_shape CHECK (typeof(repair_action_id) = 'text' AND length(repair_action_id) BETWEEN 7 AND 68 AND substr(repair_action_id, 1, 4) = 'rac_' AND substr(repair_action_id, 5) NOT GLOB '*[^a-z0-9-]*' AND substr(repair_action_id, 5) NOT LIKE '-%' AND substr(repair_action_id, -1) <> '-' AND repair_action_id NOT LIKE '%--%'), 
	CONSTRAINT ck_repair_actions_canonical_key_size CHECK (typeof(canonical_key) = 'text' AND length(canonical_key) BETWEEN 1 AND 64), 
	CONSTRAINT ck_repair_actions_action_kind_values CHECK (action_kind IN ('create_target', 'update_target')), 
	CONSTRAINT ck_repair_actions_external_idempotency_key_size CHECK (typeof(external_idempotency_key) = 'text' AND length(external_idempotency_key) BETWEEN 1 AND 128), 
	CONSTRAINT ck_repair_actions_before_sha256_shape CHECK (before_sha256 IS NULL OR (typeof(before_sha256) = 'text' AND length(before_sha256) = 64 AND before_sha256 NOT GLOB '*[^0-9a-f]*')), 
	CONSTRAINT ck_repair_actions_proposed_after_sha256_shape CHECK (typeof(proposed_after_sha256) = 'text' AND length(proposed_after_sha256) = 64 AND proposed_after_sha256 NOT GLOB '*[^0-9a-f]*'), 
	CONSTRAINT ck_repair_actions_proposed_record_json_object CHECK (typeof(proposed_record_json) = 'text' AND json_valid(proposed_record_json) AND json_type(proposed_record_json) = 'object'), 
	CONSTRAINT ck_repair_actions_expected_target_json_object CHECK (expected_target_record_json IS NULL OR (typeof(expected_target_record_json) = 'text' AND json_valid(expected_target_record_json) AND json_type(expected_target_record_json) = 'object')), 
	CONSTRAINT ck_repair_actions_mismatch_evidence_json_array CHECK (typeof(mismatch_evidence_json) = 'text' AND json_valid(mismatch_evidence_json) AND json_type(mismatch_evidence_json) = 'array'), 
	CONSTRAINT ck_repair_actions_application_status_values CHECK (application_status IN ('pending', 'applied', 'failed')), 
	CONSTRAINT ck_repair_actions_application_result_json_object CHECK (application_result_json IS NULL OR (typeof(application_result_json) = 'text' AND json_valid(application_result_json) AND json_type(application_result_json) = 'object')), 
	CONSTRAINT ck_repair_actions_applied_at_utc CHECK (applied_at IS NULL OR (typeof(applied_at) = 'text' AND length(applied_at) = 27 AND substr(applied_at, 5, 1) = '-' AND substr(applied_at, 8, 1) = '-' AND substr(applied_at, 11, 1) = 'T' AND substr(applied_at, 14, 1) = ':' AND substr(applied_at, 17, 1) = ':' AND substr(applied_at, 20, 1) = '.' AND substr(applied_at, 27, 1) = 'Z' AND substr(applied_at, 1, 4) NOT GLOB '*[^0-9]*' AND substr(applied_at, 6, 2) NOT GLOB '*[^0-9]*' AND substr(applied_at, 9, 2) NOT GLOB '*[^0-9]*' AND substr(applied_at, 12, 2) NOT GLOB '*[^0-9]*' AND substr(applied_at, 15, 2) NOT GLOB '*[^0-9]*' AND substr(applied_at, 18, 2) NOT GLOB '*[^0-9]*' AND substr(applied_at, 21, 6) NOT GLOB '*[^0-9]*' AND substr(applied_at, 1, 4) BETWEEN '0001' AND '9999' AND substr(applied_at, 6, 2) BETWEEN '01' AND '12' AND substr(applied_at, 9, 2) BETWEEN '01' AND '31' AND substr(applied_at, 12, 2) BETWEEN '00' AND '23' AND substr(applied_at, 15, 2) BETWEEN '00' AND '59' AND substr(applied_at, 18, 2) BETWEEN '00' AND '59')), 
	CONSTRAINT ck_repair_actions_failed_at_utc CHECK (failed_at IS NULL OR (typeof(failed_at) = 'text' AND length(failed_at) = 27 AND substr(failed_at, 5, 1) = '-' AND substr(failed_at, 8, 1) = '-' AND substr(failed_at, 11, 1) = 'T' AND substr(failed_at, 14, 1) = ':' AND substr(failed_at, 17, 1) = ':' AND substr(failed_at, 20, 1) = '.' AND substr(failed_at, 27, 1) = 'Z' AND substr(failed_at, 1, 4) NOT GLOB '*[^0-9]*' AND substr(failed_at, 6, 2) NOT GLOB '*[^0-9]*' AND substr(failed_at, 9, 2) NOT GLOB '*[^0-9]*' AND substr(failed_at, 12, 2) NOT GLOB '*[^0-9]*' AND substr(failed_at, 15, 2) NOT GLOB '*[^0-9]*' AND substr(failed_at, 18, 2) NOT GLOB '*[^0-9]*' AND substr(failed_at, 21, 6) NOT GLOB '*[^0-9]*' AND substr(failed_at, 1, 4) BETWEEN '0001' AND '9999' AND substr(failed_at, 6, 2) BETWEEN '01' AND '12' AND substr(failed_at, 9, 2) BETWEEN '01' AND '31' AND substr(failed_at, 12, 2) BETWEEN '00' AND '23' AND substr(failed_at, 15, 2) BETWEEN '00' AND '59' AND substr(failed_at, 18, 2) BETWEEN '00' AND '59')), 
	CONSTRAINT ck_repair_actions_target_version_range CHECK (target_version IS NULL OR (typeof(target_version) = 'integer' AND target_version BETWEEN 1 AND 2147483647)), 
	CONSTRAINT ck_repair_actions_action_shape CHECK ((action_kind = 'create_target' AND before_sha256 IS NULL AND expected_target_record_json IS NULL) OR (action_kind = 'update_target' AND before_sha256 IS NOT NULL AND expected_target_record_json IS NOT NULL)), 
	CONSTRAINT ck_repair_actions_application_result_coherence CHECK ((application_status = 'pending' AND application_result_json IS NULL AND target_version IS NULL AND applied_at IS NULL AND failed_at IS NULL) OR (application_status = 'applied' AND application_result_json IS NOT NULL AND target_version >= 1 AND applied_at IS NOT NULL AND failed_at IS NULL) OR (application_status = 'failed' AND application_result_json IS NOT NULL AND failed_at IS NOT NULL AND applied_at IS NULL))
);
CREATE TABLE repair_approvals (
	repair_plan_id VARCHAR(68) NOT NULL, 
	reconciliation_fingerprint VARCHAR(64) NOT NULL, 
	approved_by VARCHAR(128) NOT NULL, 
	approved_at VARCHAR(27) NOT NULL, 
	correlation_id VARCHAR(96) NOT NULL, 
	approval_schema_version INTEGER NOT NULL, 
	detail_json TEXT NOT NULL, 
	CONSTRAINT pk_repair_approvals PRIMARY KEY (repair_plan_id), 
	CONSTRAINT fk_repair_approvals_repair_plan_id_reconciliation_fingerprint_repair_plans FOREIGN KEY(repair_plan_id, reconciliation_fingerprint) REFERENCES repair_plans (repair_plan_id, reconciliation_fingerprint), 
	CONSTRAINT ck_repair_approvals_reconciliation_fingerprint_shape CHECK (typeof(reconciliation_fingerprint) = 'text' AND length(reconciliation_fingerprint) = 64 AND reconciliation_fingerprint NOT GLOB '*[^0-9a-f]*'), 
	CONSTRAINT ck_repair_approvals_approved_by_size CHECK (typeof(approved_by) = 'text' AND length(approved_by) BETWEEN 1 AND 128), 
	CONSTRAINT ck_repair_approvals_approved_at_utc CHECK (typeof(approved_at) = 'text' AND length(approved_at) = 27 AND substr(approved_at, 5, 1) = '-' AND substr(approved_at, 8, 1) = '-' AND substr(approved_at, 11, 1) = 'T' AND substr(approved_at, 14, 1) = ':' AND substr(approved_at, 17, 1) = ':' AND substr(approved_at, 20, 1) = '.' AND substr(approved_at, 27, 1) = 'Z' AND substr(approved_at, 1, 4) NOT GLOB '*[^0-9]*' AND substr(approved_at, 6, 2) NOT GLOB '*[^0-9]*' AND substr(approved_at, 9, 2) NOT GLOB '*[^0-9]*' AND substr(approved_at, 12, 2) NOT GLOB '*[^0-9]*' AND substr(approved_at, 15, 2) NOT GLOB '*[^0-9]*' AND substr(approved_at, 18, 2) NOT GLOB '*[^0-9]*' AND substr(approved_at, 21, 6) NOT GLOB '*[^0-9]*' AND substr(approved_at, 1, 4) BETWEEN '0001' AND '9999' AND substr(approved_at, 6, 2) BETWEEN '01' AND '12' AND substr(approved_at, 9, 2) BETWEEN '01' AND '31' AND substr(approved_at, 12, 2) BETWEEN '00' AND '23' AND substr(approved_at, 15, 2) BETWEEN '00' AND '59' AND substr(approved_at, 18, 2) BETWEEN '00' AND '59'), 
	CONSTRAINT ck_repair_approvals_correlation_id_size CHECK (typeof(correlation_id) = 'text' AND length(correlation_id) BETWEEN 1 AND 96), 
	CONSTRAINT ck_repair_approvals_approval_schema_version_range CHECK (typeof(approval_schema_version) = 'integer' AND approval_schema_version BETWEEN 1 AND 2147483647), 
	CONSTRAINT ck_repair_approvals_detail_json_object CHECK (typeof(detail_json) = 'text' AND json_valid(detail_json) AND json_type(detail_json) = 'object')
);
CREATE TABLE work_attempts (
	work_item_id VARCHAR(68) NOT NULL, 
	attempt_number INTEGER NOT NULL, 
	started_at VARCHAR(27) NOT NULL, 
	finished_at VARCHAR(27) NOT NULL, 
	runner_kind VARCHAR(32) NOT NULL, 
	worker_identity VARCHAR(128) NOT NULL, 
	outcome VARCHAR(32) NOT NULL, 
	failure_classification VARCHAR(32), 
	redacted_detail TEXT, 
	result_reference_json TEXT, 
	records_processed INTEGER DEFAULT '0' NOT NULL, 
	bytes_processed INTEGER DEFAULT '0' NOT NULL, 
	duration_microseconds INTEGER DEFAULT '0' NOT NULL, 
	CONSTRAINT pk_work_attempts PRIMARY KEY (work_item_id, attempt_number), 
	CONSTRAINT fk_work_attempts_work_item_id_work_items FOREIGN KEY(work_item_id) REFERENCES work_items (work_item_id), 
	CONSTRAINT ck_work_attempts_attempt_number_range CHECK (typeof(attempt_number) = 'integer' AND attempt_number BETWEEN 1 AND 2147483647), 
	CONSTRAINT ck_work_attempts_started_at_utc CHECK (typeof(started_at) = 'text' AND length(started_at) = 27 AND substr(started_at, 5, 1) = '-' AND substr(started_at, 8, 1) = '-' AND substr(started_at, 11, 1) = 'T' AND substr(started_at, 14, 1) = ':' AND substr(started_at, 17, 1) = ':' AND substr(started_at, 20, 1) = '.' AND substr(started_at, 27, 1) = 'Z' AND substr(started_at, 1, 4) NOT GLOB '*[^0-9]*' AND substr(started_at, 6, 2) NOT GLOB '*[^0-9]*' AND substr(started_at, 9, 2) NOT GLOB '*[^0-9]*' AND substr(started_at, 12, 2) NOT GLOB '*[^0-9]*' AND substr(started_at, 15, 2) NOT GLOB '*[^0-9]*' AND substr(started_at, 18, 2) NOT GLOB '*[^0-9]*' AND substr(started_at, 21, 6) NOT GLOB '*[^0-9]*' AND substr(started_at, 1, 4) BETWEEN '0001' AND '9999' AND substr(started_at, 6, 2) BETWEEN '01' AND '12' AND substr(started_at, 9, 2) BETWEEN '01' AND '31' AND substr(started_at, 12, 2) BETWEEN '00' AND '23' AND substr(started_at, 15, 2) BETWEEN '00' AND '59' AND substr(started_at, 18, 2) BETWEEN '00' AND '59'), 
	CONSTRAINT ck_work_attempts_finished_at_utc CHECK (typeof(finished_at) = 'text' AND length(finished_at) = 27 AND substr(finished_at, 5, 1) = '-' AND substr(finished_at, 8, 1) = '-' AND substr(finished_at, 11, 1) = 'T' AND substr(finished_at, 14, 1) = ':' AND substr(finished_at, 17, 1) = ':' AND substr(finished_at, 20, 1) = '.' AND substr(finished_at, 27, 1) = 'Z' AND substr(finished_at, 1, 4) NOT GLOB '*[^0-9]*' AND substr(finished_at, 6, 2) NOT GLOB '*[^0-9]*' AND substr(finished_at, 9, 2) NOT GLOB '*[^0-9]*' AND substr(finished_at, 12, 2) NOT GLOB '*[^0-9]*' AND substr(finished_at, 15, 2) NOT GLOB '*[^0-9]*' AND substr(finished_at, 18, 2) NOT GLOB '*[^0-9]*' AND substr(finished_at, 21, 6) NOT GLOB '*[^0-9]*' AND substr(finished_at, 1, 4) BETWEEN '0001' AND '9999' AND substr(finished_at, 6, 2) BETWEEN '01' AND '12' AND substr(finished_at, 9, 2) BETWEEN '01' AND '31' AND substr(finished_at, 12, 2) BETWEEN '00' AND '23' AND substr(finished_at, 15, 2) BETWEEN '00' AND '59' AND substr(finished_at, 18, 2) BETWEEN '00' AND '59'), 
	CONSTRAINT ck_work_attempts_runner_kind_size CHECK (typeof(runner_kind) = 'text' AND length(runner_kind) BETWEEN 1 AND 32), 
	CONSTRAINT ck_work_attempts_worker_identity_size CHECK (typeof(worker_identity) = 'text' AND length(worker_identity) BETWEEN 1 AND 128), 
	CONSTRAINT ck_work_attempts_outcome_values CHECK (outcome IN ('succeeded', 'retry_scheduled', 'quarantined', 'failed', 'cancelled', 'lease_expired')), 
	CONSTRAINT ck_work_attempts_failure_values CHECK (failure_classification IN ('connection', 'timeout', 'http_429', 'http_5xx', 'http_4xx', 'validation', 'idempotency_conflict', 'sqlite_contention', 'user_cancellation', 'unknown')), 
	CONSTRAINT ck_work_attempts_result_reference_json_object CHECK (result_reference_json IS NULL OR (typeof(result_reference_json) = 'text' AND json_valid(result_reference_json) AND json_type(result_reference_json) = 'object')), 
	CONSTRAINT ck_work_attempts_redacted_detail_size CHECK (redacted_detail IS NULL OR (typeof(redacted_detail) = 'text' AND length(redacted_detail) BETWEEN 1 AND 4096)), 
	CONSTRAINT ck_work_attempts_records_processed_range CHECK (typeof(records_processed) = 'integer' AND records_processed >= 0), 
	CONSTRAINT ck_work_attempts_bytes_processed_range CHECK (typeof(bytes_processed) = 'integer' AND bytes_processed >= 0), 
	CONSTRAINT ck_work_attempts_duration_range CHECK (typeof(duration_microseconds) = 'integer' AND duration_microseconds >= 0 AND duration_microseconds <= 31536000000000), 
	CONSTRAINT ck_work_attempts_attempt_time_order CHECK (finished_at >= started_at), 
	CONSTRAINT ck_work_attempts_failure_coherence CHECK ((outcome = 'succeeded' AND failure_classification IS NULL) OR (outcome <> 'succeeded' AND failure_classification IS NOT NULL)), 
	CONSTRAINT ck_work_attempts_lease_expired_classification CHECK (outcome <> 'lease_expired' OR failure_classification = 'timeout')
);
CREATE TABLE checkpoints (
	run_id VARCHAR(68) NOT NULL, 
	node_id VARCHAR(68) NOT NULL, 
	partition_key VARCHAR(128) NOT NULL, 
	version INTEGER NOT NULL, 
	payload_schema_version INTEGER NOT NULL, 
	source_cursor_json TEXT, 
	output_position_json TEXT, 
	artifact_id VARCHAR(68), 
	committed_at VARCHAR(27) NOT NULL, 
	CONSTRAINT pk_checkpoints PRIMARY KEY (run_id, node_id, partition_key, version), 
	CONSTRAINT fk_checkpoints_run_id_node_id_partition_key_checkpoint_heads FOREIGN KEY(run_id, node_id, partition_key) REFERENCES checkpoint_heads (run_id, node_id, partition_key), 
	CONSTRAINT fk_checkpoints_artifact_id_run_id_node_id_partition_key_artifact_manifests FOREIGN KEY(artifact_id, run_id, node_id, partition_key) REFERENCES artifact_manifests (artifact_id, run_id, node_id, partition_key), 
	CONSTRAINT ck_checkpoints_version_range CHECK (typeof(version) = 'integer' AND version BETWEEN 1 AND 2147483647), 
	CONSTRAINT ck_checkpoints_payload_schema_version_range CHECK (typeof(payload_schema_version) = 'integer' AND payload_schema_version BETWEEN 1 AND 2147483647), 
	CONSTRAINT ck_checkpoints_source_cursor_json_object CHECK (source_cursor_json IS NULL OR (typeof(source_cursor_json) = 'text' AND json_valid(source_cursor_json) AND json_type(source_cursor_json) = 'object')), 
	CONSTRAINT ck_checkpoints_output_position_json_object CHECK (output_position_json IS NULL OR (typeof(output_position_json) = 'text' AND json_valid(output_position_json) AND json_type(output_position_json) = 'object')), 
	CONSTRAINT ck_checkpoints_committed_at_utc CHECK (typeof(committed_at) = 'text' AND length(committed_at) = 27 AND substr(committed_at, 5, 1) = '-' AND substr(committed_at, 8, 1) = '-' AND substr(committed_at, 11, 1) = 'T' AND substr(committed_at, 14, 1) = ':' AND substr(committed_at, 17, 1) = ':' AND substr(committed_at, 20, 1) = '.' AND substr(committed_at, 27, 1) = 'Z' AND substr(committed_at, 1, 4) NOT GLOB '*[^0-9]*' AND substr(committed_at, 6, 2) NOT GLOB '*[^0-9]*' AND substr(committed_at, 9, 2) NOT GLOB '*[^0-9]*' AND substr(committed_at, 12, 2) NOT GLOB '*[^0-9]*' AND substr(committed_at, 15, 2) NOT GLOB '*[^0-9]*' AND substr(committed_at, 18, 2) NOT GLOB '*[^0-9]*' AND substr(committed_at, 21, 6) NOT GLOB '*[^0-9]*' AND substr(committed_at, 1, 4) BETWEEN '0001' AND '9999' AND substr(committed_at, 6, 2) BETWEEN '01' AND '12' AND substr(committed_at, 9, 2) BETWEEN '01' AND '31' AND substr(committed_at, 12, 2) BETWEEN '00' AND '23' AND substr(committed_at, 15, 2) BETWEEN '00' AND '59' AND substr(committed_at, 18, 2) BETWEEN '00' AND '59')
);
CREATE INDEX ix_audit_entries_correlation_id ON audit_entries (correlation_id);
CREATE INDEX ix_audit_entries_object_kind_object_id ON audit_entries (object_kind, object_id);
CREATE INDEX ix_audit_entries_occurred_at ON audit_entries (occurred_at);
CREATE INDEX ix_connectors_archived_at ON connectors (archived_at);
CREATE INDEX ix_connectors_kind ON connectors (kind);
CREATE INDEX ix_idempotency_records_status_created_at ON idempotency_records (status, created_at);
CREATE INDEX ix_pipelines_archived_at ON pipelines (archived_at);
CREATE INDEX ix_pipelines_display_name ON pipelines (display_name);
CREATE INDEX ix_pipeline_versions_published_at ON pipeline_versions (published_at);
CREATE INDEX ix_runs_created_at ON runs (created_at);
CREATE INDEX ix_runs_pipeline_id_pipeline_version_number ON runs (pipeline_id, pipeline_version_number);
CREATE INDEX ix_runs_state_created_at ON runs (state, created_at);
CREATE INDEX ix_execution_events_correlation_id ON execution_events (correlation_id);
CREATE INDEX ix_execution_events_occurred_at ON execution_events (occurred_at);
CREATE INDEX ix_reconciliation_conflicts_run_id_classification_canonical_key ON reconciliation_conflicts (run_id, classification, canonical_key);
CREATE INDEX ix_run_nodes_run_id_state ON run_nodes (run_id, state);
CREATE INDEX ix_artifact_manifests_run_id_node_id ON artifact_manifests (run_id, node_id);
CREATE INDEX ix_artifact_manifests_sha256 ON artifact_manifests (sha256);
CREATE INDEX ix_repair_plans_run_id_reconciliation_fingerprint ON repair_plans (run_id, reconciliation_fingerprint);
CREATE INDEX ix_work_items_lease_expires_at ON work_items (lease_expires_at) WHERE state = 'running';
CREATE INDEX ix_work_items_run_id_state_retry_available_at ON work_items (run_id, state, retry_available_at);
CREATE INDEX ix_repair_actions_conflict_id_run_id_canonical_key ON repair_actions (conflict_id, run_id, canonical_key);
CREATE INDEX ix_repair_actions_repair_plan_id_run_id ON repair_actions (repair_plan_id, run_id);
CREATE INDEX ix_repair_approvals_repair_plan_id_reconciliation_fingerprint ON repair_approvals (repair_plan_id, reconciliation_fingerprint);
CREATE INDEX ix_work_attempts_failure_classification ON work_attempts (failure_classification);
CREATE INDEX ix_work_attempts_finished_at ON work_attempts (finished_at);
CREATE INDEX ix_checkpoints_artifact_id_run_id_node_id_partition_key ON checkpoints (artifact_id, run_id, node_id, partition_key);
CREATE TRIGGER "trg_artifact_manifests_prohibit_delete" BEFORE DELETE ON "artifact_manifests" BEGIN SELECT RAISE(ABORT, 'artifact_manifests does not permit delete'); END;
CREATE TRIGGER "trg_artifact_manifests_prohibit_update" BEFORE UPDATE ON "artifact_manifests" BEGIN SELECT RAISE(ABORT, 'artifact_manifests does not permit update'); END;
CREATE TRIGGER "trg_audit_entries_prohibit_delete" BEFORE DELETE ON "audit_entries" BEGIN SELECT RAISE(ABORT, 'audit_entries does not permit delete'); END;
CREATE TRIGGER "trg_audit_entries_prohibit_update" BEFORE UPDATE ON "audit_entries" BEGIN SELECT RAISE(ABORT, 'audit_entries does not permit update'); END;
CREATE TRIGGER "trg_checkpoint_heads_current_version_must_increase" BEFORE UPDATE ON "checkpoint_heads" WHEN NEW."current_version" <= OLD."current_version" BEGIN SELECT RAISE(ABORT, 'checkpoint_heads current_version must increase'); END;
CREATE TRIGGER "trg_checkpoint_heads_prohibit_delete" BEFORE DELETE ON "checkpoint_heads" BEGIN SELECT RAISE(ABORT, 'checkpoint_heads does not permit delete'); END;
CREATE TRIGGER "trg_checkpoint_heads_protect_immutable_columns" BEFORE UPDATE ON "checkpoint_heads" WHEN NEW."run_id" IS NOT OLD."run_id" OR NEW."node_id" IS NOT OLD."node_id" OR NEW."partition_key" IS NOT OLD."partition_key" BEGIN SELECT RAISE(ABORT, 'checkpoint_heads immutable columns cannot change'); END;
CREATE TRIGGER "trg_checkpoints_prohibit_delete" BEFORE DELETE ON "checkpoints" BEGIN SELECT RAISE(ABORT, 'checkpoints does not permit delete'); END;
CREATE TRIGGER "trg_checkpoints_prohibit_update" BEFORE UPDATE ON "checkpoints" BEGIN SELECT RAISE(ABORT, 'checkpoints does not permit update'); END;
CREATE TRIGGER "trg_connector_secret_references_prohibit_delete" BEFORE DELETE ON "connector_secret_references" BEGIN SELECT RAISE(ABORT, 'connector_secret_references does not permit delete'); END;
CREATE TRIGGER "trg_connector_secret_references_prohibit_update" BEFORE UPDATE ON "connector_secret_references" BEGIN SELECT RAISE(ABORT, 'connector_secret_references does not permit update'); END;
CREATE TRIGGER "trg_connectors_prohibit_delete" BEFORE DELETE ON "connectors" BEGIN SELECT RAISE(ABORT, 'connectors does not permit delete'); END;
CREATE TRIGGER "trg_connectors_protect_immutable_columns" BEFORE UPDATE ON "connectors" WHEN NEW."connector_id" IS NOT OLD."connector_id" OR NEW."kind" IS NOT OLD."kind" OR NEW."created_at" IS NOT OLD."created_at" BEGIN SELECT RAISE(ABORT, 'connectors immutable columns cannot change'); END;
CREATE TRIGGER "trg_execution_events_prohibit_delete" BEFORE DELETE ON "execution_events" BEGIN SELECT RAISE(ABORT, 'execution_events does not permit delete'); END;
CREATE TRIGGER "trg_execution_events_prohibit_update" BEFORE UPDATE ON "execution_events" BEGIN SELECT RAISE(ABORT, 'execution_events does not permit update'); END;
CREATE TRIGGER "trg_idempotency_records_prohibit_delete" BEFORE DELETE ON "idempotency_records" BEGIN SELECT RAISE(ABORT, 'idempotency_records does not permit delete'); END;
CREATE TRIGGER "trg_idempotency_records_protect_immutable_columns" BEFORE UPDATE ON "idempotency_records" WHEN NEW."scope" IS NOT OLD."scope" OR NEW."idempotency_key" IS NOT OLD."idempotency_key" OR NEW."request_sha256" IS NOT OLD."request_sha256" OR NEW."created_at" IS NOT OLD."created_at" BEGIN SELECT RAISE(ABORT, 'idempotency_records immutable columns cannot change'); END;
CREATE TRIGGER "trg_idempotency_records_protect_terminal_status" BEFORE UPDATE ON "idempotency_records" WHEN OLD."status" IN ('completed', 'failed') BEGIN SELECT RAISE(ABORT, 'idempotency_records terminal rows cannot change'); END;
CREATE TRIGGER "trg_pipeline_versions_prohibit_delete" BEFORE DELETE ON "pipeline_versions" BEGIN SELECT RAISE(ABORT, 'pipeline_versions does not permit delete'); END;
CREATE TRIGGER "trg_pipeline_versions_prohibit_update" BEFORE UPDATE ON "pipeline_versions" BEGIN SELECT RAISE(ABORT, 'pipeline_versions does not permit update'); END;
CREATE TRIGGER "trg_pipelines_prohibit_delete" BEFORE DELETE ON "pipelines" BEGIN SELECT RAISE(ABORT, 'pipelines does not permit delete'); END;
CREATE TRIGGER "trg_pipelines_protect_immutable_columns" BEFORE UPDATE ON "pipelines" WHEN NEW."pipeline_id" IS NOT OLD."pipeline_id" OR NEW."created_at" IS NOT OLD."created_at" BEGIN SELECT RAISE(ABORT, 'pipelines immutable columns cannot change'); END;
CREATE TRIGGER "trg_reconciliation_conflicts_prohibit_delete" BEFORE DELETE ON "reconciliation_conflicts" BEGIN SELECT RAISE(ABORT, 'reconciliation_conflicts does not permit delete'); END;
CREATE TRIGGER "trg_reconciliation_conflicts_prohibit_update" BEFORE UPDATE ON "reconciliation_conflicts" BEGIN SELECT RAISE(ABORT, 'reconciliation_conflicts does not permit update'); END;
CREATE TRIGGER "trg_reconciliation_summaries_prohibit_delete" BEFORE DELETE ON "reconciliation_summaries" BEGIN SELECT RAISE(ABORT, 'reconciliation_summaries does not permit delete'); END;
CREATE TRIGGER "trg_reconciliation_summaries_prohibit_update" BEFORE UPDATE ON "reconciliation_summaries" BEGIN SELECT RAISE(ABORT, 'reconciliation_summaries does not permit update'); END;
CREATE TRIGGER "trg_repair_actions_prohibit_delete" BEFORE DELETE ON "repair_actions" BEGIN SELECT RAISE(ABORT, 'repair_actions does not permit delete'); END;
CREATE TRIGGER "trg_repair_actions_protect_immutable_columns" BEFORE UPDATE ON "repair_actions" WHEN NEW."repair_action_id" IS NOT OLD."repair_action_id" OR NEW."repair_plan_id" IS NOT OLD."repair_plan_id" OR NEW."run_id" IS NOT OLD."run_id" OR NEW."conflict_id" IS NOT OLD."conflict_id" OR NEW."canonical_key" IS NOT OLD."canonical_key" OR NEW."action_kind" IS NOT OLD."action_kind" OR NEW."external_idempotency_key" IS NOT OLD."external_idempotency_key" OR NEW."before_sha256" IS NOT OLD."before_sha256" OR NEW."proposed_after_sha256" IS NOT OLD."proposed_after_sha256" OR NEW."proposed_record_json" IS NOT OLD."proposed_record_json" OR NEW."expected_target_record_json" IS NOT OLD."expected_target_record_json" OR NEW."mismatch_evidence_json" IS NOT OLD."mismatch_evidence_json" BEGIN SELECT RAISE(ABORT, 'repair_actions immutable columns cannot change'); END;
CREATE TRIGGER "trg_repair_actions_protect_terminal_application_status" BEFORE UPDATE ON "repair_actions" WHEN OLD."application_status" IN ('applied', 'failed') BEGIN SELECT RAISE(ABORT, 'repair_actions terminal rows cannot change'); END;
CREATE TRIGGER "trg_repair_approvals_prohibit_delete" BEFORE DELETE ON "repair_approvals" BEGIN SELECT RAISE(ABORT, 'repair_approvals does not permit delete'); END;
CREATE TRIGGER "trg_repair_approvals_prohibit_update" BEFORE UPDATE ON "repair_approvals" BEGIN SELECT RAISE(ABORT, 'repair_approvals does not permit update'); END;
CREATE TRIGGER "trg_repair_plans_prohibit_delete" BEFORE DELETE ON "repair_plans" BEGIN SELECT RAISE(ABORT, 'repair_plans does not permit delete'); END;
CREATE TRIGGER "trg_repair_plans_protect_immutable_columns" BEFORE UPDATE ON "repair_plans" WHEN NEW."repair_plan_id" IS NOT OLD."repair_plan_id" OR NEW."run_id" IS NOT OLD."run_id" OR NEW."reconciliation_fingerprint" IS NOT OLD."reconciliation_fingerprint" OR NEW."content_fingerprint" IS NOT OLD."content_fingerprint" OR NEW."created_at" IS NOT OLD."created_at" BEGIN SELECT RAISE(ABORT, 'repair_plans immutable columns cannot change'); END;
CREATE TRIGGER "trg_repair_plans_protect_terminal_status" BEFORE UPDATE ON "repair_plans" WHEN OLD."status" IN ('applied', 'rejected', 'failed') BEGIN SELECT RAISE(ABORT, 'repair_plans terminal rows cannot change'); END;
CREATE TRIGGER "trg_run_event_counters_next_sequence_number_must_increase" BEFORE UPDATE ON "run_event_counters" WHEN NEW."next_sequence_number" <= OLD."next_sequence_number" BEGIN SELECT RAISE(ABORT, 'run_event_counters next_sequence_number must increase'); END;
CREATE TRIGGER "trg_run_event_counters_prohibit_delete" BEFORE DELETE ON "run_event_counters" BEGIN SELECT RAISE(ABORT, 'run_event_counters does not permit delete'); END;
CREATE TRIGGER "trg_run_event_counters_protect_immutable_columns" BEFORE UPDATE ON "run_event_counters" WHEN NEW."run_id" IS NOT OLD."run_id" BEGIN SELECT RAISE(ABORT, 'run_event_counters immutable columns cannot change'); END;
CREATE TRIGGER "trg_run_nodes_prohibit_delete" BEFORE DELETE ON "run_nodes" BEGIN SELECT RAISE(ABORT, 'run_nodes does not permit delete'); END;
CREATE TRIGGER "trg_run_nodes_protect_immutable_columns" BEFORE UPDATE ON "run_nodes" WHEN NEW."run_id" IS NOT OLD."run_id" OR NEW."node_id" IS NOT OLD."node_id" BEGIN SELECT RAISE(ABORT, 'run_nodes immutable columns cannot change'); END;
CREATE TRIGGER "trg_runs_prohibit_delete" BEFORE DELETE ON "runs" BEGIN SELECT RAISE(ABORT, 'runs does not permit delete'); END;
CREATE TRIGGER "trg_runs_protect_immutable_columns" BEFORE UPDATE ON "runs" WHEN NEW."run_id" IS NOT OLD."run_id" OR NEW."pipeline_id" IS NOT OLD."pipeline_id" OR NEW."pipeline_version_number" IS NOT OLD."pipeline_version_number" OR NEW."runner_kind" IS NOT OLD."runner_kind" OR NEW."runner_configuration_json" IS NOT OLD."runner_configuration_json" OR NEW."scenario_seed" IS NOT OLD."scenario_seed" OR NEW."created_at" IS NOT OLD."created_at" BEGIN SELECT RAISE(ABORT, 'runs immutable columns cannot change'); END;
CREATE TRIGGER "trg_system_metadata_prohibit_delete" BEFORE DELETE ON "system_metadata" BEGIN SELECT RAISE(ABORT, 'system_metadata does not permit delete'); END;
CREATE TRIGGER "trg_system_metadata_protect_immutable_columns" BEFORE UPDATE ON "system_metadata" WHEN NEW."key" IS NOT OLD."key" BEGIN SELECT RAISE(ABORT, 'system_metadata immutable columns cannot change'); END;
CREATE TRIGGER "trg_work_attempts_prohibit_delete" BEFORE DELETE ON "work_attempts" BEGIN SELECT RAISE(ABORT, 'work_attempts does not permit delete'); END;
CREATE TRIGGER "trg_work_attempts_prohibit_update" BEFORE UPDATE ON "work_attempts" BEGIN SELECT RAISE(ABORT, 'work_attempts does not permit update'); END;
CREATE TRIGGER "trg_work_items_prohibit_delete" BEFORE DELETE ON "work_items" BEGIN SELECT RAISE(ABORT, 'work_items does not permit delete'); END;
CREATE TRIGGER "trg_work_items_protect_immutable_columns" BEFORE UPDATE ON "work_items" WHEN NEW."work_item_id" IS NOT OLD."work_item_id" OR NEW."run_id" IS NOT OLD."run_id" OR NEW."node_id" IS NOT OLD."node_id" OR NEW."partition_key" IS NOT OLD."partition_key" OR NEW."input_reference_json" IS NOT OLD."input_reference_json" OR NEW."created_at" IS NOT OLD."created_at" BEGIN SELECT RAISE(ABORT, 'work_items immutable columns cannot change'); END;
CREATE TABLE alembic_version (
    version_num VARCHAR(32) NOT NULL,
    CONSTRAINT alembic_version_pkc PRIMARY KEY (version_num)
);
INSERT INTO alembic_version (version_num) VALUES ('0001_operational');
COMMIT;

/**
 * Generated from docs/generated/openapi.json by
 * scripts/generate_api_types.py (generator version 1).
 * Do not edit by hand; regenerate with the documented command.
 */

export interface components {
    schemas: {
        ArtifactPageResponse: {
            items: Array<components["schemas"]["ArtifactResponse"]>;
            limit: number;
            next_cursor: string | null;
            observed_at: string;
            run_id: string;
            run_version: number;
            schema_version?: 1;
        };
        ArtifactResponse: {
            artifact_id: string;
            artifact_schema_version: number;
            byte_size: number;
            created_at: string;
            media_type: string;
            node_id: string;
            partition_key: string;
            row_count: number;
            run_id: string;
            schema_version?: 1;
            sha256: string;
        };
        CapabilitiesResponse: {
            features: Array<components["schemas"]["FeatureBody"]>;
            limits: components["schemas"]["OperationalLimitsBody"];
            runners: Array<components["schemas"]["RunnerStrategyBody"]>;
            schema_version?: 1;
            service: string;
            sqlite: components["schemas"]["SqliteCapabilitiesBody"];
            subordinate_pools: Array<components["schemas"]["SubordinatePoolBody"]>;
            version: string;
        };
        ConflictDifference: {
            field: string;
            kind: string;
            source_text: string;
            target_text: string;
        };
        ConflictPageResponse: {
            items: Array<components["schemas"]["ConflictResponse"]>;
            limit: number;
            next_cursor: string | null;
            observed_at: string;
            reconciliation_fingerprint: string;
            run_id: string;
            run_version: number;
            schema_version?: 1;
            state: string;
        };
        ConflictReference: {
            position: number;
            record_key: string;
        };
        ConflictResponse: {
            canonical_key: string;
            classification: string;
            conflict_id: string;
            created_at: string;
            differences: Array<components["schemas"]["ConflictDifference"]>;
            schema_version?: 1;
            source_references: Array<components["schemas"]["ConflictReference"]>;
            suggested_resolution: string | null;
            target_references: Array<components["schemas"]["ConflictReference"]>;
        };
        ConnectorCreateRequest: {
            capabilities: {
                [key: string]: unknown;
            };
            configuration: {
                [key: string]: unknown;
            };
            connector_id: string;
            display_name: string;
            kind: string;
            schema_discovery?: {
                [key: string]: unknown;
            } | null;
            secret_references?: Array<components["schemas"]["ConnectorSecretReferenceBody"]>;
        };
        ConnectorPageResponse: {
            items: Array<components["schemas"]["ConnectorResponse"]>;
            limit: number;
            next_cursor: string | null;
            schema_version?: 1;
        };
        ConnectorResponse: {
            archived_at: string | null;
            capabilities: {
                [key: string]: unknown;
            };
            configuration: {
                [key: string]: unknown;
            };
            connector_id: string;
            created_at: string;
            display_name: string;
            kind: string;
            revision: number;
            row_version: number;
            schema_discovery: {
                [key: string]: unknown;
            } | null;
            schema_version?: 1;
            secret_references: Array<components["schemas"]["ConnectorSecretReferenceBody"]>;
            updated_at: string;
        };
        ConnectorSecretReferenceBody: {
            environment_variable_name: string;
            reference_name: string;
        };
        FeatureBody: {
            available: boolean;
            name: string;
        };
        HTTPValidationError: {
            detail?: Array<components["schemas"]["ValidationError"]>;
        };
        HealthResponse: {
            service: string;
            status?: "ok";
            version: string;
        };
        ObservationInput: {
            malformed_reason?: string | null;
            payload?: {
                [key: string]: unknown;
            } | null;
            position: number;
        };
        ObservationSideInput: {
            connector_id: string;
            input_identity: string;
            observations: Array<components["schemas"]["ObservationInput"]>;
        };
        OperationalLimitsBody: {
            artifact_chunk_bytes: number;
            idempotency_lease_seconds: number;
            max_concurrent_requests: number;
            max_json_depth: number;
            max_page_size: number;
            max_request_body_bytes: number;
            request_timeout_seconds: number;
            schema_version?: 1;
        };
        PipelineCreateRequest: {
            description?: string | null;
            display_name: string;
            pipeline_id: string;
        };
        PipelinePageResponse: {
            items: Array<components["schemas"]["PipelineResponse"]>;
            limit: number;
            next_cursor: string | null;
            schema_version?: 1;
        };
        PipelineResponse: {
            archived_at: string | null;
            created_at: string;
            description: string | null;
            display_name: string;
            pipeline_id: string;
            row_version: number;
            schema_version?: 1;
        };
        PipelineVersionPublishRequest: {
            document: {
                [key: string]: unknown;
            };
            expected_latest_version?: number | null;
        };
        PipelineVersionResponse: {
            pipeline_id: string;
            planner_format_version: number;
            published_at: string;
            schema_version?: 1;
            specification: {
                [key: string]: unknown;
            };
            specification_sha256: string;
            version: number;
        };
        ReadinessResponse: {
            detail: string;
            service: string;
            status: "ready" | "not_ready";
            version: string;
        };
        ReconciliationResponse: {
            analytical_query_version: number;
            counts: {
                [key: string]: number;
            };
            observed_at: string;
            reconciliation_fingerprint: string;
            reconciliation_observed_at: string;
            run_id: string;
            run_version: number;
            schema_version?: 1;
            source_input_identity: string;
            state: string;
            target_input_identity: string;
            total_count: number;
        };
        RepairActionResponse: {
            action_id: string;
            applied_at?: string | null;
            before_sha256: string;
            canonical_key: string;
            failed_at?: string | null;
            kind: string;
            proposed_after_sha256: string;
            schema_version?: 1;
            status: string;
            target_version?: number | null;
        };
        RepairApplyResponse: {
            content_fingerprint: string;
            disposition: string;
            effects: Array<{
                [key: string]: unknown;
            }>;
            observed_at: string;
            plan_id: string;
            reconciliation_fingerprint: string;
            resumed: boolean;
            run_id: string;
            run_version: number;
            schema_version?: 1;
            state: string;
            status: string;
        };
        RepairApprovalRequestBody: {
            approved_by: string;
            approved_content_fingerprint: string;
            approved_reconciliation_fingerprint: string;
            schema_version?: 1;
        };
        RepairApprovalSummary: {
            approval_schema_version: number;
            approved_at: string;
            approved_by: string;
            correlation_id: string;
            schema_version?: 1;
        };
        RepairPlanCreateRequest: {
            schema_version?: 1;
            source: components["schemas"]["ObservationSideInput"];
            target: components["schemas"]["ObservationSideInput"];
        };
        RepairPlanResponse: {
            actions: Array<components["schemas"]["RepairActionResponse"]>;
            applied_at?: string | null;
            applying_at?: string | null;
            approval?: components["schemas"]["RepairApprovalSummary"] | null;
            content_fingerprint: string;
            created_at: string;
            failed_at?: string | null;
            observed_at: string;
            plan_id: string;
            reconciliation_fingerprint: string;
            rejected_at?: string | null;
            run_id: string;
            run_version: number;
            schema_version?: 1;
            state: string;
            status: string;
        };
        RunCreateRequest: {
            pipeline_id: string;
            pipeline_version: number;
            run_id: string;
            runner_configuration?: {
                [key: string]: unknown;
            } | null;
            runner_kind: string;
            scenario_seed?: number | null;
        };
        RunPageResponse: {
            items: Array<components["schemas"]["RunResponse"]>;
            limit: number;
            next_cursor: string | null;
            schema_version?: 1;
        };
        RunResponse: {
            cancellation_requested_at: string | null;
            created_at: string;
            execution_evidence_fingerprint?: string | null;
            execution_evidence_fingerprint_version?: number | null;
            finished_at: string | null;
            observed_at: string;
            pipeline_id: string;
            pipeline_version: number;
            run_id: string;
            run_version: number;
            runner_kind: string;
            scenario_seed: number | null;
            schema_version?: 1;
            started_at: string | null;
            state: string;
        };
        RunnerStrategyBody: {
            available: boolean;
            schema_version?: 1;
            strategy_id: string;
            unavailability_reason?: string | null;
        };
        SqliteCapabilitiesBody: {
            busy_timeout_ms: number;
            journal_mode: string;
            library_version: string;
            minimum_supported_version: string;
            schema_version?: 1;
            supports_json_sql: boolean;
            supports_returning: boolean;
            synchronous_level: number;
            threadsafety: number;
        };
        SubordinatePoolBody: {
            available: boolean;
            pool_id: string;
            schema_version?: 1;
            unavailability_reason?: string | null;
        };
        ValidationError: {
            ctx?: Record<string, never>;
            input?: unknown;
            loc: Array<string | number>;
            msg: string;
            type: string;
        };
    };
}

export type ArtifactPageResponse = components["schemas"]["ArtifactPageResponse"];
export type ArtifactResponse = components["schemas"]["ArtifactResponse"];
export type CapabilitiesResponse = components["schemas"]["CapabilitiesResponse"];
export type ConflictDifference = components["schemas"]["ConflictDifference"];
export type ConflictPageResponse = components["schemas"]["ConflictPageResponse"];
export type ConflictReference = components["schemas"]["ConflictReference"];
export type ConflictResponse = components["schemas"]["ConflictResponse"];
export type ConnectorCreateRequest = components["schemas"]["ConnectorCreateRequest"];
export type ConnectorPageResponse = components["schemas"]["ConnectorPageResponse"];
export type ConnectorResponse = components["schemas"]["ConnectorResponse"];
export type ConnectorSecretReferenceBody = components["schemas"]["ConnectorSecretReferenceBody"];
export type FeatureBody = components["schemas"]["FeatureBody"];
export type HTTPValidationError = components["schemas"]["HTTPValidationError"];
export type HealthResponse = components["schemas"]["HealthResponse"];
export type ObservationInput = components["schemas"]["ObservationInput"];
export type ObservationSideInput = components["schemas"]["ObservationSideInput"];
export type OperationalLimitsBody = components["schemas"]["OperationalLimitsBody"];
export type PipelineCreateRequest = components["schemas"]["PipelineCreateRequest"];
export type PipelinePageResponse = components["schemas"]["PipelinePageResponse"];
export type PipelineResponse = components["schemas"]["PipelineResponse"];
export type PipelineVersionPublishRequest = components["schemas"]["PipelineVersionPublishRequest"];
export type PipelineVersionResponse = components["schemas"]["PipelineVersionResponse"];
export type ReadinessResponse = components["schemas"]["ReadinessResponse"];
export type ReconciliationResponse = components["schemas"]["ReconciliationResponse"];
export type RepairActionResponse = components["schemas"]["RepairActionResponse"];
export type RepairApplyResponse = components["schemas"]["RepairApplyResponse"];
export type RepairApprovalRequestBody = components["schemas"]["RepairApprovalRequestBody"];
export type RepairApprovalSummary = components["schemas"]["RepairApprovalSummary"];
export type RepairPlanCreateRequest = components["schemas"]["RepairPlanCreateRequest"];
export type RepairPlanResponse = components["schemas"]["RepairPlanResponse"];
export type RunCreateRequest = components["schemas"]["RunCreateRequest"];
export type RunPageResponse = components["schemas"]["RunPageResponse"];
export type RunResponse = components["schemas"]["RunResponse"];
export type RunnerStrategyBody = components["schemas"]["RunnerStrategyBody"];
export type SqliteCapabilitiesBody = components["schemas"]["SqliteCapabilitiesBody"];
export type SubordinatePoolBody = components["schemas"]["SubordinatePoolBody"];
export type ValidationError = components["schemas"]["ValidationError"];

export interface operations {
        download_artifact_api_v1_artifacts__artifact_id__get: {
            parameters: Array<{ name: "artifact_id"; in: "path"; required: true }>;
            responses: {
                "200": {
                    content: {
                        "application/json": unknown;
                    };
                };
                "422": {
                    content: {
                        "application/json": components["schemas"]["HTTPValidationError"];
                    };
                };
            };
        };
        list_connectors_api_v1_connectors_get: {
            parameters: Array<{ name: "limit"; in: "query"; required: false } | { name: "cursor"; in: "query"; required: false } | { name: "include_archived"; in: "query"; required: false }>;
            responses: {
                "200": {
                    content: {
                        "application/json": components["schemas"]["ConnectorPageResponse"];
                    };
                };
                "422": {
                    content: {
                        "application/json": components["schemas"]["HTTPValidationError"];
                    };
                };
            };
        };
        create_connector_api_v1_connectors_post: {
            requestBody: { content: { "application/json": components["schemas"]["ConnectorCreateRequest"]; }; };
            responses: {
                "201": {
                    content: {
                        "application/json": unknown;
                    };
                };
                "422": {
                    content: {
                        "application/json": components["schemas"]["HTTPValidationError"];
                    };
                };
            };
        };
        test_connector_api_v1_connectors__connector_id__test_post: {
            parameters: Array<{ name: "connector_id"; in: "path"; required: true }>;
            responses: {
                "200": {
                    content: {
                        "application/json": unknown;
                    };
                };
                "422": {
                    content: {
                        "application/json": components["schemas"]["HTTPValidationError"];
                    };
                };
            };
        };
        list_pipelines_api_v1_pipelines_get: {
            parameters: Array<{ name: "limit"; in: "query"; required: false } | { name: "cursor"; in: "query"; required: false } | { name: "include_archived"; in: "query"; required: false }>;
            responses: {
                "200": {
                    content: {
                        "application/json": components["schemas"]["PipelinePageResponse"];
                    };
                };
                "422": {
                    content: {
                        "application/json": components["schemas"]["HTTPValidationError"];
                    };
                };
            };
        };
        create_pipeline_api_v1_pipelines_post: {
            requestBody: { content: { "application/json": components["schemas"]["PipelineCreateRequest"]; }; };
            responses: {
                "201": {
                    content: {
                        "application/json": unknown;
                    };
                };
                "422": {
                    content: {
                        "application/json": components["schemas"]["HTTPValidationError"];
                    };
                };
            };
        };
        get_pipeline_api_v1_pipelines__pipeline_id__get: {
            parameters: Array<{ name: "pipeline_id"; in: "path"; required: true }>;
            responses: {
                "200": {
                    content: {
                        "application/json": components["schemas"]["PipelineResponse"];
                    };
                };
                "422": {
                    content: {
                        "application/json": components["schemas"]["HTTPValidationError"];
                    };
                };
            };
        };
        publish_pipeline_version_api_v1_pipelines__pipeline_id__versions_post: {
            parameters: Array<{ name: "pipeline_id"; in: "path"; required: true }>;
            requestBody: { content: { "application/json": components["schemas"]["PipelineVersionPublishRequest"]; }; };
            responses: {
                "201": {
                    content: {
                        "application/json": unknown;
                    };
                };
                "422": {
                    content: {
                        "application/json": components["schemas"]["HTTPValidationError"];
                    };
                };
            };
        };
        get_pipeline_version_api_v1_pipelines__pipeline_id__versions__version__get: {
            parameters: Array<{ name: "pipeline_id"; in: "path"; required: true } | { name: "version"; in: "path"; required: true }>;
            responses: {
                "200": {
                    content: {
                        "application/json": components["schemas"]["PipelineVersionResponse"];
                    };
                };
                "422": {
                    content: {
                        "application/json": components["schemas"]["HTTPValidationError"];
                    };
                };
            };
        };
        get_repair_plan_api_v1_repair_plans__plan_id__get: {
            parameters: Array<{ name: "plan_id"; in: "path"; required: true }>;
            responses: {
                "200": {
                    content: {
                        "application/json": components["schemas"]["RepairPlanResponse"];
                    };
                };
                "422": {
                    content: {
                        "application/json": components["schemas"]["HTTPValidationError"];
                    };
                };
            };
        };
        apply_repair_plan_api_v1_repair_plans__plan_id__apply_post: {
            parameters: Array<{ name: "plan_id"; in: "path"; required: true }>;
            responses: {
                "200": {
                    content: {
                        "application/json": components["schemas"]["RepairApplyResponse"];
                    };
                };
                "422": {
                    content: {
                        "application/json": components["schemas"]["HTTPValidationError"];
                    };
                };
            };
        };
        approve_repair_plan_api_v1_repair_plans__plan_id__approve_post: {
            parameters: Array<{ name: "plan_id"; in: "path"; required: true }>;
            requestBody: { content: { "application/json": components["schemas"]["RepairApprovalRequestBody"]; }; };
            responses: {
                "200": {
                    content: {
                        "application/json": components["schemas"]["RepairPlanResponse"];
                    };
                };
                "422": {
                    content: {
                        "application/json": components["schemas"]["HTTPValidationError"];
                    };
                };
            };
        };
        list_runs_api_v1_runs_get: {
            parameters: Array<{ name: "limit"; in: "query"; required: false } | { name: "cursor"; in: "query"; required: false } | { name: "state"; in: "query"; required: false }>;
            responses: {
                "200": {
                    content: {
                        "application/json": components["schemas"]["RunPageResponse"];
                    };
                };
                "422": {
                    content: {
                        "application/json": components["schemas"]["HTTPValidationError"];
                    };
                };
            };
        };
        create_run_api_v1_runs_post: {
            requestBody: { content: { "application/json": components["schemas"]["RunCreateRequest"]; }; };
            responses: {
                "201": {
                    content: {
                        "application/json": unknown;
                    };
                };
                "422": {
                    content: {
                        "application/json": components["schemas"]["HTTPValidationError"];
                    };
                };
            };
        };
        get_run_api_v1_runs__run_id__get: {
            parameters: Array<{ name: "run_id"; in: "path"; required: true }>;
            responses: {
                "200": {
                    content: {
                        "application/json": components["schemas"]["RunResponse"];
                    };
                };
                "422": {
                    content: {
                        "application/json": components["schemas"]["HTTPValidationError"];
                    };
                };
            };
        };
        list_run_artifacts_api_v1_runs__run_id__artifacts_get: {
            parameters: Array<{ name: "run_id"; in: "path"; required: true } | { name: "limit"; in: "query"; required: false } | { name: "cursor"; in: "query"; required: false }>;
            responses: {
                "200": {
                    content: {
                        "application/json": components["schemas"]["ArtifactPageResponse"];
                    };
                };
                "422": {
                    content: {
                        "application/json": components["schemas"]["HTTPValidationError"];
                    };
                };
            };
        };
        cancel_run_api_v1_runs__run_id__cancel_post: {
            parameters: Array<{ name: "run_id"; in: "path"; required: true }>;
            responses: {
                "200": {
                    content: {
                        "application/json": unknown;
                    };
                };
                "422": {
                    content: {
                        "application/json": components["schemas"]["HTTPValidationError"];
                    };
                };
            };
        };
        list_conflicts_api_v1_runs__run_id__conflicts_get: {
            parameters: Array<{ name: "run_id"; in: "path"; required: true } | { name: "limit"; in: "query"; required: false } | { name: "cursor"; in: "query"; required: false }>;
            responses: {
                "200": {
                    content: {
                        "application/json": components["schemas"]["ConflictPageResponse"];
                    };
                };
                "422": {
                    content: {
                        "application/json": components["schemas"]["HTTPValidationError"];
                    };
                };
            };
        };
        pause_run_api_v1_runs__run_id__pause_post: {
            parameters: Array<{ name: "run_id"; in: "path"; required: true }>;
            responses: {
                "200": {
                    content: {
                        "application/json": unknown;
                    };
                };
                "422": {
                    content: {
                        "application/json": components["schemas"]["HTTPValidationError"];
                    };
                };
            };
        };
        get_reconciliation_api_v1_runs__run_id__reconciliation_get: {
            parameters: Array<{ name: "run_id"; in: "path"; required: true }>;
            responses: {
                "200": {
                    content: {
                        "application/json": components["schemas"]["ReconciliationResponse"];
                    };
                };
                "422": {
                    content: {
                        "application/json": components["schemas"]["HTTPValidationError"];
                    };
                };
            };
        };
        create_repair_plan_api_v1_runs__run_id__repair_plans_post: {
            parameters: Array<{ name: "run_id"; in: "path"; required: true }>;
            requestBody: { content: { "application/json": components["schemas"]["RepairPlanCreateRequest"]; }; };
            responses: {
                "201": {
                    content: {
                        "application/json": components["schemas"]["RepairPlanResponse"];
                    };
                };
                "422": {
                    content: {
                        "application/json": components["schemas"]["HTTPValidationError"];
                    };
                };
            };
        };
        resume_run_api_v1_runs__run_id__resume_post: {
            parameters: Array<{ name: "run_id"; in: "path"; required: true }>;
            responses: {
                "200": {
                    content: {
                        "application/json": unknown;
                    };
                };
                "422": {
                    content: {
                        "application/json": components["schemas"]["HTTPValidationError"];
                    };
                };
            };
        };
        stream_run_events_api_v1_stream_runs__run_id__get: {
            parameters: Array<{ name: "run_id"; in: "path"; required: true } | { name: "after"; in: "query"; required: false } | { name: "Last-Event-ID"; in: "header"; required: false }>;
            responses: {
                "200": {};
                "422": {
                    content: {
                        "application/json": components["schemas"]["HTTPValidationError"];
                    };
                };
            };
        };
        capabilities_api_v1_system_capabilities_get: {
            responses: {
                "200": {
                    content: {
                        "application/json": components["schemas"]["CapabilitiesResponse"];
                    };
                };
            };
        };
        health_response_healthz_get: {
            responses: {
                "200": {
                    content: {
                        "application/json": components["schemas"]["HealthResponse"];
                    };
                };
            };
        };
        readiness_response_readyz_get: {
            responses: {
                "200": {
                    content: {
                        "application/json": components["schemas"]["ReadinessResponse"];
                    };
                };
                "503": {
                    content: {
                        "application/json": components["schemas"]["ReadinessResponse"];
                    };
                };
            };
        };
}

export type webhooks = Record<string, never>;

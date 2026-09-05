# Threat-model verification map

This document is the reviewable map required by Phase 22: every in-scope
abuse case is bound to the implementation that controls it and the
reproducible test that verifies the control against the real boundary.
It is verification evidence, not a narrative threat model; the security
requirements themselves remain authoritative in the [security model](SECURITY.md).

Method and status language:

- **Verified** — the control exists at the cited implementation point and
  the cited test exercises the real boundary (not a mock of it).
- **Not applicable** — the abuse case has no attack surface in this
  product, with the proof given inline.
- **Blocking finding** — no adequate control or test exists. A blocking
  finding must be resolved or explicitly accepted by the repository owner
  before a phase can be accepted.

Every path is relative to the repository root. Test identifiers use the
`file::test_name` form and are reproducible with
`uv run pytest <file> -k <test_name>` or the browser commands in the
[verification section](#verification-commands).

## 1. Input and resource bounds

| Abuse case | Owning control | Verification |
|---|---|---|
| Oversized request body | Request-limits middleware enforces the declared length and the streamed read; default 1 MiB | `src/paritygrid/api/middleware/request_limits.py`; `tests/api/test_limits.py::test_limit_settings_reject_out_of_range_integers`, `tests/api/test_problems.py::test_oversized_body_returns_request_too_large` |
| Malformed or deeply nested JSON, duplicate keys, NaN | Bounded JSON decoder with iterative depth walk and duplicate-key rejection | `src/paritygrid/api/json_bounds.py`; `tests/api/test_limits.py::test_bounded_json_rejects_adversarial_documents` |
| Wrong media type or unexpected body | Media-type and empty-body contracts at the same boundary | `src/paritygrid/api/middleware/request_limits.py`; `tests/api/test_limits.py::test_non_json_payload_media_types_are_rejected` |
| Request flooding past concurrency bound | Concurrency gate with problem response while saturated | `src/paritygrid/api/middleware/request_limits.py::ConcurrencyGate`; `tests/api/test_limits.py::test_concurrency_saturation_returns_a_problem`, `::test_liveness_is_answerable_while_the_gate_is_saturated` |
| Slow request bodies and hung handlers | Request timeout across body acquisition and sync-worker dispatch, with permit retention | `src/paritygrid/api/middleware/request_limits.py`; `tests/api/test_limits.py::test_request_timeout_covers_slow_body_acquisition`, `::test_slow_synchronous_route_returns_at_deadline_and_retains_its_permit` |
| Decompression abuse | The API never requests or decodes a content encoding, and the connector HTTP engines refuse chunked `Transfer-Encoding` responses outright; the synthetic sources also refuse chunked requests server-side | `src/paritygrid/adapters/connectors/http_clients.py`; `tests/connectors/test_http_clients.py::test_async_client_rejects_protocol_violations`, `::test_blocking_client_rejects_protocol_violations`; server side `tests/demo/test_async_source.py::test_source_rejects_transfer_encoding_requests` |
| Oversized record pages and collections | Connector page and reader caps on records and bytes | `src/paritygrid/adapters/connectors/source_wire.py`; `tests/connectors/test_source_wire.py::test_cursor_page_rejects_oversized_pages`, `tests/demo/test_fixture_files.py::test_bounded_reader_enforces_row_caps` |
| Oversized HTTP responses | Response size bounds enforced during streaming and header reads | `src/paritygrid/adapters/connectors/http_clients.py`; `tests/connectors/test_http_clients.py::test_async_client_enforces_response_size_bound` |
| Oversized or over-numerous artifacts | Artifact writer chunk/total limits and integrity-scanner issue limits | `src/paritygrid/adapters/artifacts/writer.py`, `src/paritygrid/adapters/artifacts/integrity.py`; `tests/artifacts/test_atomic_artifact_writer.py::test_writer_enforces_chunk_and_total_limits_before_publication`, `tests/artifacts/test_artifact_integrity_boundaries.py::test_manifest_limits_corruption_and_duplicate_paths_fail_closed` |
| Unbounded diagnostic detail | Problem Details truncation bounds (detail, error count, field length) | `src/paritygrid/api/errors/problems.py`; `tests/api/test_branch_gaps.py::test_problem_detail_is_truncated_to_the_bound`, `::test_problem_document_includes_bounded_field_errors` |
| Oversized WebSocket client messages | Strict client-message size cap that closes the channel | `src/paritygrid/api/routers/live.py`; `tests/api/test_live.py::test_oversized_client_message_closes_with_size_code` |

Upload endpoints do not exist: the API accepts bounded JSON command
bodies and produces artifacts through the internal engine only, so there
is no upload surface to bound.

## 2. Filesystem and path safety

| Abuse case | Owning control | Verification |
|---|---|---|
| Path traversal in static and SPA routes | Raw-path marker rejection, per-segment grammar, resolve-plus-containment | `src/paritygrid/api/frontend.py`; `tests/api/test_frontend.py::test_traversal_and_encoded_paths_are_confined`, `::test_raw_socket_dot_traversal_is_rejected` (real socket) |
| Symlink and junction escape from packaged assets | Strict resolution with containment and linked-component refusal | `src/paritygrid/api/frontend.py`; `tests/api/test_frontend.py::test_symlink_escape_is_rejected`, `::test_spa_fallback_rejects_an_index_symlink_escape` |
| Directory listing disclosure | Directories answer 404 and never fall back to the shell | `src/paritygrid/api/frontend.py`; `tests/api/test_frontend.py::test_directories_never_produce_listings` |
| Windows alternate data streams | Component grammar rejects any colon (drive/stream separator) in demo roots | `src/paritygrid/demo/ownership.py`; `tests/demo/test_phase20_ownership.py::test_alternate_data_stream_components_are_rejected`, `tests/demo/test_scenario_safety.py::test_windows_alternate_data_streams_are_rejected` |
| Unsafe archive names | Not applicable structurally: no archive extraction exists anywhere in the product — `zipfile` and `tarfile` are prohibited imports for the whole `src/` tree, and all file ingestion is bounded line/chunk readers | `src/paritygrid/quality/import_boundaries.py` (prohibited-import catalog); `uv run paritygrid-check-boundaries` in CI, `tests/unit/test_import_boundaries.py`; connector readers verified by `tests/connectors/` and `tests/demo/test_fixture_files.py` |
| Artifact root escape and link traversal | Artifact path resolution rejects linked roots, linked descendants, and resolver escapes | `src/paritygrid/adapters/artifacts/paths.py`; `tests/artifacts/test_safe_artifact_path.py::test_resolution_rejects_a_linked_descendant`, `::test_resolution_rejects_a_linked_root`, `tests/api/test_artifacts.py::test_unsafe_artifact_identities_are_confined` |
| Demo cleanup deleting outside the exact demo root | Ownership markers, broad-root rejection, immediate revalidation before delete, link-tree refusal, external proof registry | `src/paritygrid/demo/ownership.py`; `tests/demo/test_phase20_ownership.py::test_reset_refuses_to_delete_through_a_link_child_and_keeps_the_sentinel`, `::test_reset_revalidates_immediately_before_deletion`, `::test_static_marker_without_independent_proof_never_authorizes_open_or_reset`, `tests/quality/test_platform_matrix.py::test_junction_root_is_still_rejected` |

## 3. Network posture

| Abuse case | Owning control | Verification |
|---|---|---|
| Service reachable off loopback | Settings type restricts the bind to `127.0.0.1`, `::1`, `localhost`; no acknowledgement path exists because no non-loopback bind can be configured (stricter than an opt-in warning) | `src/paritygrid/runtime/config.py`; `tests/unit/test_config.py::test_settings_reject_non_loopback_bind_address` |
| Cross-origin access | No CORS middleware is registered; no allow-origin header is ever emitted | `src/paritygrid/api/app.py`; `tests/api/test_headers.py::test_cors_is_disabled_by_default` |
| Server fingerprinting | Internal-server header suppression | `tests/api/test_headers.py::test_responses_do_not_expose_server_internals` |
| Connector fetching request-controlled targets | Not applicable structurally: connector base URLs are operator configuration, credentials in URLs and HTTPS targets are refused, no redirects are followed, and no route composes fetch targets from request data | `src/paritygrid/adapters/connectors/http_clients.py`; `tests/connectors/test_http_clients.py::test_split_base_url_rejects_credentials_and_https`, `::test_async_client_rejects_protocol_violations` |
| Synthetic services bound broadly | Simulators bind `127.0.0.1` on dynamic ports with readiness gating | `src/paritygrid/demo/simulators/`; `tests/demo/test_simulator_lifecycle.py::test_every_service_answers_readiness_over_real_http`, `::test_start_publishes_three_distinct_dynamic_ports` |

## 4. Secrets, authorization posture, and redaction

The product is a loopback single-operator demonstration: there is no
per-request authorization model and no cookie/session processing. The
security invariant is the loopback bind posture and CORS refusal above;
secrets are never stored in configuration (environment-variable names
only) and are redacted wherever bounded evidence leaves the process.

| Abuse case | Owning control | Verification |
|---|---|---|
| Secret leakage through connector evidence | Connector redaction combines connector and per-call secret material; sensitive headers and keys are replaced wholesale; an escape gate fails construction when a secret would surface | `src/paritygrid/application/ports/connector_redaction.py`; `tests/connectors/test_redaction.py::test_sensitive_headers_are_replaced_wholesale`, `::test_sensitive_keys_are_replaced_recursively`, `::test_gate_raises_when_a_secret_would_escape` |
| Secret leakage through errors and exceptions | Bounded exception translation with flattened, redacted chains and storage-error redaction | `tests/connectors/test_redaction.py::test_error_message_quoting_a_secret_fails_at_construction`, `::test_exception_chain_is_flattened_and_redacted`; `tests/artifacts/test_artifact_integrity_boundaries.py::test_storage_error_translation_is_specific_and_redacted` |
| Secret leakage in durable events and approvals | Redacted document types are required by the persistence and approval contracts | `src/paritygrid/application/repair/approval.py` (`RedactedDocument` required); `tests/planner/test_connector_contract.py::test_binding_snapshot_is_exact_canonical_non_secret_and_redacted` |
| Stack traces and internal context in responses | Problem Details mapping with bounded detail and no exception context | `src/paritygrid/api/errors/problems.py`, `src/paritygrid/api/errors/handlers.py`; `tests/api/test_branch_gaps.py::test_repository_failure_families_map_to_bounded_problems` |
| Repository content leaking credentials | Repository-wide credential-pattern and developer-path scans run in the validation lane | `scripts/validate_instructions.ps1`; run command in the [verification section](#verification-commands) |

## 5. Browser content policy

| Abuse case | Owning control | Verification |
|---|---|---|
| Script injection from untrusted data | React text-only rendering plus a static sink guard over the whole frontend source | `tests/api` n/a; browser proof `web/e2e/inert-rendering.spec.ts::untrusted difference text renders inertly as text`, `::the frontend source contains no HTML or code-execution sinks` |
| Injection through server-originated HTML | Frontend serves no user-controlled HTML; hostile Problem Details render inertly | `web/e2e/inert-rendering.spec.ts::hostile Problem Details text renders inertly in the failure state` |
| Hostile values passing runtime validation | Strict runtime schemas fail closed on out-of-contract strings | `web/e2e/inert-rendering.spec.ts::a hostile key that violates the strict runtime schema fails closed`; `web/src/api/runtime-schemas.ts` unit coverage in `web/src/api/runtime-schemas.test.ts` |
| Script, style, frame, object, image, and connection policy bypass | One restrictive production policy for the shell, deny-all for assets and API, narrow confined documentation policy; negative browser probes prove blocked behavior with violation events | `src/paritygrid/api/middleware/security_headers.py`; contract `tests/api/test_csp_policy.py`; browser `web/e2e/csp-policy.spec.ts`; real packaged app `web/e2e/demo.spec.ts::11 the real packaged app serves the production CSP on the wire`, `::12 inline script is blocked and never executes under the real packaged app` |
| MIME confusion | `nosniff` on every response plus explicit safe media types for packaged assets | `src/paritygrid/api/middleware/security_headers.py`, `src/paritygrid/api/frontend.py`; `tests/api/test_headers.py::test_every_response_carries_the_security_headers`, `tests/api/test_frontend.py::test_hashed_assets_are_immutable_with_safe_media_types` |
| Content-disposition injection | Download filenames derive only from validated artifact identities and media types | `src/paritygrid/api/routers/artifacts.py`; `tests/api/test_artifacts.py::test_download_streams_the_full_committed_artifact` |

The `/api/docs` page keeps the narrow documentation policy accepted in
Phase 12 because FastAPI's bundled swagger page loads its script and
style assets from the pinned `cdn.jsdelivr.net` origin, and its
bootstrap script is inline: the documentation `script-src` allows
`https://cdn.jsdelivr.net` and inline scripts generally on exactly those
two paths (there is no nonce or hash). Nothing else is widened — the
policy keeps `object-src 'none'`, `frame-ancestors 'none'`, and
`connect-src 'self'` — and the contract tests confine it to exactly the
two documentation paths. This is the single deliberate exception to the
no-remote-origin rule; self-hosting the swagger assets remains the
recorded remediation direction.

## 6. Graph validation, repair approval, and idempotency

| Abuse case | Owning control | Verification |
|---|---|---|
| Cyclic or unreachable graphs | DAG cycle, reachability, port, resource, and repair-safety validation before publication | `src/paritygrid/application/planner/` (`graph.py`, `reachability.py`, `port_validation.py`, `resources.py`, `repair_safety.py`); `tests/planner/test_dag_cycle_validator.py::test_generated_directed_rings_are_rejected`, `tests/planner/test_reachability_validator.py` |
| Repair without approval | Publication-time path validation requires every `repair.apply` node to be preceded by `repair.approval` | `tests/planner/test_repair_safety_validator.py::test_valid_repair_chain_requires_approval_before_effect`, `::test_unknown_node_kind_is_never_treated_as_safe` |
| Stale or foreign plan approval | Approval binds exact plan content and reconciliation fingerprints; stale reconciliation blocks approval | `src/paritygrid/application/repair/approval.py`; `tests/repair/test_workflow_services.py::test_approval_with_a_mismatched_content_fingerprint_is_rejected`, `::test_approval_with_a_stale_reconciliation_fingerprint_is_rejected` |
| Concurrent approval and application races | Row-version fencing with exactly one durable winner | `tests/repair/test_fencing_edges.py::test_a_competing_creation_returns_the_durable_winner`, `::test_a_competing_completion_fences_the_loser_exactly_once`, `::test_a_stale_current_reconciliation_blocks_approval` |
| Replay producing duplicate effects | Exact-retry replay returns the stored fact; any divergence is rejected | `tests/repair/test_workflow_services.py::test_exact_approval_retry_replays_the_immutable_fact`, `::test_divergent_approval_replay_is_rejected`, `tests/demo/test_scenario_runner.py::test_repair_replay_is_an_idempotent_no_op` |
| Idempotency key reused with a different request | Durable request-digest binding rejects mismatched replays at the service and HTTP boundaries | `src/paritygrid/application/services/idempotency.py`; `tests/api/test_idempotency.py::test_same_key_with_a_different_request_conflicts`, `::test_http_key_reuse_with_different_request_conflicts`, `::test_repository_reclaim_rejects_digest_mismatch` |
| Stranded idempotency ownership after failure | Lease expiry, restart recovery, and reclaim with single execution | `tests/api/test_idempotency.py::test_expiry_reclaim_executes_once_across_two_owners`, `::test_restart_recovers_replay_and_stranded_ownership` |

## 7. Data integrity

| Abuse case | Owning control | Verification |
|---|---|---|
| Artifact tampering or deletion after commit | Content hashes and byte sizes verified by the integrity scanner; downloads report integrity failures | `src/paritygrid/adapters/artifacts/integrity.py`; `tests/api/test_artifacts.py::test_tampered_artifact_file_reports_integrity_gone`, `::test_deleted_artifact_file_reports_integrity_gone`; `tests/artifacts/test_artifact_integrity_scanner.py` |
| Manifest/file inconsistency | Manifest verification rejects nonregular, changed, or replaced files and path races | `src/paritygrid/adapters/artifacts/manifests.py`; `tests/artifacts/test_artifact_manifest_boundaries.py::test_verify_rejects_nonregular_changed_and_replaced_files`, `::test_repository_detects_parent_and_path_races` |
| Cache treated as authoritative | DuckDB state is rebuildable and non-authoritative; agreement tests compare against the Python engine | `src/paritygrid/adapters/analytics/duckdb.py`; `tests/reconciliation/test_duckdb_agreement.py`, `tests/reconciliation/test_resource_bounds.py::test_duckdb_rebuild_replaces_disposable_state_and_cleans_up` |
| Durable event mutation or deletion | Append-only enforced by declared SQL triggers across operational tables | `src/paritygrid/adapters/persistence/schema.py`; `tests/persistence/test_schema_triggers.py::test_catalog_declares_delete_prohibition_for_every_operational_table`, `::test_immutable_history_trigger_rejects_no_op_update` |
| Stale reconciliation or repair state accepted | Monotonic fingerprint guards in storage plus exact fingerprint comparison in services | `tests/persistence/test_schema_triggers.py::test_installed_monotonic_guards_reject_stale_values_and_allow_advances`; `tests/api/test_reconciliation.py::test_reconciliation_summary_returns_coherent_fingerprints` |
| History gap silently streamed | SSE fails closed on durable sequence gaps | `tests/api/test_sse.py::test_stream_fails_closed_when_durable_history_contains_a_gap` |

## 8. Live transports, lifecycle, and recovery

| Abuse case | Owning control | Verification |
|---|---|---|
| Slow SSE client blocking execution | Bounded send timeouts disconnect slow clients; heartbeats carry no sensitive data | `src/paritygrid/api/routers/stream.py`; `tests/api/test_sse.py::test_slow_sse_send_times_out_and_releases_the_request_slot`, `::test_stream_emits_heartbeats_while_idle`, `tests/api/test_slow_clients.py::test_real_server_survives_a_slow_disconnecting_client` |
| Slow WebSocket client or saturated hub | Drop-oldest bounded subscriptions, per-run subscriber caps, timed channel sends | `src/paritygrid/application/services/telemetry.py`; `tests/api/test_live.py::test_hub_bounds_queues_and_subscriber_capacity`, `::test_slow_consumer_is_closed_without_blocking_the_publisher` |
| Telemetry mutating execution | Telemetry is advisory only; disconnects never change run state | `tests/api/test_live.py::test_disconnected_live_client_cannot_block_or_modify_execution` |
| Cancellation under saturation | Capacity and channel bounds with deterministic cancellation coverage | `tests/api/test_limits.py::test_request_timeout_covers_slow_body_acquisition`; `tests/connectors/test_http_clients.py::test_async_client_request_cancellation_closes_connection`; lifecycle matrix in `tests/execution/test_lifecycle_matrix.py` |
| Repeated or partial shutdown | Idempotent simulator and lifespan shutdown with rollback on partial startup | `tests/demo/test_simulator_lifecycle.py::test_repeated_shutdown_is_safe_and_releases_owned_resources`, `::test_partial_startup_rolls_back_every_started_service`; `tests/api/test_lifespan.py::test_shutdown_attempts_every_closer_after_one_fails` |
| Orphaned processes or resources after crashes | Interruption proofs, restart recovery, child reaping, and orphan detection | `tests/demo/test_phase20_interruption.py::test_interruption_proof_verifies_full_recovery`; `tests/quality/test_platform_matrix.py::test_spawned_child_from_the_environment_is_detected_and_reaped`; browser `web/e2e/demo-harness.ts` tree-kill assertions |
| Recovery accepting stale or duplicate state | Recovery contract fail-closed classification | `tests/execution/test_recovery_contract.py`, `tests/execution/test_recovery_evidence.py` |

## 9. Dependency, license, and notice posture

| Abuse case | Owning control | Verification |
|---|---|---|
| Vulnerable locked dependencies | Locked hash-pinned audit for Python; `--audit-level=high` audit for the frontend, both classified through the shared outcome classifier. CI enforces the classified Python audit, the classified frontend audit, and the inventory drift check on every change; nightly enforces the classified Python audit, the inventory drift check, and the raw high-severity frontend gate | `scripts/verify_python_dependencies.py` (ci.yml python, nightly.yml python), `scripts/verify_frontend_dependencies.py` (ci.yml frontend API smoke), `src/paritygrid/quality/dependency_audit.py`, raw gate `npm --prefix web audit --audit-level=high` (ci.yml frontend, nightly.yml browsers); classification `tests/quality/test_dependency_audit.py` |
| Transport failure relabeled as success | Structured-output classification with a closed outcome set; only proven transport failures are retried, on unchanged inputs, with every attempt retained | `src/paritygrid/quality/dependency_audit.py`; `tests/quality/test_dependency_audit.py::test_findings_are_never_retried`, `::test_exhausted_transport_failure_stays_a_transport_failure`, recorded fixtures under `tests/fixtures/dependency_audit/` |
| Broad or unowned vulnerability suppression | Suppressions require an exact advisory identifier, package, reason, owner, approval authority, expiry, and upstream reference; malformed, overbroad, and expired entries fail closed | `src/paritygrid/quality/dependency_audit.py::VulnerabilitySuppression`; `tests/quality/test_dependency_audit.py::test_malformed_and_overbroad_suppressions_fail_closed`, `::test_expired_suppression_never_masks_a_finding`. The suppression registry is reserved and currently unwired: no audit lane accepts a suppression input, so no finding can be suppressed in practice |
| Notices drifting from the audited locks or shipped distribution | Deterministic inventory records the owner-approved MIT compatibility result for every Python and frontend lock entry. The final notice is generated from the locked local font packages, then must be byte-identical in the source root, frontend public input, and committed frontend distribution. The CI integration and nightly browser jobs run the drift check after installing both locked ecosystems | `src/paritygrid/quality/third_party_notices.py`, `docs/generated/third-party-notices.json`, `THIRD_PARTY_NOTICES.txt`; `tests/quality/test_third_party_notices.py`, `scripts/verify_third_party_notices.py` (ci.yml and nightly.yml). A Python entry is `direct` when any project dependency group — runtime or development — names it, `transitive` otherwise; frontend roles use the same rule over the lockfile root |
| Missing third-party attribution or license text | The two bundled OFL font assets map to their exact locked packages; the generator reads each installed package's license file, records its copyright notice and source location, and fails closed if either is absent | `src/paritygrid/quality/third_party_notices.py::render_bundled_notices`; `tests/quality/test_third_party_notices.py::test_bundled_notice_copies_match_the_locked_font_license_sources` |
| Unknown or stale distribution content | Fail-closed inventory rules for unknown assets, unmapped fonts, stale environment versions, conflicting license metadata, and a missing or altered shipped notice | `tests/quality/test_third_party_notices.py::test_unknown_distributed_asset_fails`, `::test_stale_environment_version_fails`, `::test_conflicting_frontend_license_metadata_fails`, `::test_bundled_notice_copies_match_the_locked_font_license_sources` |
| Unaudited GitHub Actions | All workflow actions are commit-SHA-pinned | `.github/workflows/ci.yml`, `.github/workflows/nightly.yml`; `tests/quality/test_nightly_workflow.py::test_every_third_party_action_is_pinned_to_a_full_sha` |
| Container or deployment inputs | Not applicable structurally: no container images, compose files, Kubernetes/helm/terraform manifests, or other deployment inputs exist in the repository | `tests/quality/test_dependency_audit.py::test_no_container_or_deployment_inputs_are_tracked` (fail-closed check over every tracked path) |

## 10. Explicit findings and accepted exceptions

- **Blocking findings:** none open in this map. Every abuse-case row
  above names an owning control and reproducible verification; the Phase
  22 implementation closed the three evidence gaps identified at phase
  start (no browser policy tests — now `web/e2e/csp-policy.spec.ts`, no
  CSP contract pin — now `tests/api/test_csp_policy.py`, no
  inert-rendering proof — now `web/e2e/inert-rendering.spec.ts`). This
  statement is scoped to the threat-model evidence only. The owner selected
  MIT for source, wheel, and packaged-frontend distribution; the approved
  project license and the generated third-party notices remain separate.
  A responsible-disclosure contact remains a pre-public-release gate and is
  intentionally not invented here.
- **Accepted exception:** the confined `/api/docs` documentation policy
  (section 5) keeps its pinned CDN origin and inline-script allowance.
  Owner: repository maintainers. Expiry: review when swagger assets are
  self-hosted or documentation is disabled in packaging.
- **Deliberate posture:** the non-loopback acknowledgement option named
  in the security model is superseded by the stricter settings-level
  refusal (section 3); the security model documents this implemented
  posture.

## Verification commands

```powershell
uv run pytest tests/api/test_csp_policy.py tests/api/test_headers.py tests/api/test_frontend.py
uv run pytest tests/quality/test_dependency_audit.py tests/quality/test_third_party_notices.py
uv run python scripts/verify_python_dependencies.py
uv run python scripts/verify_frontend_dependencies.py
uv run python scripts/verify_third_party_notices.py
npm --prefix web audit --audit-level=high
npm --prefix web exec -- playwright test --config=playwright.frontend.config.ts
npm --prefix web exec -- playwright test e2e/demo.spec.ts
pwsh scripts/validate_instructions.ps1
```

"""The versioned canonical scenario: identities, profiles, expected evidence.

The canonical scenario is a pure function of explicit versioned inputs: the
scenario format version, the scenario version, the scenario seed, the dataset
generator version, and the selected profile.  Everything this module derives —
source datasets, the divergent warehouse target, classification counts, repair
counts, fixture byte sizes, and the reconciliation fingerprint — is computed
without I/O, without wall-clock time, without randomness, and without any
run-local identity, so repeated derivations produce identical canonical bytes.

The scenario story exercises the complete product path: four source kinds
(paginated asynchronous HTTP, paginated blocking HTTP, CSV, JSON Lines), a
synthetic warehouse target that deterministically diverges from the sources,
scripted source latency, exactly one rate-limit fault that produces exactly one
durable work-item retry, malformed-data quarantine without losing valid work,
duplicate records, every reconciliation classification, deterministic repair
planning, explicit approval before application, idempotent application with one
transient post-commit connection loss resolved by the accepted ambiguous-outcome
replay, and independent target verification.

Fingerprint kinds stay separate throughout: the plan fingerprint identifies the
canonical scheduling intent, the reconciliation fingerprint identifies the
analytical reconciliation snapshot, the target-state fingerprint identifies the
post-repair inventory, and the execution-evidence fingerprint identifies
versioned durable execution evidence.  No universal "final fingerprint" exists
here and no fingerprint kind is derived from or equated with another.

Retry semantics follow the accepted contracts exactly.  The locked retry count
counts durable work-item retries: a failed attempt whose typed classification
maps to the retry disposition and whose named-policy decision schedules the next
attempt of the same work-item identity.  The single locked retry is triggered by
the scripted rate-limit (HTTP 429) fault on the asynchronous source read.  The
transient connection failure is a separate scripted fault against the warehouse
during repair application; the accepted repair applier resolves that ambiguous
post-commit outcome by replaying the same idempotency key, which consumes no
work-item retry.  Both faults stay observable and truthful in the locked
evidence; neither is reinterpreted to force the expected counts.
"""

import json
from collections.abc import Mapping
from dataclasses import dataclass
from hashlib import sha256
from typing import cast

from paritygrid.application.planner.connectors import (
    ConnectorBindingSnapshot,
    ConnectorCapability,
    ConnectorCapabilitySet,
)
from paritygrid.application.planner.documents import PipelineDocument
from paritygrid.application.planner.execution_plan import compile_execution_plan
from paritygrid.application.planner.plan_fingerprint import fingerprint_execution_plan
from paritygrid.application.planner.publication import PublishedPipelineSpecification
from paritygrid.application.ports.configuration import ConfigurationDocument
from paritygrid.application.reconciliation.analysis import (
    ReconciliationAnalysis,
    ReconciliationAnalysisRequest,
    analyze_reconciliation,
)
from paritygrid.application.repair import generate_repair_plan
from paritygrid.application.repair.verification import (
    build_expected_inventory,
    expected_fingerprint,
)
from paritygrid.demo.datasets import (
    DATASET_GENERATOR_VERSION,
    DatasetProfile,
    RowRole,
    ScenarioSeed,
    ScenarioVersion,
    SyntheticDataset,
    WireRow,
    WireValue,
    canonical_json_bytes,
    derive_source_dataset,
    generate_dataset,
)
from paritygrid.demo.failures import (
    SCRIPTED_FAILURE_VERSION,
    FailureScript,
    FailureScriptError,
    ScriptedFailure,
    ScriptedFailureKind,
)
from paritygrid.demo.fixtures import render_csv_fixture, render_jsonl_fixture
from paritygrid.domain.models import ConnectorId, RunId
from paritygrid.domain.reconciliation import ReconciliationClassification, SourceObservation

SCENARIO_FORMAT_NAME = "paritygrid-canonical-scenario"
SCENARIO_FORMAT_VERSION = 1
CANONICAL_SCENARIO_VERSION = 1
CANONICAL_SCENARIO_SEED = 19
CANONICAL_RUN_ID = "run_canonical-demo"
CANONICAL_PIPELINE_ID = "pip_canonical-demo"
CANONICAL_PIPELINE_VERSION = 1
CANONICAL_CORRELATION_ID = "corr-canonical-demo"

PLAN_FINGERPRINT_KIND = "plan"
PLAN_FINGERPRINT_VERSION = 1
RECONCILIATION_FINGERPRINT_KIND = "reconciliation"
RECONCILIATION_FINGERPRINT_VERSION = 1
TARGET_STATE_FINGERPRINT_KIND = "target_state"
TARGET_STATE_FINGERPRINT_VERSION = 1
EXECUTION_EVIDENCE_FINGERPRINT_KIND = "execution-evidence"
EXECUTION_EVIDENCE_FINGERPRINT_VERSION = 2

ASYNC_SOURCE_CONNECTOR = ConnectorId("con_canonical-async-http")
BLOCKING_SOURCE_CONNECTOR = ConnectorId("con_canonical-blocking-http")
CSV_SOURCE_CONNECTOR = ConnectorId("con_canonical-csv")
JSONL_SOURCE_CONNECTOR = ConnectorId("con_canonical-jsonl")
WAREHOUSE_CONNECTOR = ConnectorId("con_canonical-warehouse")

NODE_ASYNC_SOURCE = "nod_can-async-src"
NODE_BLOCKING_SOURCE = "nod_can-blocking-src"
NODE_CSV_SOURCE = "nod_can-csv-src"
NODE_JSONL_SOURCE = "nod_can-jsonl-src"
NODE_NORMALIZE = "nod_can-normalize"
NODE_VALIDATE = "nod_can-validate"
NODE_PARTITION = "nod_can-partition"
NODE_RECONCILE = "nod_can-reconcile"
NODE_REPAIR_PLAN = "nod_can-repair-plan"
NODE_APPROVAL = "nod_can-approval"
NODE_APPLY = "nod_can-apply"
NODE_VERIFY = "nod_can-verify"
NODE_EXPORT = "nod_can-export"
CANONICAL_NODES: tuple[str, ...] = (
    NODE_ASYNC_SOURCE,
    NODE_NORMALIZE,
    NODE_VALIDATE,
    NODE_PARTITION,
    NODE_RECONCILE,
    NODE_REPAIR_PLAN,
    NODE_APPROVAL,
    NODE_APPLY,
    NODE_VERIFY,
    NODE_EXPORT,
)
CANONICAL_EDGES: tuple[tuple[str, str], ...] = (
    (NODE_ASYNC_SOURCE, NODE_NORMALIZE),
    (NODE_NORMALIZE, NODE_VALIDATE),
    (NODE_VALIDATE, NODE_PARTITION),
    (NODE_PARTITION, NODE_RECONCILE),
    (NODE_RECONCILE, NODE_REPAIR_PLAN),
    (NODE_REPAIR_PLAN, NODE_APPROVAL),
    (NODE_APPROVAL, NODE_APPLY),
    (NODE_APPLY, NODE_VERIFY),
    (NODE_PARTITION, NODE_EXPORT),
)
CANONICAL_NODE_KINDS: dict[str, str] = {
    NODE_ASYNC_SOURCE: "source.http.async",
    NODE_NORMALIZE: "transform.normalize",
    NODE_VALIDATE: "transform.validate",
    NODE_PARTITION: "transform.partition",
    NODE_RECONCILE: "reconcile.target",
    NODE_REPAIR_PLAN: "repair.generate",
    NODE_APPROVAL: "repair.approval",
    NODE_APPLY: "repair.apply",
    NODE_VERIFY: "verify.target",
    NODE_EXPORT: "export.parquet",
}
CANONICAL_PARTITIONS_BY_NODE: dict[str, tuple[str, ...]] = {
    node: (("p0", "p1") if node == NODE_PARTITION else ("p0",)) for node in CANONICAL_NODES
}

# Fixed derivation rules of scenario format version 1.  The fault positions are
# 1-based request sequence numbers counted by the simulators in arrival order;
# the modulo rules select which canonical keys carry each classification.
ASYNC_RATE_LIMIT_REQUEST = 2
WAREHOUSE_FAULT_ACTION_INDEX = 3
TARGET_ONLY_KEY_COUNT = 4
ARTIFACT_COUNT = 3
CSV_FIXTURE_ARTIFACT_ID = "fixture:canonical-source.csv"
JSONL_FIXTURE_ARTIFACT_ID = "fixture:canonical-source.jsonl"
CONFLICT_ARTIFACT_ID = "art_canonical-conflicts"
CANONICAL_ARTIFACT_IDENTITIES: tuple[str, ...] = (
    CSV_FIXTURE_ARTIFACT_ID,
    JSONL_FIXTURE_ARTIFACT_ID,
    CONFLICT_ARTIFACT_ID,
)
LOCKED_RATE_LIMIT_RETRIES = 1
LOCKED_TRANSIENT_CONNECTION_FAILURES = 1
SOURCE_KEYS: tuple[str, ...] = ("async_http", "blocking_http", "csv", "jsonl")

MAX_RECORD_COUNT = 5_000
MAX_PAGE_SIZE = 200
MAX_SOURCE_LATENCY_MICROSECONDS = 60_000_000
_MAX_MANIFEST_BYTES = 256 * 1024
_FINGERPRINT_KEYS = {
    "plan",
    "reconciliation",
    "expected_target_state",
    "execution_evidence",
}


@dataclass(frozen=True, slots=True)
class CanonicalConnectorDefinition:
    """One public-safe connector registration used by the canonical pipeline."""

    connector_id: ConnectorId
    kind: str
    display_name: str
    configuration: ConfigurationDocument
    capabilities: ConnectorCapabilitySet


def canonical_connector_definitions() -> tuple[CanonicalConnectorDefinition, ...]:
    """Return the exact public connector registrations for the scenario."""

    def definition(
        connector_id: ConnectorId,
        kind: str,
        display_name: str,
        capabilities: tuple[ConnectorCapability, ...],
    ) -> CanonicalConnectorDefinition:
        return CanonicalConnectorDefinition(
            connector_id=connector_id,
            kind=kind,
            display_name=display_name,
            configuration=ConfigurationDocument.from_mapping({"environment": "canonical-loopback"}),
            capabilities=ConnectorCapabilitySet(capabilities),
        )

    return (
        definition(
            ASYNC_SOURCE_CONNECTOR,
            "async_http_source",
            "Canonical asynchronous HTTP source",
            (ConnectorCapability.ASYNC_IO, ConnectorCapability.READ),
        ),
        definition(
            BLOCKING_SOURCE_CONNECTOR,
            "blocking_http_source",
            "Canonical blocking HTTP source",
            (ConnectorCapability.BLOCKING_IO, ConnectorCapability.READ),
        ),
        definition(
            CSV_SOURCE_CONNECTOR,
            "csv_source",
            "Canonical CSV source",
            (ConnectorCapability.BLOCKING_IO, ConnectorCapability.READ),
        ),
        definition(
            JSONL_SOURCE_CONNECTOR,
            "jsonl_source",
            "Canonical JSON Lines source",
            (ConnectorCapability.BLOCKING_IO, ConnectorCapability.READ),
        ),
        definition(
            WAREHOUSE_CONNECTOR,
            "warehouse_target",
            "Canonical warehouse target",
            (
                ConnectorCapability.ASYNC_IO,
                ConnectorCapability.IDEMPOTENCY,
                ConnectorCapability.READ,
                ConnectorCapability.WRITE,
            ),
        ),
    )


def canonical_pipeline_document() -> PipelineDocument:
    """Return the validated canonical pipeline draft.

    This is the representative accepted execution path used for the plan
    fingerprint and cross-runner comparison.  The accepted version-1 planner
    has no multi-input aggregation node, so the document intentionally does
    not claim that ``transform.normalize`` merges four inputs.  Phase 19 owns
    the four concurrent source acquisitions and stable cross-source assembly;
    their exact identities are locked separately in the scenario manifest.
    """
    connector_for = {
        NODE_ASYNC_SOURCE: ASYNC_SOURCE_CONNECTOR,
        NODE_BLOCKING_SOURCE: BLOCKING_SOURCE_CONNECTOR,
        NODE_CSV_SOURCE: CSV_SOURCE_CONNECTOR,
        NODE_JSONL_SOURCE: JSONL_SOURCE_CONNECTOR,
        NODE_RECONCILE: WAREHOUSE_CONNECTOR,
        NODE_APPLY: WAREHOUSE_CONNECTOR,
        NODE_VERIFY: WAREHOUSE_CONNECTOR,
    }
    configuration_for: dict[str, dict[str, object]] = {
        NODE_CSV_SOURCE: {"encoding": "utf-8", "header": True},
        NODE_JSONL_SOURCE: {"encoding": "utf-8"},
        NODE_PARTITION: {"partition_count": 2},
        NODE_EXPORT: {"compression": "zstd"},
    }
    port_for_edge = {
        (NODE_RECONCILE, NODE_REPAIR_PLAN): ("reconciliation", "reconciliation"),
        (NODE_REPAIR_PLAN, NODE_APPROVAL): ("repair-plan", "repair-plan"),
        (NODE_APPROVAL, NODE_APPLY): ("approved-plan", "approved-plan"),
        (NODE_APPLY, NODE_VERIFY): ("repair-result", "repair-result"),
    }
    return PipelineDocument.from_mapping(
        {
            "canonical_format_version": 1,
            "edges": [
                {
                    "source_node_id": source,
                    "source_port": port_for_edge.get((source, target), ("records", "records"))[0],
                    "target_node_id": target,
                    "target_port": port_for_edge.get((source, target), ("records", "records"))[1],
                }
                for source, target in CANONICAL_EDGES
            ],
            "layout": [
                {"node_id": node, "x": index * 120, "y": (index % 4) * 80}
                for index, node in enumerate(CANONICAL_NODES)
            ],
            "nodes": [
                {
                    "configuration": configuration_for.get(node, {}),
                    "configuration_version": 1,
                    "connector_id": (
                        None if node not in connector_for else str(connector_for[node])
                    ),
                    "id": node,
                    "kind": CANONICAL_NODE_KINDS[node],
                }
                for node in CANONICAL_NODES
            ],
            "resource_policy": {
                "max_concurrency": 4,
                "max_in_flight": 16,
                "memory_limit_bytes": 536_870_912,
                "operation_timeout_seconds": 60,
                "queue_capacity": 256,
            },
            "schema_version": 1,
        }
    )


def canonical_published_pipeline() -> PublishedPipelineSpecification:
    """Return the pure immutable publication used to lock the plan fingerprint."""
    definitions = tuple(
        definition
        for definition in canonical_connector_definitions()
        if definition.connector_id in (ASYNC_SOURCE_CONNECTOR, WAREHOUSE_CONNECTOR)
    )
    return PublishedPipelineSpecification(
        pipeline=canonical_pipeline_document(),
        connector_bindings=tuple(
            ConnectorBindingSnapshot(
                connector_id=definition.connector_id,
                kind=definition.kind,
                revision=1,
                configuration=definition.configuration,
                capabilities=definition.capabilities,
                schema_discovery=None,
                secret_references=(),
            )
            for definition in definitions
        ),
    )


class ScenarioError(ValueError):
    """Raised when a scenario profile, derivation, or manifest is invalid."""


def canonical_plan_fingerprint() -> str:
    """Return the canonical topology-derived plan fingerprint.

    The fingerprint binds the scenario format, the node set, the closed node
    kinds, the edge set, and the per-node partition layout.  It is a
    scenario-format version 1 identity of scheduling intent; it is never
    derived from, compared with, or equated to any other fingerprint kind.
    """
    return fingerprint_execution_plan(compile_execution_plan(canonical_published_pipeline())).value


@dataclass(frozen=True, slots=True)
class CanonicalScenarioProfile:
    """Bounded, named shape of one canonical dataset profile.

    ``rate_limit_request`` is the 1-based asynchronous-source request sequence
    that carries the scripted rate-limit fault; it must land inside the first
    read attempt of the asynchronous slice.  ``warehouse_fault_action`` is the
    1-based index of the repair write that suffers the transient post-commit
    connection loss.
    """

    profile_id: str
    record_count: int
    malformed_count: int
    boundary_count: int
    duplicate_count: int
    async_page_size: int
    blocking_page_size: int
    csv_page_size: int
    jsonl_page_size: int
    source_latency_microseconds: int
    rate_limit_request: int
    warehouse_fault_action: int

    def __post_init__(self) -> None:
        if type(self.profile_id) is not str or not self.profile_id.isascii():
            raise ScenarioError("profile id must be non-empty ASCII text")
        if not 1 <= len(self.profile_id) <= 32:
            raise ScenarioError("profile id must be 1 to 32 characters")
        for name in (
            "record_count",
            "malformed_count",
            "boundary_count",
            "duplicate_count",
            "async_page_size",
            "blocking_page_size",
            "csv_page_size",
            "jsonl_page_size",
            "source_latency_microseconds",
            "rate_limit_request",
            "warehouse_fault_action",
        ):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, int):
                raise ScenarioError(f"profile {name} must be an integer")
            if value < 0:
                raise ScenarioError(f"profile {name} must not be negative")
        if self.record_count > MAX_RECORD_COUNT:
            raise ScenarioError(f"record count must not exceed {MAX_RECORD_COUNT}")
        if self.malformed_count + self.boundary_count > self.record_count:
            raise ScenarioError("malformed and boundary rows must fit within the record count")
        for name in (
            "async_page_size",
            "blocking_page_size",
            "csv_page_size",
            "jsonl_page_size",
        ):
            value = getattr(self, name)
            if not 1 <= value <= MAX_PAGE_SIZE:
                raise ScenarioError(f"profile {name} must be between 1 and {MAX_PAGE_SIZE}")
        if self.source_latency_microseconds > MAX_SOURCE_LATENCY_MICROSECONDS:
            raise ScenarioError("profile latency must not exceed 60 seconds")
        if self.rate_limit_request < 1:
            raise ScenarioError("the rate-limit fault must target request 1 or later")
        if self.warehouse_fault_action < 1:
            raise ScenarioError("the warehouse fault must target action 1 or later")

    def dataset_profile(self) -> DatasetProfile:
        """Return the dataset-generator profile for this scenario profile."""
        return DatasetProfile(
            record_count=self.record_count,
            malformed_count=self.malformed_count,
            boundary_count=self.boundary_count,
            duplicate_count=self.duplicate_count,
        )

    def identity_bytes(self) -> bytes:
        """Return the deterministic profile identity encoding."""
        return canonical_json_bytes(
            {
                "async_page_size": self.async_page_size,
                "blocking_page_size": self.blocking_page_size,
                "boundary_count": self.boundary_count,
                "csv_page_size": self.csv_page_size,
                "duplicate_count": self.duplicate_count,
                "jsonl_page_size": self.jsonl_page_size,
                "malformed_count": self.malformed_count,
                "profile_id": self.profile_id,
                "rate_limit_request": self.rate_limit_request,
                "record_count": self.record_count,
                "source_latency_microseconds": self.source_latency_microseconds,
                "warehouse_fault_action": self.warehouse_fault_action,
            }
        )


FAST_PROFILE = CanonicalScenarioProfile(
    profile_id="fast",
    record_count=48,
    malformed_count=4,
    boundary_count=3,
    duplicate_count=5,
    async_page_size=5,
    blocking_page_size=4,
    csv_page_size=6,
    jsonl_page_size=7,
    source_latency_microseconds=1_000,
    rate_limit_request=ASYNC_RATE_LIMIT_REQUEST,
    warehouse_fault_action=WAREHOUSE_FAULT_ACTION_INDEX,
)
SHOWCASE_PROFILE = CanonicalScenarioProfile(
    profile_id="showcase",
    record_count=700,
    malformed_count=10,
    boundary_count=6,
    duplicate_count=60,
    async_page_size=50,
    blocking_page_size=60,
    csv_page_size=80,
    jsonl_page_size=70,
    source_latency_microseconds=200,
    rate_limit_request=ASYNC_RATE_LIMIT_REQUEST,
    warehouse_fault_action=WAREHOUSE_FAULT_ACTION_INDEX,
)
PROFILES: dict[str, CanonicalScenarioProfile] = {
    FAST_PROFILE.profile_id: FAST_PROFILE,
    SHOWCASE_PROFILE.profile_id: SHOWCASE_PROFILE,
}


@dataclass(frozen=True, slots=True)
class SourceSlice:
    """One source's deterministic dataset slice and its read plan."""

    key: str
    connector: ConnectorId
    dataset: SyntheticDataset
    page_size: int
    expected_requests: int


@dataclass(frozen=True, slots=True)
class TargetSide:
    """The deterministic divergent warehouse inventory and its identity."""

    payloads: tuple[dict[str, object], ...]
    identity: str

    def write_count(self) -> int:
        """Return how many mutating writes load this inventory."""
        return len(self.payloads)


@dataclass(frozen=True, slots=True)
class ScenarioCounts:
    """Every locked record-level and workflow count of one profile."""

    total_input_rows: int
    accepted_rows: int
    rejected_rows: int
    quarantined_rows: int
    boundary_rows: int
    duplicate_rows: int
    duplicate_groups: int
    canonical_keys: int
    match: int
    missing_from_target: int
    missing_from_source: int
    field_mismatch: int
    duplicate_source: int
    duplicate_target: int
    duplicate_both: int
    planned_repairs: int
    applied_repairs: int
    review_only_repairs: int
    rate_limit_retries: int
    transient_connection_failures: int
    ambiguous_replays_resolved: int
    artifacts: int
    async_http_requests: int
    blocking_http_requests: int

    def as_mapping(self) -> dict[str, int]:
        """Return the closed count document in canonical key order."""
        return {
            "accepted_rows": self.accepted_rows,
            "ambiguous_replays_resolved": self.ambiguous_replays_resolved,
            "applied_repairs": self.applied_repairs,
            "artifacts": self.artifacts,
            "async_http_requests": self.async_http_requests,
            "blocking_http_requests": self.blocking_http_requests,
            "boundary_rows": self.boundary_rows,
            "canonical_keys": self.canonical_keys,
            "duplicate_both": self.duplicate_both,
            "duplicate_groups": self.duplicate_groups,
            "duplicate_rows": self.duplicate_rows,
            "duplicate_source": self.duplicate_source,
            "duplicate_target": self.duplicate_target,
            "field_mismatch": self.field_mismatch,
            "match": self.match,
            "missing_from_source": self.missing_from_source,
            "missing_from_target": self.missing_from_target,
            "planned_repairs": self.planned_repairs,
            "quarantined_rows": self.quarantined_rows,
            "rate_limit_retries": self.rate_limit_retries,
            "rejected_rows": self.rejected_rows,
            "review_only_repairs": self.review_only_repairs,
            "total_input_rows": self.total_input_rows,
            "transient_connection_failures": self.transient_connection_failures,
        }

    def coherence_errors(self) -> list[str]:
        """Return every violated count-coherence invariant."""
        errors: list[str] = []
        if self.total_input_rows != self.accepted_rows + self.rejected_rows:
            errors.append("total input rows must equal accepted plus rejected rows")
        if self.rejected_rows != self.quarantined_rows:
            errors.append("rejected rows must equal quarantined rows")
        if self.boundary_rows > self.accepted_rows:
            errors.append("boundary rows must be part of the accepted rows")
        if self.duplicate_rows > self.accepted_rows:
            errors.append("duplicate rows must be part of the accepted rows")
        classifications_total = (
            self.match
            + self.missing_from_target
            + self.missing_from_source
            + self.field_mismatch
            + self.duplicate_source
            + self.duplicate_target
            + self.duplicate_both
        )
        if self.canonical_keys != classifications_total:
            errors.append("canonical keys must cover every classification exactly once")
        if self.planned_repairs != self.missing_from_target + self.field_mismatch:
            errors.append("planned repairs must be exactly the repairable classifications")
        if self.applied_repairs != self.planned_repairs:
            errors.append("applied repairs must equal planned repairs")
        review_only = (
            self.missing_from_source
            + self.duplicate_source
            + self.duplicate_target
            + self.duplicate_both
        )
        if self.review_only_repairs != review_only:
            errors.append("review-only repairs must be exactly the review-only classifications")
        if self.rate_limit_retries != LOCKED_RATE_LIMIT_RETRIES:
            errors.append("the canonical scenario locks exactly one rate-limit retry")
        if self.transient_connection_failures != LOCKED_TRANSIENT_CONNECTION_FAILURES:
            errors.append("the canonical scenario locks exactly one transient connection failure")
        if self.ambiguous_replays_resolved != self.transient_connection_failures:
            errors.append("every transient connection failure must resolve by replay")
        if self.artifacts != ARTIFACT_COUNT:
            errors.append(f"the canonical scenario locks {ARTIFACT_COUNT} artifacts")
        return errors


@dataclass(frozen=True, slots=True)
class ScenarioExpectedEvidence:
    """The complete derived expected evidence for one profile.

    ``expected_target_fingerprint`` is the target-state fingerprint of the
    post-repair inventory computed through the accepted pure verification
    functions; the independent verifier must reproduce it by observation.  The
    execution-evidence fingerprint is intentionally absent: it is derived from
    durable run evidence and therefore locked by the run manifest and the
    cross-run byte-equality tests rather than by this pure derivation.
    """

    profile: CanonicalScenarioProfile
    dataset: SyntheticDataset
    slices: tuple[SourceSlice, ...]
    source_failure_script: FailureScript
    warehouse_failure_script: FailureScript
    target: TargetSide
    counts: ScenarioCounts
    source_input_identity: str
    target_input_identity: str
    reconciliation_fingerprint: str
    expected_target_fingerprint: str
    plan_fingerprint: str
    csv_fixture_bytes: bytes
    jsonl_fixture_bytes: bytes

    def slice_for(self, key: str) -> SourceSlice:
        """Return one source slice by its canonical key."""
        for slice_value in self.slices:
            if slice_value.key == key:
                return slice_value
        raise ScenarioError(f"unknown source slice: {key}")

    def total_generated_bytes(self) -> int:
        """Return the locked generated dataset byte total."""
        return len(self.csv_fixture_bytes) + len(self.jsonl_fixture_bytes)


def derive_scenario(profile: CanonicalScenarioProfile) -> ScenarioExpectedEvidence:
    """Derive the complete expected evidence for one profile.

    Pure and I/O-free: repeated calls on any machine return identical
    identities, counts, and fingerprints.
    """
    dataset = generate_dataset(
        ScenarioSeed(CANONICAL_SCENARIO_SEED),
        ScenarioVersion(CANONICAL_SCENARIO_VERSION),
        profile.dataset_profile(),
    )
    partitioned = _slice_rows(dataset)
    slices = tuple(
        SourceSlice(
            key=key,
            connector=connector,
            dataset=derive_source_dataset(dataset, partitioned[key]),
            page_size=page_size,
            expected_requests=_expected_requests(len(partitioned[key]), page_size),
        )
        for key, connector, page_size in (
            ("async_http", ASYNC_SOURCE_CONNECTOR, profile.async_page_size),
            ("blocking_http", BLOCKING_SOURCE_CONNECTOR, profile.blocking_page_size),
            ("csv", CSV_SOURCE_CONNECTOR, profile.csv_page_size),
            ("jsonl", JSONL_SOURCE_CONNECTOR, profile.jsonl_page_size),
        )
    )
    async_pages = (len(partitioned["async_http"]) + profile.async_page_size - 1) // (
        profile.async_page_size
    )
    if async_pages < 2 or profile.rate_limit_request > async_pages:
        raise ScenarioError(
            "the rate-limit fault must land inside the asynchronous source's first attempt"
        )
    csv_bytes, _csv_malformed, _csv_duplicates = render_csv_fixture(
        partitioned_dataset(slices, "csv")
    )
    jsonl_bytes, _jsonl_malformed, _jsonl_duplicates = render_jsonl_fixture(
        partitioned_dataset(slices, "jsonl")
    )
    csv_skus = frozenset(
        cast("str", row.payload["sku"])
        for row in partitioned["csv"]
        if row.role is not RowRole.MALFORMED
    )
    target = _derive_target_side(dataset, csv_skus)
    analysis = _analyze(slices, target, csv_skus)
    counts = _counts(profile, slices, analysis)
    generated = generate_repair_plan(run_id=RunId(CANONICAL_RUN_ID), analysis=analysis)
    plan = generated.plan
    if plan is None:
        raise ScenarioError("the canonical scenario must produce a repair plan")
    expected_inventory = build_expected_inventory(analysis, plan)
    if profile.warehouse_fault_action > counts.planned_repairs:
        raise ScenarioError("the warehouse fault must target one of the planned repairs")
    source_script = FailureScript.from_entries(
        (
            ScriptedFailure(
                sequence=profile.rate_limit_request,
                kind=ScriptedFailureKind.RATE_LIMIT,
                retry_after_seconds=1,
            ),
        )
    )
    warehouse_script = FailureScript.from_entries(
        (
            ScriptedFailure(
                sequence=target.write_count() + profile.warehouse_fault_action,
                kind=ScriptedFailureKind.CONNECTION_LOSS,
                partial_bytes=1,
            ),
        )
    )
    return ScenarioExpectedEvidence(
        profile=profile,
        dataset=dataset,
        slices=slices,
        source_failure_script=source_script,
        warehouse_failure_script=warehouse_script,
        target=target,
        counts=counts,
        source_input_identity=_source_input_identity(slices),
        target_input_identity=target.identity,
        reconciliation_fingerprint=analysis.summary.fingerprint.value,
        expected_target_fingerprint=expected_fingerprint(expected_inventory).value,
        plan_fingerprint=canonical_plan_fingerprint(),
        csv_fixture_bytes=csv_bytes,
        jsonl_fixture_bytes=jsonl_bytes,
    )


def partitioned_dataset(slices: tuple[SourceSlice, ...], key: str) -> SyntheticDataset:
    """Return one derived source slice dataset by its canonical key."""
    for slice_item in slices:
        if slice_item.key == key:
            return slice_item.dataset
    raise ScenarioError(f"unknown source slice: {key}")


def _analyze(
    slices: tuple[SourceSlice, ...],
    target: TargetSide,
    csv_skus: frozenset[str],
) -> ReconciliationAnalysis:
    return analyze_reconciliation(
        ReconciliationAnalysisRequest(
            source_observations=tuple(
                _observation(row, slice_item.connector, csv_skus)
                for slice_item in slices
                for row in slice_item.dataset.rows
            ),
            target_observations=tuple(
                SourceObservation(
                    position=100_000 + index,
                    connector_id=WAREHOUSE_CONNECTOR,
                    payload=payload,
                )
                for index, payload in enumerate(target.payloads)
            ),
            source_input_identity=_source_input_identity(slices),
            target_input_identity=target.identity,
        )
    )


def _slice_rows(dataset: SyntheticDataset) -> dict[str, list[WireRow]]:
    """Partition parent rows into per-source ordered subsets.

    Malformed rows are carried only by the file sources: the accepted HTTP
    connectors reject a record-level contract violation instead of emitting
    bounded malformed-row evidence, while the CSV and JSON Lines connectors
    quarantine it per row.  Valid, boundary, and duplicate base rows rotate
    across all four sources by base-row order; a duplicate row always joins the
    slice that already carries its base row, so every duplicate group stays
    inside one source.
    """
    slices: dict[str, list[WireRow]] = {key: [] for key in SOURCE_KEYS}
    base_ordinal = 0
    malformed_ordinal = 0
    source_of_last_base = SOURCE_KEYS[0]
    for row in dataset.rows:
        if row.role is RowRole.MALFORMED:
            key = "csv" if malformed_ordinal % 2 == 0 else "jsonl"
            malformed_ordinal += 1
            slices[key].append(row)
            continue
        if row.role is RowRole.DUPLICATE:
            slices[source_of_last_base].append(row)
            continue
        source_of_last_base = SOURCE_KEYS[base_ordinal % len(SOURCE_KEYS)]
        base_ordinal += 1
        slices[source_of_last_base].append(row)
    return slices


def _expected_requests(row_count: int, page_size: int) -> int:
    return max((row_count + page_size - 1) // page_size, 1)


def _observation(
    row: WireRow, connector: ConnectorId, csv_skus: frozenset[str] | set[str]
) -> SourceObservation:
    """Build one source observation for the derivation.

    The canonical CSV wire format carries no attributes column value, so rows
    read through the CSV source strip the attribute map on both the source and
    the target side.  The runner applies the same rule to the records the CSV
    connector actually emits.
    """
    if row.role is RowRole.MALFORMED:
        return SourceObservation(
            position=row.index,
            connector_id=connector,
            payload=None,
            malformed_reason="the connector rejected the record",
        )
    payload = cast("dict[str, object]", dict(row.payload))
    if cast("str", row.payload["sku"]) in csv_skus:
        payload["attributes"] = {}
    return SourceObservation(
        position=row.index,
        connector_id=connector,
        payload=payload,
    )


def _framed(values: list[bytes]) -> bytes:
    framed = bytearray()
    for value in values:
        framed += len(value).to_bytes(8, byteorder="big") + value
    return bytes(framed)


def _source_input_identity(slices: tuple[SourceSlice, ...]) -> str:
    preimage = b"paritygrid-canonical-source-identity-v1\0" + _framed(
        [slice_item.dataset.manifest.dataset_id.encode("ascii") for slice_item in slices]
    )
    return sha256(preimage).hexdigest()


def _target_payload_identity(payloads: tuple[dict[str, object], ...]) -> str:
    preimage = b"paritygrid-canonical-target-identity-v1\0" + _framed(
        [canonical_json_bytes(cast("Mapping[str, WireValue]", payload)) for payload in payloads]
    )
    return sha256(preimage).hexdigest()


def _derive_target_side(
    dataset: SyntheticDataset, csv_skus: frozenset[str] | set[str]
) -> TargetSide:
    """Derive the deterministic divergent warehouse inventory.

    The rules are fixed by the scenario format: enumerated over the first
    record of every canonical key in row order, duplicate-source keys at
    positions congruent to 2 modulo 5 are duplicated on the target side too
    (duplicate-both); single-record keys at positions congruent to 3 modulo 7
    are dropped (missing-from-target), positions congruent to 5 modulo 11 load
    with a changed quantity (field mismatch), positions congruent to 7 modulo 13
    load twice (duplicate-target), and the remainder load unchanged (match).  A
    fixed count of synthetic target-only keys completes the inventory so the
    missing-from-source classification is exercised too.
    """
    first_by_key: dict[str, dict[str, object]] = {}
    key_counts: dict[str, int] = {}
    for row in dataset.rows:
        if row.role is RowRole.MALFORMED:
            continue
        payload = cast("dict[str, object]", dict(row.payload))
        sku = cast("str", payload["sku"])
        if sku in csv_skus:
            payload["attributes"] = {}
        key_counts[sku] = key_counts.get(sku, 0) + 1
        first_by_key.setdefault(sku, payload)
    payloads: list[dict[str, object]] = []
    for index, (sku, payload) in enumerate(first_by_key.items()):
        if key_counts[sku] > 1:
            payloads.append(payload)
            if index % 5 == 2:
                payloads.append(dict(payload))
            continue
        if index % 7 == 3:
            continue
        if index % 11 == 5:
            tweaked = dict(payload)
            quantity = cast(int, payload["quantity"])
            tweaked["quantity"] = quantity - 3 if quantity > 3 else quantity + 3
            payloads.append(tweaked)
            continue
        payloads.append(payload)
        if index % 13 == 7:
            payloads.append(dict(payload))
    payloads.extend(_target_only_payload(extra) for extra in range(TARGET_ONLY_KEY_COUNT))
    ordered = tuple(payloads)
    return TargetSide(payloads=ordered, identity=_target_payload_identity(ordered))


def _target_only_payload(index: int) -> dict[str, object]:
    sku = f"GRID-TGT-ONLY-{index + 1:04d}"
    return {
        "attributes": {"grade": "target-only"},
        "name": f"Target-only reserve unit {index + 1}",
        "quantity": 100 + index,
        "sku": sku,
        "source_record_key": f"tgt-only-{index + 1:04d}",
        "unit_price": {"amount": "40.00", "currency": "USD"},
        "updated_at": "2025-01-15T08:30:00.000000Z",
    }


def _counts(
    profile: CanonicalScenarioProfile,
    slices: tuple[SourceSlice, ...],
    analysis: ReconciliationAnalysis,
) -> ScenarioCounts:
    total_rows = sum(slice_item.dataset.manifest.counts["total"] for slice_item in slices)
    malformed_rows = sum(slice_item.dataset.manifest.counts["malformed"] for slice_item in slices)
    boundary_rows = sum(slice_item.dataset.manifest.counts["boundary"] for slice_item in slices)
    duplicate_rows = sum(slice_item.dataset.manifest.counts["duplicate"] for slice_item in slices)
    classified = dict(analysis.summary.counts.by_classification)
    planned = (
        classified[ReconciliationClassification.MISSING_FROM_TARGET]
        + classified[ReconciliationClassification.FIELD_MISMATCH]
    )
    duplicate_keys = (
        classified[ReconciliationClassification.DUPLICATE_SOURCE]
        + classified[ReconciliationClassification.DUPLICATE_TARGET]
        + classified[ReconciliationClassification.DUPLICATE_BOTH]
    )
    # One duplicate group per duplicated side; duplicate-both keys duplicate
    # both sides and therefore contribute two groups.
    duplicate_groups = (
        classified[ReconciliationClassification.DUPLICATE_SOURCE]
        + classified[ReconciliationClassification.DUPLICATE_TARGET]
        + 2 * classified[ReconciliationClassification.DUPLICATE_BOTH]
    )
    if len(analysis.duplicate_groups) != duplicate_groups:
        raise ScenarioError("duplicate-group derivation disagrees with the analysis")
    return ScenarioCounts(
        total_input_rows=total_rows,
        accepted_rows=total_rows - malformed_rows,
        rejected_rows=malformed_rows,
        quarantined_rows=len(analysis.source_quarantined),
        boundary_rows=boundary_rows,
        duplicate_rows=duplicate_rows,
        duplicate_groups=duplicate_groups,
        canonical_keys=analysis.summary.counts.canonical_key_count,
        match=classified[ReconciliationClassification.MATCH],
        missing_from_target=classified[ReconciliationClassification.MISSING_FROM_TARGET],
        missing_from_source=classified[ReconciliationClassification.MISSING_FROM_SOURCE],
        field_mismatch=classified[ReconciliationClassification.FIELD_MISMATCH],
        duplicate_source=classified[ReconciliationClassification.DUPLICATE_SOURCE],
        duplicate_target=classified[ReconciliationClassification.DUPLICATE_TARGET],
        duplicate_both=classified[ReconciliationClassification.DUPLICATE_BOTH],
        planned_repairs=planned,
        applied_repairs=planned,
        review_only_repairs=(
            classified[ReconciliationClassification.MISSING_FROM_SOURCE] + duplicate_keys
        ),
        rate_limit_retries=LOCKED_RATE_LIMIT_RETRIES,
        transient_connection_failures=LOCKED_TRANSIENT_CONNECTION_FAILURES,
        ambiguous_replays_resolved=LOCKED_TRANSIENT_CONNECTION_FAILURES,
        artifacts=ARTIFACT_COUNT,
        async_http_requests=slices[0].expected_requests + profile.rate_limit_request,
        blocking_http_requests=slices[1].expected_requests,
    )


@dataclass(frozen=True, slots=True)
class CanonicalScenarioManifest:
    """The strict canonical manifest of one executed or expected scenario."""

    scenario_version: int
    seed: int
    generator_version: int
    profile_id: str
    profile_identity: str
    pipeline_id: str
    pipeline_version: int
    plan_fingerprint: str
    source_script_identity: str
    source_script_entries: tuple[dict[str, object], ...]
    warehouse_script_identity: str
    warehouse_script_entries: tuple[dict[str, object], ...]
    async_http_dataset_id: str
    async_http_rows: int
    blocking_http_dataset_id: str
    blocking_http_rows: int
    csv_dataset_id: str
    csv_fixture_sha256: str
    csv_fixture_bytes: int
    csv_rows: int
    jsonl_dataset_id: str
    jsonl_fixture_sha256: str
    jsonl_fixture_bytes: int
    jsonl_rows: int
    target_input_identity: str
    target_records: int
    source_input_identity: str
    artifact_identities: tuple[str, ...]
    counts: ScenarioCounts
    reconciliation_fingerprint: str
    expected_target_fingerprint: str
    execution_evidence_fingerprint: str | None
    verification_result: str
    csv_fixture_size: int
    jsonl_fixture_size: int

    def canonical_bytes(self) -> bytes:
        """Return the byte-stable canonical manifest document."""
        document = {
            "artifacts": {
                "count": len(self.artifact_identities),
                "identities": list(self.artifact_identities),
            },
            "bytes": {
                "csv_fixture": self.csv_fixture_size,
                "jsonl_fixture": self.jsonl_fixture_size,
                "total_generated": self.csv_fixture_size + self.jsonl_fixture_size,
            },
            "counts": self.counts.as_mapping(),
            "failure_scripts": {
                "source": {
                    "entries": list(self.source_script_entries),
                    "identity": self.source_script_identity,
                    "version": SCRIPTED_FAILURE_VERSION,
                },
                "warehouse": {
                    "entries": list(self.warehouse_script_entries),
                    "identity": self.warehouse_script_identity,
                    "version": SCRIPTED_FAILURE_VERSION,
                },
            },
            "fingerprints": {
                "expected_target_state": {
                    "kind": TARGET_STATE_FINGERPRINT_KIND,
                    "value": self.expected_target_fingerprint,
                    "version": TARGET_STATE_FINGERPRINT_VERSION,
                },
                "execution_evidence": {
                    "kind": EXECUTION_EVIDENCE_FINGERPRINT_KIND,
                    "value": self.execution_evidence_fingerprint,
                    "version": EXECUTION_EVIDENCE_FINGERPRINT_VERSION,
                },
                "plan": {
                    "kind": PLAN_FINGERPRINT_KIND,
                    "value": self.plan_fingerprint,
                    "version": PLAN_FINGERPRINT_VERSION,
                },
                "reconciliation": {
                    "kind": RECONCILIATION_FINGERPRINT_KIND,
                    "value": self.reconciliation_fingerprint,
                    "version": RECONCILIATION_FINGERPRINT_VERSION,
                },
            },
            "format": SCENARIO_FORMAT_NAME,
            "format_version": SCENARIO_FORMAT_VERSION,
            "generator_version": self.generator_version,
            "inputs": {
                "async_http": {
                    "dataset_id": self.async_http_dataset_id,
                    "rows": self.async_http_rows,
                },
                "blocking_http": {
                    "dataset_id": self.blocking_http_dataset_id,
                    "rows": self.blocking_http_rows,
                },
                "csv": {
                    "dataset_id": self.csv_dataset_id,
                    "fixture_sha256": self.csv_fixture_sha256,
                    "fixture_size": self.csv_fixture_bytes,
                    "rows": self.csv_rows,
                },
                "jsonl": {
                    "dataset_id": self.jsonl_dataset_id,
                    "fixture_sha256": self.jsonl_fixture_sha256,
                    "fixture_size": self.jsonl_fixture_bytes,
                    "rows": self.jsonl_rows,
                },
                "source_identity": self.source_input_identity,
                "target": {
                    "identity": self.target_input_identity,
                    "records": self.target_records,
                },
            },
            "pipeline": {
                "id": self.pipeline_id,
                "version": self.pipeline_version,
            },
            "profile": {
                "id": self.profile_id,
                "identity": self.profile_identity,
            },
            "scenario_version": self.scenario_version,
            "seed": self.seed,
            "verification": {"result": self.verification_result},
        }
        return canonical_json_bytes(cast("Mapping[str, WireValue]", document))


def build_manifest(
    evidence: ScenarioExpectedEvidence,
    *,
    execution_evidence_fingerprint: str | None,
    verification_result: str,
) -> CanonicalScenarioManifest:
    """Build the canonical manifest from derived evidence and run facts."""
    csv_slice = evidence.slice_for("csv")
    jsonl_slice = evidence.slice_for("jsonl")
    return CanonicalScenarioManifest(
        scenario_version=CANONICAL_SCENARIO_VERSION,
        seed=CANONICAL_SCENARIO_SEED,
        generator_version=DATASET_GENERATOR_VERSION,
        profile_id=evidence.profile.profile_id,
        profile_identity=evidence.profile.identity_bytes().decode("ascii"),
        pipeline_id=CANONICAL_PIPELINE_ID,
        pipeline_version=CANONICAL_PIPELINE_VERSION,
        plan_fingerprint=evidence.plan_fingerprint,
        source_script_identity=_script_identity(evidence.source_failure_script),
        source_script_entries=evidence.source_failure_script.describe(),
        warehouse_script_identity=_script_identity(evidence.warehouse_failure_script),
        warehouse_script_entries=evidence.warehouse_failure_script.describe(),
        async_http_dataset_id=evidence.slice_for("async_http").dataset.manifest.dataset_id,
        async_http_rows=len(evidence.slice_for("async_http").dataset.rows),
        blocking_http_dataset_id=evidence.slice_for("blocking_http").dataset.manifest.dataset_id,
        blocking_http_rows=len(evidence.slice_for("blocking_http").dataset.rows),
        csv_dataset_id=csv_slice.dataset.manifest.dataset_id,
        csv_fixture_sha256=sha256(evidence.csv_fixture_bytes).hexdigest(),
        csv_fixture_bytes=len(evidence.csv_fixture_bytes),
        csv_rows=len(csv_slice.dataset.rows),
        jsonl_dataset_id=jsonl_slice.dataset.manifest.dataset_id,
        jsonl_fixture_sha256=sha256(evidence.jsonl_fixture_bytes).hexdigest(),
        jsonl_fixture_bytes=len(evidence.jsonl_fixture_bytes),
        jsonl_rows=len(jsonl_slice.dataset.rows),
        target_input_identity=evidence.target_input_identity,
        target_records=len(evidence.target.payloads),
        source_input_identity=evidence.source_input_identity,
        artifact_identities=CANONICAL_ARTIFACT_IDENTITIES,
        counts=evidence.counts,
        reconciliation_fingerprint=evidence.reconciliation_fingerprint,
        expected_target_fingerprint=evidence.expected_target_fingerprint,
        execution_evidence_fingerprint=execution_evidence_fingerprint,
        verification_result=verification_result,
        csv_fixture_size=len(evidence.csv_fixture_bytes),
        jsonl_fixture_size=len(evidence.jsonl_fixture_bytes),
    )


def _script_identity(script: FailureScript) -> str:
    return sha256(script.to_canonical_bytes()).hexdigest()


_FIELDS_HEX_64 = frozenset(
    {
        "plan_fingerprint",
        "source_script_identity",
        "warehouse_script_identity",
        "async_http_dataset_id",
        "blocking_http_dataset_id",
        "csv_dataset_id",
        "jsonl_dataset_id",
        "target_input_identity",
        "source_input_identity",
        "reconciliation_fingerprint",
        "expected_target_fingerprint",
        "csv_fixture_sha256",
        "jsonl_fixture_sha256",
    }
)
_COUNT_FIELDS = frozenset(
    {
        "accepted_rows",
        "ambiguous_replays_resolved",
        "applied_repairs",
        "artifacts",
        "async_http_requests",
        "blocking_http_requests",
        "boundary_rows",
        "canonical_keys",
        "duplicate_both",
        "duplicate_groups",
        "duplicate_rows",
        "duplicate_source",
        "duplicate_target",
        "field_mismatch",
        "match",
        "missing_from_source",
        "missing_from_target",
        "planned_repairs",
        "quarantined_rows",
        "rate_limit_retries",
        "rejected_rows",
        "review_only_repairs",
        "total_input_rows",
        "transient_connection_failures",
    }
)


def parse_canonical_scenario_manifest(payload: bytes) -> CanonicalScenarioManifest:
    """Parse and fully validate canonical manifest bytes.

    Strictly rejects wrong formats, unsupported versions, unknown fields,
    missing fields, malformed identities, unknown fingerprint kinds or
    versions, incoherent counts, and truncated or oversized documents.
    """
    if len(payload) > _MAX_MANIFEST_BYTES:
        raise ScenarioError("manifest exceeds the size bound")
    try:
        document_value = json.loads(payload.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ScenarioError("manifest is not valid UTF-8 JSON") from error
    if not isinstance(document_value, dict):
        raise ScenarioError("manifest must be a JSON object")
    document = cast("dict[str, object]", document_value)
    if document.get("format") != SCENARIO_FORMAT_NAME:
        raise ScenarioError("manifest carries an unknown format")
    if document.get("format_version") != SCENARIO_FORMAT_VERSION:
        raise ScenarioError("manifest carries an unsupported format version")
    expected_top = {
        "artifacts",
        "bytes",
        "counts",
        "failure_scripts",
        "fingerprints",
        "format",
        "format_version",
        "generator_version",
        "inputs",
        "pipeline",
        "profile",
        "scenario_version",
        "seed",
        "verification",
    }
    unknown = set(document) - expected_top
    if unknown:
        raise ScenarioError(f"manifest carries unknown fields: {sorted(unknown)}")
    missing = expected_top - set(document)
    if missing:
        raise ScenarioError(f"manifest is missing fields: {sorted(missing)}")
    counts = _parse_counts(document["counts"])
    errors = counts.coherence_errors()
    if errors:
        raise ScenarioError(f"manifest counts are incoherent: {'; '.join(errors)}")
    artifacts = _require_mapping(document["artifacts"], "artifacts")
    if set(artifacts) != {"count", "identities"}:
        raise ScenarioError("artifacts must carry exactly count and identities")
    artifact_count = _require_int(artifacts["count"], "artifact count")
    artifact_values = artifacts["identities"]
    if not isinstance(artifact_values, list):
        raise ScenarioError("artifact identities must be a list of text values")
    artifact_items = cast("list[object]", artifact_values)
    if any(not isinstance(value, str) for value in artifact_items):
        raise ScenarioError("artifact identities must be a list of text values")
    artifact_identities = tuple(cast("list[str]", artifact_items))
    if artifact_count != len(artifact_identities):
        raise ScenarioError("artifact count does not match its identities")
    fingerprints = _require_mapping(document["fingerprints"], "fingerprints")
    if set(fingerprints) != _FINGERPRINT_KEYS:
        raise ScenarioError("manifest fingerprints must carry exactly the four kinds")
    plan = _require_fingerprint(
        fingerprints["plan"], PLAN_FINGERPRINT_KIND, PLAN_FINGERPRINT_VERSION
    )
    reconciliation = _require_fingerprint(
        fingerprints["reconciliation"],
        RECONCILIATION_FINGERPRINT_KIND,
        RECONCILIATION_FINGERPRINT_VERSION,
    )
    target_state = _require_fingerprint(
        fingerprints["expected_target_state"],
        TARGET_STATE_FINGERPRINT_KIND,
        TARGET_STATE_FINGERPRINT_VERSION,
    )
    execution_evidence = _parse_fingerprint(
        fingerprints["execution_evidence"],
        EXECUTION_EVIDENCE_FINGERPRINT_KIND,
        EXECUTION_EVIDENCE_FINGERPRINT_VERSION,
        allow_null=True,
    )
    inputs = _require_mapping(document["inputs"], "inputs")
    expected_inputs = {"async_http", "blocking_http", "csv", "jsonl", "source_identity", "target"}
    if set(inputs) != expected_inputs:
        raise ScenarioError("manifest inputs carry unknown or missing members")
    async_http = _require_mapping(inputs["async_http"], "async_http input")
    blocking_http = _require_mapping(inputs["blocking_http"], "blocking_http input")
    csv_input = _require_mapping(inputs["csv"], "csv input")
    jsonl_input = _require_mapping(inputs["jsonl"], "jsonl input")
    target_input = _require_mapping(inputs["target"], "target input")
    failure_scripts = _require_mapping(document["failure_scripts"], "failure_scripts")
    if set(failure_scripts) != {"source", "warehouse"}:
        raise ScenarioError("manifest failure scripts must carry source and warehouse")
    source_script = _require_mapping(failure_scripts["source"], "source script")
    warehouse_script = _require_mapping(failure_scripts["warehouse"], "warehouse script")
    for script in (source_script, warehouse_script):
        if set(script) != {"entries", "identity", "version"}:
            raise ScenarioError("a failure script must carry exactly its closed fields")
        if script["version"] != SCRIPTED_FAILURE_VERSION:
            raise ScenarioError("failure script carries an unsupported version")
        if not isinstance(script["entries"], list):
            raise ScenarioError("failure script entries must be a list")
    pipeline = _require_mapping(document["pipeline"], "pipeline")
    if set(pipeline) != {"id", "version"}:
        raise ScenarioError("pipeline must carry exactly id and version")
    profile = _require_mapping(document["profile"], "profile")
    if set(profile) != {"id", "identity"}:
        raise ScenarioError("profile must carry exactly id and identity")
    byte_section = _require_mapping(document["bytes"], "bytes")
    if set(byte_section) != {"csv_fixture", "jsonl_fixture", "total_generated"}:
        raise ScenarioError("bytes must carry exactly the closed fields")
    verification = _require_mapping(document["verification"], "verification")
    if set(verification) != {"result"}:
        raise ScenarioError("verification must carry exactly its result")
    if verification["result"] != "parity_holding":
        raise ScenarioError("the canonical scenario locks the parity_holding result")
    parsed_source_entries = _validate_script_entries(source_script["entries"])
    parsed_warehouse_entries = _validate_script_entries(warehouse_script["entries"])

    def _bound_identity(name: str, entries: list[ScriptedFailure], identity: object) -> None:
        try:
            derived = _script_identity(FailureScript.from_entries(entries))
        except FailureScriptError as error:
            raise ScenarioError(f"invalid {name} failure script: {error}") from error
        if _require_text(identity, "script identity") != derived:
            raise ScenarioError(f"the {name} script identity does not match its entries")

    _bound_identity("source", parsed_source_entries, source_script["identity"])
    _bound_identity("warehouse", parsed_warehouse_entries, warehouse_script["identity"])
    scenario_version = _require_int(document["scenario_version"], "scenario_version")
    seed = _require_int(document["seed"], "seed")
    generator_version = _require_int(document["generator_version"], "generator_version")
    profile_id = _require_text(profile["id"], "profile id")
    profile_identity = _require_text(profile["identity"], "profile identity")
    pipeline_id = _require_text(pipeline["id"], "pipeline id")
    pipeline_version = _require_int(pipeline["version"], "pipeline version")
    if scenario_version != CANONICAL_SCENARIO_VERSION:
        raise ScenarioError("manifest carries an unsupported scenario version")
    if seed != CANONICAL_SCENARIO_SEED:
        raise ScenarioError("manifest carries an unsupported scenario seed")
    if generator_version != DATASET_GENERATOR_VERSION:
        raise ScenarioError("manifest carries an unsupported generator version")
    if pipeline_id != CANONICAL_PIPELINE_ID or pipeline_version != CANONICAL_PIPELINE_VERSION:
        raise ScenarioError("manifest carries an unsupported pipeline identity")
    known_profile = PROFILES.get(profile_id)
    if known_profile is None:
        raise ScenarioError("manifest carries an unknown profile id")
    if profile_identity != known_profile.identity_bytes().decode("ascii"):
        raise ScenarioError("manifest profile identity does not match its profile id")
    manifest = CanonicalScenarioManifest(
        scenario_version=scenario_version,
        seed=seed,
        generator_version=generator_version,
        profile_id=profile_id,
        profile_identity=profile_identity,
        pipeline_id=pipeline_id,
        pipeline_version=pipeline_version,
        plan_fingerprint=plan,
        source_script_identity=_require_text(source_script["identity"], "script identity"),
        source_script_entries=tuple(
            cast("dict[str, object]", entry)
            for entry in cast("list[object]", source_script["entries"])
        ),
        warehouse_script_identity=_require_text(warehouse_script["identity"], "script identity"),
        warehouse_script_entries=tuple(
            cast("dict[str, object]", entry)
            for entry in cast("list[object]", warehouse_script["entries"])
        ),
        async_http_dataset_id=_require_text(async_http["dataset_id"], "dataset id"),
        async_http_rows=_require_int(async_http["rows"], "async rows"),
        blocking_http_dataset_id=_require_text(blocking_http["dataset_id"], "dataset id"),
        blocking_http_rows=_require_int(blocking_http["rows"], "blocking rows"),
        csv_dataset_id=_require_text(csv_input["dataset_id"], "dataset id"),
        csv_fixture_sha256=_require_text(csv_input["fixture_sha256"], "fixture sha256"),
        csv_fixture_bytes=_require_int(csv_input["fixture_size"], "fixture size"),
        csv_rows=_require_int(csv_input["rows"], "csv rows"),
        jsonl_dataset_id=_require_text(jsonl_input["dataset_id"], "dataset id"),
        jsonl_fixture_sha256=_require_text(jsonl_input["fixture_sha256"], "fixture sha256"),
        jsonl_fixture_bytes=_require_int(jsonl_input["fixture_size"], "fixture size"),
        jsonl_rows=_require_int(jsonl_input["rows"], "jsonl rows"),
        target_input_identity=_require_text(target_input["identity"], "target identity"),
        target_records=_require_int(target_input["records"], "target records"),
        source_input_identity=_require_text(inputs["source_identity"], "source identity"),
        artifact_identities=artifact_identities,
        counts=counts,
        reconciliation_fingerprint=reconciliation,
        expected_target_fingerprint=target_state,
        execution_evidence_fingerprint=execution_evidence,
        verification_result="parity_holding",
        csv_fixture_size=_require_int(byte_section["csv_fixture"], "csv fixture bytes"),
        jsonl_fixture_size=_require_int(byte_section["jsonl_fixture"], "jsonl fixture bytes"),
    )
    _require_hex64(manifest)
    if manifest.csv_fixture_size != manifest.csv_fixture_bytes:
        raise ScenarioError("csv fixture byte sections disagree")
    if manifest.jsonl_fixture_size != manifest.jsonl_fixture_bytes:
        raise ScenarioError("jsonl fixture byte sections disagree")
    if byte_section["total_generated"] != (manifest.csv_fixture_size + manifest.jsonl_fixture_size):
        raise ScenarioError("total generated bytes must equal the fixture sizes")
    expected = build_manifest(
        derive_scenario(known_profile),
        execution_evidence_fingerprint=execution_evidence,
        verification_result="parity_holding",
    )
    if manifest != expected:
        raise ScenarioError("manifest facts do not match the locked canonical profile")
    if payload != manifest.canonical_bytes():
        raise ScenarioError("manifest must use the canonical byte encoding")
    return manifest


def _require_hex64(manifest: CanonicalScenarioManifest) -> None:
    for name in _FIELDS_HEX_64:
        if not _is_lowercase_hex64(getattr(manifest, name)):
            raise ScenarioError(f"{name} must be lowercase 64-hex")


def _is_lowercase_hex64(value: object) -> bool:
    if not isinstance(value, str) or len(value) != 64 or value.lower() != value:
        return False
    try:
        int(value, 16)
    except ValueError:
        return False
    return True


_SCRIPT_ENTRY_FIELDS = frozenset(
    {"kind", "sequence", "retry_after_seconds", "delay_microseconds", "partial_bytes"}
)


def _validate_script_entries(entries: object) -> list[ScriptedFailure]:
    """Validate manifest script entries against the closed failure contract."""
    if not isinstance(entries, list):
        raise ScenarioError("failure script entries must be a list")
    failures: list[ScriptedFailure] = []
    for entry_value in cast("list[object]", entries):
        if not isinstance(entry_value, dict):
            raise ScenarioError("each failure script entry must be an object")
        entry = cast("dict[str, object]", entry_value)
        if not set(entry) <= _SCRIPT_ENTRY_FIELDS or "kind" not in entry:
            raise ScenarioError("failure script entry carries unknown or missing fields")
        kind_value = entry["kind"]
        if not isinstance(kind_value, str):
            raise ScenarioError("failure script entry kind must be text")
        try:
            kind = ScriptedFailureKind(kind_value)
        except ValueError as error:
            raise ScenarioError(f"unknown failure kind: {kind_value!r}") from error
        try:
            failures.append(
                ScriptedFailure(
                    sequence=_require_entry_int(entry, "sequence"),
                    kind=kind,
                    retry_after_seconds=_optional_entry_int(entry, "retry_after_seconds"),
                    delay_microseconds=_optional_entry_int(entry, "delay_microseconds"),
                    partial_bytes=_optional_entry_int(entry, "partial_bytes"),
                )
            )
        except FailureScriptError as error:
            raise ScenarioError(f"invalid failure script entry: {error}") from error
    return failures


def _require_entry_int(entry: dict[str, object], key: str) -> int:
    value = entry[key]
    if isinstance(value, bool) or not isinstance(value, int):
        raise ScenarioError(f"failure script entry {key} must be an integer")
    return value


def _optional_entry_int(entry: dict[str, object], key: str) -> int | None:
    if key not in entry:
        return None
    return _require_entry_int(entry, key)


def _parse_counts(value: object) -> ScenarioCounts:
    mapping = _require_mapping(value, "counts")
    if set(mapping) != set(_COUNT_FIELDS):
        raise ScenarioError("manifest counts carry unknown or missing fields")
    kwargs = {name: _require_int(mapping[name], name) for name in _COUNT_FIELDS}
    for name, count in kwargs.items():
        if count < 0:
            raise ScenarioError(f"count {name} must not be negative")
    return ScenarioCounts(**kwargs)


def _require_fingerprint(value: object, kind: str, version: int) -> str:
    fingerprint = _parse_fingerprint(value, kind, version)
    if fingerprint is None:
        raise ScenarioError(f"fingerprint {kind} must carry a value")
    return fingerprint


def _parse_fingerprint(
    value: object,
    kind: str,
    version: int,
    *,
    allow_null: bool = False,
) -> str | None:
    mapping = _require_mapping(value, "fingerprint")
    if set(mapping) != {"kind", "value", "version"}:
        raise ScenarioError("a fingerprint must carry exactly kind, value, and version")
    if mapping["kind"] != kind:
        raise ScenarioError(f"fingerprint kind must be {kind}")
    if mapping["version"] != version:
        raise ScenarioError(f"fingerprint {kind} carries an unsupported version")
    if mapping["value"] is None:
        if not allow_null:
            raise ScenarioError(f"fingerprint {kind} must carry a value")
        return None
    fingerprint = _require_text(mapping["value"], "fingerprint value")
    if not _is_lowercase_hex64(fingerprint):
        raise ScenarioError("fingerprint values must be lowercase 64-hex")
    return fingerprint


def _require_mapping(value: object, subject: str) -> dict[str, object]:
    if not isinstance(value, dict):
        raise ScenarioError(f"{subject} must be a JSON object")
    return cast("dict[str, object]", value)


def _require_text(value: object, subject: str) -> str:
    if not isinstance(value, str) or not value:
        raise ScenarioError(f"{subject} must be non-empty text")
    return value


def _require_int(value: object, subject: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ScenarioError(f"{subject} must be an integer")
    return value


__all__ = [
    "ARTIFACT_COUNT",
    "ASYNC_RATE_LIMIT_REQUEST",
    "ASYNC_SOURCE_CONNECTOR",
    "BLOCKING_SOURCE_CONNECTOR",
    "CANONICAL_CORRELATION_ID",
    "CANONICAL_EDGES",
    "CANONICAL_NODES",
    "CANONICAL_NODE_KINDS",
    "CANONICAL_PARTITIONS_BY_NODE",
    "CANONICAL_PIPELINE_ID",
    "CANONICAL_PIPELINE_VERSION",
    "CANONICAL_RUN_ID",
    "CANONICAL_SCENARIO_SEED",
    "CANONICAL_SCENARIO_VERSION",
    "CSV_SOURCE_CONNECTOR",
    "EXECUTION_EVIDENCE_FINGERPRINT_KIND",
    "EXECUTION_EVIDENCE_FINGERPRINT_VERSION",
    "FAST_PROFILE",
    "JSONL_SOURCE_CONNECTOR",
    "LOCKED_RATE_LIMIT_RETRIES",
    "LOCKED_TRANSIENT_CONNECTION_FAILURES",
    "NODE_APPLY",
    "NODE_APPROVAL",
    "NODE_ASYNC_SOURCE",
    "NODE_BLOCKING_SOURCE",
    "NODE_CSV_SOURCE",
    "NODE_EXPORT",
    "NODE_JSONL_SOURCE",
    "NODE_NORMALIZE",
    "NODE_PARTITION",
    "NODE_RECONCILE",
    "NODE_REPAIR_PLAN",
    "NODE_VALIDATE",
    "NODE_VERIFY",
    "PLAN_FINGERPRINT_KIND",
    "PLAN_FINGERPRINT_VERSION",
    "PROFILES",
    "RECONCILIATION_FINGERPRINT_KIND",
    "RECONCILIATION_FINGERPRINT_VERSION",
    "SCENARIO_FORMAT_NAME",
    "SCENARIO_FORMAT_VERSION",
    "SHOWCASE_PROFILE",
    "SOURCE_KEYS",
    "TARGET_ONLY_KEY_COUNT",
    "TARGET_STATE_FINGERPRINT_KIND",
    "TARGET_STATE_FINGERPRINT_VERSION",
    "WAREHOUSE_CONNECTOR",
    "WAREHOUSE_FAULT_ACTION_INDEX",
    "CanonicalScenarioManifest",
    "CanonicalScenarioProfile",
    "ScenarioCounts",
    "ScenarioError",
    "ScenarioExpectedEvidence",
    "SourceSlice",
    "TargetSide",
    "build_manifest",
    "canonical_plan_fingerprint",
    "derive_scenario",
    "parse_canonical_scenario_manifest",
]

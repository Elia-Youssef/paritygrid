"""Manifest schema, derivation determinism, and bound tests (Phase 19)."""

import json
from dataclasses import fields
from hashlib import sha256

import pytest

from paritygrid.demo.datasets import (
    DATASET_GENERATOR_VERSION,
    DatasetProfile,
    ScenarioSeed,
    ScenarioVersion,
    derive_source_dataset,
    generate_dataset,
)
from paritygrid.demo.scenarios import (
    CANONICAL_SCENARIO_SEED,
    CANONICAL_SCENARIO_VERSION,
    FAST_PROFILE,
    SCENARIO_FORMAT_NAME,
    SCENARIO_FORMAT_VERSION,
    SHOWCASE_PROFILE,
    CanonicalScenarioProfile,
    ScenarioError,
    build_manifest,
    canonical_plan_fingerprint,
    derive_scenario,
    parse_canonical_scenario_manifest,
)

# Golden drift locks for the pure derivation: the expected manifests carry a
# null execution-evidence value because that fingerprint is derived from durable
# run evidence. The executed-run manifest goldens live in the runner test
# module. Any derivation change is a deliberate scenario version change that
# must update these hashes.
GOLDEN_FAST_MANIFEST_SHA256 = "0464d3d169bf5ac81d0edee2fa9ca50d6776d74f2bbb6d425b8a03b355f86ed3"
GOLDEN_SHOWCASE_MANIFEST_SHA256 = "c732777145b0bdcf9742773e16a2263bb5bcca606468ff4eef5a6776de9afc2a"

MAX_DATASET_ROWS = 5_000
MAX_FIXTURE_BYTES = 8 * 1024 * 1024


def _expected_bytes(profile: CanonicalScenarioProfile) -> bytes:
    return build_manifest(
        derive_scenario(profile),
        execution_evidence_fingerprint=None,
        verification_result="parity_holding",
    ).canonical_bytes()


class TestScenarioIdentity:
    def test_identity_constants_are_locked(self) -> None:
        assert SCENARIO_FORMAT_NAME == "paritygrid-canonical-scenario"
        assert SCENARIO_FORMAT_VERSION == 1
        assert CANONICAL_SCENARIO_VERSION == 1
        assert CANONICAL_SCENARIO_SEED == 19

    def test_plan_fingerprint_is_stable_and_hex(self) -> None:
        fingerprint = canonical_plan_fingerprint()
        assert fingerprint == canonical_plan_fingerprint()
        assert len(fingerprint) == 64
        int(fingerprint, 16)

    def test_profiles_stay_within_the_accepted_dataset_ceiling(self) -> None:
        for profile in (FAST_PROFILE, SHOWCASE_PROFILE):
            assert profile.record_count <= MAX_DATASET_ROWS
            assert profile.dataset_profile() == DatasetProfile(
                record_count=profile.record_count,
                malformed_count=profile.malformed_count,
                boundary_count=profile.boundary_count,
                duplicate_count=profile.duplicate_count,
            )

    @pytest.mark.parametrize(
        "field",
        [
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
        ],
    )
    def test_profile_identity_changes_with_each_parameter(self, field: str) -> None:
        base = FAST_PROFILE.identity_bytes()
        values = {field.name: getattr(FAST_PROFILE, field.name) for field in fields(FAST_PROFILE)}
        values[field] += 1
        mutated = CanonicalScenarioProfile(**values)
        assert mutated.identity_bytes() != base


class TestDerivationDeterminism:
    def test_repeated_derivation_is_byte_identical(self) -> None:
        for profile in (FAST_PROFILE, SHOWCASE_PROFILE):
            first = _expected_bytes(profile)
            second = _expected_bytes(profile)
            assert first == second

    def test_changed_seed_changes_the_dataset_identity(self) -> None:
        profile = FAST_PROFILE.dataset_profile()
        first = generate_dataset(ScenarioSeed(19), ScenarioVersion(1), profile)
        second = generate_dataset(ScenarioSeed(20), ScenarioVersion(1), profile)
        assert first.manifest.dataset_id != second.manifest.dataset_id

    def test_changed_scenario_version_changes_the_dataset_identity(self) -> None:
        profile = FAST_PROFILE.dataset_profile()
        first = generate_dataset(ScenarioSeed(19), ScenarioVersion(1), profile)
        second = generate_dataset(ScenarioSeed(19), ScenarioVersion(2), profile)
        assert first.manifest.dataset_id != second.manifest.dataset_id

    def test_derived_source_slices_reject_repeated_rows(self) -> None:
        dataset = generate_dataset(
            ScenarioSeed(CANONICAL_SCENARIO_SEED),
            ScenarioVersion(CANONICAL_SCENARIO_VERSION),
            FAST_PROFILE.dataset_profile(),
        )
        with pytest.raises(Exception, match="must not repeat a parent row"):
            derive_source_dataset(dataset, (dataset.rows[0], dataset.rows[0]))

    def test_derived_source_slices_reject_duplicate_script_sequences(self) -> None:
        evidence = derive_scenario(FAST_PROFILE)
        manifest = build_manifest(
            evidence,
            execution_evidence_fingerprint=None,
            verification_result="parity_holding",
        )
        document = json.loads(manifest.canonical_bytes().decode("ascii"))
        script = document["failure_scripts"]["source"]
        entry = dict(script["entries"][0])
        script["entries"] = [entry, dict(entry)]
        payload = json.dumps(document, separators=(",", ":"), sort_keys=True).encode("ascii")
        with pytest.raises(ScenarioError, match="invalid source failure script"):
            parse_canonical_scenario_manifest(payload)

    def test_derived_source_slices_carry_distinct_stable_identities(self) -> None:
        dataset = generate_dataset(
            ScenarioSeed(CANONICAL_SCENARIO_SEED),
            ScenarioVersion(CANONICAL_SCENARIO_VERSION),
            FAST_PROFILE.dataset_profile(),
        )
        left = derive_source_dataset(dataset, dataset.rows[:10])
        right = derive_source_dataset(dataset, dataset.rows[10:20])
        again = derive_source_dataset(dataset, dataset.rows[:10])
        assert left.manifest.dataset_id != right.manifest.dataset_id
        assert left.manifest.dataset_id == again.manifest.dataset_id
        assert left.manifest.seed == dataset.seed
        assert left.manifest.generator_version == DATASET_GENERATOR_VERSION

    def test_generator_version_is_explicit_in_every_manifest(self) -> None:
        evidence = derive_scenario(FAST_PROFILE)
        assert evidence.dataset.manifest.generator_version == DATASET_GENERATOR_VERSION


class TestLockedExpectedEvidence:
    def test_fast_expected_manifest_reproduces_its_golden_hash(self) -> None:
        assert sha256(_expected_bytes(FAST_PROFILE)).hexdigest() == (GOLDEN_FAST_MANIFEST_SHA256)

    def test_showcase_expected_manifest_reproduces_its_golden_hash(self) -> None:
        assert sha256(_expected_bytes(SHOWCASE_PROFILE)).hexdigest() == (
            GOLDEN_SHOWCASE_MANIFEST_SHA256
        )

    def test_every_profile_exercises_every_classification(self) -> None:
        classification_keys = (
            "match",
            "missing_from_target",
            "missing_from_source",
            "field_mismatch",
            "duplicate_source",
            "duplicate_target",
            "duplicate_both",
        )
        for profile in (FAST_PROFILE, SHOWCASE_PROFILE):
            counts = derive_scenario(profile).counts.as_mapping()
            classified = {key: counts[key] for key in classification_keys}
            assert all(value > 0 for value in classified.values()), classified

    def test_expected_counts_hold_the_relationships(self) -> None:
        for profile in (FAST_PROFILE, SHOWCASE_PROFILE):
            evidence = derive_scenario(profile)
            manifest = parse_canonical_scenario_manifest(_expected_bytes(profile))
            counts = manifest.counts.as_mapping()
            assert counts["total_input_rows"] == (counts["accepted_rows"] + counts["rejected_rows"])
            assert counts["rejected_rows"] == counts["quarantined_rows"]
            assert counts["planned_repairs"] == (
                counts["missing_from_target"] + counts["field_mismatch"]
            )
            assert counts["applied_repairs"] == counts["planned_repairs"]
            assert counts["rate_limit_retries"] == 1
            assert counts["transient_connection_failures"] == 1
            assert evidence.total_generated_bytes() == (
                len(evidence.csv_fixture_bytes) + len(evidence.jsonl_fixture_bytes)
            )

    def test_fixture_bytes_stay_within_the_accepted_bounds(self) -> None:
        for profile in (FAST_PROFILE, SHOWCASE_PROFILE):
            evidence = derive_scenario(profile)
            assert len(evidence.csv_fixture_bytes) <= MAX_FIXTURE_BYTES
            assert len(evidence.jsonl_fixture_bytes) <= MAX_FIXTURE_BYTES
            for slice_value in evidence.slices:
                assert len(slice_value.dataset.rows) <= MAX_DATASET_ROWS

    def test_fixture_encoding_is_lf_only_and_platform_independent(self) -> None:
        for profile in (FAST_PROFILE, SHOWCASE_PROFILE):
            evidence = derive_scenario(profile)
            for payload in (evidence.csv_fixture_bytes, evidence.jsonl_fixture_bytes):
                assert b"\r" not in payload
                assert payload.endswith(b"\n")

    def test_manifest_document_is_ascii_and_sorted(self) -> None:
        payload = _expected_bytes(FAST_PROFILE)
        payload.decode("ascii")
        document = json.loads(payload.decode("ascii"))
        assert list(document) == sorted(document)


class TestManifestSchema:
    def _payload(self) -> bytes:
        return _expected_bytes(FAST_PROFILE)

    def test_round_trip_preserves_every_locked_fact(self) -> None:
        manifest = parse_canonical_scenario_manifest(self._payload())
        again = parse_canonical_scenario_manifest(manifest.canonical_bytes())
        assert again == manifest

    @pytest.mark.parametrize(
        "mutation",
        [
            "unknown_top_field",
            "unknown_count_field",
            "unknown_fingerprint_kind",
            "missing_required_section",
            "bad_format",
            "bad_format_version",
            "wrong_fingerprint_kind",
            "wrong_fingerprint_version",
            "incoherent_total",
            "negative_count",
            "truncated",
            "not_json",
            "wrong_verification_result",
            "tampered_seed",
            "tampered_scenario_version",
            "tampered_generator_version",
            "tampered_pipeline_id",
            "unknown_profile_id",
            "tampered_profile_identity",
            "tampered_script_entries",
            "tampered_script_identity",
            "non_hex_fingerprint_value",
            "tampered_fixture_hash",
            "coherent_but_unlocked_counts",
            "tampered_artifact_identity",
            "noncanonical_encoding",
        ],
    )
    def test_strict_rejections(self, mutation: str) -> None:
        document = json.loads(self._payload().decode("ascii"))
        if mutation == "unknown_top_field":
            document["surprise"] = True
        elif mutation == "unknown_count_field":
            document["counts"]["mystery"] = 1
        elif mutation == "unknown_fingerprint_kind":
            document["fingerprints"]["final_universal"] = {
                "kind": "final_universal",
                "value": "0" * 64,
                "version": 1,
            }
        elif mutation == "missing_required_section":
            del document["inputs"]
        elif mutation == "bad_format":
            document["format"] = "something-else"
        elif mutation == "bad_format_version":
            document["format_version"] = 99
        elif mutation == "wrong_fingerprint_kind":
            document["fingerprints"]["reconciliation"]["kind"] = "execution-evidence"
        elif mutation == "wrong_fingerprint_version":
            document["fingerprints"]["reconciliation"]["version"] = 2
        elif mutation == "incoherent_total":
            document["counts"]["total_input_rows"] += 1
        elif mutation == "negative_count":
            document["counts"]["match"] = -1
        elif mutation == "truncated":
            with pytest.raises(ScenarioError):
                parse_canonical_scenario_manifest(self._payload()[:64])
            return
        elif mutation == "not_json":
            with pytest.raises(ScenarioError):
                parse_canonical_scenario_manifest(b"{not json")
            return
        elif mutation == "wrong_verification_result":
            document["verification"]["result"] = "assumed_equal"
        elif mutation == "tampered_seed":
            document["seed"] = 9999
        elif mutation == "tampered_scenario_version":
            document["scenario_version"] = 2
        elif mutation == "tampered_generator_version":
            document["generator_version"] = 99
        elif mutation == "tampered_pipeline_id":
            document["pipeline"]["id"] = "pip_something-else"
        elif mutation == "unknown_profile_id":
            document["profile"]["id"] = "unknown"
        elif mutation == "tampered_profile_identity":
            document["profile"]["identity"] = "{}"
        elif mutation == "tampered_script_entries":
            document["failure_scripts"]["source"]["entries"] = [
                {"sequence": 99, "kind": "rate_limit", "retry_after_seconds": 1}
            ]
        elif mutation == "tampered_script_identity":
            document["failure_scripts"]["source"]["identity"] = "0" * 64
        elif mutation == "non_hex_fingerprint_value":
            document["fingerprints"]["plan"]["value"] = "z" * 64
        elif mutation == "tampered_fixture_hash":
            document["inputs"]["csv"]["fixture_sha256"] = "0" * 64
        elif mutation == "coherent_but_unlocked_counts":
            document["counts"]["match"] -= 1
            document["counts"]["missing_from_source"] += 1
            document["counts"]["review_only_repairs"] += 1
        elif mutation == "tampered_artifact_identity":
            document["artifacts"]["identities"][2] = "art_forged-conflicts"
        elif mutation == "noncanonical_encoding":
            with pytest.raises(ScenarioError, match="canonical byte encoding"):
                parse_canonical_scenario_manifest(b" \n" + self._payload())
            return
        payload = json.dumps(document, separators=(",", ":"), sort_keys=True).encode("ascii")
        with pytest.raises(ScenarioError):
            parse_canonical_scenario_manifest(payload)

    def test_execution_evidence_value_may_be_null_but_kind_is_enforced(self) -> None:
        document = json.loads(self._payload().decode("ascii"))
        assert document["fingerprints"]["execution_evidence"]["kind"] == "execution-evidence"
        assert document["fingerprints"]["execution_evidence"]["version"] == 2
        document["fingerprints"]["execution_evidence"]["value"] = None
        payload = json.dumps(document, separators=(",", ":"), sort_keys=True).encode("ascii")
        parsed = parse_canonical_scenario_manifest(payload)
        assert parsed.execution_evidence_fingerprint is None
        document["fingerprints"]["execution_evidence"]["value"] = "ZZ"
        payload = json.dumps(document, separators=(",", ":"), sort_keys=True).encode("ascii")
        with pytest.raises(ScenarioError):
            parse_canonical_scenario_manifest(payload)

    def test_oversized_manifest_is_rejected(self) -> None:
        with pytest.raises(ScenarioError):
            parse_canonical_scenario_manifest(b'{"format":"' + b"x" * (300 * 1024) + b'"}')

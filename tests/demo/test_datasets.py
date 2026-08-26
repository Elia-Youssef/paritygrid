"""Determinism, safety, and contract tests for the seeded dataset generator."""

import json
from collections.abc import Mapping

import pytest
from hypothesis import given
from hypothesis import settings as hypothesis_settings
from hypothesis import strategies as st

from paritygrid.demo.datasets import (
    DATASET_GENERATOR_VERSION,
    DatasetError,
    DatasetProfile,
    RowRole,
    ScenarioSeed,
    ScenarioVersion,
    SyntheticDataset,
    WireRow,
    generate_dataset,
    parse_wire_row,
)
from paritygrid.domain.models import ConnectorId

CONNECTOR = ConnectorId.parse("con_test-src-1")
_SEED = ScenarioSeed(20260825)
_VERSION = ScenarioVersion(1)


def _dataset(profile: DatasetProfile | None = None, seed: int = 20260825) -> SyntheticDataset:
    return generate_dataset(ScenarioSeed(seed), _VERSION, profile)


def test_identical_seed_and_version_reproduce_identical_manifests() -> None:
    first = _dataset()
    second = _dataset()
    assert first.manifest.canonical_bytes() == second.manifest.canonical_bytes()
    assert [row.payload_bytes() for row in first.rows] == [
        row.payload_bytes() for row in second.rows
    ]


def test_manifest_bytes_are_canonical_sorted_compact_ascii() -> None:
    payload = _dataset().manifest.canonical_bytes()
    text = payload.decode("ascii")
    document = json.loads(text)
    assert json.dumps(document, ensure_ascii=True, separators=(",", ":"), sort_keys=True) == text


def test_changed_seed_changes_identity_and_content_deliberately() -> None:
    baseline = _dataset()
    changed = _dataset(seed=20260826)
    assert baseline.manifest.dataset_id != changed.manifest.dataset_id
    assert baseline.manifest.canonical_bytes() != changed.manifest.canonical_bytes()
    assert baseline.manifest.rows_sha256 != changed.manifest.rows_sha256


def test_changed_scenario_version_changes_identity_deliberately() -> None:
    baseline = generate_dataset(ScenarioSeed(1), ScenarioVersion(1))
    changed = generate_dataset(ScenarioSeed(1), ScenarioVersion(2))
    assert baseline.manifest.dataset_id != changed.manifest.dataset_id
    assert baseline.manifest.rows_sha256 != changed.manifest.rows_sha256


def test_changed_profile_changes_identity() -> None:
    baseline = generate_dataset(ScenarioSeed(1), _VERSION, DatasetProfile(record_count=30))
    changed = generate_dataset(ScenarioSeed(1), _VERSION, DatasetProfile(record_count=31))
    assert baseline.manifest.dataset_id != changed.manifest.dataset_id


def test_manifest_reports_exact_counts_matching_profile() -> None:
    profile = DatasetProfile(
        record_count=40, malformed_count=6, boundary_count=5, duplicate_count=7
    )
    dataset = generate_dataset(ScenarioSeed(5), _VERSION, profile)
    assert dict(dataset.manifest.counts) == {
        "boundary": 5,
        "duplicate": 7,
        "malformed": 6,
        "total": 40,
        "valid": 22,
    }
    assert len(dataset.rows) == 40
    assert len(dataset.rows_with_role(RowRole.DUPLICATE)) == 7
    assert len(dataset.rows_with_role(RowRole.MALFORMED)) == 6
    assert len(dataset.rows_with_role(RowRole.BOUNDARY)) == 5


def test_manifest_records_generator_version_seed_and_version() -> None:
    manifest = _dataset().manifest
    assert manifest.generator_version == DATASET_GENERATOR_VERSION
    assert manifest.seed == _SEED
    assert manifest.scenario_version == _VERSION
    document = json.loads(manifest.canonical_bytes().decode("ascii"))
    assert document["seed"] == _SEED.value
    assert document["scenario_version"] == _VERSION.value


def test_every_valid_duplicate_and_boundary_row_parses_as_domain_record() -> None:
    dataset = _dataset()
    parsed = [
        dataset.to_connector_record(row, CONNECTOR)
        for row in dataset.rows
        if row.role is not RowRole.MALFORMED
    ]
    assert len(parsed) == len(dataset.rows) - len(dataset.rows_with_role(RowRole.MALFORMED))


def test_malformed_rows_never_parse_as_domain_records() -> None:
    dataset = _dataset(DatasetProfile(record_count=32, malformed_count=8))
    malformed = dataset.rows_with_role(RowRole.MALFORMED)
    assert len(malformed) == 8
    for row in malformed:
        with pytest.raises((ValueError, TypeError, DatasetError)):
            parse_wire_row(row.payload, connector_id=CONNECTOR)


def test_malformed_variants_cover_distinct_contract_violations() -> None:
    dataset = _dataset(DatasetProfile(record_count=32, malformed_count=8))
    violations: set[str] = set()
    for row in dataset.rows_with_role(RowRole.MALFORMED):
        payload = row.payload
        if "name" not in payload:
            violations.add("missing-name")
        sku = payload.get("sku")
        if isinstance(sku, str) and sku and (not sku.isupper() or len(sku) > 64):
            violations.add("bad-sku")
        if payload.get("quantity") == -5:
            violations.add("negative-quantity")
        unit_price = payload.get("unit_price")
        if isinstance(unit_price, dict) and unit_price.get("amount") == "12.3456789":
            violations.add("precision")
        name = payload.get("name")
        if isinstance(name, str) and "  " in name:
            violations.add("spacing")
        if payload.get("updated_at") == "2024-13-40T99:99:99.999999Z":
            violations.add("timestamp")
        attributes = payload.get("attributes")
        if isinstance(attributes, dict) and "Color" in attributes:
            violations.add("attribute-key")
    assert violations == {
        "missing-name",
        "bad-sku",
        "negative-quantity",
        "precision",
        "spacing",
        "timestamp",
        "attribute-key",
    }


def test_duplicate_rows_repeat_valid_content_under_distinct_source_keys() -> None:
    dataset = _dataset()
    duplicates = dataset.rows_with_role(RowRole.DUPLICATE)
    assert duplicates
    by_key = {row.payload.get("source_record_key"): row for row in dataset.rows}
    for duplicate in duplicates:
        source_key = str(duplicate.payload["source_record_key"]).removeprefix("dup-")
        source = by_key.get(f"rec-{source_key}")
        assert source is not None
        assert _without_source_key(duplicate.payload) == _without_source_key(source.payload)


def test_generated_names_include_unicode_scripts() -> None:
    dataset = _dataset(DatasetProfile(record_count=90, malformed_count=2, boundary_count=2))
    names = [str(row.payload["name"]) for row in dataset.rows if "name" in row.payload]
    non_ascii = [name for name in names if not name.isascii()]
    assert len(non_ascii) >= 10
    scripts: set[str] = set()
    for name in non_ascii:
        for character in name:
            if not character.isascii():
                scripts.add(character)
    assert len(scripts) >= 4


def test_boundary_rows_pin_documented_domain_limits() -> None:
    dataset = _dataset(DatasetProfile(record_count=60, malformed_count=2, boundary_count=9))
    boundary_rows = dataset.rows_with_role(RowRole.BOUNDARY)
    assert boundary_rows
    observed: set[str] = set()
    for row in boundary_rows:
        payload = row.payload
        if payload["quantity"] == 2_147_483_647:
            observed.add("max-quantity")
        if len(str(payload["name"])) == 160:
            observed.add("max-name")
        if len(str(payload["sku"])) == 64:
            observed.add("max-sku")
        if len(payload["attributes"]) == 32:  # type: ignore[arg-type]
            observed.add("max-attributes")
        unit_price = payload["unit_price"]
        assert isinstance(unit_price, dict)
        if unit_price["amount"] == "9999999999999.99":
            observed.add("max-money")
    assert observed >= {"max-quantity", "max-name", "max-sku", "max-attributes"}


def test_dataset_payloads_contain_no_secret_or_credential_markers() -> None:
    dataset = _dataset(DatasetProfile(record_count=120, malformed_count=3, boundary_count=3))
    blob = (
        b"\n".join(row.payload_bytes() for row in dataset.rows) + dataset.manifest.canonical_bytes()
    )
    lowered = blob.decode("ascii").lower()
    for marker in (
        "password",
        "secret",
        "token",
        "api_key",
        "apikey",
        "private_key",
        "authorization",
        "@example.com",
        "localhost:",
        "aws_",
        "begin certificate",
    ):
        assert marker not in lowered


@pytest.mark.parametrize(
    ("seed_value", "reason"),
    [(0, "below one"), (-1, "negative"), (2**63, "above maximum"), ("42", "text")],
)
def test_invalid_seeds_are_rejected(seed_value: object, reason: str) -> None:
    with pytest.raises(DatasetError):
        ScenarioSeed(seed_value)  # type: ignore[arg-type]


@pytest.mark.parametrize(
    ("version_value", "reason"),
    [(0, "below one"), (-3, "negative"), (2**31, "above maximum"), (1.5, "float")],
)
def test_invalid_scenario_versions_are_rejected(version_value: object, reason: str) -> None:
    with pytest.raises(DatasetError):
        ScenarioVersion(version_value)  # type: ignore[arg-type]


@pytest.mark.parametrize(
    "profile_kwargs",
    [
        {"record_count": -1},
        {"record_count": 5_001},
        {"record_count": 10, "malformed_count": -1},
        {"record_count": 10, "duplicate_count": 11},
        {"record_count": 10, "malformed_count": 6, "boundary_count": 6},
        {"record_count": 10, "malformed_count": 4, "duplicate_count": 7},
        {"record_count": 3, "malformed_count": True},
    ],
)
def test_invalid_profiles_are_rejected(profile_kwargs: dict[str, int]) -> None:
    with pytest.raises(DatasetError):
        DatasetProfile(**profile_kwargs)
    with pytest.raises(DatasetError):
        generate_dataset(_SEED, _VERSION, DatasetProfile(**profile_kwargs))


def test_empty_dataset_is_valid_and_reproducible() -> None:
    profile = DatasetProfile(record_count=0, malformed_count=0, boundary_count=0, duplicate_count=0)
    first = generate_dataset(_SEED, _VERSION, profile)
    second = generate_dataset(_SEED, _VERSION, profile)
    assert first.rows == ()
    assert first.manifest.canonical_bytes() == second.manifest.canonical_bytes()


def test_wire_row_payload_bytes_are_ascii_and_sorted() -> None:
    row: WireRow = _dataset().rows[0]
    text = row.payload_bytes().decode("ascii")
    document = json.loads(text)
    assert json.dumps(document, ensure_ascii=True, separators=(",", ":"), sort_keys=True) == text


@hypothesis_settings(max_examples=50, deadline=None)
@given(
    seed=st.integers(min_value=1, max_value=2**40),
    version=st.integers(min_value=1, max_value=1_000),
    valid=st.integers(min_value=0, max_value=40),
    malformed=st.integers(min_value=0, max_value=6),
    boundary=st.integers(min_value=0, max_value=6),
    duplicates=st.integers(min_value=0, max_value=6),
)
def test_generation_is_a_pure_function_of_seed_version_and_profile(
    seed: int, version: int, valid: int, malformed: int, boundary: int, duplicates: int
) -> None:
    base = valid + malformed + boundary
    record_count = base + duplicates
    assume_ok = base >= malformed + boundary and duplicates <= base - malformed
    if not assume_ok:
        with pytest.raises(DatasetError):
            DatasetProfile(
                record_count=record_count,
                malformed_count=malformed,
                boundary_count=boundary,
                duplicate_count=duplicates,
            )
        return
    profile = DatasetProfile(
        record_count=record_count,
        malformed_count=malformed,
        boundary_count=boundary,
        duplicate_count=duplicates,
    )
    first = generate_dataset(ScenarioSeed(seed), ScenarioVersion(version), profile)
    second = generate_dataset(ScenarioSeed(seed), ScenarioVersion(version), profile)
    assert first.manifest.canonical_bytes() == second.manifest.canonical_bytes()
    assert len(first.rows) == record_count
    assert len(first.rows_with_role(RowRole.MALFORMED)) == malformed
    assert len(first.rows_with_role(RowRole.BOUNDARY)) == boundary
    assert len(first.rows_with_role(RowRole.DUPLICATE)) == duplicates
    for row in first.rows:
        if row.role is not RowRole.MALFORMED:
            first.to_connector_record(row, CONNECTOR)


@hypothesis_settings(max_examples=50, deadline=None)
@given(
    seed_a=st.integers(min_value=1, max_value=2**40),
    seed_b=st.integers(min_value=1, max_value=2**40),
)
def test_distinct_identity_inputs_yield_distinct_dataset_ids(seed_a: int, seed_b: int) -> None:
    first = generate_dataset(ScenarioSeed(seed_a), ScenarioVersion(1))
    if seed_a == seed_b:
        assert (
            first.manifest.dataset_id
            == generate_dataset(ScenarioSeed(seed_b), ScenarioVersion(1)).manifest.dataset_id
        )
    else:
        assert (
            first.manifest.dataset_id
            != generate_dataset(ScenarioSeed(seed_b), ScenarioVersion(1)).manifest.dataset_id
        )


def _without_source_key(payload: Mapping[str, object]) -> dict[str, object]:
    return {key: value for key, value in payload.items() if key != "source_record_key"}

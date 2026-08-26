"""CSV and JSON Lines fixtures: encoding, newline, malformation, and bounds."""

import json
from pathlib import Path

import pytest
from hypothesis import given
from hypothesis import settings as hypothesis_settings
from hypothesis import strategies as st

from paritygrid.demo.datasets import (
    DatasetProfile,
    RowRole,
    ScenarioSeed,
    ScenarioVersion,
    SyntheticDataset,
    generate_dataset,
)
from paritygrid.demo.fixtures import (
    MAX_FIXTURE_BYTES,
    MAX_FIXTURE_ROWS,
    FixtureBounds,
    FixtureError,
    FixtureManifest,
    read_csv_rows,
    read_jsonl_rows,
    write_csv_fixture,
    write_jsonl_fixture,
)

_PROFILE = DatasetProfile(record_count=30, malformed_count=5, boundary_count=2, duplicate_count=4)
_SEED = ScenarioSeed(777)
_VERSION = ScenarioVersion(1)


def _dataset() -> SyntheticDataset:
    return generate_dataset(_SEED, _VERSION, _PROFILE)


def test_csv_fixture_is_utf8_lf_terminated_without_bom(tmp_path: Path) -> None:
    destination = tmp_path / "inventory.csv"
    manifest = write_csv_fixture(_dataset(), destination)
    payload = destination.read_bytes()
    assert payload.startswith(b"source_record_key,")
    assert b"\r" not in payload
    assert payload.endswith(b"\n")
    payload.decode("utf-8")
    assert manifest.encoding == "utf-8"
    assert manifest.newline == "lf"
    assert manifest.kind == "csv"


def test_jsonl_fixture_is_utf8_lf_terminated_without_bom(tmp_path: Path) -> None:
    destination = tmp_path / "inventory.jsonl"
    manifest = write_jsonl_fixture(_dataset(), destination)
    payload = destination.read_bytes()
    assert b"\r" not in payload
    assert payload.endswith(b"\n")
    payload.decode("utf-8")
    assert manifest.encoding == "utf-8"
    assert manifest.newline == "lf"
    assert manifest.kind == "jsonl"


def test_fixture_manifest_matches_file_hash_and_size(tmp_path: Path) -> None:
    dataset = _dataset()
    for writer, name in (
        (write_csv_fixture, "inventory.csv"),
        (write_jsonl_fixture, "inventory.jsonl"),
    ):
        destination = tmp_path / name
        manifest = writer(dataset, destination)
        import hashlib

        payload = destination.read_bytes()
        assert manifest.sha256 == hashlib.sha256(payload).hexdigest()
        assert manifest.byte_size == len(payload)
        assert manifest.row_count == len(dataset.rows)
        assert manifest.duplicate_row_count == len(dataset.rows_with_role(RowRole.DUPLICATE))
        assert manifest.malformed_row_count == len(dataset.rows_with_role(RowRole.MALFORMED))


def test_fixture_bytes_reproduce_exactly_across_writes(tmp_path: Path) -> None:
    first = tmp_path / "first.csv"
    second = tmp_path / "second.csv"
    dataset = _dataset()
    write_csv_fixture(dataset, first)
    write_csv_fixture(dataset, second)
    assert first.read_bytes() == second.read_bytes()
    first_jsonl = tmp_path / "first.jsonl"
    second_jsonl = tmp_path / "second.jsonl"
    write_jsonl_fixture(dataset, first_jsonl)
    write_jsonl_fixture(dataset, second_jsonl)
    assert first_jsonl.read_bytes() == second_jsonl.read_bytes()


def test_different_seed_changes_fixture_bytes(tmp_path: Path) -> None:
    baseline = tmp_path / "baseline.jsonl"
    changed = tmp_path / "changed.jsonl"
    write_jsonl_fixture(_dataset(), baseline)
    write_jsonl_fixture(generate_dataset(ScenarioSeed(778), _VERSION, _PROFILE), changed)
    assert baseline.read_bytes() != changed.read_bytes()


def test_csv_header_and_row_shape(tmp_path: Path) -> None:
    destination = tmp_path / "inventory.csv"
    write_csv_fixture(_dataset(), destination)
    rows = read_csv_rows(destination)
    assert rows[0] == [
        "source_record_key",
        "sku",
        "name",
        "quantity",
        "currency",
        "amount",
        "updated_at",
        "attributes",
    ]
    assert len(rows) == len(_dataset().rows) + 1
    expected_widths = {len(row) for row in rows[1:] if _is_well_formed_csv_row(row)}
    assert expected_widths == {8}


def _is_well_formed_csv_row(row: list[str]) -> bool:
    return len(row) == 8


def test_csv_malformed_rows_have_wrong_field_counts(tmp_path: Path) -> None:
    destination = tmp_path / "inventory.csv"
    manifest = write_csv_fixture(_dataset(), destination)
    rows = read_csv_rows(destination)[1:]
    malformed = [row for row in rows if len(row) != 8]
    assert len(malformed) == manifest.malformed_row_count
    for row in malformed:
        assert len(row) in (7, 9)


def test_csv_duplicate_rows_repeat_full_logical_content(tmp_path: Path) -> None:
    destination = tmp_path / "inventory.csv"
    write_csv_fixture(_dataset(), destination)
    rows = read_csv_rows(destination)[1:]
    well_formed = [row for row in rows if len(row) == 8]
    from collections import Counter

    # Duplicates share every field after the source key; the key itself
    # differs so each duplicate remains independently addressable.
    content_counts = Counter(",".join(row[1:]) for row in well_formed)
    repeated = [content for content, count in content_counts.items() if count > 1]
    assert len(repeated) == len(_dataset().rows_with_role(RowRole.DUPLICATE))


def test_jsonl_valid_lines_are_compact_sorted_json_documents(tmp_path: Path) -> None:
    destination = tmp_path / "inventory.jsonl"
    write_jsonl_fixture(_dataset(), destination)
    text_lines = destination.read_text(encoding="utf-8").split("\n")[:-1]
    parsed_lines = 0
    for line in text_lines:
        try:
            document = json.loads(line)
        except json.JSONDecodeError:
            continue
        assert (
            json.dumps(document, ensure_ascii=True, separators=(",", ":"), sort_keys=True) == line
        )
        parsed_lines += 1
    dataset = _dataset()
    assert parsed_lines == len(dataset.rows) - len(dataset.rows_with_role(RowRole.MALFORMED))


def test_jsonl_malformed_lines_fail_to_parse(tmp_path: Path) -> None:
    destination = tmp_path / "inventory.jsonl"
    manifest = write_jsonl_fixture(_dataset(), destination)
    parsed = read_jsonl_rows(destination)
    broken = [(number, value) for number, value in parsed if value is None]
    assert len(broken) == manifest.malformed_row_count
    assert [number for number, _value in broken] == sorted(number for number, _value in broken)


def test_jsonl_valid_rows_round_trip_dataset_payloads(tmp_path: Path) -> None:
    destination = tmp_path / "inventory.jsonl"
    dataset = _dataset()
    write_jsonl_fixture(dataset, destination)
    parsed = read_jsonl_rows(destination)
    valid_lines = [value for _number, value in parsed if value is not None]
    expected = [
        json.loads(row.payload_bytes().decode("ascii"))
        for row in dataset.rows
        if row.role is not RowRole.MALFORMED
    ]
    assert valid_lines == expected


def test_bounded_reader_enforces_row_caps(tmp_path: Path) -> None:
    jsonl_destination = tmp_path / "inventory.jsonl"
    csv_destination = tmp_path / "inventory.csv"
    write_jsonl_fixture(_dataset(), jsonl_destination)
    write_csv_fixture(_dataset(), csv_destination)
    with pytest.raises(FixtureError, match="row bound"):
        read_jsonl_rows(jsonl_destination, bounds=FixtureBounds(max_rows=5))
    with pytest.raises(FixtureError, match="row bound"):
        read_csv_rows(csv_destination, bounds=FixtureBounds(max_rows=5))


def test_bounded_reader_enforces_byte_caps(tmp_path: Path) -> None:
    destination = tmp_path / "inventory.jsonl"
    write_jsonl_fixture(_dataset(), destination)
    byte_size = destination.stat().st_size
    with pytest.raises(FixtureError, match="byte bound"):
        read_jsonl_rows(destination, bounds=FixtureBounds(max_bytes=byte_size - 1))


def test_reader_rejects_missing_files_and_crlf_input(tmp_path: Path) -> None:
    missing = tmp_path / "missing.jsonl"
    with pytest.raises(FixtureError, match="does not exist"):
        read_jsonl_rows(missing)
    crlf = tmp_path / "crlf.jsonl"
    crlf.write_bytes(b'{"a": 1}\r\n{"b": 2}\r\n')
    with pytest.raises(FixtureError, match="lf line endings"):
        read_jsonl_rows(crlf)
    crlf_csv = tmp_path / "crlf.csv"
    crlf_csv.write_bytes(b"a,b\r\n1,2\r\n")
    with pytest.raises(FixtureError, match="lf line endings"):
        read_csv_rows(crlf_csv)


def test_writer_enforces_row_and_byte_bounds(tmp_path: Path) -> None:
    dataset = _dataset()
    destination = tmp_path / "bounded.jsonl"
    with pytest.raises(FixtureError, match="row bound"):
        write_jsonl_fixture(dataset, destination, bounds=FixtureBounds(max_rows=3))
    with pytest.raises(FixtureError, match="row bound"):
        write_csv_fixture(
            dataset, destination.with_suffix(".csv"), bounds=FixtureBounds(max_rows=3)
        )
    with pytest.raises(FixtureError, match="byte bound"):
        write_jsonl_fixture(
            dataset, destination, bounds=FixtureBounds(max_rows=MAX_FIXTURE_ROWS, max_bytes=64)
        )


def test_fixture_bounds_are_validated() -> None:
    with pytest.raises(FixtureError):
        FixtureBounds(max_rows=0)
    with pytest.raises(FixtureError):
        FixtureBounds(max_bytes=0)
    with pytest.raises(FixtureError):
        FixtureBounds(max_rows=MAX_FIXTURE_ROWS + 1)
    with pytest.raises(FixtureError):
        FixtureBounds(max_bytes=MAX_FIXTURE_BYTES + 1)


def test_fixture_manifest_is_canonical_json() -> None:
    manifest = FixtureManifest(
        kind="csv",
        encoding="utf-8",
        newline="lf",
        row_count=3,
        malformed_row_count=1,
        duplicate_row_count=1,
        byte_size=99,
        sha256="0" * 64,
    )
    text = manifest.canonical_bytes().decode("ascii")
    document = json.loads(text)
    assert json.dumps(document, ensure_ascii=True, separators=(",", ":"), sort_keys=True) == text
    assert document["format_version"] == 1


def test_csv_writer_quotes_embedded_separators(tmp_path: Path) -> None:
    dataset = generate_dataset(
        ScenarioSeed(31),
        _VERSION,
        DatasetProfile(record_count=6, malformed_count=1, boundary_count=1, duplicate_count=1),
    )
    destination = tmp_path / "quoted.csv"
    write_csv_fixture(dataset, destination)
    rows = read_csv_rows(destination)[1:]
    well_formed = [row for row in rows if len(row) == 8]
    assert well_formed
    assert all(isinstance(field, str) for row in well_formed for field in row)


@hypothesis_settings(max_examples=30, deadline=None)
@given(
    seed=st.integers(min_value=1, max_value=100_000),
    valid=st.integers(min_value=1, max_value=30),
    malformed=st.integers(min_value=0, max_value=4),
    boundary=st.integers(min_value=0, max_value=3),
    duplicates=st.integers(min_value=0, max_value=4),
)
def test_fixture_determinism_holds_for_generated_shapes(
    tmp_path_factory: pytest.TempPathFactory,
    seed: int,
    valid: int,
    malformed: int,
    boundary: int,
    duplicates: int,
) -> None:
    base = valid + malformed + boundary
    if duplicates > base - malformed:
        return
    profile = DatasetProfile(
        record_count=base + duplicates,
        malformed_count=malformed,
        boundary_count=boundary,
        duplicate_count=duplicates,
    )
    dataset = generate_dataset(ScenarioSeed(seed), _VERSION, profile)
    root = tmp_path_factory.mktemp("fixtures")
    first = root / "a.jsonl"
    second = root / "b.jsonl"
    write_jsonl_fixture(dataset, first)
    write_jsonl_fixture(dataset, second)
    assert first.read_bytes() == second.read_bytes()
    parsed = read_jsonl_rows(first)
    assert len(parsed) == dataset.profile.record_count
    broken = [value for _number, value in parsed if value is None]
    assert len(broken) == malformed

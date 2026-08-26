"""Canonical-key matching, collision, and duplicate-detection tests."""

import pytest
from hypothesis import given
from hypothesis import strategies as st

from paritygrid.domain.reconciliation import (
    CanonicalKeyCollision,
    CanonicalKeyMatch,
    MatchingError,
    NormalizedRecord,
    RecordSide,
    SourceObservation,
    detect_canonical_key_collisions,
    detect_duplicate_record_groups,
    match_by_canonical_key,
    normalize_source_observations,
)
from tests.reconciliation.conftest import SOURCE_CONNECTOR, source_observation, wire_payload


def _normalized(*payloads: dict[str, object]) -> tuple[NormalizedRecord, ...]:
    return normalize_source_observations(
        [source_observation(index, payload) for index, payload in enumerate(payloads)]
    ).records


def test_matching_groups_both_sides_by_canonical_key_in_sku_order() -> None:
    source = _normalized(
        wire_payload(sku="GRID-B", source_record_key="s1"),
        wire_payload(sku="GRID-A", source_record_key="s2"),
    )
    target = _normalized(wire_payload(sku="GRID-A", source_record_key="t1"))
    matches = match_by_canonical_key(source, target)
    assert [match.sku for match in matches] == ["GRID-A", "GRID-B"]
    assert len(matches[0].source_records) == 1
    assert len(matches[0].target_records) == 1
    assert matches[1].target_records == ()


def test_matching_never_discards_colliding_records() -> None:
    payloads = tuple(
        wire_payload(sku="GRID-A", source_record_key=f"s{index}", quantity=index)
        for index in range(3)
    )
    source = _normalized(*payloads)
    matches = match_by_canonical_key(source, ())
    assert len(matches) == 1
    assert len(matches[0].source_records) == 3
    assert [record.record.quantity for record in matches[0].source_records] == [0, 1, 2]


def test_matching_is_independent_of_input_order() -> None:
    from paritygrid.domain.reconciliation import SourceObservation

    payloads = [
        wire_payload(sku="GRID-A", source_record_key="s1"),
        wire_payload(sku="GRID-B", source_record_key="s2"),
        wire_payload(sku="GRID-A", source_record_key="s3"),
    ]
    ordered = [
        SourceObservation(position=index, connector_id=SOURCE_CONNECTOR, payload=payload)
        for index, payload in enumerate(payloads)
    ]
    source_a = normalize_source_observations(ordered).records
    source_b = normalize_source_observations(list(reversed(ordered))).records
    target = _normalized(wire_payload(sku="GRID-A", source_record_key="t1"))
    assert source_a == source_b
    assert match_by_canonical_key(source_a, target) == match_by_canonical_key(source_b, target)


def test_unicode_keys_group_by_canonical_text() -> None:
    source = _normalized(
        wire_payload(sku="GRID-A", source_record_key="s1", name="Cafe\u0301 valve"),
        wire_payload(sku="GRID-A", source_record_key="s2", name="Caf\u00e9 valve"),
    )
    matches = match_by_canonical_key(source, ())
    assert len(matches[0].source_records) == 2


def test_collision_detection_reports_each_duplicated_side() -> None:
    source = _normalized(
        wire_payload(sku="GRID-A", source_record_key="s2"),
        wire_payload(sku="GRID-A", source_record_key="s1"),
    )
    target = _normalized(
        wire_payload(sku="GRID-A", source_record_key="t1"),
        wire_payload(sku="GRID-A", source_record_key="t2"),
        wire_payload(sku="GRID-A", source_record_key="t3"),
    )
    collisions = detect_canonical_key_collisions(match_by_canonical_key(source, target))
    assert [
        (collision.side, collision.sku, collision.record_count) for collision in collisions
    ] == [
        (RecordSide.SOURCE, "GRID-A", 2),
        (RecordSide.TARGET, "GRID-A", 3),
    ]
    assert collisions[0].member_keys == ("s1", "s2")
    assert collisions[1].member_keys == ("t1", "t2", "t3")


def test_collision_detection_preserves_repeated_record_keys() -> None:
    source = _normalized(
        wire_payload(sku="GRID-A", source_record_key="shared", quantity=1),
        wire_payload(sku="GRID-A", source_record_key="shared", quantity=2),
    )
    collisions = detect_canonical_key_collisions(match_by_canonical_key(source, ()))
    assert collisions == (
        CanonicalKeyCollision(
            side=RecordSide.SOURCE,
            sku="GRID-A",
            record_count=2,
            member_keys=("shared", "shared"),
        ),
    )


def test_duplicate_detection_distinguishes_identical_and_divergent_content() -> None:
    source = _normalized(
        wire_payload(sku="GRID-A", source_record_key="s1", quantity=5),
        wire_payload(sku="GRID-A", source_record_key="s2", quantity=5),
        wire_payload(sku="GRID-B", source_record_key="s3", quantity=1),
        wire_payload(sku="GRID-B", source_record_key="s4", quantity=2),
    )
    groups = detect_duplicate_record_groups(match_by_canonical_key(source, ()))
    assert [(group.sku, group.identical_members, group.distinct_contents) for group in groups] == [
        ("GRID-A", True, 1),
        ("GRID-B", False, 2),
    ]
    assert all(group.side is RecordSide.SOURCE for group in groups)


def test_collision_contract_rejects_inconsistent_evidence() -> None:
    with pytest.raises(MatchingError, match="at least two records"):
        CanonicalKeyCollision(RecordSide.SOURCE, "GRID-A", 1, ("s1",))
    with pytest.raises(MatchingError, match="match the record count"):
        CanonicalKeyCollision(RecordSide.SOURCE, "GRID-A", 2, ("s1",))
    with pytest.raises(MatchingError, match="must be sorted"):
        CanonicalKeyCollision(RecordSide.SOURCE, "GRID-A", 2, ("s2", "s1"))
    with pytest.raises(MatchingError, match="nonempty text"):
        CanonicalKeyCollision(RecordSide.SOURCE, "GRID-A", 2, ("", "s1"))
    count = CanonicalKeyCollision.MAX_MEMBER_KEYS + 1
    with pytest.raises(MatchingError, match="member limit"):
        CanonicalKeyCollision(RecordSide.SOURCE, "GRID-A", count, ("s",) * count)


def test_match_contract_rejects_mixed_skus_and_unordered_members() -> None:
    mixed = _normalized(
        wire_payload(sku="GRID-A", source_record_key="s1"),
        wire_payload(sku="GRID-B", source_record_key="s2"),
    )
    with pytest.raises(MatchingError, match="share one SKU"):
        CanonicalKeyMatch(sku="GRID-A", source_records=mixed)
    with pytest.raises(TypeError, match="NormalizedRecord"):
        match_by_canonical_key(["not-a-record"], ())  # pyright: ignore[reportArgumentType]


def test_per_side_duplicate_limit_fails_loudly() -> None:
    payload = wire_payload(sku="GRID-A", source_record_key="s1")
    from paritygrid.domain.reconciliation.outcomes import ReconciliationOutcome

    limit = ReconciliationOutcome.MAX_RECORDS_PER_SIDE
    observations = [
        SourceObservation(
            position=index,
            connector_id=SOURCE_CONNECTOR,
            payload=dict(payload, source_record_key=f"s{index}"),
        )
        for index in range(limit + 1)
    ]
    records = normalize_source_observations(observations).records
    with pytest.raises(MatchingError, match="per-side duplicate record limit"):
        match_by_canonical_key(records, ())


@given(
    sku_count=st.integers(min_value=1, max_value=8),
    rotation=st.integers(min_value=0, max_value=7),
    seed=st.integers(min_value=1, max_value=20_000),
)
def test_matching_groups_survive_arbitrary_reordering(
    sku_count: int, rotation: int, seed: int
) -> None:
    import random

    generator = random.Random(seed)
    payloads = [
        wire_payload(sku=f"GRID-{index:04d}", source_record_key=f"s{index}")
        for index in range(sku_count)
    ]
    generator.shuffle(payloads)
    source = _normalized(*payloads)
    rotated = tuple(source[rotation:] + source[:rotation])
    matches = match_by_canonical_key(rotated, ())
    assert [match.sku for match in matches] == sorted(record.record.sku for record in rotated)
    assert all(
        match.source_records == tuple(sorted(match.source_records, key=lambda r: r.position))
        for match in matches
    )

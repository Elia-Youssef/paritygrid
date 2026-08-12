"""Example-based verification of canonical domain identifiers."""

from collections.abc import Callable
from dataclasses import FrozenInstanceError

import pytest

from paritygrid.domain.models import (
    ArtifactId,
    AttemptNumber,
    ConflictId,
    ConnectorId,
    NodeId,
    PipelineId,
    PipelineVersion,
    RepairPlanId,
    RunId,
    WorkItemId,
)

IdentifierParser = Callable[[str], object]
type IdentifierType = (
    type[PipelineId]
    | type[NodeId]
    | type[ConnectorId]
    | type[RunId]
    | type[WorkItemId]
    | type[ArtifactId]
    | type[ConflictId]
    | type[RepairPlanId]
)

IDENTIFIER_CASES: tuple[tuple[IdentifierType, str], ...] = (
    (PipelineId, "pip_inventory-sync"),
    (NodeId, "nod_normalize-01"),
    (ConnectorId, "con_legacy-source"),
    (RunId, "run_01k2j6ec7kb9v6dw0m1yv0cxz7"),
    (WorkItemId, "wrk_partition-001"),
    (ArtifactId, "art_b8f761ea"),
    (ConflictId, "cnf_sku-0042"),
    (RepairPlanId, "rpl_reconcile-01"),
)


@pytest.mark.parametrize(("identifier_type", "text"), IDENTIFIER_CASES)
def test_identifier_round_trips_canonical_text_and_bytes(
    identifier_type: IdentifierType, text: str
) -> None:
    identifier = identifier_type.parse(text)

    assert str(identifier) == text
    assert bytes(identifier) == text.encode("ascii")
    assert identifier_type.from_bytes(bytes(identifier)) == identifier


@pytest.mark.parametrize(("identifier_type", "text"), IDENTIFIER_CASES)
def test_identifier_is_immutable(identifier_type: IdentifierType, text: str) -> None:
    identifier = identifier_type.parse(text)

    with pytest.raises(FrozenInstanceError):
        identifier.value = "changed"  # type: ignore[attr-defined]


def test_identifier_equality_and_hash_include_the_concrete_type() -> None:
    first = RunId.parse("run_example-001")
    same = RunId.parse("run_example-001")
    different = RunId.parse("run_example-002")

    assert first == same
    assert hash(first) == hash(same)
    assert first != different
    assert first != WorkItemId.parse("wrk_example-001")
    assert len({first, same, different}) == 2


@pytest.mark.parametrize(
    "value",
    [
        "",
        "   ",
        "run_ab",
        f"run_{'a' * 65}",
        "run_Example",
        "run_example_01",
        "run_example--01",
        "run_-example",
        "run_example-",
        "run_example/01",
        "run_example.01",
        "run_example%2f01",
        "run_example 01",
        "run_example\n01",
        "run_example\x7f01",
    ],
)
def test_identifier_rejects_blank_oversized_and_unsafe_forms(value: str) -> None:
    with pytest.raises(ValueError, match="identifier"):
        RunId.parse(value)


@pytest.mark.parametrize(
    "value",
    [
        "run_ex\u0430mple",  # Cyrillic small a
        "run_\uff45xample",  # Fullwidth small e
        "run_café",
        "run_example\u200b01",
        "run_example\u202e01",
    ],
)
def test_identifier_rejects_unicode_confusables_and_controls(value: str) -> None:
    with pytest.raises(ValueError, match="ASCII"):
        RunId.parse(value)


@pytest.mark.parametrize(
    ("parser", "wrong_value"),
    [
        (PipelineId.parse, "run_example"),
        (NodeId.parse, "pip_example"),
        (ConnectorId.parse, "nod_example"),
        (RunId.parse, "con_example"),
        (WorkItemId.parse, "run_example"),
        (ArtifactId.parse, "wrk_example"),
        (ConflictId.parse, "art_example"),
        (RepairPlanId.parse, "cnf_example"),
    ],
)
def test_identifier_rejects_the_wrong_type_prefix(
    parser: IdentifierParser, wrong_value: str
) -> None:
    with pytest.raises(ValueError, match="prefix"):
        parser(wrong_value)


def test_identifier_rejects_non_text_input() -> None:
    with pytest.raises(TypeError, match="text"):
        RunId.parse(42)  # type: ignore[arg-type]


def test_identifier_from_bytes_rejects_wrong_type_and_non_ascii() -> None:
    with pytest.raises(TypeError, match="bytes"):
        RunId.from_bytes(bytearray(b"run_example"))  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="ASCII"):
        RunId.from_bytes("run_café".encode())


@pytest.mark.parametrize("sequence_type", [PipelineVersion, AttemptNumber])
def test_sequence_value_round_trips_and_orders_within_its_type(
    sequence_type: type[PipelineVersion] | type[AttemptNumber],
) -> None:
    first = sequence_type(number=1)
    second = sequence_type.parse("2")

    assert first.number < second.number
    assert str(first) == "1"
    assert int(first) == 1
    assert bytes(first) == b"1"
    assert sequence_type.from_bytes(b"1") == first
    assert hash(sequence_type(number=1)) == hash(first)


def test_sequence_values_are_ordered_within_their_concrete_type() -> None:
    assert PipelineVersion(number=1) < PipelineVersion(number=2)
    assert AttemptNumber(number=1) < AttemptNumber(number=2)


@pytest.mark.parametrize("sequence_type", [PipelineVersion, AttemptNumber])
@pytest.mark.parametrize("number", [0, -1, 2_147_483_648])
def test_sequence_value_rejects_out_of_bounds(
    sequence_type: type[PipelineVersion] | type[AttemptNumber], number: int
) -> None:
    with pytest.raises(ValueError, match="between"):
        sequence_type(number=number)


@pytest.mark.parametrize("sequence_type", [PipelineVersion, AttemptNumber])
@pytest.mark.parametrize("number", [True, 1.0, "1"])
def test_sequence_value_rejects_non_integer_construction(
    sequence_type: type[PipelineVersion] | type[AttemptNumber], number: object
) -> None:
    with pytest.raises(TypeError, match="integer"):
        sequence_type(number=number)  # type: ignore[arg-type]


@pytest.mark.parametrize(
    "value",
    ["", "0", "-1", "+1", "01", "1.0", " 1", "1 ", "\uff11", "2147483648"],
)
def test_sequence_value_rejects_noncanonical_text(value: str) -> None:
    with pytest.raises(ValueError, match=r"canonical|between"):
        PipelineVersion.parse(value)


def test_sequence_parse_rejects_non_text() -> None:
    with pytest.raises(TypeError, match="text"):
        PipelineVersion.parse(1)  # type: ignore[arg-type]


def test_sequence_from_bytes_rejects_wrong_type_and_non_ascii() -> None:
    with pytest.raises(TypeError, match="bytes"):
        AttemptNumber.from_bytes(bytearray(b"1"))  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="ASCII"):
        AttemptNumber.from_bytes("\uff11".encode())

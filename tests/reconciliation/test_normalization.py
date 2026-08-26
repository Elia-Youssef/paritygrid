"""Golden and property tests for versioned source-schema normalization."""

import copy
import unicodedata
from collections.abc import Callable

import pytest
from hypothesis import given
from hypothesis import strategies as st

from paritygrid.demo.datasets import (
    DatasetProfile,
    RowRole,
    ScenarioSeed,
    ScenarioVersion,
    generate_dataset,
)
from paritygrid.domain.reconciliation import (
    NORMALIZATION_RULES_VERSION,
    NormalizationError,
    NormalizedRecord,
    QuarantineCode,
    QuarantinedObservation,
    SourceObservation,
    normalize_observation,
    normalize_source_observations,
)
from paritygrid.domain.reconciliation.differences import ComparisonValueKind
from tests.reconciliation.conftest import (
    SOURCE_CONNECTOR,
    source_observation,
    wire_payload,
)


def _valid(position: int = 0) -> NormalizedRecord:
    result = normalize_observation(source_observation(position, wire_payload()))
    assert isinstance(result, NormalizedRecord)
    return result


def _quarantined(payload: dict[str, object], position: int = 0) -> QuarantinedObservation:
    result = normalize_observation(source_observation(position, payload))
    assert isinstance(result, QuarantinedObservation)
    return result


def test_valid_payload_normalizes_to_canonical_record() -> None:
    record = _valid().record
    assert record.sku == "GRID-0001"
    assert record.connector_id == SOURCE_CONNECTOR
    assert str(record.unit_price) == "USD 12.34"
    paths = {path: value.kind for path, value in _valid().document.values}
    assert paths["name"] is ComparisonValueKind.TEXT
    assert paths["quantity"] is ComparisonValueKind.INTEGER
    assert paths["unit_price/currency"] is ComparisonValueKind.TEXT
    assert paths["unit_price/amount"] is ComparisonValueKind.MONEY_AMOUNT
    assert paths["updated_at"] is ComparisonValueKind.TIMESTAMP
    assert paths["attributes/color"] is ComparisonValueKind.ATTRIBUTE_TEXT


def test_nfc_unicode_input_normalizes_to_canonical_nfc_text() -> None:
    decomposed = "Cafe\u0301 valve"
    payload = wire_payload(name=decomposed)
    result = normalize_observation(source_observation(7, payload))
    assert isinstance(result, NormalizedRecord)
    assert result.record.name == unicodedata.normalize("NFC", decomposed)
    assert result.record.name == "Caf\u00e9 valve"


def _lowercase_sku(payload: dict[str, object]) -> None:
    payload["sku"] = "grid-0001"


def _oversize_sku(payload: dict[str, object]) -> None:
    payload["sku"] = "X" * 65


def _double_spaced_name(payload: dict[str, object]) -> None:
    payload["name"] = "Double  spaced"


def _negative_quantity(payload: dict[str, object]) -> None:
    payload["quantity"] = -5


def _impossible_timestamp(payload: dict[str, object]) -> None:
    payload["updated_at"] = "2024-13-40T99:99:99.999999Z"


def _uppercase_attribute_key(payload: dict[str, object]) -> None:
    payload["attributes"] = {"Color": "upper-case-key"}  # type: ignore[assignment]


_INVALID_MUTATIONS: tuple[tuple[Callable[[dict[str, object]], None], str], ...] = (
    (_lowercase_sku, "sku"),
    (_oversize_sku, "sku"),
    (_double_spaced_name, "name"),
    (_negative_quantity, "quantity"),
    (_impossible_timestamp, "updated_at"),
    (_uppercase_attribute_key, "attributes"),
)


@pytest.mark.parametrize(("mutate", "field"), _INVALID_MUTATIONS)
def test_domain_contract_failures_quarantine_with_field_evidence(
    mutate: Callable[[dict[str, object]], None], field: str
) -> None:
    payload = wire_payload()
    mutate(payload)
    quarantined = _quarantined(payload)
    assert quarantined.code is QuarantineCode.INVALID_VALUE
    assert quarantined.field == field
    assert quarantined.detail


def test_bad_money_amount_and_currency_attribute_precisely() -> None:
    amount_payload = wire_payload()
    amount_payload["unit_price"] = {"amount": "12.3456789", "currency": "USD"}  # type: ignore[assignment]
    quarantined = _quarantined(amount_payload)
    assert quarantined.code is QuarantineCode.INVALID_VALUE
    assert quarantined.field == "unit_price/amount"

    currency_payload = wire_payload()
    currency_payload["unit_price"] = {"amount": "12.34", "currency": "usd"}  # type: ignore[assignment]
    quarantined = _quarantined(currency_payload, position=1)
    assert quarantined.code is QuarantineCode.INVALID_VALUE
    assert quarantined.field == "unit_price/currency"


@pytest.mark.parametrize(
    ("removal", "field"),
    [
        ("name", "name"),
        ("quantity", "quantity"),
        ("updated_at", "updated_at"),
        ("sku", "sku"),
        ("source_record_key", "source_record_key"),
        ("unit_price", "unit_price"),
    ],
)
def test_missing_fields_quarantine_with_missing_field_code(removal: str, field: str) -> None:
    payload = wire_payload()
    del payload[removal]
    quarantined = _quarantined(payload)
    assert quarantined.code is QuarantineCode.MISSING_FIELD
    assert quarantined.field == field


def test_null_and_wrong_type_fields_quarantine_with_precise_codes() -> None:
    null_payload = wire_payload()
    null_payload["quantity"] = None  # type: ignore[assignment]
    quarantined = _quarantined(null_payload)
    assert quarantined.code is QuarantineCode.NULL_FIELD
    assert quarantined.field == "quantity"

    type_payload = wire_payload()
    type_payload["quantity"] = "5"  # type: ignore[assignment]
    quarantined = _quarantined(type_payload, position=1)
    assert quarantined.code is QuarantineCode.WRONG_TYPE
    assert quarantined.field == "quantity"

    nested_payload = wire_payload()
    nested_payload["attributes"] = {"color": 7}  # type: ignore[assignment]
    quarantined = _quarantined(nested_payload, position=2)
    assert quarantined.code is QuarantineCode.WRONG_TYPE
    assert quarantined.field == "attributes/color"

    price_payload = wire_payload()
    price_payload["unit_price"] = "12.34"  # type: ignore[assignment]
    quarantined = _quarantined(price_payload, position=3)
    assert quarantined.code is QuarantineCode.WRONG_TYPE
    assert quarantined.field == "unit_price"


def test_quarantine_preserves_bounded_provenance() -> None:
    payload = wire_payload(source_record_key="rec-000123")
    payload["quantity"] = None  # type: ignore[assignment]
    quarantined = _quarantined(payload)
    assert quarantined.source_record_key == "rec-000123"
    assert quarantined.connector_id == SOURCE_CONNECTOR
    assert quarantined.position == 0


def test_source_malformed_observation_quarantines_with_reason() -> None:
    result = normalize_observation(source_observation(4, None, "connector rejected the row"))
    assert isinstance(result, QuarantinedObservation)
    assert result.code is QuarantineCode.SOURCE_MALFORMED
    assert result.detail == "connector rejected the row"


def test_every_generated_malformed_variant_quarantines() -> None:
    dataset = generate_dataset(
        ScenarioSeed(4101),
        ScenarioVersion(1),
        DatasetProfile(record_count=32, malformed_count=8, boundary_count=2, duplicate_count=3),
    )
    malformed = dataset.rows_with_role(RowRole.MALFORMED)
    assert len(malformed) == 8
    observations = [
        SourceObservation(
            position=row.index,
            connector_id=SOURCE_CONNECTOR,
            payload=dict(row.payload),
        )
        for row in malformed
    ]
    result = normalize_source_observations(observations)
    assert result.records == ()
    assert len(result.quarantined) == 8
    assert {item.code for item in result.quarantined} == {
        QuarantineCode.MISSING_FIELD,
        QuarantineCode.INVALID_VALUE,
    }


def test_generated_dataset_normalizes_with_expected_split() -> None:
    dataset = generate_dataset(ScenarioSeed(4102), ScenarioVersion(1), DatasetProfile())
    observations = [
        SourceObservation(
            position=row.index, connector_id=SOURCE_CONNECTOR, payload=dict(row.payload)
        )
        for row in dataset.rows
    ]
    result = normalize_source_observations(observations)
    counts = dataset.manifest.counts
    assert len(result.records) == counts["total"] - counts["malformed"]
    assert len(result.quarantined) == counts["malformed"]


def test_batch_output_is_ordered_by_position_regardless_of_input_order() -> None:
    observations = [
        source_observation(2, wire_payload(sku="GRID-C", source_record_key="c")),
        source_observation(0, wire_payload(sku="GRID-A", source_record_key="a")),
        source_observation(1, wire_payload(quantity=-1)),
    ]
    result = normalize_source_observations(observations)
    assert [record.position for record in result.records] == [0, 2]
    assert [item.position for item in result.quarantined] == [1]


def test_duplicate_positions_are_rejected() -> None:
    with pytest.raises(NormalizationError, match="positions must be unique"):
        normalize_source_observations(
            [source_observation(0, wire_payload()), source_observation(0, wire_payload())]
        )


def test_observation_contract_rejects_ambiguous_payload_reason_pairs() -> None:
    with pytest.raises(NormalizationError, match="require a malformed reason"):
        SourceObservation(0, SOURCE_CONNECTOR, None)
    with pytest.raises(NormalizationError, match="must not carry a malformed reason"):
        SourceObservation(0, SOURCE_CONNECTOR, wire_payload(), "reason")
    with pytest.raises(NormalizationError, match="nonnegative integer"):
        SourceObservation(-1, SOURCE_CONNECTOR, wire_payload())
    with pytest.raises(TypeError, match="ConnectorId"):
        SourceObservation(0, "con_demo", wire_payload())  # type: ignore[arg-type]


def test_rules_version_is_explicit_and_frozen() -> None:
    result = normalize_source_observations([source_observation(0, wire_payload())])
    assert result.rules_version == NORMALIZATION_RULES_VERSION == 1


@given(
    quantity=st.integers(min_value=0, max_value=2_147_483_647),
    name=st.sampled_from(("Amber Valve", "琥珀色 伺服阀", "Ámbar Válvula")),
    amount_units=st.integers(min_value=0, max_value=999_999),
)
def test_valid_wire_values_always_normalize(quantity: int, name: str, amount_units: int) -> None:
    payload = wire_payload(
        quantity=quantity, name=name, amount=f"{amount_units // 100}.{amount_units % 100:02d}"
    )
    result = normalize_observation(source_observation(0, payload))
    assert isinstance(result, NormalizedRecord)
    assert result.record.quantity == quantity
    assert result.record.name == unicodedata.normalize("NFC", name)


@given(
    rotation=st.integers(min_value=0, max_value=8),
    seed=st.integers(min_value=1, max_value=50_000),
)
def test_generated_batches_normalize_deterministically(rotation: int, seed: int) -> None:
    dataset = generate_dataset(
        ScenarioSeed(seed),
        ScenarioVersion(1),
        DatasetProfile(record_count=24, malformed_count=4, boundary_count=2, duplicate_count=4),
    )
    observations = [
        SourceObservation(
            position=row.index, connector_id=SOURCE_CONNECTOR, payload=copy.deepcopy(row.payload)
        )
        for row in dataset.rows
    ]
    first = normalize_source_observations(observations)
    rotated = observations[rotation:] + observations[:rotation]
    second = normalize_source_observations(rotated)
    assert first.records == second.records
    assert first.quarantined == second.quarantined

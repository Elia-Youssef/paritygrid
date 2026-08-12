"""Example-based verification of exact monetary values."""

from dataclasses import FrozenInstanceError
from decimal import Decimal, localcontext

import pytest

from paritygrid.domain.models import CurrencyCode, Money


class _CurrencyCodeSubclass(CurrencyCode):
    pass


USD = CurrencyCode.parse("USD")


def test_currency_code_round_trips_ascii_and_is_hashable() -> None:
    currency = CurrencyCode.parse("EUR")

    assert str(currency) == "EUR"
    assert bytes(currency) == b"EUR"
    assert CurrencyCode.from_bytes(currency.to_bytes()) == currency
    assert hash(currency) == hash(CurrencyCode("EUR"))


@pytest.mark.parametrize("value", ["", "US", "USDD", "usd", "UsD", "U1D", "€UR", " USD"])
def test_currency_code_rejects_noncanonical_forms(value: str) -> None:
    with pytest.raises(ValueError, match="currency"):
        CurrencyCode.parse(value)


def test_currency_code_rejects_wrong_runtime_and_byte_types() -> None:
    with pytest.raises(TypeError, match="text"):
        CurrencyCode.parse(123)  # type: ignore[arg-type]
    with pytest.raises(TypeError, match="bytes"):
        CurrencyCode.from_bytes(bytearray(b"USD"))  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="ASCII"):
        CurrencyCode.from_bytes("€UR".encode())


@pytest.mark.parametrize(
    ("amount", "exponent", "canonical", "minor_units"),
    [
        (Decimal("0"), 0, "USD 0", 0),
        (Decimal("-0.000"), 2, "USD 0.00", 0),
        (Decimal("12.3"), 2, "USD 12.30", 1230),
        (Decimal("12.3000"), 2, "USD 12.30", 1230),
        (Decimal("-12.34"), 2, "USD -12.34", -1234),
        (Decimal("1E+2"), 2, "USD 100.00", 10000),
        (Decimal("0.000001"), 6, "USD 0.000001", 1),
    ],
)
def test_money_normalizes_exact_decimal_values(
    amount: Decimal, exponent: int, canonical: str, minor_units: int
) -> None:
    money = Money(amount=amount, currency=USD, minor_unit_exponent=exponent)

    assert str(money) == canonical
    assert money.minor_units == minor_units
    assert bytes(money) == canonical.encode("ascii")
    assert Money.parse(canonical) == money
    assert Money.from_bytes(money.to_bytes()) == money


def test_money_accepts_exact_magnitude_boundaries() -> None:
    maximum = Money(amount=Decimal("9999999999999.99"), currency=USD, minor_unit_exponent=2)
    minimum = Money(amount=Decimal("-9999999999999.99"), currency=USD, minor_unit_exponent=2)

    assert maximum.minor_units == Money.MAX_ABSOLUTE_MINOR_UNITS
    assert minimum.minor_units == -Money.MAX_ABSOLUTE_MINOR_UNITS


def test_money_operations_do_not_depend_on_ambient_decimal_precision() -> None:
    with localcontext() as context:
        context.prec = 2
        money = Money(amount=Decimal("9999999999999.99"), currency=USD, minor_unit_exponent=2)
        minor_units = money.minor_units
        with pytest.raises(ValueError, match="magnitude"):
            Money(
                amount=Decimal("10000000000000.00"),
                currency=USD,
                minor_unit_exponent=2,
            )

    assert minor_units == Money.MAX_ABSOLUTE_MINOR_UNITS


@pytest.mark.parametrize("value", [0.1, 1, "1.00"])
def test_money_rejects_non_decimal_construction(value: object) -> None:
    with pytest.raises(TypeError, match="Decimal"):
        Money(amount=value, currency=USD, minor_unit_exponent=2)  # type: ignore[arg-type]


@pytest.mark.parametrize("value", [Decimal("NaN"), Decimal("sNaN"), Decimal("Infinity")])
def test_money_rejects_non_finite_values(value: Decimal) -> None:
    with pytest.raises(ValueError, match="finite"):
        Money(amount=value, currency=USD, minor_unit_exponent=2)


@pytest.mark.parametrize("value", [Decimal("1.001"), Decimal("0.0000001"), Decimal("1E-999")])
def test_money_rejects_precision_beyond_declared_exponent(value: Decimal) -> None:
    with pytest.raises(ValueError, match="precision"):
        Money(amount=value, currency=USD, minor_unit_exponent=2)


@pytest.mark.parametrize("value", [Decimal("10000000000000.00"), Decimal("-10000000000000.00")])
def test_money_rejects_magnitude_overflow(value: Decimal) -> None:
    with pytest.raises(ValueError, match="magnitude"):
        Money(amount=value, currency=USD, minor_unit_exponent=2)


@pytest.mark.parametrize("value", [-1, 7])
def test_money_rejects_out_of_range_minor_unit_exponent(value: int) -> None:
    with pytest.raises(ValueError, match="exponent"):
        Money(amount=Decimal("1"), currency=USD, minor_unit_exponent=value)


@pytest.mark.parametrize("value", [True, 2.0, "2"])
def test_money_rejects_non_integer_minor_unit_exponent(value: object) -> None:
    with pytest.raises(TypeError, match="integer"):
        Money(
            amount=Decimal("1"),
            currency=USD,
            minor_unit_exponent=value,  # type: ignore[arg-type]
        )


def test_money_rejects_non_currency_value() -> None:
    with pytest.raises(TypeError, match="CurrencyCode"):
        Money(
            amount=Decimal("1"),
            currency="USD",  # type: ignore[arg-type]
            minor_unit_exponent=2,
        )


def test_money_rejects_a_currency_subclass() -> None:
    with pytest.raises(TypeError, match="CurrencyCode"):
        Money(Decimal("1.00"), _CurrencyCodeSubclass("USD"), 2)


@pytest.mark.parametrize(
    "value",
    [
        "",
        "USD",
        "USD 01.00",
        "USD +1.00",
        "USD -0.00",
        "usd 1.00",
        "USD 1.",
        "USD 1.0000000",
        "USD 10000000000000.00",
    ],
)
def test_money_parse_rejects_noncanonical_or_out_of_range_forms(value: str) -> None:
    with pytest.raises(ValueError, match=r"money|minor-unit"):
        Money.parse(value)


def test_money_parse_rejects_oversized_text_before_decimal_conversion() -> None:
    with pytest.raises(ValueError, match="supported size"):
        Money.parse(f"USD {'9' * 1_000}")


def test_money_parse_and_bytes_reject_wrong_runtime_types() -> None:
    with pytest.raises(TypeError, match="text"):
        Money.parse(1)  # type: ignore[arg-type]
    with pytest.raises(TypeError, match="bytes"):
        Money.from_bytes(bytearray(b"USD 1.00"))  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="ASCII"):
        Money.from_bytes("€UR 1.00".encode())


def test_money_is_immutable_hashable_and_currency_sensitive() -> None:
    money = Money.parse("USD 1.00")

    assert hash(money) == hash(Money.parse("USD 1.00"))
    assert money != Money.parse("EUR 1.00")
    assert money != Money.parse("USD 1.000")
    with pytest.raises(FrozenInstanceError):
        money.amount = Decimal("2.00")  # type: ignore[misc]

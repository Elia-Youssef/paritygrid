"""Property verification for exact monetary values."""

from decimal import Decimal

import pytest
from hypothesis import given
from hypothesis import strategies as st

from paritygrid.domain.models import CurrencyCode, Money


@given(
    st.integers(
        min_value=-Money.MAX_ABSOLUTE_MINOR_UNITS,
        max_value=Money.MAX_ABSOLUTE_MINOR_UNITS,
    ),
    st.integers(min_value=0, max_value=Money.MAX_MINOR_UNIT_EXPONENT),
)
def test_money_minor_units_and_text_round_trip_are_exact(minor_units: int, exponent: int) -> None:
    amount = Decimal(minor_units).scaleb(-exponent)
    money = Money(
        amount=amount,
        currency=CurrencyCode.parse("USD"),
        minor_unit_exponent=exponent,
    )

    assert money.minor_units == minor_units
    assert Money.parse(str(money)) == money


@given(
    st.integers(min_value=0, max_value=Money.MAX_ABSOLUTE_MINOR_UNITS - 1),
    st.integers(min_value=0, max_value=Money.MAX_MINOR_UNIT_EXPONENT - 1),
    st.integers(min_value=1, max_value=9),
)
def test_money_rejects_any_nonzero_sub_minor_digit(
    minor_units: int, exponent: int, extra_digit: int
) -> None:
    amount = Decimal(minor_units).scaleb(-exponent) + Decimal(extra_digit).scaleb(-(exponent + 1))

    with pytest.raises(ValueError, match="precision"):
        Money(
            amount=amount,
            currency=CurrencyCode.parse("USD"),
            minor_unit_exponent=exponent,
        )


@given(st.text(alphabet="ABCDEFGHIJKLMNOPQRSTUVWXYZ", min_size=3, max_size=3))
def test_uppercase_ascii_currency_codes_round_trip(code: str) -> None:
    currency = CurrencyCode.parse(code)

    assert CurrencyCode.from_bytes(bytes(currency)) == currency

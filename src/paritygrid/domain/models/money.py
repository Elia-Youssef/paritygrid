"""Exact monetary values with explicit minor-unit precision."""

import re
from dataclasses import dataclass
from decimal import Decimal, Inexact, InvalidOperation, localcontext
from typing import ClassVar, Self

_CURRENCY_PATTERN = re.compile(r"[A-Z]{3}", flags=re.ASCII)
_MONEY_PATTERN = re.compile(
    r"(?P<currency>[A-Z]{3}) (?P<amount>-?(?:0|[1-9][0-9]*)(?:\.(?P<fraction>[0-9]+))?)",
    flags=re.ASCII,
)
_MAX_MONEY_TEXT_LENGTH = 21


@dataclass(frozen=True, slots=True, order=True)
class CurrencyCode:
    """An ISO-style three-letter uppercase currency code."""

    value: str

    def __post_init__(self) -> None:
        object.__setattr__(self, "value", _validate_currency(self.value))

    @classmethod
    def parse(cls, value: str) -> Self:
        """Parse a canonical currency code without case conversion."""
        return cls(value=value)

    @classmethod
    def from_bytes(cls, value: bytes) -> Self:
        """Parse a canonical ASCII currency code."""
        return cls.parse(_decode_ascii(value, subject="currency"))

    def to_bytes(self) -> bytes:
        """Return the stable ASCII representation."""
        return self.value.encode("ascii")

    def __bytes__(self) -> bytes:
        return self.to_bytes()

    def __str__(self) -> str:
        return self.value


@dataclass(frozen=True, slots=True)
class Money:
    """A signed Decimal amount with explicit minor-unit exponent."""

    MAX_MINOR_UNIT_EXPONENT: ClassVar[int] = 6
    MAX_ABSOLUTE_MINOR_UNITS: ClassVar[int] = 999_999_999_999_999

    amount: Decimal
    currency: CurrencyCode
    minor_unit_exponent: int

    def __post_init__(self) -> None:
        exponent = _validate_minor_unit_exponent(self.minor_unit_exponent)
        currency = _validate_currency_value(self.currency)
        amount = _canonical_amount(self.amount, exponent=exponent)
        object.__setattr__(self, "minor_unit_exponent", exponent)
        object.__setattr__(self, "currency", currency)
        object.__setattr__(self, "amount", amount)

    @classmethod
    def parse(cls, value: str) -> Self:
        """Parse the canonical `<currency> <amount>` representation."""
        text = _require_text(value, subject="money")
        if len(text) > _MAX_MONEY_TEXT_LENGTH:
            raise ValueError("money representation exceeds the supported size")
        match = _MONEY_PATTERN.fullmatch(text)
        if match is None:
            raise ValueError("money must use canonical `<currency> <amount>` form")
        fraction = match.group("fraction")
        exponent = len(fraction) if fraction is not None else 0
        money = cls(
            amount=Decimal(match.group("amount")),
            currency=CurrencyCode.parse(match.group("currency")),
            minor_unit_exponent=exponent,
        )
        if str(money) != text:
            raise ValueError("money representation is not canonical")
        return money

    @classmethod
    def from_bytes(cls, value: bytes) -> Self:
        """Parse canonical ASCII money bytes."""
        return cls.parse(_decode_ascii(value, subject="money"))

    @property
    def minor_units(self) -> int:
        """Return the exact signed integer used by durable storage."""
        with localcontext() as context:
            context.prec = 32
            scaled = self.amount.scaleb(self.minor_unit_exponent)
        return int(scaled)

    def to_bytes(self) -> bytes:
        """Return the canonical ASCII monetary representation."""
        return str(self).encode("ascii")

    def __bytes__(self) -> bytes:
        return self.to_bytes()

    def __str__(self) -> str:
        return f"{self.currency} {self.amount:.{self.minor_unit_exponent}f}"


def _validate_currency(value: object) -> str:
    if not isinstance(value, str):
        raise TypeError("currency code must be text")
    if _CURRENCY_PATTERN.fullmatch(value) is None:
        raise ValueError("currency code must contain exactly three uppercase ASCII letters")
    return value


def _validate_currency_value(value: object) -> CurrencyCode:
    if type(value) is not CurrencyCode:
        raise TypeError("money currency must be a CurrencyCode")
    return value


def _validate_minor_unit_exponent(value: object) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError("minor-unit exponent must be an integer")
    if not 0 <= value <= Money.MAX_MINOR_UNIT_EXPONENT:
        raise ValueError("minor-unit exponent must be between zero and six")
    return value


def _canonical_amount(value: object, *, exponent: int) -> Decimal:
    if not isinstance(value, Decimal):
        raise TypeError("money amount must be constructed from Decimal")
    if not value.is_finite():
        raise ValueError("money amount must be finite")

    maximum_digits = tuple(int(digit) for digit in str(Money.MAX_ABSOLUTE_MINOR_UNITS))
    maximum = Decimal((0, maximum_digits, -exponent))
    if value.copy_abs() > maximum:
        raise ValueError("money amount exceeds the supported magnitude")

    quantum = Decimal((0, (1,), -exponent))
    try:
        with localcontext() as context:
            context.prec = 32
            context.traps[Inexact] = True
            canonical = value.quantize(quantum)
    except (Inexact, InvalidOperation) as error:
        raise ValueError("money amount exceeds the declared minor-unit precision") from error
    return canonical.copy_abs() if canonical.is_zero() else canonical


def _decode_ascii(value: object, *, subject: str) -> str:
    if not isinstance(value, bytes):
        raise TypeError(f"{subject} encoding must be bytes")
    try:
        return value.decode("ascii")
    except UnicodeDecodeError as error:
        raise ValueError(f"{subject} encoding must contain only ASCII") from error


def _require_text(value: object, *, subject: str) -> str:
    if not isinstance(value, str):
        raise TypeError(f"{subject} representation must be text")
    return value

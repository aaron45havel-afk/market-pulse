"""
Money is an integer number of minor units. There is no other kind.

CLAUDE.md's first non-negotiable, and the reason it is first: this codebase
already stores money as DOUBLE PRECISION for the analysis boards
(database.py:248, :371, :409). That is defensible for a market-cap estimate
derived from a public filing — the float IS the estimate's honest precision.
It is indefensible for a rent ledger, where 0.1 + 0.2 != 0.3 becomes a
tenant's balance being a cent off forever and a reconciliation nobody can
close.

So this module exists to make the wrong thing hard:

  * Every amount is an int of minor units (cents for USD). Never a float,
    never a Decimal, never a string that has been through a float.
  * parse() REFUSES a float outright rather than rounding it. If a float
    reached this function, something upstream is already wrong and rounding
    it here would hide that.
  * Division allocates remainders explicitly — split() gives every cent to
    somebody, and the caller can see who got the odd one. A prorated rent
    that loses a cent per month loses twelve a year, and that is the kind
    of drift that surfaces two years later as an unexplainable balance.

Pure: no database, no clock, no network.
"""
from __future__ import annotations

import re
from decimal import Decimal, InvalidOperation

# ISO 4217 exponent — how many minor units make one major unit. Only the
# currencies this platform actually touches; adding one is a deliberate act,
# not a silent default, because getting the exponent wrong is a 100x error.
MINOR_UNITS = {
    "USD": 2,
}
DEFAULT_CURRENCY = "USD"


class MoneyError(ValueError):
    """Rejected monetary input. The message is safe to show a user."""


def exponent(currency: str = DEFAULT_CURRENCY) -> int:
    cur = (currency or "").strip().upper()
    if cur not in MINOR_UNITS:
        raise MoneyError(
            f"Unknown currency {currency!r}. Add it to MINOR_UNITS with its "
            f"ISO-4217 exponent — guessing 2 would be a 100x error for a "
            f"zero-decimal currency like JPY."
        )
    return MINOR_UNITS[cur]


_CLEAN = re.compile(r"[,\s $]")
_VALID = re.compile(r"^-?\d+(\.\d+)?$")


def parse(value, currency: str = DEFAULT_CURRENCY) -> int:
    """A user-entered amount to integer minor units. Raises on anything odd.

    A FLOAT IS REFUSED, NOT ROUNDED. `parse(19.99)` looks harmless and is
    the exact door this module exists to close: by the time a float arrives
    the value may already be 19.989999999999998, and rounding it here would
    launder an upstream bug into a plausible-looking cent. Callers hand this
    a string or a Decimal.
    """
    exp = exponent(currency)
    if value is None or value == "":
        raise MoneyError("Amount is required.")
    if isinstance(value, bool):
        raise MoneyError("A boolean is not an amount.")
    if isinstance(value, int):
        return value * 10 ** exp
    if isinstance(value, float):
        raise MoneyError(
            "Refusing to parse a float as money. Pass a string or a Decimal "
            "— by the time a float gets here the value may already have "
            "drifted, and rounding it would hide where."
        )
    if isinstance(value, Decimal):
        dec = value
    else:
        s = _CLEAN.sub("", str(value))
        if s.startswith("(") and s.endswith(")"):        # (12.34) = -12.34
            s = "-" + s[1:-1]
        if not _VALID.match(s):
            raise MoneyError(f"{value!r} is not an amount.")
        try:
            dec = Decimal(s)
        except InvalidOperation:
            raise MoneyError(f"{value!r} is not an amount.")

    shifted = dec.scaleb(exp)
    if shifted != shifted.to_integral_value():
        raise MoneyError(
            f"{value} has more precision than {currency} can hold. "
            f"Round it deliberately before it reaches the ledger."
        )
    return int(shifted)


def format_minor(minor: int, currency: str = DEFAULT_CURRENCY,
                 symbol: bool = True) -> str:
    """Integer minor units to a display string. THE PRESENTATION EDGE ONLY.

    Nothing downstream of this may do arithmetic on the result.
    """
    if not isinstance(minor, int) or isinstance(minor, bool):
        raise MoneyError(f"{minor!r} is not integer minor units.")
    exp = exponent(currency)
    sign = "-" if minor < 0 else ""
    whole, frac = divmod(abs(minor), 10 ** exp)
    body = f"{whole:,}" + (f".{frac:0{exp}d}" if exp else "")
    return f"{sign}{'$' if symbol and currency == 'USD' else ''}{body}"


def split(total: int, parts: int) -> list[int]:
    """Divide into `parts`, giving every remaining minor unit to somebody.

    The first `remainder` parts each get one extra unit. The result always
    sums to `total` exactly — a split that quietly drops the odd cent is
    how a ledger stops reconciling.
    """
    if not isinstance(total, int) or isinstance(total, bool):
        raise MoneyError("split() takes integer minor units.")
    if not isinstance(parts, int) or parts < 1:
        raise MoneyError("parts must be a positive integer.")
    base, rem = divmod(abs(total), parts)
    out = [base + (1 if i < rem else 0) for i in range(parts)]
    return [-v for v in out] if total < 0 else out


def allocate(total: int, weights: list[int]) -> list[int]:
    """Apportion by integer weights, largest-remainder, summing exactly.

    Used wherever a payment lands across several charges. The allocation
    ORDER between rent and fees is a jurisdiction rule and is not decided
    here — this only guarantees the arithmetic is lossless once the caller
    has decided the weights.
    """
    if not isinstance(total, int) or isinstance(total, bool):
        raise MoneyError("allocate() takes integer minor units.")
    if not weights or any(w < 0 for w in weights):
        raise MoneyError("weights must be a non-empty list of non-negative ints.")
    tw = sum(weights)
    if tw == 0:
        raise MoneyError("weights sum to zero — nothing to allocate against.")

    sign = -1 if total < 0 else 1
    t = abs(total)
    raw = [(t * w) // tw for w in weights]
    rem = t - sum(raw)
    # Give the leftover units to the largest fractional parts, ties by index
    # so the result is deterministic and a test can pin it.
    fracs = sorted(range(len(weights)),
                   key=lambda i: (-((t * weights[i]) % tw), i))
    for i in range(rem):
        raw[fracs[i]] += 1
    return [sign * v for v in raw]


def pct(minor: int, basis_points: int) -> int:
    """A percentage of an amount, in basis points, rounded half-up.

    Basis points rather than a float percentage on purpose: a "5% late fee"
    is 500 bps and stays an integer all the way through. Half-up rather
    than banker's rounding because that is what a lease says and what a
    court expects.
    """
    if not isinstance(minor, int) or isinstance(minor, bool):
        raise MoneyError("pct() takes integer minor units.")
    if not isinstance(basis_points, int) or isinstance(basis_points, bool):
        raise MoneyError("basis_points must be an integer.")
    sign = -1 if minor < 0 else 1
    v = abs(minor) * abs(basis_points)
    q, r = divmod(v, 10_000)
    if r * 2 >= 10_000:
        q += 1
    return sign * (1 if basis_points >= 0 else -1) * q


def clamp(minor: int, lo: int | None = None, hi: int | None = None) -> tuple[int, bool]:
    """(value, was_clamped). Used for jurisdiction ceilings on fees.

    Returns whether it clamped so the caller can LOG it. A late fee
    silently reduced to the legal maximum looks identical to one that was
    computed correctly, and the difference matters if it is ever disputed.
    """
    if not isinstance(minor, int) or isinstance(minor, bool):
        raise MoneyError("clamp() takes integer minor units.")
    out = minor
    if lo is not None and out < lo:
        out = lo
    if hi is not None and out > hi:
        out = hi
    return out, out != minor

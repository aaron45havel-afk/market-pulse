"""Property tax and insurance defaults for underwriting — one place.

Four tables used to answer this question and disagreed by up to 2x
(data_providers' 14-state homeowner tables, CHOROPLETH_STATES, the
headroom research files, and value_add's flat 1.25% / $3,600). This module
reads the two that carry provenance and units, and says which one answers
which question:

  * INVESTOR purchase tax rate, by state — data/headroom/proptax.json
    `rate_pct` (percent of PURCHASE price per year, researched 2026-07,
    homestead removed, assessment resets applied).
  * OWNER-OCCUPIED tax rate, by state — CHOROPLETH_STATES `property_tax`
    (Tax Foundation effective rate on owner-occupied homes).
  * LANDLORD insurance (DP-3), by state — data/headroom/insurance.json
    `landlord_annual` at a $300k dwelling.
  * HOMEOWNER insurance (HO-3), by state — CHOROPLETH_STATES `insurance`.

Within a state, the ZIP's own Census figure (median real-estate taxes paid
÷ median owner-occupied value, ACS 5-year) supplies the variation. It is
an EXISTING-owner rate, so it understates what a buyer pays wherever the
assessment resets at sale; those states take the state purchase rate.

Every default is an estimate, labelled as one, and editable on the page.
Pure apart from reading the two JSON files once.
"""
from __future__ import annotations

import json
import statistics
from functools import lru_cache
from pathlib import Path

DATA = Path(__file__).resolve().parent / "data" / "headroom"

# Assessment resets (or uncaps) to the purchase price at transfer, so the
# taxes existing owners pay — which is what the Census measures — say
# little about a buyer's bill. Taken from proptax.json's own method note:
# "acquisition-value reset states set to new-purchase rate (CA ...;
# FL/OK/AR/NM/SC/MI reset or uncap at transfer)".
ACQUISITION_RESET = frozenset({"CA", "FL", "OK", "AR", "NM", "SC", "MI"})

# An owner-occupant buying in California pays the 1% Prop 13 base plus
# voter-approved debt on the PURCHASE price, whatever the neighbours pay
# on 1990s assessments. Other reset states carry homestead programs that
# a statewide floor would get wrong, so only CA has one.
OWNER_PURCHASE_FLOOR_PCT = {"CA": 1.10}

# A per-ZIP effective rate outside this band is a data fault, not a
# property-tax regime (the ratio of two medians from small samples).
ZIP_RATE_SANE_PCT = (0.05, 5.0)

# Calibration of a ZIP's Census rate to the investor level is bounded:
# an investor never pays LESS than owners (homestead and classification
# rules only run the other way), and a factor above this is the state
# table and the Census disagreeing, not a tax regime.
INVESTOR_FACTOR_BOUNDS = (1.0, 2.5)

FALLBACK_INVESTOR_TAX_PCT = 1.10   # proptax.json's stated engine fallback
FALLBACK_OWNER_TAX_PCT = 1.00
FALLBACK_LANDLORD_INS = 2900       # insurance.json's stated engine default
FALLBACK_OWNER_INS = 2500

# Premiums follow replacement cost, not land-inclusive price. Scaling a
# $300k-dwelling premium by price, floored and capped, is the honest
# approximation without a structure value: the floor is insurance.json's
# own rule, the cap stops a $2M lot-value ZIP from tripling its premium.
INS_BASE_DWELLING = 300_000
INS_SCALE_BOUNDS = (0.6, 2.0)


@lru_cache(maxsize=1)
def _tables() -> dict:
    out = {"investor_tax": {}, "landlord_ins": {}, "tax_meta": {}, "ins_meta": {}}
    try:
        d = json.loads((DATA / "proptax.json").read_text())
        out["investor_tax"] = {k: v["rate_pct"] for k, v in d.get("table", {}).items()
                               if isinstance(v, dict) and v.get("rate_pct")}
        out["tax_meta"] = d.get("_meta", {})
    except (OSError, ValueError, KeyError):
        pass
    try:
        d = json.loads((DATA / "insurance.json").read_text())
        out["landlord_ins"] = {k: v["landlord_annual"] for k, v in d.get("table", {}).items()
                               if isinstance(v, dict) and v.get("landlord_annual")}
        out["ins_meta"] = d.get("_meta", {})
    except (OSError, ValueError, KeyError):
        pass
    from data_providers import CHOROPLETH_STATES
    out["owner_tax"] = {k: v["property_tax"] for k, v in CHOROPLETH_STATES.items()
                        if v.get("property_tax")}
    out["owner_ins"] = {k: v["insurance"] for k, v in CHOROPLETH_STATES.items()
                        if v.get("insurance")}
    return out


def zip_effective_rate(median_taxes, median_value) -> float | None:
    """Census owner-occupied effective rate in percent, or None.

    The ratio of two medians, not the median ratio — close enough to read
    a ZIP against its state, not precise enough to quote to a client.
    """
    try:
        t, v = float(median_taxes), float(median_value)
    except (TypeError, ValueError):
        return None
    if t <= 0 or v <= 0:
        return None
    rate = t / v * 100.0
    lo, hi = ZIP_RATE_SANE_PCT
    return round(rate, 3) if lo <= rate <= hi else None


def state_medians(zip_rates: dict) -> dict:
    """{state: median of its ZIPs' Census effective rates} from
    {zip: (state, rate or None)}."""
    by: dict = {}
    for _z, (st, rate) in zip_rates.items():
        if st and rate is not None:
            by.setdefault(st, []).append(rate)
    return {st: statistics.median(v) for st, v in by.items() if v}


def tax_defaults(state: str, zip_rate: float | None,
                 state_median_zip_rate: float | None) -> dict:
    """Default tax rates (percent of purchase price per year) for one ZIP.

    investor: reset states → the state purchase rate, flat. Elsewhere the
      ZIP's Census rate × (state investor rate ÷ the state's median ZIP
      rate), so a ZIP that taxes above its state stays above it, at the
      investor level. No Census rate → the state investor rate.
    owner: the ZIP's Census rate, floored at the purchase floor where one
      exists. No Census rate → the state owner-occupied rate.
    """
    t = _tables()
    st_inv = t["investor_tax"].get(state)
    st_own = t["owner_tax"].get(state)

    if state in ACQUISITION_RESET and st_inv:
        inv, inv_basis = st_inv, "state purchase rate (assessment resets at sale)"
    elif zip_rate is not None and st_inv and state_median_zip_rate:
        lo, hi = INVESTOR_FACTOR_BOUNDS
        factor = min(hi, max(lo, st_inv / state_median_zip_rate))
        inv, inv_basis = zip_rate * factor, "ZIP Census rate scaled to the state investor rate"
    elif st_inv:
        inv, inv_basis = st_inv, "state investor rate (no ZIP Census rate)"
    else:
        inv, inv_basis = FALLBACK_INVESTOR_TAX_PCT, "national fallback"

    if zip_rate is not None:
        own, own_basis = zip_rate, "ZIP Census rate (existing owners)"
    elif st_own:
        own, own_basis = st_own, "state owner-occupied rate (no ZIP Census rate)"
    else:
        own, own_basis = FALLBACK_OWNER_TAX_PCT, "national fallback"
    floor = OWNER_PURCHASE_FLOOR_PCT.get(state)
    if floor and own < floor:
        own, own_basis = floor, f"{state} purchase floor (Prop 13 base + local debt on the price)"

    return {"investor_pct": round(inv, 3), "investor_basis": inv_basis,
            "owner_pct": round(own, 3), "owner_basis": own_basis}


def insurance_defaults(state: str) -> dict:
    """State premiums at a $300k dwelling: landlord (DP-3) and owner (HO-3)."""
    t = _tables()
    land = t["landlord_ins"].get(state)
    own = t["owner_ins"].get(state)
    return {
        "landlord_300k": land or FALLBACK_LANDLORD_INS,
        "landlord_basis": "state DP-3 landlord average at $300k dwelling" if land else "national fallback",
        "owner_300k": own or FALLBACK_OWNER_INS,
        "owner_basis": "state HO-3 homeowner average" if own else "national fallback",
    }


def scaled_premium(base_300k, price) -> float | None:
    """A state premium scaled to this price, within INS_SCALE_BOUNDS."""
    try:
        b, p = float(base_300k), float(price)
    except (TypeError, ValueError):
        return None
    if b <= 0 or p <= 0:
        return None
    lo, hi = INS_SCALE_BOUNDS
    return round(b * min(hi, max(lo, p / INS_BASE_DWELLING)), 2)

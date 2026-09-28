"""
What a ZIP could rent for — from measured sources, never from a formula.

THIS REPLACES AN IMPUTATION THAT WAS NOT A RENT. The old fallback wrote
`home_value / 17 / 12` into median_rent_monthly for any ZIP that Zillow's
ZORI file did not cover — 17,358 of 25,774 ZIPs, 67% of the country, and
73% of Ohio. That number is not an estimate of rent. It is the home value
divided by 204, so every yield computed from it was circular: across all
17,358 imputed ZIPs there were FIVE distinct cap rates, every one of them
5.88% by construction. The board reported a rent and a yield for two
thirds of America and neither was a measurement of anything.

The replacement is a ladder of real sources, in order of how closely each
one answers "what would this actually rent for today":

    ZORI   Zillow Observed Rent Index. A repeat-rent index of asking
           rents, monthly, current. The closest thing to a market rent
           that exists for free. ~33% of ZIPs.

    SAFMR  HUD Small Area Fair Market Rent. Set per ZIP, BY BEDROOM, for
           voucher purposes: the 40th-percentile GROSS rent (utilities
           included) of standard-quality units, in the metros HUD has
           designated. Against Zillow it lands near the median on average
           (1.01x) but wide for any one ZIP — see the tier's caveat.

    FMR    HUD Fair Market Rent at county (or metro) level, by bedroom.
           Same basis, coarser geography. Covers everything SAFMR does
           not — about 14,000 ZIPs.

    ACS    Census B25064 median GROSS rent for the ZCTA. A real
           measurement of occupied stock, but it includes utilities, it
           describes leases already signed rather than what is being
           asked today, and it lags about two years.

RULES THIS MODULE ENFORCES:

  * ONE TIER ANSWERS. Tiers are never averaged or blended into a
    composite. A blended rent has no basis you can name, and a number
    whose basis you cannot name cannot be checked.

  * THE TIER TRAVELS WITH THE NUMBER. Every result carries which source
    answered, what it measures, and what is wrong with it. $1,467 of
    asking-rent index and $1,180 of voucher floor are not the same claim.

  * NOTHING IS INVENTED. When no tier has a plausible value the answer
    is None and the caller must render an absence. That is worse to look
    at and better to rely on.

Pure: no network, no database, no clock. The fetching lives in
scripts/refresh_rents.py and the storage in zips.db; nothing here knows
about either.
"""
from __future__ import annotations

# Rents outside this band are not rents. The old imputed column ranged
# from $116 to $41,813 a month — both ends impossible for a ZIP median,
# and both silently stored. A source that hands back something outside
# the band is treated as having no answer for that ZIP, and the ladder
# falls through to the next one rather than passing the number along.
RENT_FLOOR = 200.0
RENT_CEILING = 25_000.0

# The bedroom counts HUD publishes. ZORI's ZIP file and ACS B25064 are
# both all-units figures, so a bedroom split only ever comes from HUD.
BEDROOMS = ("0", "1", "2", "3", "4")

# Order is precedence. First tier with a plausible value wins.
TIERS = (
    {
        "key": "zori",
        "label": "Zillow ZORI",
        "basis": "asking",
        "measures": "a repeat-rent index of asking rents across the "
                    "rental stock, updated monthly",
        "caveat": "Smoothed and market-wide, so it lags a fast-moving "
                  "month and does not describe any one unit.",
        "by_bedroom": False,
    },
    {
        "key": "safmr",
        "label": "HUD Small Area FMR",
        "basis": "voucher-floor",
        "measures": "HUD's per-ZIP voucher standard: the 40th-percentile "
                    "GROSS rent (utilities included) of standard-quality "
                    "units, by bedroom",
        "caveat": "A voucher benchmark, not an asking rent. Checked against "
                  "Zillow where both exist (4,073 ZIPs, FY2027) the "
                  "2-bedroom figure is 1.01x Zillow's at the median, but a "
                  "quarter of ZIPs sit 11% or more below and a quarter 12% "
                  "or more above — close on average, loose for any one ZIP. "
                  "Not a floor: including utilities offsets the 40th "
                  "percentile.",
        "by_bedroom": True,
    },
    {
        "key": "fmr",
        "label": "HUD FMR (county)",
        "basis": "voucher-floor",
        "measures": "the same 40th-percentile gross-rent voucher standard, "
                    "set for the whole county (or metro) rather than the ZIP — "
                    "in New England, for the rent area of the ZIP's town",
        "caveat": "County-wide, so a good and a bad ZIP in the same county "
                  "carry the same number. Against Zillow (8,268 ZIPs, "
                  "FY2027) the 2-bedroom figure is 0.93x at the median, and "
                  "a quarter of ZIPs sit 19% or more below it.",
        "by_bedroom": True,
    },
    {
        "key": "acs",
        "label": "Census ACS median gross rent",
        "basis": "occupied-gross",
        "measures": "the median rent actually being paid on occupied "
                    "units in the ZCTA, from the 5-year survey",
        "caveat": "GROSS rent, so it includes utilities. It describes "
                  "leases already signed, not what is being asked today, "
                  "and the 5-year window lags roughly two years — in a "
                  "market that moved, it is history.",
        "by_bedroom": False,
    },
)
TIER_BY_KEY = {t["key"]: t for t in TIERS}
TIER_ORDER = tuple(t["key"] for t in TIERS)


def is_plausible(rent) -> bool:
    """A number that could be a monthly rent for a whole ZIP.

    Rejects rather than clamps. Clamping an impossible $41,813 down to
    the ceiling would produce a plausible-looking number from a source
    that is clearly broken for that ZIP, which is the same failure as
    imputing one.
    """
    if rent is None or isinstance(rent, bool):
        return False
    try:
        v = float(rent)
    except (TypeError, ValueError):
        return False
    if v != v:                                   # NaN
        return False
    return RENT_FLOOR <= v <= RENT_CEILING


def _clean(rent):
    return round(float(rent)) if is_plausible(rent) else None


def clean_bedrooms(raw) -> dict | None:
    """Keep only the bedroom entries that are plausible rents.

    A partial answer is still an answer — HUD occasionally omits the
    4-bedroom figure for a small area — so the surviving bedrooms are
    returned rather than discarding the whole set.
    """
    if not isinstance(raw, dict):
        return None
    out = {}
    for b in BEDROOMS:
        v = _clean(raw.get(b, raw.get(int(b) if b.isdigit() else b)))
        if v is not None:
            out[b] = v
    return out or None


def resolve(zori=None, safmr=None, fmr=None, acs=None,
            safmr_bedrooms=None, fmr_bedrooms=None, as_of=None) -> dict:
    """Pick the tier that answers, and say which one it was.

    Returns a dict that ALWAYS carries every key, whether or not a rent
    was found — a caller reading `.get("tier")` on a ragged shape is how
    the /schloss page 500'd once already.

    `alternatives` carries every tier that had a plausible value, not
    just the winner. The SPREAD BETWEEN THEM IS ITSELF INFORMATION: ZORI
    far above the local FMR means asking rents have pulled away from the
    voucher standard, which is what a tightening market looks like.
    """
    values = {"zori": _clean(zori), "safmr": _clean(safmr),
              "fmr": _clean(fmr), "acs": _clean(acs)}
    alternatives = {k: v for k, v in values.items() if v is not None}

    tier = next((k for k in TIER_ORDER if values.get(k) is not None), None)

    # A bedroom split only ever comes from HUD, and it is reported under
    # its own tier name even when the headline rent came from ZORI. Two
    # labelled facts side by side, never one blended number.
    beds = clean_bedrooms(safmr_bedrooms)
    beds_tier = "safmr" if beds else None
    if not beds:
        beds = clean_bedrooms(fmr_bedrooms)
        beds_tier = "fmr" if beds else None

    if tier is None:
        return {
            "rent": None, "tier": None, "label": None, "basis": None,
            "measures": None, "caveat": None, "as_of": as_of,
            "by_bedroom": beds, "by_bedroom_tier": beds_tier,
            "alternatives": alternatives, "spread": None,
            "note": "No measured rent for this ZIP from any source. "
                    "Nothing is estimated from the home value — that "
                    "would restate the price, not measure the rent.",
        }

    t = TIER_BY_KEY[tier]
    return {
        "rent": values[tier], "tier": tier, "label": t["label"],
        "basis": t["basis"], "measures": t["measures"], "caveat": t["caveat"],
        "as_of": as_of, "by_bedroom": beds, "by_bedroom_tier": beds_tier,
        "alternatives": alternatives, "spread": spread(alternatives),
        "note": None,
    }


# A HUD rent that would take this share of the ZIP's median household
# income is flagged, not adjusted. HUD sets FMR for a whole county or metro,
# and the first national run showed it drifting ABOVE Zillow exactly where
# local incomes are low. Where Zillow also has a figure (FY2027):
#
#     HUD rent / ZIP income    county FMR ÷ Zillow    share 20%+ above Zillow
#     under 25%                0.70–0.89              0–4%
#     30–35%                   1.00                   13%
#     40–50%                   1.10                   28%
#     50%+                     1.13                   32%
#
# So from 40% of income a HUD figure is as likely as not to overstate what
# the ZIP's own market pays — though a landlord renting to voucher holders
# can still be paid it.
HUD_RENT_STRAIN = 0.40


def hud_rent_strains_income(rent, tier, median_household_income) -> bool:
    """True when a HUD-tier rent is at least HUD_RENT_STRAIN of the ZIP's
    median household income. Only HUD tiers: Zillow's figure is the ZIP's
    own market, so a high share there is a finding about the ZIP, not a
    doubt about the rent."""
    if tier not in ("safmr", "fmr") or not rent or not median_household_income:
        return False
    if median_household_income <= 0:
        return False
    return rent * 12 / median_household_income >= HUD_RENT_STRAIN


def spread(alternatives: dict) -> dict | None:
    """How far apart the sources are, when more than one answered.

    ZORI well above the voucher floor is a tight market; at or below it
    is a soft one. Reported as a ratio and in words so the reading does
    not depend on the caller doing the arithmetic correctly.
    """
    a = {k: v for k, v in (alternatives or {}).items() if v}
    if len(a) < 2:
        return None
    lo_k = min(a, key=lambda k: a[k])
    hi_k = max(a, key=lambda k: a[k])
    if a[lo_k] <= 0:
        return None
    ratio = a[hi_k] / a[lo_k]

    reading = None
    floor = a.get("safmr") or a.get("fmr")
    asking = a.get("zori")
    if floor and asking:
        r = asking / floor
        if r >= 1.25:
            reading = ("Asking rents sit well above the voucher floor — "
                       "the spread a tight rental market leaves.")
        elif r <= 1.0:
            reading = ("Asking rents are at or below the voucher floor, "
                       "which is what a soft market looks like.")
        else:
            reading = "Asking rents run modestly above the voucher floor."
    return {"low": lo_k, "low_rent": a[lo_k], "high": hi_k,
            "high_rent": a[hi_k], "ratio": round(ratio, 3),
            "reading": reading}


def cap_rate_pct(rent, home_value, expense_ratio: float = 0.40):
    """Annual net yield on the home value, or None.

    NONE, NOT A NUMBER, when the rent is missing. The old board printed
    5.88% for 17,358 ZIPs because the rent behind it was the home value
    divided by 204 — the yield was arithmetic on itself. A blank cell is
    the honest rendering of a yield nobody can compute.

    `expense_ratio` is the share of gross rent eaten by taxes, insurance,
    maintenance and vacancy. 40% is the common rule of thumb and is a
    rule of thumb, not a measurement of this property.
    """
    if not is_plausible(rent):
        return None
    try:
        hv = float(home_value)
    except (TypeError, ValueError):
        return None
    if hv <= 0:
        return None
    noi = float(rent) * 12.0 * (1.0 - expense_ratio)
    return round(noi / hv * 100.0, 2)


def coverage(rows) -> dict:
    """Tier census across a set of ZIPs — what the board is standing on.

    `real_pct` counts every tier, because every tier here is a
    measurement. The number that mattered before was the 33% of ZIPs
    with a rent that was not simply the home value.
    """
    rows = list(rows or [])
    by = {k: 0 for k in TIER_ORDER}
    none_n = 0
    for r in rows:
        t = (r or {}).get("tier") if isinstance(r, dict) else r
        if t in by:
            by[t] += 1
        else:
            none_n += 1
    total = len(rows)
    have = total - none_n
    return {"total": total, "by_tier": by, "none": none_n, "have": have,
            "real_pct": round(have / total * 100, 1) if total else 0.0}

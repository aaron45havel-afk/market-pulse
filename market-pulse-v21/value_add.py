"""Value-Add — CA multifamily-that-needs-work finder. /value-add

Two layers (decided with the user, 2-person team / $200k):

  1. HUNTING GROUNDS — rank every CA ZIP by how likely it is to hold
     needs-work 2-4 unit buildings whose value-add math works. This is
     deliberately NOT the strict screen: forced-equity hunting lives
     exactly where the strict gates refuse to (old stock, cheap per
     unit, high yields). Signals, all from zips.db:
       • 2-4 unit density (ACS)          — can't buy a duplex where none exist
       • gross yield (cap_rate_pct)      — the rent engine that makes rehab pay
       • $ discount vs county median     — cheap for the area ≈ condition
       • renter share (ACS)              — tenant pool + exit liquidity
       • old stock (pct_pre_1960 /
         median_year_built)              — deferred-maintenance probability;
                                           populates on the next monthly
                                           national-zips rebuild, gracefully
                                           absent until then
     Plus non-gating context flags: rent-control jurisdiction, soft-story
     retrofit mandate, crime (information, not exclusion).

  2. REHAB CHECKER — 203(k)-house-hack-first underwriting for a pasted
     listing: as-is price + units + rehab budget + expected rents →
       • FHA 203(k): 3.5% down on (price + rehab), vs county FHA 2-4
         unit limits, cash to close vs deployable assets, post-reno
         PITI vs (n-1) rents, income to qualify, 3-4 unit
         self-sufficiency test
       • Equity created: income-approach ARV (post-reno gross rent ÷
         county yield benchmark) minus all-in — judged vs the 70-75%
         rule (DISCOUNT / FAIR / THIN)
       • BRRRR test: cash returned by a 75%-of-ARV refi
       • CA cost adders: rent control, soft-story, pre-1978 lead

ZIP-level ARV is a first-pass underwrite, not an appraisal — the UI
says so. Sourcing is manual by design: keyword-search Zillow/Redfin in
the target ZIPs (fixer, as-is, TLC, probate, estate) — the durable,
TOS-clean, 2-person-maintainable approach.
"""
from __future__ import annotations

import sqlite3
import statistics
from pathlib import Path

from norcal import _region  # county → NorCal / SoCal / San Diego

_ZIPS_DB = Path(__file__).resolve().parent / "data" / "zips.db"

ASSETS_DEFAULT = 200_000
RESERVES_DEFAULT = 40_000
RATE_DEFAULT = 6.5           # 30-yr fixed %
MIP_ANNUAL = 0.55            # FHA annual MIP %, life-of-loan under 10% down
REHAB_CARRY_MONTHS = 6       # interest carried during renovation
CLOSING_PCT = 2.0            # closing costs (FHA allows some roll-in; cash-side estimate)

SEARCH_KEYWORDS = "fixer, as-is, TLC, needs work, probate, estate sale, contractor special"

# Local ordinances STRICTER than statewide AB 1482 (which caps increases
# at 5%+CPI ≤10% on most 15+ year-old buildings everywhere in CA).
RENT_CONTROL_CITIES = {
    "San Francisco", "Oakland", "Berkeley", "San Jose", "Los Angeles",
    "Santa Monica", "West Hollywood", "Beverly Hills", "East Palo Alto",
    "Hayward", "Alameda", "Richmond", "Mountain View", "Inglewood",
    "Culver City", "Pasadena", "Sacramento", "Baldwin Park", "Pomona",
    "Santa Ana", "Bell Gardens",
}
# Cities with mandatory soft-story seismic retrofit programs — a known
# five-to-six-figure cost on pre-1980 wood-frame multifamily.
SOFT_STORY_CITIES = {
    "San Francisco", "Los Angeles", "Oakland", "Berkeley", "Santa Monica",
    "West Hollywood", "Pasadena", "San Jose", "Torrance", "Culver City",
    "Beverly Hills",
}

# FHA 2026-ish loan limits (approx — update annually). 1-unit base by
# county tier; 2-4 unit limits derived with FHA's standard ratios.
_FHA_1U_CEILING = 1_209_750     # high-cost CA counties
_FHA_1U_FLOOR = 524_225
_FHA_COUNTY_1U = {
    "San Francisco County": _FHA_1U_CEILING, "San Mateo County": _FHA_1U_CEILING,
    "Santa Clara County": _FHA_1U_CEILING, "Marin County": _FHA_1U_CEILING,
    "Alameda County": _FHA_1U_CEILING, "Contra Costa County": _FHA_1U_CEILING,
    "Santa Cruz County": _FHA_1U_CEILING, "Los Angeles County": _FHA_1U_CEILING,
    "Orange County": _FHA_1U_CEILING, "San Benito County": _FHA_1U_CEILING,
    "Napa County": 1_017_750, "San Diego County": 1_006_250,
    "Ventura County": 954_500, "Sonoma County": 897_000,
    "Monterey County": 920_000, "San Luis Obispo County": 967_150,
    "Santa Barbara County": 838_350, "Solano County": 685_400,
    "Sacramento County": 763_600, "Placer County": 763_600,
    "El Dorado County": 763_600, "Yolo County": 763_600,
}
_FHA_UNIT_RATIO = {2: 1.2802, 3: 1.5475, 4: 1.9231}


def fha_limit(county: str | None, units: int) -> int:
    base = _FHA_COUNTY_1U.get(county or "", _FHA_1U_FLOOR)
    return round(base * _FHA_UNIT_RATIO.get(units, 1.0))


def _pct_rank(values: list[float], v: float | None) -> float | None:
    """v's percentile (0-100) within values (higher v → higher pct)."""
    if v is None or not values:
        return None
    below = sum(1 for x in values if x < v)
    return round(below / len(values) * 100, 1)


def _universe(conn) -> list[sqlite3.Row]:
    conn.row_factory = sqlite3.Row
    cols = {r[1] for r in conn.execute("PRAGMA table_info(zips)")}
    extra = ""
    if "pct_pre_1960" in cols:
        extra += ", pct_pre_1960"
    if "median_year_built" in cols:
        extra += ", median_year_built"
    return conn.execute(f"""
        SELECT zip, name, county, population, population_density,
               median_home_value, median_rent_monthly, cap_rate_pct,
               pct_renter_occupied, pct_multi_unit, pct_rent_burdened,
               crime_index {extra}
        FROM zips
        WHERE state = 'CA' AND population > 3000
          AND median_home_value IS NOT NULL
          AND median_rent_monthly IS NOT NULL
    """).fetchall()


def _flags(city: str, county: str | None, pre60, myb) -> list[dict]:
    out = []
    rc = next((c for c in RENT_CONTROL_CITIES if c in city), None)
    if rc:
        out.append({"key": "rentctl", "label": "rent control",
                    "title": f"{rc} has local rent control stricter than AB 1482 — the raise-to-market-after-reno play is constrained. Vacancy decontrol still applies on turnover."})
    ss = next((c for c in SOFT_STORY_CITIES if c in city), None)
    if ss:
        out.append({"key": "softstory", "label": "soft-story",
                    "title": f"{ss} mandates seismic retrofit of pre-1980 soft-story multifamily — budget a known five-to-six figure line item if the building qualifies."})
    if (pre60 is not None and pre60 >= 50) or (myb is not None and myb < 1960):
        out.append({"key": "old", "label": "pre-1960 stock",
                    "title": "Majority of stock predates 1960 — deferred maintenance likely, and any rehab budget carries pre-1978 lead/asbestos handling costs."})
    return out


def hunting_grounds(region: str = "All CA", limit: int = 60) -> dict:
    """Ranked value-add hunt ZIPs. Score = weighted percentile blend
    within the CA universe; old-stock term joins automatically once the
    year-built columns exist."""
    if not _ZIPS_DB.exists():
        return {"rows": [], "universe_n": 0, "has_age": False, "region": region}
    conn = sqlite3.connect(str(_ZIPS_DB))
    try:
        rows = _universe(conn)
    finally:
        conn.close()

    has_age = rows and "pct_pre_1960" in rows[0].keys()

    # County median $ (all CA rows) for the discount term.
    by_county: dict[str, list[float]] = {}
    for r in rows:
        if r["median_home_value"]:
            by_county.setdefault(r["county"] or "?", []).append(r["median_home_value"])
    county_med = {c: statistics.median(v) for c, v in by_county.items() if len(v) >= 3}

    caps = [r["cap_rate_pct"] for r in rows if r["cap_rate_pct"] is not None]
    dens = [r["population_density"] for r in rows if r["population_density"] is not None]
    multis = [r["pct_multi_unit"] for r in rows if r["pct_multi_unit"] is not None]
    renters = [r["pct_renter_occupied"] for r in rows if r["pct_renter_occupied"] is not None]
    pre60s = [r["pct_pre_1960"] for r in rows if has_age and r["pct_pre_1960"] is not None]

    scored = []
    for r in rows:
        reg = _region(r["county"])
        if region != "All CA" and reg != region:
            continue
        cmed = county_med.get(r["county"] or "?")
        discount = ((cmed - r["median_home_value"]) / cmed * 100) if cmed else None

        p_cap = _pct_rank(caps, r["cap_rate_pct"])
        p_dens = _pct_rank(dens, r["population_density"])
        p_multi = _pct_rank(multis, r["pct_multi_unit"])
        p_renter = _pct_rank(renters, r["pct_renter_occupied"])
        p_disc = max(0.0, min(100.0, 50 + (discount or 0) * 1.2)) if discount is not None else None
        pre60 = r["pct_pre_1960"] if has_age else None
        myb = r["median_year_built"] if has_age else None
        p_age = _pct_rank(pre60s, pre60) if has_age else None

        # ACS multifamily/renter/age terms join the blend when present
        # (CA rows are currently NULL there — the yield/discount/density
        # core carries the score until the next national-zips rebuild).
        parts = {"yield": (p_cap, 0.35), "discount": (p_disc, 0.30),
                 "density": (p_dens, 0.15),
                 "multi": (p_multi, 0.20), "renter": (p_renter, 0.10)}
        if p_age is not None:
            parts["age"] = (p_age, 0.15)
        avail = {k: (v, w) for k, (v, w) in parts.items() if v is not None}
        if len(avail) < 2:
            continue
        wsum = sum(w for _, w in avail.values())
        score = round(sum(v * w for v, w in avail.values()) / wsum, 1)

        city = (r["name"] or "").replace(", CA", "")
        scored.append({
            "zip": r["zip"], "name": city, "county": r["county"], "region": reg,
            "population": r["population"], "score": score,
            "median_home_value": r["median_home_value"],
            "median_rent": r["median_rent_monthly"],
            "cap_rate_pct": r["cap_rate_pct"],
            "pct_multi_unit": r["pct_multi_unit"],
            "pct_renter": r["pct_renter_occupied"],
            "discount_pct": round(discount, 1) if discount is not None else None,
            "pct_pre_1960": pre60, "median_year_built": myb,
            "crime": r["crime_index"],
            "flags": _flags(city, r["county"], pre60, myb),
        })
    scored.sort(key=lambda s: -s["score"])
    return {"rows": scored[:limit], "universe_n": len(scored),
            "has_age": bool(has_age), "region": region,
            "keywords": SEARCH_KEYWORDS}


# ── Rehab checker ────────────────────────────────────────────────────

def _piti(loan: float, rate_pct: float, price: float, mip: bool) -> dict:
    r = rate_pct / 100 / 12
    n = 360
    p_i = loan * (r * (1 + r) ** n) / ((1 + r) ** n - 1) if r > 0 else loan / n
    tax = price * 0.0125 / 12
    ins = 3600 / 12                       # multifamily hazard ballpark
    mip_mo = loan * MIP_ANNUAL / 100 / 12 if mip else 0.0
    return {"p_i": p_i, "tax": tax, "ins": ins, "mip": mip_mo,
            "piti": p_i + tax + ins + mip_mo}


def rehab_check(zip_code: str, price: float, units: int, rehab: float,
                rent_unit: float | None = None, income: float | None = None,
                assets: float = ASSETS_DEFAULT, rate_pct: float = RATE_DEFAULT) -> dict | None:
    """Underwrite one needs-work listing, 203(k)-house-hack first."""
    if not _ZIPS_DB.exists():
        return None
    conn = sqlite3.connect(str(_ZIPS_DB))
    conn.row_factory = sqlite3.Row
    try:
        r = conn.execute(
            "SELECT * FROM zips WHERE zip = ? AND state = 'CA'", (zip_code,)).fetchone()
        if r is None:
            return None
        cty_rows = conn.execute(
            "SELECT cap_rate_pct FROM zips WHERE county = ? AND state='CA' "
            "AND cap_rate_pct IS NOT NULL", (r["county"],)).fetchall()
    finally:
        conn.close()

    units = max(2, min(4, units))
    market_rent = rent_unit or r["median_rent_monthly"] or 0
    county_yield = statistics.median([x["cap_rate_pct"] for x in cty_rows]) \
        if cty_rows else (r["cap_rate_pct"] or 6.0)
    county_yield = max(3.0, min(15.0, county_yield))

    # ── ARV: LOWER of two approaches (lender-style conservatism) ──
    # Comp approach: 2-4 unit buildings trade at a multiple of the same
    # ZIP's single-family median (≈1.4x duplex / 1.7x triplex / 2.0x
    # fourplex). Income approach: post-reno gross rent ÷ the ZIP's own
    # gross yield, floored at 4.5% so expensive-neighborhood yields
    # can't inflate ARV. Taking the min keeps the equity verdict honest.
    unit_mult = {2: 1.40, 3: 1.70, 4: 2.00}[units]
    comp_arv = (r["median_home_value"] or 0) * unit_mult or None
    own_yield = max((r["cap_rate_pct"] or county_yield or 4.5), 4.5)
    gross_annual = market_rent * units * 12
    income_arv = gross_annual / (own_yield / 100)
    candidates = [a for a in (comp_arv, income_arv) if a]
    arv = min(candidates) if candidates else None
    carry = (price + rehab) * 0.965 * (rate_pct / 100) * (REHAB_CARRY_MONTHS / 12)
    all_in = price + rehab + carry
    equity = (arv - all_in) if arv else None
    margin = (equity / arv * 100) if (arv and equity is not None) else None
    if margin is None:
        verdict = "NO BENCHMARK"
    elif margin >= 20:
        verdict = "DISCOUNT"
    elif margin >= 8:
        verdict = "FAIR"
    else:
        verdict = "THIN"

    # ── FHA 203(k) house-hack ──
    total_project = price + rehab
    limit = fha_limit(r["county"], units)
    fha_ok = total_project * 0.965 <= limit
    loan_fha = total_project * 0.965
    down_fha = total_project * 0.035
    cash_close = down_fha + total_project * CLOSING_PCT / 100
    pf = _piti(loan_fha, rate_pct, total_project, mip=True)
    hh_rent = market_rent * (units - 1)
    hh_net = pf["piti"] - hh_rent
    income_needed = pf["piti"] * 12 / 0.28
    qualifies = (income is not None and income > 0 and income * 0.28 / 12 >= pf["piti"]) \
        if income else None
    self_suff = None
    if units >= 3:
        self_suff = 0.75 * market_rent * units >= pf["piti"]

    # ── BRRRR (investor exit) ──
    refi = 0.75 * arv if arv else None
    cash_in = down_fha + rehab * 0  # 203(k) rolls rehab; investor path below
    inv_down = price * 0.25
    inv_cash = inv_down + rehab + carry
    brrrr_left = (inv_cash - (refi - price * 0.75)) if refi else None

    city = (r["name"] or "").replace(", CA", "")
    return {
        "zip": zip_code, "name": city, "county": r["county"],
        "region": _region(r["county"]),
        "units": units, "price": price, "rehab": rehab,
        "market_rent": market_rent, "county_yield": round(own_yield, 2), "arv_comp": round(comp_arv) if comp_arv else None, "arv_income": round(income_arv) if income_arv else None,
        "arv": round(arv) if arv else None,
        "all_in": round(all_in), "carry": round(carry),
        "equity": round(equity) if equity is not None else None,
        "margin": round(margin, 1) if margin is not None else None,
        "verdict": verdict,
        "fha": {"limit": limit, "ok": fha_ok, "down": round(down_fha),
                "cash_close": round(cash_close),
                "cash_ok": cash_close <= max(0, assets - RESERVES_DEFAULT),
                "piti": round(pf["piti"]), "hh_rent": round(hh_rent),
                "hh_net": round(hh_net), "income_needed": round(income_needed),
                "qualifies": qualifies, "self_suff": self_suff},
        "brrrr": {"refi": round(refi) if refi else None,
                  "inv_cash": round(inv_cash),
                  "left_in": round(brrrr_left) if brrrr_left is not None else None},
        "flags": _flags(city, r["county"],
                        r["pct_pre_1960"] if "pct_pre_1960" in r.keys() else None,
                        r["median_year_built"] if "median_year_built" in r.keys() else None),
        "income": income, "assets": assets, "rate_pct": rate_pct,
    }

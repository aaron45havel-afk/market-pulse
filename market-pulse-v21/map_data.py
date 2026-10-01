"""The national ZIP map: one metric at a time over every ZCTA in
data/zip_profile.db.

No score and no blend. A metric is either a published figure (Zillow,
Realtor.com, Redfin, Census, HUD, FEMA, NOAA) or one line of the same
underwriting the ZIP page runs (underwrite.py), at the defaults the page
states. An underwritten investor metric is computed on ONE price/rent
basis for the whole map — Zillow's typical home and rent, or a 3-bed home
and HUD's 3-bed rent — never Zillow rent in one ZIP and HUD rent in the
next, which would colour the map by which source happened to exist.

The browser gets the ZIP points once (`base`) and then one array of values
per metric, in the same order (`metric_values`). Filtering, the legend and
the table are drawn from those arrays in the page.
"""
from __future__ import annotations

import json
import sqlite3
from collections import Counter
from functools import lru_cache
from pathlib import Path

import re_assumptions as RA
import underwrite as U
from zip_page import PROFILE_DB, month_label

# ── the price/rent basis for underwritten investor metrics ──────────────

BASES = {
    "zillow": {
        "label": "Zillow typical home + Zillow typical rent",
        "price": "zhvi", "rent": "zori",
        "note": "Zillow's typical home value (ZHVI) and typical asking rent (ZORI): the same "
                "mid-market home, at market rent. Covers fewer ZIPs — mostly metro areas.",
    },
    "hud3": {
        "label": "3-bed: Zillow 3-bed value + HUD 3-bed rent",
        "price": "zhvi_3br", "rent": "hud_rent_br3",
        "note": "Zillow's typical 3-bedroom value with HUD's 3-bedroom Fair Market Rent — a "
                "40th-percentile rent including utilities, set for housing vouchers, not an "
                "asking rent. Wider coverage; reads low against asking rents in hot markets.",
    },
}
DEFAULT_BASIS = "zillow"

# ── the metrics ─────────────────────────────────────────────────────────
# key, label, group, fmt, how ("col:<column>" | "inv:<underwrite.investor field>" | "own:<underwrite.owner field>"),
# source, as_of ("col:<month column>" | "meta:<key>" | literal), thin column, stale, help

_GROUPS = ("Investor — underwritten at defaults", "Owner-occupant — underwritten at defaults",
           "Prices", "Rents", "Listings (Realtor.com)", "Sales (Redfin, frozen)",
           "People and housing (Census)", "Risk and climate")

_M = [
    # investor (basis-dependent)
    ("cap_rate", "Cap rate", 0, "pct2", "inv:cap_rate_pct", "underwrite", None, None, False,
     "Net operating income ÷ price, before any loan."),
    ("cash_flow", "Cash flow / month", 0, "money", "inv:cash_flow_monthly", "underwrite", None, None, False,
     "After the loan payment, at 25% down and the investor rate."),
    ("coc", "Cash-on-cash return", 0, "pct2", "inv:cash_on_cash_pct", "underwrite", None, None, False,
     "Annual cash flow ÷ cash put in (down payment + closing costs)."),
    ("dscr", "DSCR", 0, "ratio", "inv:dscr", "underwrite", None, None, False,
     "NOI ÷ annual debt service. Lenders usually want 1.20-1.25 or more."),
    ("break_even_occ", "Break-even occupancy", 0, "pct", "inv:break_even_occupancy_pct", "underwrite",
     None, None, False, "Share of the year the home must be let to cover costs and the loan. Over 100% "
     "means rent can't cover them."),
    ("gross_yield", "Gross rent yield", 0, "pct2", "inv:gross_yield_pct", "underwrite", None, None, False,
     "Twelve months' rent ÷ price. No costs."),
    ("price_to_rent", "Price-to-rent (GRM)", 0, "x", "inv:grm", "underwrite", None, None, False,
     "Price ÷ a year's rent — the gross rent multiplier."),
    # owner (Zillow typical home)
    ("own_payment", "Monthly payment to own", 1, "money", "own:monthly_total", "owner", None, None, False,
     "Principal, interest, PMI, tax and insurance on Zillow's typical home at 20% down."),
    ("own_income_needed", "Income needed to buy", 1, "money", "own:income_needed", "owner", None, None, False,
     "At 28% of gross income for housing (36% with other debt)."),
    ("own_payment_to_income", "Payment ÷ median household income", 1, "pct", "aff:ratio",
     "affordability", None, None, False, "The payment on the typical home (20% down, tax, insurance) as a share of "
     "the ZIP's Census median household income in today's dollars. 30% is the cost-burden line."),
    ("aff_ratio19", "Payment ÷ income, 2019", 1, "pct", "aff:ratio19", "affordability", None, None, False,
     "The same share with 2019's average price, average rate and Census 2015-19 income."),
    ("aff_change", "Payment ÷ income, change since 2019", 1, "pts", "aff:change_pts", "affordability",
     None, None, False, "Percentage points; above zero, buying the typical home takes more of the typical income "
     "than in 2019."),
    ("aff_gap", "Typical home vs price affordable at 30%", 1, "pct", "aff:gap_pct", "affordability",
     None, None, False, "How far Zillow's typical home sits above (or below) the price whose payment takes 30% of "
     "the median income at today's rate."),
    ("own_minus_rent", "Cost of owning minus rent / month", 1, "money", "own:own_minus_rent_monthly",
     "owner", None, None, False, "Interest, PMI, tax, insurance, upkeep and the down payment's lost return, "
     "less Zillow's typical rent. Above zero, renting the same home costs less."),
    # prices
    ("zhvi", "Typical home value", 2, "money", "col:zhvi", "Zillow ZHVI", "col:zhvi_month", None, False, ""),
    ("zhvi_yoy_pct", "Home value, 1-yr change", 2, "pct", "col:zhvi_yoy_pct", "Zillow ZHVI", "col:zhvi_month",
     None, False, ""),
    ("zhvi_5y_pct", "Home value, 5-yr change", 2, "pct", "col:zhvi_5y_pct", "Zillow ZHVI", "col:zhvi_month",
     None, False, ""),
    ("zhvi_from_peak_pct", "Home value vs its peak", 2, "pct", "col:zhvi_from_peak_pct", "Zillow ZHVI",
     "col:zhvi_month", None, False, "Below zero: still under the highest month on record."),
    ("zhvi_3br", "Typical 3-bed value", 2, "money", "col:zhvi_3br", "Zillow ZHVI", "col:zhvi_month", None,
     False, ""),
    ("rdc_list_price", "Median list price", 2, "money", "col:rdc_list_price", "Realtor.com", "col:rdc_month",
     "rdc_thin", False, ""),
    ("rdc_list_ppsf", "List price per sq ft", 2, "money", "col:rdc_list_ppsf", "Realtor.com", "col:rdc_month",
     "rdc_thin", False, ""),
    ("price_to_income", "Price-to-income", 2, "x", "col:price_to_income", "Zillow ZHVI ÷ Census income",
     "col:zhvi_month", None, False, ""),
    ("tax_rate_zip", "Property tax, effective", 2, "pct2", "col:tax_rate_zip", "Census ACS",
     "meta:acs_vintage", None, False, "Median taxes paid ÷ median owner value — what existing owners pay."),
    # rents
    ("zori", "Typical asking rent", 3, "money", "col:zori", "Zillow ZORI", "col:zori_month", None, False, ""),
    ("zori_yoy_pct", "Asking rent, 1-yr change", 3, "pct", "col:zori_yoy_pct", "Zillow ZORI",
     "col:zori_month", None, False, ""),
    ("hud_rent_br2", "HUD Fair Market Rent, 2-bed", 3, "money", "col:hud_rent_br2", "HUD",
     "", None, False, "40th-percentile rent including utilities, set for housing vouchers."),
    ("hud_rent_br3", "HUD Fair Market Rent, 3-bed", 3, "money", "col:hud_rent_br3", "HUD",
     "", None, False, "40th-percentile rent including utilities, set for housing vouchers."),
    ("acs_gross_rent", "Median rent paid (Census)", 3, "money", "col:acs_gross_rent", "Census ACS",
     "meta:acs_vintage", None, False, "What existing tenants pay, including utilities — lags the market."),
    ("pct_rent_burdened", "Renters paying 30%+ of income", 3, "pct", "col:pct_rent_burdened", "Census ACS",
     "meta:acs_vintage", None, False, ""),
    # listings
    ("rdc_dom", "Median days on market", 4, "days", "col:rdc_dom", "Realtor.com", "col:rdc_month",
     "rdc_thin", False, ""),
    ("rdc_dom_yoy_pct", "Days on market, 1-yr change", 4, "pct", "col:rdc_dom_yoy_pct", "Realtor.com",
     "col:rdc_month", "rdc_thin", False, ""),
    ("rdc_active", "Active listings", 4, "num", "col:rdc_active", "Realtor.com", "col:rdc_month",
     "rdc_thin", False, ""),
    ("rdc_active_yoy_pct", "Active listings, 1-yr change", 4, "pct", "col:rdc_active_yoy_pct", "Realtor.com",
     "col:rdc_month", "rdc_thin", False, "Inventory building (up) or drying out (down)."),
    ("rdc_pending_ratio", "Pending ÷ active listings", 4, "ratio", "col:rdc_pending_ratio", "Realtor.com",
     "col:rdc_month", "rdc_thin", False, "Above ~0.5 is a fast market."),
    ("rdc_price_cut_pct", "Listings with a price cut", 4, "pct", "col:rdc_price_cut_pct", "Realtor.com",
     "col:rdc_month", "rdc_thin", False, ""),
    ("rdc_list_price_yoy_pct", "List price, 1-yr change", 4, "pct", "col:rdc_list_price_yoy_pct",
     "Realtor.com", "col:rdc_month", "rdc_thin", False, ""),
    # sales (frozen)
    ("rf_sale_price", "Median sale price", 5, "money", "col:rf_sale_price", "Redfin", "col:rf_period_end",
     "rf_thin", True, ""),
    ("rf_sale_to_list_pct", "Sale price ÷ list price", 5, "pct", "col:rf_sale_to_list_pct", "Redfin",
     "col:rf_period_end", "rf_thin", True, ""),
    ("rf_sold_above_list_pct", "Sold above list", 5, "pct", "col:rf_sold_above_list_pct", "Redfin",
     "col:rf_period_end", "rf_thin", True, ""),
    ("rf_months_supply", "Months of supply", 5, "num1", "col:rf_months_supply", "Redfin",
     "col:rf_period_end", "rf_thin", True, ""),
    ("rf_sold_dom", "Days to sell", 5, "days", "col:rf_sold_dom", "Redfin", "col:rf_period_end",
     "rf_thin", True, ""),
    # people and housing
    ("acs_median_income", "Median household income", 6, "money", "col:acs_median_income", "Census ACS",
     "meta:acs_vintage", None, False, ""),
    ("population", "Population", 6, "num", "col:population", "Census ACS", "meta:acs_vintage", None, False, ""),
    ("density", "People per sq km", 6, "num", "col:density", "Census ACS + Gazetteer", "meta:acs_vintage",
     None, False, ""),
    ("pct_renter", "Renter households", 6, "pct", "col:pct_renter", "Census ACS", "meta:acs_vintage",
     None, False, ""),
    ("rental_vacancy_pct", "Rental vacancy", 6, "pct", "col:rental_vacancy_pct", "Census ACS",
     "meta:acs_vintage", None, False, "Not measured where fewer than 50 rental units."),
    ("pct_2_4_units", "Homes in 2-4 unit buildings", 6, "pct", "col:pct_2_4_units", "Census ACS",
     "meta:acs_vintage", None, False, ""),
    ("acs_year_built", "Median year built", 6, "year", "col:acs_year_built", "Census ACS",
     "meta:acs_vintage", None, False, ""),
    ("pct_bachelors", "Adults with a bachelor's degree", 6, "pct", "col:pct_bachelors", "Census ACS",
     "meta:acs_vintage", None, False, ""),
    # risk and climate
    ("haz_total", "Expected hazard loss / $100k", 7, "money2", "col:haz_total", "FEMA NRI",
     "meta:hazards_as_of", None, False, "Expected annual building loss per $100,000 of building value: "
     "flood, wildfire, wind, earthquake."),
    ("haz_flood", "Flood loss / $100k", 7, "money2", "col:haz_flood", "FEMA NRI", "meta:hazards_as_of",
     None, False, ""),
    ("haz_wildfire", "Wildfire loss / $100k", 7, "money2", "col:haz_wildfire", "FEMA NRI",
     "meta:hazards_as_of", None, False, ""),
    ("haz_wind", "Wind loss / $100k", 7, "money2", "col:haz_wind", "FEMA NRI", "meta:hazards_as_of",
     None, False, "Hurricane and tornado."),
    ("haz_quake", "Earthquake loss / $100k", 7, "money2", "col:haz_quake", "FEMA NRI",
     "meta:hazards_as_of", None, False, ""),
    ("clim_summer_high", "July average high", 7, "deg", "col:clim_summer_high", "NOAA normals",
     "1991-2020", None, False, ""),
    ("clim_winter_low", "January average low", 7, "deg", "col:clim_winter_low", "NOAA normals",
     "1991-2020", None, False, ""),
    ("clim_days_90", "Days at 90°F or more", 7, "num", "col:clim_days_90", "NOAA normals", "1991-2020",
     None, False, ""),
    ("clim_snow_in", "Snowfall / year", 7, "inch", "col:clim_snow_in", "NOAA normals", "1991-2020",
     None, False, ""),
]

METRICS = {m[0]: {"key": m[0], "label": m[1], "group": _GROUPS[m[2]], "fmt": m[3], "how": m[4],
                  "source": m[5], "as_of": m[6], "thin": m[7], "stale": m[8], "help": m[9],
                  "basis": m[4].startswith("inv:")}
           for m in _M}
DEFAULT_METRIC = "cap_rate"

# Rounding per format, so a 32k-value array stays small.
_ROUND = {"money": 0, "money2": 2, "pct": 1, "pct2": 2, "x": 1, "ratio": 2, "days": 0, "num": 0, "pts": 1,
          "num1": 1, "year": 0, "deg": 1, "inch": 0}


def registry() -> list[dict]:
    """What the page needs to list, label and format every metric."""
    return [{k: m[k] for k in ("key", "label", "group", "fmt", "source", "thin", "stale", "help", "basis")}
            for m in METRICS.values()]


# ── reading ─────────────────────────────────────────────────────────────

def _mtime(path: Path) -> float:
    try:
        return path.stat().st_mtime
    except OSError:
        return 0.0


# Inputs the underwriting reads, beyond the metric columns themselves.
_UW_INPUTS = ("zhvi", "zori", "zhvi_3br", "hud_rent_br3", "tax_rate_investor", "tax_rate_owner",
              "ins_landlord_300k", "ins_owner_300k", "acs_median_income")


def _needed_columns() -> list[str]:
    cols = {"zip", "lat", "lng", "state", "city", "county", "acs_flags", "rdc_quality_flag", *_UW_INPUTS}
    for m in METRICS.values():
        kind, field = m["how"].split(":", 1)
        if kind == "col":
            cols.add(field)
        if m["thin"]:
            cols.add(m["thin"])
        if (m["as_of"] or "").startswith("col:"):
            cols.add(m["as_of"][4:])
    return sorted(cols)


@lru_cache(maxsize=2)
def _table(path_str: str, mtime: float) -> tuple[dict, dict]:
    """{column: [value per ZIP, ordered by ZIP]} for the columns the map
    reads — columnar, so 32k ZIPs stay a few lists rather than 32k dicts —
    and the meta table."""
    p = Path(path_str)
    if not p.exists():
        return {}, {}
    c = sqlite3.connect(f"file:{p}?mode=ro", uri=True)
    try:
        have = {r[1] for r in c.execute("PRAGMA table_info(zip_profile)")}
        cols = [k for k in _needed_columns() if k in have]
        data = list(zip(*c.execute(f"SELECT {', '.join(cols)} FROM zip_profile ORDER BY zip")))
        meta = dict(c.execute("SELECT key, value FROM meta").fetchall())
    finally:
        c.close()
    table = {k: list(v) for k, v in zip(cols, data)} if data else {k: [] for k in cols}
    return table, meta


def load(path: Path = PROFILE_DB) -> tuple[dict, dict]:
    return _table(str(path), _mtime(path))


def base(path: Path = PROFILE_DB) -> dict:
    """The points: ZIP, position, state and a place name, in a fixed order
    every metric array follows."""
    t, meta = load(path)
    zips = t.get("zip", [])
    states = sorted({s for s in t.get("state", []) if s})
    idx = {s: i for i, s in enumerate(states)}
    return {
        "built_at": meta.get("built_at", ""),
        "n": len(zips),
        "zip": zips,
        "lat": [None if v is None else round(v, 4) for v in t.get("lat", [])],
        "lng": [None if v is None else round(v, 4) for v in t.get("lng", [])],
        "st": [idx.get(s, -1) for s in t.get("state", [])],
        "states": states,
        "place": [c or k or "" for c, k in zip(t.get("city", []), t.get("county", []))],
    }


# ── computed metrics ────────────────────────────────────────────────────

def _investor(row: dict, basis: str, rate: float | None) -> dict | None:
    b = BASES[basis]
    price, rent = row.get(b["price"]), row.get(b["rent"])
    if not price or not rent:
        return None
    inv_rate = None if rate is None else rate + U.DEFAULTS["investor_rate_premium"]
    return U.investor(price, rent, units=1, tax_rate_pct=row.get("tax_rate_investor"),
                      insurance_annual=RA.scaled_premium(row.get("ins_landlord_300k"), price),
                      rate_pct=inv_rate)


def _owner(row: dict, rate: float | None) -> dict | None:
    price = row.get("zhvi")
    if not price:
        return None
    return U.owner(price, tax_rate_pct=row.get("tax_rate_owner"),
                   insurance_annual=RA.scaled_premium(row.get("ins_owner_300k"), price),
                   rate_pct=rate, area_income=row.get("acs_median_income"),
                   market_rent=row.get("zori"))


# The underwriting fields a metric reads, kept per field rather than per ZIP.
# `implausible` / `rent_implausible` ride along: a year's rent over
# underwrite.IMPLAUSIBLE_GROSS_YIELD_PCT of the price is flagged on the map.
_INV_FIELDS = sorted({m["how"].split(":", 1)[1] for m in METRICS.values() if m["how"].startswith("inv:")}
                     | {"implausible"})
_OWN_FIELDS = sorted({m["how"].split(":", 1)[1] for m in METRICS.values() if m["how"].startswith("own:")}
                     | {"rent_implausible"})


@lru_cache(maxsize=8)
def _underwritten(path_str: str, mtime: float, rate: float | None, basis: str | None) -> dict:
    """{field: [value per ZIP]} for the investor (on `basis`) or, with
    basis None, the owner-occupant."""
    t, _ = _table(path_str, mtime)
    n = len(t.get("zip", []))
    fields = _OWN_FIELDS if basis is None else _INV_FIELDS
    out = {f: [None] * n for f in fields}
    inputs = {k: t[k] for k in _UW_INPUTS if k in t}
    for i in range(n):
        row = {k: v[i] for k, v in inputs.items()}
        u = _owner(row, rate) if basis is None else _investor(row, basis, rate)
        if u:
            for f in fields:
                out[f][i] = u.get(f)
    return out


def _coded(flags: str | None) -> dict:
    try:
        return json.loads(flags or "{}")
    except ValueError:
        return {}


def _as_of(m: dict, t: dict, meta: dict, rate_date: str) -> str:
    a = m["as_of"]
    if m["how"].startswith(("inv:", "own:", "aff:")):
        basis_month = month_label(meta.get("zhvi_last_month"))
        out = f"{basis_month} values; rate {rate_date}" if rate_date else f"{basis_month} values"
        return out + (" (against 2019)" if m["how"] in ("aff:ratio19", "aff:change_pts") else "")
    if a is None or a == "":
        return ""
    if a.startswith("meta:"):
        v = meta.get(a[5:], "")
        return month_label(v) if a.endswith("_as_of") and v else v
    if a.startswith("col:"):
        months = Counter(v for v in t.get(a[4:], []) if v)
        return month_label(months.most_common(1)[0][0]) if months else ""
    return a


def metric_values(key: str, basis: str = DEFAULT_BASIS, rate: float | None = None,
                  rate_date: str = "", path: Path = PROFILE_DB) -> dict | None:
    """One metric for every ZIP, in base() order. None for an unknown key.

    `thin` lists the indexes whose figure rests on under 10 listings or
    sales; `suspect`, underwritten figures whose year of rent is over
    underwrite.IMPLAUSIBLE_GROSS_YIELD_PCT of the price; `coded` maps an index to 'top'/'bottom' where the Census
    published a bound ("$250,000+"), whose value is then the bound;
    `volatile` lists indexes Realtor.com flagged for the month.
    """
    m = METRICS.get(key)
    if m is None:
        return None
    basis = basis if basis in BASES else DEFAULT_BASIS
    t, meta = load(path)
    n = len(t.get("zip", []))
    kind, field = m["how"].split(":", 1)
    nd = _ROUND[m["fmt"]]
    coded: dict = {}
    if kind == "col":
        values = list(t.get(field, [None] * n))
        if field.startswith("acs_"):
            for i, v in enumerate(values):
                if v is None and t["acs_flags"][i]:
                    flag = _coded(t["acs_flags"][i]).get(field)
                    if flag:
                        values[i] = flag["bound"]
                        coded[i] = flag["side"]
    suspect: list = []
    if kind == "aff":
        # The affordability page's own figures (affordability.py), so the map
        # and the page cannot disagree. A ZIP whose income the Census gives
        # only as "$250,000+" has a share of at most the figure: coded "upper".
        import affordability as AF
        b = AF.build(rate, path)
        by_zip = {}
        for z in b["zips"]:
            mm = z["m"] or {}
            by_zip[z["zip"]] = (mm.get(field), z["inp"].get("coded"), z["inp"].get("coded19"))
        values = []
        for i, zc in enumerate(t.get("zip", [])):
            v, c_now, c_19 = by_zip.get(zc, (None, None, None))
            values.append(v)
            if v is not None and ((field in ("ratio", "gap_pct") and c_now == "top")
                                  or (field == "ratio19" and c_19 == "top")):
                coded[i] = "upper"
    elif kind != "col":
        uw = _underwritten(str(path), _mtime(path), rate, None if kind == "own" else basis)
        values = uw[field]
        flag = uw["implausible"] if kind == "inv" else (uw["rent_implausible"] if field == "own_minus_rent_monthly"
                                                         else [False] * n)
        suspect = [i for i in range(n) if flag[i] and values[i] is not None]
    values = [None if v is None else (round(v) if nd == 0 else round(v, nd)) for v in values]
    thin_col = t.get(m["thin"]) if m["thin"] else None
    thin = [i for i in range(n) if thin_col and thin_col[i] and values[i] is not None]
    flags = t.get("rdc_quality_flag", [None] * n)
    volatile = ([i for i in range(n) if flags[i] == 1 and values[i] is not None]
                if m["source"] == "Realtor.com" else [])
    return {
        "key": key, "basis": basis if m["basis"] else None,
        "values": values, "thin": thin, "suspect": suspect, "coded": coded, "volatile": volatile,
        "n": sum(v is not None for v in values),
        "as_of": _as_of(m, t, meta, rate_date),
    }

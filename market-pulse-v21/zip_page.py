"""The ZIP page: one ZIP from data/zip_profile.db, assembled for a reader.

No score, no forecast. Every figure carries where it came from and the
month it describes; listing figures and sale figures stay separate; a
county figure is never shown as the ZIP's. The underwriting runs through
underwrite.py — the page's JavaScript only sends inputs and draws results,
so the arithmetic exists once.
"""
from __future__ import annotations

import json
import sqlite3
import statistics
import zlib
from datetime import date
from functools import lru_cache
from pathlib import Path

import re_assumptions as RA
import underwrite as U

DATA = Path(__file__).resolve().parent / "data"
PROFILE_DB = DATA / "zip_profile.db"
SERIES_DB = DATA / "zip_series.db"

# A source older than this is labelled stale wherever it appears.
STALE_MONTHS = 3

# ── reading ──────────────────────────────────────────────────────────

def _connect(path: Path):
    if not path.exists():
        return None
    c = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    c.row_factory = sqlite3.Row
    return c


def load_row(zip_code: str, path: Path = PROFILE_DB) -> dict | None:
    c = _connect(path)
    if c is None:
        return None
    try:
        r = c.execute("SELECT * FROM zip_profile WHERE zip = ?", (zip_code,)).fetchone()
        return dict(r) if r else None
    finally:
        c.close()


def load_meta(path: Path = PROFILE_DB) -> dict:
    c = _connect(path)
    if c is None:
        return {}
    try:
        return dict(c.execute("SELECT key, value FROM meta").fetchall())
    finally:
        c.close()


def load_series(zip_code: str, path: Path = SERIES_DB) -> dict:
    """{'zhvi': {'start', 'values' in USD}, 'zori': {...}} for one ZIP."""
    c = _connect(path)
    if c is None:
        return {}
    try:
        out = {}
        for r in c.execute("SELECT kind, start, data FROM series WHERE zip = ?", (zip_code,)):
            vals = json.loads(zlib.decompress(r["data"]))
            scale = 100 if r["kind"] == "zhvi" else 1
            out[r["kind"]] = {"start": r["start"],
                              "values": [None if v is None else v * scale for v in vals]}
        return out
    finally:
        c.close()


# ── comparisons: the ZIP against its county, state and the US ────────

COMPARE = (
    ("zhvi", "Typical home value (Zillow)", "money", None),
    ("zhvi_yoy_pct", "Home value, 1-yr change", "pct", None),
    ("zori", "Typical asking rent (Zillow)", "money", None),
    ("price_to_rent", "Price-to-rent", "x", None),
    ("price_to_income", "Price-to-income", "x", None),
    ("tax_rate_zip", "Property tax, effective", "pct3", None),
    ("rental_vacancy_pct", "Rental vacancy (Census)", "pct", None),
    ("acs_median_income", "Median household income", "money", None),
    ("rdc_dom", "Days on market (listings)", "days", "rdc_thin"),
    ("rdc_price_cut_pct", "Listings with a price cut", "pct", "rdc_thin"),
    ("rdc_pending_ratio", "Pending ÷ active listings", "ratio", "rdc_thin"),
    ("haz_total", "Expected hazard loss / $100k", "money", None),
)


@lru_cache(maxsize=2)
def _pools(path_str: str, mtime: float) -> dict:
    """{('county', fips) | ('state', st) | ('us', ''): {col: sorted values}}.
    Thin market figures stay out of the medians they would distort."""
    c = _connect(Path(path_str))
    pools: dict = {}
    if c is None:
        return pools
    try:
        cols = [k for k, *_ in COMPARE]
        thin = {k: t for k, _l, _f, t in COMPARE if t}
        for r in c.execute(f"SELECT county_fips, state, rdc_thin, {', '.join(cols)} FROM zip_profile"):
            for key in (("county", r["county_fips"]), ("state", r["state"]), ("us", "")):
                bucket = pools.setdefault(key, {})
                for col in cols:
                    v = r[col]
                    if v is None or (col in thin and r[thin[col]]):
                        continue
                    bucket.setdefault(col, []).append(v)
    finally:
        c.close()
    for bucket in pools.values():
        for col in bucket:
            bucket[col].sort()
    return pools


def _median_n(vals):
    return (statistics.median(vals), len(vals)) if vals else (None, 0)


def _pct_rank(vals, v):
    """Share of the pool at or below v, in percent."""
    if not vals or v is None:
        return None
    import bisect
    return round(bisect.bisect_right(vals, v) / len(vals) * 100)


def comparisons(row: dict, path: Path = PROFILE_DB) -> list[dict]:
    if not path.exists():
        return []
    pools = _pools(str(path), path.stat().st_mtime)
    out = []
    for col, label, fmt, thin in COMPARE:
        v = row.get(col)
        if thin and row.get(thin):
            v = None
        cty = pools.get(("county", row.get("county_fips")), {}).get(col, [])
        st = pools.get(("state", row.get("state")), {}).get(col, [])
        us = pools.get(("us", ""), {}).get(col, [])
        out.append({
            "col": col, "label": label, "fmt": fmt, "zip": v,
            "county": _median_n(cty), "state": _median_n(st), "us": _median_n(us),
            "state_pct_rank": _pct_rank(st, v),
        })
    return out


# ── labels a reader needs beside the numbers ─────────────────────────

def month_label(period: str | None) -> str:
    """'2026-05-31' or '2026-05' → 'May 2026'."""
    if not period or len(period) < 7:
        return ""
    names = ("Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec")
    try:
        return f"{names[int(period[5:7]) - 1]} {period[:4]}"
    except (ValueError, IndexError):
        return ""


def months_old(period: str | None, today: date | None = None) -> int | None:
    today = today or date.today()
    if not period or len(period) < 7:
        return None
    try:
        return (today.year - int(period[:4])) * 12 + (today.month - int(period[5:7]))
    except ValueError:
        return None


def placeholder(row: dict, col: str) -> str | None:
    """'$250,000+' / 'under $2,500' for a Census top/bottom-coded median."""
    flags = json.loads(row.get("acs_flags") or "{}")
    f = flags.get(col)
    if not f:
        return None
    b = f.get("bound")
    if col == "acs_year_built":
        return f"{int(b)} or earlier" if f.get("side") == "bottom" else f"{int(b)} or later"
    if col == "acs_rent_pct_income":
        return f"{b:g}%+" if f.get("side") == "top" else f"under {b:g}%"
    shown = int(round(b)) - 1 if f.get("side") == "top" else int(round(b)) + 1
    return f"${shown:,}+" if f.get("side") == "top" else f"under ${shown:,}"


# ── underwriting defaults and the call into underwrite.py ────────────

def rent_options(row: dict) -> list[dict]:
    """Every honest price/rent pairing this ZIP can support.

    The pairing matters more than either number: a typical-home value
    against a voucher rent, or a 3-bed rent against an all-homes value,
    is two different properties. Each option names both halves."""
    opts = []
    if row.get("zori") and row.get("zhvi"):
        opts.append({"key": "zillow", "price": row["zhvi"], "rent": row["zori"],
                     "label": "Zillow: typical home and typical asking rent",
                     "price_note": f"Zillow typical home value, {month_label(row.get('zhvi_month'))}",
                     "rent_note": f"Zillow asking rent, all unit types, {month_label(row.get('zori_month'))}"})
    for n in (1, 2, 3, 4):
        # Price: Zillow's n-bed value, else its typical value, else — for a
        # ZIP Zillow does not cover — the Census median owner value.
        if row.get(f"zhvi_{n}br"):
            price, price_note = row[f"zhvi_{n}br"], f"Zillow {n}-bed home value"
        elif row.get("zhvi"):
            price, price_note = row["zhvi"], "Zillow typical home value (no bedroom-specific value here)"
        else:
            price = row.get("acs_median_value")
            price_note = "Census median owner value (Zillow does not cover this ZIP)"
        hud = row.get(f"hud_rent_br{n}")
        if hud and price:
            tier = "ZIP-level Small Area FMR" if row.get("hud_rent_tier") == "safmr" else "county FMR"
            opts.append({"key": f"hud_{n}", "price": price, "rent": hud,
                         "label": f"HUD {n}-bed rent with a {n}-bed value",
                         "price_note": price_note,
                         "rent_note": f"HUD {tier}, {n}-bed: 40th-percentile rent incl. utilities"})
        acs = row.get(f"acs_rent_br{n}")
        if acs and price:
            opts.append({"key": f"acs_{n}", "price": price, "rent": acs,
                         "label": f"Census {n}-bed rent with a {n}-bed value",
                         "price_note": price_note,
                         "rent_note": "Census median gross rent, existing tenants (lags the market)"})
    return opts


def default_option(opts: list[dict]) -> dict | None:
    for key in ("zillow", "hud_3", "acs_3", "hud_2", "acs_2"):
        for o in opts:
            if o["key"] == key:
                return o
    return opts[0] if opts else None


def _f(params: dict, key: str, default):
    v = params.get(key)
    if v in (None, ""):
        return default
    try:
        return float(v)
    except (TypeError, ValueError):
        return default


def underwrite_zip(row: dict, params: dict, mortgage_rate: float | None) -> dict:
    """Defaults for this ZIP, overridden by any of `params`, run through
    underwrite.py for both readers. Returns the inputs used and results."""
    opts = rent_options(row)
    chosen = next((o for o in opts if o["key"] == params.get("pairing")), None) or default_option(opts)
    price = _f(params, "price", chosen["price"] if chosen else row.get("zhvi") or row.get("acs_median_value"))
    rent = _f(params, "rent", chosen["rent"] if chosen else None)
    units = int(_f(params, "units", 1) or 1)
    units = min(4, max(1, units))
    inv_rate_default = (None if mortgage_rate is None
                        else round(mortgage_rate + U.DEFAULTS["investor_rate_premium"], 3))
    inv_ins_default = RA.scaled_premium(row.get("ins_landlord_300k"), price)
    own_ins_default = RA.scaled_premium(row.get("ins_owner_300k"), price)
    used = {
        "pairing": chosen["key"] if chosen else None,
        "price": price, "rent": rent, "units": units,
        "tax": _f(params, "tax", row.get("tax_rate_investor")),
        "ins": _f(params, "ins", inv_ins_default),
        "hoa": _f(params, "hoa", 0.0), "util": _f(params, "util", 0.0),
        "vac": _f(params, "vac", U.DEFAULTS["vacancy_pct"]),
        "mgmt": _f(params, "mgmt", U.DEFAULTS["management_pct"]),
        "maint": _f(params, "maint", U.DEFAULTS["maintenance_pct"]),
        "capex": _f(params, "capex", U.DEFAULTS["capex_pct"]),
        "rate": _f(params, "rate", inv_rate_default),
        "down": _f(params, "down", U.DEFAULTS["investor_down_pct"]),
        "closing": _f(params, "closing", U.DEFAULTS["closing_cost_pct"]),
        "o_down": _f(params, "o_down", U.DEFAULTS["owner_down_pct"]),
        "o_rate": _f(params, "o_rate", mortgage_rate),
        "o_tax": _f(params, "o_tax", row.get("tax_rate_owner")),
        "o_ins": _f(params, "o_ins", own_ins_default),
        "o_hoa": _f(params, "o_hoa", _f(params, "hoa", 0.0)),
        "o_pmi": _f(params, "o_pmi", U.DEFAULTS["pmi_annual_pct"]),
        "o_debt": _f(params, "o_debt", 0.0),
    }
    inv = U.investor(used["price"], used["rent"], units=units, tax_rate_pct=used["tax"],
                     insurance_annual=used["ins"], hoa_monthly=used["hoa"],
                     utilities_monthly=used["util"], vacancy_pct=used["vac"],
                     management_pct=used["mgmt"], maintenance_pct=used["maint"],
                     capex_pct=used["capex"], rate_pct=used["rate"], down_pct=used["down"],
                     closing_cost_pct=used["closing"])
    own = U.owner(used["price"], tax_rate_pct=used["o_tax"], insurance_annual=used["o_ins"],
                  hoa_monthly=used["o_hoa"], rate_pct=used["o_rate"], down_pct=used["o_down"],
                  pmi_annual_pct=used["o_pmi"], other_debt_monthly=used["o_debt"],
                  closing_cost_pct=used["closing"],
                  area_income=row.get("acs_median_income"),
                  market_rent=used["rent"])
    return {"used": used, "options": opts, "chosen": chosen, "investor": inv, "owner": own,
            "notes": {
                "tax": row.get("tax_basis_investor"), "o_tax": row.get("tax_basis_owner"),
                "ins": "State landlord (DP-3) average at a $300k dwelling, scaled to price within 0.6-2x",
                "o_ins": "State homeowner (HO-3) average, scaled to price within 0.6-2x",
                "rate": "Freddie Mac 30-yr average + 0.75 for an investment loan",
                "o_rate": "Freddie Mac 30-yr average (owner-occupied)",
                **{k: U.PROVENANCE[v] for k, v in (("vac", "vacancy_pct"), ("mgmt", "management_pct"),
                                                    ("maint", "maintenance_pct"), ("capex", "capex_pct"),
                                                    ("down", "investor_down_pct"),
                                                    ("closing", "closing_cost_pct"),
                                                    ("o_down", "owner_down_pct"),
                                                    ("o_pmi", "pmi_annual_pct"))},
            }}


def build_page(zip_code: str, mortgage_rate: float | None, rate_date: str = "",
               profile: Path = PROFILE_DB, series: Path = SERIES_DB,
               today: date | None = None) -> dict | None:
    """Everything the template needs, or None for an unknown ZIP."""
    row = load_row(zip_code, profile)
    if row is None:
        return None
    meta = load_meta(profile)
    today = today or date.today()
    rf_age = months_old(row.get("rf_period_end"), today)
    return {
        "row": row,
        "meta": meta,
        "series": load_series(zip_code, series),
        "compare": comparisons(row, profile),
        "uw": underwrite_zip(row, {}, mortgage_rate),
        "mortgage_rate": mortgage_rate, "rate_date": rate_date,
        "labels": {
            "zhvi": month_label(row.get("zhvi_month")),
            "zori": month_label(row.get("zori_month")),
            "rdc": month_label(row.get("rdc_month")),
            "rf": month_label(row.get("rf_period_end")),
            "rf_begin": month_label(row.get("rf_period_begin")),
            "zhvi_peak": month_label(row.get("zhvi_peak_month")),
            "rf_stale": rf_age is not None and rf_age > STALE_MONTHS,
            "rf_age": rf_age,
            "acs": meta.get("acs_vintage", ""),
        },
        "placeholders": {c: placeholder(row, c) for c in (
            "acs_median_income", "acs_owner_income", "acs_renter_income", "acs_median_value",
            "acs_median_taxes", "acs_gross_rent", "acs_rent_pct_income",
            "acs_owner_cost_mortgaged", "acs_year_built",
            *(f"acs_rent_br{b}" for b in range(6)))},
        "hazard_medians": _hazard_medians(),
    }


@lru_cache(maxsize=1)
def _hazard_medians() -> dict:
    try:
        return (json.loads((DATA / "zip_hazards.json").read_text()).get("_meta") or {}).get("median", {})
    except (OSError, ValueError):
        return {}

"""Housing affordability: what it takes for the median household to buy the
typical home, today against a fixed 2019 baseline.

THE MEASURE. Payment-to-income: the monthly payment on the typical home
(Zillow ZHVI) — principal and interest at 20% down, property tax and
homeowner insurance, the ZIP page's own arithmetic (underwrite.py) — times
twelve, over the median household income. 30% is the line: the HUD
cost-burden threshold, and the one the Atlanta Fed's affordability monitor
uses. "Affordable price" is the price whose payment takes exactly 30%.

TODAY AND 2019, EACH WITH ITS OWN FIGURES (scripts/affordability_inputs.py):

                today                                   2019
  price         ZHVI, latest month                      ZHVI, 2019 average
  rate          Freddie Mac 30-yr, latest week          Freddie Mac 30-yr, 2019 average
  income        Census 2020-24 median, brought to       Census 2015-19 median,
                today's dollars by CPI (no real          2019 dollars
                growth assumed since 2024)
  tax           the ZIP's owner rate                    the same rate
  insurance     state HO-3 average scaled to price      the same, deflated to 2019 by CPI

The baseline never rolls forward, so "against 2019" means the same thing
next year. The change since 2019 is split into prices, rates, incomes and
insurance by Shapley value — each factor's average effect over every order
of change — so the four parts add up to the whole change exactly.

STATES AND THE NATION ARE A TYPICAL HOUSEHOLD, NOT AN AVERAGE OF ZIPS: the
household-weighted median price, income and tax rate across the same ZIPs
at both ends (only ZIPs with every input in both years), run through the
same arithmetic. Like-for-like by construction: the old /fair-value page
compared a statewide index today with a median-ZIP figure then.
"""
from __future__ import annotations

import json
import math
import sqlite3
from functools import lru_cache
from pathlib import Path

import re_assumptions as RA
import underwrite as U

DATA = Path(__file__).resolve().parent / "data"
PROFILE_DB = DATA / "zip_profile.db"

SHARE = 30.0                      # % of gross income: the affordability line
DOWN_PCT = U.DEFAULTS["owner_down_pct"]
FACTORS = ("price", "rate", "income", "insurance")

COLUMNS = ("zip", "state", "city", "county", "households", "zhvi", "zhvi_month", "zhvi_2019",
           "acs_median_income", "acs19_median_income", "acs19_income_coded", "acs_flags",
           "tax_rate_owner", "ins_owner_300k")


# ── the arithmetic, one household and one home ──────────────────────────

@lru_cache(maxsize=64)
def _pi_per_dollar(rate: float) -> float:
    """Monthly P&I per $1 of price at 20% down (underwrite.monthly_payment)."""
    return U.monthly_payment(1 - DOWN_PCT / 100.0, rate)


def _premium(ins_300k: float, price: float) -> float:
    """re_assumptions.scaled_premium, unrounded (it is summed, not shown)."""
    lo, hi = RA.INS_SCALE_BOUNDS
    return ins_300k * min(hi, max(lo, price / RA.INS_BASE_DWELLING))


def payment(price: float, rate: float, tax_rate: float, ins_300k: float,
            ins_level: float = 1.0) -> float:
    """Monthly principal and interest (20% down, 30 years), property tax and
    homeowner insurance — underwrite.owner's monthly_total at its defaults,
    with no PMI (the loan is at 80%). `ins_level` scales the premium (the
    CPI deflator for 2019)."""
    return (price * _pi_per_dollar(rate) + price * tax_rate / 1200.0
            + _premium(ins_300k, price) * ins_level / 12.0)


def ratio(price, rate, income, tax_rate, ins_300k, ins_level=1.0) -> float | None:
    """Payment-to-income, percent."""
    if not (price and rate is not None and income and tax_rate is not None and ins_300k):
        return None
    return payment(price, rate, tax_rate, ins_300k, ins_level) * 1200.0 / income


def affordable_price(income, rate, tax_rate, ins_300k, share: float = SHARE) -> float | None:
    """The price whose payment takes exactly `share`% of `income`.

    The payment is linear in price within each of the premium's three
    scaling bands (floor, proportional, cap), so each band is solved in
    closed form and the one whose answer falls inside it is kept."""
    if not (income and rate is not None and tax_rate is not None and ins_300k):
        return None
    target = income * share / 100.0 / 12.0
    pi_per = _pi_per_dollar(rate)                                   # P&I per $1 of price
    tax_per = tax_rate / 100.0 / 12.0
    lo, hi = RA.INS_SCALE_BOUNDS
    base, dwelling = float(ins_300k), float(RA.INS_BASE_DWELLING)
    bands = (
        (0.0, lo * dwelling, pi_per + tax_per, base * lo / 12.0),                     # premium floor
        (lo * dwelling, hi * dwelling, pi_per + tax_per + base / dwelling / 12.0, 0.0),  # proportional
        (hi * dwelling, math.inf, pi_per + tax_per, base * hi / 12.0),                # premium cap
    )
    for start, end, slope, fixed in bands:
        p = (target - fixed) / slope
        if start <= p <= end:
            return p
    return None


@lru_cache(maxsize=8)
def _shapley_plan(n: int) -> tuple:
    """For each factor i: ((mask without i, mask with i, weight), ...) over
    every subset of the other factors — computed once per n."""
    plan = []
    for i in range(n):
        terms = []
        for mask in range(1 << n):
            if mask & (1 << i):
                continue
            size = bin(mask).count("1")
            w = math.factorial(size) * math.factorial(n - size - 1) / math.factorial(n)
            terms.append((mask, mask | (1 << i), w))
        plan.append(tuple(terms))
    return tuple(plan)


def shapley_from_values(keys: list, value: list) -> dict:
    """Shapley contributions from f evaluated on every combination of changed
    factors: value[mask], where bit j of mask means keys[j] is at its new
    level."""
    return {k: sum(w * (value[hi] - value[lo]) for lo, hi, w in terms)
            for k, terms in zip(keys, _shapley_plan(len(keys)))}


def shapley(f, then: dict, now: dict) -> dict:
    """Each factor's Shapley contribution to f(now) - f(then): its average
    marginal effect over every order in which the factors could change.
    The contributions sum to the total change exactly. f is evaluated once
    per combination of changed factors (2^n), then reused."""
    keys = list(then)
    value = [f({k: (now[k] if mask & (1 << j) else then[k]) for j, k in enumerate(keys)})
             for mask in range(1 << len(keys))]
    return shapley_from_values(keys, value)


def measure(inp: dict, macro: dict, rate_now: float | None, const: dict | None = None) -> dict | None:
    """Every figure for one household/home, from the inputs below, or None
    when an input is missing. inp: price, price19, income (Census 2020-24
    dollars of the ACS end year), income19, tax_rate, ins_300k."""
    c = const or macro_constants(macro)
    if not c or rate_now is None:
        return None
    if not all(inp.get(k) for k in ("price", "income", "ins_300k")) or inp.get("tax_rate") is None:
        return None
    y_now = inp["income"] * c["income_to_today"]
    out = {
        "price": inp["price"], "income_today": y_now, "rate": rate_now,
        "ratio": ratio(inp["price"], rate_now, y_now, inp["tax_rate"], inp["ins_300k"]),
        "payment": payment(inp["price"], rate_now, inp["tax_rate"], inp["ins_300k"]),
        "affordable": affordable_price(y_now, rate_now, inp["tax_rate"], inp["ins_300k"]),
    }
    out["gap_pct"] = (inp["price"] / out["affordable"] - 1) * 100 if out["affordable"] else None
    if inp.get("price19") and inp.get("income19"):
        # The ratio at every combination of the four factors at their 2019 or
        # today's level (bit 0 price, 1 rate, 2 income, 3 insurance) — plain
        # arithmetic, since 32k ZIPs run through this on a page load.
        prices, rates = (inp["price19"], inp["price"]), (c["rate19"], rate_now)
        incomes, levels = (inp["income19"], y_now), (c["ins_level19"], 1.0)
        tax = inp["tax_rate"] / 1200.0
        prem = [_premium(inp["ins_300k"], pr) / 12.0 for pr in prices]
        pay = {(i, j, k): prices[i] * (_pi_per_dollar(rates[j]) + tax) + prem[i] * levels[k]
               for i in (0, 1) for j in (0, 1) for k in (0, 1)}
        value = [pay[(m & 1, m >> 1 & 1, m >> 3 & 1)] * 1200.0 / incomes[m >> 2 & 1] for m in range(16)]
        out["ratio19"] = value[0]
        out["payment19"] = pay[(0, 0, 0)]
        out["change_pts"] = out["ratio"] - out["ratio19"]
        out["parts"] = shapley_from_values(list(FACTORS), value)
    return out


def macro_constants(macro: dict | None) -> dict | None:
    """The fixed constants the page needs, from zip_profile meta's
    afford_macro, or None if any is missing."""
    if not macro:
        return None
    cpi = macro.get("cpi") or {}
    end = next((k for k in cpi if k.isdigit() and k != str(macro.get("baseline_year"))), None)
    try:
        latest, c19, cend = float(cpi["latest"]), float(cpi[str(macro["baseline_year"])]), float(cpi[end])
        rate19 = float(macro["pmms"][str(macro["baseline_year"])])
    except (KeyError, TypeError, ValueError):
        return None
    return {"income_to_today": latest / cend, "ins_level19": c19 / latest, "rate19": rate19,
            "acs_end_year": int(end), "cpi_latest_month": cpi.get("latest_month"),
            "baseline_year": int(macro["baseline_year"])}


# ── reading the profile ─────────────────────────────────────────────────

def _mtime(path: Path) -> float:
    try:
        return path.stat().st_mtime
    except OSError:
        return 0.0


@lru_cache(maxsize=2)
def _load(path_str: str, mtime: float) -> tuple[list[dict], dict]:
    p = Path(path_str)
    if not p.exists():
        return [], {}
    c = sqlite3.connect(f"file:{p}?mode=ro", uri=True)
    c.row_factory = sqlite3.Row
    try:
        have = {r[1] for r in c.execute("PRAGMA table_info(zip_profile)")}
        cols = [k for k in COLUMNS if k in have]
        rows = [dict(r) for r in c.execute(f"SELECT {', '.join(cols)} FROM zip_profile ORDER BY zip")]
        meta = dict(c.execute("SELECT key, value FROM meta").fetchall())
    finally:
        c.close()
    return rows, meta


def _income(row: dict) -> tuple[float | None, str | None]:
    """Today's Census income, or its published bound with the side it is
    coded on ("$250,000+")."""
    v = row.get("acs_median_income")
    if v:
        return v, None
    try:
        flag = json.loads(row.get("acs_flags") or "{}").get("acs_median_income")
    except ValueError:
        flag = None
    return (flag["bound"], flag["side"]) if flag else (None, None)


def inputs(row: dict) -> dict:
    inc, coded = _income(row)
    return {"price": row.get("zhvi"), "price19": row.get("zhvi_2019"), "income": inc,
            "income19": row.get("acs19_median_income"), "tax_rate": row.get("tax_rate_owner"),
            "ins_300k": row.get("ins_owner_300k"), "coded": coded,
            "coded19": row.get("acs19_income_coded")}


def weighted_median(pairs: list[tuple[float, float]]) -> float | None:
    """The value at which half the weight lies on either side."""
    pairs = sorted((v, w) for v, w in pairs if v is not None and w)
    total = sum(w for _, w in pairs)
    if not total:
        return None
    acc = 0.0
    for v, w in pairs:
        acc += w
        if acc >= total / 2:
            return v
    return pairs[-1][0]


def typical(rows: list[dict]) -> dict | None:
    """The typical household of a set of ZIPs: household-weighted medians
    of each input, over the ZIPs that have every input in both years."""
    full = [r for r in rows if r["households"] and all(r["inp"].get(k) for k in
            ("price", "price19", "income", "income19", "ins_300k"))
            and r["inp"].get("tax_rate") is not None]
    if not full:
        return None
    med = {k: weighted_median([(r["inp"][k], r["households"]) for r in full])
           for k in ("price", "price19", "income", "income19", "tax_rate", "ins_300k")}
    return {**med, "n_zips": len(full), "households": sum(r["households"] for r in full)}


def build(rate_now: float | None, path: Path = PROFILE_DB) -> dict:
    """Everything the page shows: national and state figures, and per-ZIP
    rows by state. Cached per profile build and mortgage rate."""
    return _build(str(path), _mtime(path), rate_now)


@lru_cache(maxsize=4)
def _build(path_str: str, mtime: float, rate_now: float | None) -> dict:
    rows, meta = _load(path_str, mtime)
    macro = json.loads(meta.get("afford_macro") or "null")
    const = macro_constants(macro)
    zips = []
    for r in rows:
        inp = inputs(r)
        m = measure(inp, macro, rate_now, const) if const else None
        zips.append({"zip": r["zip"], "state": r["state"], "place": r.get("city") or r.get("county") or "",
                     "county": r.get("county") or "", "households": r.get("households") or 0,
                     "inp": inp, "m": m})
    by_state: dict[str, list] = {}
    for z in zips:
        by_state.setdefault(z["state"], []).append(z)
    states = {}
    for st, zs in by_state.items():
        t = typical(zs)
        all_hh = sum(z["households"] for z in zs)
        states[st] = {"typical": t, "m": measure(t, macro, rate_now) if t and const else None,
                      "households_all": all_hh,
                      "coverage_pct": round(t["households"] / all_hh * 100, 1) if t and all_hh else None}
    t = typical(zips)
    nation = {"typical": t, "m": measure(t, macro, rate_now) if t and const else None,
              "households_all": sum(z["households"] for z in zips)}
    nation["coverage_pct"] = (round(t["households"] / nation["households_all"] * 100, 1)
                              if t and nation["households_all"] else None)
    return {"meta": meta, "macro": macro, "const": const, "rate_now": rate_now,
            "zips": zips, "states": states, "nation": nation}


def income_factor(path: Path = PROFILE_DB) -> float | None:
    """CPI multiplier from the Census income's dollars to today's, or None."""
    _rows, meta = _load(str(path), _mtime(path))
    c = macro_constants(json.loads(meta.get("afford_macro") or "null"))
    return c["income_to_today"] if c else None


def zip_values(field: str, rate_now: float | None, path: Path = PROFILE_DB) -> dict:
    """{zip: value} of one per-ZIP figure, for the ZIP map."""
    out = {}
    for z in build(rate_now, path)["zips"]:
        m = z["m"]
        if not m:
            continue
        v = (m.get("parts") or {}).get(field[6:]) if field.startswith("parts_") else m.get(field)
        if v is not None:
            out[z["zip"]] = v
    return out


# ── the page ────────────────────────────────────────────────────────────

def _figures(m: dict | None, t: dict | None) -> dict | None:
    if not m:
        return None
    t = t or {}
    return {"ratio": m["ratio"], "ratio19": m.get("ratio19"), "change": m.get("change_pts"),
            "parts": m.get("parts"), "price": m["price"], "price19": t.get("price19"),
            "income": m["income_today"], "income19": t.get("income19"), "affordable": m["affordable"],
            "gap": m["gap_pct"], "payment": m["payment"], "payment19": m.get("payment19"),
            "tax_rate": t.get("tax_rate"), "rate": m["rate"]}


def page(rate_now: float | None, rate_date: str, state: str, state_info: dict,
         path: Path = PROFILE_DB) -> dict:
    """The template's context: the nation, every state, and the picked
    state's ZIPs. `state_info` = {code: {"name", "fips"}}."""
    b = build(rate_now, path)
    st = (state or "").strip().upper()
    if st not in b["states"]:
        st = ""
    rows = []
    for code, s_ in b["states"].items():
        f = _figures(s_["m"], s_["typical"])
        if not f:
            continue
        info = state_info.get(code, {})
        rows.append({"code": code, "name": info.get("name", code), "fips": info.get("fips"),
                     "coverage": s_["coverage_pct"], "n_zips": s_["typical"]["n_zips"], **f})
    rows.sort(key=lambda r: -r["ratio"])
    zip_rows = []
    for z in b["zips"] if st else ():
        m = z["m"]
        if z["state"] != st or not m:
            continue
        zip_rows.append({
            "zip": z["zip"], "place": z["place"], "county": z["county"], "hh": z["households"],
            "ratio": round(m["ratio"], 1),
            "ratio19": None if m.get("ratio19") is None else round(m["ratio19"], 1),
            "change": None if m.get("change_pts") is None else round(m["change_pts"], 1),
            "price": round(m["price"]), "affordable": round(m["affordable"]) if m["affordable"] else None,
            "gap": None if m["gap_pct"] is None else round(m["gap_pct"]),
            "coded": z["inp"].get("coded"), "coded19": z["inp"].get("coded19")})
    nation = b["nation"]
    return {"nation": _figures(nation["m"], nation["typical"]), "nation_cov": nation["coverage_pct"],
            "nation_hh": (nation["typical"] or {}).get("households"), "rows": rows,
            "picked": next((r for r in rows if r["code"] == st), None), "state": st,
            "zip_rows": zip_rows, "const": b["const"], "macro": b["macro"], "meta": b["meta"],
            "rate_date": rate_date, "share": SHARE, "down_pct": DOWN_PCT}

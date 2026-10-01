"""Market conditions by state: what listings are doing now, against the same
month a year earlier.

Realtor.com's monthly state and national core metrics (zip_profile.db,
table state_market — 25 months per geo, written by build_zip_profile).
Listings only: active, new and pending listings, median days on market,
the share of listings with a price cut, and the list price.

WHAT REPLACED WHAT. This page used to rank states by a 0-100 "Market
Climate" blend of four Redfin figures from a tracker that stopped in May
2026, read in October as "latest". Now there is no blend: each measure
stands alone, with its month, and the year-over-year change compares the
same month a year apart, so the spring rush and the autumn slowdown cancel
out rather than masquerading as a trend. Redfin's sale-to-list ratio and
months of supply — sales measures Realtor.com does not publish — stay in a
column group dated May 2026 and marked frozen.

Every change is computed here from the two months themselves (the exact
month twelve months earlier, or none): counts and prices as a percent
change, shares and ratios in points, days on market in days.
"""
from __future__ import annotations

import json
import sqlite3
from functools import lru_cache
from pathlib import Path

DATA = Path(__file__).resolve().parent / "data"
PROFILE_DB = DATA / "zip_profile.db"
REDFIN_JSON = DATA / "redfin_overrides.json"

FIELDS = ("active", "new", "pending", "pending_ratio", "dom", "price_cut_pct", "list_price", "list_ppsf",
          "quality_flag")

# key, label, field, kind, fmt, reading — "kind" is how the year-over-year
# change is measured: pct (percent change), pts (difference in percentage
# points), diff (difference in the measure's own unit).
MEASURES = (
    ("dom", "Median days on market", "dom", "diff", "days",
     "How long the typical listing has been for sale. Longer: buyers have more time and room to negotiate."),
    ("price_cut_pct", "Listings with a price cut", "price_cut_pct", "pts", "pct",
     "Share of active listings whose price was cut this month."),
    ("active", "Active listings", "active", "pct", "num",
     "Homes for sale. Rising inventory gives buyers more choice."),
    ("new", "New listings this month", "new", "pct", "num",
     "Homes coming to market — the supply pipeline."),
    ("pending_ratio", "Pending ÷ active", "pending_ratio", "diff", "ratio",
     "Listings under contract per active listing. Higher: homes are going under contract faster."),
    ("list_price", "Median list price", "list_price", "pct", "money",
     "What sellers are asking — not what homes sell for."),
)
MEASURE_KEYS = tuple(m[0] for m in MEASURES)


def month_add(ym: str, n: int) -> str:
    y, m = int(ym[:4]), int(ym[5:7])
    t = y * 12 + (m - 1) + n
    return f"{t // 12:04d}-{t % 12 + 1:02d}"


def month_label(ym: str | None) -> str:
    if not ym:
        return ""
    names = "Jan Feb Mar Apr May Jun Jul Aug Sep Oct Nov Dec".split()
    return f"{names[int(ym[5:7]) - 1]} {ym[:4]}"


def change(now, then, kind: str):
    """The year-over-year change of one measure, or None without both months."""
    if now is None or then is None:
        return None
    if kind == "pct":
        return round((now / then - 1) * 100, 1) if then else None
    if kind == "pts":
        return round(now - then, 1)
    return round(now - then, 4)


def _mtime(path: Path) -> float:
    try:
        return path.stat().st_mtime
    except OSError:
        return 0.0


@lru_cache(maxsize=2)
def _load(path_str: str, mtime: float) -> tuple[dict, dict]:
    """({geo: {month: record}}, meta)."""
    p = Path(path_str)
    if not p.exists():
        return {}, {}
    c = sqlite3.connect(f"file:{p}?mode=ro", uri=True)
    c.row_factory = sqlite3.Row
    try:
        meta = dict(c.execute("SELECT key, value FROM meta").fetchall())
        try:
            rows = [dict(r) for r in c.execute("SELECT * FROM state_market")]
        except sqlite3.Error:
            rows = []
    finally:
        c.close()
    out: dict = {}
    for r in rows:
        out.setdefault(r["geo"], {})[r["month"]] = r
    return out, meta


def geo_summary(series: dict) -> dict | None:
    """Latest month, the same month a year earlier, and each measure's level
    and change. None for a geo with no data."""
    if not series:
        return None
    month = max(series)
    now, then = series[month], series.get(month_add(month, -12))
    out = {"month": month, "year_ago_month": month_add(month, -12) if then else None,
           "month_label": month_label(month), "year_ago_label": month_label(month_add(month, -12)),
           "quality_flag": now.get("quality_flag")}
    for key, _label, field, kind, _fmt, _help in MEASURES:
        out[key] = now.get(field)
        out[key + "_chg"] = change(now.get(field), (then or {}).get(field), kind)
    return out


def chart_series(series: dict, months: int = 13) -> dict:
    """The last `months` months and the same months a year earlier, per
    measure, for a this-year-against-last-year chart."""
    if not series:
        return {}
    last = max(series)
    ms = [month_add(last, -i) for i in range(months - 1, -1, -1)]
    prior = [month_add(m, -12) for m in ms]
    return {"months": ms,
            **{k: {"now": [(series.get(m) or {}).get(f) for m in ms],
                   "prior": [(series.get(m) or {}).get(f) for m in prior]}
               for k, _l, f, *_ in MEASURES}}


def load_redfin(path: Path = REDFIN_JSON) -> tuple[dict, str | None]:
    """Redfin's last state figures — ({code: {"sale_to_list_pct",
    "months_of_supply"}}, period end) — from data/redfin_overrides.json.
    A missing or unreadable file is ({}, None): the columns show dashes."""
    try:
        payload = json.loads(Path(path).read_text())
    except (OSError, ValueError):
        return {}, None
    if not isinstance(payload, dict):
        return {}, None
    period = (payload.get("_meta") or {}).get("primary_period_end")
    out = {k: {"sale_to_list_pct": v.get("sale_to_list_pct"), "months_of_supply": v.get("months_of_supply")}
           for k, v in (payload.get("overrides") or {}).items() if isinstance(v, dict)}
    return out, period


def page(state_info: dict, redfin: dict, redfin_period_end: str | None,
         path: Path = PROFILE_DB) -> dict:
    """The template's context. `state_info` = {code: {"name", "fips"}};
    `redfin` = {code: {"sale_to_list_pct", "months_of_supply"}} (May 2026)."""
    data, meta = _load(str(path), _mtime(path))
    rows = []
    for code, series in sorted(data.items()):
        if code == "US":
            continue
        s = geo_summary(series)
        if not s:
            continue
        info = state_info.get(code, {})
        rf = redfin.get(code) or {}
        rows.append({"code": code, "name": info.get("name", code), "fips": info.get("fips"), **s,
                     "rf_stl": rf.get("sale_to_list_pct"), "rf_supply": rf.get("months_of_supply")})
    rows.sort(key=lambda r: r["name"])
    us = geo_summary(data.get("US", {}))
    latest = max((r["month"] for r in rows), default=None)
    return {
        "us": us, "rows": rows,
        "measures": [{"key": k, "label": lab, "kind": kind, "fmt": fmt, "help": h}
                     for k, lab, _f, kind, fmt, h in MEASURES],
        "charts": {g: chart_series(s) for g, s in data.items()},
        "month": latest, "month_label": month_label(latest),
        "year_ago_label": month_label(month_add(latest, -12)) if latest else "",
        "stale_months": [r["code"] for r in rows if r["month"] != latest],
        "carried": meta.get("state_market_carried") == "1",
        "redfin_period_end": redfin_period_end,
        "redfin_label": month_label((redfin_period_end or "")[:7]),
    }

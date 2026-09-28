"""Dividend Aristocrats value screen — /aristocrats.

The thesis: an aristocrat's dividend is reliable and growing, so its
YIELD mean-reverts (the Geraldine Weiss method). When a 50-year raiser
yields well above its own 5-year median, the market is offering a
historically cheap entry on a proven compounder — a far better "cheap"
signal than a 52-week low, which is momentum (not valuation), goes
empty in rallies, floods in crashes, and is exactly how people ended
up holding Leggett & Platt into its cut after 52 straight years.

Three filters, in order:
  1. VALUE   — current yield ≥ 20% above the stock's own 5-yr median.
  2. RETURN  — Chowder rule: yield + 5-yr dividend CAGR ≥ 12
               (≥ 8 for utilities / REITs / midstream, per the
               standard rule). Encodes the +12% total-return target.
  3. SAFETY  — hide likely cut candidates on payout or net debt/EBITDA,
               computed from SEC filings by the monthly refresh (see
               refresh_aristocrats.sec_safety). Elevated-but-passing names
               keep a warning badge instead (Altria stays visible).

BUY = passes all three, with BOTH safety figures actually measured. A
name that is cheap and clears Chowder but whose payout or leverage could
not be measured is UNMEASURED, never BUY: "unknown" used to read as safe,
and four of five BUYs had neither figure. The list is designed to be
SHORT — usually a handful of names, occasionally zero. Zero is a signal too.

Data model (same pattern as country_data.py):
  • UNIVERSE below carries the slow-moving facts — ticker, streak,
    sector, country, Schwab access — plus a seed snapshot of metrics.
    Seed yields/payouts for ~2 dozen names come from dividend.com as
    of 2026-07-13; names without seed metrics show as "awaiting data".
  • scripts/refresh_aristocrats.py (monthly GitHub Action) writes
    data/aristocrats.json with live price/yield/median-yield/CAGR per
    ticker; _apply_overlay() merges it per-field at request time.
    Missing/malformed overlay ⇒ seed snapshot, never a 500.

Streaks are consecutive years of dividend increases (approximate —
sources differ by a year or two on several names; refreshed by hand
annually). International streaks are in LOCAL currency: the raise
streak can be intact while your USD payment wobbles with FX.
"""
from __future__ import annotations

import json
import logging
from pathlib import Path

logger = logging.getLogger(__name__)

SEED_AS_OF = "2026-07-13"          # dividend.com snapshot in the seeds
_OVERLAY_PATH = Path(__file__).resolve().parent / "data" / "aristocrats.json"

# Chowder hurdles.
CHOWDER_HURDLE = 12.0
CHOWDER_HURDLE_LOW = 8.0           # utilities / REITs / midstream
# Value trigger: yield ≥ 20% above own 5-yr median.
VALUE_PREMIUM_MIN = 20.0
# Safety gates (hide) and badge thresholds (warn).
# Hand seeds are dividend.com's FORWARD, adjusted payouts; the refresh's
# figures are trailing three-year GAAP/cash covers, which ran a median
# ~9 points above the seeds on the 20 names that have both — so each
# basis keeps its own line.
PAYOUT_HIDE = 90.0
PAYOUT_WARN = 75.0
SEED_PAYOUT_HIDE = 80.0
SEED_PAYOUT_WARN = 65.0
ND_EBITDA_HIDE = 3.5
ND_EBITDA_WARN = 2.5
# Utilities, REITs and midstream run on regulated or contracted cash flow
# and carry 5-6x as a matter of course; one 3.5x line gated every one.
ND_EBITDA_HIDE_ASSET = 6.5
ND_EBITDA_WARN_ASSET = 5.5

# ── Universe ─────────────────────────────────────────────────────────
# keys: t=ticker, n=name, sec=sector, c=country, yrs=streak,
#       low_hurdle=True → Chowder 8 (utility/REIT/midstream),
#       note=Schwab access / FX note (intl),
# seed metrics (all optional): y=fwd yield %, dg5=5-yr div CAGR %,
#       po=payout %, nd=net debt/EBITDA ×, pe=fwd P/E
US = "US"

UNIVERSE: list[dict] = [
    # ── US — seeded from dividend.com 2026-07-13 (user-verified) ────
    {"t": "PEP",  "n": "PepsiCo",              "sec": "Staples",     "c": US, "yrs": 54, "y": 4.11, "dg5": 6.02, "po": 63.46, "nd": 2.3, "pe": 15.4},
    {"t": "ABBV", "n": "AbbVie",               "sec": "Healthcare",  "c": US, "yrs": 54, "y": 2.79, "dg5": 5.88, "po": 42.72, "nd": 2.3, "pe": 15.3},
    {"t": "ABT",  "n": "Abbott Laboratories",  "sec": "Healthcare",  "c": US, "yrs": 55, "y": 2.74, "dg5": 6.72, "po": 41.65, "nd": 0.5, "pe": 15.2},
    {"t": "BDX",  "n": "Becton Dickinson",     "sec": "Healthcare",  "c": US, "yrs": 54, "y": 2.73, "dg5": 4.56, "po": 31.34, "nd": 2.5, "pe": 11.5},
    {"t": "ADM",  "n": "Archer-Daniels-Midland","sec": "Staples",    "c": US, "yrs": 54, "y": 2.54, "dg5": 7.04, "po": 37.74, "nd": 2.4, "pe": 14.9},
    {"t": "PPG",  "n": "PPG Industries",       "sec": "Materials",   "c": US, "yrs": 55, "y": 2.49, "dg5": 4.67, "po": 32.97, "nd": 3.2, "pe": 13.2},
    {"t": "SPGI", "n": "S&P Global",           "sec": "Financials",  "c": US, "yrs": 54, "y": 0.89, "dg5": 4.73, "po": 18.62, "nd": 1.7, "pe": 21.0},
    {"t": "TNC",  "n": "Tennant Co",           "sec": "Industrials", "c": US, "yrs": 54, "y": 1.45, "dg5": 5.70, "po": 19.34, "nd": 1.7, "pe": 13.3},
    {"t": "KMB",  "n": "Kimberly-Clark",       "sec": "Staples",     "c": US, "yrs": 53, "y": 4.65, "dg5": 2.34, "po": 67.76, "pe": 14.6},
    {"t": "WMT",  "n": "Walmart",              "sec": "Staples",     "c": US, "yrs": 52, "y": 0.86, "dg5": 6.19, "po": 30.17, "pe": 35.0},
    {"t": "NUE",  "n": "Nucor",                "sec": "Materials",   "c": US, "yrs": 53, "y": 0.96, "dg5": 5.49, "po": 12.95, "nd": 0.4, "pe": 13.5},
    {"t": "GWW",  "n": "W.W. Grainger",        "sec": "Industrials", "c": US, "yrs": 56, "y": 0.72, "dg5": 8.77, "po": 19.76, "nd": 1.1, "pe": 27.6},
    {"t": "NFG",  "n": "National Fuel Gas",    "sec": "Energy",      "c": US, "yrs": 57, "y": 2.74, "dg5": 3.98, "po": 28.50, "nd": 2.4, "pe": 10.4, "low_hurdle": True},
    {"t": "MSA",  "n": "MSA Safety",           "sec": "Industrials", "c": US, "yrs": 57, "y": 1.28, "dg5": 4.20, "po": 22.26, "nd": 1.5, "pe": 17.4},
    {"t": "UVV",  "n": "Universal Corp",       "sec": "Staples",     "c": US, "yrs": 57, "y": 6.51, "dg5": 1.25, "po": 75.80, "nd": 4.1, "pe": 11.6},
    {"t": "MO",   "n": "Altria Group",         "sec": "Staples",     "c": US, "yrs": 57, "y": 5.90, "dg5": 3.79, "po": 72.04, "nd": 2.0, "pe": 12.2},
    {"t": "SYY",  "n": "Sysco",                "sec": "Staples",     "c": US, "yrs": 58, "y": 2.64, "dg5": 3.32, "po": 44.41, "nd": 3.2, "pe": 16.8},
    {"t": "SCL",  "n": "Stepan Co",            "sec": "Materials",   "c": US, "yrs": 58, "y": 2.78, "dg5": 4.80, "po": 40.36, "nd": 1.3, "pe": 14.5},
    {"t": "GRC",  "n": "Gorman-Rupp",          "sec": "Industrials", "c": US, "yrs": 53, "y": 0.97, "dg5": 3.66, "po": 25.36, "pe": 26.2},
    {"t": "MSEX", "n": "Middlesex Water",      "sec": "Utilities",   "c": US, "yrs": 53, "y": 2.60, "dg5": 5.39, "po": 51.34, "pe": 19.8, "low_hurdle": True},
    {"t": "SJW",  "n": "SJW Group",            "sec": "Utilities",   "c": US, "yrs": 58, "y": 2.73, "dg5": 5.59, "po": 59.41, "pe": 21.7, "low_hurdle": True},
    # ── US — canonical champions, metrics populate on first refresh ─
    {"t": "AWR",  "n": "American States Water","sec": "Utilities",   "c": US, "yrs": 71, "low_hurdle": True},
    {"t": "CWT",  "n": "California Water",     "sec": "Utilities",   "c": US, "yrs": 57, "low_hurdle": True},
    {"t": "DOV",  "n": "Dover Corp",           "sec": "Industrials", "c": US, "yrs": 69},
    {"t": "GPC",  "n": "Genuine Parts",        "sec": "Discretionary","c": US, "yrs": 69},
    {"t": "PG",   "n": "Procter & Gamble",     "sec": "Staples",     "c": US, "yrs": 69},
    {"t": "EMR",  "n": "Emerson Electric",     "sec": "Industrials", "c": US, "yrs": 68},
    {"t": "CINF", "n": "Cincinnati Financial", "sec": "Financials",  "c": US, "yrs": 65, "insurer": True},
    {"t": "KO",   "n": "Coca-Cola",            "sec": "Staples",     "c": US, "yrs": 63},
    {"t": "JNJ",  "n": "Johnson & Johnson",    "sec": "Healthcare",  "c": US, "yrs": 63},
    {"t": "LOW",  "n": "Lowe's",               "sec": "Discretionary","c": US, "yrs": 63},
    {"t": "LANC", "n": "Lancaster Colony",     "sec": "Staples",     "c": US, "yrs": 63},
    {"t": "CL",   "n": "Colgate-Palmolive",    "sec": "Staples",     "c": US, "yrs": 62},
    {"t": "NDSN", "n": "Nordson",              "sec": "Industrials", "c": US, "yrs": 61},
    {"t": "HRL",  "n": "Hormel Foods",         "sec": "Staples",     "c": US, "yrs": 59},
    {"t": "SWK",  "n": "Stanley Black & Decker","sec": "Industrials","c": US, "yrs": 58},
    {"t": "FRT",  "n": "Federal Realty",       "sec": "REIT",        "c": US, "yrs": 57, "low_hurdle": True},
    {"t": "ITW",  "n": "Illinois Tool Works",  "sec": "Industrials", "c": US, "yrs": 54},
    {"t": "TGT",  "n": "Target",               "sec": "Staples",     "c": US, "yrs": 54},
    {"t": "ED",   "n": "Consolidated Edison",  "sec": "Utilities",   "c": US, "yrs": 51, "low_hurdle": True},
    {"t": "ADP",  "n": "Automatic Data Proc.", "sec": "Technology",  "c": US, "yrs": 50},
    {"t": "MCD",  "n": "McDonald's",           "sec": "Discretionary","c": US, "yrs": 49},
    {"t": "PNR",  "n": "Pentair",              "sec": "Industrials", "c": US, "yrs": 49},
    {"t": "CLX",  "n": "Clorox",               "sec": "Staples",     "c": US, "yrs": 48},
    {"t": "MDT",  "n": "Medtronic",            "sec": "Healthcare",  "c": US, "yrs": 48},
    {"t": "SHW",  "n": "Sherwin-Williams",     "sec": "Materials",   "c": US, "yrs": 46},
    {"t": "BEN",  "n": "Franklin Resources",   "sec": "Financials",  "c": US, "yrs": 45},
    {"t": "APD",  "n": "Air Products",         "sec": "Materials",   "c": US, "yrs": 43},
    {"t": "CTAS", "n": "Cintas",               "sec": "Industrials", "c": US, "yrs": 43},
    {"t": "AFL",  "n": "Aflac",                "sec": "Financials",  "c": US, "yrs": 43, "insurer": True},
    {"t": "XOM",  "n": "Exxon Mobil",          "sec": "Energy",      "c": US, "yrs": 43},
    {"t": "ATO",  "n": "Atmos Energy",         "sec": "Utilities",   "c": US, "yrs": 41, "low_hurdle": True},
    {"t": "BF-B", "n": "Brown-Forman",         "sec": "Staples",     "c": US, "yrs": 41},
    {"t": "MKC",  "n": "McCormick",            "sec": "Staples",     "c": US, "yrs": 39},
    {"t": "TROW", "n": "T. Rowe Price",        "sec": "Financials",  "c": US, "yrs": 39},
    {"t": "CVX",  "n": "Chevron",              "sec": "Energy",      "c": US, "yrs": 38},
    {"t": "ERIE", "n": "Erie Indemnity",       "sec": "Financials",  "c": US, "yrs": 35},
    {"t": "GD",   "n": "General Dynamics",     "sec": "Industrials", "c": US, "yrs": 34},
    {"t": "ECL",  "n": "Ecolab",               "sec": "Materials",   "c": US, "yrs": 33},
    {"t": "ROP",  "n": "Roper Technologies",   "sec": "Technology",  "c": US, "yrs": 33},
    {"t": "WST",  "n": "West Pharmaceutical",  "sec": "Healthcare",  "c": US, "yrs": 32},
    {"t": "AOS",  "n": "A.O. Smith",           "sec": "Industrials", "c": US, "yrs": 31},
    {"t": "CAT",  "n": "Caterpillar",          "sec": "Industrials", "c": US, "yrs": 31},
    {"t": "CB",   "n": "Chubb",                "sec": "Financials",  "c": US, "yrs": 31, "insurer": True},
    {"t": "ALB",  "n": "Albemarle",            "sec": "Materials",   "c": US, "yrs": 31},
    {"t": "ESS",  "n": "Essex Property",       "sec": "REIT",        "c": US, "yrs": 31, "low_hurdle": True},
    {"t": "O",    "n": "Realty Income",        "sec": "REIT",        "c": US, "yrs": 31, "low_hurdle": True},
    {"t": "NEE",  "n": "NextEra Energy",       "sec": "Utilities",   "c": US, "yrs": 31, "low_hurdle": True},
    {"t": "BRO",  "n": "Brown & Brown",        "sec": "Financials",  "c": US, "yrs": 31},
    {"t": "CAH",  "n": "Cardinal Health",      "sec": "Healthcare",  "c": US, "yrs": 30},
    {"t": "EXPD", "n": "Expeditors Intl",      "sec": "Industrials", "c": US, "yrs": 30},
    {"t": "IBM",  "n": "IBM",                  "sec": "Technology",  "c": US, "yrs": 30},
    {"t": "CHD",  "n": "Church & Dwight",      "sec": "Staples",     "c": US, "yrs": 29},
    {"t": "CHRW", "n": "C.H. Robinson",        "sec": "Industrials", "c": US, "yrs": 27},
    {"t": "FDS",  "n": "FactSet Research",     "sec": "Financials",  "c": US, "yrs": 26},
    {"t": "FAST", "n": "Fastenal",             "sec": "Industrials", "c": US, "yrs": 26},
    # ── International — Schwab-buyable (NYSE ADR or OTC) ────────────
    # Streaks are in LOCAL currency; USD payments wobble with FX.
    {"t": "CDUAF", "n": "Canadian Utilities",  "sec": "Utilities",   "c": "CA", "yrs": 55, "y": 3.49, "dg5": 1.00, "po": 65.80, "pe": 18.9, "low_hurdle": True, "note": "OTC — thin volume, use limit orders"},
    {"t": "FTS",   "n": "Fortis",              "sec": "Utilities",   "c": "CA", "yrs": 51, "low_hurdle": True, "note": "NYSE listing"},
    {"t": "ENB",   "n": "Enbridge",            "sec": "Midstream",   "c": "CA", "yrs": 30, "low_hurdle": True, "note": "NYSE listing"},
    {"t": "CNI",   "n": "Canadian National Rwy","sec": "Industrials","c": "CA", "yrs": 29, "note": "NYSE listing"},
    {"t": "IMO",   "n": "Imperial Oil",        "sec": "Energy",      "c": "CA", "yrs": 30, "note": "NYSE American listing"},
    {"t": "CNQ",   "n": "Canadian Natural Res","sec": "Energy",      "c": "CA", "yrs": 25, "note": "NYSE listing"},
    {"t": "MTRAF", "n": "Metro Inc",           "sec": "Staples",     "c": "CA", "yrs": 30, "note": "OTC — thin volume, use limit orders"},
    {"t": "NVS",   "n": "Novartis",            "sec": "Healthcare",  "c": "CH", "yrs": 28, "note": "NYSE ADR"},
    {"t": "RHHBY", "n": "Roche Holding",       "sec": "Healthcare",  "c": "CH", "yrs": 38, "note": "OTC ADR — liquid"},
    {"t": "NSRGY", "n": "Nestlé",              "sec": "Staples",     "c": "CH", "yrs": 29, "note": "OTC ADR — liquid"},
    {"t": "SNY",   "n": "Sanofi",              "sec": "Healthcare",  "c": "FR", "yrs": 28, "note": "Nasdaq ADR"},
    {"t": "DEO",   "n": "Diageo",              "sec": "Staples",     "c": "UK", "yrs": 26, "note": "NYSE ADR"},
    {"t": "SPXSY", "n": "Spirax Group",        "sec": "Industrials", "c": "UK", "yrs": 55, "note": "OTC ADR — thin, use limit orders"},
    {"t": "HLMLY", "n": "Halma",               "sec": "Industrials", "c": "UK", "yrs": 45, "note": "OTC ADR — thin, use limit orders"},
    {"t": "BZLFY", "n": "Bunzl",               "sec": "Industrials", "c": "UK", "yrs": 31, "note": "OTC ADR — thin, use limit orders"},
    {"t": "KAOOY", "n": "Kao Corp",            "sec": "Staples",     "c": "JP", "yrs": 35, "note": "OTC ADR — Japan's longest streak"},
]


# ── Overlay (monthly refresh) ────────────────────────────────────────

def _load_overlay() -> dict:
    """data/aristocrats.json if present and well-formed, else {}.
    Same hardening as country_data: a syntactically-valid file with the
    wrong shape is treated as malformed — the page must never 500 on a
    bad refresh artifact."""
    try:
        with open(_OVERLAY_PATH, encoding="utf-8") as fh:
            data = json.load(fh)
        if isinstance(data, dict) and isinstance(data.get("tickers"), dict):
            return data
    except (OSError, ValueError):
        pass
    return {}


def data_source_label() -> str:
    overlay = _load_overlay()
    if overlay:
        return f"live — {overlay.get('as_of') or 'latest'}"
    return f"seed snapshot ({SEED_AS_OF}) — partial; run the refresh workflow for full data"


# Overlay fields allowed to override the seed (per-field merge). Payout
# and leverage now come from SEC filings; a seed survives only where the
# refresh could not compute the figure, and is marked as a seed.
_OVERLAY_FIELDS = ("y", "dg5", "median_y5", "price",
                   "pct_off_52wk_high", "pct_above_52wk_low", "po", "nd")
_OVERLAY_TEXT = ("po_basis", "po_years", "nd_basis", "safety_as_of", "safety_note")


# Every optional field templates touch — normalized to None so Jinja
# never sees an Undefined (which would pass `is not none` and crash
# format filters).
_OPTIONAL_FIELDS = ("y", "dg5", "median_y5", "po", "nd", "pe", "price",
                    "pct_off_52wk_high", "pct_above_52wk_low", "note") + _OVERLAY_TEXT


def _merged_universe(overlay: dict | None = None) -> list[dict]:
    overlay = (_load_overlay() if overlay is None else overlay).get("tickers") or {}
    out = []
    for a in UNIVERSE:
        row = dict(a)
        for f in _OPTIONAL_FIELDS:
            row.setdefault(f, None)
        row["po_src"] = "seed" if row["po"] is not None else None
        row["nd_src"] = "seed" if row["nd"] is not None else None
        entry = overlay.get(a["t"])
        if isinstance(entry, dict):
            for f in _OVERLAY_FIELDS:
                v = entry.get(f)
                if isinstance(v, (int, float)):
                    row[f] = round(float(v), 2)
                    if f in ("po", "nd"):
                        row[f + "_src"] = "sec"
            for f in _OVERLAY_TEXT:
                if isinstance(entry.get(f), str):
                    row[f] = entry[f]
            # A basis that decides the answer without a number (net cash,
            # not applicable, negative earnings) is SEC's answer too.
            if entry.get("po_basis") == "negative":
                row["po"], row["po_src"] = None, "sec"
            if entry.get("nd_basis") in ("n/a", "net cash", "no positive EBITDA"):
                row["nd_src"] = "sec"
                if entry.get("nd_basis") != "net cash":
                    row["nd"] = None
        out.append(row)
    return out


def payout_state(a: dict) -> str:
    """pass | warn | fail | unknown — on the line for the figure's basis."""
    if a.get("po_src") == "sec" and a.get("po_basis") == "negative":
        return "fail"
    po = a.get("po")
    if po is None:
        return "unknown"
    hide, warn = ((PAYOUT_HIDE, PAYOUT_WARN) if a.get("po_src") == "sec"
                  else (SEED_PAYOUT_HIDE, SEED_PAYOUT_WARN))
    return "fail" if po >= hide else "warn" if po >= warn else "pass"


def leverage_state(a: dict) -> str:
    """pass | warn | fail | unknown | n/a."""
    basis = a.get("nd_basis") if a.get("nd_src") == "sec" else None
    if basis == "n/a":
        return "n/a"
    if basis == "net cash":
        return "pass"
    if basis == "no positive EBITDA":
        return "fail"
    nd = a.get("nd")
    if nd is None:
        return "unknown"
    hide, warn = ((ND_EBITDA_HIDE_ASSET, ND_EBITDA_WARN_ASSET) if a.get("low_hurdle")
                  else (ND_EBITDA_HIDE, ND_EBITDA_WARN))
    if nd >= hide:
        return "fail"
    if basis == "floor":
        return "unknown"            # one kind of debt: proves a failure, never a pass
    return "warn" if nd >= warn else "pass"


# ── Scoring ──────────────────────────────────────────────────────────

def score(overlay: dict | None = None) -> list[dict]:
    """Merge overlay onto seeds, evaluate the three filters, classify.

    status: BUY        value + chowder pass, and payout and leverage both
                       measured and within their lines
            UNMEASURED value + chowder pass, but payout or leverage could
                       not be measured — never BUY on an unknown
            VALUE      cheap vs own history but misses the Chowder hurdle
            WATCH      full data, not currently cheap
            GATED      a measured payout or leverage past its hide line
            AWAITING   not enough market data yet
    """
    rows = []
    for a in _merged_universe(overlay):
        y = a.get("y")
        dg5 = a.get("dg5")
        med = a.get("median_y5")
        po = a.get("po")
        nd = a.get("nd")

        hurdle = CHOWDER_HURDLE_LOW if a.get("low_hurdle") else CHOWDER_HURDLE
        chowder = round(y + dg5, 1) if (y is not None and dg5 is not None) else None
        chowder_pass = chowder is not None and chowder >= hurdle

        premium = None
        value_flag = False
        if y is not None and med and med > 0:
            premium = round((y / med - 1) * 100, 1)
            value_flag = premium >= VALUE_PREMIUM_MIN

        ps, ls = payout_state(a), leverage_state(a)
        gated = "fail" in (ps, ls)
        safety_known = ps in ("pass", "warn") and ls in ("pass", "warn", "n/a")
        basis_po = (f"{a['po_basis']} cover, {a['po_years']}" if a.get("po_src") == "sec"
                    and a.get("po_years") else "dividend.com seed" if a.get("po_src") == "seed" else "")
        badges = []
        if ps == "warn":
            badges.append({"key": "payout", "label": f"payout {po:.0f}%",
                           "title": f"Elevated payout ({basis_po}) — less room to keep raising through a bad year."})
        if ls == "warn":
            badges.append({"key": "debt", "label": f"debt {nd:.1f}×",
                           "title": "Elevated net debt/EBITDA — leverage eats dividend flexibility when rates move."})
        if a["c"] != US:
            badges.append({"key": "fx", "label": "FX",
                           "title": "Streak is in local currency — your USD payment varies with exchange rates even when the raise streak is intact."})

        incomplete = y is None or dg5 is None or med is None
        if gated:
            status = "GATED"
        elif incomplete:
            status = "AWAITING"
        elif value_flag and chowder_pass:
            status = "BUY" if safety_known else "UNMEASURED"
        elif value_flag:
            status = "VALUE"
        else:
            status = "WATCH"

        rows.append({
            **a,
            "chowder": chowder,
            "hurdle": hurdle,
            "chowder_pass": chowder_pass,
            "yield_premium_pct": premium,
            "value_flag": value_flag,
            "payout_state": ps,
            "leverage_state": ls,
            "safety_known": safety_known,
            "payout_basis": basis_po,
            "badges": badges,
            "status": status,
        })

    # Sort: BUY first by premium, then UNMEASURED (cheap, unverified),
    # then VALUE, then the rest by premium (unknown premiums last).
    order = {"BUY": 0, "UNMEASURED": 1, "VALUE": 2, "WATCH": 3, "GATED": 4, "AWAITING": 5}
    rows.sort(key=lambda r: (order[r["status"]],
                             -(r["yield_premium_pct"] if r["yield_premium_pct"] is not None else -999)))
    return rows


def buy_list(rows: list[dict] | None = None) -> list[dict]:
    rows = rows if rows is not None else score()
    return [r for r in rows if r["status"] == "BUY"]

"""Peter Lynch's GARP screen — the half that is pure arithmetic.

Lynch ran Magellan 1977-1990 at about 29%/yr, hunting growth the market
had not repriced. This module holds every computation that screen needs,
separated from the fetching so it can be proved against a fixture instead
of against the internet — the same split as schloss.py and compounders.py.

WHY THIS EXISTS AT ALL. The screen ran for three monthly builds publishing
numbers that were not what their names said:

    ANF   3-yr EPS CAGR 493.6%    EPS path 4.20 -> 0.05 -> 6.22 -> 10.69 -> 10.46
    XNET  3-yr EPS CAGR 273.1%    EPS path 0.00 -> 0.06 -> 0.04 -> 0.00 ->  3.31
    VIPS  P/E 0.22               renminbi EPS divided by a dollar ADS price
    ASO   3-yr EPS CAGR  41.6%    computed between two QUARTER ends
    LULU  48 consecutive          positive-EPS years, against a 2007 IPO
    LULU  D/E 0.00               global store fleet, debt tags simply unfiled
    PDD   capex/OCF 0.04%        $6.6m of capex against $15.3bn of cash flow

None of those is a bad company. Each is a real figure from real filings
that stopped describing the business, which is the failure this whole app
keeps re-learning. Every guard below exists because of one line above.

FOUR RULES CARRIED IN FROM THE OTHER SCREENS:

  * A PERIOD IS NOT A FISCAL YEAR BECAUSE THE FILING SAYS FY. The `fp` and
    `form` fields describe the FILING, so quarterly facts inside a 10-K
    carry FY too. Only the duration between start and end settles it.
  * MISSING IS NEVER A PASS. An unfiled debt tag read as 0.0 is the best
    possible score on a gate that decides membership.
  * BOUND EVERY UNBOUNDED TERM, keep the raw figure, badge the bounded one.
  * A PERCENTAGE IS ONLY AS GOOD AS ITS DENOMINATOR — and an EPS is only as
    good as its currency and its share basis.
"""
from __future__ import annotations

import json
from datetime import date
from pathlib import Path

_DATA_DIR = Path(__file__).resolve().parent / "data" / "lynch_snapshots"

# ── Lynch's gates ────────────────────────────────────────────────────
#
# THE FLOOR IS THE UNIVERSE DIAL. At $1B the screen saw 2,218 companies;
# at $30M it sees 3,906. Lynch hunted small — a ten-bagger has to start
# small enough to become one — and the test sits after the filings pull,
# so lowering it costs no extra requests.
MARKET_CAP_MIN = 30_000_000

PE_MAX = 10.0
EPS_GROWTH_MIN = 10.0
DEBT_TO_EQUITY_MAX = 0.5
CAPEX_TO_OCF_MAX = 0.5
MIN_EPS_YEARS = 3               # need this many spans, so 4 annual points

# ── Bounds, each one paid for by a row on the live board ─────────────
#
# A P/E has no lower bound in the code and Vipshop printed 0.22. Nothing
# is priced at a fifth of one year's earnings; that is a wrong currency, a
# wrong share basis, or a one-time gain. Below the floor the multiple is
# WITHHELD and badged, not silently dropped.
PE_SANE = (3.0, 10.0)

# Growth was the last unbounded term in the app AND the default sort key,
# which made the ranking monotone in the estimator's own error. Sustaining
# 35%/yr for three years is rare enough that a GARP buyer should not
# underwrite it, and high enough not to bind on a genuine fast grower.
EPS_GROWTH_CAP = 35.0

# A base year this far below the window's median is a trough, and the
# "growth" measured off it is a recovery. Abercrombie's 0.05 against a
# median of 6.22 is 0.8%.
TROUGH_BASE_FRAC = 0.25

# THE OTHER HALF OF THE SAME PROBLEM, and the trough test structurally
# cannot catch it. Xunlei's EPS ran 0.00, 0.06, 0.04, 0.00, 3.31 — the
# base of 0.06 sits ABOVE the median of 0.04, because the entire history
# is near zero. Nothing is depressed; the company simply has no history at
# its current level, and one year at 83x the median is a step change, not
# a rate that can be extrapolated. Without this it passes at the capped
# 35%, comfortably over the 10% gate, which is worse than the 273% it
# printed before — a fabricated number that looks reasonable.
STEP_MULTIPLE = 5.0

# XBRL annual durations wander either side of 365 — 52/53-week retail
# calendars, leap years, stub periods. This band admits every real annual
# period and excludes every quarter.
ANNUAL_DAYS = (330, 400)

# EPS x shares should reconstruct net income. It does not when the EPS is
# in one currency and the share count in another basis. On the live board
# the four US filers land at 0.82-1.52 and the five foreign filers at
# 2.81-7.52, so this separates them completely — and it catches an
# ADS-ratio mismatch, which a currency check alone cannot.
UNITS_TOLERANCE = 0.35

# An ADS is a bundle of ordinary shares and the ratio is NOT in EDGAR.
# market_cap / price gives an implied count; divided by the filed count it
# lands on the bundle size. A clean integer is the ratio and belongs on
# the row. Anything else means the two numbers are not about the same
# instrument and no per-share figure from them is usable.
ADS_RATIOS = (1, 2, 3, 4, 5, 6, 8, 10, 12, 20, 25, 40, 50)
ADS_TOLERANCE = 0.08

# Buffett overlay. Kept because it is informative, defaulted OFF because
# it requires five consecutive profitable years and therefore excludes
# young fast growers BY CONSTRUCTION — the one category Lynch made most of
# his money in. Blending the two is how you end up with a portfolio
# neither man would hold; schloss.py carries the same warning about
# Schloss and Compounders for the same reason.
MOAT_ROE_MIN = 15.0
MOAT_POS_EPS_YEARS = 5

# ── Microcap guards ──────────────────────────────────────────────────
#
# Market cap alone cannot tell a $60m operating business from a $60m
# registrant with a filing agent and no revenue. In the $30M-$300M band
# 206 of 965 companies have negative equity, 129 have no revenue tag at
# all and 316 report under $10m. These ship WITH the lower floor, never
# after it.
MIN_REVENUE = 10_000_000
MIN_PRICE = 1.0                 # sub-$1 is a delisting-risk proxy, free
STALE_DAYS = 550                # ~18 months since the last annual figure
CAP_TO_ASSETS_SANE = (0.01, 100.0)


def _num(v) -> float | None:
    try:
        f = float(v)
    except (TypeError, ValueError):
        return None
    return f if f == f and abs(f) != float("inf") else None


# ═══════════════════════════════════════════════════════════════════
# XBRL EXTRACTION — where three of the seven bugs lived
# ═══════════════════════════════════════════════════════════════════

def _spans(entries: list, want_unit: str | None = None) -> list[tuple[str, float]]:
    """[(end_date, value)] for facts that are genuinely ANNUAL.

    THE FILING IS NOT THE FACT. `fp == "FY"` and `form == "10-K"` describe
    the document, so a quarterly EPS reported inside a 10-K carries both.
    Academy Sports came through with three quarter-ends in its five-year
    "annual" history and a 41.6% CAGR measured between two of them.

    Only start-to-end duration settles it. Instantaneous facts (balance
    sheet) have no start and are handled by _instant.

    Amendments are deduped on (start, end), not end alone — an annual and
    a quarterly fact ending the same day were overwriting each other
    arbitrarily.
    """
    best: dict[tuple[str, str], float] = {}
    for e in entries or []:
        if not isinstance(e, dict):
            continue
        val = _num(e.get("val"))
        start, end = e.get("start"), e.get("end")
        if val is None or not start or not end:
            continue
        try:
            days = (date.fromisoformat(end) - date.fromisoformat(start)).days
        except (ValueError, TypeError):
            continue
        if not (ANNUAL_DAYS[0] <= days <= ANNUAL_DAYS[1]):
            continue
        best[(start, end)] = val
    out: dict[str, float] = {}
    for (_s, end), val in sorted(best.items()):
        out[end] = val
    return sorted(out.items())


def _instant(entries: list) -> list[tuple[str, float]]:
    """[(end_date, value)] for point-in-time facts — balance-sheet lines.

    These legitimately have no start date, so the duration guard cannot
    apply. Dedupe on the date; the last amendment wins.
    """
    best: dict[str, float] = {}
    for e in entries or []:
        if not isinstance(e, dict):
            continue
        val, end = _num(e.get("val")), e.get("end")
        if val is not None and end and not e.get("start"):
            best[end] = val
    return sorted(best.items())


def _units(facts: dict, concept: str, taxonomy: str = "us-gaap") -> dict:
    node = ((facts or {}).get("facts") or {}).get(taxonomy) or {}
    return ((node.get(concept) or {}).get("units") or {})


def annual_series(facts: dict, concepts, unit: str = "USD") -> tuple[dict, str]:
    """({end_date: value}, concept used) for the best-covered concept.

    MOST COVERAGE WINS, rather than first-tag-wins. A filer that reports
    capex under PaymentsToAcquireProductiveAssets was being deleted with
    no trace because the code only ever looked at
    PaymentsToAcquirePropertyPlantAndEquipment.
    """
    if isinstance(concepts, str):
        concepts = [concepts]
    best: dict[str, float] = {}
    best_name = ""
    for c in concepts:
        series = dict(_spans(_units(facts, c).get(unit)))
        if len(series) > len(best):
            best, best_name = series, c
    return best, best_name


def instant_value(facts: dict, concepts, unit: str = "USD") -> tuple[float | None, str]:
    """Latest point-in-time value across a list of concepts.

    Returns (None, "") when NOTHING was filed — which is different from
    zero, and is the distinction the debt gate was losing.
    """
    if isinstance(concepts, str):
        concepts = [concepts]
    for c in concepts:
        series = _instant(_units(facts, c).get(unit))
        if series:
            return series[-1][1], c
    return None, ""


def eps_series(facts: dict) -> tuple[dict, str, str]:
    """({fy_end: eps}, concept, unit key) — CURRENCY AWARE.

    The old code accepted any unit key containing "shares", so
    `CNY/shares` passed as readily as `USD/shares` while every other
    figure on the row was pinned to USD. Vipshop's printed P/E of 0.22 is
    a renminbi EPS divided by a dollar price.

    The unit key is returned so a caller can refuse to divide a price by
    an EPS denominated in something else.
    """
    for concept in ("EarningsPerShareDiluted", "EarningsPerShareBasic"):
        units = _units(facts, concept)
        for key in ("USD/shares", "USD-per-shares"):
            if key in units:
                s = dict(_spans(units[key]))
                if s:
                    return s, concept, key
        # Nothing in dollars. Report what WAS filed rather than pretending
        # it is dollars — the caller withholds the multiple.
        for key, entries in units.items():
            if "shares" in key.lower():
                s = dict(_spans(entries))
                if s:
                    return s, concept, key
    return {}, "", ""


def first_filing_end(facts: dict) -> str | None:
    """Most recent annual period end across the high-coverage concepts —
    used to tell a live filer from a dormant registrant."""
    latest = None
    for c in ("Revenues", "RevenueFromContractWithCustomerExcludingAssessedTax",
              "NetIncomeLoss", "Assets"):
        for unit in ("USD",):
            s = _spans(_units(facts, c).get(unit)) or _instant(_units(facts, c).get(unit))
            if s and (latest is None or s[-1][0] > latest):
                latest = s[-1][0]
    return latest


# ═══════════════════════════════════════════════════════════════════
# THE INVARIANTS
# ═══════════════════════════════════════════════════════════════════

def units_check(eps: float | None, shares: float | None,
                net_income: float | None) -> dict:
    """Does EPS x shares reconstruct net income?

    One arithmetic identity, no network, no second source, no judgement —
    and it catches BOTH failure modes at once. A currency mismatch and an
    ADS-ratio mismatch both break the identity; a currency check alone
    catches only the first. Xunlei is the proof: its market cap agrees
    with the feed to 3%, and its ratio is still 2.81.

    Returns ok=None when a component is missing. It fires or abstains — it
    never certifies.
    """
    e, s, ni = _num(eps), _num(shares), _num(net_income)
    if e is None or s is None or ni is None or ni == 0 or s <= 0:
        return {"ok": None, "ratio": None, "implied": None}
    implied = e * s
    ratio = abs(implied - ni) / abs(ni)
    return {"ok": ratio <= UNITS_TOLERANCE, "ratio": round(ratio, 2),
            "implied": implied}


def ads_ratio(market_cap: float | None, price: float | None,
              filed_shares: float | None) -> dict:
    """How many ordinary shares are in one traded share.

    The ratio is not published in EDGAR and cannot be recovered by any
    choice of unit. But the bulk feed gives a market cap and a price, and
    their quotient is an implied count: divided by the filed count it
    lands on the bundle size.

    PDD's implies 1.423bn against a filed 5.69bn — a clean 4, so the feed
    is quoting per-ADS. Xunlei's implies 318m against a filed 314m — 1.01,
    so that feed is quoting per-ordinary. Both are fine and they are
    DIFFERENT, which is exactly why this has to be measured per company
    rather than assumed.

    A quotient that is neither ~1 nor a clean integer means the two
    numbers are not describing the same instrument, and no per-share
    figure derived from them may be published.
    """
    cap, p, filed = _num(market_cap), _num(price), _num(filed_shares)
    if not cap or not p or not filed or p <= 0 or filed <= 0:
        return {"ratio": None, "clean": None, "implied_shares": None}
    implied = cap / p
    q = filed / implied
    for r in ADS_RATIOS:
        if abs(q - r) / r <= ADS_TOLERANCE:
            return {"ratio": r, "clean": True, "implied_shares": implied}
    return {"ratio": round(q, 2), "clean": False, "implied_shares": implied}


# ═══════════════════════════════════════════════════════════════════
# GROWTH
# ═══════════════════════════════════════════════════════════════════

def growth(series: dict) -> dict:
    """Earnings growth, with the two things an endpoint CAGR cannot see.

    A TROUGH BASE IS NOT A GROWTH RATE. Abercrombie went
    4.20 -> 0.05 -> 6.22 -> 10.69 -> 10.46 and the screen printed 493.6%.
    Xunlei went 0.00 -> 0.06 -> 0.04 -> 0.00 -> 3.31 and printed 273.1%.
    Both are recoveries, and a GARP screen that treats a recovery as a
    forecast is buying the top of one. When the base is under a quarter of
    the window's median, no rate is reported at all.

    DECELERATION IS THE ONLY THING THAT MATTERED TO LYNCH for a fast
    grower, and an endpoint CAGR is structurally blind to it — four of the
    six names on the live board had FALLING earnings in the latest year
    and were rendered as growth stocks. Latest-year change and an up-years
    count come back on every row whether or not the CAGR does.
    """
    pts = sorted((y, v) for y, v in (series or {}).items() if _num(v) is not None)
    out = {"cagr": None, "cagr_raw": None, "capped": False, "trough": False,
           "step": False, "years": len(pts), "span": None, "latest_yoy": None,
           "up_years": None, "of_years": None, "from_year": None,
           "to_year": None}
    if len(pts) < 2:
        return out

    vals = [v for _y, v in pts]
    out["latest_yoy"] = (round((vals[-1] - vals[-2]) / abs(vals[-2]) * 100, 1)
                         if vals[-2] else None)
    ups = sum(1 for i in range(1, len(vals)) if vals[i] > vals[i - 1])
    out["up_years"], out["of_years"] = ups, len(vals) - 1

    if len(pts) < MIN_EPS_YEARS + 1:
        return out

    (y0, start), (y1, end) = pts[-(MIN_EPS_YEARS + 1)], pts[-1]
    out["from_year"], out["to_year"] = y0, y1
    try:
        span = (date.fromisoformat(y1) - date.fromisoformat(y0)).days / 365.25
    except (ValueError, TypeError):
        span = float(MIN_EPS_YEARS)
    # The old code hardcoded 3.0 regardless of the actual endpoint dates.
    out["span"] = round(span, 2)
    if span <= 0:
        return out

    ordered = sorted(vals)
    median = ordered[len(ordered) // 2] if len(ordered) % 2 else \
        (ordered[len(ordered) // 2 - 1] + ordered[len(ordered) // 2]) / 2
    if median > 0 and start < median * TROUGH_BASE_FRAC:
        out["trough"] = True
        return out
    if median > 0 and end > median * STEP_MULTIPLE:
        out["step"] = True
        return out
    if start <= 0 or end <= 0:
        return out

    raw = ((end / start) ** (1.0 / span) - 1.0) * 100.0
    out["cagr_raw"] = round(raw, 1)
    out["capped"] = raw > EPS_GROWTH_CAP
    out["cagr"] = round(min(raw, EPS_GROWTH_CAP), 1)
    return out


def price_earnings(market_cap: float | None, net_income: float | None) -> dict:
    """P/E computed as market cap over net income.

    NEITHER SIDE TOUCHES A SHARE COUNT, which makes it immune to the
    currency mismatch and the ADS ratio in one move, and both inputs come
    from currency-pinned sources. PDD's honest multiple is about 9.66
    against a printed 5.37.

    Below PE_SANE the figure is withheld rather than dropped: a P/E under
    3 is an artefact, and deleting the row hides the artefact instead of
    reporting it.
    """
    cap, ni = _num(market_cap), _num(net_income)
    if cap is None or ni is None or cap <= 0:
        return {"pe": None, "pe_raw": None, "suspect": False, "profitable": None}
    if ni <= 0:
        return {"pe": None, "pe_raw": None, "suspect": False, "profitable": False}
    raw = cap / ni
    suspect = raw < PE_SANE[0]
    return {"pe": None if suspect else round(raw, 2), "pe_raw": round(raw, 2),
            "suspect": suspect, "profitable": True}


def peg(pe: float | None, growth_pct: float | None) -> float | None:
    """Lynch's own ratio. Both inputs are bounded before they arrive here,
    so PEG has a hard floor of PE_SANE[0] / EPS_GROWTH_CAP rather than
    running to 0.01."""
    p, g = _num(pe), _num(growth_pct)
    if p is None or g is None or g <= 0:
        return None
    return round(p / g, 2)


# ═══════════════════════════════════════════════════════════════════
# THE ROW
# ═══════════════════════════════════════════════════════════════════

# Reason codes. Every rejection names itself, so a board of nine out of an
# unstated universe becomes a funnel that can be argued with. Ten bare
# `return None` exits used to produce a file that a strict screen in a
# rich market and a parser that rejects everything would both write.
REASONS = {
    "no_price": "no quote in the bulk market file",
    "no_cap": "no market capitalisation",
    "too_small": "below the market-cap floor",
    "penny": "share price under the delisting-risk floor",
    "no_equity": "stockholders' equity not filed",
    "negative_equity": "negative stockholders' equity",
    "no_revenue": "no revenue concept filed",
    "tiny_revenue": "revenue below the operating-business floor",
    "dormant": "no annual figures filed recently",
    "cap_implausible": "market cap and total assets differ by orders of magnitude",
    "units_unverified": "EPS x shares does not reconstruct net income",
    "no_earnings": "no net income filed",
    "unprofitable": "net income not positive",
    "pe_suspect": "earnings multiple below the plausible floor",
    "pe_high": "earnings multiple above the ceiling",
    "short_history": "fewer than four annual EPS periods",
    "trough_base": "base year is a trough — recovery, not growth",
    "step_change": "latest year has no history behind it — a step, not a rate",
    "no_growth": "earnings growth below the floor",
    "debt_unknown": "debt not filed and the liability total could not settle it",
    "debt_high": "debt above the leverage ceiling",
    "capex_unknown": "capex or operating cash flow not filed",
    "capex_high": "capex above the reinvestment ceiling",
    "pass": "passes every filter",
}


def evaluate(f: dict) -> dict:
    """Score one company. Always returns a row — never None.

    The row carries a verdict and a reason code whether it passed or not,
    which is what makes the census possible and the board falsifiable.
    """
    import schloss as S

    r: dict = {
        "ticker": f.get("ticker"), "name": f.get("name"),
        "exchange": f.get("exchange") or "", "sic": f.get("sic"),
        "cik": f.get("cik"),
    }
    reasons: list[str] = []

    def done(code):
        r["verdict"] = "pass" if code == "pass" else "reject"
        r["reason"] = code
        r["reasons"] = reasons
        return r

    price = _num(f.get("price"))
    cap = _num(f.get("market_cap"))
    r["price"], r["market_cap"] = price, cap
    r["size_band"] = size_band(cap)

    if price is None and cap is None:
        return done("no_price")
    if cap is None:
        return done("no_cap")

    # ── microcap guards, shipped WITH the lower floor ──
    if price is not None and price < MIN_PRICE:
        return done("penny")
    if cap < MARKET_CAP_MIN:
        return done("too_small")

    assets = _num(f.get("total_assets"))
    if assets and assets > 0:
        ratio = cap / assets
        r["cap_to_assets"] = round(ratio, 3)
        if not (CAP_TO_ASSETS_SANE[0] <= ratio <= CAP_TO_ASSETS_SANE[1]):
            return done("cap_implausible")

    equity = _num(f.get("equity"))
    r["equity"] = equity
    if equity is None:
        return done("no_equity")
    if equity <= 0:
        return done("negative_equity")

    revenue = _num(f.get("revenue"))
    r["revenue"] = revenue
    if revenue is None:
        return done("no_revenue")
    if revenue < MIN_REVENUE:
        return done("tiny_revenue")

    r["last_filing"] = f.get("last_filing")
    if _stale(f.get("last_filing"), f.get("as_of")):
        return done("dormant")

    # ── the units invariant ──
    eps_hist = f.get("eps_by_year") or {}
    eps_unit = f.get("eps_unit") or ""
    shares = _num(f.get("shares"))
    ni = _num(f.get("net_income"))
    latest_eps = eps_hist[max(eps_hist)] if eps_hist else None

    r["ttm_eps"] = latest_eps
    r["eps_unit"] = eps_unit
    r["shares_outstanding"] = shares
    r["net_income"] = ni
    r["ads"] = ads_ratio(cap, price, shares)

    u = units_check(latest_eps, shares, ni)
    r["units"] = u
    if u["ok"] is False:
        return done("units_unverified")

    # ── the multiple, from cap over earnings ──
    if ni is None:
        return done("no_earnings")
    pe_info = price_earnings(cap, ni)
    r.update({"pe_ratio": pe_info["pe"], "pe_raw": pe_info["pe_raw"],
              "pe_suspect": pe_info["suspect"]})
    if pe_info["profitable"] is False:
        return done("unprofitable")
    if pe_info["suspect"]:
        return done("pe_suspect")
    if pe_info["pe"] is None or pe_info["pe"] > PE_MAX:
        return done("pe_high")

    # ── growth ──
    g = growth(eps_hist)
    r["growth"] = g
    r["eps_3yr_cagr_pct"] = g["cagr"]
    r["eps_cagr_raw_pct"] = g["cagr_raw"]
    if g["years"] < MIN_EPS_YEARS + 1:
        return done("short_history")
    if g["trough"]:
        return done("trough_base")
    if g["step"]:
        return done("step_change")
    if g["cagr"] is None or g["cagr"] < EPS_GROWTH_MIN:
        return done("no_growth")
    r["peg"] = peg(pe_info["pe"], g["cagr"])

    # ── debt: the schloss verdict, not a reimplementation ──
    ok, ratio = S.debt_ok(f.get("short_term_debt"), f.get("long_term_debt"),
                          equity, f.get("total_liabilities"))
    r["debt_to_equity"] = ratio
    r["debt_known"] = ok is not None
    if ok is None:
        return done("debt_unknown")
    if ratio is not None and ratio > DEBT_TO_EQUITY_MAX:
        return done("debt_high")

    # ── capex intensity ──
    capex, ocf = _num(f.get("capex")), _num(f.get("ocf"))
    r["ttm_capex"], r["ttm_ocf"] = capex, ocf
    if capex is None or ocf is None or ocf <= 0:
        return done("capex_unknown")
    ppe = _num(f.get("ppe"))
    # $6.6m of capex against $15.3bn of cash flow is not an asset-light
    # business, it is an unmeasured numerator — a filer using a tag this
    # build does not read.
    if ppe and ppe > 0 and capex < ppe * 0.005:
        r["capex_suspect"] = True
        return done("capex_unknown")
    r["capex_to_ocf"] = round(capex / ocf, 2)
    if r["capex_to_ocf"] > CAPEX_TO_OCF_MAX:
        return done("capex_high")

    r["moat"] = moat(f)
    r["has_moat"] = r["moat"]["has_moat"]
    # The last five annual EPS figures, rendered on the row. Every field
    # the page needed to expose its own broken rows was already in the
    # snapshot and none was displayed — which is how a 48-year profit
    # streak on a 2007 IPO and a 1.4% operating margin beside a 27% ROE
    # survived three monthly builds.
    r["eps_path"] = [v for _y, v in sorted(eps_hist.items())[-5:]]
    r["location"] = location(f)
    return done("pass")


def location(f: dict) -> str:
    """Where the filer is, as far as EDGAR will say.

    The old page split the board into "United States" and "International
    ADRs" on the basis of `files 20-F and no 10-K`, which is a filing fact
    wearing a geography label — a Canadian 40-F filer, an Israeli issuer
    and a Chinese VIE all landed in one bucket, and a blank exchange
    string passed into the US table. Location is a column now, and an
    unknown one says so rather than defaulting to a country.
    """
    state = (f.get("state") or "").strip()
    if state and state.lower() != "united states":
        return state
    if f.get("is_international"):
        return "Foreign filer"
    return state or ""


def _stale(last_filing: str | None, as_of: str | None) -> bool:
    if not last_filing:
        return True
    try:
        last = date.fromisoformat(str(last_filing)[:10])
        ref = date.fromisoformat(str(as_of)[:10]) if as_of else date.today()
    except (ValueError, TypeError):
        return False
    return (ref - last).days > STALE_DAYS


def size_band(cap: float | None) -> str:
    c = _num(cap)
    if c is None:
        return ""
    if c < 50e6:
        return "NANO"
    if c < 300e6:
        return "MICRO"
    if c < 2e9:
        return "SMALL"
    if c < 10e9:
        return "MID"
    return "LARGE"


def moat(f: dict) -> dict:
    """The Buffett overlay — informative, defaulted off.

    ROE is bounded for the same reason compounders bounds ROIC: as equity
    approaches zero the ratio stops describing the business. Operating
    margin is now RETURNED AND RENDERED rather than computed and
    discarded — Xunlei carried a 1.4% operating margin beside a 27% ROE
    for three monthly builds because nothing showed it.
    """
    ni_by_year = f.get("net_income_by_year") or {}
    eq_by_year = f.get("equity_by_year") or {}
    eps_hist = f.get("eps_by_year") or {}

    roes = []
    for y in sorted(set(ni_by_year) & set(eq_by_year))[-3:]:
        eq = _num(eq_by_year[y])
        ni = _num(ni_by_year[y])
        if eq and eq > 0 and ni is not None:
            roes.append(ni / eq * 100)
    avg_roe = sum(roes) / len(roes) if roes else None
    roe_suspect = avg_roe is not None and avg_roe > 100.0
    if roe_suspect:
        avg_roe = 100.0

    pos = 0
    for _y, v in sorted(eps_hist.items(), reverse=True):
        if _num(v) is not None and v > 0:
            pos += 1
        else:
            break

    op = _num(f.get("op_income"))
    rev = _num(f.get("revenue"))
    op_margin = round(op / rev * 100, 1) if (op is not None and rev and rev > 0) else None

    return {
        "avg_roe_3yr_pct": round(avg_roe, 1) if avg_roe is not None else None,
        "roe_capped": roe_suspect,
        "years_positive_eps": pos,
        "op_margin_pct": op_margin,
        "has_moat": bool(avg_roe is not None and avg_roe >= MOAT_ROE_MIN
                         and pos >= MOAT_POS_EPS_YEARS),
    }


# ═══════════════════════════════════════════════════════════════════
# CENSUS — what makes a nine-row board falsifiable
# ═══════════════════════════════════════════════════════════════════

def census(rows: list[dict]) -> dict:
    """The funnel. Every rejection counted under the reason that caused it.

    Without this, the same snapshot is produced by a strict screen in a
    rich market and by a parser that rejects everything, and there is no
    way to tell which one you are looking at. It matters most at the
    bottom of the market-cap range, where XBRL tagging is thinnest and
    most of a new universe will vanish through a rejection rather than a
    verdict.
    """
    counts: dict[str, int] = {}
    for r in rows:
        code = r.get("reason") or "unknown"
        counts[code] = counts.get(code, 0) + 1
    passing = counts.get("pass", 0)
    unmeasured = sum(counts.get(k, 0) for k in
                     ("units_unverified", "debt_unknown", "capex_unknown",
                      "no_equity", "no_revenue", "no_earnings", "no_cap",
                      "no_price", "cap_implausible"))
    return {
        "screened": len(rows),
        "passing": passing,
        "rejected": len(rows) - passing,
        "unmeasured": unmeasured,
        "by_reason": dict(sorted(counts.items(), key=lambda kv: -kv[1])),
        "labels": {k: v for k, v in REASONS.items() if k in counts},
    }


# ═══════════════════════════════════════════════════════════════════
# PAGE SIDE
# ═══════════════════════════════════════════════════════════════════

def median(values: list) -> float | None:
    """The actual median.

    The page used `f[Math.floor(f.length/2)]`, the UPPER middle value, so
    every tile was biased high — on six rows the three "medians" printed
    were Atour's row exactly.
    """
    vals = sorted(v for v in values if _num(v) is not None)
    if not vals:
        return None
    n = len(vals)
    return vals[n // 2] if n % 2 else (vals[n // 2 - 1] + vals[n // 2]) / 2


def load(month: str | None = None) -> dict:
    """A snapshot by month, or the newest available."""
    try:
        files = sorted(_DATA_DIR.glob("*.json"))
    except OSError:
        return {}
    if not files:
        return {}
    target = None
    if month:
        for p in files:
            if p.stem == month:
                target = p
    target = target or files[-1]
    try:
        with open(target, encoding="utf-8") as fh:
            d = json.load(fh)
        return d if isinstance(d, dict) else {}
    except (OSError, ValueError):
        return {}


def available_months() -> list[str]:
    try:
        return sorted((p.stem for p in _DATA_DIR.glob("*.json")), reverse=True)
    except OSError:
        return []


def summary(rows: list[dict]) -> dict:
    """Tile figures, computed from a real median, with n reported so three
    tiles describing different sample sizes cannot pretend otherwise."""
    passing = [r for r in rows if r.get("verdict") == "pass"]
    return {
        "count": len(passing),
        "with_moat": sum(1 for r in passing if r.get("has_moat")),
        "pe": median([r.get("pe_ratio") for r in passing]),
        "pe_n": sum(1 for r in passing if r.get("pe_ratio") is not None),
        "growth": median([r.get("eps_3yr_cagr_pct") for r in passing]),
        "growth_n": sum(1 for r in passing if r.get("eps_3yr_cagr_pct") is not None),
        "peg": median([r.get("peg") for r in passing]),
        "peg_n": sum(1 for r in passing if r.get("peg") is not None),
    }

"""Monthly data build for /compounders — the 14%/yr long-term screen.

Three stages, all free sources, designed for the GitHub Actions runner
(the dev sandbox can't reach EDGAR/Yahoo — test with --limit there and
expect network failures; the real run happens in CI):

  A. UNIVERSE — self-discovering for US filers, seeded for the rest.
     SEC XBRL "frames" returns one concept for EVERY filer in a couple of
     calls. We union annual revenue frames across the last two calendar
     years and keep every company above the revenue floor, exchange-listed
     only (company_tickers_exchange.json), financials excluded by SIC.

     THE FRAMES ARE us-gaap ONLY. EDGAR's frames endpoint 404s for
     ifrs-full concepts, so a foreign private issuer that reports under
     IFRS cannot be discovered this way at all — no matter how large.
     ADR_SEEDS force-adds a hand-kept list of majors to paper over that,
     which means international coverage is exactly as good as that list
     and no better. Stage B does resolve ifrs-full TAGS, but resolving the
     tag was never the whole job — see the currency note there.

  B. FUNDAMENTALS — per CIK: companyfacts (10y of annual XBRL) +
     submissions (SIC code, country). Tag maps cover us-gaap AND
     ifrs-full so 20-F ADRs work. Extracts revenue, net income, gross
     profit, operating income, OCF, capex, diluted shares, debt, cash,
     equity → computes CAGRs, consistency counts, margins + trend,
     ROIC series, FCF conversion, net-debt/EBIT, buyback rate.

     CURRENCY. A 20-F filer reports in its own currency, and NOTHING here
     converts. Every ratio — growth, margins, ROIC, FCF conversion,
     capex/OCF, net debt/EBIT — is safe, because both sides are in the
     same money. The valuation term is not: P/FCF divides a
     native-currency FCF per share by a USD ADR price. The reporting
     currency is therefore recorded per company and, when it is not USD,
     the valuation term is WITHHELD rather than computed. Toyota's P/FCF
     read 0.2 and Sony's 0.0 before this; those were yen over dollars.

  C. MARKET — Yahoo chart per ticker (7y monthly + dividends): current
     price, TTM dividend yield, dividend CAGR, FCF-multiple history
     (year-average price ÷ FCF/share) for the valuation-drift term.

Output: data/compounders.json — compact per-ticker METRICS only (a few
KB per name). All scoring/thresholds live in compounders.py so tuning
the screen never requires a refetch.

SEC fair-access: ≤10 req/s allowed. Stage B runs concurrently against a
shared token bucket at 8/s; stage C stays serial because Yahoo is an
undocumented endpoint that rate-blocks these runners and concurrency is
what took the quiet-value screen down.
"""
from __future__ import annotations

import argparse
import json
import statistics
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path


class _Throttle:
    """Token bucket shared across worker threads. SEC bans on rate, and a
    per-thread sleep does not bound the aggregate — six threads sleeping
    0.34s each is 18 req/s, not 3."""

    def __init__(self, per_second: float):
        self._gap = 1.0 / per_second
        self._lock = threading.Lock()
        self._next = 0.0

    def wait(self) -> None:
        with self._lock:
            now = time.monotonic()
            due = max(now, self._next)
            self._next = due + self._gap
        if due > now:
            time.sleep(due - now)

SEC_UA = "market-pulse-research admin@focusedops.io"
HEADERS_SEC = {"User-Agent": SEC_UA, "Accept-Encoding": "gzip, deflate"}
HEADERS_YAHOO = {"User-Agent": "Mozilla/5.0 (market-pulse-refresh/1.0)",
                 "Accept": "application/json"}

SEC_SLEEP = 0.34          # used only by the single-threaded frames stage
YAHOO_SLEEP = 0.3

# SEC permits 10 req/s. The old run made every call on one thread behind a
# 0.34s sleep — about 3/s — which was tolerable at a $1B floor and is
# precisely why lowering that floor would otherwise turn a 70-minute job
# into a three-hour one. The fundamentals stage now runs concurrently
# against a shared token bucket, so throughput is governed by a stated
# rate limit instead of a sleep multiplied by however many names qualify.
SEC_RATE = 8.0            # requests/second, shared across all workers
SEC_WORKERS = 6

# THE FLOOR IS THE UNIVERSE. Nothing else here is remotely as
# load-bearing: at $1B it admitted 1,582 names and the screen scored 901.
# Revenue is used ONLY to bound the work — every quality judgement happens
# later, against the filings — so a lower floor widens what gets
# considered without loosening a single gate.
#
# $250M keeps the whole mid-cap tier and still stops short of the
# micro-cap universe, where a clean decade of XBRL usually does not exist
# and the 14%/yr question is a different question anyway.
MIN_REVENUE = 250_000_000
MIN_YEARS = 7                    # need ≥7 fiscal years to score
MAX_UNIVERSE = 6000              # hard cap on CIKs processed

# Yahoo supplies price, dividend yield and the P/FCF history behind the
# valuation term. If it stops answering, every row STILL computes from
# EDGAR and the screen quietly becomes a growth-and-quality board with no
# valuation in it — worse than failing, because it looks like it worked.
# Below this share of scored names carrying market data, refuse to
# publish. Yahoo already blocks these runners intermittently.
MIN_MARKET_COVERAGE = 0.60

# Financials excluded: banks/brokers/insurers have no meaningful
# capex/gross-margin/FCF in this framework (SIC 6000-6499 + 6700s
# holding/investment offices). REITs (6500s) fail FCF gates naturally
# but are excluded here too — different return math.
def _is_financial_sic(sic: int | None) -> bool:
    return sic is not None and 6000 <= sic <= 6799


# ── XBRL tag maps (us-gaap first, then ifrs-full for 20-F ADRs) ──────
TAGS: dict[str, list[tuple[str, str]]] = {
    "revenue": [
        ("us-gaap", "Revenues"),
        ("us-gaap", "RevenueFromContractWithCustomerExcludingAssessedTax"),
        ("us-gaap", "RevenueFromContractWithCustomerIncludingAssessedTax"),
        ("us-gaap", "SalesRevenueNet"),
        ("ifrs-full", "Revenue"),
        ("ifrs-full", "RevenueFromContractsWithCustomers"),
    ],
    "net_income": [
        ("us-gaap", "NetIncomeLoss"),
        ("us-gaap", "ProfitLoss"),
        ("ifrs-full", "ProfitLossAttributableToOwnersOfParent"),
        ("ifrs-full", "ProfitLoss"),
    ],
    "gross_profit": [
        ("us-gaap", "GrossProfit"),
        ("ifrs-full", "GrossProfit"),
    ],
    "op_income": [
        ("us-gaap", "OperatingIncomeLoss"),
        ("ifrs-full", "ProfitLossFromOperatingActivities"),
    ],
    "ocf": [
        ("us-gaap", "NetCashProvidedByUsedInOperatingActivities"),
        ("us-gaap", "NetCashProvidedByUsedInOperatingActivitiesContinuingOperations"),
        ("ifrs-full", "CashFlowsFromUsedInOperatingActivities"),
    ],
    "capex": [
        ("us-gaap", "PaymentsToAcquirePropertyPlantAndEquipment"),
        ("us-gaap", "PaymentsToAcquireProductiveAssets"),
        ("ifrs-full", "PurchaseOfPropertyPlantAndEquipmentClassifiedAsInvestingActivities"),
    ],
    "shares_diluted": [
        ("us-gaap", "WeightedAverageNumberOfDilutedSharesOutstanding"),
        ("us-gaap", "WeightedAverageNumberOfSharesOutstandingBasic"),
        ("ifrs-full", "WeightedAverageShares"),
        ("dei", "EntityCommonStockSharesOutstanding"),
    ],
    "cash": [
        ("us-gaap", "CashAndCashEquivalentsAtCarryingValue"),
        ("us-gaap", "CashCashEquivalentsRestrictedCashAndRestrictedCashEquivalents"),
        ("ifrs-full", "CashAndCashEquivalents"),
    ],
    "lt_debt": [
        ("us-gaap", "LongTermDebtNoncurrent"),
        ("us-gaap", "LongTermDebt"),
        ("ifrs-full", "NoncurrentPortionOfNoncurrentBorrowings"),
        ("ifrs-full", "Borrowings"),
    ],
    "st_debt": [
        ("us-gaap", "LongTermDebtCurrent"),
        ("us-gaap", "DebtCurrent"),
        ("us-gaap", "ShortTermBorrowings"),
        ("ifrs-full", "CurrentPortionOfNoncurrentBorrowings"),
    ],
    "equity": [
        ("us-gaap", "StockholdersEquity"),
        ("us-gaap", "StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest"),
        ("ifrs-full", "EquityAttributableToOwnersOfParent"),
        ("ifrs-full", "Equity"),
    ],
}

ANNUAL_FORMS = {"10-K", "10-K/A", "20-F", "20-F/A", "40-F", "40-F/A"}


def _get(url: str, timeout: int = 60, headers: dict | None = None) -> dict:
    req = urllib.request.Request(url, headers=headers or HEADERS_SEC)
    with urllib.request.urlopen(req, timeout=timeout) as r:
        raw = r.read()
        if r.headers.get("Content-Encoding") == "gzip":
            import gzip
            raw = gzip.decompress(raw)
        return json.loads(raw)


# ── Stage A: universe ────────────────────────────────────────────────

FRAME_TAGS = [
    ("us-gaap", "Revenues"),
    ("us-gaap", "RevenueFromContractWithCustomerExcludingAssessedTax"),
    # NOTE: EDGAR's frames API 404s for ifrs-full concepts (confirmed in
    # run logs) — IFRS-only 20-F ADRs can't self-discover through frames.
    # ADR_SEEDS below patches the gap for the majors.
]

# Large ADRs whose fundamentals live under IFRS tags only. They're in
# company_tickers_exchange.json (SEC registrants), so we resolve their
# CIKs from the ticker map and force-add them to the universe; their
# actual revenue check happens naturally in compute_metrics. Financials
# and China names still get filtered/badged downstream as usual.
ADR_SEEDS = [
    "TSM", "ASML", "NVO", "SAP", "AZN", "NVS", "SNY", "GSK", "UL", "DEO",
    "BUD", "SHEL", "TTE", "BP", "RIO", "BHP", "TM", "HMC", "SONY", "MUFG",
    "BABA", "PDD", "JD", "NTES", "BIDU", "TCOM", "YUMC", "INFY", "WIT",
    "SE", "MELI", "ARM", "STLA", "RACE", "SPOT", "TEAM", "ABBV",
]


def discover_universe(years: list[int], min_revenue: float = MIN_REVENUE) -> dict[int, float]:
    """{cik: best annual revenue} for every filer >= min_revenue, via
    frames. Union across tags and years (max wins) so non-calendar
    fiscal years and tag fragmentation don't drop real companies."""
    best: dict[int, float] = {}
    for year in years:
        for taxonomy, tag in FRAME_TAGS:
            url = (f"https://data.sec.gov/api/xbrl/frames/{taxonomy}/{tag}/USD/CY{year}.json")
            try:
                payload = _get(url)
            except (urllib.error.URLError, urllib.error.HTTPError, TimeoutError, OSError, ValueError) as e:
                print(f"[universe] frame {taxonomy}/{tag}/CY{year}: {e}")
                continue
            n = 0
            for row in payload.get("data", []):
                cik, val = row.get("cik"), row.get("val")
                if cik is None or not isinstance(val, (int, float)):
                    continue
                if val > best.get(cik, 0):
                    best[cik] = float(val)
                n += 1
            print(f"[universe] frame {taxonomy}/{tag}/CY{year}: {n} filers")
            time.sleep(SEC_SLEEP)
    return {c: v for c, v in best.items() if v >= min_revenue}


def ticker_map() -> dict[int, dict]:
    """{cik: {ticker, name, exchange}} for exchange-listed companies,
    first (most senior) listing wins so GOOG/GOOGL dedupe to one."""
    payload = _get("https://www.sec.gov/files/company_tickers_exchange.json")
    fields = payload.get("fields") or []
    idx = {f: i for i, f in enumerate(fields)}
    out: dict[int, dict] = {}
    for row in payload.get("data", []):
        try:
            cik = int(row[idx["cik"]])
            exch = (row[idx["exchange"]] or "").strip()
            if cik in out or not exch:
                continue
            out[cik] = {"ticker": str(row[idx["ticker"]]).replace(".", "-"),
                        "name": row[idx["name"]], "exchange": exch}
        except (KeyError, ValueError, TypeError, IndexError):
            continue
    return out


def fetch_profile(cik: int) -> dict:
    """SIC code + HQ country from the submissions API."""
    url = f"https://data.sec.gov/submissions/CIK{cik:010d}.json"
    try:
        s = _get(url)
    except (urllib.error.URLError, urllib.error.HTTPError, TimeoutError, OSError, ValueError):
        return {}
    addr = (s.get("addresses") or {}).get("business") or {}
    # US filers report a state name here; foreign filers a country name.
    # A US state description still means "United States" for our tiers,
    # and _country_assess treats anything unrecognized as US-tier anyway.
    country = (addr.get("stateOrCountryDescription") or "").strip()
    try:
        sic = int(s.get("sic") or 0) or None
    except (TypeError, ValueError):
        sic = None
    return {"sic": sic, "sic_desc": s.get("sicDescription") or "",
            "country_desc": country or "United States"}


# ── Stage B: fundamentals ────────────────────────────────────────────

def _rows_for(node: dict, unit: str) -> dict[int, float]:
    """{fiscal_year: value} for ONE tag in ONE unit. Annual = FY frame
    from an annual form; amended filings dedupe by keeping the last."""
    series: dict[int, float] = {}
    for v in (node.get("units") or {}).get(unit, []):
        if v.get("form") not in ANNUAL_FORMS:
            continue
        if v.get("fp") not in ("FY", None):
            continue
        fy, val = v.get("fy"), v.get("val")
        if fy is None or not isinstance(val, (int, float)):
            continue
        # Durational facts must span ~a year; instants have no start.
        # Guard quarter-length values sneaking in as FY.
        start, end = v.get("start"), v.get("end")
        if start and end and (int(end[:4]) - int(start[:4])) == 0 \
                and end[5:7] != "12" and (int(end[5:7]) - int(start[5:7])) < 9:
            continue
        series[int(fy)] = float(val)
    return series


def _annual_series(facts: dict, slots: list[tuple[str, str]],
                   want_unit: str = "USD") -> tuple[dict[int, float], str]:
    """{fiscal_year: value} merged across every tag reporting in ONE unit,
    plus the unit used.

    TWO BUGS THIS REPLACES, both caused by taking the first thing found.

    1. FIRST TAG WINS — a decade frozen at 2017. The old version returned
       the first tag with >=3 years and stopped there. ASC 606 moved
       essentially every US company off `Revenues` and onto
       `RevenueFromContractWithCustomer...` for fiscal years beginning
       after Dec 2017, so any company with three pre-606 years locked onto
       the dead tag and never advanced. That was 114 of 901 names — 13% of
       the board — Broadridge and Maximus among them, both carrying a
       headline growth figure computed from a series ending in 2017.

       Merging fixes it. Earlier slots still win any year they cover, so
       the existing preference order is unchanged where it applies; later
       slots only ADD years the earlier ones lack. Pre- and post-606
       revenue are not an identical basis, which is a real caveat and is
       stated on the page — but a spliced series is vastly closer to the
       truth than one that stops nine years ago.

    2. FIRST UNIT WINS — yen divided by dollars. companyfacts keys values
       by unit and the old version broke on whichever came first in the
       JSON. Foreign filers arrived in TWD/JPY/CNY/INR and were then
       divided by a USD ADR price: Toyota's P/FCF read 0.2, Sony's 0.0.
       The unit is now chosen deliberately, preferring `want_unit`, and is
       RETURNED so the caller can refuse to mix it with a dollar price.
       Two units are never spliced into one series.
    """
    # Pass 1: choose the unit ONCE across all slots. Per-slot selection
    # would let a stray non-USD tag set the currency for the whole series
    # merely by appearing first.
    available: set[str] = set()
    for taxonomy, tag in slots:
        node = (facts.get(taxonomy) or {}).get(tag)
        if node:
            available |= set((node.get("units") or {}).keys())
    if not available:
        return {}, ""
    # Deterministic fallback, so a company's currency cannot change
    # between runs just because the SEC reordered a JSON object.
    unit = want_unit if want_unit in available else sorted(available)[0]

    # Pass 2: merge every slot that speaks that unit.
    merged: dict[int, float] = {}
    for taxonomy, tag in slots:
        node = (facts.get(taxonomy) or {}).get(tag)
        if not node:
            continue
        for fy, val in _rows_for(node, unit).items():
            merged.setdefault(fy, val)      # earlier slots win the year
    return (merged, unit) if len(merged) >= 3 else ({}, unit)


def _cagr(series: dict[int, float], years: int) -> float | None:
    if not series:
        return None
    ys = sorted(series)
    last = ys[-1]
    first = last - years
    if first not in series:
        # nearest available at least years-1 back
        candidates = [y for y in ys if y <= last - (years - 1)]
        if not candidates:
            return None
        first = candidates[-1]
    a, b = series[first], series[last]
    span = last - first
    if a <= 0 or b <= 0 or span < 3:
        return None
    return round(((b / a) ** (1 / span) - 1) * 100, 2)


def _up_years(series: dict[int, float], window: int = 10) -> tuple[int, int]:
    ys = sorted(series)[-(window + 1):]
    ups = total = 0
    for i in range(1, len(ys)):
        total += 1
        if series[ys[i]] > series[ys[i - 1]]:
            ups += 1
    return ups, total


# Share counts are reported in "shares", everything else in a currency.
# Asking for USD on a share count would fall through to the deterministic
# fallback and quietly pick something wrong.
WANT_UNIT = {"shares_diluted": "shares"}


def compute_metrics(facts: dict) -> dict | None:
    pulled = {k: _annual_series(facts, slots, WANT_UNIT.get(k, "USD"))
              for k, slots in TAGS.items()}
    s = {k: v[0] for k, v in pulled.items()}

    # The reporting currency of the MONEY series. Revenue is the anchor:
    # if a company reports revenue in yen, every other money figure is in
    # yen too, and none of them may be compared to a USD ADR price.
    currency = pulled["revenue"][1] or "USD"

    rev, ni, ocf = s["revenue"], s["net_income"], s["ocf"]
    if len(rev) < MIN_YEARS or len(ni) < MIN_YEARS - 1 or len(ocf) < MIN_YEARS - 1:
        return None
    years = sorted(rev)
    last = years[-1]

    op, gp, capex = s["op_income"], s["gross_profit"], s["capex"]
    shares, cash, equity = s["shares_diluted"], s["cash"], s["equity"]
    lt, st = s["lt_debt"], s["st_debt"]

    def margin_series(num: dict[int, float]) -> dict[int, float]:
        return {y: num[y] / rev[y] * 100 for y in num if y in rev and rev[y] > 0}

    op_m = margin_series(op)
    gp_m = margin_series(gp)

    # ROIC per year ≈ op income × (1 − 23%) ÷ (equity + debt − cash)
    roics = []
    for y in sorted(op):
        if y not in equity:
            continue
        invested = equity[y] + lt.get(y, 0.0) + st.get(y, 0.0) - cash.get(y, 0.0)
        if invested > 0:
            roics.append(op[y] * 0.77 / invested * 100)
    roic_med = round(statistics.median(roics), 1) if len(roics) >= 5 else None

    # FCF series + conversion vs net income (the cash-is-real gate).
    fcf = {y: ocf[y] - capex.get(y, 0.0) for y in ocf}
    yrs10 = [y for y in sorted(fcf) if y > last - 10]
    sum_fcf = sum(fcf[y] for y in yrs10)
    sum_ni = sum(ni[y] for y in yrs10 if y in ni)
    fcf_conv = round(sum_fcf / sum_ni * 100, 1) if sum_ni > 0 else None

    capex_ratio = None
    cap5 = [(capex.get(y, 0.0), ocf[y]) for y in sorted(ocf)[-5:] if ocf[y] > 0]
    if cap5:
        capex_ratio = round(sum(c for c, _ in cap5) / sum(o for _, o in cap5) * 100, 1)

    # Net debt / EBIT (proxy for leverage capacity).
    nd_ebit = None
    if last in op and op[last] > 0:
        nd = lt.get(last, 0.0) + st.get(last, 0.0) - cash.get(last, 0.0)
        nd_ebit = round(nd / op[last], 2)

    # Buybacks: 5-yr share-count CAGR (negative = shrinking count).
    shares_cagr5 = _cagr(shares, 5)

    op_m_vals = [op_m[y] for y in sorted(op_m)][-10:]
    op_m_now = op_m_vals[-1] if op_m_vals else None
    op_m_med = statistics.median(op_m_vals) if len(op_m_vals) >= 5 else None
    # Cycle position: current margin's percentile within own history.
    cycle_pos = None
    if op_m_vals and len(op_m_vals) >= 6 and op_m_now is not None:
        below = sum(1 for v in op_m_vals if v < op_m_now)
        cycle_pos = round(below / (len(op_m_vals) - 1) * 100)
    # Margin trend: slope of op margin, %-pts per year (last ≤10 yrs).
    margin_slope = None
    if len(op_m_vals) >= 6:
        n = len(op_m_vals)
        xs = list(range(n))
        mx, my = statistics.fmean(xs), statistics.fmean(op_m_vals)
        denom = sum((x - mx) ** 2 for x in xs)
        if denom:
            margin_slope = round(sum((xs[i] - mx) * (op_m_vals[i] - my) for i in range(n)) / denom, 2)
    # Cyclicality: margin variability + revenue chop.
    rev_ups, rev_tot = _up_years(rev)
    cyclical = False
    if op_m_vals and statistics.fmean(op_m_vals) > 0:
        cv = statistics.pstdev(op_m_vals) / abs(statistics.fmean(op_m_vals))
        cyclical = cv > 0.35 or (rev_tot >= 8 and rev_ups <= rev_tot - 4)

    ni_pos_years = sum(1 for y in sorted(ni)[-10:] if ni[y] > 0)

    fcf_ps_by_year = {y: fcf[y] / shares[y] for y in fcf
                      if y in shares and shares[y] > 0}

    return {
        "fy_last": last,
        "years": len(years),
        "currency": currency,
        "revenue_last": rev[last],
        "rev_cagr5": _cagr(rev, 5), "rev_cagr10": _cagr(rev, 10),
        "rev_up_years": rev_ups, "rev_up_total": rev_tot,
        "fcf_cagr5": _cagr({y: v for y, v in fcf.items() if v > 0}, 5),
        "ni_pos_years": ni_pos_years,
        "roic_med": roic_med,
        "fcf_conv": fcf_conv,
        "capex_ocf": capex_ratio,
        "nd_ebit": nd_ebit,
        "shares_cagr5": shares_cagr5,
        "gross_margin": round(statistics.median([gp_m[y] for y in sorted(gp_m)[-5:]]), 1) if len(gp_m) >= 3 else None,
        "op_margin_now": round(op_m_now, 1) if op_m_now is not None else None,
        "op_margin_med": round(op_m_med, 1) if op_m_med is not None else None,
        "margin_slope": margin_slope,
        "cycle_pos": cycle_pos,
        "cyclical": cyclical,
        "fcf_ps": {str(y): round(v, 4) for y, v in fcf_ps_by_year.items()},
        "fcf_last": fcf.get(last),
    }


def fetch_fundamentals(cik: int) -> dict | None:
    url = f"https://data.sec.gov/api/xbrl/companyfacts/CIK{cik:010d}.json"
    try:
        facts = (_get(url) or {}).get("facts") or {}
    except (urllib.error.URLError, urllib.error.HTTPError, TimeoutError, OSError, ValueError):
        return None
    return compute_metrics(facts)


# ── Stage C: market data ─────────────────────────────────────────────

def fetch_market(ticker: str, fcf_ps: dict[str, float]) -> dict | None:
    url = ("https://query1.finance.yahoo.com/v8/finance/chart/"
           f"{urllib.parse.quote(ticker, safe='')}?range=7y&interval=1mo&events=div")
    try:
        res = _get(url, headers=HEADERS_YAHOO)["chart"]["result"][0]
    except (urllib.error.URLError, urllib.error.HTTPError, TimeoutError,
            OSError, ValueError, KeyError, IndexError, TypeError):
        return None
    ts = res.get("timestamp") or []
    closes = (res.get("indicators", {}).get("quote") or [{}])[0].get("close") or []
    pts = [(t, c) for t, c in zip(ts, closes) if c]
    if len(pts) < 12:
        return None
    price = pts[-1][1]

    divs = sorted((int(d["date"]), float(d["amount"]))
                  for d in ((res.get("events") or {}).get("dividends") or {}).values()
                  if d.get("date") and d.get("amount"))
    now_ts = pts[-1][0]
    year = 365 * 24 * 3600
    ttm_div = sum(a for t, a in divs if now_ts - year < t <= now_ts)
    div_yield = round(ttm_div / price * 100, 2) if price > 0 else None
    # Dividend CAGR over the covered span (anchored inside history).
    div_cagr = None
    if divs and ttm_div > 0:
        anchor = max(now_ts - 5 * year, divs[0][0] + int(1.05 * year))
        then = sum(a for t, a in divs if anchor - year < t <= anchor)
        yrs = (now_ts - anchor) / year
        if then > 0 and yrs >= 2:
            div_cagr = round(((ttm_div / then) ** (1 / yrs) - 1) * 100, 1)

    # P/FCF now + historical median: year-average price ÷ that FY's FCF/share.
    from collections import defaultdict
    year_prices: dict[int, list[float]] = defaultdict(list)
    for t, c in pts:
        year_prices[datetime.fromtimestamp(t, tz=timezone.utc).year].append(c)
    mults = []
    for fy_str, f in fcf_ps.items():
        fy = int(fy_str)
        if f and f > 0 and year_prices.get(fy):
            mults.append(statistics.fmean(year_prices[fy]) / f)
    pfcf_med = round(statistics.median(mults), 1) if len(mults) >= 4 else None
    fcf_now = fcf_ps.get(max(fcf_ps.keys(), key=int)) if fcf_ps else None
    pfcf_now = round(price / fcf_now, 1) if fcf_now and fcf_now > 0 else None

    return {"price": round(price, 2), "div_yield": div_yield,
            "div_cagr5": div_cagr, "pfcf_now": pfcf_now, "pfcf_med": pfcf_med}


# ── Main ─────────────────────────────────────────────────────────────

def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=0,
                    help="Process only the N largest (testing).")
    ap.add_argument("--min-revenue", type=float, default=MIN_REVENUE,
                    help="Revenue floor in dollars. This IS the universe size dial.")
    ap.add_argument("--max-universe", type=int, default=MAX_UNIVERSE,
                    help="Hard cap on CIKs processed after the ticker join.")
    args = ap.parse_args()

    this_year = datetime.now(timezone.utc).year
    print("[compounders] Stage A: universe discovery via EDGAR frames…")
    revenue_by_cik = discover_universe([this_year - 2, this_year - 1],
                                       min_revenue=args.min_revenue)
    print(f"[compounders] {len(revenue_by_cik)} filers ≥ ${args.min_revenue/1e6:,.0f}M revenue")
    tickers = ticker_map()
    universe = [(cik, rev) for cik, rev in revenue_by_cik.items() if cik in tickers]
    universe.sort(key=lambda x: -x[1])
    universe = universe[:args.max_universe]
    # Force-add the IFRS-only ADR seeds (frames can't discover them).
    have = {cik for cik, _ in universe}
    by_ticker = {info["ticker"]: cik for cik, info in tickers.items()}
    seeded = 0
    for t in ADR_SEEDS:
        cik = by_ticker.get(t)
        if cik and cik not in have:
            universe.append((cik, args.min_revenue))  # verified from facts later
            have.add(cik)
            seeded += 1
    if args.limit:
        universe = universe[:args.limit]
    print(f"[compounders] {len(universe)} exchange-listed after ticker join "
          f"(+{seeded} ADR seeds)")

    # ── Stage B: fundamentals, concurrently ──────────────────────────
    # Two SEC calls per name (submissions + companyfacts) against one
    # shared rate limiter. This is the stage that scales with the floor,
    # so it is the stage that had to stop being serial.
    skipped = {"financial": 0, "no_facts": 0, "market": 0, "error": 0}
    lock = threading.Lock()
    throttle = _Throttle(SEC_RATE)
    # (cik, info, profile, metrics) — cik is carried explicitly because the
    # ticker map keys ON it and does not repeat it inside the value.
    scored: list[tuple[int, dict, dict, dict]] = []
    t0 = time.time()
    done_n = 0

    def fundamentals(entry: tuple[int, float]) -> None:
        nonlocal done_n
        cik, _rev = entry
        info = tickers[cik]
        # One malformed company must never kill a long run — catch
        # everything per-company, log it, move on.
        try:
            throttle.wait()
            profile = fetch_profile(cik)
            if _is_financial_sic(profile.get("sic")):
                with lock:
                    skipped["financial"] += 1
                return
            throttle.wait()
            metrics = fetch_fundamentals(cik)
            if metrics is None:
                with lock:
                    skipped["no_facts"] += 1
                return
            with lock:
                scored.append((cik, info, profile, metrics))
        except Exception as e:                              # noqa: BLE001
            with lock:
                skipped["error"] += 1
            print(f"[compounders] {info['ticker']} (CIK {cik}): "
                  f"{type(e).__name__}: {e} — skipped")
        finally:
            with lock:
                done_n += 1
                n = done_n
            if n % 250 == 0:
                rate = n / max(1e-9, time.time() - t0)
                print(f"[compounders] fundamentals {n}/{len(universe)} · "
                      f"kept {len(scored)} · ~{(len(universe)-n)/rate/60:.0f} min left")

    print(f"[compounders] Stage B: fundamentals for {len(universe)} CIKs "
          f"at {SEC_RATE:.0f} req/s across {SEC_WORKERS} workers…")
    with ThreadPoolExecutor(max_workers=SEC_WORKERS) as ex:
        list(ex.map(fundamentals, universe))
    print(f"[compounders] Stage B done in {(time.time()-t0)/60:.0f} min: "
          f"{len(scored)} scored, skipped {skipped}")
    if not scored:
        print("[compounders] Nothing survived the fundamentals stage — "
              "refusing to overwrite a good dataset.")
        return 2

    # ── Stage C: market data, gently and serially ────────────────────
    # Deliberately NOT parallelised. Yahoo is an undocumented endpoint
    # that rate-blocks these runners, and the quiet-value screen was
    # taken down by exactly the concurrency that would speed this up.
    # It only runs on names that already scored, so it is the short stage.
    print(f"[compounders] Stage C: market data for {len(scored)} names…")
    out: dict[str, dict] = {}
    with_market = 0
    non_usd = 0
    for i, (cik, info, profile, metrics) in enumerate(scored, 1):
        # THE VALUATION TERM IS THE ONLY CURRENCY-UNSAFE NUMBER HERE.
        # Growth, margins, ROIC, FCF conversion, capex/OCF and net
        # debt/EBIT are all ratios WITHIN one currency, so they are
        # correct whatever the filer reports in. P/FCF is not: it divides
        # a native-currency FCF per share by a USD ADR price. Withholding
        # fcf_ps blanks P/FCF and its history, which makes compounders.py
        # drop the valuation-drift term for that name — the row stays, its
        # quality and growth stay, and the one figure we cannot compute
        # honestly is absent rather than wrong.
        usd = (metrics.get("currency") or "USD") == "USD"
        if not usd:
            non_usd += 1
        try:
            market = fetch_market(info["ticker"],
                                  (metrics.get("fcf_ps") or {}) if usd else {})
        except Exception:                                   # noqa: BLE001
            market = None
        time.sleep(YAHOO_SLEEP)
        if market is None:
            skipped["market"] += 1
            market = {}
        else:
            with_market += 1
        row = {**metrics, **market,
               "name": info["name"], "cik": cik,
               "exchange": info["exchange"],
               "sic": profile.get("sic"), "industry": profile.get("sic_desc"),
               "country": profile.get("country_desc") or "United States"}
        row.pop("fcf_ps", None)   # working data — not needed in output
        out[info["ticker"]] = row
        if i % 250 == 0:
            print(f"[compounders] market {i}/{len(scored)} · {with_market} with prices")

    coverage = with_market / len(scored) if scored else 0.0
    print(f"[compounders] Done: kept {len(out)}, market coverage "
          f"{coverage:.0%}, {non_usd} reporting in a non-USD currency "
          f"(valuation term withheld for those), skipped {skipped}")

    # A board with no valuation term is not a smaller board, it is a
    # different and much weaker screen wearing the same name.
    if coverage < MIN_MARKET_COVERAGE and not args.limit:
        print(f"[compounders] Only {coverage:.0%} of names carry market data "
              f"(floor {MIN_MARKET_COVERAGE:.0%}). Without prices there is no "
              f"valuation term and no P/FCF — the screen would silently become "
              f"growth-and-quality only. Refusing to publish.")
        return 2
    if len(out) < 200 and not args.limit:
        print("[compounders] Far fewer names than expected — refusing to "
              "overwrite a good dataset with a bad run.")
        return 2

    payload = {
        "as_of": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
        "universe_input": len(universe),
        "count": len(out),
        "min_revenue": args.min_revenue,
        "market_coverage_pct": round(coverage * 100, 1),
        "non_usd_reporters": non_usd,
        "skipped": skipped,
        "tickers": out,
    }
    out_path = Path(__file__).resolve().parent.parent / "data" / "compounders.json"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as fh:
        json.dump(payload, fh, separators=(",", ":"), sort_keys=True)
        fh.write("\n")
    print(f"[compounders] ✓ Wrote {out_path} ({out_path.stat().st_size/1e6:.1f} MB)")
    return 0


if __name__ == "__main__":
    sys.exit(main())

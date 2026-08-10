"""Monthly data build for /compounders — the 14%/yr long-term screen.

Three stages, all free sources, designed for the GitHub Actions runner
(the dev sandbox can't reach EDGAR/Yahoo — test with --limit there and
expect network failures; the real run happens in CI):

  A. UNIVERSE — self-discovering, in three passes, no hand-kept list.
     SEC XBRL "frames" returns one concept for EVERY filer in a couple of
     calls. Exchange-listed only (company_tickers_exchange.json),
     financials excluded by SIC.

     1. us-gaap revenue frames, above the revenue floor. Covers domestic
        filers, and until recently was the ONLY pass — which is why
        international coverage used to be a 37-ticker list typed into
        this file.

     2. ifrs-full revenue frames, asked ONE REPORTING CURRENCY AT A TIME.
        The frames URL contains the unit, so asking for ifrs-full revenue
        in USD returns almost nothing and looks like an unsupported
        taxonomy. Asking in yen is a different question. No revenue floor
        applies here: a yen figure cannot be compared to a dollar floor
        without an FX rate, and one will not be invented to size a
        universe. Bounded by count instead.

     3. Fallback, only if (2) comes back near-empty: exchange-listed CIKs
        that frames never measured AT ALL. Distinct from "measured and
        below the floor" — that distinction is the whole point, because
        an unmeasured company may simply be reporting in a taxonomy the
        sweep cannot read.

     ADR_SEEDS survives as a backstop only, so that a discovery failure
     degrades to the coverage we already had rather than to none.

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
import math
import statistics
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timezone
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
MIN_YEARS = 7                    # need ≥7 fiscal years to score at all

# THE LOOKBACK. Fifteen years is not a preference, it is the ceiling: XBRL
# begins around 2010-11, so 44% of companies have 15 years, 22% have 16,
# and essentially none have 18. Going longer buys nothing that exists.
#
# It matters because a ten-year window is one expansion plus a pandemic.
# Fifteen spans the 2015-16 industrial recession, the 2018 tightening,
# 2020, and the 2022 rate shock — an actual cycle, which is the only thing
# that can distinguish a durable compounder from a company that has never
# been tested. And it demotes 2020 from a fifth of the window to a
# fifteenth.
LOOKBACK = 15
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
    # WIDENED after 162 rows came out at exactly 0.0% capex/OCF. The
    # generic PP&E tag is not what oil, mining, telecom and utility filers
    # use, and IFRS filers use a different element again. Missing capex is
    # now an honest None rather than a zero (see compute_metrics), so a tag
    # this list still fails to catch produces a GATED row and a badge
    # instead of a company that looks capital-light.
    "capex": [
        ("us-gaap", "PaymentsToAcquirePropertyPlantAndEquipment"),
        ("us-gaap", "PaymentsToAcquireProductiveAssets"),
        ("us-gaap", "PaymentsForCapitalImprovements"),
        ("us-gaap", "PaymentsToAcquireOilAndGasProperty"),
        ("us-gaap", "PaymentsToExploreAndDevelopOilAndGasProperties"),
        ("us-gaap", "PaymentsToAcquirePropertyPlantAndEquipmentAndIntangibleAssets"),
        ("ifrs-full", "PurchaseOfPropertyPlantAndEquipmentClassifiedAsInvestingActivities"),
        ("ifrs-full", "PaymentsToAcquirePropertyPlantAndEquipment"),
        ("ifrs-full", "AcquisitionOfPropertyPlantAndEquipment"),
    ],
    # Capitalised software, content and spectrum. Real reinvestment that
    # never touches the PP&E line — Comcast pays ~$2.8bn a year here on top
    # of $12.5bn of capex, so PP&E alone understates what it costs that
    # business to stand still. Added to capex, never substituted for it.
    "capex_intangible": [
        ("us-gaap", "PaymentsToAcquireIntangibleAssets"),
        ("us-gaap", "PaymentsToDevelopSoftware"),
        ("us-gaap", "PaymentsToAcquireSoftware"),
        ("ifrs-full", "PurchaseOfIntangibleAssetsClassifiedAsInvestingActivities"),
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

# A real annual period, in days. 52/53-week retail years run 357-371;
# transition periods and the odd long year stretch further. Anything
# outside this is a quarter, a half, or a stub — not a year.
ANNUAL_DAYS = (330, 400)


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
]

# ── Finding the foreign filers ───────────────────────────────────────
#
# TESTED, AND THE ORIGINAL NOTE WAS RIGHT. The old code said EDGAR's
# frames API "404s for ifrs-full concepts". The theory below — that this
# was really a UNIT problem, since the frames URL contains one — was
# plausible and is WRONG: run #6 swept all 20 currencies and matched zero
# CIKs. The sweep is kept because it costs ~80 calls, logs what it finds,
# and will start working the day the SEC indexes the taxonomy. The
# never-measured fallback below is what actually finds foreign filers.
#
# The reasoning, preserved so nobody re-derives it as a new idea:
#
#     /api/xbrl/frames/{taxonomy}/{tag}/{UNIT}/CY{year}.json
#
# An IFRS filer reports in its own currency. Asking for ifrs-full/Revenue
# in USD is asking for the handful of IFRS filers who happen to report in
# dollars — so a 404 or a near-empty answer is exactly what you would
# expect, whether or not the taxonomy is supported. Nobody had tried
# asking in yen.
#
# One run settled it. Every call returned nothing, and the exhaustive
# fallback took over — which is the branch that now does all the work.
IFRS_TAGS = ["Revenue", "RevenueFromContractsWithCustomers"]
IFRS_UNITS = [
    "USD", "EUR", "GBP", "JPY", "CHF", "CAD", "AUD", "CNY", "HKD", "TWD",
    "INR", "KRW", "BRL", "MXN", "SEK", "DKK", "NOK", "ILS", "SGD", "ZAR",
]

# Foreign candidates carry no comparable revenue figure — a floor in
# dollars cannot be applied to a number in yen without an FX rate we do
# not have and will not invent. They are admitted unfiltered and bounded
# by count instead. There are roughly 900 foreign private issuers filing
# 20-F with the SEC, so this is a generous ceiling, not a tight one.
# Run #6 hit this cap exactly, which means foreign coverage was truncated
# and the board silently under-represented international names — the thing
# this whole exercise set out to fix. That run took 27 minutes of a
# 150-minute budget, so the ceiling was costing coverage for no reason.
MAX_FOREIGN = 4000

# A BACKSTOP, no longer the mechanism. This list used to BE the entire
# international universe; discovery now finds foreign filers on its own.
# It is kept, and always force-added, so that a discovery failure degrades
# to the coverage we already had rather than to none — an empty foreign
# cohort must never be publishable as though the market had no foreign
# companies in it. If discovery is working, everything here is already
# found and the list adds nothing.
ADR_SEEDS = [
    "TSM", "ASML", "NVO", "SAP", "AZN", "NVS", "SNY", "GSK", "UL", "DEO",
    "BUD", "SHEL", "TTE", "BP", "RIO", "BHP", "TM", "HMC", "SONY", "MUFG",
    "BABA", "PDD", "JD", "NTES", "BIDU", "TCOM", "YUMC", "INFY", "WIT",
    "SE", "MELI", "ARM", "STLA", "RACE", "SPOT", "TEAM", "ABBV",
]


def _frame(taxonomy: str, tag: str, unit: str, year: int) -> list[dict]:
    """One frames call. A 404 means the combination does not exist, which
    for the per-currency IFRS sweep is the common and expected answer."""
    url = f"https://data.sec.gov/api/xbrl/frames/{taxonomy}/{tag}/{unit}/CY{year}.json"
    try:
        payload = _get(url)
    except urllib.error.HTTPError as e:
        if e.code != 404:
            print(f"[universe] frame {taxonomy}/{tag}/{unit}/CY{year}: HTTP {e.code}")
        return []
    except (urllib.error.URLError, TimeoutError, OSError, ValueError) as e:
        print(f"[universe] frame {taxonomy}/{tag}/{unit}/CY{year}: {e}")
        return []
    finally:
        time.sleep(SEC_SLEEP)
    return payload.get("data") or []


def discover_universe(years: list[int],
                      min_revenue: float = MIN_REVENUE) -> tuple[dict[int, float], set[int]]:
    """({cik: best annual revenue} for filers >= min_revenue, every cik seen).

    The second value matters as much as the first. A CIK that frames
    reported BELOW the floor was measured and excluded on purpose; a CIK
    frames never mentioned at all was not measured, and might be a foreign
    filer whose revenue lives in a taxonomy this sweep does not read.
    Collapsing those two cases is what made foreign companies invisible.
    """
    best: dict[int, float] = {}
    seen: set[int] = set()
    for year in years:
        for taxonomy, tag in FRAME_TAGS:
            rows = _frame(taxonomy, tag, "USD", year)
            n = 0
            for row in rows:
                cik, val = row.get("cik"), row.get("val")
                if cik is None or not isinstance(val, (int, float)):
                    continue
                seen.add(int(cik))
                if val > best.get(cik, 0):
                    best[cik] = float(val)
                n += 1
            print(f"[universe] frame {taxonomy}/{tag}/USD/CY{year}: {n} filers")
    return {c: v for c, v in best.items() if v >= min_revenue}, seen


def discover_ifrs(years: list[int]) -> set[int]:
    """CIKs of IFRS filers, asked for one reporting currency at a time.

    No revenue floor: a yen figure cannot be compared to a dollar floor
    without an FX rate, and inventing one to size a universe would put a
    fabricated number in the middle of the pipeline. The cohort is bounded
    by count instead.
    """
    found: set[int] = set()
    hits: dict[str, int] = {}
    for year in years:
        for tag in IFRS_TAGS:
            for unit in IFRS_UNITS:
                rows = _frame("ifrs-full", tag, unit, year)
                n = 0
                for row in rows:
                    cik = row.get("cik")
                    if cik is not None:
                        found.add(int(cik))
                        n += 1
                if n:
                    hits[f"{tag}/{unit}"] = hits.get(f"{tag}/{unit}", 0) + n
    if hits:
        top = sorted(hits.items(), key=lambda kv: -kv[1])[:8]
        print(f"[universe] IFRS frames answered: {len(found)} distinct CIKs. "
              f"Best: {', '.join(f'{k}={v}' for k, v in top)}")
    else:
        print("[universe] IFRS frames returned NOTHING for any currency — the "
              "taxonomy really is unsupported by the frames endpoint. Falling "
              "back to the exhaustive scan.")
    return found


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


# EDGAR's stateOrCountry CODES for non-US locations. The DESCRIPTION field
# alongside them is blank for about half of foreign private issuers, which
# is what produced 160 Chinese, Japanese, Taiwanese and Indian companies
# printed as "United States". The code is populated far more reliably, so
# it is read as the fallback. US state codes are deliberately absent: a
# two-letter state is already what the description carries for domestic
# filers, and the Location column shows it as-is.
EDGAR_COUNTRY_CODES = {
    "B0": "Canada", "A0": "Alberta, Canada", "A1": "British Columbia, Canada",
    "A2": "Manitoba, Canada", "A3": "New Brunswick, Canada",
    "A4": "Newfoundland, Canada", "A5": "Nova Scotia, Canada",
    "A6": "Ontario, Canada", "A7": "Prince Edward Island, Canada",
    "A8": "Quebec, Canada", "A9": "Saskatchewan, Canada",
    "C3": "Australia", "C5": "Bahamas", "D0": "Belgium", "D5": "British Virgin Islands",
    "D8": "Bermuda", "D6": "Brazil", "F4": "China", "G0": "Cyprus",
    "G7": "Denmark", "H6": "Finland", "I0": "France", "2M": "Germany",
    "L2": "Hong Kong", "K7": "India", "L6": "Indonesia", "L8": "Ireland",
    "L3": "Israel", "L6I": "Italy", "M0": "Japan", "M5": "Jersey",
    "M4": "Kazakhstan", "M8": "Korea, Republic of", "N0": "Luxembourg",
    "N5": "Malaysia", "O5": "Mexico", "P7": "Netherlands", "Q2": "New Zealand",
    "Q8": "Norway", "R0": "Panama", "R6": "Philippines", "R8": "Poland",
    "S1": "Singapore", "T3": "South Africa", "U3": "Spain", "V7": "Sweden",
    "V8": "Switzerland", "F5": "Taiwan", "W1": "Thailand", "X0": "United Kingdom",
    "X2": "Cayman Islands", "Y6": "Bermuda", "Y7": "Marshall Islands",
    "1C": "Greece", "1E": "Guernsey", "1F": "Isle of Man", "K3": "Iceland",
}


def fetch_profile(cik: int) -> dict:
    """SIC code + HQ location from the submissions API.

    NEVER MANUFACTURES A COUNTRY. The previous version ended in
    `country or "United States"`, and EDGAR leaves the business-address
    country description blank for roughly half of foreign private issuers
    — so that fallback printed Alibaba, Baidu, NetEase, Weibo, PDD, TSMC,
    Sony and Infosys as US companies. 160 of 339 20-F/40-F filers on the
    last board. It also suppressed the China, Taiwan and sanctions badges
    for exactly those rows, because the badge logic reads the country
    field that had just been overwritten.

    Four sources, best first, and an empty string when none of them
    answers. Downstream decides what to show for unknown; this function's
    only job is to avoid inventing a fact.
    """
    url = f"https://data.sec.gov/submissions/CIK{cik:010d}.json"
    try:
        s = _get(url)
    except (urllib.error.URLError, urllib.error.HTTPError, TimeoutError, OSError, ValueError):
        return {}
    addrs = s.get("addresses") or {}
    biz = addrs.get("business") or {}
    mail = addrs.get("mailing") or {}
    # NOTE: this is a business ADDRESS, so US filers report a state name
    # here, not a country. The page labels the column "Location" for that
    # reason — 804 of 901 rows read "TX"/"CA"/"NY" when it said "Country".
    country = (biz.get("stateOrCountryDescription") or "").strip()
    if not country:
        country = (mail.get("stateOrCountryDescription") or "").strip()
    if not country:
        for src in (biz, mail):
            code = (src.get("stateOrCountry") or "").strip().upper()
            if code in EDGAR_COUNTRY_CODES:
                country = EDGAR_COUNTRY_CODES[code]
                break
    try:
        sic = int(s.get("sic") or 0) or None
    except (TypeError, ValueError):
        sic = None
    # Foreign private issuers file 20-F; Canadian MJDS filers file 40-F.
    # The form type is the fact — incorporation and address are both
    # unreliable proxies (AerCap and Allegion are Irish and show a US
    # business address).
    forms = ((s.get("filings") or {}).get("recent") or {}).get("form") or []
    foreign = any(f.split("/")[0] in ("20-F", "40-F", "6-K", "40-FR") for f in forms)
    return {"sic": sic, "sic_desc": s.get("sicDescription") or "",
            "country_desc": country,
            "incorporation": (s.get("stateOfIncorporationDescription") or "").strip(),
            "foreign_filer": foreign}


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
        # MEASURE THE PERIOD, DO NOT PATTERN-MATCH THE DATES.
        #
        # The previous guard read: same calendar year AND does not end in
        # December AND spans under nine months. The middle clause was
        # meant to protect calendar-year annuals and instead whitelisted
        # every period ENDING in December — so Q4 (Oct-Dec) and H2
        # (Jul-Dec) walked straight in, and `series[fy] = val` let
        # whichever appeared last in the JSON overwrite the real figure.
        #
        # It survived because it only bites the OLDEST year in a window,
        # which is exactly the year a CAGR divides by. Mastercard's
        # ten-year revenue CAGR read 31.67% against a true ~11%: the 2015
        # base was $2.09bn, a quarter, not the $9.7bn year. IDEXX, Pool
        # and Group 1 were the same shape — implied bases of 37%, 31% and
        # 47% of a year.
        #
        # `fp` cannot help: companyfacts reports the fiscal period of the
        # FILING, so every fact in a 10-K carries FY whatever span it
        # actually covers. The duration is the only real signal, so
        # measure it.
        start, end = v.get("start"), v.get("end")
        if start and end:
            try:
                span = (date.fromisoformat(end) - date.fromisoformat(start)).days
            except ValueError:
                continue                    # unparseable dates are not evidence
            if not (ANNUAL_DAYS[0] <= span <= ANNUAL_DAYS[1]):
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
    available: set[str] = set()
    for taxonomy, tag in slots:
        node = (facts.get(taxonomy) or {}).get(tag)
        if node:
            available |= set((node.get("units") or {}).keys())
    if not available:
        return {}, ""

    # Build the merged series for EVERY unit, then choose. Preferring the
    # wanted unit outright is not safe: a company whose real history is in
    # yen can also carry a two-year stray USD tagging, and taking the USD
    # one loses the company entirely (it falls under the 3-year minimum)
    # or, worse, keeps a truncated series and divides it by a dollar price.
    # Run #6 lost Toyota, Novo Nordisk and SAP that way, and gave Taiwan
    # Semi a P/FCF of 410.
    #
    # So: take the wanted unit when it is genuinely the company's
    # reporting unit — within a year of the best coverage available — and
    # otherwise take whichever unit actually holds the history.
    by_unit: dict[str, dict[int, float]] = {}
    for unit in available:
        # PRIMARY TAG FIRST, not slot order. Slot-order-wins mixes two
        # accounting bases inside one series: a company tagging `Revenues`
        # for a few scattered years and `RevenueFromContractWithCustomer`
        # for the rest gets whichever happens to be listed first for each
        # year, so one substituted year rewrites its history. Xerox's
        # five-year revenue CAGR went 0.0% -> 126.8% between runs without
        # its fiscal year moving at all; only one base year's value did.
        #
        # The tag with the most coverage IS the company's reporting basis.
        # Take it whole and let the others fill only the years it lacks.
        per_tag: list[dict[int, float]] = []
        for taxonomy, tag in slots:
            node = (facts.get(taxonomy) or {}).get(tag)
            if node:
                got = _rows_for(node, unit)
                if got:
                    per_tag.append(got)
        if not per_tag:
            continue
        # Stable: most years wins, and slot order still breaks ties, so
        # the existing preference survives wherever coverage is equal.
        per_tag.sort(key=lambda d: -len(d))
        merged: dict[int, float] = {}
        for got in per_tag:
            for fy, val in got.items():
                merged.setdefault(fy, val)
        if merged:
            by_unit[unit] = merged
    if not by_unit:
        return {}, ""

    best = max(len(s) for s in by_unit.values())
    if want_unit in by_unit and len(by_unit[want_unit]) >= best - 1:
        unit = want_unit
    else:
        # Most history wins; ties break toward the wanted unit and then
        # alphabetically, so the answer cannot change between runs because
        # the SEC reordered a JSON object.
        unit = sorted(by_unit, key=lambda u: (-len(by_unit[u]), u != want_unit, u))[0]
    series = by_unit[unit]
    return (series, unit) if len(series) >= 3 else ({}, unit)


# 2020 is not an economic base year, and 2021 is only half of one. A
# trailing CAGR measured from the bottom of a global shutdown reports a
# RECOVERY as if it were growth, and this screen exists to find durable
# compounding — the opposite thing.
#
# Before #217 the damage was hidden: 114 companies were frozen at 2017-18,
# so their five-year window sat in normal years. Fixing that pushed the
# window forward, and 1,540 of 1,833 rows landed on a 2020 base at once:
#
#     LUV  Southwest      -16.6  ->  69.4    RCL  Royal Caribbean  ->  250.0
#     CTAS Cintas         -16.6  ->  41.7    SHW  Sherwin-Williams ->   39.3
#     COLM Columbia Sportswear     30.0      AOS  A.O. Smith            35.6
#
# Cintas and Sherwin-Williams are steady high-single-digit growers. None
# of those numbers describe the businesses; they describe 2020.
#
# So the window is anchored before the shutdown. Where that is impossible
# — a company without pre-2020 filings — the answer is UNKNOWN rather than
# a recovery rate, because a company whose entire record is the rebound
# has no measurable durable growth yet, and admitting one to the board on
# the strength of the rebound is exactly the error being fixed.
NO_BASE_YEARS = (2020, 2021)


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
    # Walk the base back out of the shutdown, lengthening the span rather
    # than measuring from the hole.
    while first in NO_BASE_YEARS:
        earlier = [y for y in ys if y < first]
        if not earlier:
            return None
        first = earlier[-1]
    a, b = series[first], series[last]
    span = last - first
    if a <= 0 or b <= 0 or span < 3:
        return None
    return round(((b / a) ** (1 / span) - 1) * 100, 2)


def _trend_growth(series: dict[int, float], window: int = LOOKBACK) -> float | None:
    """Annualized growth FITTED across the window, not measured corner to
    corner.

    An endpoint CAGR over fifteen years still rests on exactly two of the
    fifteen numbers, so one restated year, one 53-week year, one
    acquisition or one pandemic sets the entire answer. A least-squares
    fit on log revenue uses every observation: 2020 becomes one point in
    fifteen and moves the slope slightly instead of defining it.

    This is also why the fitted measure needs no COVID guard while the
    endpoint CAGRs do — robustness beats exclusion, and dropping real
    observations to protect a fragile estimator is treating the symptom.
    """
    if not series:
        return None
    ys = sorted(series)
    last = ys[-1]
    pts = [(y, series[y]) for y in ys if y > last - window and series[y] > 0]
    if len(pts) < 5:
        return None            # a slope through four points is a guess
    x0 = pts[0][0]
    xs = [y - x0 for y, _ in pts]
    lg = [math.log(v) for _, v in pts]
    n = len(pts)
    mx, my = sum(xs) / n, sum(lg) / n
    sxx = sum((x - mx) ** 2 for x in xs)
    if sxx == 0:
        return None
    slope = sum((xs[i] - mx) * (lg[i] - my) for i in range(n)) / sxx
    try:
        return round((math.exp(slope) - 1) * 100, 2)
    except (OverflowError, ValueError):
        return None


def _up_years(series: dict[int, float], window: int = LOOKBACK) -> tuple[int, int]:
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
    capex_int = s["capex_intangible"]
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

    # ── Reinvestment: a year with no capex tag is NOT a year with no capex
    #
    # `capex.get(y, 0.0)` read an untagged year as a zero-capex year, and
    # 162 of 1,958 rows came out at exactly 0.0% capex/OCF — Shell, BP,
    # TotalEnergies, Verizon, Rio Tinto, Petrobras, ArcelorMittal and
    # Toyota among them. The most capital-hungry businesses on earth
    # scoring as the most capital-light, and passing the capex gate on it.
    #
    # It corrupted the cash gate too, because FCF was OCF minus that zero:
    # BP's free-cash-flow conversion read 1,322%, KT's 553%, Shell's 294%.
    # Two of the six quality gates were not merely wrong but inverted.
    #
    # Both now require the year to carry BOTH figures. A company whose
    # capex we cannot find scores None and is GATED with a badge saying
    # why — it is not deleted, and it is not waved through either.
    #
    # Intangible capex is additive and treated as zero when absent, which
    # is the opposite convention on purpose: most companies genuinely
    # capitalise nothing, and the asymmetry is safe because it can only
    # ever make the ratio LARGER and the gate stricter.
    both = [y for y in sorted(ocf) if y in capex]
    reinvest = {y: capex[y] + capex_int.get(y, 0.0) for y in both}

    yrs_win = [y for y in both if y > last - LOOKBACK]
    fcf = {y: ocf[y] - reinvest[y] for y in both}
    ni_win = [y for y in yrs_win if y in ni]
    sum_fcf = sum(fcf[y] for y in ni_win)
    sum_ni = sum(ni[y] for y in ni_win)
    fcf_conv = round(sum_fcf / sum_ni * 100, 1) if sum_ni > 0 else None

    # Three of the last five years must actually report capex. One gap is
    # a filing quirk; four gaps means we are not measuring the company.
    capex_ratio = reinvest_ratio = None
    win5 = [y for y in both[-5:] if ocf[y] > 0]
    if len(win5) >= 3:
        o = sum(ocf[y] for y in win5)
        capex_ratio = round(sum(capex[y] for y in win5) / o * 100, 1)
        reinvest_ratio = round(sum(reinvest[y] for y in win5) / o * 100, 1)

    # Net debt / EBIT (proxy for leverage capacity).
    nd_ebit = None
    if last in op and op[last] > 0:
        nd = lt.get(last, 0.0) + st.get(last, 0.0) - cash.get(last, 0.0)
        nd_ebit = round(nd / op[last], 2)

    # Buybacks: 5-yr share-count CAGR (negative = shrinking count).
    shares_cagr5 = _cagr(shares, 5)

    op_m_vals = [op_m[y] for y in sorted(op_m)][-LOOKBACK:]
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

    ni_pos_years = sum(1 for y in sorted(ni)[-LOOKBACK:] if ni[y] > 0)
    ni_years_seen = len(sorted(ni)[-LOOKBACK:])

    fcf_ps_by_year = {y: fcf[y] / shares[y] for y in fcf
                      if y in shares and shares[y] > 0}

    return {
        "fy_last": last,
        "years": len(years),
        "currency": currency,
        "revenue_last": rev[last],
        "rev_cagr5": _cagr(rev, 5), "rev_cagr10": _cagr(rev, 10),
        "rev_cagr15": _cagr(rev, LOOKBACK),
        "rev_trend": _trend_growth(rev),
        "fcf_trend": _trend_growth({y: v for y, v in fcf.items() if v > 0}),
        "ni_years_seen": ni_years_seen,
        "lookback": LOOKBACK,
        "rev_up_years": rev_ups, "rev_up_total": rev_tot,
        "fcf_cagr5": _cagr({y: v for y, v in fcf.items() if v > 0}, 5),
        "ni_pos_years": ni_pos_years,
        "roic_med": roic_med,
        "fcf_conv": fcf_conv,
        "capex_ocf": capex_ratio,
        "reinvest_ocf": reinvest_ratio,
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
                    help="Process only the N largest (testing). NOTE: this also "
                         "skips foreign discovery, so a --limit run has no "
                         "international names in it.")
    ap.add_argument("--min-revenue", type=float, default=MIN_REVENUE,
                    help="Revenue floor in dollars. This IS the universe size dial.")
    ap.add_argument("--max-universe", type=int, default=MAX_UNIVERSE,
                    help="Hard cap on CIKs processed after the ticker join.")
    args = ap.parse_args()

    this_year = datetime.now(timezone.utc).year
    years = [this_year - 2, this_year - 1]
    print("[compounders] Stage A: universe discovery via EDGAR frames…")
    revenue_by_cik, seen_in_frames = discover_universe(years, min_revenue=args.min_revenue)
    print(f"[compounders] {len(revenue_by_cik)} filers ≥ ${args.min_revenue/1e6:,.0f}M "
          f"revenue ({len(seen_in_frames)} measured in total)")
    tickers = ticker_map()

    universe = [(cik, rev) for cik, rev in revenue_by_cik.items() if cik in tickers]
    universe.sort(key=lambda x: -x[1])
    universe = universe[:args.max_universe]
    have = {cik for cik, _ in universe}
    origin = {"us_gaap": len(universe), "ifrs": 0, "unmeasured": 0, "seeds": 0}

    def admit(cik: int, bucket: str) -> None:
        if cik in have or cik not in tickers:
            return
        # No revenue figure exists for these in any comparable unit. They
        # are admitted to be MEASURED, not because they qualify; the
        # 7-year history rule and the SIC filter still apply downstream.
        universe.append((cik, 0.0))
        have.add(cik)
        origin[bucket] += 1

    # ── the foreign cohort ───────────────────────────────────────────
    if not args.limit:
        for cik in sorted(discover_ifrs(years)):
            if origin["ifrs"] >= MAX_FOREIGN:
                break
            admit(cik, "ifrs")
        print(f"[compounders] +{origin['ifrs']} IFRS filers from per-currency frames")

        # Fallback: exchange-listed companies frames never measured AT ALL.
        # Not "below the floor" — absent. That set is where a foreign filer
        # hides when its taxonomy is unreadable, and it is the only way to
        # reach one without knowing its name in advance.
        if origin["ifrs"] < 50:
            budget = MAX_FOREIGN - origin["ifrs"]
            unmeasured = sorted(c for c in tickers if c not in seen_in_frames and c not in have)
            origin["unmeasured_pool"] = len(unmeasured)
            print(f"[compounders] IFRS frames yielded little; scanning "
                  f"{min(len(unmeasured), budget)} of {len(unmeasured)} "
                  f"never-measured exchange-listed CIKs")
            if len(unmeasured) > budget:
                print(f"[compounders] NOTE: {len(unmeasured) - budget} unmeasured CIKs "
                      f"skipped at the {MAX_FOREIGN} cap — foreign coverage is "
                      f"incomplete this run.")
            for cik in unmeasured[:budget]:
                admit(cik, "unmeasured")

    # Backstop, not mechanism: if discovery found them, this adds nothing.
    by_ticker = {info["ticker"]: cik for cik, info in tickers.items()}
    for t in ADR_SEEDS:
        cik = by_ticker.get(t)
        if cik:
            admit(cik, "seeds")

    if args.limit:
        universe = universe[:args.limit]
    print(f"[compounders] {len(universe)} to process — {origin['us_gaap']} us-gaap, "
          f"{origin['ifrs']} IFRS, {origin['unmeasured']} unmeasured, "
          f"{origin['seeds']} seed backstop")

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
               "country": profile.get("country_desc") or "United States",
               "incorporation": profile.get("incorporation") or "",
               "foreign": bool(profile.get("foreign_filer"))}
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
        "foreign_filers": sum(1 for r in out.values() if r.get("foreign")),
        "discovery": origin,
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

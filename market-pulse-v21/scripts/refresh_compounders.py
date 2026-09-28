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
    # The "Other" capex tags, which some filers use for their WHOLE capex
    # line: Eli Lilly ($7.8bn in 2025), ADP, and Verizon since 2019 ($17bn
    # — it tagged PaymentsToAcquireProductiveAssets until 2018, so its
    # capex ratio was being measured on 2014-18). FILL ONLY: a year any tag
    # above covers keeps that figure, because for other filers these are a
    # minor line beside the main one, and a "most years wins" merge could
    # otherwise put the minor line in charge.
    "capex_other": [
        ("us-gaap", "PaymentsToAcquireOtherPropertyPlantAndEquipment"),
        ("us-gaap", "PaymentsToAcquireOtherProductiveAssets"),
    ],
    # EBIT for the years a filer reports no operating-income line: pre-tax
    # income plus interest expense. TJX stopped tagging OperatingIncomeLoss
    # in 2019 and Sherwin-Williams in 2023, which left both with no net
    # debt/EBIT and GATED on debt nobody had measured. Fill only.
    "pretax_income": [
        ("us-gaap", "IncomeLossFromContinuingOperationsBeforeIncomeTaxesExtraordinaryItemsNoncontrollingInterest"),
        ("us-gaap", "IncomeLossFromContinuingOperationsBeforeIncomeTaxesMinorityInterestAndIncomeLossFromEquityMethodInvestments"),
    ],
    "interest_expense": [
        ("us-gaap", "InterestExpense"),
        ("us-gaap", "InterestExpenseNonoperating"),
        ("us-gaap", "InterestExpenseDebt"),
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

def _rows_for(node: dict, unit: str, filed_out: dict | None = None) -> dict[int, float]:
    """{fiscal_year: value} for ONE tag in ONE unit. Annual = FY frame
    from an annual form; amended filings dedupe by keeping the last.

    `filed_out`, when given, receives {fiscal_year: filing date} for the
    value kept — a share count needs it, because a stock split after that
    date is not in the number (see restate_shares)."""
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
        if filed_out is not None:
            filed_out[int(fy)] = v.get("filed")
    return series


# ── balance sheet: the enterprise-value inputs ───────────────────────
#
# Everything above this point is a DURATION fact — revenue over a year,
# cash flow over a year. These are INSTANTS: a balance on one date. They
# need their own extractor for a reason that bites immediately.
#
# A 10-K reports two balance sheets, this year's and last year's, and
# companyfacts stamps BOTH with the fiscal year of the FILING. So keying
# an instant on `fy` the way _rows_for does for durations gives two facts
# competing for one slot, and whichever the SEC happened to list last
# wins. For a balance sheet that is not a rounding difference — it is
# last year's debt on this year's row.
#
# The `end` date is unambiguous where `fy` is not, so these are keyed on
# `end` and ignore `fy` entirely — and every component is read as of ONE
# date, the company's latest balance sheet (see balance_sheet()).
BALANCE_TAGS: dict[str, list[tuple[str, str]]] = {
    "cash": [
        ("us-gaap", "CashAndCashEquivalentsAtCarryingValue"),
        ("us-gaap", "CashCashEquivalentsRestrictedCashAndRestrictedCashEquivalents"),
        ("us-gaap", "CashAndDueFromBanks"),
        ("ifrs-full", "CashAndCashEquivalents"),
    ],
    # VFLO's definition is "total debt", so operating-lease liabilities are
    # deliberately absent: ASC 842 put them on the balance sheet in 2019
    # and including them would make every retailer and restaurant look
    # abruptly more levered than the index it is being compared against.
    # A single figure for ALL debt. Air Products and Hertz file theirs as
    # DebtAndCapitalLeaseObligations; IFRS filers as Borrowings (Petrobras,
    # Anheuser-Busch). A floor like the rest, not an override.
    "debt_total": [
        ("us-gaap", "DebtLongtermAndShorttermCombinedAmount"),
        ("us-gaap", "DebtAndCapitalLeaseObligations"),
        ("ifrs-full", "Borrowings"),
    ],
    # THE BALANCE-SHEET LINE for long-term debt, excluding what falls due
    # this year. `LongTermDebtAndCapitalLeaseObligations` is that line too
    # (finance leases included) and it is what Union Pacific, AbbVie,
    # Lowe's, AT&T, Home Depot and Micron file today — it plus the current
    # portion reproduces each one's `LongTermDebt` total. It used to sit
    # with `LongTermDebt` as "ambiguous" and was thrown away.
    #
    # The first slot with a value on the balance-sheet date wins, so the
    # entries after the first three are fallbacks for filers whose ONLY
    # long-term line they are (Palo Alto's converts, Illumina's notes, SM
    # Energy's senior notes, Block) — never added beside a main line.
    "debt_noncurrent": [
        ("us-gaap", "LongTermDebtNoncurrent"),
        ("us-gaap", "LongTermDebtAndCapitalLeaseObligations"),
        ("ifrs-full", "NoncurrentPortionOfNoncurrentBorrowings"),
        ("ifrs-full", "LongtermBorrowings"),
        ("us-gaap", "ConvertibleDebtNoncurrent"),
        ("us-gaap", "LongTermNotesPayable"),
        ("us-gaap", "SeniorLongTermNotes"),
        ("us-gaap", "OtherLongTermDebtNoncurrent"),
        ("us-gaap", "ConvertibleLongTermNotesPayable"),
    ],
    # The TOTAL of long-term debt, current portion included. Used as the
    # long-term figure when no line above was filed, and never added to a
    # current portion it contains.
    "debt_longterm_total": [
        ("us-gaap", "LongTermDebt"),
        ("us-gaap", "LongTermDebtAndCapitalLeaseObligationsIncludingCurrentMaturities"),
    ],
    # Totals of ONE KIND of debt, current portion included — the only
    # long-term figure some filers tag (Teva's senior notes, Avista's
    # secured bonds). The LARGEST is a floor beside the others, never a
    # sum: kinds overlap (senior notes are unsecured debt), so two are
    # undercounted rather than double-counted. Alone, it stands only if it
    # covers the debt due this year (see balance_sheet).
    "debt_kind_total": [
        ("us-gaap", "SeniorNotes"),
        ("us-gaap", "ConvertibleNotesPayable"),
        ("us-gaap", "SecuredDebt"),
        ("us-gaap", "UnsecuredDebt"),
        ("us-gaap", "UnsecuredLongTermDebt"),
        ("us-gaap", "SecuredLongTermDebt"),
        ("us-gaap", "LongTermLoansPayable"),
    ],
    "debt_current": [
        ("us-gaap", "LongTermDebtCurrent"),
        ("us-gaap", "LongTermDebtAndCapitalLeaseObligationsCurrent"),
        ("ifrs-full", "CurrentPortionOfLongtermBorrowings"),
        ("ifrs-full", "CurrentPortionOfNoncurrentBorrowings"),
        ("us-gaap", "ConvertibleDebtCurrent"),
        ("us-gaap", "ConvertibleNotesPayableCurrent"),
        ("us-gaap", "SeniorNotesCurrent"),
        ("us-gaap", "OtherLongTermDebtCurrent"),
    ],
    # All debt due within a year, short-term borrowings INCLUDED — so it
    # stands in for current portion AND short-term borrowings, never
    # beside them (Micron files only this).
    "debt_current_all": [
        ("us-gaap", "DebtCurrent"),
        ("ifrs-full", "CurrentBorrowingsAndCurrentPortionOfNoncurrentBorrowings"),
    ],
    "debt_short": [
        ("us-gaap", "ShortTermBorrowings"),
        ("us-gaap", "OtherShortTermBorrowings"),
        ("us-gaap", "CommercialPaper"),
        ("ifrs-full", "ShorttermBorrowings"),
        ("us-gaap", "NotesPayableCurrent"),
        ("us-gaap", "LoansPayableCurrent"),
        ("us-gaap", "ShortTermBankLoansAndNotesPayable"),
    ],
    "preferred": [
        ("us-gaap", "PreferredStockValue"),
        ("us-gaap", "PreferredStockValueOutstanding"),
    ],
    "minority": [
        ("us-gaap", "MinorityInterest"),
        ("ifrs-full", "NoncontrollingInterests"),
    ],
    # Not an EV component. Presence of either is EVIDENCE THAT A BALANCE
    # SHEET WAS FILED AT ALL, which is what lets "no debt tag" be read as
    # "no debt" rather than "not mapped" — see balance_sheet().
    "equity": [
        ("us-gaap", "StockholdersEquity"),
        ("us-gaap", "StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest"),
        ("ifrs-full", "Equity"),
    ],
    "liabilities": [
        ("us-gaap", "Liabilities"),
        ("ifrs-full", "Liabilities"),
    ],
}


def _latest_instant(facts: dict, slots: list[tuple[str, str]],
                    want_unit: str = "USD"):
    """(value, as_of, unit) for the most recent annual balance-sheet fact.

    Latest `end` wins, across every slot and in one unit. `fy` is not
    consulted: a 10-K's comparative prior-year balance carries the same
    `fy` as the current one, so it is not an identifier here.

    The unit is RETURNED rather than assumed, for the same reason
    _annual_series returns it — a balance sheet in yen must never be
    subtracted from a market capitalisation in dollars. Toyota's P/FCF
    read 0.2 the last time this repo mixed two currencies.
    """
    best_by_unit: dict[str, tuple[str, float]] = {}
    for taxonomy, tag in slots:
        node = (facts.get(taxonomy) or {}).get(tag)
        if not node:
            continue
        for unit, rows in (node.get("units") or {}).items():
            for v in rows:
                if v.get("form") not in ANNUAL_FORMS:
                    continue
                # A duration fact has a start; an instant does not. Guard
                # against a tag that reports both shapes.
                if v.get("start"):
                    continue
                end, val = v.get("end"), v.get("val")
                if not end or not isinstance(val, (int, float)):
                    continue
                prev = best_by_unit.get(unit)
                if prev is None or end > prev[0]:
                    best_by_unit[unit] = (end, float(val))
    if not best_by_unit:
        return None, None, None
    if want_unit in best_by_unit:
        unit = want_unit
    else:
        # Most recent wins, then alphabetical, so the answer cannot change
        # between runs because the SEC reordered a JSON object.
        unit = sorted(best_by_unit, key=lambda u: (best_by_unit[u][0], u),
                      reverse=True)[0]
    end, val = best_by_unit[unit]
    return val, end, unit


def _instant_at(facts: dict, slots: list[tuple[str, str]], end: str,
                unit: str) -> float | None:
    """The first slot's value AT exactly `end`, in `unit`, from an annual
    filing — the latest-filed one when a 10-K/A restated it. None if no
    slot has a value on that date."""
    for taxonomy, tag in slots:
        rows = (((facts.get(taxonomy) or {}).get(tag) or {}).get("units") or {}).get(unit) or []
        hits = [v for v in rows
                if v.get("form") in ANNUAL_FORMS and not v.get("start")
                and v.get("end") == end and isinstance(v.get("val"), (int, float))]
        if hits:
            return float(max(hits, key=lambda v: v.get("filed") or "")["val"])
    return None


def _last_filed(facts: dict, slots: list[tuple[str, str]],
                unit: str) -> tuple[str | None, list[float]]:
    """(date, [each slot's value on it]) for the latest date any slot has
    an annual balance-sheet value in `unit`; (None, []) if none ever."""
    ends = [v["end"] for taxonomy, tag in slots
            for v in (((facts.get(taxonomy) or {}).get(tag) or {}).get("units") or {}).get(unit) or []
            if v.get("form") in ANNUAL_FORMS and not v.get("start") and v.get("end")
            and isinstance(v.get("val"), (int, float))]
    if not ends:
        return None, []
    end = max(ends)
    return end, [x for x in (_instant_at(facts, [slot], end, unit) for slot in slots)
                 if x is not None]


def _duration_at(facts: dict, slots: list[tuple[str, str]], end: str,
                 unit: str) -> float | None:
    """The first slot's value for the fiscal YEAR ending `end`, from an
    annual filing (latest-filed wins). None if no slot has one."""
    for taxonomy, tag in slots:
        rows = (((facts.get(taxonomy) or {}).get(tag) or {}).get("units") or {}).get(unit) or []
        hits = []
        for v in rows:
            if (v.get("form") not in ANNUAL_FORMS or v.get("end") != end or not v.get("start")
                    or not isinstance(v.get("val"), (int, float))):
                continue
            try:
                span = (date.fromisoformat(end) - date.fromisoformat(v["start"])).days
            except ValueError:
                continue
            if ANNUAL_DAYS[0] <= span <= ANNUAL_DAYS[1]:
                hits.append(v)
        if hits:
            return float(max(hits, key=lambda v: v.get("filed") or "")["val"])
    return None


DEBT_KEYS = ("debt_total", "debt_noncurrent", "debt_longterm_total", "debt_kind_total",
             "debt_current", "debt_current_all", "debt_short")

# What a company paid in interest over the year to its latest balance
# sheet. Evidence only — never an EV component.
INTEREST_SLOTS = [
    ("us-gaap", "InterestExpense"),
    ("us-gaap", "InterestExpenseNonoperating"),
    ("us-gaap", "InterestExpenseDebt"),
    ("us-gaap", "InterestPaidNet"),
    ("ifrs-full", "InterestExpense"),
    ("ifrs-full", "FinanceCosts"),
]

# A zero read from what a company did NOT file stands only if its interest
# bill agrees. At a 5% coupon, 0.25% of revenue is debt of 5% of a year's
# sales. The first branch run's unknowns split on it: debt-free companies
# paying facility fees and lease interest reached 0.32% (Teekay), Vertex
# and Signet 0.11%; Caleres, whose revolver is tagged under a name this
# list does not read, 0.64%, and Babcock & Wilcox 6.4%.
DEBT_FREE_INTEREST_MAX = 0.0025


def balance_sheet(facts: dict, want_unit: str = "USD",
                  revenue: float | None = None) -> dict:
    """The enterprise-value inputs, or an explicit absence.

    Returns total_debt, cash, preferred and minority in ONE unit, as of the
    company's latest balance sheet, with the reason for anything missing.

    ONE DATE FOR EVERYTHING. Each component used to take its own latest
    value, so a tag the company stopped using years ago supplied a debt
    figure beside this year's cash: Home Depot's "noncurrent debt" was
    $8.7bn from its 2011 10-K against $46bn today, AT&T's and Micron's
    came from 2011-12 too. Now the date is the latest balance sheet —
    the newest equity, liabilities or cash figure in an annual filing —
    and a component with no value on that date is simply not filed.

    TOTAL DEBT IS ASSEMBLED, NOT READ. Almost nobody files a single
    total-debt tag, so, on that one date, each of these is a FLOOR and the
    largest stands:

        a combined total tag (US GAAP or IFRS `Borrowings`)
        noncurrent line + debt due within a year
        LongTermDebt (current included) + short-term borrowings
        the largest one-kind total (senior notes, secured debt) + short-term

    where "debt due within a year" is the larger of current portion +
    short-term borrowings and DebtCurrent. None of them adds two figures
    that overlap, so none overstates; a figure that is only part of the
    debt cannot beat a fuller one. A combined tag smaller than the debt due
    within a year is not a total and is ignored (SK Telecom's). The old
    rule threw `LongTermDebt` away whenever a current portion was filed
    (Union Pacific read $1.5bn against $31.8bn, AbbVie $8.6bn against
    $64.5bn) and took a combined tag alone (ON Semiconductor, $0.9m).

    NO DEBT TAG IS NOT AUTOMATICALLY UNKNOWN. A genuinely debt-free
    company files no debt tag, and treating that as unmeasurable would
    throw away exactly the balance sheets this screen most wants. So if the
    company filed a balance sheet and has NEVER filed a debt tag, debt is
    read as zero and FLAGGED as inferred. A company whose last debt figures
    were all ZERO and that has filed none since repaid it and stopped
    tagging an empty line — Copart, Lululemon, Vertex, Signet — and reads
    as zero too, with the date it last said so. A company whose last debt
    figure was NOT zero and files none now has moved it to a tag this list
    does not read: unknown, not zero.

    Either zero needs `revenue` to agree: interest of DEBT_FREE_INTEREST_MAX
    of revenue or more over the year means there is debt somewhere, and
    the answer is unknown.
    """
    anchors = BALANCE_TAGS["equity"] + BALANCE_TAGS["liabilities"] + BALANCE_TAGS["cash"]
    debt_slots = [s for k in DEBT_KEYS for s in BALANCE_TAGS[k]]
    # The unit: the wanted one if ANY balance-sheet figure is in it (a
    # stray euro cash line must not turn dollar debt into euros), else
    # whatever the balance sheet is in.
    _, _, unit = _latest_instant(facts, anchors + debt_slots, want_unit)
    # The date: the latest balance sheet in that unit — an anchor's date,
    # or failing any anchor, the newest debt figure's.
    ref = None
    for slots in (anchors, debt_slots):
        _, end, u = _latest_instant(facts, slots, unit or want_unit)
        if end is not None and u == unit:
            ref = end
            break
    if ref is None:
        return {"total_debt": None, "debt_inferred_zero": False, "debt_zero_as_of": None,
                "cash": None,
                "preferred": None, "minority": None, "unit": None, "as_of": None,
                "filed_balance_sheet": False, "notes": ["no balance sheet filed"]}

    def at(key):
        return _instant_at(facts, BALANCE_TAGS[key], ref, unit)

    cash = at("cash")
    combined, noncur, lt_total = at("debt_total"), at("debt_noncurrent"), at("debt_longterm_total")
    current, current_all, short = at("debt_current"), at("debt_current_all"), at("debt_short")

    notes = []
    debt = None
    partial = False
    # What falls due within a year. DebtCurrent holds the current portion
    # AND short-term borrowings, so the larger of it and the two parts is
    # the floor (SGRP files a zero current portion beside $20m of DebtCurrent).
    parts = (current or 0.0) + (short or 0.0) if (current is not None or short is not None) else None
    near = max((v for v in (parts, current_all) if v is not None), default=None)
    if current is None and current_all is not None:
        notes.append("DebtCurrent stands in for current portion and short-term borrowings")
    # EVERY ROUTE IS A FLOOR, and the largest stands. Each adds figures that
    # cannot overlap, so none overstates; a tag that is only part of the
    # debt (one note, the converts, a stray combined figure) must not beat
    # a fuller one. The combined tag used to be taken alone, and ON
    # Semiconductor's $0.9m of it stood in for $2.98bn of LongTermDebt.
    routes = []
    if combined is not None and combined >= (near or 0.0):
        routes.append(combined)
    elif combined is not None:
        notes.append("the combined debt tag is smaller than the debt due this year — ignored")
    if noncur is not None:
        routes.append(noncur + (near or 0.0))
    if lt_total is not None:
        # A total already holding the current portion: add only what it
        # cannot contain — short-term borrowings.
        routes.append(max(lt_total, current or 0.0) + (short or 0.0))
    kinds = [v for v in (_instant_at(facts, [slot], ref, unit)
                         for slot in BALANCE_TAGS["debt_kind_total"]) if v is not None]
    if kinds and routes:
        routes.append(max(kinds) + (short or 0.0))
    elif kinds:
        # One kind of debt, standing in for the total — unless it is
        # smaller than what falls due this year, which a long-term total
        # holds. Deere's $6.6bn of securitisation borrowings beside $13.8bn
        # of current debt is one slice of ~$60bn, not the whole.
        due = current if current is not None else current_all
        if max(kinds) >= (due or 0.0):
            routes.append(max(kinds) + (short or 0.0))
            notes.append("one kind of debt is the only long-term figure filed")
        else:
            partial = True
            notes.append("the only long-term figure is one kind of debt, smaller than "
                         "the debt due this year — part of the total; unknown")
    if routes:
        debt = max(routes)
        if debt == combined:
            notes.append("single combined debt tag")
        elif noncur is None and lt_total is not None:
            notes.append("LongTermDebt used as the total (current portion included)")
    elif near is not None and not partial:
        # Only debt due within a year. For a company that has filed a
        # long-term figure before, that is the part this list can still
        # see, not the whole — Deere's long-term borrowings moved to its
        # own tag, and its $13.8bn current debt alone read as its total.
        longterm = (BALANCE_TAGS["debt_noncurrent"] + BALANCE_TAGS["debt_longterm_total"]
                    + BALANCE_TAGS["debt_kind_total"] + BALANCE_TAGS["debt_total"])
        if _latest_instant(facts, longterm, unit)[0] is None:
            debt = near
        elif near:
            partial = True
            notes.append("only short-term debt on the latest balance sheet, but long-term "
                         "debt was filed before under a tag no longer used — unknown, "
                         "not the short-term part alone")
        # A zero here is a reported zero: left to the rule below.

    filed_a_balance_sheet = (at("equity") is not None or at("liabilities") is not None
                             or cash is not None)
    debt_inferred_zero = False
    zero_as_of = None
    if debt is None and filed_a_balance_sheet and not partial:
        last_end, last_vals = _last_filed(facts, debt_slots, unit)
        interest = _duration_at(facts, INTEREST_SLOTS, ref, unit)
        owes = (interest is not None and revenue is not None and revenue > 0
                and abs(interest) >= DEBT_FREE_INTEREST_MAX * revenue)
        if any(last_vals):
            notes.append("debt tags were filed before but none on the latest balance "
                         "sheet — moved to a tag this list does not read; unknown, not zero")
        elif owes:
            notes.append(f"no debt figure to read, but interest of {abs(interest) / revenue:.2%} "
                         f"of revenue says there is debt — unknown, not zero")
        elif last_end is None:
            debt = 0.0
            debt_inferred_zero = True
            notes.append("no debt tag on a filed balance sheet — read as zero "
                         "debt rather than unknown, because a debt-free "
                         "company files nothing here")
        else:
            debt = 0.0
            zero_as_of = last_end
            notes.append(f"debt last reported as zero ({last_end}) and none filed since "
                         f"— read as zero")

    return {
        "total_debt": debt,
        "debt_inferred_zero": debt_inferred_zero,
        "debt_zero_as_of": zero_as_of,
        "cash": cash,
        "preferred": at("preferred"),
        "minority": at("minority"),
        "unit": unit,
        "as_of": ref,
        "filed_balance_sheet": filed_a_balance_sheet,
        "notes": notes,
    }


def _annual_series(facts: dict, slots: list[tuple[str, str]],
                   want_unit: str = "USD",
                   filed_out: dict | None = None) -> tuple[dict[int, float], str]:
    """{fiscal_year: value} merged across every tag reporting in ONE unit,
    plus the unit used. `filed_out` receives each kept value's filing date.

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
    filed_by_unit: dict[str, dict[int, str]] = {}
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
        per_tag: list[tuple[dict[int, float], dict[int, str]]] = []
        for taxonomy, tag in slots:
            node = (facts.get(taxonomy) or {}).get(tag)
            if node:
                filed: dict[int, str] = {}
                got = _rows_for(node, unit, filed)
                if got:
                    per_tag.append((got, filed))
        if not per_tag:
            continue
        # Stable: most years wins, and slot order still breaks ties, so
        # the existing preference survives wherever coverage is equal.
        per_tag.sort(key=lambda d: -len(d[0]))
        merged: dict[int, float] = {}
        merged_filed: dict[int, str] = {}
        for got, filed in per_tag:
            for fy, val in got.items():
                if fy not in merged:
                    merged[fy] = val
                    merged_filed[fy] = filed.get(fy)
        if merged:
            by_unit[unit] = merged
            filed_by_unit[unit] = merged_filed
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
    if len(series) < 3:
        return {}, unit
    if filed_out is not None:
        filed_out.update(filed_by_unit[unit])
    return series, unit


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


# ── share counts filed at the wrong scale ────────────────────────────
#
# Some filers tag a count kept "in millions" or "in thousands" as if it
# were shares. From the SEC's own companyfacts:
#
#     McDonald's      FY2022  741,300,000    FY2023  732.3        (millions)
#     ConocoPhillips  FY2021    1,328,151    FY2022  1,278,163,000 (thousands before)
#     Dillard's       FY2021   20,592,000    FY2022  17,549       (thousands)
#     Ultra Clean     FY2018   38,919,000    FY2019  39.5 … FY2024 45,300,000
#
# McDonald's came out at 0.0x P/FCF; ConocoPhillips' share count "grew"
# 222% a year. A company's share count does not move a thousandfold in a
# year, so a jump within SCALE_TOL of 1,000 or 1,000,000 is read as a
# change of unit and the series is put back on one scale — the one that
# gives the LATEST count a size a listed company can have (LISTED_MIN).
SCALE_TOL = 3.0
LISTED_MIN = 1e6      # no exchange-listed company has fewer shares than this


def fix_share_scale(series: dict[int, float]) -> tuple[dict[int, float], bool]:
    """(series on one scale, whether anything was rescaled).

    Consecutive years whose ratio is within SCALE_TOL of 1,000^k (k = ±1,
    ±2) are a unit change; anything else, however large, is left as a real
    change. A series with no such break is returned as it is, whatever its
    size — a whole series in the wrong unit has nothing to be anchored to.
    """
    ys = sorted(y for y, v in series.items() if v and v > 0)
    if len(ys) < 2:
        return dict(series), False
    exp = {ys[0]: 0}
    for a, b in zip(ys, ys[1:]):
        r = series[b] / series[a]
        k = next((k for k in (1, 2, -1, -2)
                  if 1000.0 ** k / SCALE_TOL <= r <= 1000.0 ** k * SCALE_TOL), 0)
        exp[b] = exp[a] + k
    if len(set(exp.values())) == 1:
        return dict(series), False
    last = ys[-1]
    ref = exp[last]
    while series[last] * 1000.0 ** (ref - exp[last]) < LISTED_MIN and ref - exp[last] < 2:
        ref += 1
    return {y: series[y] * 1000.0 ** (ref - exp[y]) for y in ys}, True


def compute_metrics(facts: dict) -> dict | None:
    shares_filed: dict[int, str] = {}
    pulled = {k: _annual_series(facts, slots, WANT_UNIT.get(k, "USD"),
                                shares_filed if k == "shares_diluted" else None)
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
    # FILL-ONLY sources (see TAGS): a year the main tags cover keeps their
    # figure. Money in the reporting currency only — a filler in another
    # unit is dropped rather than spliced in.
    def same_ccy(key):
        return s[key] if pulled[key][1] in (currency, "") else {}
    capex = {**same_ccy("capex_other"), **capex}
    interest = same_ccy("interest_expense")
    ebit = {y: v + interest.get(y, 0.0) for y, v in same_ccy("pretax_income").items()}
    if pulled["op_income"][1] in (currency, ""):
        op = {**ebit, **op}
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

    # Buybacks: 5-yr share-count CAGR (negative = shrinking count). Split
    # history isn't known yet, so a split reads as dilution here; Stage C
    # restates it (market_metrics) wherever it can.
    shares_cagr5 = _cagr(fix_share_scale(shares)[0], 5)

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

    # ── enterprise-value inputs ──
    # Read here rather than in the consumer so the currency check happens
    # where the currency is known. `currency` is the unit of the MONEY
    # series (revenue is the anchor); a balance sheet in any other unit is
    # dropped rather than mixed, because the market capitalisation it will
    # be combined with is in dollars.
    bs = balance_sheet(facts, want_unit=currency, revenue=rev[last])
    bs_usable = bs["unit"] == currency
    return {
        "fy_last": last,
        "years": len(years),
        "currency": currency,
        "total_debt": bs["total_debt"] if bs_usable else None,
        "cash": bs["cash"] if bs_usable else None,
        "preferred": bs["preferred"] if bs_usable else None,
        "minority": bs["minority"] if bs_usable else None,
        "bs_as_of": bs["as_of"] if bs_usable else None,
        "debt_inferred_zero": bs["debt_inferred_zero"] if bs_usable else None,
        "debt_zero_as_of": bs["debt_zero_as_of"] if bs_usable else None,
        "bs_unit": bs["unit"],
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
        "fcf_last": fcf.get(last),
        # Working data for Stage C, popped before output: FCF per share is
        # built there, once the split history is known.
        "_fcf": dict(fcf),
        "_shares": dict(shares),
        "_shares_filed": dict(shares_filed),
    }


def fetch_fundamentals(cik: int) -> dict | None:
    url = f"https://data.sec.gov/api/xbrl/companyfacts/CIK{cik:010d}.json"
    try:
        facts = (_get(url) or {}).get("facts") or {}
    except (urllib.error.URLError, urllib.error.HTTPError, TimeoutError, OSError, ValueError):
        return None
    return compute_metrics(facts)


# ── Stage C: market data ─────────────────────────────────────────────
#
# STOCK SPLITS. Yahoo's prices are split-adjusted to today's share basis.
# The SEC's share counts are as filed, and a filing never restates for a
# split that came after it. Divide one by the other and a split reads as
# the stock getting that many times cheaper: Booking Holdings split 25-for-1
# in April 2026, two months after its FY2025 10-K reported 32.6M diluted
# shares, so its $164 post-split price came out at 0.6x free cash flow and
# sorted to the top of every board that ranks by cheapness. Chipotle's
# 50-for-1 split gave it a 1.2x "historical median" and a share count that
# grew 90% a year — the maximum dilution penalty for a company that buys
# back stock. So each share count is restated to today's basis by the
# splits after the date it was filed, and FCF per share and the share-count
# trend are built from the restated counts.
CHART_RANGE = "10y"   # split history reaching past the oldest share count used
PFCF_YEARS = 7        # the P/FCF history window (the 7-year chart it used before)


def parse_splits(res: dict) -> list[tuple[str, float]]:
    """Yahoo chart events → [(ex-date 'YYYY-MM-DD', ratio)], oldest first.

    ratio is new shares per old: 25.0 for 25-for-1, 0.1 for a 1-for-10
    reverse split. A malformed or 1:1 event is skipped, never read as zero.
    """
    out = []
    events = (res or {}).get("events") or {}
    for s in (events.get("splits") or {}).values() if isinstance(events, dict) else ():
        try:
            num, den = float(s["numerator"]), float(s["denominator"])
            day = datetime.fromtimestamp(int(s["date"]), tz=timezone.utc).date().isoformat()
        except (KeyError, TypeError, ValueError, OverflowError, OSError, AttributeError):
            continue
        if num > 0 and den > 0 and num != den:
            out.append((day, num / den))
    return sorted(out)


def restate_shares(shares: dict[int, float], filed: dict[int, str],
                   splits: list[tuple[str, float]], since: str) -> dict[int, float]:
    """Share counts on today's basis: each multiplied by every split after
    the date it was filed. A split on or before that date is already in the
    number — a filing restates per-share figures for a split made before
    it is issued, and every later filing restates its comparatives.

    A count with no filing date, or filed before `since` (where the split
    history in hand begins), is DROPPED: a split before the window could be
    missing, and an unrestated count is the fault this exists to remove.
    """
    out: dict[int, float] = {}
    for y, n in shares.items():
        f = filed.get(y)
        if not f or f < since:
            continue
        factor = 1.0
        for day, ratio in splits:
            if day > f:
                factor *= ratio
        out[y] = n * factor
    return out


def market_inputs(metrics: dict, profile: dict) -> tuple[dict, dict, dict, bool]:
    """(fcf, shares, filed, apply_splits) for fetch_market.

    A non-USD reporter's FCF and shares are withheld (its P/FCF would divide
    yen by an ADR's dollars). Splits are applied for domestic filers only:
    an ADR's splits and ratio changes are not its ordinary shares' splits.
    """
    usd = (metrics.get("currency") or "USD") == "USD"
    return ((metrics.get("_fcf") or {}) if usd else {},
            (metrics.get("_shares") or {}) if usd else {},
            metrics.get("_shares_filed") or {},
            not (profile or {}).get("foreign_filer"))


def fetch_chart(ticker: str) -> dict | None:
    url = ("https://query1.finance.yahoo.com/v8/finance/chart/"
           f"{urllib.parse.quote(ticker, safe='')}?range={CHART_RANGE}"
           f"&interval=1mo&events=div%7Csplit")
    try:
        return _get(url, headers=HEADERS_YAHOO)["chart"]["result"][0]
    except (urllib.error.URLError, urllib.error.HTTPError, TimeoutError,
            OSError, ValueError, KeyError, IndexError, TypeError):
        return None


def fetch_market(ticker: str, fcf: dict[int, float], shares: dict[int, float],
                 filed: dict[int, str], apply_splits: bool = True) -> dict | None:
    res = fetch_chart(ticker)
    return market_metrics(res, fcf, shares, filed, apply_splits) if res else None


def market_metrics(res: dict, fcf: dict[int, float], shares: dict[int, float],
                   filed: dict[int, str], apply_splits: bool = True) -> dict | None:
    """Price, dividends and the P/FCF terms from one Yahoo chart response.

    fcf / shares / filed are by fiscal year; pass them empty to withhold the
    valuation (a non-USD reporter). apply_splits=False keeps the counts as
    filed — for foreign filers, whose ADR splits are not their ordinary
    shares' splits. With splits applied the result also carries the
    restated `shares_cagr5`, which replaces the as-filed one.
    """
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

    # FCF per share on today's share basis (see STOCK SPLITS above).
    splits = parse_splits(res) if apply_splits else []
    since = datetime.fromtimestamp(ts[0], tz=timezone.utc).date().isoformat()
    counts = restate_shares(shares, filed, splits, since) if apply_splits else dict(shares)
    # Splits first: a 1-for-1,000 reverse split must be restated, not
    # mistaken for a change of unit.
    counts, rescaled = fix_share_scale(counts)
    fcf_ps = {y: fcf[y] / counts[y] for y in fcf if y in counts and counts[y] > 0}

    # P/FCF now + historical median: year-average price ÷ that FY's FCF/share.
    from collections import defaultdict
    year_prices: dict[int, list[float]] = defaultdict(list)
    window_start = now_ts - PFCF_YEARS * 365.25 * 24 * 3600
    for t, c in pts:
        if t > window_start:
            year_prices[datetime.fromtimestamp(t, tz=timezone.utc).year].append(c)
    mults = []
    for fy, f in fcf_ps.items():
        if f and f > 0 and year_prices.get(fy):
            mults.append(statistics.fmean(year_prices[fy]) / f)
    pfcf_med = round(statistics.median(mults), 1) if len(mults) >= 4 else None
    fcf_now = fcf_ps.get(max(fcf_ps)) if fcf_ps else None
    pfcf_now = round(price / fcf_now, 1) if fcf_now and fcf_now > 0 else None

    out = {"price": round(price, 2), "div_yield": div_yield,
           "div_cagr5": div_cagr, "pfcf_now": pfcf_now, "pfcf_med": pfcf_med,
           "_rescaled": rescaled}
    if apply_splits and shares:
        out["shares_cagr5"] = _cagr(counts, 5)
        out["_restated"] = any(day > (filed.get(y) or "") for y in counts for day, _ in splits)
    return out


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
    restated = rescaled = 0
    for i, (cik, info, profile, metrics) in enumerate(scored, 1):
        # THE VALUATION TERM IS THE ONLY CURRENCY-UNSAFE NUMBER HERE.
        # Growth, margins, ROIC, FCF conversion, capex/OCF and net
        # debt/EBIT are all ratios WITHIN one currency, so they are
        # correct whatever the filer reports in. P/FCF is not: it divides
        # a native-currency FCF per share by a USD ADR price. Withholding
        # FCF and shares blanks P/FCF and its history, which makes
        # compounders.py drop the valuation-drift term for that name — the
        # row stays, its quality and growth stay, and the one figure we
        # cannot compute honestly is absent rather than wrong.
        if (metrics.get("currency") or "USD") != "USD":
            non_usd += 1
        try:
            market = fetch_market(info["ticker"], *market_inputs(metrics, profile))
        except Exception:                                   # noqa: BLE001
            market = None
        time.sleep(YAHOO_SLEEP)
        if market is None:
            skipped["market"] += 1
            market = {}
        else:
            with_market += 1
            restated += bool(market.pop("_restated", False))
            rescaled += bool(market.pop("_rescaled", False))
        row = {**metrics, **market,
               "name": info["name"], "cik": cik,
               "exchange": info["exchange"],
               "sic": profile.get("sic"), "industry": profile.get("sic_desc"),
               "country": profile.get("country_desc") or "United States",
               "incorporation": profile.get("incorporation") or "",
               "foreign": bool(profile.get("foreign_filer"))}
        for k in ("_fcf", "_shares", "_shares_filed"):
            row.pop(k, None)      # working data — not needed in output
        out[info["ticker"]] = row
        if i % 250 == 0:
            print(f"[compounders] market {i}/{len(scored)} · {with_market} with prices")

    coverage = with_market / len(scored) if scored else 0.0
    print(f"[compounders] Done: kept {len(out)}, market coverage "
          f"{coverage:.0%}, {non_usd} reporting in a non-USD currency "
          f"(valuation term withheld for those), {restated} with share counts "
          f"restated for a stock split, {rescaled} with a share count filed at "
          f"the wrong scale, skipped {skipped}")

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
        "split_restated": restated,
        "share_scale_fixed": rescaled,
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

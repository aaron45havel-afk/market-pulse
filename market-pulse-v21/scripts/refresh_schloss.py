#!/usr/bin/env python3
"""Build data/schloss.json — the Walter Schloss screen, from filings only.

WHY THIS USES FRAMES AND NOT COMPANYFACTS
=========================================
The compounders build asks companyfacts one company at a time, because it
needs fifteen years of six series each and there is no other way to get
that. It costs ~6,000 requests and reaches ~1,900 companies.

Schloss needs one balance sheet and a short dividend series. That fits the
frames API, which returns ONE CONCEPT FOR EVERY FILER IN A SINGLE CALL:

    /api/xbrl/frames/us-gaap/StockholdersEquity/USD/CY2026Q1I.json

So the entire screen costs ~110 requests instead of 6,000, and covers
every SEC registrant that tagged the concept — roughly 7,000 companies,
including the small and unloved end of the market where Schloss actually
worked and where a $250M revenue floor would have deleted him.

That is the whole reason this screen can be free. The cheapness half needs
a price feed and honestly reports UNKNOWN without one; the balance-sheet
half needs nothing but EDGAR, and EDGAR is free, official and complete.

NO FINANCIALS EXCLUSION, DELIBERATELY. The compounders screen drops SIC
6000-6799 because ROIC and FCF are meaningless for a bank. Schloss bought
banks and insurers happily — book value is exactly the right lens for
them. They are kept. Banks file unclassified balance sheets, so they
simply have no AssetsCurrent, and NCAV comes out as unknown rather than
wrong, which is the correct answer and needs no special case.

Every parser here is a pure function tested offline in
tests/test_schloss_build.py. sec.gov is unreachable from the sandbox this
was written in, so anything that cannot be proved against a fixture is
something that ships unverified.
"""
from __future__ import annotations

import argparse
import gzip
import json
import os
import sys
import threading
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import schloss as S  # noqa: E402
import pricefeed as PF  # noqa: E402

SEC_UA = "market-pulse-research admin@focusedops.io"
HEADERS = {"User-Agent": SEC_UA, "Accept-Encoding": "gzip, deflate"}
SEC_RATE = 8.0            # SEC permits 10/s; leave headroom
SEC_WORKERS = 5
OUT = Path(__file__).resolve().parent.parent / "data" / "schloss.json"
# The board is overwritten each run; these are the point-in-time
# copies that make a forward-return study possible at all.
SNAPSHOT_DIR = OUT.parent / "schloss_snapshots"
MAX_SNAPSHOTS = 36        # three years — a one-to-two-year hold
                          # needs more than one holding period
                          # of history to say anything.

# How many years of dividends to pull. The cut signal only needs two, but
# a reader deciding whether a cut is an aberration or the fourth in a row
# needs the run of them.
DIV_YEARS = 8

# How far back to look for the balance sheet. A company reporting on a
# November year-end lands in a different calendar quarter from one on
# December, and frames bucket by calendar quarter — so the newest quarter
# alone would silently drop every off-calendar filer. Four quarters back,
# newest wins.
BS_QUARTERS = 4

# Share count five years prior, for the dilution measure.
DILUTION_YEARS = 5


class _Throttle:
    """Token bucket shared across worker threads. A per-thread sleep does
    not bound the aggregate rate; five threads sleeping 0.125s each is
    40 req/s, not 8."""

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


THROTTLE = _Throttle(SEC_RATE)


# ═══════════════════════════════════════════════════════════════════
# CONCEPTS
# ═══════════════════════════════════════════════════════════════════
#
# Ordered lists: the first tag that carries a value for a given company
# wins, so put the specific tag ahead of the general one. These are the
# balance-sheet lines Schloss's method actually reads — assets he could
# name, obligations ahead of him, and the two intangible lines that
# separate reported book from the tangible book he meant.

INSTANT: dict[str, list[str]] = {
    "stockholders_equity": [
        "StockholdersEquity",
        "StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest",
    ],
    "total_assets": ["Assets"],
    "total_liabilities": ["Liabilities"],
    "current_assets": ["AssetsCurrent"],
    "current_liabilities": ["LiabilitiesCurrent"],
    "cash": [
        "CashAndCashEquivalentsAtCarryingValue",
        "CashCashEquivalentsRestrictedCashAndRestrictedCashEquivalents",
    ],
    "receivables": [
        "AccountsReceivableNetCurrent",
        "ReceivablesNetCurrent",
    ],
    "inventory": ["InventoryNet"],
    "ppe": ["PropertyPlantAndEquipmentNet"],
    # The two lines that make tangible book different from book. A company
    # that carries neither omits both tags, which is why schloss.py treats
    # them as zero rather than unknown.
    "goodwill": ["Goodwill"],
    "intangibles": [
        "IntangibleAssetsNetExcludingGoodwill",
        "FiniteLivedIntangibleAssetsNet",
    ],
    "preferred_stock": ["PreferredStockValue"],
    "short_term_debt": [
        "ShortTermBorrowings",
        "OtherShortTermBorrowings",
        "LongTermDebtCurrent",
    ],
    "long_term_debt": ["LongTermDebt", "LongTermDebtNoncurrent"],
}

ANNUAL: dict[str, list[str]] = {
    "revenue": [
        "RevenueFromContractWithCustomerExcludingAssessedTax",
        "Revenues",
    ],
    "sbc": ["ShareBasedCompensation", "AllocatedShareBasedCompensationExpense"],
    # BANKS DO NOT REPORT THEIR REVENUE UNDER "Revenues". The tag catches
    # fee income; net interest income — most of the business — sits under
    # its own element and was being left out, so East West Bancorp's
    # $2.6bn of revenue came through as $55m and its stock compensation
    # read 137.7% of it. Added to revenue rather than replacing it, since
    # a bank has both.
    "interest_net": ["InterestIncomeExpenseNet",
                     "InterestIncomeExpenseAfterProvisionForLoanLoss"],
}

# Dividends per share, in USD-per-share. Kept separate because the unit is
# not USD and because we want the whole SERIES, not one year.
DIV_TAGS = [
    ("CommonStockDividendsPerShareDeclared", "USD-per-shares"),
    ("CommonStockDividendsPerShareCashPaid", "USD-per-shares"),
]

SHARES_TAGS = [("CommonStockSharesOutstanding", "shares")]

# ── Survival: probing the filing index, not the financial history ────
#
# XBRL starts around 2010, so no company can show more than ~16 years of
# machine-readable financials — and Schloss wanted 20+. Measuring survival
# from the data would cap every company below his bar.
#
# EDGAR's quarterly master.idx reaches back to 1994 and lists every filing
# made that quarter. Asking "was this CIK filing in 2006?" answers the
# 20-year question directly, in two requests per probe year.
#
# Q1 and Q3 only. A public company files a 10-K and three 10-Qs a year, so
# one of those two quarters catches it whatever its fiscal calendar. A
# company missed by both is recorded as younger than it is, which
# understates history and can only ever cause a false REJECTION — the safe
# direction for a gate.
PROBE_YEARS = [1996, 2001, 2006, 2011, 2016]
PROBE_QUARTERS = [1, 3]

# Ticker suffixes that are not companies.
WARRANT_SUFFIXES = ("W", "WS", "WT", "R", "U", "RT")


# ═══════════════════════════════════════════════════════════════════
# FETCH
# ═══════════════════════════════════════════════════════════════════

def _raw(url: str, timeout: int = 120) -> bytes:
    THROTTLE.wait()
    req = urllib.request.Request(url, headers=HEADERS)
    with urllib.request.urlopen(req, timeout=timeout) as r:
        data = r.read()
        if r.headers.get("Content-Encoding") == "gzip":
            data = gzip.decompress(data)
        return data


def _json(url: str) -> dict | None:
    try:
        return json.loads(_raw(url))
    except urllib.error.HTTPError as e:
        if e.code != 404:
            print(f"  ! {url.rsplit('/', 3)[-1]}: HTTP {e.code}")
        return None
    except (urllib.error.URLError, TimeoutError, OSError, ValueError) as e:
        print(f"  ! {url.rsplit('/', 3)[-1]}: {e}")
        return None


def _text(url: str) -> str | None:
    try:
        return _raw(url).decode("latin-1")
    except urllib.error.HTTPError as e:
        print(f"  ! {url}: HTTP {e.code}")
        return None
    except (urllib.error.URLError, TimeoutError, OSError) as e:
        print(f"  ! {url}: {e}")
        return None


# ═══════════════════════════════════════════════════════════════════
# PARSERS — pure, tested offline
# ═══════════════════════════════════════════════════════════════════

def parse_frame(payload: dict | None) -> dict[int, float]:
    """{cik: value} from one frames response.

    A frame is already one period, so there is at most one row per CIK and
    no period selection to get wrong here. Non-numeric and missing values
    are dropped rather than coerced — a company that reported nothing is
    not a company that reported zero.
    """
    out: dict[int, float] = {}
    for row in (payload or {}).get("data") or []:
        cik, val = row.get("cik"), row.get("val")
        if cik is None or isinstance(val, bool) or not isinstance(val, (int, float)):
            continue
        try:
            out[int(cik)] = float(val)
        except (TypeError, ValueError):
            continue
    return out


def merge_newest_first(frames: list[dict[int, float]]) -> dict[int, float]:
    """Overlay frames given newest-first; the first value for a CIK wins.

    This is what lets a November-year-end filer be read at all. Frames
    bucket by CALENDAR quarter, so companies scatter across the last four
    of them, and taking only the newest quarter would drop most of the
    market rather than a corner of it.
    """
    out: dict[int, float] = {}
    for frame in frames:
        for cik, val in frame.items():
            if cik not in out:
                out[cik] = val
    return out


def pick_series(by_tag: dict[str, dict[int, float]]) -> dict[int, float]:
    """Given {tag: {year: value}}, return the series from the single tag
    with the MOST years, filling gaps from the others.

    Mixing tags year-by-year is how the compounders build produced growth
    rates spanning two different definitions of revenue. One tag is one
    definition; the tag that covers the most of the window is the one that
    describes the company, and the rest may only fill years it is silent
    on.
    """
    if not by_tag:
        return {}
    best = max(by_tag.values(), key=len)
    out = dict(best)
    for series in by_tag.values():
        if series is best:
            continue
        for year, val in series.items():
            out.setdefault(year, val)
    return out


def parse_master_idx(text: str | None) -> set[int]:
    """CIKs appearing in an EDGAR quarterly master.idx.

    Format is a header block, a dashed rule, then pipe-delimited rows:

        CIK|Company Name|Form Type|Date Filed|Filename
        320193|Apple Inc.|10-Q|2006-02-01|edgar/data/320193/...

    We want only the first column, and only that the CIK was there at all.
    """
    found: set[int] = set()
    for line in (text or "").splitlines():
        head = line.split("|", 1)[0].strip()
        if head.isdigit():
            found.add(int(head))
    return found


def is_warrant(ticker: str) -> bool:
    """Warrants, rights and SPAC units are not companies to own.

    They carry the issuer's CIK, so they inherit its whole balance sheet
    and would appear as duplicate rows with the same fundamentals and a
    different price.
    """
    t = (ticker or "").upper()
    if "-" in t or "." in t:
        tail = t.replace(".", "-").split("-")[-1]
        # PREFERRED SERIES ARE NOT THE COMMON EITHER, and they were getting
        # through: AHL-PD, ANG-PD, CDR-PB, OAK-PA, SCE-PG, TRTN-PA, CFTR-PA,
        # PHXE-P and SEAL-PA all carried their issuer's whole balance sheet
        # into the board, which is precisely the duplicate this function
        # exists to stop. A preferred share has a fixed claim and a par
        # value; tangible book per COMMON share says nothing about it, and
        # no quote feed answered for any of them anyway.
        if len(tail) in (1, 2) and tail.startswith("P"):
            return True
        # A trailing separator with nothing after it is a malformed row,
        # not a class: "NONE." arrived in the August universe.
        if not tail:
            return True
        return tail in WARRANT_SUFFIXES
    return len(t) == 5 and t.endswith(("W", "R", "U"))


def shares_growth(then: float | None, now: float | None,
                  years: int) -> float | None:
    """Compound annual change in share count, as a percentage.

    Negative is buying back. Returns None rather than a number whenever
    the base is missing or non-positive — a company we could not measure
    must not read as one that issued nothing.
    """
    if then is None or now is None or years <= 0:
        return None
    if then <= 0 or now <= 0:
        return None
    return round(((now / then) ** (1.0 / years) - 1.0) * 100, 2)


def bounded_first_year(found: int | None, newest_probe: int) -> int:
    """The year survival should be SCORED from.

    A probe hit is the real answer. A complete miss is not "unknown": it
    means the company was not filing in the newest probe year, so it has
    fewer than (this year − newest probe) years of history and
    definitively fails a 20-year gate. Passing None through would park
    every young company in the could-not-read list — the same confusion
    between unmeasured and measured-and-failed that this screen exists to
    prevent, run in the other direction.

    The residual risk is a company that filed nothing in Q1 or Q3 of any
    probe year despite being old. That understates its history and can
    only cause a false rejection, which is the safe direction for a gate.
    """
    return found if found else newest_probe + 1


def history(first_filing_year: int | None, this_year: int,
            newest_probe: int) -> dict:
    """How long it has been filing, and which DIRECTION the number errs.

    A probe hit gives a lower bound: seen in 1996 means at least 30 years,
    possibly more. A complete miss gives an upper bound: not filing in
    2016 means fewer than ten years. Both are useful and they are not the
    same claim, so the bound travels with the number instead of being
    flattened into a single integer the page would render as exact.
    """
    if first_filing_year:
        return {"years": this_year - first_filing_year, "bound": "min",
                "since": first_filing_year}
    return {"years": this_year - newest_probe, "bound": "max", "since": None}


# ═══════════════════════════════════════════════════════════════════
# BULK QUOTES — free, and one request instead of 5,673
# ═══════════════════════════════════════════════════════════════════

BROWSER_UA = ("Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
              "(KHTML, like Gecko) Chrome/120.0 Safari/537.36")
QUOTE_HEADERS = {"User-Agent": BROWSER_UA, "Accept-Encoding": "gzip, deflate",
                 "Accept": "application/json, text/plain, */*",
                 "Accept-Language": "en-US,en;q=0.9"}


def _quote_json(url: str, timeout: int = 90) -> dict | None:
    """No SEC throttle here — these are different hosts entirely, and there
    is at most one request per source."""
    try:
        req = urllib.request.Request(url, headers=QUOTE_HEADERS)
        with urllib.request.urlopen(req, timeout=timeout) as r:
            raw = r.read()
            if r.headers.get("Content-Encoding") == "gzip":
                raw = gzip.decompress(raw)
        return json.loads(raw)
    except Exception as e:                      # noqa: BLE001 — any failure
        print(f"  ! {url.split('/')[2]}: {type(e).__name__}: {e}")
        return None


def _fetch_source(src: dict, want: set) -> dict:
    """{TICKER: quote} from one bulk source, for the tickers we still need.

    Extracted so the gap-fill pass and the first pass share one
    implementation — a batched source asked to fill 700 names must batch
    exactly the way it does when asked for 5,000, or the fill would be a
    second, subtly different fetcher.
    """
    quotes: dict = {}
    if src.get("batched"):
        size = src.get("batch_size", 200)
        ordered = sorted(want)
        for i in range(0, len(ordered), size):
            batch = ordered[i:i + size]
            payload = _quote_json(src["url"].format(
                syms=urllib.parse.quote(",".join(batch))))
            if payload is None:
                break
            quotes.update(src["parse"](payload))
            time.sleep(0.4)              # ~2.5 req/s against one host
    else:
        payload = _quote_json(src["url"])
        if payload is not None:
            quotes = src["parse"](payload)
    return quotes


def merge_quote_fill(want: set, primary: dict,
                     extra: list) -> tuple[dict, dict]:
    """(quotes, {source: names_added}) — primary first, then gaps filled.

    Pure, so the never-overwrite rule is testable without a network. The
    winning source is authoritative for every ticker it answered; later
    sources may only ADD names it missed, never replace a price it gave.
    Two feeds disagreeing on a price is a fact about the feeds, and
    silently preferring whichever ran last would make the board's numbers
    depend on source ordering.
    """
    out = {k: v for k, v in primary.items() if k in want}
    stats: dict = {}
    for name, quotes in extra:
        added = 0
        for k, v in (quotes or {}).items():
            if k in want and k not in out:
                out[k] = v
                added += 1
        if added:
            stats[name] = added
    return out, stats


def fetch_quotes(tickers: list[str]) -> tuple[dict, str]:
    """({TICKER: {price, market_cap}}, source name). Empty dict if none answer.

    Sources are tried in order and the FIRST one to cover enough of the
    board wins. A source that answers with a fraction of the market is a
    failure, not a partial success: half a price column reads as "these
    ones are not cheap" rather than "we could not look at these".
    """
    want = set(tickers)
    for src in PF.BULK_SOURCES:
        print(f"  trying {src['name']}...")
        quotes = _fetch_source(src, want)

        hit = quotes.keys() & want
        cover = len(hit) / len(want) if want else 0.0
        print(f"    {len(quotes):,} quotes, {len(hit):,} matched "
              f"({cover:.0%} of the board)")
        if cover >= PF.BULK_COVERAGE_MIN:
            won = {k: quotes[k] for k in hit}
            # THE OTHER SOURCES NOW FILL THE GAPS. Winner-take-all left
            # 1,148 of 5,673 companies unpriced on the August board, among
            # them every OTC name — the screener feed that wins on
            # coverage lists major exchanges only. Nine companies clearing
            # every balance-sheet gate had no price at all, so their
            # cheapness was permanently unknown: George Risk Industries,
            # Nobility Homes and seven community banks, which is exactly
            # the corner of the market this screen exists to find.
            #
            # The floor still applies to the WINNER, so a board whose
            # primary feed failed is still not published; the fill can only
            # add names the winner missed.
            missing = want - won.keys()
            extra = []
            if missing:
                print(f"  {len(missing):,} still unpriced — filling from the rest")
                for other in PF.BULK_SOURCES:
                    if other["name"] == src["name"] or not missing:
                        continue
                    got = _fetch_source(other, missing)
                    if got:
                        extra.append((other["name"], got))
                        missing = missing - got.keys()
            merged, stats = merge_quote_fill(want, won, extra)
            label = src["name"]
            for n, added in stats.items():
                label += f" + {n} ({added:,})"
                print(f"    {n} filled {added:,}")
            print(f"    priced {len(merged):,} of {len(want):,} "
                  f"({len(merged) / len(want):.0%})")
            return merged, label
        print(f"    below the {PF.BULK_COVERAGE_MIN:.0%} floor — trying the next source")
    return {}, ""


# ═══════════════════════════════════════════════════════════════════
# STAGES
# ═══════════════════════════════════════════════════════════════════

def frames_url(tag: str, unit: str, period: str, taxonomy: str = "us-gaap") -> str:
    return (f"https://data.sec.gov/api/xbrl/frames/{taxonomy}/{tag}"
            f"/{unit}/{period}.json")


def quarters_back(year: int, month: int, count: int) -> list[tuple[int, int]]:
    """The last `count` calendar quarters as (year, quarter), newest first.

    Starts one quarter back from the current one: frames lag reality by
    about a quarter, so the current period is one nobody has filed into
    yet and asking for it wastes a call on an empty answer.
    """
    y, q = year, (month - 1) // 3 + 1
    out = []
    for _ in range(count):
        q -= 1
        if q == 0:
            q, y = 4, y - 1
        out.append((y, q))
    return out


def instant_period(yq: tuple[int, int]) -> str:
    return f"CY{yq[0]}Q{yq[1]}I"


def fetch_instant(periods: list[str]) -> dict[str, dict[int, float]]:
    """One merged {cik: value} per balance-sheet field."""
    jobs = [(field, tag, period)
            for field, tags in INSTANT.items()
            for tag in tags
            for period in periods]
    results: dict[tuple[str, str, str], dict[int, float]] = {}
    with ThreadPoolExecutor(max_workers=SEC_WORKERS) as ex:
        futs = {ex.submit(_json, frames_url(t, "USD", p)): (f, t, p)
                for f, t, p in jobs}
        for fut, key in futs.items():
            results[key] = parse_frame(fut.result())

    out: dict[str, dict[int, float]] = {}
    for field, tags in INSTANT.items():
        # Newest period first, then preferred tag first: a specific tag in
        # an older quarter still beats a general one the company does not
        # actually use.
        ordered = [results.get((field, tag, period), {})
                   for period in periods for tag in tags]
        out[field] = merge_newest_first(ordered)
        print(f"  {field:22s} {len(out[field]):>6,} companies")
    return out


def fetch_annual(years: list[int]) -> dict[str, dict[int, dict[int, float]]]:
    """{field: {cik: {year: value}}} for duration concepts."""
    jobs = [(field, tag, year)
            for field, tags in ANNUAL.items()
            for tag in tags
            for year in years]
    raw: dict[tuple[str, str, int], dict[int, float]] = {}
    with ThreadPoolExecutor(max_workers=SEC_WORKERS) as ex:
        futs = {ex.submit(_json, frames_url(t, "USD", f"CY{y}")): (f, t, y)
                for f, t, y in jobs}
        for fut, key in futs.items():
            raw[key] = parse_frame(fut.result())

    out: dict[str, dict[int, dict[int, float]]] = {}
    for field, tags in ANNUAL.items():
        per_cik: dict[int, dict[str, dict[int, float]]] = {}
        for tag in tags:
            for year in years:
                for cik, val in raw.get((field, tag, year), {}).items():
                    per_cik.setdefault(cik, {}).setdefault(tag, {})[year] = val
        out[field] = {cik: pick_series(by_tag) for cik, by_tag in per_cik.items()}
        print(f"  {field:22s} {len(out[field]):>6,} companies")
    return out


def fetch_dividends(years: list[int]) -> dict[int, dict[int, float]]:
    """{cik: {year: dividend per share}} — the series the cut signal reads."""
    raw: dict[tuple[str, int], dict[int, float]] = {}
    jobs = [(tag, unit, year) for tag, unit in DIV_TAGS for year in years]
    with ThreadPoolExecutor(max_workers=SEC_WORKERS) as ex:
        futs = {ex.submit(_json, frames_url(t, u, f"CY{y}")): (t, y)
                for t, u, y in jobs}
        for fut, key in futs.items():
            raw[key] = parse_frame(fut.result())

    per_cik: dict[int, dict[str, dict[int, float]]] = {}
    for (tag, year), frame in raw.items():
        for cik, val in frame.items():
            per_cik.setdefault(cik, {}).setdefault(tag, {})[year] = val
    out = {cik: pick_series(by_tag) for cik, by_tag in per_cik.items()}
    print(f"  {'dividends/share':22s} {len(out):>6,} companies")
    return out


def fetch_shares(periods: list[str], back_periods: list[str]) -> tuple[dict, dict]:
    """(now, five years ago) share counts, merged newest-first."""
    def sweep(ps):
        frames = []
        with ThreadPoolExecutor(max_workers=SEC_WORKERS) as ex:
            futs = [ex.submit(_json, frames_url(t, u, p))
                    for t, u in SHARES_TAGS for p in ps]
            frames = [parse_frame(f.result()) for f in futs]
        return merge_newest_first(frames)

    now, then = sweep(periods), sweep(back_periods)
    print(f"  {'shares now':22s} {len(now):>6,} companies")
    print(f"  {'shares -5y':22s} {len(then):>6,} companies")
    return now, then


def fetch_survival() -> dict[int, int]:
    """{cik: earliest probe year it was filing in}."""
    out: dict[int, int] = {}
    for year in PROBE_YEARS:               # oldest first; first hit wins
        seen: set[int] = set()
        for qtr in PROBE_QUARTERS:
            url = (f"https://www.sec.gov/Archives/edgar/full-index/"
                   f"{year}/QTR{qtr}/master.idx")
            seen |= parse_master_idx(_text(url))
        new = 0
        for cik in seen:
            if cik not in out:
                out[cik] = year
                new += 1
        print(f"  filing in {year}: {len(seen):>6,} CIKs ({new:,} new)")
    return out


def fetch_tickers() -> dict[int, dict]:
    """{cik: {ticker, name, exchange}} — one row per company.

    company_tickers_exchange.json lists every ticker, so a company with
    several share classes appears more than once. The shortest ticker is
    taken as the primary listing, which is BRK-A over BRK-B and GOOG over
    GOOGL only by length — good enough to name the row, and the CIK is
    what everything else keys on.
    """
    payload = _json("https://www.sec.gov/files/company_tickers_exchange.json")
    if not payload or "data" not in payload:
        return {}
    fields = payload.get("fields", [])
    idx = {n: fields.index(n) for n in ("cik", "name", "ticker", "exchange")
           if n in fields}
    out: dict[int, dict] = {}
    for row in payload["data"]:
        try:
            cik = int(row[idx["cik"]])
            ticker = str(row[idx["ticker"]] or "")
        except (KeyError, IndexError, TypeError, ValueError):
            continue
        if not ticker or is_warrant(ticker):
            continue
        prev = out.get(cik)
        if prev and len(prev["ticker"]) <= len(ticker):
            continue
        out[cik] = {
            "ticker": ticker,
            "name": str(row[idx["name"]] or "") if "name" in idx else "",
            "exchange": str(row[idx["exchange"]] or "") if "exchange" in idx else "",
        }
    return out


# ═══════════════════════════════════════════════════════════════════
# BUILD
# ═══════════════════════════════════════════════════════════════════

def build(limit: int = 0) -> dict:
    now = datetime.now(timezone.utc)
    this_year = now.year
    qs = quarters_back(now.year, now.month, BS_QUARTERS)
    periods = [instant_period(q) for q in qs]
    # Same quarters, five years earlier. Shifting the QUARTER pair rather
    # than the date avoids the Feb-29 trap in datetime.replace(year=...)
    # and keeps the two share counts on matching fiscal calendars, which
    # is the whole point of comparing them.
    back = [instant_period((y - DILUTION_YEARS, q)) for y, q in qs]
    years = list(range(this_year - DIV_YEARS, this_year + 1))

    print(f"Balance sheet from {periods[0]} back to {periods[-1]}")
    inst = fetch_instant(periods)
    print("Annual concepts:")
    ann = fetch_annual(years)
    divs = fetch_dividends(years)
    sh_now, sh_then = fetch_shares(periods, back)
    print("Filing history probes:")
    first_year = fetch_survival()
    tick = fetch_tickers()
    print(f"  {'tickers':22s} {len(tick):>6,} companies")
    print("Bulk quotes (free, one request per source):")
    quotes, quote_src = fetch_quotes([v["ticker"] for v in tick.values()])
    if not quotes:
        print("  no source answered — the cheapness half stays UNKNOWN, "
              "which is the honest outcome and not a failure")

    if not tick:
        raise SystemExit("Ticker map empty — refusing to publish a nameless board.")

    ciks = sorted(set(tick) & (set(inst.get("stockholders_equity", {}))
                              | set(inst.get("total_assets", {}))))
    if limit:
        ciks = ciks[:limit]
    print(f"\nScoring {len(ciks):,} companies with a ticker and a balance sheet")

    rows = []
    for cik in ciks:
        meta = tick[cik]
        rev = ann.get("revenue", {}).get(cik, {})
        sbc = ann.get("sbc", {}).get(cik, {})
        nii = ann.get("interest_net", {}).get(cik, {})
        latest_rev = rev.get(max(rev)) if rev else None
        latest_sbc = sbc.get(max(sbc)) if sbc else None
        # Fee income plus net interest income is what a bank earns. For a
        # company with no interest business this adds nothing.
        latest_nii = nii.get(max(nii)) if nii else None
        if latest_nii is not None:
            latest_rev = (latest_rev or 0.0) + latest_nii

        filings = {k: v.get(cik) for k, v in inst.items()}
        filings.update({
            "shares": sh_now.get(cik),
            "shares_cagr": shares_growth(sh_then.get(cik), sh_now.get(cik),
                                         DILUTION_YEARS),
            "revenue": latest_rev,
            "sbc": latest_sbc,
            # ALWAYS present, possibly empty: the key is this build telling
            # schloss.evaluate it searched. An absent key would be read as
            # "not looked at" and leave every non-payer unknown.
            "div_per_share_by_year": divs.get(cik, {}),
            # Not found in any probe is a FACT about age, not an absence
            # of one — see bounded_first_year.
            "first_filing_year": bounded_first_year(first_year.get(cik),
                                                    PROBE_YEARS[-1]),
        })
        q = quotes.get(meta["ticker"]) or {}
        filings["market_cap"] = q.get("market_cap")
        filings["price"] = q.get("price")

        row = S.evaluate(filings, this_year)
        row.update({
            "cik": cik,
            # The anchor every "share of the balance sheet" figure needs.
            # Without it the asset mix is percentages of whatever lines
            # happened to be tagged, and a REIT that files only its cash
            # reads as 100% hard assets.
            "total_assets": filings.get("total_assets"),
            "ticker": meta["ticker"],
            "name": meta["name"],
            "exchange": meta["exchange"],
            "revenue": latest_rev,
            "shares": sh_now.get(cik),
            "history": history(first_year.get(cik), this_year, PROBE_YEARS[-1]),
            "price": q.get("price"),
        })
        rows.append(row)

    # Balance-sheet qualifiers first, then by how much of the price we are
    # missing — there is no cheapness ranking to sort on until a feed
    # lands, and inventing one would be a ranking of nothing.
    rows.sort(key=lambda r: (not r["gates_pass"], -(r["gates_passed"] or 0),
                             r["ticker"]))

    return {
        "generated": now.isoformat(timespec="seconds"),
        "census": S.census(rows),
        "rows": rows,
        "params": {
            "quote_source": quote_src,
            "balance_sheet_periods": periods,
            "dividend_years": [years[0], years[-1]],
            "probe_years": PROBE_YEARS,
            "dilution_years": DILUTION_YEARS,
            "thresholds": {
                "book_discount_min": S.BOOK_DISCOUNT_MIN,
                "ncav_max_price": round(S.NCAV_MAX_PRICE, 4),
                "debt_to_equity_max": S.DEBT_TO_EQUITY_MAX,
                "survival_years_min": S.SURVIVAL_YEARS_MIN,
                "sbc_to_revenue_max": S.SBC_TO_REVENUE_MAX,
                "dilution_max": S.DILUTION_MAX,
                "insider_aligned_min": S.INSIDER_ALIGNED_MIN,
            },
        },
    }


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--limit", type=int, default=0,
                    help="score only the first N companies (testing)")
    args = ap.parse_args()

    started = time.time()
    payload = build(limit=args.limit)
    c = payload["census"]

    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(payload, separators=(",", ":")))

    # ── THE POINT-IN-TIME RECORD ────────────────────────────────────
    # data/schloss.json is overwritten every run, so this board has never
    # had a history. That is not a cosmetic gap: the universe is CURRENT
    # SEC registrants, so a company that delists leaves it entirely, and
    # any backward-looking test is therefore missing exactly the names a
    # deep-value screen most needs to account for. There is no way to ask
    # "would that cutoff have beaten the market" of data that cannot see
    # what died.
    #
    # A name captured here stays captured. From this run forward the
    # answer becomes measurable — in about a year for a one-year hold,
    # which is the horizon this screen is actually used on.
    #
    # --limit runs are excluded on purpose. The file is keyed by month, so
    # a `--limit 50` smoke test run AFTER the real one would overwrite a
    # full month of history with fifty alphabetically-first companies and
    # nothing would look wrong. A truncated universe is not a
    # point-in-time record of anything.
    if args.limit:
        print(f"  snapshot                 skipped (--limit {args.limit}; "
              f"a partial universe is not a point-in-time record)")
    else:
        key = date.today().strftime("%Y-%m")
        SNAPSHOT_DIR.mkdir(parents=True, exist_ok=True)
        snap_path = SNAPSHOT_DIR / f"{key}.json"
        snap = S.snapshot(payload, date.today().isoformat())
        tmp = snap_path.with_suffix(".json.tmp")
        tmp.write_text(json.dumps(snap, separators=(",", ":")))
        os.replace(tmp, snap_path)
        kept = sorted(SNAPSHOT_DIR.glob("*.json"))
        dropped = 0
        for old in kept[:-MAX_SNAPSHOTS] if len(kept) > MAX_SNAPSHOTS else []:
            old.unlink()
            dropped += 1
        print(f"  snapshot                 {snap_path.name} "
              f"({len(snap['rows']):,} rows, "
              f"{snap_path.stat().st_size / 1e6:.1f}MB)"
              + (f", pruned {dropped}" if dropped else ""))

    print(f"\nWrote {OUT} in {time.time() - started:.0f}s")
    print(f"  screened                 {c['screened']:>6,}")
    print(f"  clear every gate         {c['balance_sheet_qualifying']:>6,}")
    print(f"  positive net current     {c['positive_ncav']:>6,}")
    print(f"  recent dividend cuts     {c['recent_dividend_cuts']:>6,}")
    print(f"  priced                   {c['priced']:>6,} "
          f"({c['price_coverage_pct']}%) via {payload['params']['quote_source'] or 'no source'}")
    if c["below_book"] is None:
        print("  below book / net-nets    UNKNOWN — no price feed attached")
    else:
        print(f"  below tangible book      {c['below_book']:>6,}")
        print(f"  net-nets                 {c['net_nets']:>6,}")
    return 0


if __name__ == "__main__":
    sys.exit(main())

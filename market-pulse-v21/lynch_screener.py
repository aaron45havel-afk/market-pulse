"""Peter Lynch GARP screener — the network half.

Everything that can be wrong about a NUMBER lives in lynch.py, which is
pure and tested offline. This file only fetches and assembles: universe,
one bulk market file, companyfacts per survivor, then hand the record to
`lynch.evaluate`.

WHY THE PRICES COME FROM A BULK FILE NOW. The old path asked one endpoint
per ticker — Finnhub at 1.2s each (~90 minutes) or a Yahoo retry ladder
that pricefeed.py documents returning ZERO of 100 tickers from GitHub
Actions IP ranges. The workflow set no timeout, so that path died at
GitHub's six-hour ceiling with no snapshot and no signal.

Rate limits count REQUESTS, not rows. One bulk market file carries price
AND market capitalisation for the entire US market in a single request,
needs no API key, and lets the Finnhub secret be deleted.

MARKET CAP COMES FROM THE FEED, NOT FROM price x shares. The old
computation disagreed with the Schloss board on the same companies in the
same week — PDD $504.2B against $130.6B, Vipshop $1.71B against $7.53B —
and the errors ran in BOTH directions, so no constant correction existed.
105 companies were overstated by more than 2x and 106 understated. Taking
the cap directly also removes the share count from the critical path,
which is the figure most often missing or wrong.
"""
from __future__ import annotations

import json
import logging
import time
import urllib.error
import urllib.request
from datetime import date
from pathlib import Path

import lynch as L
import pricefeed as PF
from sec_edgar import (
    SEC_UA,
    _get,
    _rc,
    _wc,
    _is_excluded_sic,
    _excluded_keyword,
    _is_warrant,
    get_tickers,
    get_exchanges,
    get_company_details_bulk,
)

log = logging.getLogger(__name__)

# Thresholds live in lynch.py beside the logic that applies them, and are
# re-exported here so the snapshot's _meta can carry them and the page can
# template its own copy from the payload instead of hard-coding prose that
# drifts out of step with the board it describes.
LYNCH_RULES = {
    "market_cap_min": L.MARKET_CAP_MIN,
    "pe_max": L.PE_MAX,
    "pe_min": L.PE_SANE[0],
    "eps_growth_3yr_min_pct": L.EPS_GROWTH_MIN,
    "eps_growth_cap_pct": L.EPS_GROWTH_CAP,
    "debt_to_equity_max": L.DEBT_TO_EQUITY_MAX,
    "capex_to_ocf_max": L.CAPEX_TO_OCF_MAX,
    "exchanges": ("NYSE", "Nasdaq", "NASDAQ", "AMEX", "NYSE American"),
    "min_eps_years": L.MIN_EPS_YEARS,
    "min_revenue": L.MIN_REVENUE,
    "min_price": L.MIN_PRICE,
    "moat_roe_min_pct": L.MOAT_ROE_MIN,
    "moat_consecutive_positive_eps_years": L.MOAT_POS_EPS_YEARS,
}

MAJOR_EXCHANGES = {e.upper() for e in LYNCH_RULES["exchanges"]}

BROWSER_UA = ("Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
              "(KHTML, like Gecko) Chrome/120.0 Safari/537.36")
QUOTE_HEADERS = {"User-Agent": BROWSER_UA, "Accept-Encoding": "gzip, deflate",
                 "Accept": "application/json, text/plain, */*",
                 "Accept-Language": "en-US,en;q=0.9"}


# ═══════════════════════════════════════════════════════════════════
# BULK QUOTES
# ═══════════════════════════════════════════════════════════════════

def _quote_json(url: str, timeout: int = 90) -> dict | None:
    try:
        req = urllib.request.Request(url, headers=QUOTE_HEADERS)
        with urllib.request.urlopen(req, timeout=timeout) as r:
            raw = r.read()
            if r.headers.get("Content-Encoding") == "gzip":
                import gzip
                raw = gzip.decompress(raw)
        return json.loads(raw)
    except Exception as e:                      # noqa: BLE001 — any failure
        log.warning("  ! %s: %s: %s", url.split("/")[2], type(e).__name__, e)
        return None


def fetch_quotes_bulk(tickers: list[str]) -> tuple[dict, str]:
    """({TICKER: {price, market_cap}}, source). One request per source.

    Sources are tried in order and the first to cover enough of the board
    wins. A source that answers with a fraction of the market is a
    FAILURE, not a partial success: half a board priced reads as "these
    ones did not qualify" rather than "we could not look at these".
    """
    want = set(tickers)
    for src in PF.BULK_SOURCES:
        log.info("  trying %s …", src["name"])
        quotes: dict = {}
        if src.get("batched"):
            size = src.get("batch_size", 200)
            ordered = sorted(want)
            for i in range(0, len(ordered), size):
                import urllib.parse
                payload = _quote_json(src["url"].format(
                    syms=urllib.parse.quote(",".join(ordered[i:i + size]))))
                if payload is None:
                    break
                quotes.update(src["parse"](payload))
                time.sleep(0.4)
        else:
            payload = _quote_json(src["url"])
            if payload is not None:
                quotes = src["parse"](payload)

        hit = quotes.keys() & want
        cover = len(hit) / len(want) if want else 0.0
        log.info("    %d quotes, %d matched (%.0f%% of the universe)",
                 len(quotes), len(hit), cover * 100)
        if cover >= PF.BULK_COVERAGE_MIN:
            return {k: quotes[k] for k in hit}, src["name"]
        log.warning("    below the %.0f%% floor — trying the next source",
                    PF.BULK_COVERAGE_MIN * 100)
    return {}, ""


# ═══════════════════════════════════════════════════════════════════
# UNIVERSE
# ═══════════════════════════════════════════════════════════════════

def build_universe() -> list[dict]:
    """Major-exchange filers minus financials, biotech, warrants and SPACs.

    ADRs stay in — they are buyable on any US broker — but the old page
    split them into a table headed "International ADRs" on the basis of
    `has 20-F and no 10-K`, which is a filing fact wearing a geography
    label. A Canadian 40-F filer, an Israeli issuer and a Chinese VIE all
    landed in one bucket. The split is gone; location is a column.
    """
    tickers = get_tickers()
    exchanges = get_exchanges()
    log.info("Universe: %d tickers, %d with exchange data",
             len(tickers), len(exchanges))

    pre: list[dict] = []
    for cik, t in tickers.items():
        ticker = (t.get("ticker") or "").upper()
        name = t.get("name") or ""
        if not ticker or not name or _is_warrant(ticker) or _excluded_keyword(name):
            continue
        exch = (exchanges.get(cik) or {}).get("exchange", "")
        # FAIL CLOSED. The old test was `if exch and exch.upper() not in
        # MAJOR_EXCHANGES` — a blank exchange string passed straight
        # through into a table headed "United States".
        if exch.upper() not in MAJOR_EXCHANGES:
            continue
        pre.append({"cik": cik, "ticker": ticker, "name": name, "exchange": exch})

    log.info("  after exchange/name/warrant filter: %d", len(pre))

    details = get_company_details_bulk([r["cik"] for r in pre])
    out: list[dict] = []
    dropped_sic = 0
    for row in pre:
        d = details.get(row["cik"]) or {}
        sic = d.get("sic")
        if _is_excluded_sic(sic):
            dropped_sic += 1
            continue
        row["sic"] = sic
        row["state"] = d.get("state_desc") or d.get("state") or ""
        out.append(row)
    log.info("  after SIC filter: %d (%d excluded)", len(out), dropped_sic)
    return out


def _fetch_companyfacts(cik: str) -> dict | None:
    padded = str(cik).zfill(10)
    return _get(f"https://data.sec.gov/api/xbrl/companyfacts/CIK{padded}.json")


# ═══════════════════════════════════════════════════════════════════
# FACTS → RECORD
# ═══════════════════════════════════════════════════════════════════

# Most-coverage-wins across each list, rather than first-tag-wins. A filer
# reporting capex under PaymentsToAcquireProductiveAssets was deleted with
# no trace because only the generic PP&E tag was ever consulted.
REVENUE_TAGS = ["RevenueFromContractWithCustomerExcludingAssessedTax",
                "Revenues", "RevenueFromContractWithCustomerIncludingAssessedTax",
                "SalesRevenueNet"]
CAPEX_TAGS = ["PaymentsToAcquirePropertyPlantAndEquipment",
              "PaymentsToAcquireProductiveAssets",
              "PaymentsForCapitalImprovements",
              "PaymentsToAcquirePropertyPlantAndEquipmentAndIntangibleAssets"]
OCF_TAGS = ["NetCashProvidedByUsedInOperatingActivities",
            "NetCashProvidedByUsedInOperatingActivitiesContinuingOperations"]
ST_DEBT_TAGS = ["ShortTermBorrowings", "OtherShortTermBorrowings",
                "LongTermDebtCurrent", "DebtCurrent"]
LT_DEBT_TAGS = ["LongTermDebt", "LongTermDebtNoncurrent",
                "LongTermDebtAndCapitalLeaseObligations"]


def _last(series: dict):
    return series[max(series)] if series else None


def facts_to_record(row: dict, quote: dict, facts: dict, as_of: str) -> dict:
    """Assemble everything lynch.evaluate needs. No judgement here."""
    eps_by_year, _c, eps_unit = L.eps_series(facts)
    revenue, _rc = L.annual_series(facts, REVENUE_TAGS)
    ni_by_year, _nc = L.annual_series(facts, "NetIncomeLoss")
    op_by_year, _oc = L.annual_series(facts, "OperatingIncomeLoss")
    capex, _cc = L.annual_series(facts, CAPEX_TAGS)
    ocf, _oc2 = L.annual_series(facts, OCF_TAGS)

    equity, _ = L.instant_value(facts, ["StockholdersEquity",
                                        "StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest"])
    assets, _ = L.instant_value(facts, "Assets")
    liabilities, _ = L.instant_value(facts, "Liabilities")
    ppe, _ = L.instant_value(facts, "PropertyPlantAndEquipmentNet")
    st_debt, _ = L.instant_value(facts, ST_DEBT_TAGS)
    lt_debt, _ = L.instant_value(facts, LT_DEBT_TAGS)

    shares = None
    for tax, concept in (("us-gaap", "CommonStockSharesOutstanding"),
                         ("dei", "EntityCommonStockSharesOutstanding")):
        node = ((facts or {}).get("facts") or {}).get(tax) or {}
        entries = ((node.get(concept) or {}).get("units") or {}).get("shares")
        if entries:
            pts = L._instant(entries) or L._spans(entries)
            if pts:
                shares = pts[-1][1]
                break

    # Equity by year for the ROE series — instantaneous, so year-keyed on
    # the period end.
    eq_entries = ((((facts or {}).get("facts") or {}).get("us-gaap") or {})
                  .get("StockholdersEquity") or {}).get("units", {}).get("USD")
    equity_by_year = dict(L._instant(eq_entries)) if eq_entries else {}

    return {
        "ticker": row["ticker"], "name": row["name"], "cik": row.get("cik"),
        "exchange": row.get("exchange") or "", "sic": row.get("sic"),
        "state": row.get("state") or "",
        "as_of": as_of,
        "price": quote.get("price"),
        "market_cap": quote.get("market_cap"),
        "shares": shares,
        "equity": equity,
        "total_assets": assets,
        "total_liabilities": liabilities,
        "ppe": ppe,
        "short_term_debt": st_debt,
        "long_term_debt": lt_debt,
        "revenue": _last(revenue),
        "net_income": _last(ni_by_year),
        "op_income": _last(op_by_year),
        "capex": _last(capex),
        "ocf": _last(ocf),
        "eps_by_year": eps_by_year,
        "eps_unit": eps_unit,
        "net_income_by_year": ni_by_year,
        "equity_by_year": equity_by_year,
        "last_filing": L.first_filing_end(facts),
        "is_international": _is_foreign_issuer(facts),
    }


def _is_foreign_issuer(facts: dict) -> bool:
    """Files 20-F but no 10-K. Reported as a FILING fact, never as a
    geography — the old page used it as one."""
    if not facts:
        return False
    us_gaap = (facts.get("facts") or {}).get("us-gaap") or {}
    has_20f = False
    for concept in ("NetIncomeLoss", "Revenues", "Assets",
                    "EarningsPerShareDiluted", "EarningsPerShareBasic"):
        for entries in ((us_gaap.get(concept) or {}).get("units") or {}).values():
            for e in entries:
                form = e.get("form", "")
                if form.startswith("10-K"):
                    return False
                if form.startswith("20-F"):
                    has_20f = True
    return has_20f


# ═══════════════════════════════════════════════════════════════════
# BUILD
# ═══════════════════════════════════════════════════════════════════

def build_lynch_screener(max_companyfacts: int | None = None) -> dict:
    """{"rows": [...], "census": {...}, "quote_source": str}.

    RETURNS EVERY ROW, passing or not, each carrying a verdict and a
    reason code. The old version returned only survivors, so a file with
    nine rows was produced identically by a strict screen in a rich market
    and by a parser rejecting everything — there was no way to tell which
    one you were looking at.
    """
    as_of = date.today().isoformat()
    universe = build_universe()
    tickers = [u["ticker"] for u in universe]

    log.info("Bulk quotes (one request per source):")
    quotes, source = fetch_quotes_bulk(tickers)
    if not quotes:
        raise SystemExit(
            "No quote source answered. Refusing to publish a board with no "
            "prices — a snapshot of nothing overwrites a good one.")

    priced = [u for u in universe if u["ticker"] in quotes]
    log.info("Priced: %d/%d (source: %s)", len(priced), len(tickers), source)
    if max_companyfacts:
        priced = priced[:max_companyfacts]

    log.info("Pulling companyfacts for %d companies …", len(priced))
    rows: list[dict] = []
    last_log = time.time()
    for i, row in enumerate(priced, 1):
        if i % 10 == 0:
            time.sleep(1.1)                     # SEC fair access: 10 req/s
        key = f"lynch_facts_{row['cik']}"
        facts = _rc(key, 168)
        if facts is None:
            facts = _fetch_companyfacts(row["cik"]) or {}
            _wc(key, facts)
        try:
            rec = facts_to_record(row, quotes[row["ticker"]], facts, as_of)
            rows.append(L.evaluate(rec))
        except Exception as e:                  # pragma: no cover — defensive
            log.warning("  screen error for %s: %s", row["ticker"], e)
        if time.time() - last_log > 30:
            passing = sum(1 for r in rows if r["verdict"] == "pass")
            log.info("  %d/%d done (%d passing)", i, len(priced), passing)
            last_log = time.time()

    # Sort passers by PEG, but ONLY after every input to PEG is bounded —
    # unbounded growth made the old ranking monotone in the estimator's
    # own error, and DEEP fired on 100% of rows.
    rows.sort(key=lambda r: (
        r["verdict"] != "pass",
        r.get("peg") if r.get("peg") is not None else 99.0,
        r.get("ticker") or "",
    ))
    c = L.census(rows)
    log.info("Lynch: %d of %d pass; %d unmeasurable",
             c["passing"], c["screened"], c["unmeasured"])
    return {"rows": rows, "census": c, "quote_source": source}

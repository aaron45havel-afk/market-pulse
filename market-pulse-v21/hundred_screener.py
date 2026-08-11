"""100-bagger checklist screener — the network half.

Every number lives in checklist.py, which is pure and provable offline.
This file fetches and assembles, reusing the Lynch screener's universe and
its single bulk quote file so the monthly run costs no extra requests
beyond the companyfacts pull the other boards already make.

TWO PASSES, and the second one is the reason this file exists rather than
a loop. Criterion 11 asks how far below its PEER GROUP a company trades,
and a peer median cannot be computed until every peer has been read. So:
pass one builds records and P/Es, then medians are taken by SIC, then pass
two scores. The medians are computed over the whole screened universe
rather than over the survivors, because a median of survivors is a median
of companies already selected for being cheap.
"""
from __future__ import annotations

import logging
import time
from datetime import date

import checklist as C
import lynch as L
import schloss as S
from lynch_screener import (CAPEX_TAGS, OCF_TAGS, REVENUE_TAGS, _fetch_companyfacts,
                            build_universe, fetch_quotes_bulk)
from sec_edgar import _rc, _wc

log = logging.getLogger(__name__)

# The two-digit SIC division a company is compared against for criterion
# 11. Four digits is a truer peer group and empties out — most four-digit
# codes carry fewer than the 25 peers a median needs, and a criterion that
# is unmeasured for everybody teaches nothing.
SIC_PREFIX = 2

# A row is LISTED when at least this many of the seven scoreable criteria
# were measured AND at least this many of them came out Good or Great.
#
# BOTH CONDITIONS, and the second is what stops thin data buying a place.
# "Every criterion we measured passed" is trivially true for a company we
# measured once; requiring four actual passes means a company must be seen
# clearly AND be good, which is the only combination worth reading about.
MIN_MEASURED = 4
MIN_PASSING = 4

GROSS_PROFIT_TAGS = ["GrossProfit"]
COST_TAGS = ["CostOfRevenue", "CostOfGoodsAndServicesSold",
             "CostOfGoodsSold", "CostOfServices"]
EQUITY_TAGS = ["StockholdersEquity",
               "StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest"]

RULES = {
    "criteria_total": C.CRITERIA,
    "scoreable_ids": list(C.SCOREABLE_IDS),
    "never_scored_ids": list(C.NEVER_SCORED_IDS),
    "not_scored_because": C.NOT_SCORED_BECAUSE,
    "criterion_names": {str(k): v for k, v in C.CRITERION_NAMES.items()},
    "thresholds": {str(k): list(v) for k, v in C.THRESHOLDS.items()},
    "descending_ids": list(C.DESCENDING),
    "max_spans": C.MAX_SPANS,
    "peer_min": C.PEER_MIN,
    "sic_prefix": SIC_PREFIX,
    "min_measured": MIN_MEASURED,
    "min_passing": MIN_PASSING,
}


def facts_to_record(row: dict, quote: dict, facts: dict, as_of: str) -> dict:
    """One company's filings reduced to the inputs the checklist needs."""
    rev_by_year, rev_tag = L.annual_series(facts, REVENUE_TAGS)
    ni_by_year, _ = L.annual_series(facts, "NetIncomeLoss")
    op_by_year, _ = L.annual_series(facts, "OperatingIncomeLoss")
    gp_by_year, _ = L.annual_series(facts, GROSS_PROFIT_TAGS)
    cost_by_year, _ = L.annual_series(facts, COST_TAGS)
    eps_by_year, eps_concept, eps_unit = L.eps_series(facts)

    equity, _, _ = L.instant_value(facts, EQUITY_TAGS)
    assets, _, _ = L.instant_value(facts, "Assets")
    ppe, _, _ = L.instant_value(facts, "PropertyPlantAndEquipmentNet")
    cur_assets, _, _ = L.instant_value(facts, "AssetsCurrent")
    cur_liabs, _, _ = L.instant_value(facts, "LiabilitiesCurrent")

    eq_entries = ((((facts or {}).get("facts") or {}).get("us-gaap") or {})
                  .get("StockholdersEquity") or {}).get("units", {}).get("USD")
    equity_by_year = dict(L._instant(eq_entries)) if eq_entries else {}

    last = lambda s: s[max(s)] if s else None
    return {
        "ticker": row["ticker"], "name": row["name"], "cik": row.get("cik"),
        "exchange": row.get("exchange") or "",
        # SIC IS LOAD-BEARING HERE, not decoration: it is the peer grouping
        # for criterion 11, and it must reach the row even for companies
        # that get rejected, or the peer medians are computed over
        # survivors only.
        "sic": row.get("sic"), "state": row.get("state") or "",
        "as_of": as_of,
        "price": quote.get("price"), "market_cap": quote.get("market_cap"),
        "revenue_by_year": rev_by_year, "revenue_tag": rev_tag,
        "revenue": last(rev_by_year),
        "gross_profit": last(gp_by_year), "cost_of_revenue": last(cost_by_year),
        "net_income_by_year": ni_by_year, "net_income": last(ni_by_year),
        "equity_by_year": equity_by_year, "equity": equity,
        "eps_by_year": eps_by_year, "eps_unit": eps_unit,
        "eps_concept": eps_concept,
        "op_income": last(op_by_year), "ppe": ppe,
        "current_assets": cur_assets, "current_liabilities": cur_liabs,
        "total_assets": assets,
        "last_filing": L.first_filing_end(facts),
    }


def _pe(rec: dict) -> float | None:
    """Market cap over net income, bounded by the Lynch sanity band.

    Reused rather than reimplemented so the two boards cannot disagree
    about the same company's multiple. A P/E outside the plausible band is
    withheld, which also keeps it out of the peer median — one company
    reporting a 0.2 multiple would otherwise drag its whole SIC group.
    """
    info = L.price_earnings(rec.get("market_cap"), rec.get("net_income"))
    return info["pe"]


def peer_medians(records: list[dict]) -> dict:
    """{sic_prefix: (median P/E, peer count)} over the WHOLE universe.

    Not over the survivors. A median computed from companies already
    selected for being cheap is a median of cheap companies, and every
    discount measured against it would collapse toward zero.
    """
    groups: dict[str, list] = {}
    for r in records:
        sic = str(r.get("sic") or "")[:SIC_PREFIX]
        pe = r.get("pe_ratio")
        if sic and pe is not None:
            groups.setdefault(sic, []).append(pe)
    return {k: (L.median(v), len(v)) for k, v in groups.items()}


def evaluate(rec: dict, medians: dict) -> dict:
    """Score one company against the seven scoreable criteria."""
    out = {k: rec.get(k) for k in
           ("ticker", "name", "cik", "exchange", "sic", "state",
            "price", "market_cap", "revenue", "eps_unit", "last_filing")}
    out["size_band"] = L.size_band(rec.get("market_cap"))
    out["location"] = L.location(rec)

    # Data sanity first. These are not checklist criteria — they are the
    # conditions under which the checklist can be applied at all, and a
    # company failing them is REJECTED rather than scored badly.
    if rec.get("price") is None or rec.get("market_cap") is None:
        return {**out, "verdict": "reject", "reason": "no_quote", "ledger": C.ledger({})}
    if L._stale(rec.get("last_filing"), rec.get("as_of")):
        return {**out, "verdict": "reject", "reason": "dormant", "ledger": C.ledger({})}

    pe = rec.get("pe_ratio")
    out["pe_ratio"] = pe
    sic = str(rec.get("sic") or "")[:SIC_PREFIX]
    med, n = medians.get(sic, (None, 0))
    out["peer_median_pe"], out["peer_count"], out["peer_sic"] = med, n, sic

    m = {
        1: C.sales_growth(rec.get("revenue_by_year")),
        2: C.gross_margin(rec.get("revenue"), rec.get("gross_profit"),
                          rec.get("cost_of_revenue")),
        3: C.eps_growth(rec.get("eps_by_year")),
        4: C.return_on_equity(rec.get("net_income_by_year"),
                              rec.get("equity_by_year")),
        5: C.return_on_capital(rec.get("op_income"), rec.get("current_assets"),
                               rec.get("current_liabilities"), rec.get("ppe")),
        11: C.peer_discount(pe, med, n),
        13: C.market_cap(rec.get("market_cap")),
    }
    out["measures"] = {
        str(cid): {"value": (x.value if x.measured else None),
                   "measured": x.measured, "band": x.band,
                   "reason": x.reason, "basis": x.basis}
        for cid, x in m.items()
    }
    led = C.ledger(m)
    out["ledger"] = led

    if led["measured_n"] < MIN_MEASURED:
        return {**out, "verdict": "thin",
                "reason": f"only {led['measured_n']} of "
                          f"{len(C.SCOREABLE_IDS)} scoreable criteria measured"}
    if led["passing_n"] < MIN_PASSING:
        return {**out, "verdict": "reject",
                "reason": f"{led['passing_n']} Good-or-Great, need {MIN_PASSING}"}
    return {**out, "verdict": "list", "reason": "listed"}


def build(max_companyfacts: int | None = None) -> dict:
    """{"rows", "census", "quote_source"} — every company, scored or not."""
    as_of = date.today().isoformat()
    universe = build_universe()
    tickers = [u["ticker"] for u in universe]

    log.info("Bulk quotes (one request per source):")
    quotes, source = fetch_quotes_bulk(tickers)
    if not quotes:
        raise SystemExit("No quote source answered. Refusing to publish a "
                         "board with no prices.")

    priced = [u for u in universe if u["ticker"] in quotes]
    log.info("Priced: %d/%d (source: %s)", len(priced), len(tickers), source)

    rows: list[dict] = [
        {"ticker": u["ticker"], "name": u["name"], "cik": u.get("cik"),
         "sic": u.get("sic"), "verdict": "reject", "reason": "no_quote",
         "ledger": C.ledger({})}
        for u in universe if u["ticker"] not in quotes
    ]
    if rows:
        log.info("  %d with no quote — screened, not dropped", len(rows))

    if max_companyfacts:
        priced = priced[:max_companyfacts]

    log.info("Pass 1: companyfacts for %d companies …", len(priced))
    records: list[dict] = []
    last_log = time.time()
    for i, row in enumerate(priced, 1):
        if i % 10 == 0:
            time.sleep(1.1)
        key = f"lynch_facts_{row['cik']}"
        facts = _rc(key, 168)
        if facts is None:
            facts = _fetch_companyfacts(row["cik"])
            if facts:
                _wc(key, facts)
        if not facts:
            rows.append({"ticker": row["ticker"], "name": row.get("name") or "",
                         "cik": row.get("cik"), "sic": row.get("sic"),
                         "verdict": "reject", "reason": "facts_unavailable",
                         "ledger": C.ledger({})})
            continue
        try:
            rec = facts_to_record(row, quotes[row["ticker"]], facts, as_of)
            rec["pe_ratio"] = _pe(rec)
            records.append(rec)
        except Exception as e:                  # pragma: no cover — defensive
            log.warning("  record error for %s: %s", row["ticker"], e)
            rows.append({"ticker": row["ticker"], "name": row.get("name") or "",
                         "cik": row.get("cik"), "sic": row.get("sic"),
                         "verdict": "reject", "reason": "screen_error",
                         "ledger": C.ledger({})})
        if time.time() - last_log > 30:
            log.info("  %d/%d read", i, len(priced))
            last_log = time.time()

    medians = peer_medians(records)
    usable = sum(1 for _m, n in medians.values() if n >= C.PEER_MIN)
    log.info("Pass 2: %d SIC groups, %d with %d+ peers for a median",
             len(medians), usable, C.PEER_MIN)

    for rec in records:
        try:
            rows.append(evaluate(rec, medians))
        except Exception as e:                  # pragma: no cover — defensive
            log.warning("  score error for %s: %s", rec.get("ticker"), e)
            rows.append({"ticker": rec.get("ticker"), "name": rec.get("name") or "",
                         "cik": rec.get("cik"), "sic": rec.get("sic"),
                         "verdict": "reject", "reason": "screen_error",
                         "ledger": C.ledger({})})

    # SORTED BY HOW MANY OF THE SEVEN IT CLEARS, and that is all this is.
    # Not a quality order and not a rank: the page says so, because the
    # sister board's own data shows one criterion owning 60-84% of every
    # floor, which makes any composite ordering a proxy for that criterion.
    rows.sort(key=lambda r: (
        {"list": 0, "thin": 1, "reject": 2}.get(r.get("verdict"), 3),
        -(r.get("ledger") or {}).get("passing_n", 0),
        -(r.get("ledger") or {}).get("measured_n", 0),
        r.get("ticker") or "",
    ))
    cen = C.census(rows)
    log.info("100-bagger: %d listed, %d thin, %d rejected of %d screened",
             cen["listed"], cen["thin"], cen["rejected"], cen["screened"])
    return {"rows": rows, "census": cen, "quote_source": source}

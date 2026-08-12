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
from sec_edgar import SEC_UA, _get, _rc, _wc

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
    "gate_ids": list(C.GATE_IDS),
    "veto_ids": list(C.VETO_IDS),
    "veto_reasons": {str(k): v for k, v in C.VETO_REASONS.items()},
    "insider_min_pct": C.INSIDER_MIN_PCT,
    "market_cap_ceiling": C.MARKET_CAP_CEILING,
    "pe_plausible_max": C.PE_PLAUSIBLE_MAX,
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
    eq_by_year_all = equity_by_year

    last = lambda s: s[max(s)] if s else None

    # Series for the two FREE veto criteria. Both come out of the same
    # companyfacts blob already in hand — no extra request, which is why
    # they ship before the one that costs 296.
    margin_by_year = {}
    for y, rev in rev_by_year.items():
        gp = gp_by_year.get(y)
        if gp is None and y in cost_by_year:
            gp = rev - cost_by_year[y]
        if gp is not None and rev and rev > 0:
            margin_by_year[y] = gp / rev * 100.0
    # Invested capital, Greenblatt's basis so it matches criterion 5's
    # denominator rather than inventing a second definition.
    cap_by_year = {}
    for y in set(eq_by_year_all) & set(op_by_year):
        e = eq_by_year_all.get(y)
        if e is not None and e > 0:
            cap_by_year[y] = e

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
        "op_income_by_year": op_by_year,
        "margin_by_year": margin_by_year,
        "invested_capital_by_year": cap_by_year,
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
    pe = info["pe"]
    # BOUNDED AT BOTH ENDS BEFORE IT REACHES THE MEDIAN. Withholding it in
    # peer_discount alone would keep an implausible multiple out of that
    # company's own row while still letting it into its SIC group's median,
    # where it would move every peer's discount by a little instead of one
    # company's by a lot.
    return None if (pe is not None and pe > C.PE_PLAUSIBLE_MAX) else pe


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
    # THE SIZE PRECONDITION, before anything is scored. Not a criterion
    # among thirteen here — the arithmetic of a hundred-bagger simply is
    # not available above it, whatever the other twelve say.
    cap = rec.get("market_cap")
    if cap is not None and cap > C.MARKET_CAP_CEILING:
        return {**out, "verdict": "reject", "reason": "too_big_to_100x",
                "ledger": C.ledger({})}

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
        # THE VETOES. Free, from series already built. They can only
        # remove a company — see checklist.VETO_IDS for why a count-of-
        # passes rule could not have used them.
        6: C.capital_veto(rec.get("op_income_by_year"),
                          rec.get("invested_capital_by_year")),
        8: C.moat_veto(rec.get("revenue_by_year"), rec.get("margin_by_year")),
    }
    if "insider_pct" in rec or "insider_reason" in rec:
        m[7] = C.ownership_veto(rec.get("insider_pct"), rec.get("insider_reason") or "")
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
    # THE VETO IS LAST. A company has to earn its way to this line first;
    # only then can one of the three say no. That ordering keeps the veto
    # strictly subtractive — it can never promote anything.
    if led["vetoed"]:
        why = ", ".join(C.VETO_REASONS[c] for c in led["vetoed_by"])
        return {**out, "verdict": "reject", "reason": f"veto: {why}"}
    return {**out, "verdict": "list", "reason": "listed"}


# ── PASS THREE: ownership, bought only for survivors ────────────────
#
# THE COST ARGUMENT CHANGED WITH THE POPULATION. Criterion 7 was ruled
# unobtainable against 3,319 companies, where two requests each is 6,638
# and the run has no budget for it. The reading list is ~150 names: two
# each is ~300 requests, half a minute at SEC's 10/second, inside a job
# with 70 minutes of headroom. Same shape as the $30m floor costing
# nothing because it sits after the fetch.
#
# It is bought ONLY for rows already listed, which is also why criterion 7
# can never be a scoreable criterion: you cannot gate on a value you
# purchase for survivors. It vetoes or it says nothing.
OWNERSHIP_TIMEOUT = 30


def _submissions(cik: str) -> dict | None:
    return _get(f"https://data.sec.gov/submissions/CIK{str(cik).zfill(10)}.json")


def find_proxy(subs: dict) -> tuple[str | None, str]:
    """(document URL, reason). The most recent DEF 14A, or why there is none.

    THE ELIGIBILITY TEST IS FREE and comes out of the same call. A filer
    with a 20-F or 40-F and no DEF 14A is a foreign private issuer: Rule
    3a12-3(b) exempts it from Section 16 AND Regulation 14A, so there is no
    proxy statement and no Form 4 to find. That is a property of the filing
    regime, not of the company's insiders, and the row must say so rather
    than report no ownership.
    """
    recent = ((subs or {}).get("filings") or {}).get("recent") or {}
    forms = recent.get("form") or []
    if not forms:
        return None, "no filing index"
    accs = recent.get("accessionNumber") or []
    docs = recent.get("primaryDocument") or []
    cik = str((subs or {}).get("cik") or "").lstrip("0")
    for i, f in enumerate(forms):
        # EXACTLY "DEF 14A". Not DEFA14A (additional soliciting material,
        # no ownership table) and not DEFM14A (merger proxy).
        if f == "DEF 14A" and i < len(accs) and i < len(docs):
            return (f"https://www.sec.gov/Archives/edgar/data/{cik}/"
                    f"{accs[i].replace('-', '')}/{docs[i]}"), ""
    if any(f in ("20-F", "40-F") for f in forms):
        return None, ("foreign private issuer — files 20-F, exempt from "
                      "Regulation 14A; no proxy statement exists")
    return None, "no proxy statement filed yet"


def fetch_ownership(rows: list[dict]) -> dict:
    """{ticker: (pct|None, reason)} for LISTED rows only.

    Fails soft on every path. If SEC rate-limits, times out or changes the
    document layout, criterion 7 goes unmeasured and the other six publish
    exactly as they would have — the pass is strictly additive.
    """
    import urllib.request
    out: dict = {}
    for i, r in enumerate(rows, 1):
        if i % 10 == 0:
            time.sleep(1.1)
        cik = r.get("cik")
        if not cik:
            out[r["ticker"]] = (None, "no CIK")
            continue
        try:
            url, why = find_proxy(_submissions(cik))
            if not url:
                out[r["ticker"]] = (None, why)
                continue
            req = urllib.request.Request(url, headers={"User-Agent": SEC_UA})
            with urllib.request.urlopen(req, timeout=OWNERSHIP_TIMEOUT) as resp:
                body = resp.read().decode("utf-8", "replace")
            out[r["ticker"]] = C.parse_group_ownership(body)
        except Exception as e:                  # noqa: BLE001 — fail soft
            out[r["ticker"]] = (None, f"{type(e).__name__} reading the proxy")
    return out


def build(max_companyfacts: int | None = None,
          skip_ownership: bool = False) -> dict:
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
    # ── pass three ──
    listed = [r for r in rows if r.get("verdict") == "list"]
    if listed and not skip_ownership:
        log.info("Pass 3: ownership for %d listed companies (~%d requests) …",
                 len(listed), len(listed) * 2)
        try:
            own = fetch_ownership(listed)
            by_ticker = {r["ticker"]: r for r in records}
            replaced = 0
            for r in listed:
                pct, why = own.get(r["ticker"], (None, "not attempted"))
                rec = by_ticker.get(r["ticker"])
                if rec is None:
                    continue
                rec["insider_pct"], rec["insider_reason"] = pct, why
                scored = evaluate(rec, medians)
                rows[rows.index(r)] = scored
                replaced += 1
            got = sum(1 for p, _w in own.values() if p is not None)
            vetoed = sum(1 for r in rows if r.get("verdict") == "reject"
                         and str(r.get("reason", "")).startswith("veto:"))
            log.info("  ownership read for %d of %d; %d rows re-scored; "
                     "%d vetoed in total", got, len(listed), replaced, vetoed)
        except Exception as e:                  # noqa: BLE001
            # STRICTLY ADDITIVE. The six criteria publish regardless.
            log.warning("  ownership pass failed (%s) — criterion 7 unmeasured, "
                        "everything else unaffected", e)

    cen = C.census(rows)
    log.info("100-bagger: %d listed, %d thin, %d rejected of %d screened",
             cen["listed"], cen["thin"], cen["rejected"], cen["screened"])
    return {"rows": rows, "census": cen, "quote_source": source}

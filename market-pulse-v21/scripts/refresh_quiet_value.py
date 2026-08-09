"""Build the quiet-value screen: low-turnover small companies that are
also cash-rich, low-debt, capital-light, profitable and cheap.

WHY THIS IS A CI SCRIPT AND NOT A LIVE ROUTE: it needs three things the
app server should never do on a request — SEC EDGAR XBRL frames, a price
per ticker, and a YEAR of daily volume per ticker. That is thousands of
HTTP calls. It also cannot be developed against live data in the sandbox
at all: sec.gov, Yahoo and Finnhub are all unreachable from there, the
same wall that forced the crime layer into CI. So the maths lives in
liquidity.py and quality_value.py where it is provable offline, and this
script is only the plumbing that feeds them.

ORDER OF OPERATIONS, and why: the expensive call is the volume history,
so it runs last and only on names that have already survived the cheap
filters. EDGAR frames give the whole universe's fundamentals in a
handful of calls; total assets narrows to plausible small companies;
prices turn that into a real market cap; only then do we ask for a year
of daily volume.

Usage:  python scripts/refresh_quiet_value.py [--limit 400] [--dry-run]
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

import liquidity as LQ                                    # noqa: E402
import quality_value as QV                                # noqa: E402
import sec_edgar as SE                                    # noqa: E402
from lynch_screener import fetch_prices_bulk              # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
log = logging.getLogger("quiet-value")

OUT = ROOT / "data" / "quiet_value.json"
YAHOO_CHART = ("https://query1.finance.yahoo.com/v8/finance/chart/{t}"
               "?range=1y&interval=1d")
BROWSER_UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
              "(KHTML, like Gecko) Chrome/124.0 Safari/537.36")

# Only names small enough to plausibly be in the cohort the research
# describes. Assets is a crude stand-in for market cap, used only to
# avoid fetching prices for the whole market; the real size cut happens
# after we have a price.
MAX_ASSETS_FOR_CANDIDACY = 3_000_000_000
MAX_MARKET_CAP = 2_000_000_000        # nano through small
MIN_MARKET_CAP = 10_000_000           # below this, listings are mostly shells

# Refuse to publish a mostly-empty run. A screen that silently shrinks to
# nine names reads as "the market has nine bargains", not "the feed
# broke" — the same failure the crime refresher guards against.
MIN_ROWS_TO_WRITE = 25


def _extra_frames() -> dict[str, dict]:
    """Concepts the net-net screener does not fetch but this screen needs.

    Kept here rather than added to sec_edgar.BS_CONCEPTS so the net-net
    screener's cached payload and its cache key stay exactly as they are.
    """
    cbs, cis, _pbs = SE._periods()
    wanted = {
        "operating_income": ("OperatingIncomeLoss", cis, "USD"),
        "capex": ("PaymentsToAcquirePropertyPlantAndEquipment", cis, "USD"),
        "ocf": ("NetCashProvidedByUsedInOperatingActivities", cis, "USD"),
    }
    out: dict[str, dict] = {}
    with ThreadPoolExecutor(max_workers=4) as ex:
        futs = {ex.submit(SE._frame, c, p, u): name for name, (c, p, u) in wanted.items()}
        for fut in as_completed(futs):
            name = futs[fut]
            try:
                for cik, entry in (fut.result() or {}).items():
                    out.setdefault(cik, {})[name] = entry.get("val")
            except Exception as e:                          # noqa: BLE001
                log.warning("frame %s failed: %s", name, e)
    return out


def fetch_volume_year(ticker: str) -> list[float | None] | None:
    """A year of daily share volume. None on miss — never an empty list,
    which downstream would read as 'no trades' rather than 'no data'."""
    url = YAHOO_CHART.format(t=urllib.parse.quote(ticker.upper()))
    for attempt, backoff in enumerate((8.0, 24.0, 60.0, None)):
        try:
            req = urllib.request.Request(
                url, headers={"User-Agent": BROWSER_UA, "Accept": "application/json"})
            with urllib.request.urlopen(req, timeout=20) as r:
                data = json.loads(r.read())
            break
        except urllib.error.HTTPError as e:
            if e.code == 429 and backoff is not None:
                import random
                time.sleep(backoff * (0.8 + 0.4 * random.random()))
                continue
            return None
        except (urllib.error.URLError, TimeoutError, OSError, ValueError):
            return None
    else:
        return None

    results = ((data or {}).get("chart") or {}).get("result") or []
    if not results:
        return None
    quote = ((results[0].get("indicators") or {}).get("quote") or [{}])[0]
    vols = quote.get("volume")
    return vols if isinstance(vols, list) and vols else None


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=600,
                    help="max candidates to pull volume history for")
    ap.add_argument("--dry-run", action="store_true")
    a = ap.parse_args()

    fin = SE.fetch_financials()
    if not fin:
        log.error("EDGAR fundamentals fetch returned nothing — refusing to write.")
        return 1
    tickers = SE.get_tickers()
    exchanges = SE.get_exchanges()
    extra = _extra_frames()
    log.info("EDGAR: %d companies, %d tickers, %d with extra frames",
             len(fin), len(tickers), len(extra))

    # ── cheap narrowing, before we spend a single market-data call ──
    candidates = []
    for cik, f in fin.items():
        meta = tickers.get(cik)
        if not meta:
            continue
        ta = f.get("total_assets")
        if not ta or ta > MAX_ASSETS_FOR_CANDIDACY:
            continue
        name = meta.get("name", "")
        if SE._excluded_keyword(name) or SE._is_warrant(meta.get("ticker", "")):
            continue
        candidates.append({
            "cik": cik, "ticker": meta["ticker"], "name": name,
            "exchange": (exchanges.get(cik) or {}).get("exchange", ""),
            **{k: f.get(k) for k in ("cash", "total_assets", "total_liabilities",
                                     "stockholders_equity", "short_term_debt",
                                     "long_term_debt", "shares", "revenue",
                                     "net_income", "div_per_share")},
            **(extra.get(cik) or {}),
        })
    log.info("Candidates after asset filter: %d", len(candidates))
    if not candidates:
        log.error("No candidates — refusing to write.")
        return 1

    # ── price, then the REAL size cut ──
    prices = fetch_prices_bulk([c["ticker"] for c in candidates])
    log.info("Prices: %d of %d", len(prices), len(candidates))
    sized = []
    for c in candidates:
        px = prices.get(c["ticker"])
        sh = c.get("shares")
        if not px or not sh or sh <= 0:
            continue
        mcap = px * sh
        if not (MIN_MARKET_CAP <= mcap <= MAX_MARKET_CAP):
            continue
        c["price"] = px
        c["market_cap"] = mcap
        sized.append(c)
    # Smallest first: the cohort the research is about.
    sized.sort(key=lambda r: r["market_cap"])
    if a.limit and len(sized) > a.limit:
        log.info("Capping volume fetches at %d of %d candidates (smallest first).",
                 a.limit, len(sized))
        sized = sized[:a.limit]
    log.info("In size band with a price: %d", len(sized))

    # ── the expensive call, on survivors only ──
    def one(row: dict) -> dict:
        vols = fetch_volume_year(row["ticker"])
        row["_volumes_ok"] = vols is not None
        t = LQ.annual_turnover(vols or [], row.get("shares"))
        row["turnover"] = t["turnover"] if t else None
        row["liq"] = t
        row["trade"] = LQ.tradeability(t, row.get("price"))
        return row

    done = []
    with ThreadPoolExecutor(max_workers=4) as ex:
        for fut in as_completed([ex.submit(one, r) for r in sized]):
            try:
                done.append(fut.result())
            except Exception as e:                          # noqa: BLE001
                log.warning("volume fetch failed: %s", e)
    got = sum(1 for r in done if r["_volumes_ok"])
    log.info("Volume history: %d of %d", got, len(done))

    # ── classify + score ──
    classified = LQ.classify(done, within_size=True)
    rows = []
    for r in classified:
        debt = (r.get("short_term_debt") or 0) + (r.get("long_term_debt") or 0)
        shares = r.get("shares") or 0
        eps = (r["net_income"] / shares) if r.get("net_income") and shares else None
        bvps = (r["stockholders_equity"] / shares) if r.get("stockholders_equity") and shares else None
        qv = QV.evaluate({
            "price": r.get("price"), "market_cap": r.get("market_cap"),
            "cash": r.get("cash"), "total_debt": debt,
            "equity": r.get("stockholders_equity"),
            "capex": r.get("capex"), "ocf": r.get("ocf"),
            "operating_income": r.get("operating_income"), "revenue": r.get("revenue"),
            "dividends_per_share": r.get("div_per_share"),
            "eps": eps, "book_value_per_share": bvps,
        })
        rows.append({
            "ticker": r["ticker"], "name": r["name"], "cik": r["cik"],
            "exchange": r["exchange"],
            "price": round(r["price"], 2), "market_cap": int(r["market_cap"]),
            "size_bucket": r.get("size_bucket"),
            "turnover": r.get("turnover"),
            "liq_bucket": r.get("liq_bucket"),
            "liq_cuts": r.get("liq_cuts"),
            "liq_cuts_fallback": r.get("liq_cuts_fallback"),
            "liq_cohort_n": r.get("liq_cohort_n"),
            "zero_day_pct": (r["liq"] or {}).get("zero_day_pct"),
            "median_daily_usd": (r["trade"] or {}).get("median_daily_usd"),
            "days_to_build": (r["trade"] or {}).get("days_to_build"),
            "impractical": (r["trade"] or {}).get("impractical"),
            "metrics": qv["metrics"], "verdicts": qv["verdicts"],
            "passed": qv["passed"], "known": qv["known"],
            "unknown": qv["unknown"], "why": QV.summarize(qv),
        })

    rows.sort(key=lambda r: (-(r["passed"]), r.get("turnover") if r.get("turnover") is not None else 9e9))
    cov = LQ.coverage(classified)
    log.info("Rows: %d · liquidity measured on %d (%d%%)",
             len(rows), cov["measured"], cov["pct"])

    if len(rows) < MIN_ROWS_TO_WRITE:
        log.error("REFUSING TO WRITE: only %d rows (min %d). A short board would "
                  "read as a verdict on the market rather than a broken feed.",
                  len(rows), MIN_ROWS_TO_WRITE)
        return 1

    blob = {
        "_meta": {
            "as_of": date.today().isoformat(),
            "universe_candidates": len(candidates),
            "in_size_band": len(sized),
            "volume_coverage_pct": cov["pct"],
            "limits": QV.DEFAULTS,
            "note": ("Turnover is trailing-12m share volume / shares OUTSTANDING "
                     "(not float), matching Ibbotson et al. so the quartiles mean "
                     "what the research says they mean. Quartiles are cut WITHIN "
                     "each size band. Returns in that research are gross of "
                     "trading costs, which for this cohort are substantial."),
        },
        "rows": rows,
    }
    if a.dry_run:
        log.info("dry run — not writing (%d rows)", len(rows))
        return 0
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(blob, indent=1))
    log.info("Wrote %s (%d rows)", OUT, len(rows))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

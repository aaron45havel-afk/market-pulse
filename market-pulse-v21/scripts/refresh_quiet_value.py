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
import random
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

import liquidity as LQ                                    # noqa: E402
import pricefeed as PF                                    # noqa: E402
import quality_value as QV                                # noqa: E402
import sec_edgar as SE                                    # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
log = logging.getLogger("quiet-value")

OUT = ROOT / "data" / "quiet_value.json"

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


# FLOWS ARE A YEAR, NOT A QUARTER. The first version read earnings, revenue,
# dividends and cash flows from the net-net screener's single-quarter
# frames: a P/E on one quarter's earnings is four times too high, a
# dividend yield on one quarter's dividend four times too low, and the
# cash-flow frames for a discrete quarter are nearly empty — companies
# report cash flows year-to-date, so CY2026Q2 carried capex for 198 filers
# and the capex test could not be measured for anyone. The last complete
# calendar year's frames carry them all. Revenue is merged across the tags
# companies actually use (Revenues alone covered 1,655 filers).
ANNUAL_INPUTS = {
    "revenue": (("Revenues", "RevenueFromContractWithCustomerExcludingAssessedTax",
                 "RevenueFromContractWithCustomerIncludingAssessedTax", "SalesRevenueNet"), "USD"),
    "net_income": (("NetIncomeLoss",), "USD"),
    "operating_income": (("OperatingIncomeLoss",), "USD"),
    "ocf": (("NetCashProvidedByUsedInOperatingActivities",), "USD"),
    "capex": (("PaymentsToAcquirePropertyPlantAndEquipment",), "USD"),
    "div_per_share": (("CommonStockDividendsPerShareDeclared", "CommonStockDividendsPerShareCashPaid"),
                      "USD-per-shares"),
    # Many small payers file only the cash-flow line, the dollars paid, and
    # no per-share figure — and the per-share tags alone left the dividend
    # test unknown on 1,198 of 1,417 rows. Used only where no per-share
    # figure was filed: the year's payout over today's share count.
    "div_paid": (("PaymentsOfDividendsCommonStock", "PaymentsOfDividends"), "USD"),
}


def dividend_per_share(per_share, paid, shares) -> float | None:
    """The filed per-share dividend; failing that, the year's dividends paid
    over the share count. A filed zero is a measured zero, not a gap."""
    if per_share is not None:
        return per_share
    if paid is None or not shares or shares <= 0:
        return None
    return abs(paid) / shares


def annual_period(today: date | None = None) -> str:
    """The last complete calendar year's frame: CY2025 in October 2026."""
    return f"CY{(today or date.today()).year - 1}"


def merge_annual(frames: dict) -> dict:
    """{name: [{cik: {"val": ...}} per concept, in preference order]} →
    {cik: {name: value}}. The first concept with a value for a company wins;
    later ones only fill companies the earlier ones lack."""
    out: dict[str, dict] = {}
    for name, per_concept in frames.items():
        for frame in per_concept:
            for cik, entry in (frame or {}).items():
                val = (entry or {}).get("val")
                row = out.setdefault(cik, {})
                if row.get(name) is None and val is not None:
                    row[name] = val
    return out


def _annual_frames(period: str) -> dict[str, dict]:
    jobs = [(name, i, concept, unit) for name, (concepts, unit) in ANNUAL_INPUTS.items()
            for i, concept in enumerate(concepts)]
    got: dict = {}
    with ThreadPoolExecutor(max_workers=4) as ex:
        futs = {ex.submit(SE._frame, c, period, u): (name, i) for name, i, c, u in jobs}
        for fut in as_completed(futs):
            name, i = futs[fut]
            try:
                got[(name, i)] = fut.result() or {}
            except Exception as e:                          # noqa: BLE001
                log.warning("frame %s[%d] failed: %s", name, i, e)
                got[(name, i)] = {}
    return merge_annual({name: [got.get((name, i), {}) for i in range(len(concepts))]
                         for name, (concepts, _unit) in ANNUAL_INPUTS.items()})


# INDUSTRY, BY SEC SIC CODE. The candidate filter drops names containing
# "Bancorp", "Insurance", "REIT" and the like, and that is all it can do:
# ENB Financial, LCNB Corp, BayCom Corp and four more banks walked past it
# and made up seven of the 13 names on the default view (October 2026),
# passing "net cash" and "low debt" because deposits are not filed as debt.
# The code is asked for only after the size cut, of names that can reach
# the board. Without it a bank is scored as an operating company again, so
# a run that cannot classify most of the board refuses to write.
SUBMISSIONS = "https://data.sec.gov/submissions/CIK{cik}.json"
MIN_SIC_COVERAGE = 0.90
SEC_INTERVAL = 0.125                  # 8 requests a second; SEC's ceiling is 10
SIC_CHECK_AFTER = 50                  # all of the first 50 unanswered: SEC is refusing us


def sic_coverage_ok(answered: int, asked: int) -> bool:
    return asked == 0 or answered / asked >= MIN_SIC_COVERAGE


def fetch_sic(ciks: list[str], get=None, interval: float = SEC_INTERVAL) -> dict[str, str | None]:
    """{cik: SIC code or None} for every CIK SEC answered for. A CIK SEC did
    not answer for is absent, which is how the caller counts coverage."""
    get = get or SE._get
    lock = threading.Lock()
    state = {"next": 0.0, "asked": 0, "answered": 0}

    def one(cik):
        with lock:
            # The stop is decided by the workers, before each request: a
            # queue of futures cancelled from the outside has already run.
            if state["asked"] >= SIC_CHECK_AFTER and not state["answered"]:
                return cik, None
            state["asked"] += 1
            # Pace the whole pool, not each worker.
            wait = state["next"] - time.monotonic()
            state["next"] = max(state["next"], time.monotonic()) + interval
        if wait > 0:
            time.sleep(wait)
        data = get(SUBMISSIONS.format(cik=str(cik).zfill(10)))
        if data is not None:
            with lock:
                state["answered"] += 1
        return cik, data

    out: dict[str, str | None] = {}
    with ThreadPoolExecutor(max_workers=4) as ex:
        for cik, data in ex.map(one, ciks):
            if data is not None:
                out[str(cik)] = str(data["sic"]) if data.get("sic") else None
    if state["asked"] < len(ciks):
        log.error("SIC: none of the first %d requests answered — stopped asking.", state["asked"])
    return out


class FeedDown(Exception):
    """Raised when the chosen source stops answering mid-run."""


# Failure counters, so an hour of silence can never happen again. The
# 2026-08-09 run fetched 100 tickers, got data for none, and logged
# nothing at all — every rejection was swallowed by an except that
# returned None.
STATUS: dict[str, int] = {}


# Once this many requests fail back to back, stop backing off and start
# failing fast. Backing off is the right response to a transient rate
# limit and the wrong one to a source that is refusing everything: the
# 2026-08-09 run spent 92 seconds per ticker sleeping between rejections
# that were never going to stop coming. Politeness that never notices is
# not politeness.
DEGRADE_AFTER = 12
_CONSECUTIVE = 0
_STATUS_LOCK = threading.Lock()


def _bump(key: str, ok: bool) -> bool:
    """Record an outcome. Returns True while the feed still looks healthy."""
    global _CONSECUTIVE
    with _STATUS_LOCK:
        STATUS[key] = STATUS.get(key, 0) + 1
        _CONSECUTIVE = 0 if ok else _CONSECUTIVE + 1
        return _CONSECUTIVE < DEGRADE_AFTER


def reset_feed_state() -> None:
    global _CONSECUTIVE
    with _STATUS_LOCK:
        _CONSECUTIVE = 0
        STATUS.clear()


def fetch_one(source: dict, ticker: str, retry: bool = True) -> dict | None:
    """One call per name: price and a year of volume together.

    The ladder is short, and it is skipped entirely once DEGRADE_AFTER
    consecutive failures say the source is not rate-limiting us but
    refusing us. The old ladder slept 8s, 24s then 60s before giving up:
    400 tickers x 92s across 3 workers is three and a half hours against a
    sixty-minute budget, which is how an hour got spent writing nothing.
    """
    today = date.today()
    url = source["url"].format(s=urllib.parse.quote(source["symbol"](ticker), safe=".-"),
                               fd=today - timedelta(days=370), td=today)
    raw = None
    for backoff in (3.0, 9.0, None):
        try:
            # Each source's own headers: Yahoo answers a plain named agent
            # and refuses a browser string (pricefeed.SOURCES).
            req = urllib.request.Request(url, headers=dict(source["headers"]))
            with urllib.request.urlopen(req, timeout=20) as r:
                body = r.read()
            raw = json.loads(body) if source["json"] else body.decode("utf-8", "replace")
            _bump("ok", True)
            break
        except urllib.error.HTTPError as e:
            healthy = _bump(f"http_{e.code}", False)
            if retry and healthy and e.code in (429, 503) and backoff is not None:
                time.sleep(backoff * (0.8 + 0.4 * random.random()))
                continue
            return None
        except (urllib.error.URLError, TimeoutError, OSError) as e:
            _bump(type(e).__name__, False)
            return None
        except ValueError:                  # malformed JSON
            _bump("bad_payload", False)
            return None
    else:
        return None
    parsed = source["parse"](raw)
    if parsed is None:
        # A 200 that parses to nothing is a failure too — Stooq answers an
        # exhausted quota with plain text and HTTP 200. Counting it as a
        # success would keep the circuit breaker open through a total
        # outage.
        _bump("http_200_unparseable", False)
        _sample(source["name"], raw)
    return parsed


# What a 200 that we could not read actually contained. The first run with
# the probe in place reported `{'http_200_unparseable': 3, 'ok': 3}` for
# Stooq — it answers GitHub's runners, but not with what the parser
# expects — and there was no way to tell an exhausted quota from a changed
# format from a wrong URL. Guessing at that is how the EDGAR index parser
# shipped broken twice. Keep the first bytes instead.
SAMPLES: dict[str, str] = {}
SAMPLE_CHARS = 300


def _sample(name: str, raw) -> None:
    with _STATUS_LOCK:
        if name in SAMPLES:
            return
        text = raw if isinstance(raw, str) else json.dumps(raw)[:SAMPLE_CHARS]
        SAMPLES[name] = " ".join((text or "")[:SAMPLE_CHARS].split())


def probe(source: dict) -> tuple[bool, str]:
    """Does this source answer AT ALL? Settled before the budget is spent.

    NO RETRIES HERE. A probe asks a question; a retry ladder only delays
    the answer. Three known-liquid tickers, one attempt each — a dead
    source is established in about a second rather than the hour it cost
    last time.
    """
    hits, why = [], []
    for t in PF.PROBE_TICKERS:
        try:
            got = fetch_one(source, t, retry=False)
        except Exception as e:                              # noqa: BLE001
            return False, f"{type(e).__name__}: {e}"
        if got and got.get("volumes"):
            hits.append(t)
        else:
            why.append(t)
        time.sleep(0.3)
    if hits:
        return True, f"answered for {', '.join(hits)}"
    return False, f"no data for any of {', '.join(why)}"


def choose_source(preferred: str = "") -> dict | None:
    """First source that proves it works. Order is a hint, not a decision."""
    order = list(PF.SOURCES)
    if preferred:
        want = PF.source_by_name(preferred)
        if not want:
            log.error("Unknown --source %r; known: %s", preferred,
                      ", ".join(s["name"] for s in PF.SOURCES))
            return None
        order = [want]
    for src in order:
        reset_feed_state()          # each source gets a clean verdict
        ok, why = probe(src)
        log.info("  probe %-6s → %s", src["name"], dict(sorted(STATUS.items())))
        if ok:
            log.info("Price source: %s — %s", src["name"], why)
            reset_feed_state()
            return src
        log.warning("Source %s is unusable: %s (%s)", src["name"], why, src["note"])
        if src["name"] in SAMPLES:
            # It answered; we could not read it. Print what arrived, because
            # "unparseable" alone cannot distinguish an exhausted quota from
            # a changed format from a wrong URL.
            log.warning("    %s replied with HTTP 200 and this body: %r",
                        src["name"], SAMPLES[src["name"]])
    return None


def main() -> int:
    ap = argparse.ArgumentParser()
    # Every candidate: Yahoo answers ~10 names a second on three workers, so
    # the whole ~2,750 list is about five minutes. The old cap of 400 was
    # sized for a feed that was refusing us, and the 400 SMALLEST by assets
    # are mostly pre-revenue shells — the profitable small companies this
    # screen exists to find were never priced.
    ap.add_argument("--limit", type=int, default=4000,
                    help="max candidates to pull market data for (one call each)")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--source", default="", help="force a price source (yahoo, nasdaq, stooq)")
    a = ap.parse_args()

    # PROVE THE PRICE SOURCE FIRST. Everything after this — the EDGAR
    # fundamentals pull, the frames, the narrowing — is wasted if no source
    # will answer, and the EDGAR half is the slow part of the setup. Ten
    # seconds here replaces the sixty minutes the 2026-08-09 run spent
    # discovering that Yahoo blocks GitHub's IPs.
    source = choose_source(a.source)
    if source is None:
        log.error("No price source is reachable from this runner. Tried: %s. "
                  "The screen needs a year of daily volume per name and the SEC "
                  "does not publish it, so there is nothing to fall back on — "
                  "refusing to write a board built on no market data.",
                  ", ".join(s["name"] for s in PF.SOURCES))
        return 1

    fin = SE.fetch_financials()
    if not fin:
        log.error("EDGAR fundamentals fetch returned nothing — refusing to write.")
        return 1
    tickers = SE.get_tickers()
    exchanges = SE.get_exchanges()
    period = annual_period()
    annual = _annual_frames(period)
    log.info("EDGAR: %d companies, %d tickers; %s annual flows for %d (revenue %d, net income %d, "
             "capex %d, operating cash flow %d)", len(fin), len(tickers), period, len(annual),
             *(sum(1 for v in annual.values() if v.get(k) is not None)
               for k in ("revenue", "net_income", "capex", "ocf")))

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
            # Balance sheet: the latest quarter-end (an instant is an instant).
            **{k: f.get(k) for k in ("cash", "total_assets", "total_liabilities",
                                     "stockholders_equity", "short_term_debt",
                                     "long_term_debt", "shares")},
            # Flows: the last complete year (ANNUAL_INPUTS).
            **{k: (annual.get(cik) or {}).get(k) for k in ANNUAL_INPUTS},
        })
    log.info("Candidates after asset filter: %d", len(candidates))
    if not candidates:
        log.error("No candidates — refusing to write.")
        return 1

    # ── cap the work BEFORE spending anything ──
    # Total assets is a crude size proxy, but it is free and it is the only
    # ordering available before we have a price. Smallest first, because
    # that is the cohort the research is about — and if the budget runs
    # out, the large end is the right end to lose.
    candidates.sort(key=lambda c: c.get("total_assets") or 0)
    if a.limit and len(candidates) > a.limit:
        log.info("Capping market-data fetches at %d of %d candidates "
                 "(smallest by assets first).", a.limit, len(candidates))
        candidates = candidates[:a.limit]

    # ── one call per name: price and a year of volume together ──
    def one(row: dict) -> dict:
        chart = fetch_one(source, row["ticker"])
        row["_chart_ok"] = chart is not None
        px = (chart or {}).get("price")
        sh = row.get("shares")
        row["price"] = px
        row["market_cap"] = (px * sh) if (px and sh and sh > 0) else None
        t = LQ.annual_turnover((chart or {}).get("volumes") or [], sh)
        row["turnover"] = t["turnover"] if t else None
        row["liq"] = t
        row["trade"] = LQ.tradeability(t, px)
        return row

    # THE CIRCUIT BREAKER. A probed source can still start refusing partway
    # through — a quota trips, an IP gets flagged. Zero successes in the
    # first CHECK_AFTER names is proof, not bad luck, and grinding through
    # the remaining 350 to confirm it is how sixty minutes got spent
    # establishing something the first fifty already showed.
    CHECK_AFTER, MIN_HIT_RATE = 40, 0.05
    fetched, done, aborted = 0, [], False
    with ThreadPoolExecutor(max_workers=3) as ex:
        futures = [ex.submit(one, r) for r in candidates]
        for fut in as_completed(futures):
            try:
                done.append(fut.result())
            except Exception as e:                          # noqa: BLE001
                log.warning("chart fetch failed: %s", e)
            fetched += 1
            ok = sum(1 for r in done if r.get("_chart_ok"))
            if fetched % 50 == 0:
                log.info("  %d/%d fetched, %d with data", fetched, len(candidates), ok)
            if fetched >= CHECK_AFTER and ok / fetched < MIN_HIT_RATE:
                log.error("STOPPING: %d of %d fetches returned data (%.0f%%). The "
                          "source went dark mid-run — continuing would spend the "
                          "whole budget proving it. Status counts: %s",
                          ok, fetched, 100.0 * ok / fetched, dict(sorted(STATUS.items())))
                for f in futures:
                    f.cancel()
                aborted = True
                break
    got = sum(1 for r in done if r["_chart_ok"])
    log.info("Market data via %s: %d of %d attempted. Status counts: %s",
             source["name"], got, len(done), dict(sorted(STATUS.items())))
    if aborted or not got:
        log.error("Refusing to write: the price feed failed. A short board would "
                  "read as a verdict on the market instead of a broken source.")
        return 1

    # ── the REAL size cut, now that we have prices ──
    sized = [r for r in done
             if r.get("market_cap") and MIN_MARKET_CAP <= r["market_cap"] <= MAX_MARKET_CAP]
    log.info("In the size band: %d of %d fetched", len(sized), len(done))
    done = sized

    # ── industry, for the names that can reach the board ──
    sics = fetch_sic([r["cik"] for r in done])
    if not sic_coverage_ok(len(sics), len(done)):
        log.error("REFUSING TO WRITE: SEC gave an industry code for %d of %d names (min %.0f%%). "
                  "Without it a bank is scored as an operating company again.",
                  len(sics), len(done), MIN_SIC_COVERAGE * 100)
        return 1
    industries = {}
    for r in done:
        r["sic"] = sics.get(str(r["cik"]))
        r["industry"] = QV.industry(r["sic"])
        industries[r["industry"] or "other"] = industries.get(r["industry"] or "other", 0) + 1
    log.info("SIC answered for %d of %d; industries %s", len(sics), len(done), industries)

    # ── classify + score ──
    classified = LQ.classify(done, within_size=True)
    rows = []
    dps_from_paid = 0
    for r in classified:
        debt = (r.get("short_term_debt") or 0) + (r.get("long_term_debt") or 0)
        shares = r.get("shares") or 0
        eps = (r["net_income"] / shares) if r.get("net_income") and shares else None
        bvps = (r["stockholders_equity"] / shares) if r.get("stockholders_equity") and shares else None
        dps = dividend_per_share(r.get("div_per_share"), r.get("div_paid"), shares)
        dps_from_paid += r.get("div_per_share") is None and dps is not None
        qv = QV.evaluate({
            "price": r.get("price"), "market_cap": r.get("market_cap"),
            "cash": r.get("cash"), "total_debt": debt,
            "equity": r.get("stockholders_equity"),
            "capex": r.get("capex"), "ocf": r.get("ocf"),
            "operating_income": r.get("operating_income"), "revenue": r.get("revenue"),
            "dividends_per_share": dps,
            "eps": eps, "book_value_per_share": bvps,
        }, industry=r["industry"])
        rows.append({
            "ticker": r["ticker"], "name": r["name"], "cik": r["cik"],
            "exchange": r["exchange"],
            "sic": r["sic"], "industry": r["industry"],
            "not_applicable": qv["not_applicable"],
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
    log.info("Rows: %d · liquidity measured on %d (%d%%) · dividend measured on %d (%d from dollars paid)",
             len(rows), cov["measured"], cov["pct"],
             sum(1 for r in rows if r["verdicts"]["dividend"] is not None), dps_from_paid)

    if len(rows) < MIN_ROWS_TO_WRITE:
        log.error("REFUSING TO WRITE: only %d rows (min %d). A short board would "
                  "read as a verdict on the market rather than a broken feed.",
                  len(rows), MIN_ROWS_TO_WRITE)
        return 1

    blob = {
        "_meta": {
            "as_of": date.today().isoformat(),
            "universe_candidates": len(candidates),
            "in_size_band": len(done),
            "volume_coverage_pct": cov["pct"],
            "sic_coverage_pct": round(len(sics) / len(done) * 100) if done else 0,
            "industries": industries,
            "price_source": source["name"],
            "fetch_status": dict(sorted(STATUS.items())),
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

"""Monthly data build for /compounders — the 14%/yr long-term screen.

Three stages, all free sources, designed for the GitHub Actions runner
(the dev sandbox can't reach EDGAR/Yahoo — test with --limit there and
expect network failures; the real run happens in CI):

  A. UNIVERSE — self-discovering, no hand-curated list to maintain.
     SEC XBRL "frames" API returns one concept for EVERY filer in a
     couple of calls. We take annual revenue frames (us-gaap Revenues +
     RevenueFromContractWithCustomer… + ifrs-full Revenue, union of the
     last two calendar years) and keep every company ≥ $1B revenue —
     roughly the top ~2000 operating companies incl. US-listed ADRs
     (which file 10-K/20-F and appear in frames). Exchange-listed only
     (company_tickers_exchange.json), financials excluded by SIC.

  B. FUNDAMENTALS — per CIK: companyfacts (10y of annual XBRL) +
     submissions (SIC code, country). Tag maps cover us-gaap AND
     ifrs-full so 20-F ADRs work. Extracts revenue, net income, gross
     profit, operating income, OCF, capex, diluted shares, debt, cash,
     equity → computes CAGRs, consistency counts, margins + trend,
     ROIC series, FCF conversion, net-debt/EBIT, buyback rate.

  C. MARKET — Yahoo chart per ticker (7y monthly + dividends): current
     price, TTM dividend yield, dividend CAGR, FCF-multiple history
     (year-average price ÷ FCF/share) for the valuation-drift term.

Output: data/compounders.json — compact per-ticker METRICS only (a few
KB per name). All scoring/thresholds live in compounders.py so tuning
the screen never requires a refetch.

SEC fair-access: ≤10 req/s allowed; we run ~3/s with a descriptive UA.
Full run ≈ 2000 CIKs × (companyfacts + submissions) + ~1800 Yahoo calls
≈ 45-70 min — fine for a monthly job (limit 6h).
"""
from __future__ import annotations

import argparse
import json
import statistics
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

SEC_UA = "market-pulse-research admin@focusedops.io"
HEADERS_SEC = {"User-Agent": SEC_UA, "Accept-Encoding": "gzip, deflate"}
HEADERS_YAHOO = {"User-Agent": "Mozilla/5.0 (market-pulse-refresh/1.0)",
                 "Accept": "application/json"}

SEC_SLEEP = 0.34          # ~3 req/s, well under SEC's 10/s policy
YAHOO_SLEEP = 0.3
MIN_REVENUE = 1_000_000_000     # $1B floor → ≈ top 2000 operating cos
MIN_YEARS = 7                    # need ≥7 fiscal years to score
MAX_UNIVERSE = 2600              # hard cap on CIKs processed

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
    ("ifrs-full", "Revenue"),
]


def discover_universe(years: list[int]) -> dict[int, float]:
    """{cik: best annual revenue} for every filer ≥ MIN_REVENUE, via
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
    return {c: v for c, v in best.items() if v >= MIN_REVENUE}


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
    country = (addr.get("stateOrCountryDescription") or "").strip()
    # US states come through as the state name; normalize to United States.
    if addr.get("stateOrCountry", "").isupper() and len(addr.get("stateOrCountry", "")) == 2 \
            and not country.lower().startswith(("canada",)):
        # two-letter codes that are US states → US; foreign codes come as
        # longer descriptions in stateOrCountryDescription anyway.
        pass
    try:
        sic = int(s.get("sic") or 0) or None
    except (TypeError, ValueError):
        sic = None
    return {"sic": sic, "sic_desc": s.get("sicDescription") or "",
            "country_desc": country or "United States"}


# ── Stage B: fundamentals ────────────────────────────────────────────

def _annual_series(facts: dict, slots: list[tuple[str, str]]) -> dict[int, float]:
    """{fiscal_year: value} from companyfacts for the first tag that
    yields a usable annual series. Annual = FY frame from an annual
    form. Dedupes amended filings by keeping the LAST value per fy."""
    for taxonomy, tag in slots:
        node = (facts.get(taxonomy) or {}).get(tag)
        if not node:
            continue
        units = node.get("units") or {}
        # USD for money, shares for share counts — take the first unit.
        series: dict[int, float] = {}
        for unit_vals in units.values():
            for v in unit_vals:
                if v.get("form") not in ANNUAL_FORMS:
                    continue
                if v.get("fp") not in ("FY", None):
                    continue
                fy, val = v.get("fy"), v.get("val")
                if fy is None or not isinstance(val, (int, float)):
                    continue
                # Durational facts must span ~a year; instants have no
                # start. Guard quarter-length values sneaking in as FY.
                start, end = v.get("start"), v.get("end")
                if start and end and (int(end[:4]) - int(start[:4])) == 0 \
                        and end[5:7] != "12" and (int(end[5:7]) - int(start[5:7])) < 9:
                    continue
                series[int(fy)] = float(val)
            if series:
                break
        if len(series) >= 3:
            return series
    return {}


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


def compute_metrics(facts: dict) -> dict | None:
    s = {k: _annual_series(facts, slots) for k, slots in TAGS.items()}
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
    args = ap.parse_args()

    this_year = datetime.now(timezone.utc).year
    print("[compounders] Stage A: universe discovery via EDGAR frames…")
    revenue_by_cik = discover_universe([this_year - 2, this_year - 1])
    print(f"[compounders] {len(revenue_by_cik)} filers ≥ ${MIN_REVENUE/1e9:.0f}B revenue")
    tickers = ticker_map()
    universe = [(cik, rev) for cik, rev in revenue_by_cik.items() if cik in tickers]
    universe.sort(key=lambda x: -x[1])
    universe = universe[:MAX_UNIVERSE]
    if args.limit:
        universe = universe[:args.limit]
    print(f"[compounders] {len(universe)} exchange-listed after ticker join")

    out: dict[str, dict] = {}
    skipped = {"financial": 0, "no_facts": 0, "thin": 0, "market": 0}
    t0 = time.time()
    for i, (cik, rev) in enumerate(universe, 1):
        info = tickers[cik]
        profile = fetch_profile(cik)
        time.sleep(SEC_SLEEP)
        if _is_financial_sic(profile.get("sic")):
            skipped["financial"] += 1
            continue
        metrics = fetch_fundamentals(cik)
        time.sleep(SEC_SLEEP)
        if metrics is None:
            skipped["no_facts"] += 1
            continue
        market = fetch_market(info["ticker"], metrics.get("fcf_ps") or {})
        time.sleep(YAHOO_SLEEP)
        if market is None:
            skipped["market"] += 1
            market = {}
        row = {**metrics, **market,
               "name": info["name"], "cik": cik, "exchange": info["exchange"],
               "sic": profile.get("sic"), "industry": profile.get("sic_desc"),
               "country": profile.get("country_desc") or "United States"}
        row.pop("fcf_ps", None)   # working data — not needed in output
        out[info["ticker"]] = row
        if i % 50 == 0:
            rate = i / (time.time() - t0)
            eta = (len(universe) - i) / rate / 60
            print(f"[compounders] {i}/{len(universe)} · kept {len(out)} · ~{eta:.0f} min left")

    print(f"[compounders] Done: kept {len(out)}, skipped {skipped}")
    if len(out) < 200 and not args.limit:
        print("[compounders] Far fewer names than expected — refusing to "
              "overwrite a good dataset with a bad run.")
        return 2

    payload = {
        "as_of": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
        "universe_input": len(universe),
        "count": len(out),
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

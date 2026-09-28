"""Monthly refresh of dividend-aristocrat valuation data for /aristocrats.

For every ticker in aristocrats.UNIVERSE, pulls from Yahoo Finance's
chart API (no key needed):
  • 6 years of monthly closes + dividend events  → current TTM yield,
    the stock's own 5-yr median trailing yield (the value anchor), and
    the 5-yr dividend CAGR (the Chowder growth term)
  • 1 year of daily closes                       → 52-week high/low

Writes data/aristocrats.json:

    {
      "as_of": "2026-07-14",
      "fetched_at": "2026-07-14T13:30Z",
      "count": 88,
      "tickers": {
        "PEP": {"y": 4.11, "median_y5": 2.86, "dg5": 6.0, "price": 138.4,
                 "pct_off_52wk_high": 21.3, "pct_above_52wk_low": 2.1},
        ...
      }
    }

aristocrats._merged_universe() overlays these per-field at request
time; payout/debt gates keep their hand-seeded values (Yahoo's chart
API doesn't carry them, and they move slowly).

Failure model: per-ticker failures are logged and skipped — partial
data is still useful and still gets committed. The run only exits
non-zero when < 30% of the universe succeeded (Yahoo layout change or
a block), in which case the workflow doesn't commit and the app keeps
serving the previous overlay.

Cadence: 10th of each month (see refresh-aristocrats.yml) + manual
workflow_dispatch for the first population run.
"""
from __future__ import annotations

import json
import statistics
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
sys.path.insert(0, str(Path(__file__).resolve().parent))
from aristocrats import UNIVERSE  # noqa: E402
import refresh_compounders as RC  # noqa: E402  — SEC readers shared with the compounders build

CHART_URL = ("https://query1.finance.yahoo.com/v8/finance/chart/"
             "{t}?range={rng}&interval={iv}{events}")
HEADERS = {"User-Agent": "Mozilla/5.0 (market-pulse-refresh/1.0)",
           "Accept": "application/json"}
SLEEP_BETWEEN = 0.35          # polite pacing: ~2 req/ticker, ~90 tickers
MIN_SUCCESS_FRACTION = 0.30


def _fetch(url: str, timeout: int = 30) -> dict:
    req = urllib.request.Request(url, headers=HEADERS)
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read())


def _chart_result(payload: dict) -> dict | None:
    try:
        res = payload["chart"]["result"][0]
    except (KeyError, IndexError, TypeError):
        return None
    return res


def _ttm_dividends(divs: list[tuple[int, float]], at_ts: int) -> float:
    """Sum of dividends in the 365 days ending at at_ts."""
    year = 365 * 24 * 3600
    return sum(amt for ts, amt in divs if at_ts - year < ts <= at_ts)


def fetch_ticker(ticker: str) -> dict | None:
    # Yahoo uses '-' for share classes (BF-B) — universe already stores
    # tickers in Yahoo form.
    monthly = _chart_result(_fetch(CHART_URL.format(
        t=ticker, rng="7y", iv="1mo", events="&events=div")))
    if not monthly:
        return None
    ts = monthly.get("timestamp") or []
    closes = (monthly.get("indicators", {}).get("quote") or [{}])[0].get("close") or []
    div_events = ((monthly.get("events") or {}).get("dividends") or {})
    divs = sorted((int(d["date"]), float(d["amount"]))
                  for d in div_events.values()
                  if d.get("date") and d.get("amount"))
    if not ts or not closes or not divs:
        return None

    points = [(t, c) for t, c in zip(ts, closes) if c]
    if len(points) < 24:
        return None
    now_ts, price = points[-1]

    ttm_now = _ttm_dividends(divs, now_ts)
    if ttm_now <= 0 or price <= 0:
        return None
    current_yield = ttm_now / price * 100

    # 5-yr median trailing yield: TTM dividends ÷ close at each of the
    # last 60 monthly bars (needs a year of dividend lead-in, which the
    # 6y range provides).
    yields = []
    for t, c in points[-60:]:
        ttm = _ttm_dividends(divs, t)
        if ttm > 0 and c > 0:
            yields.append(ttm / c * 100)
    median_y5 = statistics.median(yields) if len(yields) >= 24 else None

    # 5-yr dividend CAGR: TTM now vs TTM ending ~60 months ago. If the
    # event series doesn't reach back far enough to fully cover that
    # trailing window (short history, sparse Yahoo data), anchor the
    # "then" window just inside the covered span and annualize over the
    # actual distance — an uncovered window silently overstates growth.
    year_s = 365 * 24 * 3600
    anchor = max(now_ts - 5 * year_s, divs[0][0] + int(1.05 * year_s))
    ttm_then = _ttm_dividends(divs, anchor)
    years = (now_ts - anchor) / year_s
    dg5 = (((ttm_now / ttm_then) ** (1 / years) - 1) * 100
           if ttm_then > 0 and years >= 2 else None)

    time.sleep(SLEEP_BETWEEN)
    daily = _chart_result(_fetch(CHART_URL.format(
        t=ticker, rng="1y", iv="1d", events="")))
    pct_off_high = pct_above_low = None
    if daily:
        dcloses = [c for c in ((daily.get("indicators", {}).get("quote") or [{}])[0].get("close") or []) if c]
        if dcloses:
            hi, lo = max(dcloses), min(dcloses)
            if hi > 0:
                pct_off_high = (1 - price / hi) * 100
            if lo > 0:
                pct_above_low = (price / lo - 1) * 100

    out = {
        "y":     round(current_yield, 2),
        "price": round(price, 2),
    }
    if median_y5 is not None:
        out["median_y5"] = round(median_y5, 2)
    if dg5 is not None:
        out["dg5"] = round(dg5, 2)
    if pct_off_high is not None:
        out["pct_off_52wk_high"] = round(pct_off_high, 1)
    if pct_above_low is not None:
        out["pct_above_52wk_low"] = round(pct_above_low, 1)
    return out


# ── Dividend safety from SEC filings ────────────────────────────────
#
# The page used to gate on hand-seeded payout and leverage — 22 and 16 of
# 92 names — and read every missing figure as safe: four of its five BUYs
# had neither. Both are now computed here from companyfacts. A figure that
# cannot be computed is left out, and the page shows it as not measured.

SEC_TICKERS_URL = "https://www.sec.gov/files/company_tickers.json"
FACTS_URL = "https://data.sec.gov/api/xbrl/companyfacts/CIK{cik:010d}.json"
G, I = "us-gaap", "ifrs-full"
SAFETY_TAGS = {
    "div_paid": [(G, "PaymentsOfDividendsCommonStock"), (G, "PaymentsOfDividends"),
                 (G, "PaymentsOfOrdinaryDividends"), (G, "DividendsCommonStockCash"),
                 (G, "DividendsCommonStock"),
                 (I, "DividendsPaidClassifiedAsFinancingActivities"), (I, "DividendsPaid"),
                 (I, "DividendsPaidToEquityHoldersOfParentClassifiedAsFinancingActivities")],
    # D&A WITH amortization included…
    "dda": [(G, "DepreciationDepletionAndAmortization"), (G, "DepreciationAndAmortization"),
            (G, "DepreciationAmortizationAndAccretionNet"),
            (I, "DepreciationAndAmortisationExpense"),
            (I, "AdjustmentsForDepreciationAndAmortisationExpense")],
    # …or depreciation alone, with intangible amortization tagged apart
    # (AbbVie: $0.76bn of depreciation, $7.4bn of amortization).
    "dep": [(G, "Depreciation"), (I, "DepreciationExpense")],
    "amort": [(G, "AmortizationOfIntangibleAssets"), (I, "AmortisationExpense")],
}
PAYOUT_YEARS = 3          # a one-off charge in one year must not decide it
EBITDA_YEARS = 3
BALANCE_SHEET_MAX_AGE_DAYS = 640   # ~21 months: older is not today's balance sheet
FFO_SECTORS = {"REIT", "Midstream"}


def ticker_ciks(payload: dict) -> dict[str, int]:
    """company_tickers.json -> {TICKER: cik}, every ticker listed."""
    out: dict[str, int] = {}
    for k in sorted(payload or {}, key=lambda x: int(x) if str(x).isdigit() else 0):
        e = payload[k] or {}
        if e.get("ticker") and e.get("cik_str"):
            out.setdefault(str(e["ticker"]).upper(), int(e["cik_str"]))
    return out


def _window(series: list[dict], last_fy: int, years: int) -> list[int]:
    """Fiscal years up to `last_fy` present in every series; the last year
    must be among them and at least two years in all."""
    fys = [y for y in range(last_fy - years + 1, last_fy + 1)
           if all(s.get(y) is not None for s in series)]
    return fys if last_fy in fys and len(fys) >= 2 else []


def payout_cover(div: dict, ni: dict, ocf: dict, capex: dict, last_fy: int,
                 ffo_da: dict | None = None) -> tuple[float | None, str | None, list[int]]:
    """(payout %, basis, fiscal years): dividends paid over the BETTER of
    two covers, each summed over PAYOUT_YEARS — earnings, and free cash
    flow. A one-off charge sinks one cover in one year (AbbVie's GAAP
    payout reads 282% on acquired R&D; on cash it is 58%), and a
    capex-funded utility has no free cash flow at all, so the better cover
    is the fair one. REITs and midstream add an FFO proxy, earnings plus
    D&A, because their GAAP earnings are after depreciation (Realty
    Income's GAAP payout reads 275%).

    Basis "negative": dividends were paid while every available cover was
    at or below zero over the window — a failure, not an unknown.
    """
    covers, tried = [], False
    w = _window([div, ni], last_fy, PAYOUT_YEARS)
    if w:
        tried = True
        n = sum(ni[y] for y in w)
        if n > 0:
            covers.append((sum(div[y] for y in w) / n * 100, "earnings", w))
        if ffo_da is not None:
            wf = _window([div, ni, ffo_da], last_fy, PAYOUT_YEARS)
            f = sum(ni[y] + ffo_da[y] for y in wf) if wf else 0
            if wf and f > 0:
                covers.append((sum(div[y] for y in wf) / f * 100, "ffo", wf))
    w = _window([div, ocf, capex], last_fy, PAYOUT_YEARS)
    if w:
        tried = True
        f = sum(ocf[y] - capex[y] for y in w)
        if f > 0:
            covers.append((sum(div[y] for y in w) / f * 100, "free cash flow", w))
    if covers:
        return min(covers, key=lambda c: c[0])
    return (None, "negative", []) if tried else (None, None, [])


def ebitda_by_year(ebit: dict, dda: dict, dep: dict, amort: dict) -> dict:
    """{fy: EBIT + D&A}, D&A being the combined tag where filed, else
    depreciation plus intangible amortization."""
    out = {}
    for y, e in ebit.items():
        if dda.get(y) is not None:
            out[y] = e + dda[y]
        elif dep.get(y) is not None:
            out[y] = e + dep[y] + (amort.get(y) or 0.0)
    return out


def net_debt_to_ebitda(debt: float | None, cash: float | None, ebitda: dict,
                       last_fy: int, notes: list | None = None,
                       insurer: bool = False) -> tuple[float | None, str]:
    """(ratio, basis). EBITDA is the HIGHER of the latest year and the
    median of the last EBITDA_YEARS, so one impairment year does not
    decide a gate (Air Products' 2025 EBITDA is a fraction of its normal).

    Bases: "n/a" (insurers — EBITDA is not how an underwriter's leverage is
    read), "net cash", "measured", "floor" (debt is one kind only, per
    balance_sheet: can prove a failure, never a pass), "no positive EBITDA"
    (net debt with nothing to carry it — a failure), "unknown".
    """
    if insurer:
        return None, "n/a"
    if debt is None:
        return None, "unknown"
    nd = debt - (cash or 0.0)
    kind_only = any("one kind of debt" in n for n in (notes or []))
    if nd <= 0 and cash is not None and not kind_only:
        return round(nd / ebitda[last_fy], 2) if ebitda.get(last_fy, 0) > 0 else None, "net cash"
    recent = [ebitda[y] for y in range(last_fy - EBITDA_YEARS + 1, last_fy + 1) if y in ebitda]
    if last_fy not in ebitda:
        return None, "unknown"
    base = max(ebitda[last_fy], statistics.median(recent))
    if base <= 0:
        return None, "no positive EBITDA"
    return round(nd / base, 2), ("floor" if kind_only else "measured")


def sec_safety(facts: dict, sector: str, insurer: bool, today: str) -> dict:
    """Payout and net debt/EBITDA for one company from its companyfacts,
    or an empty dict where the filings do not support them."""
    f = (facts or {}).get("facts") or {}
    rev, currency = RC._annual_series(f, RC.TAGS["revenue"], "USD")
    currency = currency or "USD"

    def series(slots):
        s, unit = RC._annual_series(f, slots, currency)
        return s if unit in (currency, "") else {}

    ni, ocf, capex = (series(RC.TAGS[k]) for k in ("net_income", "ocf", "capex"))
    div = series(SAFETY_TAGS["div_paid"])
    dda, dep, amort = (series(SAFETY_TAGS[k]) for k in ("dda", "dep", "amort"))
    interest = series(RC.TAGS["interest_expense"])
    ebit = {y: v + interest.get(y, 0.0) for y, v in series(RC.TAGS["pretax_income"]).items()}
    ebit.update(series(RC.TAGS["op_income"]))          # the operating line wins where filed

    bs = RC.balance_sheet(f, want_unit=currency, revenue=rev.get(max(rev)) if rev else None)
    as_of = bs.get("as_of")
    if not as_of or bs.get("unit") != currency:
        return {}
    try:
        age = (datetime.fromisoformat(today) - datetime.fromisoformat(as_of)).days
    except ValueError:
        return {}
    if age > BALANCE_SHEET_MAX_AGE_DAYS:
        return {"safety_as_of": as_of, "safety_note": "latest balance sheet is over 21 months old"}
    # The fiscal year the latest balance sheet closes. A January/February
    # year-end (retailers) belongs to the fiscal year before.
    last_fy = int(as_of[:4]) - (1 if as_of[5:7] in ("01", "02") and int(as_of[:4]) not in ni else 0)
    da = {y: dda.get(y, (dep.get(y) or 0.0) + (amort.get(y) or 0.0)) for y in set(dda) | set(dep)}
    po, po_basis, po_years = payout_cover(div, ni, ocf, capex, last_fy,
                                          da if sector in FFO_SECTORS else None)
    nd, nd_basis = net_debt_to_ebitda(bs.get("total_debt"), bs.get("cash"),
                                      ebitda_by_year(ebit, dda, dep, amort), last_fy,
                                      bs.get("notes"), insurer)
    out = {"safety_as_of": as_of, "po_basis": po_basis, "nd_basis": nd_basis}
    if po is not None:
        out["po"] = round(po, 1)
        out["po_years"] = f"FY{po_years[0]}–{po_years[-1]}" if len(po_years) > 1 else f"FY{po_years[0]}"
    if nd is not None:
        out["nd"] = nd
    return {k: v for k, v in out.items() if v is not None}


def fetch_safety(today: str) -> dict[str, dict]:
    """{ticker: safety fields} for every UNIVERSE name SEC has filings for."""
    try:
        ciks = ticker_ciks(RC._get(SEC_TICKERS_URL))
    except (urllib.error.URLError, TimeoutError, OSError, ValueError) as e:
        print(f"[aristocrats] SEC ticker map failed — {e}; safety stays unmeasured")
        return {}
    out: dict[str, dict] = {}
    for a in UNIVERSE:
        cik = ciks.get(a["t"].upper())
        if not cik:
            continue
        try:
            facts = RC._get(FACTS_URL.format(cik=cik))
        except (urllib.error.URLError, TimeoutError, OSError, ValueError) as e:
            print(f"[aristocrats] {a['t']}: SEC facts failed — {e}")
            continue
        s = sec_safety(facts, a.get("sec", ""), bool(a.get("insurer")), today)
        if s:
            out[a["t"]] = s
        time.sleep(0.15)                     # SEC fair access
    print(f"[aristocrats] SEC safety: {sum('po' in v for v in out.values())} payouts, "
          f"{sum('nd' in v or v.get('nd_basis') in ('n/a', 'net cash') for v in out.values())} "
          f"leverage figures of {len(UNIVERSE)} names")
    return out


def main() -> int:
    tickers = [a["t"] for a in UNIVERSE]
    print(f"[aristocrats] Fetching {len(tickers)} tickers from Yahoo chart API…")
    results: dict[str, dict] = {}
    failures: list[str] = []
    for i, t in enumerate(tickers, 1):
        try:
            row = fetch_ticker(t)
        except (urllib.error.URLError, urllib.error.HTTPError, TimeoutError,
                OSError, ValueError, KeyError) as e:
            row = None
            print(f"[aristocrats] {t}: fetch failed — {e}")
        if row:
            results[t] = row
        else:
            failures.append(t)
        if i % 10 == 0:
            print(f"[aristocrats] {i}/{len(tickers)} done ({len(results)} ok)")
        time.sleep(SLEEP_BETWEEN)

    frac = len(results) / max(1, len(tickers))
    print(f"[aristocrats] Success: {len(results)}/{len(tickers)} "
          f"({frac:.0%}); failed: {', '.join(failures) or 'none'}")
    if frac < MIN_SUCCESS_FRACTION:
        print("[aristocrats] Too many failures — Yahoo may have changed "
              "its chart API or blocked the runner. Not writing output.")
        return 2

    now = datetime.now(timezone.utc)
    for t, fields in fetch_safety(now.strftime("%Y-%m-%d")).items():
        results.setdefault(t, {}).update(fields)
    payload = {
        "as_of":      now.strftime("%Y-%m-%d"),
        "fetched_at": now.strftime("%Y-%m-%dT%H:%MZ"),
        "count":      len(results),
        "tickers":    results,
    }
    out_path = Path(__file__).resolve().parent.parent / "data" / "aristocrats.json"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as fh:
        json.dump(payload, fh, indent=1, sort_keys=True)
        fh.write("\n")
    print(f"[aristocrats] ✓ Wrote {out_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())

"""Build the FCF-quality snapshot by joining two files this repo already has.

    python scripts/build_fcf_quality.py [--out data/fcf_quality_snapshots/]

NO NETWORK. Everything it needs is already committed:

    data/schloss.json       5,725 rows — market cap, shares, price
    data/compounders.json   1,948 rows — free cash flow, revenue, trends

That is deliberate rather than a shortcut. Both files are produced by
Actions that already handle SEC's rate limits, its tag ladders and its
foreign filers; a third fetcher doing the same work differently is a
third set of parsing bugs and a third thing to keep in step.

WHY MARKET CAP AND NOT PRICE. BACKLOG.md records that
refresh_compounders.py writes a wrong `price` for some tickers — Booking
Holdings comes through at $193 against a real ~$4,500. The schloss file's
`market_cap` for the same company is $145bn, which at ~32m shares is
right. So the market cap is sound where the price is not, and every
multiple here is computed from it. Booking's P/FCF goes from a
nonsensical 0.7x to 16.0x on that one change.

THE BASIS IS DECIDED BY THE DATA, not by this script. refresh_compounders
now extracts total debt, cash, preferred stock and minority interest, so
each row may be able to supply an enterprise value. fcf_quality.
choose_basis() looks at the cohort: if any row can, the board ranks on EV
and rows that cannot are listed unranked; if none can — a compounders file
built before that extractor existed — every row drops to market cap
together, which is a different measure and is labelled as one everywhere
it appears.

What is never allowed is a board with some rows on each. Market cap is
smaller than enterprise value for any company carrying net debt, so a
mixed board would hand a higher yield, and a better rank, to whichever
rows were missing a debt tag.
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import date, timezone, datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

import fcf_quality as Q          # noqa: E402
import holt as H                 # noqa: E402

SCHLOSS = ROOT / "data" / "schloss.json"
COMPOUNDERS = ROOT / "data" / "compounders.json"
OUT_DIR = ROOT / "data" / "fcf_quality_snapshots"


def _oracle_net_debt(c: dict):
    """Net debt implied by this pipeline's own net-debt/EBIT ratio.

    Deliberately a DIFFERENT path to the same number than the balance
    sheet: nd_ebit is computed upstream from EBIT and a net-debt figure
    the compounders refresh assembles its own way. Two paths agreeing is
    evidence; one path alone is an assertion.

    EBIT is approximated as revenue x operating margin, so this carries
    real error and the check that consumes it is loose by design.
    """
    nd, rev, om = c.get("nd_ebit"), c.get("revenue_last"), c.get("op_margin_now")
    if nd is None or not rev or om is None:
        return None
    ebit = rev * om / 100.0
    if ebit <= 0:
        return None
    return nd * ebit


def load_join() -> tuple[list, dict]:
    """Every company both files can describe, as screen() input rows."""
    sch = json.loads(SCHLOSS.read_text())
    cmp_ = json.loads(COMPOUNDERS.read_text())
    by_ticker = {r["ticker"]: r for r in sch["rows"] if r.get("ticker")}
    comp = cmp_["tickers"]

    rows, skipped = [], {"no_market_cap": 0, "not_in_schloss": 0}
    for ticker, c in comp.items():
        s = by_ticker.get(ticker)
        if not s:
            skipped["not_in_schloss"] += 1
            continue
        mc = s.get("market_cap")
        if not mc:
            skipped["no_market_cap"] += 1
            continue

        fcf = c.get("fcf_last")
        rows.append({
            "ticker": ticker,
            "name": c.get("name") or s.get("name") or ticker,
            "market_cap": mc,
            "fcf": fcf,
            "revenue": c.get("revenue_last"),
            "exchange": c.get("exchange") or s.get("exchange"),
            "country": c.get("country"),
            "industry": c.get("industry"),
            "sic": c.get("sic"),
            # Enterprise-value inputs, once refresh_compounders has been
            # re-run with the balance-sheet extractor. Absent until then,
            # and absent is what they must be: enterprise_value() refuses
            # rather than computing an EV out of assumed zeros, and
            # choose_basis() drops the whole board to market cap so no row
            # gains an advantage from a tag another row is missing.
            "total_debt": c.get("total_debt"),
            "cash": c.get("cash"),
            "preferred": c.get("preferred"),
            "minority": c.get("minority"),
            "debt_inferred_zero": c.get("debt_inferred_zero"),
            "bs_as_of": c.get("bs_as_of"),
            # An INDEPENDENT estimate of the same quantity, from a
            # different code path: this pipeline's own net-debt/EBIT,
            # multiplied back out. fcf_quality.debt_cross_check() refuses
            # a row whose extracted debt is under half of it — foreign
            # IFRS filers whose borrowings sit under element names the tag
            # ladder does not carry come through with a fraction of their
            # real debt, and a fraction of the debt is an inflated yield.
            "oracle_net_debt": _oracle_net_debt(c),
            # The two growth legs available from filings. VFLO's third —
            # a 3-5 year consensus EPS growth estimate — has no filing
            # equivalent and is simply not here.
            "growth_components": {
                "sales_trend": c.get("rev_trend"),
                "fcf_trend": c.get("fcf_trend"),
            },
            # Carried for the page, not used in the ranking.
            "fcf_conv": c.get("fcf_conv"),
            "capex_ocf": c.get("capex_ocf"),
            "roic_med": c.get("roic_med"),
            "op_margin_now": c.get("op_margin_now"),
            "nd_ebit": c.get("nd_ebit"),
            "pfcf_med": c.get("pfcf_med"),
            "rev_cagr5": c.get("rev_cagr5"),
            "years": c.get("years"),
            "cyclical": c.get("cyclical"),
            "foreign": c.get("foreign"),
        })
    meta = {
        "schloss_as_of": sch.get("generated"),
        "compounders_as_of": cmp_.get("as_of"),
        "schloss_rows": len(by_ticker),
        "compounders_rows": len(comp),
        "joined": len(rows),
        "skipped": skipped,
    }
    return rows, meta


def multiple_guard(rows: list) -> int:
    """Apply holt.py's P/FCF fault test to every ranked row.

    Not a second opinion on the yield — a second opinion on the PRICE
    behind it. holt.py was written against this exact compounders file
    after 31 companies came through under 2x P/FCF and 29 under a fifth
    of their own fifteen-year median: data faults wearing the costume of
    bargains, and on a board sorted by cheapness they were the whole top.

    A yield screen has the identical exposure from the other direction —
    a broken multiple of 0.8x IS a 125% yield — so the guard is reused
    rather than reasoned about again. Marks rows; does not drop them.
    """
    flagged = 0
    for r in rows:
        mc, fcf = r.get("market_cap"), r.get("fcf")
        if not mc or not fcf or fcf <= 0:
            continue
        mult = mc / fcf
        r["pfcf"] = round(mult, 1)
        fault = H.multiple_fault(mult, r.get("pfcf_med"))
        if fault:
            r["multiple_fault"] = fault
            flagged += 1
    return flagged


def build(out_dir: Path = OUT_DIR) -> dict:
    rows, join_meta = load_join()
    result = Q.screen(rows)

    for bucket in ("final", "fcf_cut", "measured"):
        multiple_flagged = multiple_guard(result[bucket])
    for bucket in ("measured", "fcf_cut", "final"):
        for r in result[bucket]:
            r["size_band"] = Q.size_band(r.get("market_cap"))
            note = Q.leverage_note(r.get("nd_ebit"), result["basis"])
            if note:
                r["leverage_note"] = note

    keep = ("ticker", "name", "market_cap", "fcf", "revenue", "fcf_yield",
            "growth_score", "growth_ranks", "growth_components", "size_band",
            "exchange", "country", "industry", "pfcf", "pfcf_med",
            "multiple_fault", "fcf_conv", "capex_ocf", "roic_med",
            "op_margin_now", "nd_ebit", "rev_cagr5", "years", "cyclical",
            "foreign", "basis", "ev", "reason", "leverage_note",
            "total_debt", "cash", "debt_inferred_zero", "bs_as_of")

    def slim(r):
        return {k: r.get(k) for k in keep if r.get(k) is not None}

    below = Q.below_large_cap(result["final"])
    snapshot = {
        "_meta": {
            "built": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
            "basis": result["basis"],
            "basis_note": (
                "Yield is free cash flow over MARKET CAP, not enterprise "
                "value: neither source file carries total debt or cash, so "
                "no row can supply an EV and the whole board shares one "
                "denominator. Adding debt and cash to the compounders "
                "refresh turns this into a true EV board."
                if result["basis"] == "market_cap" else
                "Yield is free cash flow over enterprise value, VFLO's own "
                "denominator."),
            "forward_looking": False,
            "forward_note": (
                "VFLO averages trailing and FORWARD 12-month free cash "
                "flow and includes a 3-5 year consensus EPS growth "
                "estimate. Both need an analyst feed. Everything here is "
                "trailing, from filings."),
            "fcf_take": Q.FCF_TAKE,
            "growth_take": Q.GROWTH_TAKE,
            "min_market_cap": Q.MIN_MARKET_CAP,
            "min_revenue": Q.MIN_REVENUE,
            "growth_components": ["sales_trend", "fcf_trend"],
            "vflo_components": ["sales trend", "EBITDA trend",
                               "long-term EPS growth estimate"],
            "ev_inputs": {
                "debt_cross_check_refused": sum(
                    1 for r in result["rejected"]
                    if "net-debt/EBIT" in (r.get("reason") or "")),
                "with_debt": sum(1 for r in rows if r.get("total_debt") is not None),
                "with_cash": sum(1 for r in rows if r.get("cash") is not None),
                "debt_inferred_zero": sum(1 for r in rows
                                          if r.get("debt_inferred_zero")),
                "of": len(rows),
            },
            "join": join_meta,
            "census": result["census"],
            "multiple_flagged": multiple_flagged,
            "vflo_published": Q.VFLO_PUBLISHED,
        },
        "stages": result["stages"],
        "final": [slim(r) for r in result["final"]],
        "fcf_cut": [slim(r) for r in result["fcf_cut"]],
        "below_large_cap": [r["ticker"] for r in below],
        "rejected": [{"ticker": r.get("ticker"), "name": r.get("name"),
                      "market_cap": r.get("market_cap"),
                      "reason": r.get("reason")}
                     for r in result["rejected"]],
    }

    out_dir.mkdir(parents=True, exist_ok=True)
    month = date.today().strftime("%Y-%m")
    path = out_dir / f"{month}.json"
    path.write_text(json.dumps(snapshot, indent=1, sort_keys=True))
    return {"path": path, "snapshot": snapshot}


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--out", default=str(OUT_DIR))
    args = ap.parse_args()

    r = build(Path(args.out))
    s, m = r["snapshot"], r["snapshot"]["_meta"]
    c = m["census"]
    print(f"wrote {r['path']}")
    print(f"  basis           {m['basis']}")
    ev = m["ev_inputs"]
    print(f"  ev inputs       {ev['with_debt']}/{ev['of']} with debt, "
          f"{ev['with_cash']}/{ev['of']} with cash, "
          f"{ev['debt_inferred_zero']} inferred debt-free")
    print(f"  joined          {m['join']['joined']} of "
          f"{m['join']['compounders_rows']} compounders rows")
    print(f"  measured        {c['measured']}")
    print(f"  unmeasurable    {c['unmeasurable']} "
          f"({c.get('seen_but_unrankable', 0)} seen but not rankable)")
    print(f"  fcf cut         {c['fcf_cut']}")
    print(f"  final           {c['final']}")
    print(f"  below large cap {len(s['below_large_cap'])} of {c['final']}")
    print(f"  multiple faults {m['multiple_flagged']}")
    lev = sum(1 for r in s["final"] if r.get("leverage_note"))
    print(f"  levered (yield overstated on market cap) {lev} of {c['final']}")
    print()
    print("  stage                    n   median yield   median growth")
    for st in s["stages"]:
        print(f"  {st['label']:<22} {st['count']:>4}   "
              f"{str(st['median_fcf_yield']):>12}   "
              f"{str(st['median_growth_score']):>13}")
    return 0


if __name__ == "__main__":
    sys.exit(main())

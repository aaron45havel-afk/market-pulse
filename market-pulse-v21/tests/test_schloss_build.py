"""Parsers behind the Schloss build, proved without touching the network.

Run:  python tests/test_schloss_build.py      (exit 0 = all pass)

sec.gov is unreachable from the sandbox this was written in, so every
parser had to be provable against a fixture or it would ship unverified.
That constraint shaped the module: the fetch functions are thin wrappers
that do nothing but assemble URLs, and all the logic that can be wrong
lives in pure functions tested here.

The two that carry real risk:

  * MERGE ORDER. Frames bucket by CALENDAR quarter, so a November
    year-end filer's balance sheet lands one bucket away from a December
    one. Reading only the newest quarter drops most of the market, not a
    corner of it — so four quarters are overlaid newest-first, and the
    overlay direction is the whole correctness argument.

  * TAG MIXING. The compounders build produced growth rates that spanned
    two incompatible definitions of revenue because it took values
    tag-by-tag per year. `pick_series` exists so that one series means one
    definition, and other tags may only fill years the winner is silent
    on.
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(
    os.path.abspath(__file__))), "scripts"))

import refresh_schloss as R

_COUNT = 0
_FAILS = []


def check(cond, msg):
    global _COUNT
    _COUNT += 1
    if not cond:
        _FAILS.append(msg)


# ── parse_frame ─────────────────────────────────────────────────────
FRAME = {
    "taxonomy": "us-gaap", "tag": "StockholdersEquity", "ccp": "CY2026Q1I",
    "uom": "USD",
    "data": [
        {"cik": 320193, "entityName": "Apple Inc.", "val": 57000000000},
        {"cik": 789019, "entityName": "Microsoft", "val": 302000000000.0},
        {"cik": 1000045, "entityName": "No Value", "val": None},
        {"cik": None, "entityName": "No CIK", "val": 5},
        {"cik": 1000046, "entityName": "Junk", "val": "n/a"},
        {"cik": 1000047, "entityName": "Deficit", "val": -400000},
    ],
}
f = R.parse_frame(FRAME)
check(f[320193] == 5.7e10, "an integer value parses to float")
check(f[789019] == 3.02e11, "so does a float")
check(f[1000047] == -400000.0, "negative equity is a real reading, kept")
check(1000045 not in f,
      "a null value is DROPPED — a company that reported nothing is not a "
      "company that reported zero")
check(1000046 not in f, "and so is a non-numeric one")
check(len(f) == 3, f"three usable rows out of six (got {len(f)})")
check(R.parse_frame(None) == {}, "a failed request is an empty frame, not a crash")
check(R.parse_frame({}) == {}, "and so is a malformed payload")
check(R.parse_frame({"data": []}) == {}, "and an empty one")
check(R.parse_frame({"data": [{"cik": 1, "val": True}]}) == {},
      "a boolean is not a dollar figure — bool is an int subclass in Python "
      "and would otherwise land as 1.0")


# ── merge_newest_first ──────────────────────────────────────────────
q1 = {320193: 57.0, 789019: 302.0}           # newest quarter
q2 = {320193: 55.0, 1045810: 79.0}           # one back
q3 = {66740: 45.0, 320193: 50.0}             # two back
m = R.merge_newest_first([q1, q2, q3])
check(m[320193] == 57.0, "the newest reading wins for a company in all three")
check(m[789019] == 302.0, "a company only in the newest is kept")
check(m[1045810] == 79.0,
      "and so is one that only appears a quarter back — an off-calendar "
      "year-end must not delete the company")
check(m[66740] == 45.0, "or two quarters back")
check(len(m) == 4, "four distinct companies across three quarters")
check(R.merge_newest_first([]) == {}, "no frames is no data")

# The direction is the correctness argument, so it is pinned explicitly.
check(R.merge_newest_first([{1: 10.0}, {1: 20.0}])[1] == 10.0,
      "FIRST list entry wins — the caller passes newest-first, and reversing "
      "this would publish year-old balance sheets as current")


# ── pick_series ─────────────────────────────────────────────────────
# The ASC 606 shape: an old tag with a couple of early years, a new tag
# with the bulk of the window.
by_tag = {
    "Revenues": {2018: 100.0, 2019: 110.0},
    "RevenueFromContractWithCustomerExcludingAssessedTax": {
        2020: 130.0, 2021: 140.0, 2022: 150.0, 2023: 160.0, 2024: 170.0},
}
s = R.pick_series(by_tag)
check(s[2024] == 170.0, "the better-covered tag supplies its own years")
check(s[2018] == 100.0, "and the sparse tag fills years it alone covers")
check(len(s) == 7, f"seven years in total (got {len(s)})")

# Where they overlap, the winner must not be overwritten.
overlap = R.pick_series({
    "Winner": {2020: 1.0, 2021: 2.0, 2022: 3.0},
    "Loser": {2022: 99.0, 2023: 4.0},
})
check(overlap[2022] == 3.0,
      "the most-covered tag owns every year it reports — a filled gap must "
      "never overwrite the definition that won")
check(overlap[2023] == 4.0, "but a genuine gap is still filled")
check(R.pick_series({}) == {}, "no tags is an empty series")
check(R.pick_series({"Only": {2024: 5.0}}) == {2024: 5.0}, "one tag is that tag")


# ── parse_master_idx ────────────────────────────────────────────────
IDX = """Description:           Master Index of EDGAR Dissemination Feed
Last Data Received:    March 31, 2006
Comments:              webmaster@sec.gov

CIK|Company Name|Form Type|Date Filed|Filename
--------------------------------------------------------------------------------
320193|APPLE COMPUTER INC|10-Q|2006-02-01|edgar/data/320193/0001104659.txt
320193|APPLE COMPUTER INC|8-K|2006-01-18|edgar/data/320193/0001104659.txt
66740|MINNESOTA MINING & MANUFACTURING CO|10-K|2006-02-16|edgar/data/66740/x.txt
1045810|NVIDIA CORP|10-Q|2006-03-06|edgar/data/1045810/y.txt
"""
idx = R.parse_master_idx(IDX)
check(idx == {320193, 66740, 1045810},
      f"three distinct CIKs, duplicates collapsed (got {sorted(idx)})")
check(R.parse_master_idx("") == set(), "an empty index is an empty set")
check(R.parse_master_idx(None) == set(), "and so is a failed download")
check(R.parse_master_idx("garbage\nno pipes here") == set(),
      "a body that is not an index yields nothing rather than noise")


# ── is_warrant ──────────────────────────────────────────────────────
check(R.is_warrant("ABCDW") is True, "a five-letter W ticker is a warrant")
check(R.is_warrant("XYZ-WS") is True, "and so is an explicit -WS")
check(R.is_warrant("XYZ.U") is True, "and a SPAC unit")
check(R.is_warrant("AAPL") is False, "AAPL is a company")
check(R.is_warrant("GOOGL") is False,
      "GOOGL is five letters but does not end in W, R or U")
check(R.is_warrant("BRK-B") is False, "and a share class is not a warrant")
check(R.is_warrant("") is False, "an empty ticker does not crash")
# These matter because a warrant carries the ISSUER's CIK, so it would
# inherit the whole balance sheet and appear as a duplicate row.
check(R.is_warrant("SPACU") is True, "SPAC units are excluded")


# ── shares_growth ───────────────────────────────────────────────────
check(R.shares_growth(100.0, 100.0, 5) == 0.0, "a flat share count is 0%")
check(R.shares_growth(100.0, 121.0, 2) == 10.0, "21% over two years is 10%/yr")
g = R.shares_growth(100.0, 90.0, 5)
check(-2.2 < g < -2.0, f"a buyback is negative (got {g})")
check(R.shares_growth(None, 100.0, 5) is None,
      "no base is UNKNOWN — never 0%, which would read as 'issued nothing'")
check(R.shares_growth(100.0, None, 5) is None, "and neither is no endpoint")
check(R.shares_growth(0.0, 100.0, 5) is None, "a zero base is undefined, not infinite")
check(R.shares_growth(-5.0, 100.0, 5) is None, "and so is a negative one")
check(R.shares_growth(100.0, 110.0, 0) is None, "as is a zero-year span")


# ── history: which direction the number errs ────────────────────────
h = R.history(1996, 2026, 2016)
check(h == {"years": 30, "bound": "min", "since": 1996},
      f"seen filing in 1996 is AT LEAST 30 years (got {h})")
h = R.history(2006, 2026, 2016)
check(h["years"] == 20 and h["bound"] == "min",
      "seen in 2006 clears the 20-year bar")
h = R.history(None, 2026, 2016)
check(h == {"years": 10, "bound": "max", "since": None},
      f"never seen in any probe means FEWER than 10 years (got {h})")
check(h["bound"] == "max",
      "and the bound travels with the number — 'at least 30' and 'at most 10' "
      "are different claims and the page must not render either as exact")

# The DISPLAY bound above and the SCORING year below are different jobs.
check(R.bounded_first_year(1996, 2016) == 1996, "a probe hit is the real answer")
check(R.bounded_first_year(None, 2016) == 2017,
      "a complete miss scores from the year AFTER the newest probe — not from "
      "None. The company was demonstrably not filing in 2016, so it fails a "
      "20-year gate on the merits, and passing None would park every young "
      "company in 'could not read' instead")
import schloss as _S  # noqa: E402
check(_S.survival(R.bounded_first_year(None, 2016), 2026) == (False, 9),
      "which resolves the gate to a definite FAIL rather than an unknown")
check(_S.survival(R.bounded_first_year(2001, 2016), 2026) == (True, 25),
      "and leaves a real hit scoring normally")


# ── quarters_back: the lag, and the year rollover ───────────────────
qs = R.quarters_back(2026, 8, 4)
check(qs == [(2026, 2), (2026, 1), (2025, 4), (2025, 3)],
      f"August 2026 starts at Q2, one back from the current quarter (got {qs})")
check(R.instant_period((2026, 2)) == "CY2026Q2I",
      "instantaneous periods carry the I suffix")
check(R.quarters_back(2026, 1, 2) == [(2025, 4), (2025, 3)],
      "January rolls back into the previous year")
check(R.quarters_back(2026, 12, 1) == [(2026, 3)],
      "December is in Q4, so one back is Q3")
# The five-year shift used for dilution keeps the same quarters.
shifted = [(y - 5, q) for y, q in qs]
check(shifted[0] == (2021, 2),
      "shifting the quarter pair rather than the date avoids Feb-29 and "
      "keeps both share counts on matching fiscal calendars")


# ── frames_url ──────────────────────────────────────────────────────
check(R.frames_url("StockholdersEquity", "USD", "CY2026Q1I").endswith(
      "us-gaap/StockholdersEquity/USD/CY2026Q1I.json"),
      "the frames URL carries taxonomy, tag, UNIT and period")
check("USD-per-shares" in R.frames_url("CommonStockDividendsPerShareDeclared",
                                       "USD-per-shares", "CY2025"),
      "per-share units are hyphenated in the path, not slashed — a slash "
      "would split the URL and 404 every dividend call")
check(R.DIV_TAGS[0][1] == "USD-per-shares",
      "and the dividend tags declare that unit")


# ── the concept map covers what the method reads ────────────────────
for need in ("stockholders_equity", "goodwill", "intangibles", "current_assets",
             "total_liabilities", "cash", "receivables", "inventory", "ppe",
             "short_term_debt", "long_term_debt", "preferred_stock"):
    check(need in R.INSTANT, f"the balance-sheet pull includes {need}")
check("Goodwill" in R.INSTANT["goodwill"],
      "goodwill is pulled explicitly — without it, reported book and tangible "
      "book are indistinguishable and a roll-up at 1.0x book looks cheap")
check(R.INSTANT["stockholders_equity"][0] == "StockholdersEquity",
      "the parent-only equity tag is preferred over the one including "
      "minority interests, which are not the common holder's book")


# ── PREFERRED SERIES ARE NOT THE COMMON ──
# They carry their issuer's CIK, so they inherit its whole balance sheet
# and appear as a duplicate row — precisely the case is_warrant exists to
# stop. Nine reached the August board and not one of them got a quote.
for _t in ("AHL-PD", "ANG-PD", "CDR-PB", "OAK-PA", "SCE-PG", "TRTN-PA",
           "CFTR-PA", "PHXE-P", "SEAL-PA"):
    check(R.is_warrant(_t),
          f"{_t} is a preferred series, not the common — a fixed claim with "
          f"a par value, about which tangible book per COMMON share says "
          f"nothing")
check(R.is_warrant("NONE."),
      "and a trailing separator with nothing after it is a malformed row "
      "rather than a share class")
for _t in ("MOG-A", "BRK-B", "BF-B", "CRD-A"):
    check(not R.is_warrant(_t),
          f"{_t} IS the common in a dual-class company and stays — it needs "
          f"a price, not an exclusion")
check(not R.is_warrant("AAPL") and R.is_warrant("XYZ-WT")
      and R.is_warrant("ABCDW"),
      "ordinary tickers are untouched and warrants still go")


# ── THE QUOTE FILL: winner-take-all left the small end unpriced ──
# The screener feed that wins on coverage lists major exchanges only, so
# 1,148 of 5,673 companies carried no price on the August board — every
# OTC name among them. Nine of those clear every balance-sheet gate,
# including George Risk Industries and Nobility Homes, so their cheapness
# was permanently unknown. The other sources now fill the gaps.
_want = {"AAA", "BBB", "CCC", "DDD"}
_primary = {"AAA": {"price": 1.0}, "BBB": {"price": 2.0}, "ZZZ": {"price": 9.0}}
_extra = [("yahoo", {"CCC": {"price": 3.0}, "AAA": {"price": 999.0}}),
          ("stockanalysis", {"DDD": {"price": 4.0}, "CCC": {"price": 888.0}})]
_merged, _stats = R.merge_quote_fill(_want, _primary, _extra)

check({k: v["price"] for k, v in _merged.items()}
      == {"AAA": 1.0, "BBB": 2.0, "CCC": 3.0, "DDD": 4.0},
      "the gaps fill and every name gets a price")
check(_merged["AAA"]["price"] == 1.0,
      "the WINNING source is authoritative for what it answered — yahoo's "
      "999 does not overwrite it. Two feeds disagreeing is a fact about the "
      "feeds, and preferring whichever ran last would make the board's "
      "numbers depend on source ordering")
check(_merged["CCC"]["price"] == 3.0,
      "and the first filler to answer a name keeps it, for the same reason")
check("ZZZ" not in _merged,
      "a quote for a company not in the universe is dropped, not carried")
check(_stats == {"yahoo": 1, "stockanalysis": 1},
      "each filler reports what it added, so the source label can say which "
      "feed priced how many rather than naming one and implying it did all")
check(R.merge_quote_fill(_want, _primary, [])[0].keys() == {"AAA", "BBB"},
      "with no fillers the result is exactly the winner's answer — the old "
      "behaviour, unchanged, when the other sources cannot be reached")


# ── the monthly snapshot, driven through main() itself ──
#
# The snapshot is the board's only memory. Everything it is for — asking
# whether a Riklis or net-net cutoff would actually have paid, including
# the names that later delisted — depends on a file appearing once a month
# and never being disturbed again. So these drive the REAL main(), with
# only build() and the two output paths swapped out. A test that
# re-implemented the write would have proved nothing about the script
# that runs in CI.
import json as _json
import pathlib as _pathlib
import tempfile as _tempfile


def _fake_payload(limit=0):
    n = limit or 40
    rows = [{"ticker": f"T{i}", "name": f"Co {i}", "cik": str(i),
             "price": 1.0, "market_cap": 1e6, "tangible_book": 2e6,
             "p_tb": 0.5, "ncav": 1e6, "p_ncav": 1.0, "is_net_net": True,
             "below_book": True, "clears_every_gate": True}
            for i in range(n)]
    return {"rows": rows, "generated": "x",
            "params": {"quote_source": "test"},
            "census": {"screened": n, "balance_sheet_qualifying": 1,
                       "positive_ncav": 1, "recent_dividend_cuts": 0,
                       "priced": n, "price_coverage_pct": 100.0,
                       "below_book": n, "net_nets": n}}


class _Quiet:
    """main() prints a summary; the suite's output should stay readable."""
    def write(self, _):
        return 0

    def flush(self):
        pass


def _run_main(argv, snap_dir, out):
    real_build, real_out, real_dir = R.build, R.OUT, R.SNAPSHOT_DIR
    real_argv, real_stdout = sys.argv, sys.stdout
    R.build, R.OUT, R.SNAPSHOT_DIR = _fake_payload, out, snap_dir
    sys.argv, sys.stdout = argv, _Quiet()
    try:
        return R.main()
    finally:
        R.build, R.OUT, R.SNAPSHOT_DIR = real_build, real_out, real_dir
        sys.argv, sys.stdout = real_argv, real_stdout


_tmp = _pathlib.Path(_tempfile.mkdtemp())
_snaps = _tmp / "schloss_snapshots"
_out = _tmp / "schloss.json"

_run_main(["refresh_schloss.py"], _snaps, _out)
_written = sorted(_snaps.glob("*.json"))
check(len(_written) == 1,
      "a full run lays down exactly one snapshot, named for the month")
check(len(_json.loads(_written[0].read_text())["rows"]) == 40,
      "the snapshot carries every screened row — no threshold is applied, "
      "because filtering to today's idea of cheap destroys the ability to "
      "test tomorrow's")
check(not list(_snaps.glob("*.tmp")),
      "the write goes through a temp file and is renamed into place, so a "
      "run killed mid-write leaves last month's file intact rather than "
      "half a JSON document")

_before = _json.loads(_written[0].read_text())
_run_main(["refresh_schloss.py", "--limit", "5"], _snaps, _out)
check(len(_json.loads(_written[0].read_text())["rows"]) == 40,
      "a --limit run does NOT overwrite the month's snapshot — the file is "
      "keyed by month, so a smoke test run after the real one would "
      "otherwise replace a month of history with five companies and "
      "nothing would look wrong")
check(len(list(_snaps.glob("*.json"))) == 1,
      "and it writes no snapshot of its own either")

for _y in (2021, 2022, 2023):
    for _m in range(1, 13):
        (_snaps / f"{_y}-{_m:02d}.json").write_text("{}")
_run_main(["refresh_schloss.py"], _snaps, _out)
_kept = sorted(p.name for p in _snaps.glob("*.json"))
check(len(_kept) == R.MAX_SNAPSHOTS,
      f"the directory is pruned to {R.MAX_SNAPSHOTS} months, so the record "
      f"cannot grow without bound")
check(_written[0].name in _kept,
      "and the prune drops the OLDEST months — never the one just written, "
      "which sorts last by name")


# ── report ──
if _FAILS:
    print(f"FAIL — {len(_FAILS)}/{_COUNT} checks failed:")
    for m in _FAILS:
        print("  ✗", m)
    sys.exit(1)
print(f"OK — all {_COUNT} Schloss build-parser checks passed.")
print(f"   {len(R.INSTANT)} balance-sheet fields, {len(R.ANNUAL)} annual, "
      f"{len(R.PROBE_YEARS)} filing-history probes")
sys.exit(0)

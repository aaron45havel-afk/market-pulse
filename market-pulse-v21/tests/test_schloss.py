"""Walter Schloss screen — the balance-sheet half, provable offline.

Run:  python tests/test_schloss.py      (exit 0 = all pass)

This module takes no price on purpose, so almost all of it is testable
here with no network at all. That is the point of the split: the part
Schloss considered the real work is the part a sandbox can verify.

The assertions below exist mostly to pin ONE rule, stated three ways:

    a figure we did not measure is never scored as a pass,
    a figure we did not measure is never scored as a fail either,
    and a figure we DID measure is never reported as unmeasured.

Every bug this file has caught so far was one of those three collapsing
into another — a missing debt tag reading as zero debt, a missing book
value reading as a failed gate, a negative book value reading as "could
not check". They all produce numbers. None of them produce a complaint.
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import schloss as S

_COUNT = 0
_FAILS = []


def check(cond, msg):
    global _COUNT
    _COUNT += 1
    if not cond:
        _FAILS.append(msg)


# ── tangible book: the un-sellable parts come out ───────────────────
check(S.tangible_book(1000, 300, 100) == 600.0,
      "goodwill and intangibles are subtracted from equity")
check(S.tangible_book(1000) == 1000.0,
      "no goodwill tag means no goodwill — asset-heavy industrials routinely "
      "omit both tags and must not be dropped for it")
check(S.tangible_book(None, 300) is None,
      "but equity itself missing is unknown, not zero")
check(S.tangible_book(500, 900) == -400.0,
      "tangible book is allowed to go negative — that is a finding, not an error")

# The case the subtraction exists for: cheap on reported book, expensive
# on what could actually be sold.
rollup = S.tangible_book(1000, 800, 100)
check(rollup == 100.0,
      f"an acquisition roll-up at 1.0x book is 10x tangible book (got {rollup})")


# ── NCAV: Graham's floor ────────────────────────────────────────────
check(S.ncav(500, 200) == 300.0, "current assets less all liabilities")
check(S.ncav(500, 200, 50) == 250.0, "preferred ranks ahead of the common")
check(S.ncav(500, 800) == -300.0, "and it is allowed to be negative")
check(S.ncav(None, 200) is None, "missing current assets is unknown")
check(S.ncav(500, None) is None, "missing liabilities is unknown — NOT zero, "
      "which would make every company a net-net")


# ── debt: absent tags are not zero debt ─────────────────────────────
ok, ratio = S.debt_ok(100, 200, 1000)
check(ok is True and ratio == 0.3, f"0.3x debt/equity passes (got {ok}, {ratio})")
ok, ratio = S.debt_ok(400, 400, 1000)
check(ok is False and ratio == 0.8, f"0.8x fails (got {ok}, {ratio})")
ok, ratio = S.debt_ok(0, 500, 1000)
check(ok is True and ratio == 0.5, "the boundary itself passes")

ok, ratio = S.debt_ok(100, 200, -50)
check(ok is False and ratio is None,
      "negative equity fails with no ratio — undefined number, defined verdict")
ok, ratio = S.debt_ok(100, 200, None)
check(ok is None and ratio is None, "no equity figure is unknown")

# The one that matters. A leveraged company that simply did not tag its
# borrowings must not walk through the gate built to stop it.
ok, ratio = S.debt_ok(None, None, 1000)
check(ok is None and ratio is None,
      "neither debt tag and no liability total is UNKNOWN, not debt-free")
ok, ratio = S.debt_ok(None, None, 1000, total_liabilities=300)
check(ok is True and ratio is None,
      "but if the ENTIRE liability stack is inside the limit, debt cannot "
      "exceed it — no-debt is proved, and the ratio stays unknown because "
      "we did not actually measure it")
ok, ratio = S.debt_ok(None, None, 1000, total_liabilities=3000)
check(ok is None and ratio is None,
      "a large liability stack proves nothing either way — still unknown")
ok, ratio = S.debt_ok(100, None, 1000, total_liabilities=3000)
check(ok is True and ratio == 0.1,
      "when a real debt tag exists it is used, and the liability bound is ignored")


# ── dividends: paying, and the cut ──────────────────────────────────
d = S.dividend_history({2021: 1.0, 2022: 1.1, 2023: 1.2, 2024: 1.3})
check(d["pays"] is True and d["cut"] is False, "a rising payer pays and has not cut")
check(d["cut_pct"] == 0.0, "and the cut size is zero, not unknown")
check(d["years"] == 4, "the number of years observed is reported")

d = S.dividend_history({2023: 2.00, 2024: 0.50})
check(d["cut"] is True, "a 75% reduction is a cut")
check(d["cut_pct"] == 75.0, f"and its size is measured (got {d['cut_pct']})")
check(d["pays"] is True, "a cut payer is still a payer")
check((d["from_year"], d["to_year"]) == (2023, 2024),
      "the two years compared are reported so recency can be judged by the caller")

# Elimination and reduction are different facts about governance.
gone = S.dividend_history({2023: 2.00, 2024: 0.0})
check(gone["cut"] is True and gone["cut_pct"] == 100.0, "elimination is a 100% cut")
check(gone["pays"] is False,
      "but a company that eliminated its dividend does not pay one — the "
      "governance check and the entry signal disagree here, and both are right")

# A SPECIAL DIVIDEND NOT REPEATING IS NOT A CUT. Costco's real series:
# $15/share special on top of the regular $4.16 in fiscal 2024, which read
# as a 74.6% reduction and landed second on the entry list.
cost = S.dividend_history({2021: 3.16, 2022: 3.48, 2023: 3.86,
                           2024: 19.16, 2025: 4.92})
check(cost["cut"] is False,
      "the year after a special dividend is not a cut — nothing was reduced")
check(cost["special_prior"] is True, "and the row records why it was excluded")
check(cost["pays"] is True and cost["latest"] == 4.92,
      "while the ordinary dividend still reports normally")

# A real cut from a sustained level must survive the same test. Kohl's
# went $2.00 -> $0.50 with no special anywhere in the series.
kss = S.dividend_history({2021: 0.60, 2022: 2.00, 2023: 2.00,
                          2024: 2.00, 2025: 0.50})
check(kss["cut"] is True and kss["cut_pct"] == 75.0,
      f"a genuine 75% cut is untouched (got {kss['cut']}, {kss['cut_pct']})")
check(kss["special_prior"] is False, "and is not mistaken for a special")

# A company that has always paid a lot has a high median too, so the
# multiple never fires on it.
rich = S.dividend_history({2022: 10.0, 2023: 10.0, 2024: 10.0, 2025: 3.0})
check(rich["cut"] is True and rich["special_prior"] is False,
      "a consistently large payout being cut is a cut")

check(S.dividend_history({})["pays"] is None,
      "no series at all is unknown, not 'does not pay'")
check(S.dividend_history({})["cut"] is None, "and the cut is unknown too")
check(S.dividend_history({2024: 1.0})["cut"] is None,
      "one year cannot establish a cut")
check(S.dividend_history({2024: 1.0})["pays"] is True,
      "but one year is enough to establish that it pays")
check(S.dividend_history({"2023": "1.0", "2024": "0.5"})["cut"] is True,
      "strings out of JSON parse")
check(S.dividend_history({"n/a": 1.0, 2024: 1.0})["years"] == 1,
      "unparseable years are dropped, not counted")
flat = S.dividend_history({2023: 0.0, 2024: 0.0})
check(flat["cut"] is False,
      "zero to zero is not a cut — the percentage off a zero base is "
      "undefined, but the verdict is not, and calling it unknown would put "
      "every non-payer in the just-cut bucket this screen shops in")
check(flat["cut_pct"] == 0.0, "a non-cut has zero size")
check(S.dividend_history({2023: 0.0, 2024: 1.0})["cut"] is False,
      "initiating a dividend is likewise not a cut")


# ── self-dealing: unmeasured is never a pass ────────────────────────
m = S.self_dealing(sbc=5e6, revenue=1e9, shares_cagr=-1.0, insider_pct=25.0)
check(m["sbc_pct_revenue"] == 0.5, "SBC is expressed against revenue")
check(m["checks"] == {"pay": True, "dilution": True, "aligned": True},
      f"a clean company passes all three (got {m['checks']})")
check(m["passed"] == 3 and m["known"] == 3, "three known, three passed")

m = S.self_dealing(sbc=3e8, revenue=1e9, shares_cagr=6.0, insider_pct=0.2)
check(m["checks"] == {"pay": False, "dilution": False, "aligned": False},
      "30% of revenue in stock, 6%/yr dilution and no ownership fails all three")
check(m["passed"] == 0 and m["known"] == 3, "three known, none passed")

blank = S.self_dealing()
check(blank["checks"] == {"pay": None, "dilution": None, "aligned": None},
      "nothing reported is three unknowns")
check(blank["known"] == 0 and blank["passed"] == 0,
      "and `passed` counts only what was actually cleared — a company that "
      "discloses nothing must not read as a company that passed everything")
check(S.self_dealing(sbc=5e6, revenue=0)["sbc_pct_revenue"] is None,
      "zero revenue makes the ratio undefined, not infinite")
check(S.self_dealing(sbc=5e6)["checks"]["pay"] is None,
      "SBC without revenue cannot be scored")

# A PERCENTAGE IS ONLY AS GOOD AS ITS DENOMINATOR. Banks report fee
# income under `Revenues` and leave net interest income — most of the
# business — outside it, so East West Bancorp's $2.6bn came through as
# $55m and its stock compensation read 137.7% of revenue.
bank = S.self_dealing(sbc=76e6, revenue=55.3e6, total_assets=76e9)
check(bank["sbc_pct_revenue"] is None,
      "revenue at 0.07% of total assets is not the company's revenue, so the "
      "ratio built on it is withheld rather than published as 137%")
check(bank["revenue_unrepresentative"] is True, "and the row says why")
check(bank["checks"]["pay"] is None,
      "the pay check goes unknown — it must not read as a FAILURE either, "
      "since nothing about the company was measured")
check(bank["sbc_pct_assets"] == 0.1,
      f"SBC against the balance sheet is still reported as context "
      f"(got {bank['sbc_pct_assets']})")

fixed = S.self_dealing(sbc=76e6, revenue=2.6e9, total_assets=76e9)
check(fixed["sbc_pct_revenue"] == 2.92,
      f"with net interest income included the same bank reads 2.9% "
      f"(got {fixed['sbc_pct_revenue']})")
check(fixed["checks"]["pay"] is True, "and passes on the merits")

# A genuinely tiny-revenue company is NOT the same case. Applied
# Energetics really does pay ten times its revenue in stock, and that is
# the signal, not an artefact.
real = S.self_dealing(sbc=4.8e6, revenue=461_727, total_assets=5e6)
check(real["revenue_unrepresentative"] is False,
      "revenue at 9% of assets is small but representative")
check(real["sbc_pct_revenue"] > 1000 and real["checks"]["pay"] is False,
      "so the ratio stands and the company fails the check — a small "
      "denominator is not a wrong one")

check(S.self_dealing(sbc=1e6, revenue=1e8)["sbc_pct_revenue"] == 1.0,
      "with no total assets there is nothing to judge the denominator "
      "against, so the old behaviour stands rather than a guess")

# Read the other way round from most screens, deliberately: Schloss took
# Potlatch's 40% insider stake as alignment, not entrenchment.
check(S.self_dealing(insider_pct=40.0)["checks"]["aligned"] is True,
      "heavy insider ownership is alignment here, not a governance flag")


# ── survival: filings, not financials ───────────────────────────────
ok, yrs = S.survival(1994, 2026)
check(ok is True and yrs == 32, f"32 years of filings clears 20 (got {ok}, {yrs})")
ok, yrs = S.survival(2015, 2026)
check(ok is False and yrs == 11, "11 years does not")
ok, yrs = S.survival(2006, 2026)
check(ok is True and yrs == 20, "the boundary itself clears")
check(S.survival(None, 2026) == (None, None), "no first filing year is unknown")
check(S.survival(2030, 2026) == (None, None), "a future start is bad data, not -4 years")

# The reason this reads the filing index rather than the XBRL history:
# machine-readable financials start around 2010, so no company can show
# more than ~16 years of them. Measuring survival from that would cap
# every company below the 20-year bar Schloss set.
ok, _ = S.survival(2010, 2026)
check(ok is False,
      "16 years of XBRL cannot clear 20 — which is exactly why survival is "
      "measured from the submissions index instead")


# ── asset backing: what the book actually consists of ───────────────
b = S.asset_backing(cash=200, receivables=100, inventory=100, ppe=600,
                    goodwill=0, intangibles=0, anchor=1100)
check(b["total"] == 1000.0, "the named components total")
check(b["hard_pct"] == 30.0, f"cash + receivables is the hard part (got {b['hard_pct']})")
check(b["mix"]["ppe"] == 60.0, "and the mix is reported so a reader can haircut it")
check(S.asset_backing()["total"] is None,
      "nothing reported is unknown, not a zero-asset company")
soft = S.asset_backing(cash=10, goodwill=990, anchor=1000)
check(soft["hard_pct"] == 1.0,
      "a company that is 99% goodwill reads as 1% hard — the number Schloss "
      "would have wanted before anything else")

# A PERCENTAGE OF A FRAGMENT IS NOT A PERCENTAGE OF THE COMPANY.
# NNN REIT's real figures: $4.2m of named asset lines against a $4.46bn
# balance sheet, reported as "100% hard assets" — and the flagship list
# was RANKED on that number, so the least-readable companies led it.
reit = S.asset_backing(cash=4_223_000, anchor=4_460_736_000)
check(reit["mix"]["cash"] == 100.0,
      "the mix still says cash was the only line filed — that is worth seeing")
check(reit["coverage"] == 0.001,
      f"but coverage records that it explains 0.1% of the company "
      f"(got {reit['coverage']})")
check(reit["hard_pct"] is None,
      "so the hard-asset share is WITHHELD rather than published as 100%")

edge = S.asset_backing(cash=500, ppe=100, anchor=1000)
check(edge["hard_pct"] == 83.3,
      f"60% coverage is enough to describe the balance sheet (got {edge['hard_pct']})")
check(S.asset_backing(cash=400, anchor=1000)["hard_pct"] is None,
      "40% is not")
check(S.asset_backing(cash=200, receivables=100, inventory=100, ppe=600)["hard_pct"] == 30.0,
      "and with no anchor at all the old behaviour stands — coverage cannot "
      "be judged, so nothing is withheld on a guess")


# ── evaluate: the price half is honestly absent ─────────────────────
FILINGS = {
    "stockholders_equity": 1000.0, "goodwill": 100.0, "intangibles": 50.0,
    "shares": 100.0, "current_assets": 800.0, "total_liabilities": 300.0,
    "short_term_debt": 50.0, "long_term_debt": 100.0,
    "div_per_share_by_year": {2022: 1.0, 2023: 1.2, 2024: 1.4},
    "sbc": 1e7, "revenue": 1e9, "shares_cagr": 0.5, "insider_pct": 15.0,
    "first_filing_year": 1996, "cash": 300.0, "receivables": 200.0,
    "inventory": 150.0, "ppe": 400.0,
}

r = S.evaluate(FILINGS, 2026)
check(r["tangible_book"] == 850.0, "tangible book computes from filings alone")
check(r["tangible_book_ps"] == 8.5, "and per share")
check(r["ncav"] == 500.0, "so does NCAV")
check(r["debt_ratio"] == 0.15, "and the debt ratio")
check(r["survival_years"] == 30, "and the operating history")
check(r["gates_pass"] is True, f"this company clears every gate (got {r['gates']})")
check(r["gates_known"] == 4, "with all four gates measured")

check(r["price_ready"] is False, "but no price arrived")
check(r["p_tangible_book"] is None and r["p_ncav"] is None,
      "so the ratios are absent")
check(r["below_book"] is None and r["is_net_net"] is None,
      "and the cheapness verdicts are UNKNOWN — never defaulted to False, "
      "which would read on the page as 'checked and not cheap'")

# Same company, with a price. The whole screen lights up.
priced = S.evaluate({**FILINGS, "price": 5.0}, 2026)
check(priced["price_ready"] is True, "a price makes the second half computable")
check(priced["p_tangible_book"] == 0.588,
      f"price/tangible book (got {priced['p_tangible_book']})")
check(priced["below_book"] is True, "5.00 against 8.50 of tangible book is a 41% discount")
check(priced["p_ncav"] == 1.0, "price/NCAV")
check(priced["is_net_net"] is False,
      "at 1.0x NCAV it is not a net-net — Graham wanted two thirds")
check(S.evaluate({**FILINGS, "price": 3.0}, 2026)["is_net_net"] is True,
      "at 0.6x NCAV it is")
check(S.evaluate({**FILINGS, "price": 8.0}, 2026)["below_book"] is False,
      "a 6% discount is not the 20% he required")

# Known-negative and unmeasured are different answers, and the earlier
# version of this collapsed them: it returned None for both.
broke = S.evaluate({**FILINGS, "stockholders_equity": 100.0,
                    "goodwill": 400.0, "price": 5.0}, 2026)
check(broke["tangible_book"] == -350.0, "tangible book can be negative")
check(broke["price_ready"] is True, "the price is still a price")
check(broke["below_book"] is False,
      "and with no tangible assets left, 'trading below tangible book' is "
      "definitively FALSE — not unknown. We checked; the answer is no")
check(broke["gates"]["assets"] is False, "the asset gate fails on the merits")

# The mirror case: no equity figure at all.
dark = S.evaluate({"first_filing_year": 1990}, 2026)
check(dark["gates"]["assets"] is None,
      "no equity figure leaves the asset gate UNKNOWN, not failed — a company "
      "we could not read is not a company we rejected")
check(dark["gates"]["debt"] is None, "same for debt")
check(dark["gates"]["pays_dividend"] is None, "same for the dividend")
check(dark["gates"]["survival"] is True, "the one thing we do know still scores")
check(dark["gates_known"] == 1, "and only that one counts as known")
check(dark["gates_pass"] is False,
      "one gate out of four is not a pass — 'all the gates we happened to "
      "manage' is how a barely-readable company ends up presented as having "
      "cleared the screen. `gates_known` carries the thinness instead")

# Absence of a dividend series is genuinely ambiguous — a non-payer files
# no dividend facts, and so does a company we failed to fetch. Only the
# caller knows which, so presence of the key is the fetcher's way of
# saying it looked.
unlooked = S.evaluate({k: v for k, v in FILINGS.items()
                       if k != "div_per_share_by_year"}, 2026)
check(unlooked["gates"]["pays_dividend"] is None,
      "no dividend key at all means nobody looked — unknown")
check(unlooked["gates_pass"] is False, "and an unknown gate is not a pass")
looked = S.evaluate({**FILINGS, "div_per_share_by_year": {}}, 2026)
check(looked["gates"]["pays_dividend"] is False,
      "an EMPTY dividend key means the fetcher searched and found nothing — "
      "that is a definite no, not an unknown")
check(looked["gates_known"] == 4, "so all four gates are measured")
check(looked["gates_pass"] is False,
      "and the non-payer fails on the merits: the dividend is a governance "
      "check here, not a yield, so skipping it is a real failure")

check(S.evaluate({}, 2026)["gates_pass"] is False,
      "an empty filing record passes nothing at all")
check(S.evaluate({}, 2026)["gates_known"] == 0, "having established nothing")

# A dividend cut does not disqualify — it is why he showed up.
cutter = S.evaluate({**FILINGS,
                     "div_per_share_by_year": {2023: 2.0, 2024: 0.6}}, 2026)
check(cutter["dividend"]["cut"] is True and cutter["dividend"]["cut_pct"] == 70.0,
      "the cut is measured")
check(cutter["gates_pass"] is True,
      "and the company still clears the gates — compounders.py caps a swollen "
      "yield as a forecast cut; here the cut has already happened and that is "
      "the entry, not the exit")


# ── census: the market temperature, honestly ────────────────────────
c = S.census([r, priced, broke, dark])
check(c["screened"] == 4, "every row is counted")
check(c["priced"] == 2, "two of four had a price")
check(c["price_coverage_pct"] == 50.0, "and coverage says so plainly")
check(c["below_book"] == 1, "one of the priced rows is below tangible book")
check(c["positive_ncav"] == 3, "three have positive net current assets")
check(c["ncav_unknown"] == 1, "and one could not be measured at all")

blind = S.census([r, dark])
check(blind["priced"] == 0, "with no prices anywhere")
check(blind["below_book"] is None and blind["net_nets"] is None,
      "the cheapness counts are None, NOT zero — 'the working capital stocks "
      "have disappeared' and 'we could not check' are the opposite readings "
      "of the same empty board")
check(blind["balance_sheet_qualifying"] == 1,
      "while the filings half still reports normally")
check(S.census([])["screened"] == 0, "an empty board does not divide by zero")
check(S.census([])["price_coverage_pct"] == 0.0, "and reports 0% coverage")


# ── the page's four lists ───────────────────────────────────────────
BOARD = {"rows": [
    {"ticker": "AAA", "gates_pass": True, "ncav": 500.0,
     "dividend": {"cut": True, "cut_pct": 40.0}, "backing": {"hard_pct": 55.0}},
    {"ticker": "BBB", "gates_pass": True, "ncav": 900.0,
     "dividend": {"cut": True, "cut_pct": 70.0}, "backing": {"hard_pct": 20.0}},
    {"ticker": "CCC", "gates_pass": True, "ncav": -100.0,
     "dividend": {"cut": False, "cut_pct": 0.0}, "backing": {"hard_pct": 90.0}},
    {"ticker": "DDD", "gates_pass": False, "ncav": 2000.0,
     "dividend": {"cut": True, "cut_pct": 99.0}, "backing": {"hard_pct": 99.0}},
    {"ticker": "EEE", "gates_pass": True, "ncav": None,
     "dividend": {"cut": None, "cut_pct": None}, "backing": {"hard_pct": None}},
]}
b = S.board(BOARD)
check([r["ticker"] for r in b["qualify"]] == ["AAA", "BBB", "CCC", "EEE"],
      f"the qualifying list is the gate result (got {[r['ticker'] for r in b['qualify']]})")
check([r["ticker"] for r in b["cuts"]] == ["BBB", "AAA"],
      f"cuts are ranked biggest first (got {[r['ticker'] for r in b['cuts']]})")
check("DDD" not in [r["ticker"] for r in b["cuts"]],
      "and a 99% cut on a company that fails the balance-sheet gates is not "
      "an opportunity — he bought cut dividends on SOUND balance sheets")
check([r["ticker"] for r in b["working"]] == ["DDD", "BBB", "AAA"],
      f"working-capital stocks rank by size of net current assets "
      f"(got {[r['ticker'] for r in b['working']]})")
check("CCC" not in [r["ticker"] for r in b["working"]],
      "negative net current assets is not a working-capital stock")
check("EEE" not in [r["ticker"] for r in b["working"]],
      "and neither is an unmeasured one — unknown must not sort as zero, "
      "or a bank with no classified balance sheet lands mid-table")
check([r["ticker"] for r in b["core"]] == ["CCC", "AAA", "BBB", "EEE"],
      f"the core list leads with the hardest asset backing "
      f"(got {[r['ticker'] for r in b['core']]})")
check(b["core"][-1]["ticker"] == "EEE",
      "and an unmeasured backing sorts LAST rather than first — the -inf "
      "guard is what stops None from winning a descending sort")

EMPTY = {"qualify": [], "cuts": [], "working": [], "core": [], "unread": []}
# ── the balance sheet must be arithmetically possible ───────────────
# Universe Pharmaceuticals arrived with $55.8bn of equity, $17.9bn of
# revenue and 563,338 shares — a nano-cap reporting in something other
# than the dollars its frame claimed, published as $99,110 of book per
# share at the top of a list.
imposs = S.evaluate({"stockholders_equity": 55_832_725_000,
                     "total_assets": 1_000_000_000, "shares": 563_338,
                     "current_assets": 900_000_000,
                     "total_liabilities": 100_000_000}, 2026)
check(imposs["implausible"] is True,
      "book value above total assets is impossible and is flagged")
check(imposs["tangible_book"] is None and imposs["tangible_book_ps"] is None,
      "and both figures are withheld rather than printed")
check(imposs["ncav"] is None,
      "along with NCAV, which came off the same suspect balance sheet")

fine = S.evaluate({"stockholders_equity": 400.0, "total_assets": 1000.0,
                   "shares": 10.0}, 2026)
check(fine["implausible"] is False, "an ordinary balance sheet is not flagged")
check(fine["tangible_book"] == 400.0, "and reports normally")
check(S.evaluate({"stockholders_equity": 400.0, "shares": 10.0}, 2026)["implausible"] is False,
      "with no total assets there is nothing to contradict, so nothing is claimed")


# ── recency is part of the cut signal ───────────────────────────────
# The first three rows of the live board were cuts from 2020, 2022 and
# 2020 — AstroNova's COVID suspension leading a list captioned "the
# overreaction has already happened".
CUTS = {"generated": "2026-08-10", "rows": [
    {"ticker": "OLD", "gates_pass": True, "ncav": 10.0, "total_assets": 100.0,
     "dividend": {"cut": True, "cut_pct": 75.0, "to_year": 2020},
     "backing": {"hard_pct": 50.0}},
    {"ticker": "NEW", "gates_pass": True, "ncav": 10.0, "total_assets": 100.0,
     "dividend": {"cut": True, "cut_pct": 30.0, "to_year": 2025},
     "backing": {"hard_pct": 50.0}},
]}
cb = S.board(CUTS)
check([r["ticker"] for r in cb["cuts"]] == ["NEW"],
      f"a six-year-old cut is history, not an entry — only the recent one "
      f"survives (got {[r['ticker'] for r in cb['cuts']]})")
check("OLD" in [r["ticker"] for r in S.board(CUTS, this_year=2021)["cuts"]],
      "the same 2020 cut read from 2021 IS recent — the window moves with "
      "the board, it is not a hard-coded year")


# ── working capital is ranked relative to the balance sheet ─────────
# Sorting by absolute NCAV returned Nvidia, Alphabet, Micron and Tesla —
# a list of the largest companies, which is what "most current assets in
# dollars" measures.
WC = {"generated": "2026-08-10", "rows": [
    {"ticker": "MEGA", "ncav": 87_000_000_000.0, "total_assets": 300_000_000_000.0,
     "gates_pass": False, "dividend": {}, "backing": {}},
    {"ticker": "SMALL", "ncav": 40_000_000.0, "total_assets": 50_000_000.0,
     "gates_pass": False, "dividend": {}, "backing": {}},
]}
wc = S.board(WC)
check([r["ticker"] for r in wc["working"]] == ["SMALL", "MEGA"],
      f"80% of the balance sheet in net current assets beats 29% of a much "
      f"bigger one (got {[r['ticker'] for r in wc['working']]})")
check(wc["working"][0]["ncav"] < wc["working"][1]["ncav"],
      "which is the opposite of the dollar ranking, and the point")

check(S.board({"rows": []}) == EMPTY,
      "an empty payload yields empty lists, not a crash")
check(S.board({}) == EMPTY, "and so does a malformed one")


# ── could-not-read is its own population, not silence ───────────────
# `dark` fails nothing: three gates were simply never filed. `looked` and
# `broke` genuinely FAIL gates. Only the first is unreadable.
u = S.board({"rows": [r, dark, broke, looked, unlooked]})["unread"]
check(dark in u, "a company with unmeasured gates and no failures is unreadable")
check(broke not in u,
      "a company whose tangible book is negative FAILED — it was measured "
      "and rejected, which is a different thing")
check(looked not in u, "and so did the confirmed non-payer")
check(r not in u, "a company that clears everything is not on this list")
check(u.index(unlooked) < u.index(dark),
      "the closest to readable sorts first — three of four gates known "
      f"before one of four (got {[x['gates_known'] for x in u]})")

# The list exists so that "we could not check" leaves a trace. Without it
# these rows appear on no list at all, which reads exactly like a market
# with nothing in it.
allrows = [r, dark, broke, looked]
lists = S.board({"rows": allrows})
shown = {id(x) for k in ("core", "working", "unread") for x in lists[k]}
for row in allrows:
    check(id(row) in shown or any(v is False for v in row["gates"].values()),
          "every row either appears somewhere or was explicitly rejected — "
          "nothing may vanish for being unmeasurable")

check(S.missing_gates(dark) == ["shareholders' equity",
                                "debt (and the liability total could not settle it)",
                                "dividend record"],
      f"the missing gates are named, not merely counted (got {S.missing_gates(dark)})")
check(S.missing_gates(r) == [], "a fully-measured row is missing nothing")

cen = S.census(allrows)
check(cen["unreadable"] == 1,
      f"the census separates unreadable from rejected (got {cen['unreadable']})")
check(cen["screened"] - cen["balance_sheet_qualifying"] - cen["unreadable"] == 2,
      "leaving the genuinely rejected as the remainder")
check(S.load({"rows": []}) == {"rows": []}, "injected data bypasses the file")
check("no data yet" in S.data_source_label({}),
      "and a missing build says so instead of implying a screened market")


# ── report ──
if _FAILS:
    print(f"FAIL — {len(_FAILS)}/{_COUNT} checks failed:")
    for m in _FAILS:
        print("  ✗", m)
    sys.exit(1)
print(f"OK — all {_COUNT} Schloss screen checks passed.")
print(f"   gates: tangible assets, debt <= {S.DEBT_TO_EQUITY_MAX}x equity, "
      f"{S.SURVIVAL_YEARS_MIN}+ years filing, pays a dividend")
print(f"   priced tests ({S.BOOK_DISCOUNT_MIN:.0%} book discount, "
      f"{S.NCAV_MAX_PRICE:.2f}x NCAV) return UNKNOWN with no feed attached")
sys.exit(0)

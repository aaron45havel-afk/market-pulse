"""Special-situation triage checks.

Run:  python tests/test_catalysts.py      (exit 0 = all pass)

The properties here decide whether a queue is worth reading. Four matter
more than the rest, and each one is a mistake that hand analysis makes:

  * ANNUALIZED, not headline. A small spread closing soon must outrank a
    large spread resolving in years.
  * The round-trip bid-ask comes off BEFORE ranking. In illiquid names —
    which is where these situations live — it routinely eats the whole
    gap.
  * Tax applies at the holding period. A 60-day event is ordinary income,
    and any ranking that ignores that over-rates short deals structurally.
  * Dollars at stake sit next to the percentage. An odd-lot tender can
    annualize beautifully and be worth $200.
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import catalysts as C

_COUNT = 0
_FAILS = []


def check(cond, msg):
    global _COUNT
    _COUNT += 1
    if not cond:
        _FAILS.append(msg)


# ── annualizing is the whole ranking ────────────────────────────────
small_soon = C.annualized(3.0, 30)
big_slow = C.annualized(20.0, 1095)
check(small_soon > 40, f"3% in 30 days annualizes above 40% (got {small_soon})")
check(big_slow < 7, f"20% over 3 years annualizes below 7% (got {big_slow})")
check(small_soon > big_slow * 5,
      "the small fast spread beats the big slow one by a wide margin — "
      "the ranking most people get backwards")

# Horizons that cannot be annualized honestly are refused.
check(C.annualized(2.0, 0.5) is None,
      "a sub-one-day horizon does not annualize — that number is arithmetically "
      "true and useless")
check(C.annualized(2.0, 0) is None and C.annualized(2.0, None) is None,
      "no horizon -> no annualized figure")
check(C.annualized(-150.0, 30) is None, "a more-than-total loss does not annualize")

# ── the round trip comes off first ──────────────────────────────────
# 6% gap, 4% round trip, 30 days: a 2% trade, not a 6% one.
r = C.evaluate(price=10.0, consideration=10.60, days=30, roundtrip_spread_pct=4.0, shares=1000)
check(r["gross_spread_pct"] == 6.0, "gross spread")
check(r["net_spread_pct"] == 2.0, f"net of the round trip (got {r['net_spread_pct']})")
check(r["annualized_net_pct"] < r["annualized_gross_pct"] / 2,
      "netting the spread more than halves the annualized figure here")

# And when the round trip exceeds the gap, the play is dead — said plainly.
dead = C.evaluate(price=10.0, consideration=10.30, days=30, roundtrip_spread_pct=5.0, shares=1000)
t = C.triage(dead)
check(t["verdict"] == "skip", "a spread smaller than the round trip is a skip")
check("eats the entire" in t["reason"], f"and says why (got: {t['reason']})")

# ── tax at the holding period ───────────────────────────────────────
short = C.after_tax(25.0, 60, "taxable", ordinary_rate=0.37)
long = C.after_tax(25.0, 400, "taxable", ltcg_rate=0.20)
ira = C.after_tax(25.0, 60, "ira")
check(short["net_pct"] == 15.75, f"25% gross over 60 days nets 15.75% (got {short['net_pct']})")
check(short["treatment"] == "short-term / ordinary", "labelled as ordinary income")
check(long["net_pct"] == 20.0, "the same 25% held 13 months nets 20%")
check(long["net_pct"] > short["net_pct"],
      "the tax code pays you to be slow — a ranking ignoring this over-rates short deals")
check(ira["net_pct"] == 25.0 and ira["rate"] == 0.0, "sheltered accounts pay nothing")

# A 25% gross two-month play BARELY clears a 15%-after-tax mandate.
check(15.0 <= short["net_pct"] < 16.0,
      "25% gross over 60 days is only just at a 15% after-tax bar — "
      "which is the whole argument for holding longer")

# ── the account gap is often the biggest lever available ────────────
cmp = C.account_comparison(price=10.0, consideration=11.0, days=45, shares=2000)
check(cmp["sheltered_pct"] > cmp["taxable_pct"], "the same play is worth more sheltered")
check(cmp["gap_pct"] > 20,
      f"and on a short event the gap is enormous (got {cmp['gap_pct']} pts)")
check(cmp["treatment"] == "short-term / ordinary", "because it is taxed as ordinary income")

# ── capacity: the killer nobody applies ─────────────────────────────
# $30 stock, $32 tender, odd lot: 99 x $2 = $198. However good the rate.
odd = C.evaluate(price=30.0, consideration=32.0, days=25, odd_lot=True)
check(odd["shares_assumed"] == 99, "an odd lot is capped at 99 shares")
check(abs(odd["dollars_at_stake"] - 198.0) < 0.01,
      f"which is $198 of profit (got {odd['dollars_at_stake']})")
check(odd["annualized_after_tax_pct"] > 50,
      "it annualizes spectacularly...")
verdict = C.triage(odd, target_after_tax_pct=15.0, min_dollars=2_000)
check(verdict["verdict"] == "too_small",
      "...and is still not worth the evening — flagged too_small, not read_first")
check("$198" in verdict["reason"], f"the reason names the dollars (got: {verdict['reason']})")
# The same economics at size clears easily.
size = C.evaluate(price=30.0, consideration=32.0, days=25, shares=5000)
check(C.triage(size)["verdict"] == "read_first",
      "the identical spread on 5,000 shares is worth reading first")

# ── expected value once the break price is weighed ──────────────────
# 8% up if it closes, -30% if it breaks, 75% odds -> negative EV.
risky = C.evaluate(price=10.0, consideration=10.80, days=90, shares=2000,
                   completion=0.75, downside=7.0)
check(risky["expected_value_pct"] < 0,
      f"a wide break makes this negative EV despite an 8% headline (got {risky['expected_value_pct']})")
check(C.triage(risky)["verdict"] == "skip", "and it is skipped")
check("expected value" in C.triage(risky)["reason"], "with the reason given")
# A high-confidence deal with a modest break survives.
safe = C.evaluate(price=10.0, consideration=10.80, days=90, shares=2000,
                  completion=0.95, downside=9.8)
check(safe["expected_value_pct"] > 0, "a tight break at high odds stays positive")

# ── no blended score exists ─────────────────────────────────────────
keys = set(C.evaluate(price=10.0, consideration=11.0, days=30, shares=100) or {})
check(not any(k in keys for k in ("score", "rating", "grade")),
      "there is no composite score — a blend built on a guessed probability "
      "would look like a measurement")
check("completion_assumed" in keys,
      "the assumed completion rate is exposed, not buried")

# ── form classification ─────────────────────────────────────────────
check(C.classify("SC TO-I")["label"] == "Issuer tender offer", "SC TO-I")
check(C.classify("SC 13D")["label"].startswith("Activist"), "13D is the activist one")
check(C.classify("SC 13G") is None,
      "13G is PASSIVE and must not be classified as a catalyst — the single "
      "easiest way to generate false alerts")
check(C.classify("15")["warning"] and not C.classify("15")["actionable"],
      "going dark is a warning, not an opportunity")
check(C.classify("NT 10-K")["warning"], "a late filing is a warning")
check(C.classify("DEF 14A") is None, "a routine proxy is not a catalyst")
check(C.classify("DEF 14A", "adopt the plan of liquidation and dissolution")["label"]
      .startswith("Plan of liquidation"),
      "...but one carrying a plan of liquidation is")
check(C.classify("10-K") is None and C.classify("") is None, "ordinary filings are not catalysts")
check(C.classify("SC TO-I/A")["label"] == "Issuer tender offer", "amendments classify too")
# Form 15 is filed as 15-12B / 15-12G / 15-15D, essentially never bare "15".
# Matching the exact string only meant the going-dark alarm never fired —
# the one warning that matters most if you actually hold the thing.
for variant in ("15-12B", "15-12G", "15-15D", "15"):
    hit = C.classify(variant)
    check(hit is not None and hit["warning"],
          f"Form {variant} is caught as the going-dark warning")
check(C.classify("40-APP") is None,
      "a form merely STARTING with a watched digit is not swept in")
check(C.classify("10-K") is None and C.classify("10-Q") is None,
      "routine periodic reports are not catalysts")

# Every catalyst carries a note that says what to actually check.
for key, meta in C.CATALYSTS.items():
    check(bool(meta.get("note")) and len(meta["note"]) > 20,
          f"{key} explains what to look for")
    check(0 < meta["completion"] <= 1.0, f"{key} has a sane completion prior")
check(C.CATALYSTS["SC 13E-3"]["note"].count("RE-CUT") == 1,
      "going-private warns that the first price is a negotiating position")
check(C.CATALYSTS["DEF 14A/LIQUIDATION"]["completion"] > 0.7
      and "slip" in C.CATALYSTS["DEF 14A/LIQUIDATION"]["note"],
      "liquidations complete but the timeline slips — both stated")

# ── triage thresholds are two independent bars ──────────────────────
good = C.evaluate(price=10.0, consideration=10.5, days=20, shares=4000)
check(C.triage(good, target_after_tax_pct=15.0)["verdict"] == "read_first",
      "clears both bars")
check(C.triage(good, target_after_tax_pct=200.0)["verdict"] == "skip",
      "an unreachable target rejects it on return")
check(C.triage(good, min_dollars=1e9)["verdict"] == "too_small",
      "an unreachable dollar floor rejects it on size")
check(C.triage(None)["verdict"] == "skip", "nothing to price -> skip")

# ── the daily-index parser, on a realistic fixture ──────────────────
# sec.gov is unreachable from the sandbox, so the parser is proved against
# a fixture rather than a live fetch. The shape is EDGAR's form.idx.
sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "scripts"))
import importlib.util as _u
_spec = _u.spec_from_file_location(
    "rc", os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                       "scripts", "refresh_catalysts.py"))
_rc = _u.module_from_spec(_spec)
_spec.loader.exec_module(_rc)

IDX_FIXTURE = """Description:           Daily Index of EDGAR Dissemination Feed by Form Type
Last Data Received:    August 08, 2026

Form Type   Company Name                                  CIK         Date Filed  File Name
---------------------------------------------------------------------------------------------
10-K        ACME INDUSTRIES INC                           0000012345  2026-08-08  edgar/data/12345/a.txt
SC 13D      SMALLCO HOLDINGS INC                          0000067890  2026-08-08  edgar/data/67890/b.txt
SC 13G      BIGFUND PARTNERS LP                           0000011111  2026-08-08  edgar/data/11111/c.txt
SC TO-I     TENDERCO LTD                                  0000022222  2026-08-08  edgar/data/22222/d.txt
15-12B      GOINGDARK CORP                                0000033333  2026-08-08  edgar/data/33333/e.txt
NT 10-K     LATEFILER INC                                 0000044444  2026-08-08  edgar/data/44444/f.txt
8-K         ROUTINE CORP                                  0000055555  2026-08-08  edgar/data/55555/g.txt
4           INSIDERCO INC                                 0000066666  2026-08-08  edgar/data/66666/h.txt
"""
parsed, total = _rc.parse_index(IDX_FIXTURE, "2026-08-08")
forms = {r["form"] for r in parsed}
check(total == 8, f"every data row parses, catalyst or not (got {total})")
check("SC 13D" in forms, "the activist filing is picked up")
check("SC 13G" not in forms,
      "the PASSIVE 13G is not — the single easiest source of false alerts")
check("SC TO-I" in forms, "the tender offer is picked up")
check("15-12B" in forms, "the going-dark filing is picked up")
check("NT 10-K" in forms, "the late filing is picked up")
check("4" in forms, "insider buys are picked up")
check("10-K" not in forms and "8-K" not in forms, "routine filings are ignored")
by_form = {r["form"]: r for r in parsed}
check(by_form["SC 13D"]["company"] == "SMALLCO HOLDINGS INC",
      f"multi-word company names survive parsing (got {by_form['SC 13D']['company']!r})")
check(by_form["SC 13D"]["cik"] == "0000067890", "CIK parsed")
check(by_form["SC 13D"]["filed"] == "2026-08-08", "filing date parsed")
check(by_form["SC 13D"]["url"] == "https://www.sec.gov/Archives/edgar/data/67890/b.txt",
      "the filing URL is absolute and points at EDGAR")
check(by_form["15-12B"]["warning"] and not by_form["15-12B"]["actionable"],
      "going dark is flagged as a warning, not an opportunity")

# Column WIDTHS vary between EDGAR files and over time. Splitting on runs of
# 2+ spaces has to survive that; column offsets read off a header do not.
WIDE = """Form Type        Company Name                                                         CIK              Date Filed   File Name
----------------------------------------------------------------------------------------------------------------------------
SC TO-I          A VERY LONG COMPANY NAME WITH MANY WORDS INC                         0000099999       2026-08-07   edgar/data/99999/z.txt
"""
wide, wtotal = _rc.parse_index(WIDE, "2026-08-07")
check(wtotal == 1 and len(wide) == 1, "a differently-padded file still parses")
check(wide[0]["form"] == "SC TO-I", "multi-token form survives wider padding")
check(wide[0]["company"] == "A VERY LONG COMPANY NAME WITH MANY WORDS INC",
      f"and so does a long multi-word name (got {wide[0]['company']!r})")

# No header at all — the parser must not depend on finding one.
HEADERLESS = "SC TO-I   NOHEADER CORP   0000088888  2026-08-07  edgar/data/88888/y.txt\n"
hl, htotal = _rc.parse_index(HEADERLESS, "2026-08-07")
check(htotal == 1 and hl and hl[0]["form"] == "SC TO-I",
      "rows parse with no header present — anchoring on the archive path, not a header")

# The distinction the first live run got wrong: fetched-and-unparseable is
# NOT the same as no-catalysts-today, and only one of them is a real result.
noise, ntotal = _rc.parse_index("<html><body>Request Rate Threshold Exceeded</body></html>", "2026-08-07")
check(ntotal == 0 and noise == [],
      "an HTML error page served with a 200 parses to nothing — and reports "
      "ZERO ROWS PARSED so the caller can refuse to write")
only_routine, rtotal = _rc.parse_index(
    "10-K   QUIET CORP   0000012345  2026-08-07  edgar/data/12345/q.txt\n", "2026-08-07")
check(rtotal == 1 and only_routine == [],
      "a real file with no catalysts parses rows but yields none — which is a "
      "genuine result, and must be distinguishable from the broken case above")
check(_rc.parse_index("", "2026-08-08") == ([], 0), "an empty file yields nothing, not a crash")

# ── degenerate inputs must not raise ────────────────────────────────
for kw in ({"price": 0, "consideration": 10, "days": 30},
           {"price": 10, "consideration": 0, "days": 30},
           {"price": float("nan"), "consideration": 10, "days": 30},
           {"price": float("inf"), "consideration": 10, "days": 30},
           {"price": 10, "consideration": 1e308, "days": 1},
           {"price": 10, "consideration": 11, "days": -5}):
    try:
        res = C.evaluate(**kw)
        C.triage(res)
    except Exception as e:                                   # noqa: BLE001
        _FAILS.append(f"evaluate({kw}) raised {type(e).__name__}: {e}")
    _COUNT += 1

# ── report ──
if _FAILS:
    print(f"FAIL — {len(_FAILS)}/{_COUNT} checks failed:")
    for m in _FAILS:
        print("  ✗", m)
    sys.exit(1)
print(f"OK — all {_COUNT} catalyst-triage checks passed.")
print(f"   3% in 30d = {small_soon}%/yr vs 20% over 3yr = {big_slow}%/yr")
print(f"   odd-lot tender: {odd['annualized_after_tax_pct']}%/yr on ${odd['dollars_at_stake']:,.0f} → {verdict['verdict']}")
print(f"   same 45-day play: {cmp['taxable_pct']}% taxable vs {cmp['sheltered_pct']}% in an IRA")
sys.exit(0)

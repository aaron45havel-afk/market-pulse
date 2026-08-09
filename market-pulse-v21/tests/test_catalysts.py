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

# ── Form 4: reading the document, because the index cannot ──────────
#
# Every check below is a claim the queue makes on the page. If the parser
# is wrong, the page says "insider bought" over a tax-withholding event,
# which is worse than saying nothing.

def _f4(owners_xml, txn_xml, issuer="Acme Industries, Inc.",
        cik="0000012345", sym="ACME"):
    """Wrap fragments in a realistic full .txt submission — SGML preamble
    and all — because that is what sec.gov actually serves."""
    return f"""-----BEGIN PRIVACY-ENHANCED MESSAGE-----
<SEC-DOCUMENT>0001234567-26-000001.txt : 20260806
<SEC-HEADER>ACCESSION NUMBER: 0001234567-26-000001
</SEC-HEADER>
<DOCUMENT>
<TYPE>4
<FILENAME>form4.xml
<TEXT>
<XML>
<?xml version="1.0"?>
<ownershipDocument>
  <schemaVersion>X0508</schemaVersion>
  <issuer>
    <issuerCik>{cik}</issuerCik>
    <issuerName>{issuer}</issuerName>
    <issuerTradingSymbol>{sym}</issuerTradingSymbol>
  </issuer>
{owners_xml}
  <nonDerivativeTable>
{txn_xml}
  </nonDerivativeTable>
</ownershipDocument>
</XML>
</TEXT>
</DOCUMENT>
"""


def _owner(name, cik="0000999888", officer=0, director=0, ten=0, title=""):
    return f"""  <reportingOwner>
    <reportingOwnerId>
      <rptOwnerCik>{cik}</rptOwnerCik>
      <rptOwnerName>{name}</rptOwnerName>
    </reportingOwnerId>
    <reportingOwnerRelationship>
      <isDirector>{director}</isDirector>
      <isOfficer>{officer}</isOfficer>
      <isTenPercentOwner>{ten}</isTenPercentOwner>
      <officerTitle>{title}</officerTitle>
    </reportingOwnerRelationship>
  </reportingOwner>"""


def _txn(code, shares, price, ad="A", date="2026-08-05"):
    price_xml = (f"<transactionPricePerShare><value>{price}</value></transactionPricePerShare>"
                 if price is not None else "<transactionPricePerShare/>")
    return f"""    <nonDerivativeTransaction>
      <securityTitle><value>Common Stock</value></securityTitle>
      <transactionDate><value>{date}</value></transactionDate>
      <transactionCoding>
        <transactionFormType>4</transactionFormType>
        <transactionCode>{code}</transactionCode>
        <equitySwapInvolved>0</equitySwapInvolved>
      </transactionCoding>
      <transactionAmounts>
        <transactionShares><value>{shares}</value></transactionShares>
        {price_xml}
        <transactionAcquiredDisposedCode><value>{ad}</value></transactionAcquiredDisposedCode>
      </transactionAmounts>
    </nonDerivativeTransaction>"""


# A real purchase: CEO buys 1,000 shares at $25.50.
buy_doc = C.parse_form4(_f4(_owner("Doe John A", officer=1, title="Chief Executive Officer"),
                            _txn("P", 1000, 25.50)))
check(buy_doc is not None, "a Form 4 wrapped in the SGML submission parses")
check(buy_doc["issuer"] == "Acme Industries, Inc.", "issuer name comes off the document")
check(buy_doc["issuer_cik"] == "12345", "issuer CIK is unpadded")
check(buy_doc["ticker"] == "ACME", "ticker is captured for cross-referencing")
check(buy_doc["owners"][0]["name"] == "Doe John A", "reporting owner is captured")
check("Chief Executive Officer" in buy_doc["owners"][0]["roles"],
      "the officer TITLE is the role, not the word 'Officer'")

buy = C.insider_buy(buy_doc)
check(buy is not None, "code P acquisition is recognised as a purchase")
check(buy["shares"] == 1000, f"purchase shares total 1000 (got {buy['shares']})")
check(buy["dollars"] == 25500.0, f"dollars committed = 1000 x 25.50 (got {buy['dollars']})")
check(buy["fully_priced"] is True, "a priced purchase reports as fully priced")

# ── THE WHOLE POINT: everything that is not a code-P buy must vanish ──
# These are what actually fills EDGAR every day. If any of them survives,
# the queue is noise with a caption claiming it is a purchase.
for code, label in (("A", "a grant"), ("M", "an option exercise"),
                    ("F", "tax withholding"), ("S", "a sale"),
                    ("G", "a gift"), ("C", "a conversion"),
                    ("X", "an in-the-money option exercise"),
                    ("D", "a disposition to the issuer")):
    d = C.parse_form4(_f4(_owner("Doe John A", director=1), _txn(code, 5000, 30.0,
                                                                 ad=("A" if code in "AMCX" else "D"))))
    check(C.insider_buy(d) is None, f"{label} (code {code}) is NOT reported as a purchase")

# A code-P line that DISPOSES shares is not a buy either.
p_disposed = C.parse_form4(_f4(_owner("Doe John A", director=1), _txn("P", 900, 12.0, ad="D")))
check(C.insider_buy(p_disposed) is None,
      "code P with an acquired/disposed code of D is not a purchase")

# Mixed filing: a grant AND a real purchase. Only the purchase counts, and
# the grant's shares must not be added to the dollars committed.
mixed = C.parse_form4(_f4(_owner("Roe Jane", officer=1, title="CFO"),
                          _txn("A", 50000, 0.0) + "\n" + _txn("P", 400, 10.0)))
mb = C.insider_buy(mixed)
check(mb is not None and mb["shares"] == 400,
      f"only the code-P shares count in a mixed filing (got {mb and mb['shares']})")
check(mb["dollars"] == 4000.0, f"the 50,000-share grant adds nothing (got {mb['dollars']})")

# ── a missing price is UNKNOWN, never zero ──────────────────────────
# Sorting an unpriced real purchase to the bottom of a dollar ranking as
# if it were a $0 trade hides it exactly as effectively as dropping it.
unpriced = C.parse_form4(_f4(_owner("Poe Ann", director=1), _txn("P", 750, None)))
ub = C.insider_buy(unpriced)
check(ub is not None, "a purchase with no reported price is still a purchase")
check(ub["dollars"] is None, "an unpriced purchase reports dollars as None, not 0")
check(ub["unpriced_shares"] == 750, "the unpriced shares are reported separately")
check(ub["fully_priced"] is False, "and the filing is flagged as not fully priced")

# ── derivatives are not somebody buying stock ───────────────────────
deriv = """<derivativeTable>
    <derivativeTransaction>
      <securityTitle><value>Employee Stock Option</value></securityTitle>
      <transactionCoding><transactionCode>P</transactionCode></transactionCoding>
      <transactionAmounts>
        <transactionShares><value>10000</value></transactionShares>
        <transactionPricePerShare><value>0.01</value></transactionPricePerShare>
        <transactionAcquiredDisposedCode><value>A</value></transactionAcquiredDisposedCode>
      </transactionAmounts>
    </derivativeTransaction>
  </derivativeTable>"""
dd = C.parse_form4(_f4(_owner("Doe John A", officer=1, title="CEO"), "").replace(
    "  </nonDerivativeTable>", "  </nonDerivativeTable>\n  " + deriv))
check(C.insider_buy(dd) is None,
      "a code-P DERIVATIVE line is not counted — an option at a nominal strike "
      "is not a person buying stock")

# ── roles: a 10% owner is not an insider in the useful sense ────────
ten_pct = C.parse_form4(_f4(_owner("Big Capital LP", ten=1), _txn("P", 100000, 5.0)))
tb = C.insider_buy(ten_pct)
check(tb["roles"] == ["10% owner"], f"10% owner role captured (got {tb['roles']})")
check(tb["by_insider"] is False,
      "a 10%-owner purchase is not flagged as an officer/director buy")

both = C.parse_form4(_f4(_owner("Doe John A", officer=1, director=1, title="President"),
                         _txn("P", 100, 5.0)))
check(set(C.insider_buy(both)["roles"]) == {"President", "Director"},
      "officer and director roles are both captured")

# ── malformed input must degrade, not explode ───────────────────────
for bad in ("", "not xml at all", "<html>404</html>",
            "<ownershipDocument><issuer>", _f4("", "")):
    try:
        d = C.parse_form4(bad)
        C.insider_buy(d)
    except Exception as e:                                   # noqa: BLE001
        _FAILS.append(f"parse_form4 raised {type(e).__name__} on {bad[:30]!r}: {e}")
    _COUNT += 1
check(C.parse_form4("<html>Rate limit exceeded</html>") is None,
      "an error page served as 200 parses to None, not to a fake filing")
check(C.insider_buy(None) is None, "insider_buy(None) is None, not a crash")

# ── Form 15 is three different events ───────────────────────────────
# 12(b) is the exchange registration — withdrawing it is the delisting.
# 12(g) and 15(d) frequently cover a secondary class, so calling them
# "going dark" cries wolf on an NYSE company whose common is untouched.
c12b = C.classify("15-12B")
c12g = C.classify("15-12G")
c15d = C.classify("15-15D")
check(c12b["label"] != c12g["label"],
      "15-12B and 15-12G do not share a label — they are different events")
check("exchange" in c12b["label"].lower(),
      f"15-12B names the exchange deregistration (got {c12b['label']!r})")
check("secondary" in c12g["note"].lower(),
      "15-12G warns that it is often a secondary class, not the listed common")
check(c15d is not None and "15(d)" in c15d["label"],
      "15-15D is classified on its own terms")
check(C.classify("15-12B/A")["label"] == c12b["label"],
      "an amended 15-12B keeps the specific classification")
check(C.classify("15F-12B") is not None,
      "a foreign private issuer's Form 15F is classified, not dropped")
check(C.classify("15") is not None, "a bare Form 15 still classifies")

# The generic key must not swallow the specific ones. This is the bug that
# a shortest-match scan reintroduces silently.
check(C.classify("15-12G")["label"] != C.classify("15")["label"],
      "the generic '15' entry does not shadow the specific variants")
check(C.classify("SC TO-I")["label"] != C.classify("SC TO-T")["label"],
      "issuer and third-party tenders stay distinct")
check(C.classify("10-12B")["label"].lower().startswith("spin"),
      "10-12B is a spin-off, not a Form 15 variant")

# ── scheduled fund repurchases are a calendar, not an event ─────────
for name in ("Ares Private Markets Fund", "Apollo Debt Solutions BDC",
             "AMG Pantheon Master Fund, LLC", "Stellus Private Credit BDC",
             "Hedge Fund Guided Portfolio Solution",
             "GCM GROSVENOR CORE ABSOLUTE RETURN FUND II, LLC"):
    check(C.is_routine_repurchase("SC TO-I", name) is True,
          f"{name!r} is a scheduled at-NAV repurchase, not a tender to read")
    check(C.is_routine_repurchase("SC TO-I/A", name) is True,
          f"{name!r} amendment is routine too")

for name in ("AVANOS MEDICAL, INC.", "Presidio Property Trust, Inc.",
             "Forte Biosciences, Inc.", "ARGENX SE", "Identiv, Inc.",
             "Fundamental Industries Corp", "Refund Technologies Inc"):
    check(C.is_routine_repurchase("SC TO-I", name) is False,
          f"{name!r} is a real issuer tender — must NOT be set aside")

# Word boundaries: "Fundamental" and "Refund" contain 'fund' as a substring
# and are ordinary companies. A naive substring match buries them.
check(C.is_routine_repurchase("SC TO-I", "Fundamental Global Inc") is False,
      "'Fundamental' is not a fund — substring matching would hide it")

# Scoped to SC TO-I on purpose: a fund in a MERGER vote is a real event.
check(C.is_routine_repurchase("DEFM14A", "Ares Private Markets Fund") is False,
      "a fund's merger vote is a real event, not a scheduled repurchase")
check(C.is_routine_repurchase("SC TO-T", "Ares Private Markets Fund") is False,
      "a THIRD-PARTY tender for a fund is a real event")
check(C.is_routine_repurchase("SC TO-I", "") is False, "empty company name is not routine")


# ── the arithmetic has to describe a trade a human could make ───────
#
# The first live board led with "Reborn Coffee, Inc. — $23,649,660,000":
# 131,387 shares at exactly $180,000.00 each, in a stock trading near a
# dollar. The filer had put the AGGREGATE value in the per-share field.
# Nothing in the XML is malformed, so no amount of parser strictness
# catches it — the only defence is checking the result, and the only
# honest response is to mark it rather than delete it.
real = C.implausible(131387, 23_649_660_000.0)
check(real is not None, "the Reborn Coffee row is caught as implausible")
check("180,000.00" in real, f"the reason quotes the implied price (got {real!r})")
check("per-share" in real, "and names the likely cause, so the reader knows what to look for")

# The rest of that same board must survive untouched. These are its real
# extremes: the median was $15.25 and the highest sane row $370.79.
for shares, dollars, label in ((3_855_275, 69_394_950.0, "Braveheart at $18.00"),
                               (105_631, 4_999_959.0, "Aptiv at $47.33"),
                               (400, 49_596.0, "Granite at $123.99"),
                               (3_181, 251_170.0, "Navios at $78.96"),
                               (60_000, 23_814.0, "AppTech at $0.40"),
                               (1_000, 106_075.0, "Entergy at $106.08")):
    check(C.implausible(shares, dollars) is None, f"{label} is a normal purchase")

# BRK.A is the only US common above the bound, and being asked to check it
# is the correct outcome — the check says "verify", not "discard".
check(C.implausible(10, 7_000_000.0) is not None,
      "a $700,000-a-share name is flagged for checking rather than trusted")
check(C.implausible(100, 999_000.0) is None,
      "$9,990 a share is under the bound and passes")

# Absurd totals are caught even when the per-share price looks ordinary.
check(C.implausible(500_000_000, 10_000_000_000.0) is not None,
      "$10bn from one insider is flagged even at a plausible $20 a share")

# Unknowns are handled elsewhere and must not be reported as implausible.
for s, d in ((None, 100.0), (100, None), (0, 100.0), (100, 0.0), (None, None)):
    check(C.implausible(s, d) is None,
          f"implausible({s}, {d}) is None — missing is not the same as wrong")

# ── the flag rides on the purchase itself ───────────────────────────
absurd = C.parse_form4(_f4(_owner("Lim Jung Jae", officer=1, title="CEO"),
                           _txn("P", 131387, 180000.0)))
ab = C.insider_buy(absurd)
check(ab["suspect"] is not None, "insider_buy carries the suspicion forward")
check(ab["price_per_share"] == 180000.0,
      f"and reports the per-share figure that gives it away (got {ab['price_per_share']})")
check(ab["dollars"] == 23_649_660_000.0,
      "the total is reproduced as filed, not silently corrected — we do not "
      "know what the filer meant, only that it cannot be what it says")

sane = C.insider_buy(C.parse_form4(_f4(_owner("Doe John A", director=1),
                                       _txn("P", 1000, 25.50))))
check(sane["suspect"] is None, "an ordinary purchase is not flagged")
check(sane["price_per_share"] == 25.5, "and still reports its price per share")

# Per-share is computed on the PRICED shares only. Mixing in unpriced ones
# would understate the price and could mask a bad row.
part = C.insider_buy(C.parse_form4(_f4(_owner("Roe Jane", director=1),
                                       _txn("P", 100, 10.0) + "\n" + _txn("P", 900, None))))
check(part["price_per_share"] == 10.0,
      f"per-share divides by priced shares only (got {part['price_per_share']})")
check(part["shares"] == 1000 and part["unpriced_shares"] == 900,
      "while the share counts still report the whole purchase")

# ── an issuer with no symbol has no symbol ──────────────────────────
for placeholder in ("NONE", "N/A", "NA", "NULL", "-"):
    d = C.parse_form4(_f4(_owner("Doe John A", director=1), _txn("P", 10, 1.0),
                          sym=placeholder))
    check(d["ticker"] == "",
          f"issuerTradingSymbol {placeholder!r} becomes empty, not a ticker "
          f"reading {placeholder!r} (got {d['ticker']!r})")
d = C.parse_form4(_f4(_owner("Doe John A", director=1), _txn("P", 10, 1.0), sym="nvt"))
check(d["ticker"] == "NVT", "a real symbol is kept and upper-cased")

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

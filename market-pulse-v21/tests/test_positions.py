"""Broker position exports → /capital holdings (positions.py): Schwab's
one-account and all-accounts exports, Fidelity's and Vanguard's by their
columns, the check against the broker's total, margin as a debt, sync blocks
written, replaced and kept, the routes and the page.

Every fixture is made up — tickers, values and account numbers are synthetic,
laid out the way each broker lays out its file.

Run: python tests/test_positions.py
"""
from __future__ import annotations

import asyncio
import os
import re
import sys
from datetime import date
from types import SimpleNamespace

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

import allocation as A  # noqa: E402
import capital as K  # noqa: E402
import positions as P  # noqa: E402

_FAILS: list[str] = []
_COUNT = 0


def check(cond, msg):
    global _COUNT
    _COUNT += 1
    if not cond:
        _FAILS.append(msg)


def near(a, b, tol=0.01):
    return a is not None and b is not None and abs(a - b) <= tol


def raises(fn, *a, **k):
    try:
        fn(*a, **k)
    except P.CannotImport as e:
        return str(e)
    return None


# ── fixtures, in each broker's layout ──────────────────────────────
SCHWAB_HEAD = ('"Symbol","Description","Gain % (Gain/Loss %)","Gain $ (Gain/Loss $)","Price","Cost/Share",'
               '"Mkt Val (Market Value)","Div Yld (Dividend Yield)","Div Pay Date","Price Chng % (Price Change %)",'
               '"Price Chng $ (Price Change $)","Day Chng % (Day Change %)","Day Chng $ (Day Change $)","Cost Basis",'
               '"% of Acct (% of Account)","P/E Ratio (Price/Earnings Ratio)","Qty (Quantity)","Asset Type",')


def srow(sym, desc, val, basis, qty, asset):
    cells = [sym, desc, "1%", "$1.00", "10.00", "$9.00", val, "N/A", "N/A", "0%", "0.00", "0%", "$0.00", basis,
             "1%", "N/A", qty, asset]
    return ",".join(f'"{c}"' for c in cells) + ","


SCHWAB_ONE = "\n".join([
    '"Positions for account Individual ...901 as of 04:05 PM ET, 2026/09/30"', "", SCHWAB_HEAD,
    srow("AAA", "AAA CORP", "$1,000.00", "$800.00", "10", "Equity"),
    srow("BBB", "BBB SECTOR ETF", "$2,500.50", "$3,000.00", "1,250.25", "ETFs & Closed End Funds"),
    srow("SWVXX", "SCHWAB VALUE ADVANTAGE MONEY FUND", "$1,200.00", "$1,200.00", "1,200", "Cash and Money Market"),
    srow("Cash & Cash Investments", "--", "-$500.00", "--", "--", "Cash and Money Market"),
    srow("Positions Total", "", "$4,200.50", "$5,000.00", "--", "--"),
])
SCHWAB_ALL = "\n".join([
    '"Positions for All-Accounts as of 09:15 AM ET, 2026/09/30"', "",
    '"Individual ...111"', SCHWAB_HEAD,
    srow("CCC", "CCC INC", "$5,000.00", "$4,000.00", "50", "Equity"),
    srow("BRK/B", "BERKSHIRE HATHAWAY CL B", "$900.00", "$700.00", "2", "Equity"),
    srow("Cash & Cash Investments", "--", "$250.00", "--", "--", "Cash and Money Market"),
    srow("Account Total", "", "$6,150.00", "--", "--", "--"),
    "",
    '"Roth Contributory IRA ...222"', SCHWAB_HEAD,
    srow("DDD", "DDD GROWTH ETF", "$3,000.00", "$2,000.00", "30", "ETFs & Closed End Funds"),
    srow("DDD 12/18/2026 50.00 C", "CALL DDD GROWTH ETF", "$120.00", "$100.00", "1", "Option"),
    srow("912797KX4", "US TREASURY BILL", "$990.00", "$980.00", "1,000", "Fixed Income"),
    srow("Cash & Cash Investments", "--", "$40.00", "--", "--", "Cash and Money Market"),
    srow("Account Total", "", "$4,150.00", "--", "--", "--"),
])
FID_HEAD = ("Account Number,Account Name,Symbol,Description,Quantity,Last Price,Last Price Change,Current Value,"
            "Today's Gain/Loss Dollar,Today's Gain/Loss Percent,Total Gain/Loss Dollar,Total Gain/Loss Percent,"
            "Percent Of Account,Cost Basis Total,Average Cost Basis,Type")
FIDELITY = "\n".join([
    FID_HEAD,
    "Z00000123,Individual,SPAXX**,HELD IN MONEY MARKET,,,,$1500.00,,,,,47.24%,,,Cash,",
    "Z00000123,Individual,EEE,EEE CORP,10,$100.00,+$1.00,$1000.00,+$10.00,+1.00%,+$100.00,+11.11%,31.50%,$900.00,$90.00,Cash,",
    "Z00000123,Individual,EEE,EEE CORP,5,$100.00,+$1.00,$500.00,+$5.00,+1.00%,+$50.00,+11.11%,15.75%,$450.00,$90.00,Margin,",
    "Z00000123,Individual,Pending Activity,,,,,-$25.00,,,,,,,,,",
    "Z00000123,Individual, -EEE261218C50,EEE DEC 18 2026 $50 CALL,1,$2.00,,$200.00,,,,,6.30%,$150.00,$150.00,Margin,",
    "234000456,ROTH IRA,FXAIX,FIDELITY 500 INDEX FUND,20,$200.00,+$1.00,$4000.00,,,,,97.56%,$3000.00,$150.00,Cash,",
    "234000456,ROTH IRA,FZFXX**,HELD IN MONEY MARKET,,,,$100.00,,,,,2.44%,,,Cash,",
    "Y00000789,HEALTH SAVINGS ACCOUNT,VTI,VANGUARD TOTAL STOCK MKT ETF,8,$250.00,,$2000.00,,,,,100%,$1500.00,$187.50,Cash,",
    "", "",
    '"The data and information in this spreadsheet is provided to you solely for your use and is not for distribution."',
    '"Date downloaded Oct-03-2026 4:10 p.m ET"',
])
# Fidelity's download follows the view on screen: this one (lower-case headers,
# income columns, no cost basis, no totals) is a 401(k) brokerage window.
FID_VIEW = "\n".join([
    "Account number,Account name,Symbol,Description,Quantity,Last price,Last price change,Current value,"
    "Percent of account,Ex-date,Amount per share,Pay date,Dist. rate,Distribution rate as of,SEC yield,"
    "SEC yield as of,Est. annual income,Type",
    "900000555,BrokerageLink,GGG,GGG HOLDINGS SPON ADS,100,$60.00,+$0.10,$6000.00,60.07%,--,--,--,--,--,--,--,--,Cash,",
    "900000555,BrokerageLink,HHH,HHH CORP,40,$100.00,$0.00,$4000.00,40.05%,--,--,--,--,--,--,--,--,Cash,",
    "900000555,BrokerageLink,Pending activity,,,,,-$12.00,,,,,,,,,,,",
    "",
    '"The data and information in this spreadsheet is provided to you solely for your use."',
    "",
    '"Date downloaded Oct-03-2026 2:20 p.m ET"',
])
VANGUARD = "\n".join([
    "Account Number,Investment Name,Symbol,Shares,Share Price,Total Value,",
    "11112222,VANGUARD TOTAL STOCK MARKET ETF,VTI,10,250.00,2500.00,",
    "11112222,VANGUARD FEDERAL MONEY MARKET FUND,VMFXX,300,1.00,300.00,",
    "33334444,VANGUARD 500 INDEX ADMIRAL,VFIAX,5,500.00,2500.00,",
    "", "", "",
    "Account Number,Trade Date,Settlement Date,Transaction Type,Transaction Description,Investment Name,Symbol,"
    "Shares,Share Price,Principal Amount,Commission Fees,Net Amount,Accrued Interest,Account Type,",
    "11112222,2026-09-01,2026-09-02,Buy,Buy,VANGUARD TOTAL STOCK MARKET ETF,VTI,1,250,-250,0,-250,0,CASH,",
])

# ── reading Schwab's one-account export ────────────────────────────
s1 = P.parse_export(SCHWAB_ONE, "Individual-Positions-2026-09-30-160500.csv")
a = s1["accounts"][0]
check(s1["broker"] == "Schwab" and s1["as_of"] == "2026-09-30 16:05 ET" and len(s1["accounts"]) == 1,
      "Schwab: the broker and the as-of time from the title line (4:05 PM → 16:05)")
check(a["key"] == "schwab-901" and a["label"] == "Individual …901" and a["account"] == "taxable" and a["sure"],
      "the account keyed by its last digits only, an Individual account is taxable")
check([p["symbol"] for p in a["positions"]] == ["AAA", "BBB", "SWVXX"]
      and [p["kind"] for p in a["positions"]] == ["security", "security", "mmf"],
      "stocks and ETFs are positions; a money fund is a money fund; the cash and total rows are not positions")
check(near(a["positions"][1]["value"], 2500.50) and near(a["positions"][1]["basis"], 3000.0)
      and near(a["positions"][1]["qty"], 1250.25), "'$2,500.50', '$3,000.00', '1,250.25' read as numbers")
check(a["cash"] == 0 and near(a["margin"], 500.0), "NEGATIVE CASH is a margin loan, not negative cash")
check(a["check"] == "ok" and near(a["value"], 4200.50) and a["check_diff"] == 0,
      "positions + cash − margin match Schwab's 'Positions Total' to the cent")
off = P.parse_export(SCHWAB_ONE.replace('"$4,200.50"', '"$4,300.50"'))
check(off["accounts"][0]["check"] == "off" and near(off["accounts"][0]["check_diff"], 100.0),
      "a total that does not add up is flagged with the gap")

# ── Schwab's all-accounts export ───────────────────────────────────
s2 = P.parse_export(SCHWAB_ALL)
ind, roth = s2["accounts"]
check([x["key"] for x in s2["accounts"]] == ["schwab-111", "schwab-222"] and s2["as_of"] == "2026-09-30 09:15 ET",
      "ALL ACCOUNTS: one account per section, each from its label line")
check(ind["account"] == "taxable" and roth["account"] == "roth" and roth["label"] == "Roth Contributory IRA …222",
      "a Roth Contributory IRA is a Roth, not a traditional IRA")
check([p["symbol"] for p in ind["positions"]] == ["CCC", "BRK.B"] and near(ind["cash"], 250.0)
      and ind["check"] == "ok", "BRK/B is read as BRK.B; positive cash is cash; each section checks against its own total")
check([p["symbol"] for p in roth["positions"]] == ["DDD"]
      and [s["why"].split(" —")[0] for s in roth["skipped"]] == ["an option", "a bond or CD"]
      and roth["check"] == "ok", "an option and a T-bill are named and left out — and still counted in the check")

# ── Fidelity, by its columns ───────────────────────────────────────
f = P.parse_export(FIDELITY, "Portfolio_Positions_Oct-03-2026.csv")
fa = {x["key"]: x for x in f["accounts"]}
check(f["broker"] == "Fidelity" and f["as_of"] == "2026-10-03 16:10 ET" and list(fa) == ["fidelity-0123", "fidelity-0456", "fidelity-0789"],
      "Fidelity: accounts from the Account Number column (last four kept), the date from the footer")
check([fa[k]["account"] for k in fa] == ["taxable", "roth", "hsa"], "Individual, ROTH IRA, HEALTH SAVINGS ACCOUNT")
e = next(p for p in fa["fidelity-0123"]["positions"] if p["symbol"] == "EEE")
check(near(e["value"], 1500.0) and near(e["basis"], 1350.0) and near(e["qty"], 15),
      "the same stock on two lines (cash and margin lots) is one position")
check(next(p for p in fa["fidelity-0123"]["positions"] if p["symbol"] == "SPAXX")["kind"] == "mmf"
      and fa["fidelity-0123"]["skipped"][0]["why"] == "not settled yet",
      "SPAXX** is the core money fund; Pending Activity is left out")
check([s["why"].split(" —")[0] for s in fa["fidelity-0123"]["skipped"]] == ["not settled yet", "an option"],
      "an option is known by its symbol when the file has no asset type")
check("FZFXX money fund, roth, 100, 4.2%" in P.block_lines(fa["fidelity-0456"], None, {"rf_rate": 4.2}),
      "a money fund inside a Roth stays in the Roth, at the T-bill rate")
check([(x["check"], x["check_from"]) for x in f["accounts"]] == [("ok", "percent")] * 3,
      "Fidelity gives no per-account total: each account is checked against what its percentages imply")
check("Z00000123" not in str(f) and "234000456" not in str(f), "a full account number is never kept")

fv = P.parse_export(FID_VIEW, "Portfolio_Positions_Oct-03-2026.csv")
w = fv["accounts"][0]
check(fv["as_of"] == "2026-10-03 14:20 ET" and w["key"] == "fidelity-0555" and w["label"] == "BrokerageLink …0555",
      "Fidelity's other view: lower-case headers read; the time from 'Oct-03-2026 2:20 p.m ET'")
check(w["account"] == "k401" and not w["sure"] and w["window"],
      "BROKERAGELINK is a window inside a 401(k) — pre-tax or Roth it does not say, so the owner is asked")
check(w["check_from"] == "percent" and w["check"] == "ok" and near(w["reported"], 9988.35)
      and all(p["basis"] is None for p in w["positions"]),
      "no totals row: the '% of account' column implies the total (pending counted, as Fidelity counts it)")
w_off = P.parse_export(FID_VIEW.replace("$4000.00,40.05%", "$3000.00,40.05%"))["accounts"][0]
check(w_off["check"] == "off", "a line missing from the file shows up against the implied total")
check(P.account_type("Roth BrokerageLink") == ("roth401k", True) and P.is_window("Schwab PCRA")
      and not P.is_window("Individual"), "a Roth window says so; Schwab's PCRA is a window too")

# ── Vanguard, by its columns ───────────────────────────────────────
v = P.parse_export(VANGUARD, "OfxDownload.csv")
check(v["broker"] == "Vanguard" and [x["key"] for x in v["accounts"]] == ["vanguard-2222", "vanguard-4444"],
      "Vanguard: accounts by number")
check([p["symbol"] for p in v["accounts"][0]["positions"]] == ["VTI", "VMFXX"]
      and v["accounts"][0]["positions"][1]["kind"] == "mmf",
      "its trades table further down the file is not read as positions")
check(not v["accounts"][0]["skipped"] and not v["accounts"][1]["skipped"], "nor as lines it could not read")
check(not v["accounts"][0]["sure"] and v["accounts"][0]["positions"][0]["basis"] is None,
      "Vanguard does not name the kind of account (the owner chooses it) or give a cost basis")

# ── refused before anything is written ─────────────────────────────
check("empty" in raises(P.parse_export, "  \n "), "an empty file")
check("No positions" in raises(P.parse_export, "just,some\nrandom,csv\n"), "a CSV that is not a positions export")
check("1 MB" in raises(P.parse_export, "x" * 1_000_001), "a file far too big to be one")
check(P._money("($12.50)") == -12.5 and P._money("N/A") is None and P._money("--") is None and P._money("+$3") == 3,
      "money: parentheses are negative; N/A and -- are nothing")
check(P.account_type("ROLLOVER IRA") == ("ira", True) and P.account_type("BrokerageLink Roth") == ("roth401k", True)
      and P.account_type("401(K) PLAN") == ("k401", True) and P.account_type("Joint Tenant") == ("taxable", True)
      and P.account_type("College fund") == ("taxable", False),
      "account names: rollover IRA, a Roth BrokerageLink, a 401(k), joint — and an unknown one is flagged")
check(P.mask_of("Individual ...456") == "456" and P.mask_of("Smith-Ira") is None and P.mask_of("XXXX-1234") == "1234",
      "a mask is digits the broker shows, not a word after a dash")

# ── the block written into the holdings ────────────────────────────
prof0 = {"rf_rate": 4.2}
blk = P.block_lines(a, s1["as_of"], prof0)
check(blk[0] == "# sync schwab-901 | Schwab · Individual …901 | as of 2026-09-30 16:05 ET"
      and blk[-1] == "# end sync schwab-901", "the block is marked with its key, label and as-of time")
rows, bad = A.parse_holdings("\n".join(blk))
by = {r["name"]: r for r in rows}
check(not bad and len(rows) == 3, "every line in the block reads back")
check(by["BBB"]["account"] == "taxable" and near(by["BBB"]["basis"], 3000.0) and by["BBB"]["ticker"] == "BBB",
      "a taxable position keeps its cost basis (the switch test needs it) and is read as its ticker")
check(by["SWVXX"]["account"] == "cash" and near(by["SWVXX"]["rate"], 4.2),
      "a money fund in a taxable account is cash, at the board's T-bill rate")
rb = P.block_lines(roth, "x", prof0)
rrows = {r["name"]: r for r in A.parse_holdings("\n".join(rb))[0]}
check(rrows["DDD"]["account"] == "roth" and rrows["DDD"]["basis"] is None
      and rrows["Cash in Schwab …222"]["account"] == "roth" and rrows["Cash in Schwab …222"]["rate"] == 0,
      "in a Roth: no cost basis, and its idle cash stays in the Roth at 0%")
irows = {r["name"]: r for r in A.parse_holdings("\n".join(P.block_lines(ind, "x", prof0)))[0]}
check(irows["Cash at Schwab …111"]["account"] == "cash" and irows["Cash at Schwab …111"]["rate"] == 0,
      "idle cash in a taxable account is cash on hand, at 0% until the owner says more")

# ── merging: replace in place, keep what was typed ─────────────────
typed = "Checking, cash, 20000, 4%\nAAA, taxable, 900, 700\n# my note\nPlan fund, 401k, 50000"
m1 = P.merge(typed, {a["key"]: blk}, remove=["AAA, taxable, 900, 700"])
check(m1.startswith("Checking, cash, 20000, 4%\n# my note\nPlan fund, 401k, 50000\n\n# sync schwab-901"),
      "FIRST SYNC: typed lines and notes stay; the duplicate the owner ticked goes; the block is added at the end")
newer = dict(a, positions=[dict(a["positions"][0], value=1100.0)], margin=0.0)
m2 = P.merge(m1 + "\nVTI, roth, 3000", {a["key"]: P.block_lines(newer, "2026-10-03", prof0)})
check(m2.count("# sync schwab-901") == 1 and "AAA, taxable, 1100, 800" in m2 and "BBB" not in m2
      and m2.endswith("VTI, roth, 3000") and "as of 2026-10-03" in m2,
      "RE-SYNC: the block is replaced where it sits — sold positions go, typed lines after it stay")
m3 = P.merge(m2, {"schwab-222": rb}, remove=["AAA, taxable, 1100, 800"])
check("AAA, taxable, 1100, 800" in m3 and m3.count("# sync") == 2,
      "a line inside a synced block is never removed as a 'typed' duplicate")
check("lines" in raises(P.merge, "", {"k": ["# sync k | x"] + ["AAA, taxable, 1"] * 220 + ["# end sync k"]}),
      "a sync that would push the holdings past what the page reads is refused")
check(P.blocks_in("# sync k | x\nAAA, taxable, 1")[0]["end"] == 1, "a block missing its end marker runs to the end")

over = P.overlaps(typed + "\nSchwab cash, cash, 50\nCCC, roth, 10", s1["accounts"])
check([(o["name"], o["duplicate"]) for o in over] == [("AAA", True), ("Schwab cash", False)],
      "OVERLAPS: a typed AAA in taxable is the same holding (ticked); a typed 'Schwab cash' is offered; "
      "checking, the 401(k) and other account types are not")

# ── applying: the profile, margin as a debt ────────────────────────
saved = {"holdings": typed, "debts": [{"name": "Card", "balance": 1000, "apr": 20}], "rf_rate": 4.2}
check("rate" in raises(P.apply_import, saved, s1, {}), "a margin loan needs its rate before the sync")
prof, title = P.apply_import(saved, s1, {"margin_apr": {"schwab-901": 11.3}, "remove": ["AAA, taxable, 900, 700"]})
check(prof["debts"][-1] == {"name": "Schwab margin …901", "balance": 500.0, "apr": 11.3} and prof["debts"][0]["name"] == "Card",
      "the margin loan becomes a debt at the owner's rate; other debts stay")
check(title == "Synced Schwab Individual …901 — 3 positions" and "AAA, taxable, 900" not in prof["holdings"]
      and saved["holdings"] == typed, "named on the banner; the saved profile itself is not changed in place")
again, _ = P.apply_import(prof, P.parse_export(SCHWAB_ONE.replace('"-$500.00"', '"$10.00"')
                                               .replace('"$4,200.50"', '"$4,710.50"')), {})
check([d["name"] for d in again["debts"]] == ["Card"] and "Cash at Schwab …901, cash, 10, 0%" in again["holdings"],
      "margin paid off at the broker: the next sync removes the debt")
b2, _ = P.apply_import({}, s2, {"accounts": {"schwab-111": {"include": False}, "schwab-222": {"account": "ira"}}})
check("schwab-111" not in b2["holdings"] and "DDD, ira, 3000" in b2["holdings"],
      "an account left out is not written; the owner's choice of account kind wins")
check("choose" in raises(P.apply_import, {}, s2, {"accounts": {"schwab-222": {"account": "cash"}}}),
      "a brokerage account cannot be called checking")
check("No account" in raises(P.apply_import, {}, s1, {"accounts": {"schwab-901": {"include": False}}}),
      "nothing chosen: nothing to do")

# ── what the dashboard reads ───────────────────────────────────────
import capital_view as V  # noqa: E402
OCT = date(2026, 10, 3)
p = K.profile_with_defaults({**prof, "monthly_invest": 1000, "monthly_expenses": 3000})
sy = V.synced(p, OCT)
check(len(sy) == 1 and sy[0]["label"] == "Schwab · Individual …901" and sy[0]["age"] == 3
      and near(sy[0]["value"], 4700.50) and [r["name"] for r in sy[0]["rows"]] == ["BBB", "SWVXX", "AAA"],
      "the synced group: its label, how old the export is, its lines and their total")
typed_rows, typed_bad = V._typed_holdings(p)
check([r["name"] for r in typed_rows] == ["Checking", "Plan fund"] and not typed_bad,
      "the row editor gets the typed lines only — synced lines are shown, not edited")
hold_step = {s["key"]: s for s in V.setup(p, OCT)}["hold"]
check("Schwab · Individual …901 synced 3 days ago" in hold_step["summary"] and not any("fresh" in t for t in hold_step["todo"]),
      "the holdings step says what is synced and when")
late = {s["key"]: s for s in V.setup(p, date(2026, 12, 1))}["hold"]
check("a fresh export of Schwab · Individual …901" in late["todo"], "an export over 30 days old asks for a fresh one")
d = K.profile_with_defaults(prof)
check(sy[0]["rows"][0]["account_label"] == "Taxable" and sy[0]["rows"][1]["account_label"] == "Cash",
      "the synced lines name their accounts as the page does")
debts_step = {s["key"]: s for s in V.setup(p, OCT)}["debts"]
check(debts_step["todo"] == ["the monthly payment on Card"],
      "a margin loan is not asked for a monthly payment (it has none)")
check(near(d["_net_worth"], 20000 + 50000 + 4700.50 - 1000 - 500),
      "net worth counts the positions and subtracts the margin loan")

# ── the routes ─────────────────────────────────────────────────────
import database  # noqa: E402
import main  # noqa: E402


class _Req(SimpleNamespace):
    async def json(self):
        if isinstance(self.body, Exception):
            raise self.body
        return self.body


STORE: dict = {}
saved_fns = (main._check_admin_token, database.get_capital_profile, database.save_capital_profile)
main._check_admin_token = lambda request: True
database.get_capital_profile = lambda owner="owner": ({**STORE[owner], "_updated_at": "x"} if owner in STORE else None)
database.save_capital_profile = lambda prof, owner="owner": STORE.update({owner: {k: v for k, v in prof.items()
                                                                                  if not k.startswith("_")}}) or True
try:
    STORE["owner"] = {"holdings": typed, "debts": [{"name": "Schwab margin …901", "balance": 400, "apr": 9.9}]}
    pv = asyncio.run(main.capital_import_preview(_Req(body={"csv": SCHWAB_ONE, "filename": "x.csv"})))
    j = __import__("json").loads(pv.body)
    check(pv.status_code == 200 and j["accounts"][0]["margin_apr"] == 9.9 and j["accounts"][0]["synced_before"] is None
          and j["overlaps"][0]["duplicate"] and STORE["owner"]["holdings"] == typed,
          "PREVIEW: what would be written, the margin rate already known, the duplicate found — nothing saved")
    ap = asyncio.run(main.capital_import_apply(_Req(body={"csv": SCHWAB_ONE, "margin_apr": {"schwab-901": 12},
                                                          "remove": ["AAA, taxable, 900, 700"]})))
    check(ap.status_code == 200 and "# sync schwab-901" in STORE["owner"]["holdings"]
          and STORE["owner:undo"]["holdings"] == typed
          and STORE["owner"]["last_step"]["title"].startswith("Synced Schwab")
          and STORE["owner"]["debts"] == [{"name": "Schwab margin …901", "balance": 500.0, "apr": 12.0}],
          "APPLY: synced, the profile before it kept for undo, named on the banner, the margin debt updated")
    pv2 = __import__("json").loads(asyncio.run(main.capital_import_preview(_Req(body={"csv": SCHWAB_ONE}))).body)
    check(pv2["accounts"][0]["synced_before"]["as_of"] == "2026-09-30 16:05 ET", "a second preview says what it replaces")
    nomar = asyncio.run(main.capital_import_apply(_Req(body={"csv": SCHWAB_ONE})))
    check(nomar.status_code == 400 and "rate" in __import__("json").loads(nomar.body)["error"],
          "no margin rate sent: refused, nothing saved")
    check(asyncio.run(main.capital_import_preview(_Req(body={"csv": 5}))).status_code == 400
          and asyncio.run(main.capital_import_preview(_Req(body=ValueError()))).status_code == 400
          and asyncio.run(main.capital_import_preview(_Req(body={"csv": "a,b\n1,2"}))).status_code == 400,
          "not a CSV string, not JSON, not an export: 400")
    main._check_admin_token = lambda request: False
    no = asyncio.run(main.capital_import_apply(_Req(body={"csv": SCHWAB_ONE}, cookies={}, query_params={}, headers={})))
    check(no.status_code == 401, "only the admin can sync")
finally:
    main._check_admin_token, database.get_capital_profile, database.save_capital_profile = saved_fns

# ── the page ───────────────────────────────────────────────────────
from jinja2 import Environment, FileSystemLoader  # noqa: E402

COMP = [{"ticker": t, "name": t, "status": "COMPOUNDER", "expected": e, "er_div": 1.0}
        for t, e in (("AAA", 30.0), ("BBB", 26.0), ("CCC", 24.0))]
SRC = {"compounders": COMP, "aristocrats": [], "lynch": [], "quiet_value": [], "schloss": [], "house_hack": [],
       "brrrr": [], "flip": [], "home": []}
board = K.build({**prof, "monthly_invest": 1000, "monthly_expenses": 3000}, today=OCT, sources=SRC)
env = Environment(loader=FileSystemLoader(os.path.join(ROOT, "templates")), autoescape=True)
env.globals.update(is_admin=lambda r: True, current_user=lambda r: None, pipeline_access=lambda r: False)
req = SimpleNamespace(url=SimpleNamespace(path="/capital"), query_params={})
html = env.get_template("capital.html").render(
    request=req, board=board, p=board["profile"], view=V.build_view(board, OCT), saved=True, updated_at="2026-10-03",
    last_step={"title": title, "impact": None}, debts_text="", limits=K.LIMITS_2026, hours_default=K.HOURS_DEFAULT,
    accounts=V.EDIT_ACCOUNT_LABELS)
check('id="impBtn"' in html and 'id="impFile"' in html and "/api/capital/import/preview" in html
      and "/api/capital/import/apply" in html, "the holdings step has the import and its preview")
check('data-sync="schwab-901"' in html and "# sync schwab-901 | Schwab · Individual …901" in html
      and "data-sync-unlink" in html and "data-sync-drop" in html,
      "a synced group shows its lines, carries its block for the save, and can be edited by hand or removed")
check(html.count('data-c="name" value="BBB"') == 0 and 'data-c="name" value="Checking"' in html,
      "synced lines are not in the row editor (a save cannot half-edit a block)")
check("Done:</b> Synced Schwab Individual …901 — 3 positions." in html, "the banner names the sync, with no dollar figure")
visible = re.sub(r"<script.*?</script>", "", html, flags=re.S)
for bad_ in (">None<", "None%", "Undefined", "NaN", "Infinity"):
    check(bad_ not in visible, f"no '{bad_}' leaks into the page")

if _FAILS:
    print(f"FAIL — {len(_FAILS)}/{_COUNT} checks failed:")
    for x in _FAILS:
        print("  ✗", x)
    sys.exit(1)
print(f"OK — all {_COUNT} positions-import checks passed.")

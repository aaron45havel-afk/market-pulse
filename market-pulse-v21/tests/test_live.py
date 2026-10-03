"""Live holding values on /capital: a holdings line with a share count is
valued at shares × the live price (allocation.reprice) before the engine
reads it; prices come in one cached, time-boxed batch (stock_lookup.
live_prices). Offline — prices are passed in, or faked.

Run: python tests/test_live.py
"""
from __future__ import annotations

import asyncio
import os
import sys
import time
from datetime import date, datetime, timezone
from types import SimpleNamespace

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

import allocation as A  # noqa: E402
import capital as K  # noqa: E402
import capital_view as V  # noqa: E402
import positions as P  # noqa: E402
import stock_lookup as S  # noqa: E402

_FAILS: list[str] = []
_COUNT = 0


def check(cond, msg):
    global _COUNT
    _COUNT += 1
    if not cond:
        _FAILS.append(msg)


def near(a, b, tol=0.01):
    return a is not None and b is not None and abs(a - b) <= tol


# ── the line format ────────────────────────────────────────────────
TEXT = ("Checking, cash, 5000, 4%\nVTI, taxable, 1000, 800, 4 sh\nAAPL, roth, 500\n# a note\n"
        "BBB, taxable, 300, 250, 10.5 shares\nTop picks, roth, 900, 3 sh")
rows, bad = A.parse_holdings(TEXT)
by = {r["name"]: r for r in rows}
check(not bad and by["VTI"]["qty"] == 4 and by["BBB"]["qty"] == 10.5 and by["AAPL"]["qty"] is None
      and near(by["VTI"]["basis"], 800), "'4 sh' / '10.5 shares' is the share count; the basis is still the bare number")
check(A.priceable(rows) == ["BBB", "VTI"], "only lines with a share count and a real ticker are priced")
check(A.parse_holdings("VTI, taxable, 1, 2 sh, 3 sh")[1], "two share counts on one line: it cannot be read")
check(A.holding_line("VXUS", "taxable", 12345.67, 11000.5, qty=210.123456) == "VXUS, taxable, 12345.67, 11000.5, 210.123456 sh"
      and A.parse_holdings(A.holding_line("X", "taxable", 1, qty=0.000001))[0][0]["qty"] == 0.000001,
      "written back with the share count to six places")

# ── repricing ──────────────────────────────────────────────────────
PR = {"VTI": {"price": 300.0, "day_change": -2.0, "day_change_pct": -0.66}, "BBB": {"price": None}}
new, info = A.reprice(TEXT, PR)
check(new.splitlines()[1] == "VTI, taxable, 1200, 800, 4 sh" and new.splitlines()[0] == "Checking, cash, 5000, 4%"
      and new.splitlines()[3] == "# a note" and new.splitlines()[4] == TEXT.splitlines()[4],
      "VTI at 4 × $300 = $1,200; every other line — cash, a note, a line with no price — exactly as typed")
check(info["priced"] == 1 and info["of"] == 2 and info["missing"] == ["BBB"] and near(info["change"], 200)
      and near(info["day_change"], -8.0) and info["lines"][1]["was"] == 1000,
      "what changed: one of two priced, BBB stale, +$200 since the sync, −$8 today (4 shares × −$2)")

# ── the batch: cache, time box, market hours ───────────────────────
calls = []


def fake(t):
    calls.append(t)
    if t == "SLOW":
        time.sleep(1.5)
        return {"price": 1.0}
    if t == "BAD":
        return {"error": "no"}
    return {"price": 10.0, "day_change": 0.5, "day_change_pct": 5.0, "source": "yahoo"}


S._LIVE.clear()
clock = [1000.0]
OPEN = datetime(2026, 10, 5, 15, 0, tzinfo=timezone.utc)      # a Monday, 11:00 in New York
SHUT = datetime(2026, 10, 4, 15, 0, tzinfo=timezone.utc)      # a Sunday
t0 = time.time()
got = S.live_prices(["VTI", "BAD", "SLOW"], get=fake, now=OPEN, clock=lambda: clock[0], budget=0.3)
check(sorted(got) == ["VTI"] and time.time() - t0 < 1.2,
      "the page waits at most the budget: a slow ticker and a failed one are left out, the rest returned")
check(S.market_open(OPEN) and not S.market_open(SHUT) and not S.market_open(datetime(2026, 10, 5, 21, 0, tzinfo=timezone.utc)),
      "market hours in New York time: Monday 11:00 open, Sunday and 17:00 shut")
calls.clear()
clock[0] += 14 * 60
S.live_prices(["VTI"], get=fake, now=OPEN, clock=lambda: clock[0])
check(not calls, "within 15 minutes while the market is open: from the cache")
clock[0] += 2 * 60
S.live_prices(["VTI"], get=fake, now=OPEN, clock=lambda: clock[0])
check(calls == ["VTI"], "after 15 minutes: fetched again")
calls.clear()
clock[0] += 30 * 60
S.live_prices(["VTI"], get=fake, now=SHUT, clock=lambda: clock[0])
check(not calls, "market shut: an hour's cache (the close does not move)")
S.live_prices(["VTI"], get=fake, now=SHUT, clock=lambda: clock[0], fresh=True)
check(calls == ["VTI"], "Refresh prices goes past the cache")
S._LIVE.clear()

# ── the engine reads live values ───────────────────────────────────
OCT = date(2026, 10, 3)
COMP = [{"ticker": t, "name": t, "status": "COMPOUNDER", "expected": e, "er_div": 1.0}
        for t, e in (("AAA", 30.0), ("BBB", 26.0), ("CCC", 24.0))]
SRC = {"compounders": COMP, "aristocrats": [], "lynch": [], "quiet_value": [], "schloss": [], "house_hack": [],
       "brrrr": [], "flip": [], "home": []}
PROF = {"monthly_invest": 3000, "monthly_expenses": 3000, "home_state": "CA", "housing_cost": 2000, "age": 34,
        "holdings": "Checking, cash, 20000, 4%\nVTI, taxable, 10000, 9000, 40 sh\nNKE, taxable, 5000, 8000, 50 sh"}
NOW = {"VTI": {"price": 300.0, "day_change": 3.0, "day_change_pct": 1.0, "as_of": "2026-10-03T19:42:00+00:00"},
       "NKE": {"price": 90.0, "day_change": -1.0, "day_change_pct": -1.1, "as_of": "2026-10-03T19:41:00+00:00"}}


def build(prices=None, prof=None):
    src = {**SRC, **({"prices": prices} if prices is not None else {})}
    return K.build(prof or PROF, today=OCT, sources=src)


asked = []
_lp = S.live_prices
S.live_prices = lambda tickers, **kw: asked.append((list(tickers), kw.get("fresh"))) or {}
off = build()
check(not asked, "a test build (sources given) asks for no prices — no network")
real = K.build(PROF, today=OCT)              # no sources at all: the real page
K.build(PROF, today=OCT, fresh_prices=True)
S.live_prices = _lp
picks = [r["detail"]["ticker"] for r in A.sleeve_of(real)]
check(asked[0] == (["NKE", "VTI"], False) and asked[2] == (["NKE", "VTI"], True),
      "the real page asks for its holdings' prices in one batch (fresh when asked)")
check(picks and asked[1] == (picks, False) and asked[3] == (picks, True),
      "and the board's top picks' — so a pick bought through Mark done is written with its share count")
live = build(NOW)
check(near(off["profile"]["_net_worth"], 35_000) and near(live["profile"]["_net_worth"], 20_000 + 12_000 + 4_500),
      "NET WORTH at live prices: VTI 40 × $300, NKE 50 × $90 (synced values $10,000 and $5,000)")
check(off["live"]["priced"] == 0 and off["live"]["of"] == 2,
      "tests pass no prices and the build makes no request — the values stay as synced, and it says so")
hv = {i["label"]: i for i in live["current"]["holdings"]["items"]}
check(near(hv["VTI"]["value"], 12_000) and near(hv["NKE"]["value"], 4_500),
      "the holdings the plan measures are the live ones")
sm = V.live_summary(live, now=datetime(2026, 10, 3, 20, 0, tzinfo=timezone.utc))
check(sm["as_of"] == "3:42 PM ET" and near(sm["day_change"], 40 * 3 - 50 * 1) and sm["priced"] == 2 and not sm["missing"]
      and near(sm["day_pct"], 70 / (16_500 - 70) * 100, 0.01),
      "the overview's line: prices as of 3:42 PM New York time, +$70 today on the priced holdings")
check(V.live_summary(build(NOW, {**PROF, "holdings": "VTI, taxable, 100"})) is None,
      "no share counts anywhere: no live line at all")
yday = V.live_summary(live, now=datetime(2026, 10, 5, 15, 0, tzinfo=timezone.utc))
check(yday["as_of"] == "3:42 PM ET, Oct 3", "prices from another day say which day")

# ── steps edit the repriced lines; ids survive price moves ─────────
prof_lp = {**PROF, "holdings": PROF["holdings"] + "\nIdle, cash, 30000, 0.01%", "monthly_expenses": 1000}
b1 = build(NOW, prof_lp)
b2 = build({**NOW, "VTI": {**NOW["VTI"], "price": 310.0}}, prof_lp)
ids1 = {s["id"] for s in V.build_view(b1, OCT)["todo"]}
ids2 = {s["id"] for s in V.build_view(b2, OCT)["todo"]}
check(ids1 and ids1 == ids2, "A STEP'S ID IS WHAT MOVES WHERE: a price tick between loading the page and Mark done "
                             "does not turn the step into 'no longer on the list'")
nke_move = [{"to_kind": "picks", "to_id": "picks", "proceeds": 2250.0, "sold": 2250.0, "line": 2, "acct": "taxable",
             "name": "NKE", "from_id": "x", "prop": None}]
lived = {**PROF, "holdings": b1["profile"]["holdings"]}
after = A.apply_moves(lived, nke_move, {**b1, "live": {"prices": {"AAA": {"price": 50.0}}}})
line = next(x for x in after["holdings"].splitlines() if x.startswith("NKE"))
check(line == "NKE, taxable, 2250, 4000, 25 sh",
      "SELLING HALF at the live value: $2,250 left, half the basis, half the shares (25 of 50)")
aaa = next(x for x in after["holdings"].splitlines() if x.startswith("AAA"))
check(aaa.endswith(" sh") and near(A.parse_holdings(aaa)[0][0]["qty"], A.parse_holdings(aaa)[0][0]["value"] / 50.0, 1e-4),
      "a pick bought at a live price gets its share count — it stays live")

# ── the import writes share counts ─────────────────────────────────
blk = P.block_lines({"key": "schwab-456", "mask": "456", "broker": "Schwab", "label": "Individual …456",
                     "account": "taxable", "cash": 0.0, "positions": [
                         {"symbol": "VTI", "value": 12000.0, "basis": 9000.0, "qty": 40.0, "kind": "security"}]},
                    "2026-10-03", {})
check(blk[1] == "VTI, taxable, 12000, 9000, 40 sh", "a synced position carries the export's share count")

# ── the routes: the board is built live; ?fresh=1 goes past the cache ──
import database  # noqa: E402
import main  # noqa: E402

seen = []
saved_fns = (main._check_admin_token, database.get_capital_profile, K.build)
main._check_admin_token = lambda request: True
database.get_capital_profile = lambda owner="owner": dict(PROF)
_real = K.build
K.build = lambda prof, today=None, sources=None, fresh_prices=False: (seen.append(fresh_prices) or _real(
    prof, today=OCT, sources={**SRC, "prices": NOW}))
try:
    for q in ({"fresh": "1"}, {}):
        req = SimpleNamespace(query_params=q, url=SimpleNamespace(path="/capital"), cookies={}, headers={})
        try:
            asyncio.run(main.capital_page(req))
        except Exception:  # noqa: BLE001 — rendering needs a real request; the build is what is checked
            pass
    check(seen == [True, False], "/capital?fresh=1 asks for fresh prices; a plain load uses the cache")
finally:
    main._check_admin_token, database.get_capital_profile, K.build = saved_fns

# ── the page ───────────────────────────────────────────────────────
from jinja2 import Environment, FileSystemLoader  # noqa: E402

env = Environment(loader=FileSystemLoader(os.path.join(ROOT, "templates")), autoescape=True)
env.globals.update(is_admin=lambda r: True, current_user=lambda r: None, pipeline_access=lambda r: False)
req = SimpleNamespace(url=SimpleNamespace(path="/capital"), query_params={})


def render(b):
    return env.get_template("capital.html").render(
        request=req, board=b, p=b["profile"], view=V.build_view(b, OCT), saved=True, updated_at="2026-10-03",
        last_step=None, debts_text="", limits=K.LIMITS_2026, hours_default=K.HOURS_DEFAULT,
        accounts=V.EDIT_ACCOUNT_LABELS)


html = render(live)
check('class="cx-live"' in html and "<b>Live</b>" in html and 'href="/capital?fresh=1"' in html,
      "the overview says the values are live, as of when, with a way to refresh")
check("40 sh × $300.00" in html and "50 sh × $90.00" in html, "each priced holding shows shares × price")
check('data-c="qty"' in html and "q + ' sh'" in html, "the editor keeps a share count, so a typed line can be live too")
check("Prices unavailable" in render(off), "no prices came back: said plainly, values as last synced")

if _FAILS:
    print(f"FAIL — {len(_FAILS)}/{_COUNT} checks failed:")
    for x in _FAILS:
        print("  ✗", x)
    sys.exit(1)
print(f"OK — all {_COUNT} live-value checks passed.")

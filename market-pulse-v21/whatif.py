"""What if — a move the owner is weighing, against leaving things as they are
and against the best use of the same money.

A move shifts an amount out of one holding (or cash) into a destination:
the top picks, a ticker, an index fund, T-bills, paying down a debt, or
something else at a return the owner gives. The money keeps to its
account's rules — an IRA, Roth or HSA move stays inside that account, a
401(k) holds the plan's funds, a debt is paid from cash or taxable money.

The verdict is the board's own arithmetic, nothing new:
  keep  = amount × (1 + h)^hold                      (h: its return where it is)
  move  = amount × (1 − costs − tax) × (1 + a)^hold  (a: the destination's)
both in after-tax dollars (a traditional balance counted at its after-tax
value), and the destination's return is the board's after-tax return net
of friction for new money in that account. "Better" or "worse" needs a
gap of at least 1% of the money over the hold — the same bar a Do-next
move has to clear. The best use of the same money fills the board's
destinations best first (a debt takes at most its balance); if keeping
beats them all, keeping is the best use.

kit() hands the page the numbers its live calculator needs; evaluate()
runs the same arithmetic in Python for saved scenarios, the apply route
and the tests, and the page's JavaScript mirrors it line for line.
"""
from __future__ import annotations

import math
import secrets

import allocation as A
import capital as K

DEST_KINDS = ("picks", "ticker", "index", "tbill", "debt", "custom")
MAX_SCENARIOS = 20
SAME_BAND = 0.01          # within 1% of the money over the hold: about the same


def account_class(acct: str) -> str:
    """Where money from this account can go: taxable (cash and brokerage),
    plan (a 401(k)'s own funds), or the IRA, Roth or HSA it is in."""
    if acct in ("cash", "taxable"):
        return "taxable"
    if acct in A.PLAN:
        return "plan"
    return "hsa" if acct == "hsa" else "sheltered"


# ═══════════════════════════════════════════════════════════════════
# THE KIT — everything the live calculator needs
# ═══════════════════════════════════════════════════════════════════

def kit(board: dict) -> dict:
    p = board["profile"]
    t = K.tax_rates(p)
    H = A._H(p)
    mr = K._f(p.get("market_return"), 7.0)
    rf = K._f(p.get("rf_rate"), 4.0)
    q = t["qualified"]
    hold = (board.get("current") or {}).get("holdings") or {}
    debt_rows = {r["id"].split(":", 1)[-1]: r["detail"] for r in board["rows"] if r["kind"] == "debt"}
    debt_net = {n: d["hold_rate"] for n, d in debt_rows.items() if d.get("hold_rate") is not None}
    payoff_label = {n: d.get("payoff_label") for n, d in debt_rows.items()}
    sleeve = A.sleeve_of(board)
    td = A.stock_time_drag(p, board.get("monthly_free", 0.0))
    rt_large = K.ROUND_TRIP_PCT["stock_large"] / H
    hsa_st = A._hsa_state(p)
    sources = []
    for i in hold.get("items", []):
        if i.get("property") or i.get("line") is None:
            continue
        sources.append({k: i.get(k) for k in ("line", "label", "account", "account_label", "value", "h", "tau", "c",
                                              "weq", "basis", "ticker", "why_not", "state_exempt")})
    tickers = {}
    for r in board["rows"]:
        if r["kind"] != "stock" or r.get("ret_net") is None:
            continue
        tk = r["detail"]["ticker"]
        pre_net = r["ret_pre"] - r["cost_drag"] - r["time_drag"]
        tickers[tk] = {"label": r["label"], "source": ", ".join(dict.fromkeys(r.get("sources") or [r["source"]])),
                       "taxable": round(r["ret_net"], 4), "sheltered": round(pre_net, 4)}
    if sleeve:
        n = len(sleeve)
        picks = {"taxable": sum(r["ret_net"] for r in sleeve) / n,
                 "sheltered": sum(r["ret_pre"] - r["cost_drag"] - r["time_drag"] for r in sleeve) / n}
    else:
        picks = None
    nets = {
        "index": {"taxable": K.after_tax_taxable(mr, A.INDEX_DIV, H, q, q), "sheltered": mr},
        "offboard": {"taxable": K.after_tax_taxable(mr, 0.0, H, q, q) - rt_large - td, "sheltered": mr - rt_large - td},
        "tbill": rf * (1 - t["tbill"]),
        "picks": picks,
    }
    cash_total = K._f(p.get("cash"))
    ef = K._f(p.get("monthly_expenses")) * K._f(p.get("emergency_months"), 6.0)
    positions: dict[str, float] = {}
    picks_held = 0.0
    for h in A.parse_holdings(p.get("holdings"))[0]:
        if h.get("ticker") and h["account"] != "cash":
            positions[h["ticker"]] = positions.get(h["ticker"], 0.0) + h["value"]
            if A.is_pick(h):
                picks_held += h["value"]
    return {
        "hold": H, "q": q, "ordinary": t["ordinary"], "market": mr, "index_div": A.INDEX_DIV, "hsa_state": hsa_st,
        "sources": sources, "tickers": tickers, "nets": nets,
        "sleeve": [r["detail"]["ticker"] for r in sleeve],
        "debts": [{"name": d["name"], "apr": K._f(d.get("apr")), "balance": K._f(d.get("balance")),
                   "net": debt_net.get(d["name"], K._f(d.get("apr"))), "payoff": payoff_label.get(d["name"])}
                  for d in p.get("debts") or [] if K._f(d.get("balance")) > 0],
        "cash_total": cash_total, "ef": ef, "monthly_expenses": K._f(p.get("monthly_expenses")),
        "emergency_months": K._f(p.get("emergency_months"), 6.0),
        "pick_max": K._f(p.get("pick_max_pct"), 20.0), "picks_held": picks_held, "positions": positions,
        "min_gain": SAME_BAND, "tlh_cap": 3000.0,
    }


def _net(kt: dict | None, cls: str, hsa_state: float) -> float | None:
    """A {taxable, sheltered} net for this account class. HSA growth is taxed
    by California and New Jersey; a 401(k) dollar is treated as sheltered."""
    base = kt.get("sheltered" if cls != "taxable" else "taxable") if kt else None
    if base is None:
        return None
    return base * (1 - hsa_state) if cls == "hsa" else base


def _after_custom(k: dict, cls: str, rate: float) -> float:
    if cls == "taxable":
        return K.after_tax_taxable(rate, 0.0, k["hold"], k["q"], k["q"])
    return rate * (1 - k["hsa_state"]) if cls == "hsa" else rate


def destination(k: dict, cls: str, dest: dict) -> dict:
    """{net, label, allowed, note, cap} for a destination from this account class."""
    kind = dest.get("kind")
    hs = k["hsa_state"]
    if kind == "picks":
        if not k["nets"]["picks"]:
            return {"net": None, "label": "The top picks", "allowed": False, "note": "There are no top picks on the board right now."}
        return {"net": _net(k["nets"]["picks"], cls, k["hsa_state"]), "label": f"The top picks ({', '.join(k['sleeve'])})",
                "allowed": True, "note": "a 401(k) holds the plan's funds — only if your plan has a brokerage window"
                if cls == "plan" else None}
    if kind == "ticker":
        tk = str(dest.get("name") or "").strip().upper()
        if not tk:
            return {"net": None, "label": "A ticker", "allowed": False, "note": "Name the ticker."}
        row = k["tickers"].get(tk)
        net = _net(row, cls, hs) if row else _net(k["nets"]["offboard"], cls, hs)
        note = None if row else f"{tk} is on no screen, so the market return ({k['market']:g}%) is assumed."
        if cls == "plan":
            note = "A 401(k) holds the plan's funds — this works only through a brokerage window."
        return {"net": net, "label": tk if not row else row["label"], "allowed": True, "note": note, "single": True,
                "ticker": tk}
    if kind == "index":
        return {"net": _net(k["nets"]["index"], cls, k["hsa_state"]), "label": "An index fund", "allowed": True, "note": None}
    if kind == "tbill":
        if cls != "taxable":
            return {"net": None, "label": "T-bills", "allowed": False,
                    "note": "Money in a retirement account stays there; T-bills are for cash and taxable money."}
        return {"net": k["nets"]["tbill"], "label": "T-bills", "allowed": True, "note": None}
    if kind == "debt":
        d = next((x for x in k["debts"] if x["name"] == dest.get("name")), None)
        if cls != "taxable":
            return {"net": None, "label": "Pay down a debt", "allowed": False,
                    "note": "Paying a debt from a retirement account means a withdrawal — taxes and penalties."}
        if not d:
            return {"net": None, "label": "Pay down a debt", "allowed": False, "note": "Pick one of your debts."}
        return {"net": d["net"], "label": f"Pay down {d['name']}", "allowed": True,
                "note": (f"Your pay clears it by {d['payoff']} anyway: paying it now saves {d['apr']:g}% only until "
                         f"then." if d.get("payoff") else None), "cap": d["balance"], "debt": d["name"]}
    if kind == "custom":
        rate = dest.get("rate")
        if rate is None:
            return {"net": None, "label": dest.get("name") or "Something else", "allowed": False,
                    "note": "Give its expected return a year, before tax."}
        return {"net": _after_custom(k, cls, float(rate)), "label": dest.get("name") or "Something else",
                "allowed": True, "note": "Your own return estimate, taxed as growth taken at the end of the hold."
                if cls == "taxable" else "Your own return estimate."}
    return {"net": None, "label": "?", "allowed": False, "note": "Choose where the money goes."}


def best_use(k: dict, cls: str, proceeds: float) -> list[dict]:
    """The board's destinations for this account class, best first, filled
    with the proceeds — a debt takes at most its balance. Individual tickers
    are not on this list: the board's answer for stocks is the top picks."""
    opts = []
    if cls == "taxable":
        opts += [{"label": f"Pay down {d['name']}", "net": d["net"], "cap": d["balance"]} for d in k["debts"]]
        opts.append({"label": "T-bills", "net": k["nets"]["tbill"], "cap": math.inf})
    if k["nets"]["picks"] and cls != "plan":
        opts.append({"label": "The top picks", "net": _net(k["nets"]["picks"], cls, k["hsa_state"]), "cap": math.inf})
    opts.append({"label": "An index fund", "net": _net(k["nets"]["index"], cls, k["hsa_state"]), "cap": math.inf})
    opts.sort(key=lambda o: -o["net"])
    left, out = proceeds, []
    for o in opts:
        if left <= 0.005:
            break
        take = min(left, o["cap"])
        if take > 0.005:
            out.append({"label": o["label"], "net": o["net"], "amount": take})
            left -= take
    return out


def _payback(keep: float, a: float, h: float) -> float | None:
    if a <= h:
        return None
    if keep >= 1:
        return 0.0
    return math.log(1 / keep) / math.log((1 + a / 100) / (1 + h / 100))


def evaluate(k: dict, move: dict) -> dict:
    """The verdict on one move. move = {"line": int (or "src_name" +
    "src_account"), "amount": float | None (all), "dest": {"kind", "name",
    "rate"}}."""
    src = None
    if move.get("line") is not None:
        src = next((s for s in k["sources"] if s["line"] == move["line"]), None)
    if src is None and move.get("src_name"):
        src = next((s for s in k["sources"] if s["label"] == move["src_name"]
                    and s["account"] == move.get("src_account")), None)
    if src is None:
        return {"ok": False, "error": "That holding is no longer in your profile."}
    if src["h"] is None:
        return {"ok": False, "error": src.get("why_not") or f"{src['label']} has no return to compare."}
    cls = account_class(src["account"])
    dest = move.get("dest") or {}
    d = destination(k, cls, dest)
    if not d["allowed"] or d["net"] is None:
        return {"ok": False, "error": d["note"] or "That destination is not open to this money."}
    warnings = []
    value = float(src["value"])
    amt = value if move.get("amount") in (None, "", "all") else max(0.0, float(move["amount"]))
    if amt > value + 0.005:
        warnings.append(f"Only {_m(value)} is in {src['label']}; the move uses all of it.")
        amt = value
    if amt <= 0:
        return {"ok": False, "error": "Enter an amount to move."}
    tau = src["tau"]
    if tau is None:
        tau = 0.0
        warnings.append(f"No cost basis for {src['label']}: the tax to sell is not counted, so this may look better "
                        f"than it is. Add the basis in Profile → What you hold.")
    c = float(src["c"] or 0.0)
    keep = 1 - c - tau
    proceeds = amt * keep
    if d.get("cap") is not None and proceeds > d["cap"] + 0.005:
        warnings.append(f"{d['debt']} is only {_m(d['cap'])}; the move pays it off and moves no more.")
        proceeds = d["cap"]
        amt = proceeds / keep
    H, w, h, a = k["hold"], float(src["weq"]), float(src["h"]), float(d["net"])
    keep_w = amt * w * (1 + h / 100) ** H
    move_w = proceeds * w * (1 + a / 100) ** H
    diff = move_w - keep_w
    per_year = w * (proceeds * a - amt * h) / 100
    band = k["min_gain"] * amt * w
    verdict = "better" if diff >= band else ("worse" if diff <= -band else "same")
    fill = best_use(k, cls, proceeds)
    best_w = sum(f["amount"] * w * (1 + f["net"] / 100) ** H for f in fill)
    best_year = w * (sum(f["amount"] * f["net"] for f in fill) - amt * h) / 100
    keeping_best = keep_w >= best_w
    best = {"label": "Keeping it where it is" if keeping_best else ", then ".join(f["label"] for f in fill),
            "w": keep_w if keeping_best else best_w, "per_year": 0.0 if keeping_best else best_year,
            "fill": [] if keeping_best else fill}
    shortfall = best["w"] - move_w
    # what else to know
    if src["account"] == "cash":
        left = k["cash_total"] - amt
        if left < k["ef"] - 0.5 and k["monthly_expenses"] > 0:
            warnings.append(f"Leaves {_m(left)} in cash — {left / k['monthly_expenses']:.1f} months of expenses, below "
                            f"your {k['emergency_months']:g}-month emergency fund.")
    if d.get("single") and src.get("ticker") == d["ticker"]:
        warnings.append("That is where the money already is.")
    elif d.get("single"):
        pos = k["positions"].get(d["ticker"], 0.0) + proceeds
        sleeve_total = k["picks_held"] + proceeds
        if sleeve_total > 0 and pos / sleeve_total * 100 > k["pick_max"] + 0.01:
            warnings.append(f"{d['ticker']} would be {pos / sleeve_total * 100:.0f}% of your stock picks; your cap per "
                            f"name is {k['pick_max']:g}%.")
    loss_note = None
    if src["account"] == "taxable" and src.get("basis") is not None and src["basis"] > value + 0.5 and value > 0:
        loss = (src["basis"] - value) * amt / value
        saves = min(loss, k["tlh_cap"]) * k["ordinary"]
        loss_note = (f"Selling realizes a {_m(loss)} loss — about {_m(saves)} less tax this year (not counted above).")
        if d.get("single") and src.get("ticker") == d["ticker"]:
            warnings.append("Buying the same shares back within 30 days cancels the loss (the wash-sale rule).")
    if d.get("note"):
        warnings.append(d["note"])
    return {"ok": True, "source": src["label"], "source_account": src["account_label"], "dest": d["label"],
            "amount": amt, "proceeds": proceeds, "tax": amt * tau, "cost": amt * c, "h": h, "a": a, "weq": w,
            "keep_w": keep_w, "move_w": move_w, "diff": diff, "per_year": per_year, "verdict": verdict,
            "payback": _payback(keep, a, h), "best": best, "shortfall": shortfall,
            "is_best": shortfall <= band, "warnings": warnings, "loss_note": loss_note, "hold": H,
            "line": src["line"], "account": src["account"], "cls": cls, "dest_kind": dest.get("kind"),
            "dest_name": d.get("ticker") or d.get("debt") or dest.get("name"), "dest_rate": dest.get("rate")}


def _m(v: float) -> str:
    return ("−" if v < 0 else "") + f"${abs(v):,.0f}"


def summary(r: dict) -> str:
    return f"{_m(r['amount'])} from {r['source']} → {r['dest']}"


# ═══════════════════════════════════════════════════════════════════
# APPLYING A MOVE, SAVED SCENARIOS
# ═══════════════════════════════════════════════════════════════════

def as_move(r: dict) -> dict:
    """The evaluation as an allocation move apply_moves can carry out."""
    kind = r["dest_kind"]
    dest_acct = "taxable" if r["cls"] == "taxable" else r["account"]
    to_id = {"picks": "picks" if r["cls"] == "taxable" else "picks_in", "debt": f"debt:{r['dest_name']}"}.get(kind, kind)
    return {"to_kind": kind, "to_id": to_id, "proceeds": round(r["proceeds"], 2), "sold": round(r["amount"], 2),
            "line": r["line"], "acct": r["account"], "dest_acct": dest_acct, "prop": None,
            "name": r["dest_name"], "rate": r["dest_rate"]}


def parse_scenarios(items) -> list[dict]:
    """Saved moves, from the page: each names its source by holding name and
    account (lines move as the profile changes), an amount (blank = all),
    and a destination."""
    out = []
    for it in (items if isinstance(items, list) else [])[:MAX_SCENARIOS * 10]:
        if len(out) >= MAX_SCENARIOS:
            break
        if not isinstance(it, dict):
            continue
        dest = it.get("dest") if isinstance(it.get("dest"), dict) else {}
        kind = dest.get("kind")
        if kind not in DEST_KINDS or not it.get("src_name"):
            continue
        amt = K._f(str(it.get("amount")).replace(",", "").replace("$", ""), None) if it.get("amount") not in (None, "", "all") else None
        rate = K._f(str(dest.get("rate")).replace("%", ""), None) if dest.get("rate") not in (None, "") else None
        out.append({"id": str(it.get("id") or secrets.token_hex(4))[:16],
                    "name": str(it.get("name") or "").strip()[:60] or "A move",
                    "src_name": str(it["src_name"])[:40], "src_account": str(it.get("src_account") or "")[:12],
                    "amount": None if amt is None else max(0.0, min(amt, 1e10)),
                    "dest": {"kind": kind, "name": str(dest.get("name") or "")[:40] or None,
                             "rate": None if rate is None else max(-50.0, min(100.0, rate))}})
    return out


def scenarios(board: dict, k: dict) -> list[dict]:
    out = []
    for s in parse_scenarios(board["profile"].get("scenarios")):
        r = evaluate(k, {"src_name": s["src_name"], "src_account": s["src_account"], "amount": s["amount"],
                         "dest": s["dest"]})
        out.append({**s, "result": r})
    return out

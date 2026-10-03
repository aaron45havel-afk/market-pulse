"""The /capital dashboard: everything the page shows, worked out from the
board (capital.build) — the money picture, the ranked Do-next list, the
slower questions (independence, debts, passive income, how fast money can
be reached), each property's keep-borrow-sell inputs, the tax helpers, a
normal month's split, and how complete the profile is.

Nothing here is a second model. Returns, moves and the waterfall all come
from capital.py and allocation.py; this module groups them into steps a
person can act on, ranks them by what each adds after tax, and adds the
arithmetic an individual asks about (when do I reach independence, when is
the car paid off, how much of my spending does my passive income cover).

Percentages are in PERCENT throughout (7.0 means 7%).
"""
from __future__ import annotations

import hashlib
import math
import re
from datetime import date

import allocation as A
import capital as K

MIN_MONTHLY_GAIN = 50.0       # a split change worth less than $50 a year is not a step
# The account names the profile editor offers — each one account_of reads back.
EDIT_ACCOUNT = {"cash": "cash", "taxable": "taxable", "k401": "401k", "roth401k": "roth 401k", "ira": "ira",
                "roth": "roth", "hsa": "hsa", "debt": "debt"}
EDIT_ACCOUNT_LABELS = [("cash", "Cash (checking, savings)"), ("tbills", "T-bills"), ("taxable", "Taxable brokerage"),
                       ("401k", "401(k)"), ("roth 401k", "Roth 401(k)"), ("ira", "Traditional IRA"),
                       ("roth", "Roth IRA"), ("hsa", "HSA")]
TLH_ORDINARY_CAP = 3_000      # a net capital loss offsets at most $3,000 of ordinary income a year
SMALL_STEP = 100.0            # a one-time move adding less a year is a clean-up, folded at the end


def _sid(*parts) -> str:
    return hashlib.sha1("|".join(str(x) for x in parts).encode()).hexdigest()[:12]


def _money(v: float) -> str:
    return ("−" if v < 0 else "") + f"${abs(v):,.0f}"


def _pct(v: float) -> str:
    return f"{v:.1f}%".replace("-", "−")


# ═══════════════════════════════════════════════════════════════════
# DO NEXT — the moves grouped into steps, plus the monthly split
# ═══════════════════════════════════════════════════════════════════

def _group_key(m: dict) -> tuple:
    if m.get("prop") is not None:
        return ("prop", m["prop"])               # a property is sold whole: one step
    if m["to_kind"] == "tbill":
        return ("tbill",)                          # the emergency fund and idle cash into T-bills: one step
    if m["to_kind"] in ("re", "dpfund"):
        return (m["to_kind"], m["to_id"])
    return (m["from_id"], m["to_id"])


def _short_to(m: dict) -> str:
    if m["to_kind"] == "debt":
        return m["to_id"].split(":", 1)[1]
    return {"picks": "the top picks", "index": "the index fund", "tbill": "T-bills"}.get(
        m["to_kind"], m["to"].replace(" (T-bills until it closes)", ""))


def _step_text(g: dict, p: dict, debts: dict, holdings: dict) -> tuple[str, str]:
    """(title, one-line explanation) for a grouped step."""
    m = g["parts"][0]
    kind, name = m["to_kind"], m["name"]
    if m.get("prop") is not None:
        return (f"Sell {name} and redeploy",
                f"After {_money(g['tax'])} of tax and 7% to sell, the money earns more than the property earns on "
                f"the equity in it.")
    if kind == "debt":
        dn = m["to_id"].split(":", 1)[1]
        d = debts.get(dn) or {}
        off = g["proceeds"] >= K._f(d.get("balance")) - 0.5
        when = (m.get("payoff_label") if m.get("payoff_label") else None)
        return (f"{'Pay off' if off else 'Pay down'} {dn}",
                f"A guaranteed {K._f(d.get('apr')):g}%" + (" — and its monthly payment stops" if off and d.get("payment") else "")
                + (f", for the months until your pay would clear it ({when})." if when else "."))
    if kind == "tbill":
        reserve = all(x["key"] == "reserve" for x in g["parts"])
        return ("Keep the emergency fund in T-bills" if reserve else "Move idle cash to T-bills",
                "Your emergency fund stays cash, at the T-bill rate, and T-bill interest is free of state tax."
                if reserve else "Nothing on the board beats T-bills for this money right now.")
    if kind == "dpfund":
        return ("Start the down payment",
                f"For {m['to'].replace('Down-payment fund for ', '').replace(' (T-bills until it closes)', '')}: held "
                f"in T-bills until it closes. The board already counts this cash toward that deal.")
    if kind == "re":
        return (f"Buy: {m['to']}", m.get("conditional") or "One deal's cash, all at once.")
    h = holdings.get(m.get("line")) or {}
    if kind == "index":
        return (f"Move {name} to the index fund",
                "A 401(k) can only hold the plan's funds; the index fund is the best one it has."
                if m["acct"] in A.PLAN else "No tax to switch inside the account.")
    if m["to_id"] == "picks_in":
        return (f"Hold the top picks in your {m['account']}", "No tax to switch inside the account, and the growth "
                f"is {'never taxed' if m['acct'] in ('roth', 'roth401k') else 'taxed only on the way out'}.")
    if m["acct"] == "cash":
        return (f"Put idle {name} to work", f"Money above the emergency fund earns {_pct(m['h'])} after tax there.")
    loss = (h.get("basis") or 0) - (h.get("value") or 0)
    if loss > 0.5:
        return (f"Sell {name} at a loss, buy the picks",
                f"Selling realizes a {_money(loss)} loss — against $3,000 of pay a year in all, the rest carried forward; see Taxes.")
    if g["tax"] > 0.5:
        return (f"Switch {name} to the top picks", "Worth it even after the capital-gains tax on the gain.")
    return (f"Switch {name} to the top picks", "No gain to tax, and the picks earn more after tax.")


def once_steps(board: dict) -> list[dict]:
    cur = board.get("current") or {}
    hold = cur.get("holdings") or {}
    p = board["profile"]
    debts = {d["name"]: d for d in p.get("debts") or []}
    holdings = {r["line"]: r for r in A.parse_holdings(p.get("holdings"))[0]}
    groups: dict[tuple, dict] = {}
    for m in hold.get("moves") or []:
        g = groups.setdefault(_group_key(m), {"parts": [], "sold": 0.0, "tax": 0.0, "cost": 0.0, "proceeds": 0.0,
                                              "impact": 0.0, "gain_hold": 0.0})
        g["parts"].append(m)
        for k in ("sold", "tax", "cost", "proceeds"):
            g[k] += m[k]
        g["impact"] += m["weq"] * (m["proceeds"] * m["a"] - m["sold"] * m["h"]) / 100
        g["gain_hold"] += m["gain"]
    out = []
    for key, g in groups.items():
        m = g["parts"][0]
        title, note = _step_text(g, p, debts, holdings)
        names = list(dict.fromkeys(x["name"] for x in g["parts"]))

        def realized(x):
            h = holdings.get(x.get("line"))
            if not h or x["acct"] != "taxable" or h["basis"] is None or h["value"] <= 0:
                return 0.0
            return max(0.0, h["basis"] - h["value"]) * x["sold"] / h["value"]
        loss = sum(realized(x) for x in g["parts"])
        rate = _step_rate(g, m, board)
        out.append({
            # what moves where — not how many dollars: with live prices the
            # amount drifts between loading the page and pressing Mark done
            "id": _sid(*sorted((x["from_id"], x["to_id"]) for x in g["parts"])),
            "type": "once", "kind": m["to_kind"], "title": title, "note": note,
            "what": f"{_money(g['sold'])} from {' + '.join(names)} → {_short_to(m)}",
            "impact": round(g["impact"], 2), "tax": round(g["tax"], 2), "cost": round(g["cost"], 2),
            "gain_hold": round(g["gain_hold"], 2), "sold": round(g["sold"], 2),
            "account": m["account"] if m["acct"] in A.SHELTERED else None,
            "loss": round(loss, 2), "harvest": 0.0, "carry": 0.0,        # shared out in do_next
            "parts": g["parts"], "can_apply": m["to_kind"] != "re",
            "h": rate["h"], "a": rate["a"], "gap": rate["gap"], "rate_note": rate["note"],
            "cost_pct": rate["cost_pct"], "payback_months": rate["payback_months"],
            "conditional": m.get("conditional"),
        })
    return out


def _step_rate(g: dict, m: dict, board: dict) -> dict:
    """A step's return in percent, the way the owner reads a decision: what
    the money earns a year where it is (each holding weighted by what is sold
    of it) → what it earns where it goes (weighted by what arrives), after
    tax; the gap in points; what the destination's rate is; and the one-time
    tax and costs as a share of the money, with the months they take to earn
    back at the gain."""
    sold = sum(x["sold"] for x in g["parts"]) or 1.0
    got = sum(x["proceeds"] for x in g["parts"]) or 1.0
    h = sum(x["sold"] * x["h"] for x in g["parts"]) / sold
    a = sum(x["proceeds"] * x["a"] for x in g["parts"]) / got
    row = next((r for r in board["rows"] if r["id"] == m["to_id"]), None) or {}
    det = row.get("detail") or {}
    kind = m["to_kind"]
    if kind == "debt":
        note = (f"its {K._f(row.get('ret_after')):g}% APR, saved until your pay would clear it ({det['payoff_label']})"
                if det.get("payoff_label") else f"its {K._f(row.get('ret_after')):g}% APR, guaranteed, for as long as "
                                                "it would run")
    elif kind == "re":
        note = (f"a year over your {det.get('years', '')}-year hold — {det['year_one']:g}% in year one alone"
                if det.get("year_one") is not None else "a year over your hold")
    elif kind == "dpfund":
        note = "the property's return, the months of saving included"
    elif kind == "picks":
        note = "the top picks' average, after tax and research time"
    elif kind == "tbill":
        note = "the T-bill rate after federal tax (no state tax)"
    elif kind == "index":
        note = "the market return, in the same account"
    else:
        note = ""
    once = g["tax"] + g["cost"]
    gain_yr = g["impact"]
    return {"h": round(h, 2), "a": round(a, 2), "gap": round(a - h, 2), "note": note,
            "cost_pct": round(once / sold * 100, 2) if once > 0.5 else 0.0,
            "payback_months": (max(1, round(once / gain_yr * 12)) if once > 0.5 and gain_yr > 0 else None)}


def steady_plan(board: dict, today: date) -> dict:
    """A normal month, from January: the one-time steps done (the emergency
    fund full, debt above the hurdle paid), a full year of IRA, 401(k) and
    HSA room, split across the waterfall."""
    p = board["profile"]
    mr = K._f(p.get("market_return"), 7.0)
    ef = K._f(p.get("monthly_expenses")) * max(K._f(p.get("emergency_months"), 6.0), K._f(p.get("starter_months"), 1.0))
    lim = K.LIMITS_2026
    ps = {**p, "cash": max(K._f(p.get("cash")), ef),
          "debts": [d for d in p.get("debts") or [] if K._f(d.get("apr")) < mr],
          "k401_room": float(lim["k401"]), "ira_room": float(lim["ira"]),
          "hsa_room": float(lim["hsa_self"])}
    flows = A.parse_flows(p.get("current_monthly"))[0]
    total = round(sum(f["amount"] for f in flows), 2) or K._f(p.get("monthly_invest"))
    jan = date(today.year + 1, 1, 15)
    wf = K.waterfall({**ps, "monthly_invest": total}, board["rows"], jan)
    sleeve = A.sleeve_of(board)
    steps = A.step_returns(wf["steps"], wf["left"], {**board, "profile": ps}, sleeve)
    return {"steps": steps, "raw": wf["steps"], "left": wf["left"], "total": total,
            "per_year": round(sum(s["amount"] * (s["ret"] or 0) / 100 for s in steps) * 12, 2)}


_SPLIT_WORDS = [("match", "to the 401(k) match"), ("debt", "to {name}"), ("cash", "to the emergency fund"),
                ("hsa", "to the HSA"), ("ira", "to the Roth IRA"), ("wrapper", "more to the 401(k)"),
                ("stock", "across the top picks"), ("re", "toward the down payment"), ("tbill", "to T-bills")]


def _split_sentence(steps: list[dict], left: float) -> str:
    """'$500 to the 401(k) match, $625 to the Roth IRA, $2,875 across the top
    picks' — the waterfall's split in words, one phrase per kind of use."""
    parts = []
    for kind, words in _SPLIT_WORDS:
        group = [st for st in steps if st["kind"] == kind]
        if kind == "debt":
            for st in group:
                name = str(st.get("ref") or st["to"]).split(":", 1)[-1].replace("Pay down ", "")
                parts.append(f"{_money(st['amount'])} {words.format(name=name)}")
        elif group:
            parts.append(f"{_money(sum(st['amount'] for st in group))} {words}")
    if left > 0.5:
        parts.append(f"{_money(left)} left in T-bills")
    return ", ".join(parts) if parts else "nothing to split"


def monthly_step(board: dict, steady: dict) -> dict | None:
    """The owner's split against a normal month's waterfall of the same
    amount. A payment to a debt the plan clears (above the hurdle) is counted
    as freed money at the T-bill rate — it will not keep earning that APR."""
    p = board["profile"]
    flows = A.parse_flows(p.get("current_monthly"))[0]
    if not flows:
        return None
    t = K.tax_rates(p)
    mr = K._f(p.get("market_return"), 7.0)
    tbill = K._f(p.get("rf_rate"), 4.0) * (1 - t["tbill"])
    dear = {d["name"].lower() for d in p.get("debts") or [] if K._f(d.get("apr")) >= mr}
    lines = A.measure_flows(flows, p, A.stock_rows_of(board), A.stock_time_drag(p, board.get("monthly_free", 0.0)))
    # A payment to a debt that is no longer listed (paid off) is freed money too.
    if any(l["ret"] is None and f["account"] != "debt" for f, l in zip(flows, lines)):
        return None
    now = 0.0
    for f, l in zip(flows, lines):
        freed = f["account"] == "debt" and (f["name"].strip().lower() in dear or l["ret"] is None)
        now += f["amount"] * (tbill if freed else l["ret"]) / 100
    now *= 12
    gain = steady["per_year"] - now
    if gain < MIN_MONTHLY_GAIN:
        return None
    per_yr = steady["total"] * 12
    return {"id": _sid("monthly", A.split_text(steady["raw"], steady["left"], p)), "type": "monthly", "kind": "monthly",
            "h": round(now / per_yr * 100, 2) if per_yr > 0 else None,
            "a": round(steady["per_year"] / per_yr * 100, 2) if per_yr > 0 else None,
            "gap": round((steady["per_year"] - now) / per_yr * 100, 2) if per_yr > 0 else None,
            "rate_note": "on each month's contributions, from January", "cost_pct": 0.0, "payback_months": None,
            "title": "Switch to the new monthly split",
            "note": "From January: " + _split_sentence(steady["raw"], steady["left"]) + ".",
            "what": f"Your {_money(steady['total'])} a month, split the waterfall's way",
            "impact": round(gain, 2), "now_yr": round(now, 2), "opt_yr": steady["per_year"], "tax": 0.0, "cost": 0.0,
            "harvest": 0.0, "carry": 0.0, "can_apply": True, "account": None}


def do_next(board: dict, steady: dict) -> list[dict]:
    """Every step, debt first (a guaranteed return), then by what it adds a
    year after tax."""
    steps = once_steps(board)
    m = monthly_step(board, steady)
    if m:
        steps.append(m)
    steps.sort(key=lambda s: (s["kind"] != "debt", -s["impact"]))
    for i, s in enumerate(steps, 1):
        s["rank"] = i
    share_losses(steps, K.tax_rates(board["profile"])["ordinary"])
    return steps


def share_losses(steps: list[dict], rate: float) -> None:
    """A net capital loss offsets at most $3,000 of ordinary income a year —
    in all, not per sale. The steps use it in their order; what is left of a
    loss carries forward (to gains, or to next year's $3,000)."""
    left = TLH_ORDINARY_CAP
    for s in steps:
        loss = s.get("loss") or 0.0
        if loss > 0.5:
            used = min(loss, left)
            left -= used
            s["harvest"] = round(used * rate, 2)
            s["carry"] = round(loss - used, 2)


def find_step(board: dict, today: date, step_id: str) -> tuple[dict | None, dict]:
    steady = steady_plan(board, today)
    return next((s for s in do_next(board, steady) if s["id"] == step_id), None), steady


def apply_step(saved: dict, board: dict, today: date, step_id: str) -> tuple[dict, dict]:
    """(the saved profile with the step done, the step). Raises
    allocation.CannotApply when the step is gone or cannot be done here."""
    step, steady = find_step(board, today, step_id)
    if step is None:
        raise A.CannotApply("That step is no longer on the list — the plan changed. Reload to see the current one.")
    if step["type"] == "monthly":
        prof = {k: v for k, v in saved.items() if not str(k).startswith("_")}
        prof["current_monthly"] = A.split_text(steady["raw"], steady["left"], board["profile"])
        return prof, step
    return A.apply_moves(saved, step["parts"], board), step


# ═══════════════════════════════════════════════════════════════════
# THE SLOWER QUESTIONS
# ═══════════════════════════════════════════════════════════════════

def independence(board: dict, hold: dict | None, today: date) -> dict | None:
    """The number (a year of expenses × the multiple), progress, the year
    each path reaches it — net worth compounding at the after-tax return of
    the path less inflation, plus the monthly saving — and Coast FI: what is
    needed today to reach the number by the retirement age with no more
    saving. The home you live in is left out: it houses you, it does not pay
    for anything."""
    p = board["profile"]
    exp = K._f(p.get("monthly_expenses")) * 12
    if exp <= 0:
        return None
    number = exp * K._f(p.get("fi_multiple"), 25.0)
    home = sum(r["value"] - r["loan"] for r in A.parse_owned_re(p.get("owned_re")) if r["use"] == "home")
    start = K._f(p.get("_net_worth")) - home
    infl = K._f(p.get("inflation"), 2.5)
    monthly = K._f(p.get("monthly_invest"))
    age = p.get("age")
    retire = K._f(p.get("retire_age"), 65)

    def path(r_nom):
        if r_nom is None:
            return None
        r = (1 + r_nom / 100) / (1 + infl / 100) - 1
        v, m = start, 0
        while v < number and m < 600:
            v = v * (1 + r) ** (1 / 12) + monthly
            m += 1
        yrs = m / 12
        coast = number / (1 + r) ** (retire - age) if age is not None and retire > age else None
        return {"years": yrs if m < 600 else None, "year": today.year + (today.month - 1) / 12 + yrs if m < 600 else None,
                "age": (age + yrs) if (age is not None and m < 600) else None, "real": r * 100, "coast": coast}

    now = path(hold.get("now_pct") if hold else None)
    opt = path(hold.get("opt_pct") if hold else None)
    return {"number": number, "expenses": exp, "multiple": K._f(p.get("fi_multiple"), 25.0), "start": start,
            "home_left_out": home, "progress": max(0.0, start / number * 100), "inflation": infl, "monthly": monthly,
            "age": age, "retire_age": retire, "now": now, "opt": opt,
            "withdrawal": 100 / K._f(p.get("fi_multiple"), 25.0)}


def _payoff(balance: float, apr: float, payment: float) -> tuple[int | None, float]:
    r, m, interest = apr / 1200, 0, 0.0
    while balance > 0.005 and m < 600:
        i = balance * r
        if payment <= i:
            return None, interest                    # the payment never covers the interest
        interest += i
        balance = balance + i - payment
        m += 1
    return m, interest


def debt_plan(board: dict, todo: list[dict], today: date) -> list[dict]:
    """Each debt: paid by a step, or its payoff month and the interest left at
    its payment — and what $100 more a month would save, set against what that
    $100 earns in the best use left."""
    p = board["profile"]
    # What an extra $100 a month would otherwise do: the month's best use — for
    # picks, the top picks as a group, since new money is split across them.
    win = (board.get("plan") or {}).get("winner") or {}
    sl = A.sleeve_of(board)
    best = (sum(r["ret_net"] for r in sl) / len(sl) if win.get("kind") == "stock" and sl
            else K._f(win.get("ret_net"), K._f(p.get("rf_rate"), 4.0) * (1 - K.tax_rates(p)["tbill"])))
    by_step = {}
    for s in todo:
        for m in s.get("parts") or []:
            if m["to_kind"] == "debt":
                by_step[m["to_id"].split(":", 1)[1]] = s
    import timeline as T
    plan = T.debt_payoffs(board.get("timeline"))
    mr = K._f(p.get("market_return"), 7.0)
    out = []
    for d in p.get("debts") or []:
        bal, apr, pay = K._f(d.get("balance")), K._f(d.get("apr")), K._f(d.get("payment"))
        row = {"name": d["name"], "balance": bal, "apr": apr, "payment": pay or None,
               "below_hurdle": apr < mr}
        step = by_step.get(d["name"])
        if step:
            row["step"] = step["rank"]
        # THE PLAN'S DATE FIRST: the monthly plan pays a debt above the hurdle
        # from what you put aside, so its payoff is the plan's, not the
        # payment's alone (the panel had said January where the plan said now)
        if d["name"] in plan:
            row.update(plan_payoff=plan[d["name"]]["label"], plan_months=plan[d["name"]]["months"])
        if pay > 0:
            m, interest = _payoff(bal, apr, pay)
            row.update(months=m, interest=interest)
            if m is not None:
                yy, mm = divmod(today.month - 1 + m, 12)
                row["payoff"] = date(today.year + yy, mm + 1, 1).strftime("%b %Y")
                m2, i2 = _payoff(bal, apr, pay + 100)
                if m2 is not None and "plan_payoff" not in row:
                    yy, mm = divmod(today.month - 1 + m2, 12)
                    row.update(extra_payoff=date(today.year + yy, mm + 1, 1).strftime("%b %Y"),
                               extra_saves=interest - i2, extra_months=m - m2,
                               extra_verdict="pay extra" if apr > best else "keep the minimum", best=best)
        out.append(row)
    return out


def passive_income(board: dict) -> dict | None:
    """What arrives without work, a year, against a year of expenses: rent
    after the loan payment (rentals and the other units of a house hack),
    interest on cash at its rate, and dividends in taxable accounts (money in
    a 401(k), IRA or HSA cannot be spent yet)."""
    p = board["profile"]
    exp = K._f(p.get("monthly_expenses")) * 12
    rent = sum((r["rent"] - r["costs"] - r["payment"]) * 12 for r in A.parse_owned_re(p.get("owned_re"))
               if r["use"] in ("rental", "house_hack") and r["rent"] > 0)
    hold = A.parse_holdings(p.get("holdings"))[0]
    rows = A.stock_rows_of(board)
    interest = sum(h["value"] * (h["rate"] or 0) / 100 for h in hold if h["account"] == "cash" and h["rate"] is not None)
    divs = 0.0
    for h in hold:
        if h["account"] == "taxable":
            est = A.estimate(h, p, rows)
            divs += h["value"] * (est[1] if est else 0) / 100
    parts = {"Rent after the loan": rent, "Dividends": divs, "Interest": interest}
    total = sum(parts.values())
    if total <= 0 and not hold:
        return None
    return {"parts": parts, "total": total, "expenses": exp, "cover": total / exp * 100 if exp > 0 else None}


def liquidity(board: dict) -> dict | None:
    """How fast each dollar can be reached: cash today; taxable holdings in
    days; retirement accounts only with a penalty before 59½ (Roth
    contributions excepted); property equity in months, by selling or
    borrowing."""
    p = board["profile"]
    hold = A.parse_holdings(p.get("holdings"))[0]
    owned = A.parse_owned_re(p.get("owned_re"))
    if not hold and not owned:
        return None
    eq = sum(max(0.0, r["value"] * 0.93 - r["loan"]) for r in owned)
    tiers = [("Today", "Cash and T-bills", K._f(p.get("cash"))),
             ("In days", "Taxable brokerage", sum(h["value"] for h in hold if h["account"] == "taxable")),
             ("With a penalty", "401(k), IRA, Roth and HSA before 59½",
              sum(h["value"] for h in hold if h["account"] in A.SHELTERED)),
             ("In months", "Property equity, after 7% to sell", eq)]
    total = sum(v for _, _, v in tiers)
    return {"tiers": [{"when": a, "what": b, "amount": v, "pct": v / total * 100 if total else 0} for a, b, v in tiers],
            "total": total}


# ═══════════════════════════════════════════════════════════════════
# PROPERTY, TAXES
# ═══════════════════════════════════════════════════════════════════

def properties(board: dict, today: date) -> list[dict]:
    """Each property's numbers and the inputs the page's keep-borrow-sell
    calculator runs on (it compares what each choice leaves after the hold)."""
    import headroom as HR
    p = board["profile"]
    t = K.tax_rates(p)
    rate = K._f(p.get("mortgage_rate")) or board.get("mortgage_rate") or 7.0
    sl = A.sleeve_of(board)
    picks = (sum(r["ret_after"] - r["cost_drag"] - r["time_drag"] for r in sl) / len(sl)) if sl else None
    items = {i.get("prop"): i for i in ((board.get("current") or {}).get("holdings") or {}).get("items", [])
             if i.get("property")}
    out = []
    for n, r in enumerate(A.parse_owned_re(p.get("owned_re"))):
        it = items.get(n) or A.measure_property(r, p, today, n)
        noi = (r["rent"] - r["costs"]) * 12
        years = today.year - r["year"] if r.get("year") else None
        dep = (r["basis"] * HR.BUILDING_SHARE / HR.DEP_YEARS
               if r["use"] == "rental" and r.get("basis") and (years is None or years < HR.DEP_YEARS) else 0.0)
        out.append({
            "index": n, "name": r["name"], "use": r["use"], "value": r["value"], "loan": r["loan"], "rate": r["rate"],
            "payment": r["payment"], "rent": r["rent"], "costs": r["costs"], "noi": noi,
            "cash_flow": noi + (r["shelter_rent"] * 12 if r["use"] != "rental" else 0) - r["payment"] * 12,
            "equity_out": it.get("value"), "h": it.get("h"), "why_not": it.get("why_not"),
            "sale_tax": it.get("sale_tax"), "dscr": noi / (12 * r["payment"]) if r["payment"] > 0 and noi > 0 else None,
            "cap_rate": noi / r["value"] * 100 if r["value"] > 0 else None,
            "ltv": r["loan"] / r["value"] * 100 if r["value"] > 0 else None,
            "calc": None if r["use"] != "rental" or it.get("h") is None or picks is None else {
                "V": r["value"], "L0": r["loan"], "r0": r["rate"], "P0": r["payment"], "noi": noi, "dep": dep,
                "ordinary": t["ordinary"], "h": it["h"], "E": it["value"], "sale_tax": it.get("sale_tax"),
                "picks": round(picks, 2), "hold": max(1, int(K._f(p.get("hold_years"), 5))),
                "time": r["hours"] * 12 * K._f(p.get("hourly_value")),
                "refi_rate": round(rate + 0.5, 2), "second_rate": round(rate + 1.25, 2)},
        })
    return out


def taxes(board: dict, todo: list[dict]) -> dict:
    """The rates every after-tax number uses; holdings that could be sold at a
    loss to cut this year's tax; Roth or traditional by the retirement rate;
    and how close income is to the Roth IRA limit."""
    p = board["profile"]
    t = K.tax_rates(p)
    selling = {m.get("line") for s in todo for m in s.get("parts") or []}
    harvest = []
    for h in A.parse_holdings(p.get("holdings"))[0]:
        if h["account"] == "taxable" and h["basis"] is not None and h["basis"] - h["value"] > 0.5:
            loss = h["basis"] - h["value"]
            harvest.append({"name": h["name"], "value": h["value"], "basis": h["basis"], "loss": loss,
                            "in_plan": h["line"] in selling})
    # $3,000 a year against pay IN ALL, not per holding: biggest loss first
    harvest.sort(key=lambda x: -x["loss"])
    room = TLH_ORDINARY_CAP
    for x in harvest:
        used = min(x["loss"], room)
        room -= used
        x.update(saves=used * t["ordinary"], carry=x["loss"] - used)
    flows = A.parse_flows(p.get("current_monthly"))[0]
    trad_401k = sum(f["amount"] for f in flows if f["account"] == "k401") * 12
    status = p.get("filing_status") or "single"
    lo, hi = K.LIMITS_2026["roth_phaseout"].get(status, K.LIMITS_2026["roth_phaseout"]["single"])
    magi = K._f(p.get("salary")) - trad_401k
    return {"fed": K._f(p.get("fed_rate")), "state": K._f(p.get("state_rate")), "ltcg": K._f(p.get("ltcg_rate")),
            "niit": bool(p.get("niit")), "ordinary": t["ordinary"] * 100, "qualified": t["qualified"] * 100,
            "retire": t["retire"] * 100, "market": K._f(p.get("market_return"), 7.0),
            "hold": max(1, int(K._f(p.get("hold_years"), 5))), "harvest": harvest,
            "roth_limit": {"status": status, "magi": magi, "start": lo, "end": hi,
                           "state": "below" if magi < lo else ("phasing" if magi < hi else "above")}
            if K._f(p.get("salary")) > 0 else None}


# ═══════════════════════════════════════════════════════════════════
# THE REST OF THE PAGE
# ═══════════════════════════════════════════════════════════════════

def board_groups(board: dict) -> list[dict]:
    groups = [("Debt, cash and accounts", lambda r: r["kind"] in ("debt", "tbill", "wrapper")),
              ("Stock picks", lambda r: r["kind"] == "stock"), ("Real estate", lambda r: r["kind"] == "re")]
    out = []
    for name, f in groups:
        rows = [r for r in board["rows"] if f(r)]
        out.append({"name": name, "rows": rows, "open": [r for r in rows if r.get("ret_net") is not None
                                                        and not r.get("blocked")]})
    return out


SYNC_STALE_DAYS = 30          # an export older than this is flagged for a fresh one


def synced(p: dict, today: date) -> list[dict]:
    """The accounts synced from a broker export: label, as of, age in days,
    and the block's lines and rows for the editor."""
    import positions as P
    text = str(p.get("holdings") or "")
    lines = text.splitlines()
    rows, _ = A.parse_holdings(text)
    out = []
    for b in P.blocks_in(text):
        d = P.describe_block(b)
        try:
            seen = date.fromisoformat((d["as_of"] or "")[:10])
        except ValueError:
            seen = None
        mine = [r for r in rows if b["start"] < r["line"] < b["end"]]
        out.append({**d, "text": "\n".join(lines[b["start"]:b["end"] + 1]),
                    "age": (today - seen).days if seen else None, "date": seen.isoformat() if seen else None,
                    "value": round(sum(r["value"] for r in mine), 2),
                    "rows": [{"name": r["name"], "account": "tbills" if r["state_exempt"] else EDIT_ACCOUNT[r["account"]],
                              "account_label": "T-bills" if r["state_exempt"] else _cap(A.ACCOUNT_LABEL[r["account"]]),
                              "value": r["value"], "basis": r["basis"], "rate": r["rate"], "qty": r.get("qty"),
                              "extra": _extra(r)} for r in mine]})
    return out


def _extra(r: dict) -> str:
    """The line's tokens the row editor has no column for — its dividend
    yield and its fund mark — so a save writes them back."""
    out = []
    if r.get("div") is not None:
        out.append(f"{A._n(r['div'])}% div")
    if r.get("fund"):
        out.append("fund")
    return ", ".join(out)


def _cap(s: str) -> str:
    return s[:1].upper() + s[1:]


def _typed_holdings(p: dict) -> tuple[list[dict], list[str]]:
    """The holdings lines the owner typed — outside any synced block (line
    numbers kept), with the lines that could not be read."""
    import positions as P
    lines = str(p.get("holdings") or "").splitlines()
    for b in P.blocks_in("\n".join(lines)):
        for n in range(b["start"], b["end"] + 1):
            lines[n] = ""
    return A.parse_holdings("\n".join(lines))


def setup(p: dict, today: date | None = None) -> list[dict]:
    """The profile as five steps, each with whether it is complete enough
    for the page to be right."""
    today = today or date.today()
    hold, bad = A.parse_holdings(p.get("holdings"))
    sync = synced(p, today)
    stale = [s for s in sync if s["age"] is None or s["age"] > SYNC_STALE_DAYS]
    owned = A.parse_owned_re(p.get("owned_re"))
    debts = p.get("debts") or []
    cash_unrated = [h["name"] for h in hold if h["account"] == "cash" and h["rate"] is None]
    no_basis = [h["name"] for h in hold if h["account"] == "taxable" and h["basis"] is None]
    untested = [r["name"] for r in owned if r["use"] == "rental" and (r["basis"] is None or r["year"] is None)]
    # a margin loan has no schedule — there is no payment to ask for
    no_pay = [d["name"] for d in debts if not d.get("payment") and not re.search(r"\bmargin\b", d["name"], re.I)]
    return [
        {"key": "pay", "title": "Pay and taxes",
         "done": K._f(p.get("monthly_invest")) > 0 and K._f(p.get("monthly_expenses")) > 0,
         "summary": (f"{_money(K._f(p.get('monthly_invest')))} a month to put to work · {_money(K._f(p.get('monthly_expenses')))} "
                     f"of expenses" + (f" · {_money(K._f(p.get('salary')))} salary" if K._f(p.get("salary")) else "")),
         "todo": [x for x, ok in (("what you can put aside a month", K._f(p.get("monthly_invest")) > 0),
                                  ("monthly expenses", K._f(p.get("monthly_expenses")) > 0),
                                  ("take-home pay", K._f(p.get("take_home")) > 0)) if not ok]},
        {"key": "hold", "title": "What you hold", "done": bool(hold) and not cash_unrated and not bad,
         "summary": (f"{len(hold)} holding{'s' if len(hold) != 1 else ''} · "
                     f"{_money(sum(h['value'] for h in hold))}" if hold else "Nothing listed yet")
                    + "".join(f" · {s['label']} synced {_ago(s['age'])}" for s in sync),
         "todo": [f"a fresh export of {s['label']}" for s in stale]
                 + ([f"a rate for {', '.join(cash_unrated)}"] if cash_unrated else [])
                 + ([f"the cost basis of {', '.join(no_basis)}"] if no_basis else [])
                 + ([f"{len(bad)} line{'s' if len(bad) != 1 else ''} that could not be read"] if bad else [])},
        {"key": "re", "title": "Property you own", "done": not untested,
         "summary": ", ".join(r["name"] for r in owned) if owned else "None listed",
         "todo": [f"purchase price and year bought for {', '.join(untested)}"] if untested else []},
        {"key": "debts", "title": "Debts", "done": not no_pay,
         "summary": ", ".join(f"{d['name']} {_money(K._f(d.get('balance')))}" for d in debts) if debts else "None listed",
         "todo": [f"the monthly payment on {', '.join(no_pay)}"] if no_pay else []},
        {"key": "goals", "title": "You and your assumptions", "done": p.get("age") is not None,
         "summary": (f"Age {p['age']:g} · " if p.get("age") is not None else "") +
                    f"market {K._f(p.get('market_return'), 7):g}% · hold {K._f(p.get('hold_years'), 5):g} years",
         "todo": ["your age"] if p.get("age") is None else []},
    ]


def _ago(days: int | None) -> str:
    if days is None:
        return "(date unknown)"
    return "today" if days <= 0 else ("yesterday" if days == 1 else f"{days} days ago")


def live_summary(board: dict, now=None) -> dict | None:
    """What the live prices did: how many holdings are priced, as of when
    (New York time), today's move in dollars, which tickers kept their last
    synced value. None when no holding has a share count."""
    lv = board.get("live")
    if not lv or not lv.get("of"):
        return None
    from datetime import datetime, timezone
    from zoneinfo import ZoneInfo
    import stock_lookup as SL
    ny = ZoneInfo("America/New_York")
    label = None
    if lv.get("as_of"):
        t = datetime.fromisoformat(lv["as_of"]).astimezone(ny)
        today_ny = (now or datetime.now(timezone.utc)).astimezone(ny).date()
        label = t.strftime("%-I:%M %p ET") + ("" if t.date() == today_ny else t.strftime(", %b %-d"))
    held = sum(x["value"] for x in lv["lines"].values())
    day = lv.get("day_change") or 0.0
    return {"priced": lv["priced"], "of": lv["of"], "as_of": label, "day_change": round(day, 2),
            "day_pct": round(day / (held - day) * 100, 2) if held - day > 0 else None,
            "missing": lv.get("missing") or [], "open": SL.market_open(now)}


def headline(board: dict) -> dict:
    p = board["profile"]
    cur = board.get("current") or {}
    hold = cur.get("holdings")
    mix = hold["bars_now"] if hold else []
    return {"net_worth": K._f(p.get("_net_worth")), "mix": mix, "live": live_summary(board), "has_holdings": bool(hold and hold.get("base")),
            "now_pct": hold.get("now_pct") if hold else None, "opt_pct": hold.get("opt_pct") if hold else None,
            "now_dollars": hold.get("now_dollars") if hold else None, "opt_dollars": hold.get("opt_dollars") if hold else None,
            "gap": hold.get("gap_dollars") if hold else None, "one_time": hold.get("one_time") if hold else None,
            "gain_hold": hold.get("gain_hold") if hold else None, "hold_years": hold.get("hold_years") if hold else None}


def vitals(board: dict, fi: dict | None) -> list[dict]:
    p = board["profile"]
    cur = board.get("current") or {}
    exp = K._f(p.get("monthly_expenses"))
    target = K._f(p.get("emergency_months"), 6.0)
    out = []
    if exp > 0:
        runway = K._f(p.get("cash")) / exp
        out.append({"key": "runway", "label": "Cash runway", "value": f"{runway:.0f}", "unit": "months",
                    "detail": f"{target:g} is your target",
                    "chip": ("warn", f"{runway - target:.0f} mo idle") if runway > target + 2 else
                            ("bad", f"{target - runway:.0f} mo short") if runway < target else ("ok", "On target"),
                    "meter": min(1.0, runway / max(24.0, target * 2)), "mark": target / max(24.0, target * 2),
                    "color": "gold"})
    th = K._f(p.get("take_home"))
    if th > 0:
        rate = K._f(p.get("monthly_invest")) / th * 100
        out.append({"key": "saving", "label": "Saving", "value": f"{rate:.0f}%", "unit": "of take-home",
                    "detail": f"{_money(K._f(p.get('monthly_invest')))} of {_money(th)} a month",
                    "chip": ("ok", "Strong") if rate >= 20 else ("warn", "Under 20%"), "meter": min(1.0, rate / 100),
                    "mark": None, "color": "mint"})
    if cur.get("re_share") is not None:
        cap = cur.get("re_cap")
        out.append({"key": "re", "label": "Real estate", "value": f"{cur['re_share']:.0f}%", "unit": "of net worth",
                    "detail": f"Your cap at this net worth: {cap:g}%" if cap is not None else "No cap at this net worth",
                    "chip": ("bad", "Over cap") if cap is not None and cur["re_share"] > cap else ("ok", "Within cap"),
                    "meter": min(1.0, max(0.0, cur["re_share"] / 100)), "mark": cap / 100 if cap is not None else None,
                    "color": "re"})
    if fi:
        opt = fi.get("opt") or {}
        out.append({"key": "fi", "label": "Independence", "value": f"{fi['progress']:.0f}%",
                    "unit": f"of {_money(fi['number'])}",
                    "detail": (f"Optimal path gets there in {int(opt['year'])}" if opt.get("year") else
                               "List what you hold to date it"),
                    "chip": ("info", f"{fi['multiple']:g}× expenses"), "meter": min(1.0, fi["progress"] / 100),
                    "mark": None, "color": "primary"})
    return out


def build_view(board: dict, today: date | None = None) -> dict:
    today = today or date.today()
    p = board["profile"]
    cur = board.get("current") or {}
    hold = cur.get("holdings")
    steady = steady_plan(board, today)
    todo = do_next(board, steady)
    fi = independence(board, hold, today)
    hold_rows, hold_bad = _typed_holdings(p)
    flow_rows, flow_bad = A.parse_flows(p.get("current_monthly"))
    editor = {"holdings": [{"name": h["name"], "account": "tbills" if h["state_exempt"] else EDIT_ACCOUNT[h["account"]],
                            "value": h["value"], "basis": h["basis"], "rate": h["rate"], "qty": h.get("qty"),
                            "extra": _extra(h)} for h in hold_rows],
              "holdings_bad": hold_bad, "synced": synced(p, today),
              "flows": [{"name": f["name"], "account": "tbills" if f["state_exempt"] else EDIT_ACCOUNT[f["account"]],
                         "amount": f["amount"], "rate": f["rate"]} for f in flow_rows],
              "flows_bad": flow_bad}
    import timeline as T
    import whatif as W
    wk = W.kit(board)
    small = [s for s in todo if s["type"] == "once" and s["kind"] != "debt" and s["impact"] < SMALL_STEP]
    if len(small) < 2 or len(small) == len(todo):
        small = []                # one small step, or nothing but small ones: no point folding
    ids = {s["id"] for s in small}
    return {"headline": headline(board), "vitals": vitals(board, fi), "todo": todo, "editor": editor,
            "todo_main": [s for s in todo if s["id"] not in ids], "todo_small": small,
            "todo_small_gain": round(sum(s["impact"] for s in small), 2),
            "whatif": wk, "scenarios": W.scenarios(board, wk), "timeline": board.get("timeline") if "timeline" in board else T.simulate(board, today),
            "todo_once": sum(1 for s in todo if s["type"] == "once"),
            "todo_monthly": sum(1 for s in todo if s["type"] == "monthly"),
            "fi": fi, "debts": debt_plan(board, todo, today), "passive": passive_income(board),
            "liquidity": liquidity(board), "properties": properties(board, today), "taxes": taxes(board, todo),
            "steady": steady, "groups": board_groups(board), "setup": setup(p, today),
            "setup_done": sum(1 for s in setup(p, today) if s["done"])}

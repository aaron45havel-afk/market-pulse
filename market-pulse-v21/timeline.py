"""The monthly plan, month by month: the owner's monthly amount run through
the board's own waterfall (capital.waterfall), one month at a time, with what
each month changes carried into the next.

A single "this month" and a single "normal month" could not say when anything
happens — and the normal month put a down payment into T-bills forever. Here:

- The cushion and the emergency fund fill from the cash steps.
- A card's balance takes a month's interest at its APR, then what the plan
  pays it. A debt with a listed monthly payment is paid that from expenses
  (outside the plan); once it is gone, its payment joins the monthly amount.
- Roth IRA, HSA and 401(k) room is drawn down and resets each January (at the
  2026 limits; later years' limits are not known yet).
- A property the plan saves for: cash above the emergency fund starts the
  fund, each month's down-payment step adds to it, and once it holds the cash
  to close the plan stops saving for property (one deal at a time). After the
  months to close, the rent stops and the other units' rent less the full
  payment arrives: the monthly amount changes by both.
- Selling what is held is not assumed — that is Do next. Marking a step done
  edits the profile, and this reruns from the new state.

Months are calendar months from the current one. Nothing here grows at a
return: it says where each month's money goes and when each goal is met, not
what it will be worth (the overview's independence path does that).
"""
from __future__ import annotations

from datetime import date

import allocation as A
import capital as K

MAX_MONTHS = 120               # ten years: past that the plan is the steady month
SHOW_MONTHS = 24               # the chart's window
STEADY_AFTER = 2               # months the split must hold to call it steady

CATEGORY = {"cash": "Savings", "debt": "Debt", "match": "401(k)", "wrapper": "401(k)", "hsa": "HSA",
            "ira": "Roth IRA", "re": "Down payment", "stock": "Stock picks", "tbill": "T-bills", "left": "T-bills"}
CATEGORY_ORDER = ["Savings", "Debt", "401(k)", "HSA", "Roth IRA", "Down payment", "Stock picks", "T-bills"]
CATEGORY_CLASS = {"Savings": "sav", "Debt": "debt", "401(k)": "k401", "HSA": "hsa", "Roth IRA": "roth",
                  "Down payment": "dp", "Stock picks": "picks", "T-bills": "tb"}


def _add_months(d: date, n: int) -> date:
    y, m = divmod(d.month - 1 + n, 12)
    return date(d.year + y, m + 1, 1)


def _label(d: date) -> str:
    return d.strftime("%b %Y")


def simulate(board: dict, today: date | None = None, months: int = MAX_MONTHS) -> dict | None:
    """{"months": [...], "milestones": [...], "steady": {...} | None, ...};
    None when there is no monthly amount to plan."""
    today = today or date.today()
    p = board["profile"]
    base = K._f(p.get("monthly_invest"))
    if base <= 0:
        return None
    lim = K.LIMITS_2026
    mr = K._f(p.get("market_return"), 7.0)
    exp = K._f(p.get("monthly_expenses"))
    starter = exp * K._f(p.get("starter_months"), 1.0)
    ef = exp * K._f(p.get("emergency_months"), 6.0)
    sleeve = A.sleeve_of(board)

    cash = K._f(p.get("cash"))
    debts = [{"name": str(d.get("name")), "balance": K._f(d.get("balance")), "apr": K._f(d.get("apr")),
              "payment": K._f(d.get("payment"))} for d in p.get("debts") or [] if K._f(d.get("balance")) > 0]
    rooms = {"ira": K._f(p.get("ira_room")), "hsa": K._f(p.get("hsa_room")), "k401": K._f(p.get("k401_room"))}
    freed = 0.0                 # payments of loans paid off (when they came from expenses)
    from_expenses = p.get("debt_payments_from", "invest") == "expenses"
    housing = 0.0               # rent stopped + net rent, once moved in
    dp_saved, deal, ready_i, move_in_i = 0.0, None, None, None
    done_once = {"cushion": cash >= starter, "ef": cash >= ef}
    start = date(today.year, today.month, 1)
    out_months, milestones = [], []
    steady_i = None
    prev_split = None
    prev_clean = False          # the month before began with no one-time goal left
    open_goals = True           # one-time goals left at the end of the last month
    same = 0

    def event(i, d, kind, text, detail="", name=None):
        milestones.append({"i": i, "date": d.isoformat()[:7], "label": _label(d), "kind": kind, "text": text,
                           "detail": detail, "name": name})

    for i in range(months):
        d = _add_months(start, i)
        if i and d.month == 1:
            rooms = {"ira": float(lim["ira"]), "hsa": float(lim["hsa_self"]), "k401": float(lim["k401"])}
        events_before = len(milestones)
        if i:
            for x in debts:
                if x["balance"] <= 0:
                    continue
                x["balance"] *= 1 + x["apr"] / 1200
                # paid from expenses, outside the plan; from what you put
                # aside, the waterfall's own step pays it
                if x["payment"] > 0 and from_expenses:
                    x["balance"] = max(0.0, x["balance"] - x["payment"])
                    if x["balance"] <= 0.5:
                        x["balance"] = 0.0
                        freed += x["payment"]
                        event(i, d, "debt", f"{x['name']} paid off",
                              f"its ${x['payment']:,.0f}/mo payment joins your monthly amount", name=x["name"])
        if move_in_i is not None and i == move_in_i:
            det = (deal or {}).get("detail") or {}
            if det.get("owner_occupied"):
                # the rent stops; the place's own cash flow after the full payment starts
                housing = K._f(det.get("rent_saved")) + K._f(det.get("monthly_surplus"))
                s = K._f(det.get("monthly_surplus"))
                parts = ([f"${K._f(det.get('rent_saved')):,.0f}/mo rent stops"] if K._f(det.get("rent_saved")) > 0 else [])
                parts.append((f"the other units {'add' if s >= 0 else 'cost'} ${abs(s):,.0f}/mo after the full payment"
                              if deal["id"].startswith("hh:") else
                              f"owning costs ${abs(s):,.0f}/mo {'less' if s >= 0 else 'more'} than renting there"))
                event(i, d, "move_in", "Close and move in",
                      "; ".join(parts) + f" — {'+' if housing >= 0 else '−'}${abs(housing):,.0f}/mo to your monthly amount")
            else:
                event(i, d, "move_in", "Close on the deal", "its cash flow is not added to the plan (a conditional deal)")

        live = {x["name"] for x in debts if x["balance"] > 0.5}
        rows = [r for r in board["rows"]
                if not (r["kind"] == "debt" and r["id"].split(":", 1)[-1] not in live)
                and not (r["kind"] == "re" and ready_i is not None)]
        pm = {**p, "cash": cash, "monthly_invest": base + freed + housing,
              "debts": [{"name": x["name"], "balance": x["balance"], "apr": x["apr"], "payment": x["payment"]}
                        for x in debts if x["balance"] > 0.5],
              "ira_room": rooms["ira"], "hsa_room": rooms["hsa"], "k401_room": rooms["k401"]}
        # Cash above the emergency fund starts a down payment the plan is
        # saving for — the board's "ready in" already counted it.
        wf = K.waterfall(pm, rows, date(d.year, d.month, 15))
        win = wf.get("winner")
        if win and win["kind"] == "re" and ready_i is None and deal is None:
            deal = win
            spare = max(0.0, cash - ef)
            if spare > 0.5:
                dp_saved += spare
                cash -= spare
                pm["cash"] = cash
                wf = K.waterfall(pm, rows, date(d.year, d.month, 15))
                event(i, d, "dp_start", f"Down-payment fund started for {deal['label'].split(' — ')[-1]}",
                      f"${spare:,.0f} of cash above the emergency fund")
        steps, left = [], wf["left"]
        for s in wf["steps"]:
            k, amt = s["kind"], s["amount"]
            if k == "cash":
                cash += amt
            elif k == "match":
                rooms["k401"] = max(0.0, rooms["k401"] - amt)
            elif k in ("hsa", "ira"):
                rooms[k] = max(0.0, rooms[k] - amt)
            elif k == "debt":
                # the fixed steps name the debt; the board's own debt row is "debt:<name>"
                name = str(s.get("ref") or "").split(":", 1)[-1]
                x = next((x for x in debts if x["name"] == name), None)
                if x:
                    paid = min(amt, x["balance"])
                    x["balance"] -= paid
                    if paid < amt - 0.005:             # the cheap debt the board ranked top is gone
                        left += amt - paid
                        amt = paid
                    if x["balance"] <= 0.5 and paid > 0:
                        x["balance"] = 0.0
                        if x["payment"] > 0 and from_expenses:
                            freed += x["payment"]
                        event(i, d, "debt", f"{x['name']} paid off",
                              f"its ${x['payment']:,.0f}/mo payment joins your monthly amount"
                              if x["payment"] > 0 and from_expenses else
                              f"its ${x['payment']:,.0f}/mo payment goes to the rest of the plan" if x["payment"] > 0
                              else f"a guaranteed {x['apr']:g}% no longer charged", name=x["name"])
            elif k == "re" and deal:
                take = min(amt, max(0.0, K._f(deal["min_capital"]) - dp_saved))
                left += amt - take
                amt = take
                dp_saved += take
            if amt > 0.005:
                cat = CATEGORY.get(k, "T-bills")
                steps.append({"to": s["to"], "amount": round(amt, 2), "kind": k, "ref": s.get("ref"),
                              "category": cat, "cls": CATEGORY_CLASS[cat]})
        if not done_once["cushion"] and cash >= starter - 0.5:
            done_once["cushion"] = True
            event(i, d, "cushion", "Starter cushion in place", f"${starter:,.0f}, a month of expenses")
        if not done_once["ef"] and cash >= ef - 0.5:
            done_once["ef"] = True
            event(i, d, "ef", "Emergency fund full", f"${ef:,.0f}, {K._f(p.get('emergency_months'), 6):g} months of expenses")
        if deal and ready_i is None and dp_saved >= K._f(deal.get("min_capital")) - 0.5:
            ready_i = i
            move_in_i = i + max(1, int(deal.get("deploy_months") or 0))
            event(i, d, "dp_ready", f"Down payment ready — {deal['label'].split(' — ')[-1]}",
                  f"${K._f(deal.get('min_capital')):,.0f} to close; about {move_in_i - i} months to close and move in")

        rets = A.step_returns([{**s, "kind": s["kind"]} for s in steps], left, {**board, "profile": pm}, sleeve)
        for s, r in zip(steps, rets):
            s["ret"] = r["ret"]
        total = round(pm["monthly_invest"], 2)          # what came in this month, before it freed anything
        month = {"i": i, "date": d.isoformat()[:7], "label": _label(d), "short": d.strftime("%b"),
                 "year": d.year, "total": total, "steps": steps, "left": round(left, 2),
                 "events": milestones[events_before:],
                 "by_category": _by_category(steps, left)}
        out_months.append(month)

        # STEADY: months that each BEGAN with every one-time goal met (no
        # fund to fill, no debt above the hurdle, no property still to close)
        # and split the money the same way.
        clean = not open_goals
        open_goals = (not done_once["ef"] or any(x["balance"] > 0.5 and x["apr"] >= mr for x in debts)
                      or (deal is not None and (move_in_i is None or i + 1 < move_in_i)))
        split = tuple(sorted((s["category"], round(s["amount"])) for s in steps))
        same = same + 1 if (clean and prev_clean and split == prev_split) else 0
        prev_split, prev_clean = split, clean
        if steady_i is None and same >= STEADY_AFTER - 1:
            steady_i = i - (STEADY_AFTER - 1)
        if steady_i is not None and i >= SHOW_MONTHS - 1:
            break

    steady = None
    if steady_i is not None:
        # A January shows a normal year: the Roth and HSA room spread over
        # twelve months, not this year's catch-up over the months left.
        sm = next((m for m in out_months[steady_i:] if m["date"].endswith("-01")), out_months[steady_i])
        steady = {**sm, "from_label": out_months[steady_i]["label"],
                  "per_year": round(sum(s["amount"] * (s["ret"] or 0) / 100 for s in sm["steps"])
                                    + sm["left"] * _tbill(p) / 100, 2) * 12}
        if not any(m["kind"] == "steady" for m in milestones):
            first = out_months[steady_i]
            milestones.append({"i": steady_i, "date": first["date"], "label": first["label"], "kind": "steady",
                               "text": "Steady month",
                               "detail": "every one-time goal met — the same split from here"})
    milestones.sort(key=lambda m: (m["i"], m["kind"] == "steady"))
    shown = out_months[:max(SHOW_MONTHS, (steady_i or 0) + STEADY_AFTER)]
    peak = max((m["total"] for m in shown), default=0.0) or 1.0
    return {"months": shown, "milestones": milestones, "steady": steady, "steady_i": steady_i,
            "horizon": len(out_months), "base": base, "peak": peak,
            "categories": [{"name": c, "cls": CATEGORY_CLASS[c]} for c in CATEGORY_ORDER
                           if any(c in m["by_category"] for m in shown)],
            "deal": ({"label": deal["label"], "cash": K._f(deal.get("min_capital")), "ready_i": ready_i,
                      "move_in_i": move_in_i} if deal else None)}


def debt_payoffs(tl: dict | None) -> dict[str, dict]:
    """{debt name: {"months", "label"}} — when the plan, from pay alone,
    clears each debt (0 = this month's pay does). A debt not in it outlasts
    the simulation."""
    out = {}
    for m in (tl or {}).get("milestones") or []:
        if m["kind"] == "debt" and m.get("name") and m["name"] not in out:
            out[m["name"]] = {"months": m["i"], "label": m["label"]}
    return out


def _tbill(p: dict) -> float:
    return K._f(p.get("rf_rate"), 4.0) * (1 - K.tax_rates(p)["tbill"])


def _by_category(steps: list[dict], left: float) -> dict:
    out: dict[str, float] = {}
    for s in steps:
        out[s["category"]] = round(out.get(s["category"], 0.0) + s["amount"], 2)
    if left > 0.5:
        out["T-bills"] = round(out.get("T-bills", 0.0) + left, 2)
    return out

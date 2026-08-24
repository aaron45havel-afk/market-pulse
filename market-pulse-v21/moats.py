"""
Permit-moat watchlist — companies whose advantage is a physical or legal
asset that cannot be replicated. A salt mine, an adjudicated water right,
an NRC license, a Jones Act hull, consecrated cemetery land.

THIS MODULE IS NOT A SCREENER. Every other board in this app measures
companies. This one measures the user's own discipline, and it exists
because the strategy — wait for an unrepeatable asset to fall to a
pre-set price, then buy — only pays if you can act when the price
actually falls. The price only falls when the news is bad. So:

  1. COMMITMENT. The plan is written while calm and locked. When the
     target triggers, the app shows the user what they wrote and makes
     them read it. Rewriting the thesis mid-drawdown is the exact
     failure being defended against, so editing a locked plan is
     deliberately awkward and the previous text is kept forever.

  2. FUNNEL. Adding a name is free. Promoting one to where it can
     trigger a purchase is gated. All the friction sits at the promotion
     boundary and nowhere else, so the list can grow for years without
     rotting into an unreviewed drawer of eighty tickers.

Pure: no database, no network, no clock. Every function that needs the
time takes `now` as an argument, so the tests are deterministic and the
whole file is provable offline. The persistence lives in database.py and
the routes in main.py; nothing here knows about either.

NOTHING HERE RECOMMENDS A TRADE. The app reflects the user's own targets
back at them and says nothing about whether a target is wise.
"""
from __future__ import annotations

import csv
import io
import re
from datetime import datetime, timedelta


class Invalid(ValueError):
    """Rejected input, with a message meant to be shown to the user."""


# ── vocabulary ───────────────────────────────────────────────────────
STAGES = ("CANDIDATE", "QUALIFIED", "ARMED", "ARCHIVED")

MOAT_TYPES = {
    "SHIPPING_RADIUS": "Shipping radius",
    "MINERAL_DEPOSIT": "Mineral deposit",
    "WATER_RIGHT": "Water right",
    "LICENSE": "License",
    "JONES_ACT": "Jones Act",
    "LAND_USE": "Land use",
}
SENTIMENTS = {
    "BID_UP": "Bid up",
    "OUT_OF_FAVOR": "Out of favor",
    "NEUTRAL": "Neutral",
    "SPECULATIVE": "Speculative",
}
PERMIT_TRENDS = ("DECLINING", "FLAT", "RISING", "UNKNOWN")
TERMINAL_DEMAND = ("INTACT", "UNCERTAIN", "DECLINING")
CHEAP_BECAUSE = ("OPERATIONAL_STUMBLE", "CYCLICAL_TROUGH",
                 "STRUCTURAL_DECLINE", "NOT_CHEAP")

# Capital is finite and the cap is the app admitting it. Candidates and
# qualified names are uncapped — only the stage that can actually spend
# money is limited.
ARMED_CAP = 15
DECAY_DAYS = 90          # a candidate untouched this long needs triage
STALE_DAYS = 30          # a price older than this is visibly stale
EVIDENCE_MIN = 120       # characters — an unsourced rubric is a guess

# Fields a holding must carry before it can leave CANDIDATE. Enforced
# here in application logic, not as NOT NULL, because a candidate is
# allowed to be an empty shell and the same table holds both.
QUALIFY_FIELDS = ("assetLine", "thesis", "invalidation",
                  "moatType", "sentiment")

# Gauge geometry. The target notch sits at 40% rather than centre so the
# buy zone — the only part that matters — gets 40% of the bar while the
# long boring stretch above target is compressed into the other 60%.
GAUGE_LOW, GAUGE_HIGH, GAUGE_NOTCH = 0.6, 1.6, 40.0


# ── the rubric ───────────────────────────────────────────────────────
#
# Two gates and a score. The gates decide; the score only ranks. Keeping
# them separate is the whole design: a high score must never buy its way
# past a gate, because the gates are the two ways this strategy actually
# dies.
SCORE_CRITERIA = (
    ("replicability", "Cannot be reproduced with capital alone"),
    ("permit_trend", "Permits getting harder to obtain"),
    ("freight", "Freight is 30%+ of delivered value"),
    ("terminal_demand", "Demand intact on a 50-year view"),
    ("pricing", "Cheap for a fixable reason"),
)


def score(rubric: dict) -> int:
    """0–5. Advisory only — it ranks, it does not decide."""
    r = rubric or {}
    got = 0
    if r.get("replicableWithCapital") is False:
        got += 1
    if r.get("permitTrend") == "DECLINING":
        got += 1
    freight = r.get("freightPctOfValue")
    if isinstance(freight, (int, float)) and freight >= 30:
        got += 1
    if r.get("terminalDemand50yr") == "INTACT":
        got += 1
    if r.get("cheapBecause") in ("OPERATIONAL_STUMBLE", "CYCLICAL_TROUGH"):
        got += 1
    return got


def gate_failures(rubric: dict) -> list[dict]:
    """The hard stops, in plain language. Empty list means both cleared.

    THERE IS NO OVERRIDE PATH for either of these, in this function or
    in the UI. That is deliberate. A gate you can click past is not a
    gate, and both of these are the kind of mistake that is invisible at
    the time and obvious in hindsight.
    """
    r = rubric or {}
    out = []
    if r.get("replicableWithCapital") is True:
        out.append({
            "gate": "replicableWithCapital",
            "message": "If capital alone reproduces the asset, there is no "
                       "moat — only capital intensity. This is the test "
                       "that separates an unrepeatable permit from a "
                       "company that merely owns expensive machines.",
        })
    if r.get("cheapBecause") == "STRUCTURAL_DECLINE":
        out.append({
            "gate": "cheapBecause",
            "message": "A melting asset is not a discount. Structural "
                       "decline means the price is low because the "
                       "business is going away, which no entry price "
                       "fixes.",
        })
    return out


def evaluate(rubric: dict) -> dict:
    """Score, gates, and whether this rubric passes. Never mutates input."""
    fails = gate_failures(rubric)
    return {
        "score": score(rubric),
        "max_score": len(SCORE_CRITERIA),
        "passed": not fails,
        "failures": fails,
    }


def validate_rubric(payload: dict) -> dict:
    """Normalise a submitted rubric, or raise Invalid with a usable message.

    `passed` and `score` are DERIVED here and overwrite anything the
    caller sent. They are conclusions about the answers, and a client
    that could post its own conclusion could post `passed: true` past a
    gate — which is the one thing this module exists to prevent.
    """
    p = payload or {}

    rep = p.get("replicableWithCapital")
    if isinstance(rep, str):
        rep = {"true": True, "false": False}.get(rep.strip().lower())
    if rep not in (True, False):
        raise Invalid("Answer whether capital alone could reproduce the asset.")

    trend = (p.get("permitTrend") or "").strip().upper()
    if trend not in PERMIT_TRENDS:
        raise Invalid(f"Permit trend must be one of: {', '.join(PERMIT_TRENDS)}.")

    demand = (p.get("terminalDemand50yr") or "").strip().upper()
    if demand not in TERMINAL_DEMAND:
        raise Invalid(f"50-year demand must be one of: {', '.join(TERMINAL_DEMAND)}.")

    cheap = (p.get("cheapBecause") or "").strip().upper()
    if cheap not in CHEAP_BECAUSE:
        raise Invalid(f"Why it is cheap must be one of: {', '.join(CHEAP_BECAUSE)}.")

    freight = p.get("freightPctOfValue")
    if freight in ("", None):
        freight = None
    else:
        try:
            freight = int(float(freight))
        except (TypeError, ValueError):
            raise Invalid("Freight percent must be a number, or left blank.")
        if not 0 <= freight <= 100:
            raise Invalid("Freight percent must be between 0 and 100.")

    evidence = (p.get("evidence") or "").strip()
    if len(evidence) < EVIDENCE_MIN:
        raise Invalid(
            f"Evidence must be at least {EVIDENCE_MIN} characters — where "
            f"these answers came from. You wrote {len(evidence)}. A rubric "
            f"without a source is a guess wearing a score."
        )

    out = {
        "replicableWithCapital": rep,
        "permitTrend": trend,
        "freightPctOfValue": freight,
        "terminalDemand50yr": demand,
        "cheapBecause": cheap,
        "evidence": evidence,
        "grandfathered": bool(p.get("grandfathered")),
    }
    ev = evaluate(out)
    out["score"] = ev["score"]
    out["passed"] = ev["passed"]
    return out


# The twelve seeds were judged before this app existed. Every answer on
# their rubric is None ON PURPOSE: inventing rubric answers for them
# would put fabricated evidence into the one record the whole design
# depends on. `score` is None rather than 0 because 0 is a result and
# this is an absence — a board showing "0/5" would be asserting they
# scored badly, which nobody measured.
GRANDFATHERED_EVIDENCE = (
    "Assessed before this watchlist existed. No rubric was filled in, so "
    "there are no answers here and no score was computed. Re-review to "
    "replace this with a real assessment."
)


def grandfathered_rubric() -> dict:
    return {
        "replicableWithCapital": None, "permitTrend": None,
        "freightPctOfValue": None, "terminalDemand50yr": None,
        "cheapBecause": None, "evidence": GRANDFATHERED_EVIDENCE,
        "score": None, "passed": True, "grandfathered": True,
    }


def rubric_is_complete(rubric: dict | None) -> bool:
    """Whether a rubric actually answered its questions.

    THIS EXISTS BECAUSE A BLANK RUBRIC PASSES BOTH GATES. The gates fire
    on specific disqualifying ANSWERS — `replicable is True`, `cheap
    because STRUCTURAL_DECLINE` — so a form with nothing filled in trips
    neither and would otherwise sail through promotion. Absence of a
    disqualifying answer is not evidence of a qualifying one.
    """
    r = rubric or {}
    if r.get("grandfathered"):
        return True
    return (r.get("replicableWithCapital") in (True, False)
            and r.get("permitTrend") in PERMIT_TRENDS
            and r.get("terminalDemand50yr") in TERMINAL_DEMAND
            and r.get("cheapBecause") in CHEAP_BECAUSE
            and len((r.get("evidence") or "").strip()) >= EVIDENCE_MIN)


def display_score(rubric: dict | None) -> str:
    """'4/5', or 'not scored' for a grandfathered placeholder."""
    r = rubric or {}
    if r.get("grandfathered") or r.get("score") is None:
        return "not scored"
    return f"{r['score']}/{len(SCORE_CRITERIA)}"


# ── stage transitions ────────────────────────────────────────────────
def missing_qualify_fields(holding: dict) -> list[str]:
    """Which of the thesis fields are still blank."""
    h = holding or {}
    return [f for f in QUALIFY_FIELDS if not (h.get(f) or "").strip()]


def can_promote(holding: dict, rubric: dict | None) -> dict:
    """CANDIDATE -> QUALIFIED. {ok, reasons[], failures[]}.

    `failures` carries the gate objects so the UI can name which gate
    failed rather than saying "rejected". A refusal that does not say
    what would have to be different is not a refusal, it is a wall.
    """
    h = holding or {}
    reasons: list[str] = []

    stage = h.get("stage")
    if stage == "ARCHIVED":
        reasons.append("This holding is archived. Restore it first.")
    elif stage not in ("CANDIDATE", None):
        reasons.append(f"Only a candidate can be promoted — this one is {stage}.")

    if not rubric:
        reasons.append("No rubric has been completed for this holding.")
        return {"ok": False, "reasons": reasons, "failures": []}
    if not rubric_is_complete(rubric):
        reasons.append("The rubric is incomplete. Both gates fire on a "
                       "specific disqualifying answer, so a half-filled "
                       "form trips neither — leaving it blank is not the "
                       "same as passing.")

    fails = gate_failures(rubric)
    for f in fails:
        reasons.append(f["message"])

    missing = missing_qualify_fields(h)
    if missing:
        reasons.append("Still blank: " + ", ".join(_label(m) for m in missing) + ".")

    return {"ok": not reasons, "reasons": reasons, "failures": fails}


def _label(field: str) -> str:
    return {
        "assetLine": "the one irreplaceable thing it owns",
        "thesis": "thesis",
        "invalidation": "invalidation",
        "moatType": "moat type",
        "sentiment": "sentiment",
    }.get(field, field)


def can_arm(holding: dict, position: dict | None, armed_count: int,
            rubric: dict | None = None) -> dict:
    """QUALIFIED -> ARMED. Target, price, plan, AND a completed rubric.

    `armed_count` is the number ALREADY armed, excluding this one.

    THE RUBRIC REQUIREMENT IS ENFORCED AT THIS BOUNDARY, NOT AT
    QUALIFICATION. Grandfathering exists so the twelve seeds can sit at
    QUALIFIED without a rubric they were never given — they were judged
    before this app existed. It was never meant to carry them all the
    way to armed, which is the stage that can trigger a real purchase
    decision. A holding whose every rubric answer is null must not be
    able to demand money of you.

    `rubric` defaults to None, which reads as incomplete and REFUSES.
    Failing closed is deliberate: a caller that forgets to pass the
    rubric gets a loud refusal rather than a silent hole in the one gate
    that stands between a null assessment and an armed position.
    """
    h = holding or {}
    p = position or {}
    reasons: list[str] = []
    needs_rubric = not rubric_is_complete_for_arming(rubric)

    if h.get("stage") != "QUALIFIED":
        reasons.append(
            f"Only a qualified holding can be armed — this one is "
            f"{h.get('stage') or 'unset'}."
        )
    if needs_rubric:
        reasons.append("Arming needs a completed rubric. This holding has "
                       "not been through the questions yet.")
    else:
        for f in gate_failures(rubric):
            reasons.append(f["message"])
    if p.get("targetPrice") in (None, ""):
        reasons.append("Set the price you would actually buy at.")
    if p.get("lastPrice") in (None, ""):
        reasons.append("Enter what it trades at now.")
    if not (p.get("plan") or "").strip():
        reasons.append("Write what you will do when it hits the target. "
                       "This is the commitment the app exists to hold you to.")

    at_cap = armed_count >= ARMED_CAP
    if at_cap:
        reasons.append(
            f"{ARMED_CAP} armed already. Capital is finite — disarm "
            f"something before arming this."
        )
    # `needs_rubric` travels back so the UI can route the user into the
    # rubric form rather than showing a dead button. A refusal with
    # nowhere to go is the same as hiding the action.
    return {"ok": not reasons, "reasons": reasons, "at_cap": at_cap,
            "needs_rubric": needs_rubric}


def rubric_is_complete_for_arming(rubric: dict | None) -> bool:
    """Like rubric_is_complete, but grandfathering does NOT count.

    The two differ on exactly one case and it is the whole point of this
    function: a grandfathered placeholder is enough to hold a seed at
    QUALIFIED, and is not enough to arm it. Anywhere else they agree.
    """
    r = rubric or {}
    if r.get("grandfathered"):
        return False
    return rubric_is_complete(r)


def can_archive(reason: str) -> dict:
    """Archiving always requires a reason, at every stage.

    The record of what was passed on and why is the most valuable thing
    this app accumulates over a decade, and it is worth nothing if half
    the entries say nothing. Nothing is ever hard-deleted.
    """
    r = (reason or "").strip()
    if len(r) < 3:
        return {"ok": False, "reasons": ["Say why. In a year this is the "
                                         "only thing that will explain the "
                                         "decision."]}
    return {"ok": True, "reasons": [], "reason": r}


# ── position math ────────────────────────────────────────────────────
def _num(v):
    """Float or None. Never raises, never guesses a value for junk."""
    if v is None or v == "":
        return None
    try:
        f = float(v)
    except (TypeError, ValueError):
        return None
    return None if f != f else f          # NaN is not a price


def distance_pct(last, target):
    """How far above the target, in percent. Negative means below it.

    None when either side is unknown or the target is zero. Never 0 —
    "at the target" is the single most consequential reading on the
    page and must not be what a missing number looks like.
    """
    l, t = _num(last), _num(target)
    if l is None or t is None or t == 0:
        return None
    return (l - t) / t * 100.0


def is_triggered(last, target) -> bool:
    """At or below the target. Both sides must be known."""
    l, t = _num(last), _num(target)
    return l is not None and t is not None and l <= t


def gauge(last, target) -> dict:
    """Where the marker sits on the bar, 0–100, plus a spoken label.

    The scale is deliberately not linear across its whole width: the
    notch is at GAUGE_NOTCH so the buy zone gets 40% of the bar and
    everything above target is squeezed into the rest. Outside
    [GAUGE_LOW, GAUGE_HIGH] the marker clamps to an end and `clamped`
    says so, because a marker resting on the edge otherwise reads as a
    real position rather than "further than this bar can show".
    """
    l, t = _num(last), _num(target)
    if l is None or t is None or t <= 0:
        return {"pos": None, "ratio": None, "in_buy_zone": False,
                "clamped": False, "notch": GAUGE_NOTCH,
                "label": "No gauge — enter a target and a price."}

    ratio = l / t
    if ratio <= 1.0:
        pos = (ratio - GAUGE_LOW) / (1.0 - GAUGE_LOW) * GAUGE_NOTCH
    else:
        pos = GAUGE_NOTCH + (ratio - 1.0) / (GAUGE_HIGH - 1.0) * (100.0 - GAUGE_NOTCH)
    clamped = not (GAUGE_LOW <= ratio <= GAUGE_HIGH)
    pos = max(0.0, min(100.0, pos))

    d = (ratio - 1.0) * 100.0
    if ratio <= 1.0:
        spoken = (f"In the buy zone — {abs(d):.0f}% below target"
                  if d < -0.5 else "At target")
    else:
        spoken = f"{d:.0f}% above target"
    if clamped:
        spoken += " (beyond the range of this gauge)"

    return {"pos": round(pos, 2), "ratio": round(ratio, 4),
            "in_buy_zone": ratio <= 1.0, "clamped": clamped,
            "notch": GAUGE_NOTCH, "label": spoken}


def apply_price(position: dict, new_price, now: datetime) -> dict:
    """Record a price. Returns the fields that changed, plus what happened.

    Trigger stamps ONCE — the first time the price is at or below the
    target — and is never cleared by the price recovering. The point is
    that the moment happened and the user has to have faced their own
    plan; a trigger that quietly un-set itself on a bounce would let
    exactly the decision this app exists for slip past unmade.

    `acknowledged` is reset to False only on that first stamp, so an
    acknowledgement already given is not demanded again on every tick.
    """
    p = dict(position or {})
    price = _num(new_price)
    if price is None:
        raise Invalid("Enter a number.")
    if price <= 0:
        raise Invalid("A price has to be greater than zero.")

    changed = {"lastPrice": price, "lastPriceAt": now}
    newly_triggered = False
    if not p.get("triggeredAt") and is_triggered(price, p.get("targetPrice")):
        changed["triggeredAt"] = now
        changed["acknowledged"] = False
        newly_triggered = True

    merged = {**p, **changed}
    return {
        "changed": changed,
        "newly_triggered": newly_triggered,
        "triggered": bool(merged.get("triggeredAt")),
        "distance_pct": distance_pct(price, merged.get("targetPrice")),
        "gauge": gauge(price, merged.get("targetPrice")),
    }


def needs_acknowledgement(position: dict) -> bool:
    """Triggered, and the user has not yet confirmed reading their plan."""
    p = position or {}
    return bool(p.get("triggeredAt")) and not p.get("acknowledged")


# ── the plan, and the cost of changing it ────────────────────────────
def lock_plan(position: dict, plan: str, now: datetime) -> dict:
    """First save of a plan. Stamps planLockedAt."""
    text = (plan or "").strip()
    if not text:
        raise Invalid("Write what you will do when it hits the target.")
    p = position or {}
    if p.get("planLockedAt"):
        raise Invalid("This plan is already locked — use the edit path, "
                      "which records what it replaced.")
    return {"plan": text, "planLockedAt": now}


def edit_plan(position: dict, new_plan: str, now: datetime,
              confirmed: bool = False) -> dict:
    """Change a locked plan. Requires explicit confirmation.

    The previous text is appended to `notes` with the date it was
    committed to, so a plan rewritten during a drawdown leaves a visible
    trail rather than replacing history. `confirmed` is not a formality:
    the UI has to show the user the date of the commitment they are
    overwriting before this is allowed through.
    """
    p = position or {}
    text = (new_plan or "").strip()
    if not text:
        raise Invalid("A plan cannot be blank. Disarm instead if you are out.")
    if not p.get("planLockedAt"):
        return lock_plan(p, text, now)
    if not confirmed:
        raise Invalid("Editing a locked plan needs confirmation.")
    if text == (p.get("plan") or "").strip():
        return {}                      # nothing changed; write nothing

    stamped = _fmt_date(p.get("planLockedAt"))
    entry = (f"[{_fmt_stamp(now)}] Plan replaced. Previous commitment, "
             f"made {stamped}:\n{p.get('plan') or ''}")
    return {
        "plan": text,
        "planLockedAt": now,
        "notes": _append_note(p.get("notes"), entry),
    }


def append_note(position: dict, text: str, now: datetime) -> dict:
    """Append-only log. Existing entries are never rewritten."""
    t = (text or "").strip()
    if not t:
        raise Invalid("Nothing to add.")
    return {"notes": _append_note((position or {}).get("notes"),
                                  f"[{_fmt_stamp(now)}] {t}")}


def _append_note(existing: str | None, entry: str) -> str:
    prior = (existing or "").rstrip()
    return f"{prior}\n\n{entry}" if prior else entry


def _fmt_stamp(dt) -> str:
    return dt.strftime("%Y-%m-%d %H:%M") if hasattr(dt, "strftime") else str(dt)


def _fmt_date(dt) -> str:
    return dt.strftime("%-d %b %Y") if hasattr(dt, "strftime") else str(dt)


# ── age, staleness, decay ────────────────────────────────────────────
def age_days(ts, now: datetime):
    """Whole days between a timestamp and now. None if unknown."""
    if not ts or not hasattr(ts, "year"):
        return None
    a, b = ts, now
    if hasattr(a, "date") and not hasattr(a, "hour"):
        a = datetime(a.year, a.month, a.day)
    if hasattr(b, "date") and not hasattr(b, "hour"):
        b = datetime(b.year, b.month, b.day)
    try:
        return max(0, (b - a).days)
    except TypeError:                  # tz-aware vs naive
        return None


def relative_age(ts, now: datetime) -> str:
    """'today' / 'yesterday' / 'N days ago' / 'N months ago'."""
    d = age_days(ts, now)
    if d is None:
        return ""
    if d == 0:
        return "today"
    if d == 1:
        return "yesterday"
    if d < 60:
        return f"{d} days ago"
    return f"{d // 30} months ago"


def price_staleness(position: dict, now: datetime) -> dict:
    """How old the price is, and whether that is old enough to distrust.

    A price nobody has updated in two months is not a current price, and
    a page that renders it in the same weight as a fresh one is telling
    the user something false without saying anything false.
    """
    p = position or {}
    at = p.get("lastPriceAt")
    if p.get("lastPrice") in (None, "") or not at:
        return {"days": None, "stale": False, "label": "",
                "missing": p.get("lastPrice") in (None, "")}
    d = age_days(at, now)
    return {"days": d, "stale": d is not None and d >= STALE_DAYS,
            "label": f"entered {relative_age(at, now)}", "missing": False}


def needs_triage(holding: dict, now: datetime) -> bool:
    """A CANDIDATE nobody has touched in DECAY_DAYS."""
    h = holding or {}
    if h.get("stage") != "CANDIDATE":
        return False
    d = age_days(h.get("lastTouchedAt") or h.get("addedAt"), now)
    return d is not None and d >= DECAY_DAYS


def counts(holdings: list, now: datetime) -> dict:
    """Header counts. Always visible, and triage never clears itself."""
    rows = holdings or []
    by = {s: 0 for s in STAGES}
    for h in rows:
        s = (h or {}).get("stage")
        if s in by:
            by[s] += 1
    return {
        "candidates": by["CANDIDATE"], "qualified": by["QUALIFIED"],
        "armed": by["ARMED"], "archived": by["ARCHIVED"],
        "triage": sum(1 for h in rows if needs_triage(h, now)),
        "armed_cap": ARMED_CAP,
        "armed_remaining": max(0, ARMED_CAP - by["ARMED"]),
    }


# ── sorting ──────────────────────────────────────────────────────────
def sort_rows(rows: list, mode: str = "order") -> list:
    """Default sortOrder; 'distance' sorts by distance ascending.

    Triggered rows pin to the top under EVERY mode. A trigger the user
    has not acknowledged is the one thing on this page that cannot be
    allowed to scroll away, and a sort that could bury it would defeat
    the app. Rows with no distance sort last rather than first, so an
    unpriced row never masquerades as the closest to target.
    """
    out = list(rows or [])

    def key(r):
        pos = (r or {}).get("position") or {}
        pinned = 0 if needs_acknowledgement(pos) else 1
        if mode == "distance":
            d = distance_pct(pos.get("lastPrice"), pos.get("targetPrice"))
            return (pinned, 1, 0.0) if d is None else (pinned, 0, d)
        return (pinned, 0, (r or {}).get("sortOrder") or 0)

    return sorted(out, key=key)


# ── NAICS import ─────────────────────────────────────────────────────
#
# The honest constraint: NO financial database has a field for "owns an
# unrepeatable permit". Any purely numeric screen surfaces
# capital-intensive companies, which is a different and much worse
# thing. Industry classification is the one screen that works, because
# these moats cluster into a short list of codes.
#
# This narrows the reading list. It does not do the reading.
NAICS_TARGETS = {
    "212311": "dimension stone mining",
    "212312": "crushed & broken limestone",
    "212313": "crushed & broken granite",
    "212319": "other crushed & broken stone",
    "212321": "construction sand & gravel",
    "212322": "industrial sand",
    "212390": "other nonmetallic mineral mining",
    "327310": "cement manufacturing",
    "327410": "lime manufacturing",
    "221310": "water supply & irrigation systems",
    "562211": "hazardous waste treatment & disposal",
    "562212": "solid waste landfill",
    "483113": "coastal & great lakes freight",
    "483211": "inland water freight",
    "482110": "rail transportation",
    "486110": "crude oil pipelines",
    "486210": "natural gas pipelines",
    "486910": "refined petroleum pipelines",
    "812220": "cemeteries & crematories",
}
DEFAULT_CAP_CEILING = 5_000_000_000.0

_HEADER_ALIASES = {
    "ticker": "ticker", "symbol": "ticker", "tickersymbol": "ticker",
    "name": "name", "companyname": "name", "company": "name",
    "security": "name", "description": "name",
    "naics": "naics", "naicscode": "naics", "naics_code": "naics",
    "sic": "naics",
    "marketcap": "marketCap", "market_cap": "marketCap",
    "marketcapitalization": "marketCap", "mktcap": "marketCap",
    "cap": "marketCap",
}
_CAP_SUFFIX = {"K": 1e3, "M": 1e6, "B": 1e9, "T": 1e12}


def _norm_header(h: str) -> str:
    return re.sub(r"[^a-z0-9_]", "", (h or "").strip().lower())


def normalize_naics(v) -> str | None:
    """Pull a 6-digit code out of whatever the export produced.

    Spreadsheets mangle these constantly: '212311.0' from a float
    column, '="212311"' from an Excel text guard, quotes and spaces.
    None of the target codes start with a zero, so a stripped leading
    zero is not a risk here — but a code that arrives as a float and is
    silently unmatched IS, because it looks like "not in the target
    list" rather than "not parsed".
    """
    s = str(v or "").strip().strip('="\'')
    if not s:
        return None
    s = s.split(".")[0]                        # 212311.0 -> 212311
    digits = re.sub(r"\D", "", s)
    return digits[:6] if len(digits) >= 6 else (digits or None)


def parse_market_cap(v):
    """'$1.2B', '1,234,000', '450M', 1.23e9 -> float. None if unreadable.

    None is NOT zero. A row whose cap could not be read is reported as
    unreadable rather than passed through as free, which would let it
    slip under any ceiling.
    """
    if v is None or v == "":
        return None
    if isinstance(v, (int, float)):
        return float(v)
    s = str(v).strip().replace(",", "").replace("$", "").replace("(", "-").rstrip(")")
    if not s:
        return None
    mult = 1.0
    if s and s[-1].upper() in _CAP_SUFFIX:
        mult = _CAP_SUFFIX[s[-1].upper()]
        s = s[:-1]
    try:
        return float(s) * mult
    except ValueError:
        return None


def parse_csv(text: str) -> dict:
    """Read a pasted or uploaded CSV. Reports what it could not read.

    Unreadable rows are COUNTED AND RETURNED, never silently dropped. A
    parser that quietly discards a fifth of the file makes an import
    look complete when it is not, and the user has no way to notice.
    """
    raw = (text or "").strip()
    if not raw:
        raise Invalid("Nothing to import.")

    try:
        dialect = csv.Sniffer().sniff(raw[:4096], delimiters=",;\t|")
    except csv.Error:
        dialect = csv.excel
    reader = csv.reader(io.StringIO(raw), dialect)
    try:
        header = next(reader)
    except StopIteration:
        raise Invalid("Nothing to import.")

    cols = {}
    for i, h in enumerate(header):
        key = _HEADER_ALIASES.get(_norm_header(h))
        if key and key not in cols:
            cols[key] = i
    missing = [c for c in ("ticker", "naics") if c not in cols]
    if missing:
        raise Invalid(
            "The file needs at least a ticker column and a NAICS column. "
            f"Missing: {', '.join(missing)}. Found: "
            f"{', '.join(h.strip() for h in header if h.strip()) or 'nothing'}."
        )

    rows, skipped = [], []
    for n, rec in enumerate(reader, start=2):
        if not any((c or "").strip() for c in rec):
            continue

        def cell(k):
            i = cols.get(k)
            return (rec[i] if i is not None and i < len(rec) else "") or ""

        ticker = cell("ticker").strip().upper().strip('="\'')
        if not ticker:
            skipped.append({"line": n, "why": "no ticker"})
            continue
        naics = normalize_naics(cell("naics"))
        if not naics:
            skipped.append({"line": n, "ticker": ticker, "why": "no NAICS code"})
            continue
        rows.append({
            "ticker": ticker,
            "name": cell("name").strip() or ticker,
            "naics": naics,
            "marketCap": parse_market_cap(cell("marketCap")),
        })
    return {"rows": rows, "skipped": skipped, "columns": sorted(cols)}


def filter_import(rows: list, existing_tickers=(), naics_targets: dict | None = None,
                  cap_ceiling: float | None = DEFAULT_CAP_CEILING) -> dict:
    """Split parsed rows into what to add, what is already here, and what missed.

    Every rejected row lands in exactly one named bucket and the buckets
    sum to the input. A filter that reports only its winners cannot be
    checked, and "we found 140 matches" means nothing without "out of
    what, and why not the rest".
    """
    targets = NAICS_TARGETS if naics_targets is None else naics_targets
    have = {str(t).strip().upper() for t in (existing_tickers or ())}

    matched, already, wrong_naics, too_big, unreadable_cap, dupes = [], [], [], [], [], []
    seen: set[str] = set()

    for r in rows or []:
        code = r.get("naics")
        if code not in targets:
            wrong_naics.append(r)
            continue
        cap = r.get("marketCap")
        if cap_ceiling is not None:
            if cap is None:
                unreadable_cap.append(r)
                continue
            if cap > cap_ceiling:
                too_big.append(r)
                continue
        tkr = r.get("ticker")
        if tkr in have:
            already.append(r)
            continue
        if tkr in seen:
            dupes.append(r)
            continue
        seen.add(tkr)
        matched.append({**r, "sourceNote": targets[code]})

    return {
        "matched": matched, "already_tracked": already,
        "wrong_naics": wrong_naics, "over_ceiling": too_big,
        "unreadable_cap": unreadable_cap, "duplicate_in_file": dupes,
        "counts": {
            "input": len(rows or []), "matched": len(matched),
            "already_tracked": len(already), "wrong_naics": len(wrong_naics),
            "over_ceiling": len(too_big), "unreadable_cap": len(unreadable_cap),
            "duplicate_in_file": len(dupes),
        },
    }


# ── new candidate ────────────────────────────────────────────────────
def validate_candidate(payload: dict) -> dict:
    """Ticker, name, why you noticed it. Nothing else, ever.

    This has to survive being done one-handed on a phone in under
    fifteen seconds, so anything not on this list is not asked for here.
    Every extra field is a reason not to write the name down at all, and
    a name not written down is the one certain way to lose it.
    """
    p = payload or {}
    ticker = (p.get("ticker") or "").strip().upper()
    if not ticker:
        raise Invalid("Ticker is required.")
    if not re.fullmatch(r"[A-Z0-9][A-Z0-9.\-]{0,11}", ticker):
        raise Invalid(f"'{ticker}' does not look like a ticker.")
    name = (p.get("name") or "").strip()
    if not name:
        raise Invalid("Company name is required.")
    note = (p.get("sourceNote") or "").strip()
    if not note:
        raise Invalid("One line on why you noticed it — future you will "
                      "not remember.")
    ex = (p.get("exchange") or "").strip().upper()
    return {"ticker": ticker, "name": name, "sourceNote": note,
            "exchange": ex, "stage": "CANDIDATE"}


# ── the twelve ───────────────────────────────────────────────────────
#
# Seeded at QUALIFIED with grandfathered rubrics: they were assessed
# before this app existed, and inventing rubric answers for them would
# put fabricated evidence in the record the whole design depends on.
# `grandfathered` says plainly that no rubric was actually filled in.
SEED = [
    {
        "ticker": "CMP", "name": "Compass Minerals", "exchange": "NYSE",
        "moatType": "MINERAL_DEPOSIT", "sentiment": "OUT_OF_FAVOR",
        "assetLine": "Goderich, Ontario — the largest underground salt mine on earth, plus Cote Blanche in Louisiana",
        "thesis": "Nobody is building another Goderich. The hatred is earned: years of production shortfalls, a securities settlement over failed mine upgrades, and hoisting constraints that keep unit costs above plan. Leverage has come down to roughly 2.8x from 4.3x. The asset is permanent; the execution has not been.",
        "invalidation": "Mine cost structure never gets fixed under any management, or de-icing demand structurally declines with warmer winters. Cost per ton failing to improve across two more full winters is the tell.",
        "anchorPrice": 28.50, "anchorAsOf": "2026-08-01",
    },
    {
        "ticker": "USLM", "name": "United States Lime & Minerals", "exchange": "NASDAQ",
        "moatType": "SHIPPING_RADIUS", "sentiment": "BID_UP",
        "assetLine": "Lime and limestone plants across TX, OK, AR, LA, MO and CO — quarries that cannot be permitted again",
        "thesis": "The textbook version of this moat, priced accordingly: roughly $2.95B market cap, no debt, a large net cash position, seventeen analysts covering, consensus fair value. Nothing is wrong with it. That is the problem.",
        "invalidation": "Nothing structural. This is a valuation-only wait — a Texas construction recession is the plausible source of a discount.",
        "anchorPrice": 102.78, "anchorAsOf": "2026-07-06",
    },
    {
        "ticker": "FRPH", "name": "FRP Holdings", "exchange": "NASDAQ",
        "moatType": "MINERAL_DEPOSIT", "sentiment": "NEUTRAL",
        "assetLine": "Aggregates royalties on mining land it leases out, plus industrial and mixed-use property",
        "thesis": "Collects a per-ton royalty without operating the quarries — the moat with none of the capex. Family-influenced, lightly covered, and reported earnings understate the land.",
        "invalidation": "Royalty tonnage falls with a regional construction downturn, or management redeploys aggregates cash into mediocre development projects.",
        "anchorPrice": None, "anchorAsOf": None,
    },
    {
        "ticker": "MCEM", "name": "Monarch Cement", "exchange": "OTC",
        "moatType": "SHIPPING_RADIUS", "sentiment": "NEUTRAL",
        "assetLine": "A cement plant in Humboldt, Kansas serving a radius no competitor can economically cross",
        "thesis": "Cement past roughly 150 miles by truck stops making money, which makes the plant a regional monopoly by freight arithmetic. Family controlled, trades over the counter, barely followed.",
        "invalidation": "Illiquidity is the real risk, not the business. Thin volume means fills may not happen at the intended price in either direction.",
        "anchorPrice": None, "anchorAsOf": None,
    },
    {
        "ticker": "PCYO", "name": "Pure Cycle", "exchange": "NASDAQ",
        "moatType": "WATER_RIGHT", "sentiment": "NEUTRAL",
        "assetLine": "Colorado water rights and the land they serve, east of Denver",
        "thesis": "The purest legal moat available — water rights in the arid west are adjudicated, finite, and cannot be manufactured. Revenue is lumpy and tied to development pace, which keeps institutional money away.",
        "invalidation": "Front Range homebuilding stalls for years, or a water-law or rate ruling changes what the rights are worth.",
        "anchorPrice": None, "anchorAsOf": None,
    },
    {
        "ticker": "GWRS", "name": "Global Water Resources", "exchange": "NASDAQ",
        "moatType": "WATER_RIGHT", "sentiment": "NEUTRAL",
        "assetLine": "Regulated water and wastewater service areas in metro Phoenix",
        "thesis": "Grows by annexing new service territory — a franchise granted, not competed for. Arizona water scarcity cuts both ways: it makes the franchise valuable and the regulator nervous.",
        "invalidation": "Arizona restricts new-development water allocations hard enough to stop the growth engine, or the rate case cycle turns hostile.",
        "anchorPrice": None, "anchorAsOf": None,
    },
    {
        "ticker": "YORW", "name": "York Water", "exchange": "NASDAQ",
        "moatType": "WATER_RIGHT", "sentiment": "BID_UP",
        "assetLine": "Reservoirs and mains serving south-central Pennsylvania since 1816",
        "thesis": "The oldest investor-owned utility in the country and the longest continuous dividend record in America. Owned almost entirely for that record, which is exactly why it rarely gets cheap.",
        "invalidation": "Nothing structural. Wait for a rate-case disappointment or a rate-driven selloff in small utilities.",
        "anchorPrice": None, "anchorAsOf": None,
    },
    {
        "ticker": "ARTNA", "name": "Artesian Resources", "exchange": "NASDAQ",
        "moatType": "WATER_RIGHT", "sentiment": "NEUTRAL",
        "assetLine": "Regulated water service across most of Delaware, plus Maryland and Pennsylvania",
        "thesis": "A small, boring, geographically locked franchise. Delaware is not adding a second water company.",
        "invalidation": "PFAS remediation mandates land harder than the rate base can absorb, or acquisition growth stops.",
        "anchorPrice": None, "anchorAsOf": None,
    },
    {
        "ticker": "PESI", "name": "Perma-Fix Environmental", "exchange": "NASDAQ",
        "moatType": "LICENSE", "sentiment": "SPECULATIVE",
        "assetLine": "NRC-licensed facilities for treating radioactive and mixed waste",
        "thesis": "The license is close to unobtainable — that is the entire moat. But it has burned cash for long stretches and depends on federal contract timing it does not control.",
        "invalidation": "Serious risk of dilution or balance-sheet trouble before the DOE work scales. Small position or none.",
        "anchorPrice": None, "anchorAsOf": None,
    },
    {
        "ticker": "GLDD", "name": "Great Lakes Dredge & Dock", "exchange": "NASDAQ",
        "moatType": "JONES_ACT", "sentiment": "NEUTRAL",
        "assetLine": "The largest US-flag dredging fleet — vessels that must be built in American yards",
        "thesis": "Port deepening and coastal restoration are federally funded and legally reserved for US-built hulls. New capacity is limited by shipyard slots, not by capital.",
        "invalidation": "Federal appropriations dry up, or a newbuild program runs over budget and eats several years of returns.",
        "anchorPrice": None, "anchorAsOf": None,
    },
    {
        "ticker": "KEX", "name": "Kirby", "exchange": "NYSE",
        "moatType": "JONES_ACT", "sentiment": "NEUTRAL",
        "assetLine": "The largest inland tank barge fleet on the Mississippi river system",
        "thesis": "Barges must be US-built and the fleet has been shrinking for years as old hulls retire faster than yards replace them. Tight supply shows up as pricing power.",
        "invalidation": "A newbuild wave finally arrives, or petrochemical volumes fall far enough to slacken the fleet.",
        "anchorPrice": None, "anchorAsOf": None,
    },
    {
        "ticker": "CSV", "name": "Carriage Services", "exchange": "NYSE",
        "moatType": "LAND_USE", "sentiment": "NEUTRAL",
        "assetLine": "Funeral homes and cemetery land — permitted, consecrated, and effectively impossible to re-site",
        "thesis": "You cannot open a new cemetery next to an old one. Demand is demographically certain. The market dislikes the leverage and the roll-up history.",
        "invalidation": "Debt service outruns cash generation, or cremation shifts revenue per case down faster than the cemetery segment grows.",
        "anchorPrice": None, "anchorAsOf": None,
    },
]

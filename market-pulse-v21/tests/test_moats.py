"""Permit-moat watchlist domain — proved without a database.

Run:  python tests/test_moats.py      (exit 0 = all pass)

This app's job is not to measure companies, it is to hold the user to a
decision they made while calm. So the checks that matter most here are
not arithmetic — they are the ones that prove a gate cannot be walked
past, a trigger cannot quietly un-set itself, and a rewritten plan
cannot erase the one it replaced.

Everything is pure and takes `now` as an argument, so no check depends
on the day it runs.
"""
import os
import sys
from datetime import datetime, timedelta

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import moats as M

_COUNT = 0
_FAILS = []


def check(cond, msg):
    global _COUNT
    _COUNT += 1
    if not cond:
        _FAILS.append(msg)


def raises(fn, *a, **k):
    try:
        fn(*a, **k)
        return False
    except M.Invalid:
        return True


NOW = datetime(2026, 8, 24, 12, 0)

GOOD = {
    "replicableWithCapital": False, "permitTrend": "DECLINING",
    "freightPctOfValue": 45, "terminalDemand50yr": "INTACT",
    "cheapBecause": "OPERATIONAL_STUMBLE",
    "evidence": "Read the 2025 10-K and the Ontario MNRF permit register; "
                "the mine plan and hoisting constraints are described at "
                "length in MD&A, and no new salt permits have issued in "
                "the province since 2009.",
}


# ── the gates ──
check(M.evaluate(GOOD)["score"] == 5 and M.evaluate(GOOD)["passed"] is True,
      "a rubric answering every criterion well scores 5 and passes")

_rep = {**GOOD, "replicableWithCapital": True}
_e = M.evaluate(_rep)
check(_e["passed"] is False and _e["failures"][0]["gate"] == "replicableWithCapital",
      "replicable-with-capital is a HARD FAIL — capital intensity is not a "
      "moat, and this single test is what separates an unrepeatable permit "
      "from a company that merely owns expensive machines")
check(_e["score"] == 4,
      "and it still scores 4, which is the trap: the score stays high while "
      "the answer is disqualifying, so the score must never be what decides")

_str = {**GOOD, "cheapBecause": "STRUCTURAL_DECLINE"}
check(M.evaluate(_str)["passed"] is False,
      "structural decline is a hard fail — a melting asset is not a discount")

_both = {**GOOD, "replicableWithCapital": True, "cheapBecause": "STRUCTURAL_DECLINE"}
check(len(M.gate_failures(_both)) == 2,
      "both gates report independently, so a refusal names everything wrong "
      "rather than sending the user round the loop twice")
check(all(len(f["message"]) > 60 for f in M.gate_failures(_both)),
      "and each failure explains itself in plain language — a refusal that "
      "does not say what would have to be different is a wall, not a gate")

# The scoring table, one criterion at a time.
check(M.score({**GOOD, "permitTrend": "RISING"}) == 4, "permit trend scores")
check(M.score({**GOOD, "freightPctOfValue": 29}) == 4, "freight scores at >= 30")
check(M.score({**GOOD, "freightPctOfValue": 30}) == 5, "30 exactly still scores")
check(M.score({**GOOD, "freightPctOfValue": None}) == 4,
      "an unknown freight percent scores zero for that line rather than "
      "raising — the rubric is answerable without it")
check(M.score({**GOOD, "terminalDemand50yr": "UNCERTAIN"}) == 4, "demand scores")
check(M.score({**GOOD, "cheapBecause": "NOT_CHEAP"}) == 4, "pricing scores")
check(M.score({**GOOD, "cheapBecause": "CYCLICAL_TROUGH"}) == 5,
      "a cyclical trough counts alongside an operational stumble")
check(M.score({}) == 0 and M.evaluate({})["passed"] is True,
      "an empty rubric scores 0; it fails no GATE because it asserts "
      "nothing — promotion is blocked by the missing answers, not by "
      "pretending a blank form said something disqualifying")


# ── validation derives its own verdict ──
_v = M.validate_rubric({**GOOD, "passed": True, "score": 5,
                        "replicableWithCapital": True})
check(_v["passed"] is False and _v["score"] == 4,
      "validate DERIVES passed and score, overwriting whatever was posted — "
      "a client that could post its own verdict could post itself past a "
      "gate, which is the one thing this module exists to prevent")
check(M.validate_rubric({**GOOD, "replicableWithCapital": "false"})
      ["replicableWithCapital"] is False,
      "a form posting the string 'false' is read as the boolean, not as "
      "truthy — the difference here is a hard gate silently inverting")
check(raises(M.validate_rubric, {**GOOD, "replicableWithCapital": None}),
      "but an UNANSWERED replicability question is rejected outright rather "
      "than defaulting either way")
check(raises(M.validate_rubric, {**GOOD, "evidence": "too short"}),
      f"evidence under {M.EVIDENCE_MIN} chars is rejected — a rubric with no "
      f"source is a guess wearing a score")
check(raises(M.validate_rubric, {**GOOD, "permitTrend": "MAYBE"})
      and raises(M.validate_rubric, {**GOOD, "cheapBecause": "DUNNO"})
      and raises(M.validate_rubric, {**GOOD, "terminalDemand50yr": "X"}),
      "unknown enum values are rejected rather than stored and later "
      "compared against, where they would silently never match")
check(raises(M.validate_rubric, {**GOOD, "freightPctOfValue": 140})
      and raises(M.validate_rubric, {**GOOD, "freightPctOfValue": "lots"}),
      "freight must be a real percentage or blank")
check(M.validate_rubric({**GOOD, "freightPctOfValue": ""})["freightPctOfValue"] is None,
      "and blank stays None rather than becoming 0, which would read as "
      "'freight is irrelevant here' — a measurement nobody made")


# ── promotion ──
FULL = {"stage": "CANDIDATE", "assetLine": "a salt mine", "thesis": "t",
        "invalidation": "i", "moatType": "MINERAL_DEPOSIT",
        "sentiment": "OUT_OF_FAVOR"}
check(M.can_promote(FULL, GOOD)["ok"] is True,
      "a candidate with every field and a passing rubric promotes")
check(M.can_promote(FULL, None)["ok"] is False,
      "no rubric, no promotion")
check(M.can_promote({**FULL, "thesis": "  "}, GOOD)["ok"] is False,
      "whitespace is not a thesis")
_p = M.can_promote({"stage": "CANDIDATE"}, GOOD)
check(_p["ok"] is False and len(_p["reasons"]) == 1
      and "invalidation" in _p["reasons"][0],
      "missing thesis fields are listed in ONE reason naming all of them, "
      "not five separate refusals")
_pg = M.can_promote(FULL, _rep)
check(_pg["ok"] is False and _pg["failures"][0]["gate"] == "replicableWithCapital",
      "a gate failure blocks promotion and travels back so the UI can name "
      "which gate — there is NO override path here or anywhere")
check(M.can_promote({**FULL, "stage": "ARCHIVED"}, GOOD)["ok"] is False
      and M.can_promote({**FULL, "stage": "ARMED"}, GOOD)["ok"] is False,
      "only a candidate promotes; archived and armed are refused")

# The hole a blank form opens, and the seeds that legitimately have none.
check(M.can_promote(FULL, {})["ok"] is False,
      "an EMPTY rubric cannot promote. Both gates fire on a specific "
      "disqualifying answer, so a form with nothing filled in trips "
      "neither and would otherwise sail straight through — absence of a "
      "disqualifying answer is not evidence of a qualifying one")
check(M.can_promote(FULL, {**GOOD, "evidence": "thin"})["ok"] is False,
      "and neither can one whose evidence was never written")
check(M.rubric_is_complete(GOOD) is True and M.rubric_is_complete({}) is False,
      "completeness is about whether the questions were ANSWERED, which is "
      "a different question from whether the answers passed")

_gf = M.grandfathered_rubric()
check(_gf["grandfathered"] is True and _gf["passed"] is True
      and M.rubric_is_complete(_gf) is True,
      "a grandfathered placeholder is admitted — the twelve seeds were "
      "judged before this app existed")
check(all(_gf[k] is None for k in ("replicableWithCapital", "permitTrend",
                                   "terminalDemand50yr", "cheapBecause")),
      "with every answer None ON PURPOSE, because inventing rubric answers "
      "for them would put fabricated evidence into the one record the "
      "whole design depends on")
check(_gf["score"] is None and M.display_score(_gf) == "not scored",
      "and no score at all rather than 0 — 0 is a result, this is an "
      "absence, and a board showing '0/5' would assert they scored badly "
      "when nobody measured")
check(M.display_score(M.validate_rubric(GOOD)) == "5/5"
      and M.display_score({"score": 3}) == "3/5",
      "a real rubric shows its real score")
check(M.display_score({}) == "not scored",
      "and a rubric carrying no score says so rather than showing 0/5 — "
      "the same absence-is-not-a-result rule as the grandfathered case")
check(M.can_promote(FULL, _gf)["ok"] is True,
      "so the seeds can qualify honestly without pretending to a rubric")


# ── arming ──
ARMABLE = {"stage": "QUALIFIED"}
POS = {"targetPrice": 20.0, "lastPrice": 28.5, "plan": "Buy a third."}
check(M.can_arm(ARMABLE, POS, 0, GOOD)["ok"] is True, "target + price + plan arms")
check(M.can_arm(ARMABLE, {**POS, "plan": ""}, 0, GOOD)["ok"] is False,
      "no plan, no arming — the plan IS the commitment, so arming without "
      "one would create a holding that can trigger with nothing to show")
check(M.can_arm(ARMABLE, {**POS, "targetPrice": None}, 0, GOOD)["ok"] is False
      and M.can_arm(ARMABLE, {**POS, "lastPrice": None}, 0, GOOD)["ok"] is False,
      "and both prices are required")
check(M.can_arm({"stage": "CANDIDATE"}, POS, 0, GOOD)["ok"] is False,
      "a candidate cannot skip qualification and arm directly")

# ── the rubric requirement, enforced at the ARMING boundary only ──
#
# Grandfathering lets the twelve seeds sit at QUALIFIED without a rubric
# they were never given. It was never meant to carry them to ARMED, which
# is the stage that can demand money of you.
_gfr = M.grandfathered_rubric()
check(M.can_arm(ARMABLE, POS, 0, _gfr)["ok"] is False,
      "A GRANDFATHERED HOLDING CANNOT BE ARMED. Its every rubric answer is "
      "null; arming is what makes a holding able to trigger a real purchase "
      "decision, and a null assessment must not be able to do that")
check(M.can_arm(ARMABLE, POS, 0, _gfr)["needs_rubric"] is True,
      "and it says so, so the UI can route into the rubric rather than "
      "showing a dead button — a refusal with nowhere to go is the same as "
      "hiding the action")
check(M.can_promote({**FULL, "stage": "CANDIDATE"}, _gfr)["ok"] is True,
      "while QUALIFYING on a grandfathered rubric still works — the "
      "requirement applies at the arming boundary and nowhere else")
check(M.can_arm(ARMABLE, POS, 0)["ok"] is False
      and M.can_arm(ARMABLE, POS, 0)["needs_rubric"] is True,
      "omitting the rubric argument entirely FAILS CLOSED rather than "
      "skipping the check — a caller that forgets it gets a loud refusal, "
      "not a silent hole in the one gate before an armed position")
check(M.can_arm(ARMABLE, POS, 0, {})["ok"] is False,
      "and neither an empty rubric")
_arm_gate = M.can_arm(ARMABLE, POS, 0, {**GOOD, "replicableWithCapital": True})
check(_arm_gate["ok"] is False and _arm_gate["needs_rubric"] is False
      and any("capital intensity" in r for r in _arm_gate["reasons"]),
      "a COMPLETE rubric that fails a gate is refused too, and names the "
      "gate rather than asking for a rubric that already exists")
check(M.rubric_is_complete(_gfr) is True
      and M.rubric_is_complete_for_arming(_gfr) is False,
      "the two completeness tests differ on exactly one case — the "
      "grandfathered placeholder — which is the whole reason both exist")
check(M.rubric_is_complete_for_arming(GOOD) is True
      and M.rubric_is_complete_for_arming({}) is False,
      "and agree everywhere else")

# The checks below deliberately reference the CONSTANTS, so they prove the
# behaviour rather than a magic number. That leaves a hole: the constants
# themselves are specified, and every one of those checks would still pass
# if the cap silently became 40. These four pin the values.
check(M.ARMED_CAP == 15, "the armed cap is 15")
check(M.DECAY_DAYS == 90, "a candidate decays after 90 days")
check(M.STALE_DAYS == 30, "a price is stale after 30 days")
check(M.EVIDENCE_MIN == 120, "evidence needs 120 characters")

_cap = M.can_arm(ARMABLE, POS, M.ARMED_CAP, GOOD)
check(_cap["ok"] is False and _cap["at_cap"] is True,
      f"the {M.ARMED_CAP + 1}th arm is refused until something is disarmed — "
      f"capital is finite and the cap is the app admitting it")
check(M.can_arm(ARMABLE, POS, M.ARMED_CAP - 1, GOOD)["ok"] is True,
      "the 15th is allowed; the cap is a ceiling, not an off-by-one")


# ── archiving ──
check(M.can_archive("")["ok"] is False and M.can_archive("  ")["ok"] is False,
      "archiving demands a reason — the record of what was passed on and "
      "why is the most valuable thing this app accumulates, and it is "
      "worth nothing if half the entries say nothing")
check(M.can_archive("Cremation mix worsened two years running")["ok"] is True,
      "a real reason is accepted and trimmed")


# ── distance and the gauge ──
check(M.distance_pct(28.5, 20.0) == 42.5, "42.5% above target")
check(round(M.distance_pct(19.0, 20.0), 1) == -5.0, "below target is negative")
check(M.distance_pct(None, 20) is None and M.distance_pct(20, None) is None
      and M.distance_pct(20, 0) is None,
      "an unknown side yields None, NEVER 0 — 'at the target' is the most "
      "consequential reading on the page and must not be what a missing "
      "number looks like")
check(M.distance_pct("abc", 20) is None, "and junk is not a price")

check(M.is_triggered(20.0, 20.0) is True, "at the target counts as triggered")
check(M.is_triggered(19.99, 20.0) is True and M.is_triggered(20.01, 20.0) is False,
      "and the boundary is at-or-below")
check(M.is_triggered(None, 20.0) is False,
      "an unpriced holding never triggers on its own")

_g = M.gauge(20.0, 20.0)
check(_g["pos"] == M.GAUGE_NOTCH and _g["in_buy_zone"] is True,
      "at target the marker sits exactly on the notch")
check(M.gauge(12.0, 20.0)["pos"] == 0.0 and M.gauge(32.0, 20.0)["pos"] == 100.0,
      "the visible range runs 0.6x to 1.6x of target, end to end")
check(M.gauge(16.0, 20.0)["pos"] == 20.0,
      "halfway into the buy zone is halfway to the notch")
check(M.gauge(26.0, 20.0)["pos"] == 70.0,
      "and halfway above target is halfway across the remaining 60%")
check(M.gauge(6.0, 20.0)["pos"] == 0.0 and M.gauge(6.0, 20.0)["clamped"] is True,
      "beyond the range the marker clamps AND says it clamped — a marker "
      "resting on the edge otherwise reads as a real position rather than "
      "'further than this bar can show'")
check(M.gauge(19.0, 20.0)["in_buy_zone"] is True
      and M.gauge(21.0, 20.0)["in_buy_zone"] is False,
      "the buy zone is everything at or left of the notch")
check("below target" in M.gauge(16.0, 20.0)["label"]
      and "above target" in M.gauge(26.0, 20.0)["label"]
      and M.gauge(20.0, 20.0)["label"] == "At target",
      "every state is conveyed in WORDS for the aria-label — colour alone "
      "would leave the gauge unreadable to a screen reader and to anyone "
      "who cannot distinguish the tint")
check(M.gauge(None, 20)["pos"] is None and M.gauge(20, None)["label"],
      "with nothing to plot the gauge reports no position and says why, "
      "rather than drawing a marker at zero")


# ── the trigger ──
ARMED = {"targetPrice": 20.0, "lastPrice": 25.0, "plan": "Buy a third.",
         "planLockedAt": datetime(2026, 1, 5, 9, 0)}
_r = M.apply_price(ARMED, 19.5, NOW)
check(_r["newly_triggered"] is True and _r["changed"]["triggeredAt"] == NOW
      and _r["changed"]["acknowledged"] is False,
      "crossing the target stamps the moment and demands acknowledgement")

_after = {**ARMED, **_r["changed"], "acknowledged": True}
_r2 = M.apply_price(_after, 18.0, NOW + timedelta(days=1))
check(_r2["newly_triggered"] is False and "acknowledged" not in _r2["changed"],
      "falling further does not re-demand acknowledgement — the user has "
      "already faced the plan and nagging them again teaches them to dismiss it")
_r3 = M.apply_price(_after, 26.0, NOW + timedelta(days=2))
check("triggeredAt" not in _r3["changed"] and _r3["triggered"] is True,
      "and a recovery NEVER clears the trigger. The moment happened. A "
      "trigger that quietly un-set itself on a bounce would let exactly "
      "the decision this app exists for slip past unmade")

check(M.apply_price({"targetPrice": 20.0}, 25.0, NOW)["newly_triggered"] is False,
      "a price above target changes nothing but the price")
check(raises(M.apply_price, ARMED, "", NOW)
      and raises(M.apply_price, ARMED, "abc", NOW)
      and raises(M.apply_price, ARMED, 0, NOW)
      and raises(M.apply_price, ARMED, -5, NOW),
      "blank, junk, zero and negative prices are refused rather than "
      "stored — a zero would trigger every target on the board")

check(M.needs_acknowledgement({"triggeredAt": NOW, "acknowledged": False}) is True,
      "a triggered, unacknowledged holding needs the user to read their plan")
check(M.needs_acknowledgement({"triggeredAt": NOW, "acknowledged": True}) is False
      and M.needs_acknowledgement({}) is False,
      "and nothing else does")


# ── the plan, and the cost of changing it ──
_l = M.lock_plan({}, "Buy a third at 20, another third at 16.", NOW)
check(_l["planLockedAt"] == NOW and _l["plan"].startswith("Buy a third"),
      "the first save stamps the lock date")
check(raises(M.lock_plan, {}, "   ", NOW), "a blank plan is not a plan")
check(raises(M.lock_plan, {"planLockedAt": NOW, "plan": "x"}, "y", NOW),
      "and a second save through the locking path is refused — it must go "
      "through the edit path, which records what it replaced")

LOCKED = {"plan": "Buy a third at 20.", "planLockedAt": datetime(2026, 1, 5, 9, 0),
          "notes": ""}
check(raises(M.edit_plan, LOCKED, "Actually, wait for 12.", NOW),
      "editing a locked plan without confirmation is refused — rewriting "
      "the thesis mid-drawdown is the exact failure being defended against")
_ed = M.edit_plan(LOCKED, "Actually, wait for 12.", NOW, confirmed=True)
check(_ed["plan"] == "Actually, wait for 12." and _ed["planLockedAt"] == NOW,
      "confirmed, it saves and re-stamps the lock")
check("Buy a third at 20." in _ed["notes"] and "5 Jan 2026" in _ed["notes"],
      "and the PREVIOUS commitment is preserved in the notes with the date "
      "it was made — a plan rewritten during a drawdown leaves a visible "
      "trail rather than replacing history")
check(M.edit_plan(LOCKED, "Buy a third at 20.", NOW, confirmed=True) == {},
      "re-saving identical text writes nothing, so the notes do not fill "
      "with entries recording that nothing changed")
check(M.edit_plan({}, "First plan", NOW)["planLockedAt"] == NOW,
      "editing a plan that was never locked just locks it, no confirmation "
      "needed — there is no commitment to break yet")
check(raises(M.edit_plan, LOCKED, "", NOW, True),
      "and a plan can never be emptied; disarm instead")

_n1 = M.append_note({"notes": "first"}, "second", NOW)
check("first" in _n1["notes"] and "second" in _n1["notes"]
      and _n1["notes"].index("first") < _n1["notes"].index("second"),
      "notes are append-only and chronological — existing entries are "
      "never rewritten")
check("2026-08-24" in _n1["notes"], "and every entry is stamped")
check(raises(M.append_note, {}, "  ", NOW), "an empty note is refused")


# ── age, staleness, decay ──
check(M.age_days(NOW - timedelta(days=6), NOW) == 6, "whole days")
check(M.relative_age(NOW, NOW) == "today"
      and M.relative_age(NOW - timedelta(days=1), NOW) == "yesterday"
      and M.relative_age(NOW - timedelta(days=6), NOW) == "6 days ago"
      and M.relative_age(NOW - timedelta(days=95), NOW) == "3 months ago",
      "relative age reads the way a person would say it")
check(M.relative_age(None, NOW) == "" and M.age_days(None, NOW) is None,
      "an unknown timestamp produces no claim about age")

_fresh = M.price_staleness({"lastPrice": 10, "lastPriceAt": NOW - timedelta(days=6)}, NOW)
check(_fresh["stale"] is False and _fresh["label"] == "entered 6 days ago",
      "a recent price shows its age without a warning")
_old = M.price_staleness({"lastPrice": 10,
                          "lastPriceAt": NOW - timedelta(days=M.STALE_DAYS)}, NOW)
check(_old["stale"] is True,
      f"a price {M.STALE_DAYS} days old is flagged — a price nobody has "
      f"updated in a month is not a current price, and rendering it in the "
      f"same weight as a fresh one says something false without saying "
      f"anything false")
check(M.price_staleness({"lastPrice": None}, NOW)["missing"] is True,
      "and no price at all is reported as missing, not as stale")

_cand = {"stage": "CANDIDATE", "lastTouchedAt": NOW - timedelta(days=M.DECAY_DAYS)}
check(M.needs_triage(_cand, NOW) is True,
      f"a candidate untouched for {M.DECAY_DAYS} days surfaces for triage")
check(M.needs_triage({**_cand, "lastTouchedAt": NOW - timedelta(days=89)}, NOW) is False,
      "one day short does not")
check(M.needs_triage({**_cand, "stage": "QUALIFIED"}, NOW) is False
      and M.needs_triage({**_cand, "stage": "ARCHIVED"}, NOW) is False,
      "only candidates decay — a qualified name has already been read, and "
      "an archived one is a closed decision")
check(M.needs_triage({"stage": "CANDIDATE",
                      "addedAt": NOW - timedelta(days=200)}, NOW) is True,
      "and a candidate never touched since being added falls back to when "
      "it was added, rather than never decaying at all")

_c = M.counts([
    {"stage": "CANDIDATE", "lastTouchedAt": NOW - timedelta(days=200)},
    {"stage": "CANDIDATE", "lastTouchedAt": NOW},
    {"stage": "QUALIFIED"}, {"stage": "ARMED"}, {"stage": "ARCHIVED"},
], NOW)
check(_c["candidates"] == 2 and _c["qualified"] == 1 and _c["armed"] == 1
      and _c["archived"] == 1 and _c["triage"] == 1,
      "the header counts every stage plus the triage backlog")
check(_c["armed_remaining"] == M.ARMED_CAP - 1,
      "and how many arms are left, so the cap is visible before it bites")


# ── sorting ──
def _row(order, last, target, trig=None, ack=False):
    return {"sortOrder": order,
            "position": {"lastPrice": last, "targetPrice": target,
                         "triggeredAt": trig, "acknowledged": ack}}


_rows = [_row(3, 30, 20), _row(1, 22, 20), _row(2, None, None)]
check([r["sortOrder"] for r in M.sort_rows(_rows)] == [1, 2, 3],
      "default order is the user's own")
check([r["sortOrder"] for r in M.sort_rows(_rows, "distance")] == [1, 3, 2],
      "by distance, closest first — and the UNPRICED row sorts LAST rather "
      "than first, where a None treated as zero would put it, pretending an "
      "unpriced holding is the closest to target")

_pinned = M.sort_rows([_row(1, 30, 20), _row(9, 19, 20, trig=NOW)], "distance")
check(_pinned[0]["sortOrder"] == 9,
      "a triggered, unacknowledged row pins to the top under every sort — "
      "it is the one thing on this page that must not scroll away")
_ackd = M.sort_rows([_row(1, 30, 20), _row(9, 19, 20, trig=NOW, ack=True)])
check(_ackd[0]["sortOrder"] == 1,
      "once acknowledged it returns to normal order; the pin is a demand "
      "for attention, not a permanent promotion")


# ── candidate entry ──
_cd = M.validate_candidate({"ticker": " cmp ", "name": "Compass",
                            "sourceNote": "salt mine"})
check(_cd["ticker"] == "CMP" and _cd["stage"] == "CANDIDATE",
      "a candidate needs only ticker, name and why you noticed it")
check(raises(M.validate_candidate, {"ticker": "X", "name": "Y"}),
      "the source note is required — future you will not remember")
check(raises(M.validate_candidate, {"name": "Y", "sourceNote": "z"})
      and raises(M.validate_candidate, {"ticker": "X", "sourceNote": "z"}),
      "and so are ticker and name")
check(M.validate_candidate({"ticker": "BRK.B", "name": "B",
                            "sourceNote": "s"})["ticker"] == "BRK.B",
      "dotted and hyphenated class tickers are accepted, not rejected as "
      "malformed")
check(raises(M.validate_candidate, {"ticker": "not a ticker!",
                                    "name": "B", "sourceNote": "s"}),
      "but free text is not a ticker")


# ── NAICS import ──
check(M.normalize_naics("212311.0") == "212311",
      "a NAICS code arriving as a float from a spreadsheet still matches — "
      "unparsed, it would look like 'not in the target list' rather than "
      "'not read', and the miss would be invisible")
check(M.normalize_naics('="327310"') == "327310",
      "and so does an Excel text guard")
check(M.normalize_naics("") is None and M.normalize_naics(None) is None,
      "nothing in, nothing claimed")

check(M.parse_market_cap("$1.2B") == 1.2e9
      and M.parse_market_cap("450M") == 450e6
      and M.parse_market_cap("1,234,000") == 1234000.0
      and M.parse_market_cap(2.5e9) == 2.5e9,
      "market caps are read in the formats exports actually produce")
check(M.parse_market_cap("n/a") is None and M.parse_market_cap("") is None,
      "and an unreadable cap is None, NOT zero — zero would slip under "
      "every ceiling and import the whole file")

CSV = (
    "Symbol,Company Name,NAICS,Market Cap\n"
    "MCEM,Monarch Cement,327310,$180M\n"
    "USLM,US Lime,327410,2950000000\n"
    "BIGCO,Huge Cement,327310,$44B\n"
    "AAPL,Apple,334220,$3T\n"
    "NOCAP,Mystery Quarry,212321,\n"
    "MCEM,Monarch Cement again,327310,$180M\n"
    ",Nameless,327310,$1M\n"
)
_p = M.parse_csv(CSV)
check(len(_p["rows"]) == 6 and len(_p["skipped"]) == 1,
      "the header aliases resolve Symbol/Company Name/Market Cap, and the "
      "row with no ticker is SKIPPED AND COUNTED rather than silently "
      "dropped — a parser that quietly discards rows makes an import look "
      "complete when it is not")

_f = M.filter_import(_p["rows"], existing_tickers=["USLM"])
check([r["ticker"] for r in _f["matched"]] == ["MCEM"],
      "one new name survives: in a target NAICS, under the ceiling, not "
      "already tracked, not a duplicate")
check(_f["matched"][0]["sourceNote"] == "cement manufacturing",
      "and it arrives with the NAICS description as its source note, so "
      "even a bulk-added candidate says why it is there")
check([r["ticker"] for r in _f["already_tracked"]] == ["USLM"],
      "an existing ticker is flagged rather than re-added")
check([r["ticker"] for r in _f["over_ceiling"]] == ["BIGCO"], "the ceiling bites")
check([r["ticker"] for r in _f["wrong_naics"]] == ["AAPL"], "so does the code list")
check([r["ticker"] for r in _f["unreadable_cap"]] == ["NOCAP"],
      "a row whose cap could not be read is held back in its OWN bucket, "
      "not waved through as free and not lumped in with the too-big")
check([r["ticker"] for r in _f["duplicate_in_file"]] == ["MCEM"],
      "and the file's own duplicate is caught")
check(sum(v for k, v in _f["counts"].items() if k != "input") == _f["counts"]["input"],
      "EVERY input row lands in exactly one bucket and the buckets sum to "
      "the input — a filter that reports only its winners cannot be checked")

check(M.filter_import(_p["rows"], cap_ceiling=None)["counts"]["over_ceiling"] == 0
      and any(r["ticker"] == "NOCAP" for r in
              M.filter_import(_p["rows"], cap_ceiling=None)["matched"]),
      "with no ceiling set, an unreadable cap stops being a reason to hold "
      "a row back, because nothing is being compared against")
check(raises(M.parse_csv, "")
      and raises(M.parse_csv, "col_a,col_b\n1,2\n"),
      "an empty paste, or one with no ticker/NAICS column, is refused with "
      "a message rather than importing nothing and reporting success")
check("Symbol" in str(_p["columns"]) or "ticker" in _p["columns"],
      "the resolved columns are reported back so a surprising result can "
      "be traced to how the header was read")
check(len(M.NAICS_TARGETS) == 19 and all(len(c) == 6 for c in M.NAICS_TARGETS),
      "the target list is the 19 six-digit codes, configurable rather than "
      "hardcoded at the call site")


# ── the twelve ──
check(len(M.SEED) == 12, "twelve seed holdings")
check(len({s["ticker"] for s in M.SEED}) == 12, "with distinct tickers")
check(all(s.get("moatType") in M.MOAT_TYPES for s in M.SEED)
      and all(s.get("sentiment") in M.SENTIMENTS for s in M.SEED),
      "every seed carries a valid moat type and sentiment")
check(all(not M.missing_qualify_fields(s) for s in M.SEED),
      "and every one of them already satisfies the promotion fields, which "
      "is what lets them seed at QUALIFIED honestly")
_cmp = next(s for s in M.SEED if s["ticker"] == "CMP")
check(_cmp["anchorPrice"] == 28.50 and _cmp["anchorAsOf"] == "2026-08-01",
      "anchor prices are carried where known")
check(sum(1 for s in M.SEED if s["anchorPrice"] is None) == 10,
      "and left as None for the ten with no known price — never invented, "
      "which is what an anchor of 0 would be")


# ── report ──
if _FAILS:
    print(f"FAIL — {len(_FAILS)}/{_COUNT} checks failed:")
    for m in _FAILS:
        print("  ✗", m)
    sys.exit(1)
print(f"OK — all {_COUNT} permit-moat checks passed.")
print(f"   {len(M.SEED)} seeds, {len(M.NAICS_TARGETS)} NAICS codes, "
      f"armed cap {M.ARMED_CAP}, decay {M.DECAY_DAYS}d, stale {M.STALE_DAYS}d")
sys.exit(0)

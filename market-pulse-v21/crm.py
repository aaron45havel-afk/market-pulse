"""Private sales-pipeline CRM (per BUILD_SPEC.md).

Single-page tracker for our two-person government-AI consulting
funnel. Admin-gated. Stage changes write to crm_stage_events so we
can compute funnel/KPI metrics over any date range, not just the
current-state snapshot.

Schema lives in database.py; this module is the read/write API plus
the analytics + seeding.
"""
from __future__ import annotations

import logging
import math
from datetime import date, datetime, timedelta
from typing import Any

from database import _get_conn

logger = logging.getLogger(__name__)

STAGES = (
    "QUEUED",
    "CONTACTED",
    "REPLIED",
    "DISCOVERY_CALL",
    "PILOT",
    "RECURRING",
    "LOST",
)
ACTIVE_STAGES = tuple(s for s in STAGES if s != "LOST")
METRICS = ("NEW_CONTACTS", "EMAILS_SENT", "CALLS_BOOKED", "PILOTS_CLOSED")

METRIC_LABELS = {
    "NEW_CONTACTS":  "New contacts",
    "EMAILS_SENT":   "Emails sent",
    "CALLS_BOOKED":  "Calls booked",
    "PILOTS_CLOSED": "Pilots closed",
}

# ─── Path to $1M ARR in 3 years ─────────────────────────────────────
# These constants are the inputs to the back-calculation that drives
# the weekly volume targets. Tweak ARR_GOAL or GOAL_HORIZON_WEEKS and
# the dashboard updates everywhere.
ARR_GOAL = 1_000_000
GOAL_HORIZON_WEEKS = 156          # 3 years
AVG_RECURRING_DEAL_FALLBACK = 22_000   # seed median; live data overrides
PILOT_TO_RECURRING_RATE = 0.60    # rough industry default for consulting

# Funnel rates used both for the targets in funnel_conversion() and
# for the volume math here. Keep these in sync.
PLAYBOOK_REPLY_RATE = 0.175
PLAYBOOK_CALL_RATE = 0.50
PLAYBOOK_PILOT_RATE = 0.33


def _avg_recurring_deal_live() -> int:
    """Average recurring_value across all non-zero contacts. Falls back
    to the seed median when there's no data yet OR when the DB driver
    isn't available (local dev / test importing the module)."""
    try:
        conn = _get_conn()
    except Exception:
        return AVG_RECURRING_DEAL_FALLBACK
    if not conn:
        return AVG_RECURRING_DEAL_FALLBACK
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT AVG(recurring_value) FROM crm_contacts
            WHERE recurring_value > 0
        """)
        avg = cur.fetchone()[0]
        cur.close()
        return int(avg) if avg else AVG_RECURRING_DEAL_FALLBACK
    finally:
        conn.close()


def _derive_weekly_targets() -> dict[str, int]:
    """Working backwards from ARR_GOAL through pilot→recurring and the
    three funnel rates. All four metrics round UP so hitting the
    target keeps you on pace or ahead."""
    avg_deal = _avg_recurring_deal_live()
    deals_needed = ARR_GOAL / avg_deal
    pilots_needed = deals_needed / PILOT_TO_RECURRING_RATE
    pilots_per_wk = pilots_needed / GOAL_HORIZON_WEEKS
    calls_per_wk = pilots_per_wk / PLAYBOOK_PILOT_RATE
    replies_per_wk = calls_per_wk / PLAYBOOK_CALL_RATE
    emails_per_wk = replies_per_wk / PLAYBOOK_REPLY_RATE
    return {
        "NEW_CONTACTS":  max(1, math.ceil(emails_per_wk)),
        "EMAILS_SENT":   max(1, math.ceil(emails_per_wk)),
        "CALLS_BOOKED":  max(1, math.ceil(calls_per_wk)),
        "PILOTS_CLOSED": max(1, math.ceil(pilots_per_wk)),
    }


def arr_path_to_goal() -> dict:
    """Snapshot of the path: assumptions, derived targets, current
    progress, deadline. Powers the 'Path to $1M ARR' panel."""
    avg_deal = _avg_recurring_deal_live()
    targets = _derive_weekly_targets()

    # Current booked ARR
    contacts = list_contacts()
    booked = sum(c["recurring_value"] or 0 for c in contacts if c["stage"] == "RECURRING")
    pct = (booked / ARR_GOAL * 100) if ARR_GOAL else 0

    deadline = date.today() + timedelta(days=GOAL_HORIZON_WEEKS * 7)
    weeks_left = max(0, (deadline - date.today()).days // 7)
    return {
        "goal":          ARR_GOAL,
        "horizon_weeks": GOAL_HORIZON_WEEKS,
        "deadline":      deadline,
        "weeks_left":    weeks_left,
        "current_arr":   booked,
        "pct_to_goal":   round(pct, 1),
        "avg_deal":      avg_deal,
        "deals_needed":  math.ceil(ARR_GOAL / avg_deal),
        "pilots_needed": math.ceil(ARR_GOAL / avg_deal / PILOT_TO_RECURRING_RATE),
        "targets":       targets,
    }


# get_weekly_goals() calls _derive_weekly_targets() on each request so
# the live math (current avg_deal × funnel rates) always reflects the
# latest pipeline data. No module-level constant — would freeze
# numbers at import time and force a restart per deal closed.

STAGE_LABELS = {
    "QUEUED":         "Queued",
    "CONTACTED":      "Contacted",
    "REPLIED":        "Replied",
    "DISCOVERY_CALL": "Discovery call",
    "PILOT":          "Pilot",
    "RECURRING":      "Recurring",
    "LOST":           "Lost",
}

# ─── Industries + email templates ────────────────────────────────────
INDUSTRIES = (
    "Government / Municipal Finance",
    "Real Estate",
    "Healthcare",
    "Manufacturing",
    "Tech / SaaS",
    "Nonprofit",
    "Professional Services",
    "Other",
)

# Template triggers — what email you'd send next. Each maps to a
# natural stage of the funnel; STAGE_TO_NEXT_TRIGGER picks the right
# one for a contact based on their current stage.
EMAIL_TRIGGERS = {
    "INTRO":            "First cold outreach",
    "BUMP_NO_REPLY":    "Bump after silence",
    "SCHEDULING":       "Schedule the call (positive reply)",
    "PRE_CALL_CONFIRM": "Day-before call confirmation",
    "POST_CALL":        "Post-call summary",
    "PROPOSAL":         "Pilot proposal",
    "CHECKIN":          "Mid-pilot check-in",
    "RENEWAL":          "Renewal nudge",
}

STAGE_TO_NEXT_TRIGGER = {
    "QUEUED":         "INTRO",
    "CONTACTED":      "BUMP_NO_REPLY",
    "REPLIED":        "SCHEDULING",
    "DISCOVERY_CALL": "POST_CALL",
    "PILOT":          "CHECKIN",
    "RECURRING":      "RENEWAL",
    "LOST":           None,
}

# Sender name used in {my_name} substitution. Could move to env var if
# Jim needs his own outgoing identity later.
SENDER_NAME = "Aaron Havel"


# ─── Date helpers ────────────────────────────────────────────────────
def iso_week_start(d: date | None = None) -> date:
    """Monday of the ISO week containing d (default today). Used for
    bucketing weekly KPIs."""
    d = d or date.today()
    return d - timedelta(days=d.weekday())


def iso_week_range(d: date | None = None) -> tuple[date, date]:
    """(monday, sunday) of the ISO week containing d."""
    monday = iso_week_start(d)
    return monday, monday + timedelta(days=6)


# ─── Contacts CRUD ───────────────────────────────────────────────────
def list_contacts() -> list[dict]:
    """All contacts ordered by stage then most-recent update."""
    conn = _get_conn()
    if not conn:
        return []
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT id, name, title, agency, email, stage,
                   pilot_value, recurring_value,
                   date_emailed, next_date, subject, notes,
                   industry, created_at, updated_at
            FROM crm_contacts
            ORDER BY updated_at DESC
        """)
        rows = cur.fetchall()
        cols = ["id", "name", "title", "agency", "email", "stage",
                "pilot_value", "recurring_value", "date_emailed",
                "next_date", "subject", "notes",
                "industry", "created_at", "updated_at"]
        out = [dict(zip(cols, r)) for r in rows]
        cur.close()
        return out
    finally:
        conn.close()


def add_contact(*, name: str, title: str | None, agency: str | None,
                email: str | None, stage: str, pilot_value: int,
                recurring_value: int, date_emailed: date | None,
                next_date: date | None, subject: str | None,
                notes: str | None, industry: str | None = None) -> int | None:
    """Insert a new contact + initial StageEvent (from_stage=NULL).
    Returns the new contact's id."""
    if stage not in STAGES:
        raise ValueError(f"unknown stage: {stage}")
    conn = _get_conn()
    if not conn:
        return None
    try:
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO crm_contacts
              (name, title, agency, email, stage, pilot_value, recurring_value,
               date_emailed, next_date, subject, notes, industry)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            RETURNING id
        """, (name, title, agency, email, stage, pilot_value, recurring_value,
              date_emailed, next_date, subject, notes, industry))
        contact_id = cur.fetchone()[0]
        cur.execute("""
            INSERT INTO crm_stage_events (contact_id, from_stage, to_stage)
            VALUES (%s, NULL, %s)
        """, (contact_id, stage))
        conn.commit()
        cur.close()
        return contact_id
    finally:
        conn.close()


def change_stage(contact_id: int, new_stage: str) -> bool:
    """Update a contact's stage and append a StageEvent. No-op if the
    new stage matches the current one."""
    if new_stage not in STAGES:
        raise ValueError(f"unknown stage: {new_stage}")
    conn = _get_conn()
    if not conn:
        return False
    try:
        cur = conn.cursor()
        cur.execute("SELECT stage FROM crm_contacts WHERE id = %s", (contact_id,))
        row = cur.fetchone()
        if not row:
            cur.close()
            return False
        old_stage = row[0]
        if old_stage == new_stage:
            cur.close()
            return True
        cur.execute("""
            UPDATE crm_contacts SET stage = %s, updated_at = NOW()
            WHERE id = %s
        """, (new_stage, contact_id))
        cur.execute("""
            INSERT INTO crm_stage_events (contact_id, from_stage, to_stage)
            VALUES (%s, %s, %s)
        """, (contact_id, old_stage, new_stage))
        conn.commit()
        cur.close()
        return True
    finally:
        conn.close()


def update_contact(contact_id: int, *,
                   name: str | None = None, title: str | None = None,
                   agency: str | None = None, email: str | None = None,
                   pilot_value: int | None = None,
                   recurring_value: int | None = None,
                   date_emailed: date | None = None,
                   next_date: date | None = None,
                   subject: str | None = None,
                   notes: str | None = None) -> bool:
    """Patch any subset of editable fields. None means 'leave as-is' for
    the scalar fields; pass an explicit empty string to clear text fields
    or 0 to clear money fields."""
    conn = _get_conn()
    if not conn:
        return False
    try:
        cur = conn.cursor()
        sets = []
        params: list = []
        for col, val in [
            ("name", name), ("title", title), ("agency", agency),
            ("email", email), ("pilot_value", pilot_value),
            ("recurring_value", recurring_value),
            ("date_emailed", date_emailed), ("next_date", next_date),
            ("subject", subject), ("notes", notes),
        ]:
            if val is not None:
                sets.append(f"{col} = %s")
                params.append(val)
        if not sets:
            cur.close()
            return True
        sets.append("updated_at = NOW()")
        params.append(contact_id)
        cur.execute(
            f"UPDATE crm_contacts SET {', '.join(sets)} WHERE id = %s",
            tuple(params),
        )
        conn.commit()
        cur.close()
        return True
    finally:
        conn.close()


def delete_contact(contact_id: int) -> bool:
    conn = _get_conn()
    if not conn:
        return False
    try:
        cur = conn.cursor()
        cur.execute("DELETE FROM crm_contacts WHERE id = %s", (contact_id,))
        conn.commit()
        cur.close()
        return True
    finally:
        conn.close()


# ─── Analytics ───────────────────────────────────────────────────────
def arr_rollup(contacts: list[dict]) -> dict:
    """Top-of-page money summary."""
    booked = sum(c["recurring_value"] or 0 for c in contacts if c["stage"] == "RECURRING")
    pilots_in_flight = [c for c in contacts if c["stage"] == "PILOT"]
    pilots_value = sum(c["pilot_value"] or 0 for c in pilots_in_flight)
    open_pipeline = sum(
        (c["recurring_value"] or 0)
        for c in contacts if c["stage"] in ("DISCOVERY_CALL", "PILOT")
    )
    return {
        "booked_arr": booked,
        "pilots_count": len(pilots_in_flight),
        "pilots_value": pilots_value,
        "open_recurring_pipeline": open_pipeline,
    }


def weekly_kpis(week_start: date | None = None) -> dict[str, int]:
    """Compute the 4 metrics for a single ISO week (Mon-Sun).
    Returns {metric: actual_count}."""
    monday, sunday = iso_week_range(week_start)
    # Use end-of-day sunday so events on Sunday are included.
    end_inclusive = datetime.combine(sunday + timedelta(days=1), datetime.min.time())
    start_inclusive = datetime.combine(monday, datetime.min.time())

    conn = _get_conn()
    if not conn:
        return {m: 0 for m in METRICS}
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT COUNT(*) FROM crm_contacts
            WHERE created_at >= %s AND created_at < %s
        """, (start_inclusive, end_inclusive))
        new_contacts = cur.fetchone()[0]

        cur.execute("""
            SELECT to_stage, COUNT(*) FROM crm_stage_events
            WHERE occurred_at >= %s AND occurred_at < %s
            GROUP BY to_stage
        """, (start_inclusive, end_inclusive))
        by_stage = {r[0]: r[1] for r in cur.fetchall()}
        cur.close()

        return {
            "NEW_CONTACTS":  new_contacts,
            "EMAILS_SENT":   by_stage.get("CONTACTED", 0),
            "CALLS_BOOKED":  by_stage.get("DISCOVERY_CALL", 0),
            "PILOTS_CLOSED": by_stage.get("PILOT", 0) + by_stage.get("RECURRING", 0),
        }
    finally:
        conn.close()


# ─── Funnel conversion (over any date range) ────────────────────────
# Lynch's reply-rate / call-rate / pilot-rate targets, used to color
# the funnel steps green/red. From the team's outbound playbook.
FUNNEL_TARGETS = {
    "reply_rate":    17.5,   # REPLIED / CONTACTED
    "call_rate":     50.0,   # DISCOVERY_CALL / REPLIED
    "pilot_rate":    33.0,   # PILOT / DISCOVERY_CALL
}

FUNNEL_STAGES = ("CONTACTED", "REPLIED", "DISCOVERY_CALL", "PILOT", "RECURRING")


def funnel_conversion(start: date, end: date) -> dict:
    """Counts of stage-events landing in each funnel stage between
    [start, end] inclusive, plus the conversion % at each step.

    Computes from stage history (crm_stage_events), so a contact that
    was created LOST during this window contributes only to LOST and
    not to CONTACTED — which is what you want for true conversion."""
    start_dt = datetime.combine(start, datetime.min.time())
    end_dt = datetime.combine(end + timedelta(days=1), datetime.min.time())
    conn = _get_conn()
    counts = {s: 0 for s in FUNNEL_STAGES}
    if not conn:
        return {"counts": counts, "rates": {}, "targets": FUNNEL_TARGETS}
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT to_stage, COUNT(*) FROM crm_stage_events
            WHERE occurred_at >= %s AND occurred_at < %s
              AND to_stage = ANY(%s)
            GROUP BY to_stage
        """, (start_dt, end_dt, list(FUNNEL_STAGES)))
        for stage, n in cur.fetchall():
            counts[stage] = n
        cur.close()
    finally:
        conn.close()

    def _pct(num: int, den: int) -> float | None:
        if not den:
            return None
        return round(num / den * 100, 1)

    rates = {
        "reply_rate":     _pct(counts["REPLIED"], counts["CONTACTED"]),
        "call_rate":      _pct(counts["DISCOVERY_CALL"], counts["REPLIED"]),
        "pilot_rate":     _pct(counts["PILOT"], counts["DISCOVERY_CALL"]),
        "recurring_rate": _pct(counts["RECURRING"], counts["PILOT"]),
    }
    return {"counts": counts, "rates": rates, "targets": FUNNEL_TARGETS}


# ─── Trailing 8-week KPI series (per metric) ────────────────────────
def trailing_weekly_kpis(weeks: int = 8) -> list[dict]:
    """One row per ISO week (Monday-start) covering the trailing N
    weeks ending with the current one. Each row has the 4 metrics so
    the template can render one mini bar chart per metric.

    Returns oldest → newest so chart bars read left-to-right.
    """
    today = date.today()
    current_monday = iso_week_start(today)
    first_monday = current_monday - timedelta(days=7 * (weeks - 1))
    end_dt = datetime.combine(current_monday + timedelta(days=7), datetime.min.time())
    start_dt = datetime.combine(first_monday, datetime.min.time())

    # Build empty weekly buckets
    series: list[dict] = []
    for i in range(weeks):
        wk_start = first_monday + timedelta(days=7 * i)
        series.append({
            "week_start": wk_start,
            "NEW_CONTACTS": 0,
            "EMAILS_SENT": 0,
            "CALLS_BOOKED": 0,
            "PILOTS_CLOSED": 0,
        })

    def _week_index(dt) -> int | None:
        d = dt.date() if hasattr(dt, "date") else dt
        delta = (iso_week_start(d) - first_monday).days // 7
        return delta if 0 <= delta < weeks else None

    conn = _get_conn()
    if not conn:
        return series
    try:
        cur = conn.cursor()
        # New contacts per week (uses created_at).
        cur.execute("""
            SELECT created_at FROM crm_contacts
            WHERE created_at >= %s AND created_at < %s
        """, (start_dt, end_dt))
        for (created_at,) in cur.fetchall():
            i = _week_index(created_at)
            if i is not None:
                series[i]["NEW_CONTACTS"] += 1

        # Stage events per week.
        cur.execute("""
            SELECT to_stage, occurred_at FROM crm_stage_events
            WHERE occurred_at >= %s AND occurred_at < %s
        """, (start_dt, end_dt))
        for to_stage, occurred_at in cur.fetchall():
            i = _week_index(occurred_at)
            if i is None:
                continue
            if to_stage == "CONTACTED":
                series[i]["EMAILS_SENT"] += 1
            elif to_stage == "DISCOVERY_CALL":
                series[i]["CALLS_BOOKED"] += 1
            elif to_stage in ("PILOT", "RECURRING"):
                series[i]["PILOTS_CLOSED"] += 1
        cur.close()
    finally:
        conn.close()
    return series


# ─── Weekly goals ────────────────────────────────────────────────────
def get_weekly_goals(week_start: date | None = None) -> dict[str, int]:
    """Return effective weekly goals for the given week.

    Targets are derived live from the path-to-$1M math each call —
    so closing a higher-value deal automatically softens the volume
    targets without a restart. Manual per-week DB overrides still win
    (no UI surface today; endpoint stays in case we want it back)."""
    week_start = week_start or iso_week_start()
    out = _derive_weekly_targets()
    conn = _get_conn()
    if not conn:
        return out
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT metric, target FROM crm_weekly_goals
            WHERE week_start = %s
        """, (week_start,))
        for metric, target in cur.fetchall():
            out[metric] = target
        cur.close()
        return out
    finally:
        conn.close()


def goals_completion_stats() -> dict:
    """Walk every COMPLETED past ISO week (strictly before current
    Monday) once, and count goal hits + 'perfect' weeks. Also reports
    in-progress numbers for the current week so the UI can show
    'N/4 hit so far'.

    Returns:
      {
        "this_week_hits": int 0-4,
        "perfect_weeks":  int — weeks where all 4 metrics hit target,
        "total_hits":     int — sum of individual goals hit,
        "possible_hits":  int — perfect_weeks would be x of /
                              possible_hits / 4, total_hits of /
                              possible_hits.
        "completed_weeks": int — how many past weeks we counted.
      }
    """
    current_monday = iso_week_start()
    current_monday_dt = datetime.combine(current_monday, datetime.min.time())

    conn = _get_conn()
    blank = {"this_week_hits": 0, "perfect_weeks": 0, "total_hits": 0,
             "possible_hits": 0, "completed_weeks": 0}
    if not conn:
        return blank
    try:
        cur = conn.cursor()
        # Earliest activity tells us where to start counting weeks.
        cur.execute("SELECT MIN(created_at) FROM crm_contacts")
        c_min = cur.fetchone()[0]
        cur.execute("SELECT MIN(occurred_at) FROM crm_stage_events")
        e_min = cur.fetchone()[0]
        anchors = [a for a in (c_min, e_min) if a is not None]
        if not anchors:
            cur.close()
            return blank
        start_week = iso_week_start(min(a.date() for a in anchors))

        # One-shot weekly aggregations (Postgres date_trunc('week', ...)
        # returns the Monday of the ISO week — same definition we use).
        cur.execute("""
            SELECT date_trunc('week', created_at)::date AS wk, COUNT(*)
            FROM crm_contacts
            WHERE created_at >= %s
            GROUP BY wk
        """, (datetime.combine(start_week, datetime.min.time()),))
        new_by_wk = {r[0]: r[1] for r in cur.fetchall()}

        cur.execute("""
            SELECT date_trunc('week', occurred_at)::date AS wk, to_stage, COUNT(*)
            FROM crm_stage_events
            WHERE occurred_at >= %s
            GROUP BY wk, to_stage
        """, (datetime.combine(start_week, datetime.min.time()),))
        ev_by_wk: dict[date, dict[str, int]] = {}
        for wk, stage, n in cur.fetchall():
            ev_by_wk.setdefault(wk, {})[stage] = n
        cur.close()
    finally:
        conn.close()

    def _metrics_for_week(wk: date) -> dict[str, int]:
        events = ev_by_wk.get(wk, {})
        return {
            "NEW_CONTACTS":  new_by_wk.get(wk, 0),
            "EMAILS_SENT":   events.get("CONTACTED", 0),
            "CALLS_BOOKED":  events.get("DISCOVERY_CALL", 0),
            "PILOTS_CLOSED": events.get("PILOT", 0) + events.get("RECURRING", 0),
        }

    # Past completed weeks: start_week ... current_monday - 7 (inclusive)
    perfect_weeks = 0
    total_hits = 0
    completed_weeks = 0
    wk = start_week
    while wk < current_monday:
        completed_weeks += 1
        goals = get_weekly_goals(wk)
        metrics = _metrics_for_week(wk)
        hits = sum(1 for m in METRICS if metrics[m] >= goals[m])
        total_hits += hits
        if hits == len(METRICS):
            perfect_weeks += 1
        wk = wk + timedelta(days=7)

    # Current week (partial)
    this_goals = get_weekly_goals(current_monday)
    this_metrics = _metrics_for_week(current_monday)
    this_week_hits = sum(1 for m in METRICS if this_metrics[m] >= this_goals[m])

    return {
        "this_week_hits":   this_week_hits,
        "perfect_weeks":    perfect_weeks,
        "total_hits":       total_hits,
        "possible_hits":    completed_weeks * len(METRICS),
        "completed_weeks":  completed_weeks,
    }


def set_weekly_goal(metric: str, target: int,
                    week_start: date | None = None) -> bool:
    if metric not in METRICS:
        raise ValueError(f"unknown metric: {metric}")
    week_start = week_start or iso_week_start()
    conn = _get_conn()
    if not conn:
        return False
    try:
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO crm_weekly_goals (week_start, metric, target)
            VALUES (%s, %s, %s)
            ON CONFLICT (week_start, metric)
            DO UPDATE SET target = EXCLUDED.target
        """, (week_start, metric, target))
        conn.commit()
        cur.close()
        return True
    finally:
        conn.close()


# ─── Industry update ────────────────────────────────────────────────
def set_contact_industry(contact_id: int, industry: str | None) -> bool:
    """Inline industry edit from the pipeline card. None clears it."""
    if industry is not None and industry not in INDUSTRIES:
        return False
    conn = _get_conn()
    if not conn:
        return False
    try:
        cur = conn.cursor()
        cur.execute("""
            UPDATE crm_contacts SET industry = %s, updated_at = NOW()
            WHERE id = %s
        """, (industry, contact_id))
        conn.commit()
        cur.close()
        return True
    finally:
        conn.close()


# ─── Email templates ────────────────────────────────────────────────
def list_templates() -> list[dict]:
    conn = _get_conn()
    if not conn:
        return []
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT id, industry, trigger, subject, body,
                   created_at, updated_at
            FROM crm_email_templates
            ORDER BY industry, trigger
        """)
        rows = cur.fetchall()
        cols = ["id", "industry", "trigger", "subject", "body",
                "created_at", "updated_at"]
        out = [dict(zip(cols, r)) for r in rows]
        cur.close()
        return out
    finally:
        conn.close()


def get_template(industry: str, trigger: str) -> dict | None:
    conn = _get_conn()
    if not conn:
        return None
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT id, industry, trigger, subject, body
            FROM crm_email_templates
            WHERE industry = %s AND trigger = %s
        """, (industry, trigger))
        row = cur.fetchone()
        cur.close()
        if not row:
            return None
        return dict(zip(["id", "industry", "trigger", "subject", "body"], row))
    finally:
        conn.close()


def upsert_template(*, industry: str, trigger: str,
                    subject: str, body: str) -> bool:
    if industry not in INDUSTRIES or trigger not in EMAIL_TRIGGERS:
        return False
    conn = _get_conn()
    if not conn:
        return False
    try:
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO crm_email_templates
              (industry, trigger, subject, body)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (industry, trigger) DO UPDATE
              SET subject = EXCLUDED.subject,
                  body    = EXCLUDED.body,
                  updated_at = NOW()
        """, (industry, trigger, subject, body))
        conn.commit()
        cur.close()
        return True
    finally:
        conn.close()


def delete_template(template_id: int) -> bool:
    conn = _get_conn()
    if not conn:
        return False
    try:
        cur = conn.cursor()
        cur.execute("DELETE FROM crm_email_templates WHERE id = %s", (template_id,))
        conn.commit()
        cur.close()
        return True
    finally:
        conn.close()


def render_template(template: dict, contact: dict,
                    sender_name: str = SENDER_NAME) -> dict:
    """Substitute {first_name} / {name} / {title} / {agency} /
    {my_name} in subject + body, plus {call_summary} / {win_condition}
    from the contact's most recent discovery call when one exists.
    Falls through to the raw string when a template references
    something we don't know about."""
    name = (contact.get("name") or "").strip()
    first_name = name.split()[0] if name else ""
    variables = {
        "first_name":     first_name,
        "name":           name,
        "title":          contact.get("title") or "",
        "agency":         contact.get("agency") or "",
        "my_name":        sender_name,
        "call_summary":   "",
        "win_condition":  "",
    }

    # Pull call data if the contact has had a discovery call.
    cid = contact.get("id")
    if cid:
        try:
            call = get_call_for_contact(cid)
            if call:
                variables["call_summary"] = (call.get("exec_summary") or "").strip()
                try:
                    import json as _json
                    ex = _json.loads(call.get("extraction_json") or "{}")
                    variables["win_condition"] = (ex.get("win_condition") or "").strip()
                except (ValueError, TypeError):
                    pass
        except Exception:
            pass

    def _sub(s: str) -> str:
        if not s:
            return s
        try:
            return s.format(**variables)
        except (KeyError, IndexError, ValueError):
            return s

    return {
        "subject": _sub(template.get("subject", "") or ""),
        "body":    _sub(template.get("body", "") or ""),
    }


def suggest_email_for_contact(contact: dict) -> dict:
    """Pick the right template for a contact based on their current
    stage + industry, render it, and report whether we had to fall
    back. Always returns a payload; callers can render the modal
    even when no template exists yet."""
    trigger = STAGE_TO_NEXT_TRIGGER.get(contact.get("stage"))
    industry = contact.get("industry")
    template = None
    fallback_industry = False

    if trigger and industry:
        template = get_template(industry, trigger)
    # Fallback: if no template for this industry, try the first
    # industry that has one for this trigger. Better than empty.
    if trigger and not template:
        for ind in INDUSTRIES:
            t = get_template(ind, trigger)
            if t:
                template = t
                fallback_industry = True
                break

    if not template:
        return {
            "trigger":          trigger,
            "trigger_label":    EMAIL_TRIGGERS.get(trigger, "") if trigger else "",
            "industry":         industry,
            "has_template":     False,
            "fallback_industry": False,
            "subject":          "",
            "body":             "",
        }

    rendered = render_template(template, contact)
    return {
        "trigger":          trigger,
        "trigger_label":    EMAIL_TRIGGERS.get(trigger, ""),
        "industry":         template.get("industry"),
        "has_template":     True,
        "fallback_industry": fallback_industry,
        "subject":          rendered["subject"],
        "body":             rendered["body"],
    }


# ─── Seed data ───────────────────────────────────────────────────────
SEED_CONTACTS = [
    dict(
        name="Valerie Ahr", title="Deputy Controller",
        agency="City of Fort Wayne", email="valerie.ahr@cityoffortwayne.org",
        stage="CONTACTED", pilot_value=12000, recurring_value=28000,
        date_emailed=date(2026, 6, 12), next_date=date(2026, 6, 19),
        subject="Invoice approvals without new headcount",
        notes="Peer-credibility variant. Bump once ~6/19 if quiet.",
    ),
    dict(
        name="Nick Jordan", title="Deputy Controller",
        agency="City of Fort Wayne", email="nick.jordan@cityoffortwayne.org",
        stage="QUEUED", pilot_value=12000, recurring_value=28000,
        date_emailed=None, next_date=date(2026, 6, 17),
        subject="Quick question about your AP workflow",
        notes="Send Wed 6/17. Distinct wording from Valerie.",
    ),
    dict(
        name="Leslee Robinson", title="Clerk-Treasurer",
        agency="Columbia City", email="lrobinson@columbiacity.net",
        stage="CONTACTED", pilot_value=6000, recurring_value=10000,
        date_emailed=date(2026, 6, 12), next_date=None,
        subject="A quick hello from one finance office to another",
        notes=("Ex-insurance sales, relationship-driven, Whitley Co Chamber "
               "Ambassador. Real door = Chamber. Small budget."),
    ),
]


def maybe_seed() -> int:
    """Idempotent: only inserts if crm_contacts is empty. Called once
    at app start, after init_db."""
    conn = _get_conn()
    if not conn:
        return 0
    try:
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM crm_contacts")
        if cur.fetchone()[0] > 0:
            cur.close()
            return 0
        cur.close()
    finally:
        conn.close()

    inserted = 0
    for c in SEED_CONTACTS:
        try:
            cid = add_contact(**c)
            if cid:
                inserted += 1
        except Exception as e:
            logger.warning("Seed contact %s failed: %s", c.get("name"), e)
    logger.info("CRM seed: inserted %d starter contacts", inserted)
    return inserted


# ─── Discovery-call framework ────────────────────────────────────────
# The 20-minute discovery-call workflow Aaron wrote. Surfaced in the
# /pipeline modal as Agenda (live cheat sheet) + Process (prompts to
# run through Claude.ai) + Artifacts (saved outputs + scorecard).

DISCOVERY_AGENDA = [
    {
        "time": "0:00–1:30",
        "title": "Frame + consent",
        "who":   "You",
        "script": (
            "Thanks for the time. Before we start — I record these so I can "
            "focus on listening instead of scribbling notes, and so nothing "
            "gets lost. Are you okay with me recording? [WAIT FOR EXPLICIT "
            "YES.] Here's how I'd like to use the next 20 minutes: I'm not "
            "going to pitch anything. I want to understand one process in "
            "your world well enough to tell you honestly whether it's worth "
            "automating. I'll ask a lot, you talk a lot, and at the end I'll "
            "recap. Does that sound good?"
        ),
    },
    {
        "time": "1:30–4:00",
        "title": "Context snapshot",
        "who":   "Them",
        "script": (
            "• Give me the 60-second version: what does the business/agency "
            "do, and where do you personally sit in how the money and the "
            "work move through it?\n"
            "• Of everything you do on a day to day basis, what's the part "
            "that eats the most of your time or causes the most headaches?"
        ),
    },
    {
        "time": "4:00–11:00",
        "title": "The core dig — pick the process they just named",
        "who":   "Them (you probe)",
        "script": (
            "• Walk me through [that process] from the very first moment it "
            "starts to the moment it's finished. Who touches it, and what do "
            "they do at each step?  [Let them run. Silence is your tool.]\n"
            "• Where does it break down or back up most often?\n"
            "• Tell me about the last time it went wrong — what actually "
            "happened?\n"
            "• What do you do today to work around that — spreadsheets, "
            "manual re-keying, other tools?"
        ),
    },
    {
        "time": "11:00–15:00",
        "title": "Quantify + implication",
        "who":   "Them",
        "script": (
            "• How many of these do you handle in a typical week or month, "
            "and how long does each one take?\n"
            "• When it goes wrong, what does that cost you — hours, dollars, "
            "a missed deadline, an angry customer?\n"
            "• What have you already tried to fix it, and why didn't it stick?\n"
            "• If nothing changes, what does this look like in a year?"
        ),
    },
    {
        "time": "15:00–18:00",
        "title": "Decision + money reality",
        "who":   "Them",
        "script": (
            "• Who else touches this, and who would have to sign off to "
            "actually fix it?\n"
            "• When you've bought tools or brought in help before, how does "
            "that decision usually get made — and what's a number that feels "
            "reasonable versus crazy for solving this?\n"
            "• Is this a 'this quarter' problem or a 'someday' problem?\n"
            "• If we built one thing and got it right, what would it have to "
            "do for you to call it a clear win?"
        ),
    },
    {
        "time": "18:00–20:00",
        "title": "Mirror + next step",
        "who":   "You",
        "script": (
            "• Let me play back what I heard: the real problem is [X], it's "
            "costing you roughly [Y], today it works like [Z], and a win "
            "looks like [W]. Did I get that right?\n"
            "• Here's what I'd suggest: I'll turn this into a one-page "
            "picture of the problem and a proposed first slice, and we book "
            "a 30-minute working session to pressure-test it. Fair?"
        ),
    },
]

# Stage-1 extraction. Output JSON schema chosen to make every
# downstream artifact reliable (every claim cites a field). Anything
# implied but not stated lands in assumptions[] so we never hallucinate.
DISCOVERY_PROMPT_EXTRACT = '''You are analyzing a 20-minute discovery-call transcript for a custom-software consulting engagement.

Your job: extract ONLY what was actually said in the transcript. Do not infer, do not guess, do not generalize. If something was implied but not stated, put it in `assumptions[]` with a confidence rating.

Return strict JSON matching this schema exactly:
{
  "core_process": {
    "name": "name of the single process they walked through",
    "steps": ["ordered step in their words", "..."],
    "breakpoints": ["where it breaks or backs up", "..."]
  },
  "problems": [
    {
      "description": "...",
      "is_root_problem": true,
      "severity_1_to_10": 8,
      "supporting_quote": "verbatim quote from transcript"
    }
  ],
  "metrics": [
    {"what": "...", "value": "...", "unit": "hours/dollars/count", "source_quote": "..."}
  ],
  "stakeholders": [
    {"name_or_role": "...", "decision_power": "high|medium|low"}
  ],
  "current_tools": ["..."],
  "failed_solutions": ["..."],
  "concrete_story": "the 'last time it went wrong' story, paraphrased tightly",
  "decision_signals": ["who signs off, approval process, etc."],
  "budget_signals": ["the reasonable/crazy framing, prior spend, etc."],
  "urgency": "this quarter | someday | unclear",
  "win_condition": "the prospect's stated 'if we got one thing right, what would it have to do?'",
  "pain_quotes": ["3-5 verbatim quotes capturing pain"],
  "assumptions": [
    {"text": "thing implied but not stated", "confidence": "low|medium|high"}
  ]
}

Rules:
- Never fabricate names, numbers, or quotes.
- If a field has no evidence, return [] or "".
- Quotes must be verbatim from the transcript.
- Output the JSON object only, no preamble, no markdown fence.

TRANSCRIPT:
"""
{transcript}
"""'''

DISCOVERY_PROMPT_EXEC_SUMMARY = '''Using ONLY the extraction object below, produce a 5-sentence executive summary in this exact structure:

1. Who they are (role + organization).
2. The single core problem, in their words.
3. The cost (use a real number from metrics[] if available; otherwise mark "ASSUMPTION — confirm").
4. The proposed first slice that hits their win_condition.
5. The committed next step.

Every claim must trace to a field in the extraction. Anything not supported by evidence, mark "ASSUMPTION — confirm".

EXTRACTION:
{extraction_json}

Return the 5 sentences only, numbered. No preamble.'''

DISCOVERY_PROMPT_PAIN = '''Using ONLY the extraction object below, produce a ranked pain analysis.

For each item in `problems[]`:
- **Severity** (their 1–10 if given, else "ASSUMPTION")
- **Frequency** (from metrics[] if a count/cadence is mentioned, else "ASSUMPTION — frequency not stated")
- **Cost** (from metrics[] if a $ or hours figure exists for this problem, else "ASSUMPTION")
- **Verbatim quote** that captures it

Order: highest severity first, root problem first on ties.

Output as a numbered Markdown list (1., 2., 3., …). No preamble.

EXTRACTION:
{extraction_json}'''

DISCOVERY_PROMPT_MVP = '''Using ONLY the extraction object below, propose the smallest possible MVP that hits the prospect's stated `win_condition`.

Output in this exact Markdown structure:

**Win condition (their words):** [from win_condition]

**MVP must do:**
- [3–5 items, each one tied to a problem or breakpoint from the extraction]

**Explicitly OUT of MVP:**
- [3–5 items, including anything they mentioned as nice-to-have]

**Open questions for the working session:**
- [3–5 questions that must be answered before scoping the build]

Every MVP item must cite the extraction field it derives from (e.g., "(breakpoints[1])" or "(problems[0])"). Anything not supported, mark "ASSUMPTION — confirm".

EXTRACTION:
{extraction_json}'''


# ─── Scorecard heuristics ───────────────────────────────────────────
# 8 dimensions from the framework's Part 6. Auto-compute from the
# extraction JSON; the user can adjust before saving if the
# transcript fooled the heuristic.
SCORECARD_DIMENSIONS = [
    ("talk_ratio",          15, "Talk ratio (prospect ≥70%)"),
    ("root_problem",        20, "Root problem found"),
    ("quantified_pain",     15, "Quantified pain (≥1 hard number)"),
    ("process_mapped",      15, "Process mapped end-to-end"),
    ("concrete_story",      10, "Concrete past story"),
    ("decision_money",      15, "Decision + money path known"),
    ("win_condition",        5, "Win condition defined"),
    ("next_step",            5, "Next step secured"),
]


def compute_scorecard(extraction_json: str) -> dict:
    """Heuristic 0–100 from the extracted JSON. Talk-ratio and
    next-step can't be inferred from the JSON alone — they default to
    middling values that the user can adjust."""
    import json as _json
    try:
        ex = _json.loads(extraction_json) if extraction_json else {}
    except (ValueError, TypeError):
        ex = {}

    scores: dict[str, int] = {}

    # talk_ratio — no signal in JSON, default to ~half credit.
    scores["talk_ratio"] = 8

    # root_problem — full if any problem is is_root_problem AND has a
    # supporting quote. Half-credit if just is_root_problem.
    root = next((p for p in (ex.get("problems") or [])
                 if p.get("is_root_problem")), None)
    if root and root.get("supporting_quote"):
        scores["root_problem"] = 20
    elif root:
        scores["root_problem"] = 10
    else:
        scores["root_problem"] = 0

    # quantified_pain — full if any metric has a non-empty value.
    has_metric = any((m.get("value") or "").strip()
                     for m in (ex.get("metrics") or []))
    scores["quantified_pain"] = 15 if has_metric else 0

    # process_mapped — full if ≥3 steps walked.
    steps = (ex.get("core_process") or {}).get("steps") or []
    if len(steps) >= 3:
        scores["process_mapped"] = 15
    elif len(steps) >= 1:
        scores["process_mapped"] = 8
    else:
        scores["process_mapped"] = 0

    # concrete_story — full if concrete_story is non-empty.
    scores["concrete_story"] = 10 if (ex.get("concrete_story") or "").strip() else 0

    # decision_money — full if both signals present, half if one.
    has_decision = bool(ex.get("decision_signals"))
    has_budget   = bool(ex.get("budget_signals"))
    if has_decision and has_budget:
        scores["decision_money"] = 15
    elif has_decision or has_budget:
        scores["decision_money"] = 8
    else:
        scores["decision_money"] = 0

    # win_condition — full if non-empty.
    scores["win_condition"] = 5 if (ex.get("win_condition") or "").strip() else 0

    # next_step — can't infer; assume secured (5) since the framework
    # ends with mirror + next-step ask. User can drop to 0 if not.
    scores["next_step"] = 5

    total = sum(scores.values())
    if total >= 85:
        band = "proposal-ready"
        suggested_stage = "PILOT"
    elif total >= 65:
        band = "usable — one gap to close async"
        suggested_stage = "DISCOVERY_CALL"   # stay; book working session
    else:
        band = "underperformed — don't write a proposal yet"
        suggested_stage = "DISCOVERY_CALL"

    return {
        "scores":          scores,
        "total":           total,
        "band":            band,
        "suggested_stage": suggested_stage,
    }


# ─── Discovery-call CRUD ────────────────────────────────────────────
def get_call_for_contact(contact_id: int) -> dict | None:
    conn = _get_conn()
    if not conn:
        return None
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT id, contact_id, call_date, transcript, extraction_json,
                   exec_summary, pain_analysis, mvp_scope, scorecard_json,
                   suggested_stage, created_at, updated_at
            FROM crm_discovery_calls
            WHERE contact_id = %s
        """, (contact_id,))
        row = cur.fetchone()
        cur.close()
        if not row:
            return None
        cols = ["id", "contact_id", "call_date", "transcript",
                "extraction_json", "exec_summary", "pain_analysis",
                "mvp_scope", "scorecard_json", "suggested_stage",
                "created_at", "updated_at"]
        return dict(zip(cols, row))
    finally:
        conn.close()


def upsert_call(*, contact_id: int, call_date: date | None,
                transcript: str, extraction_json: str,
                exec_summary: str, pain_analysis: str, mvp_scope: str) -> dict | None:
    """Save (or overwrite) the discovery call for a contact. Computes
    scorecard + suggested_stage server-side."""
    sc = compute_scorecard(extraction_json)
    import json as _json
    sc_json = _json.dumps(sc)
    conn = _get_conn()
    if not conn:
        return None
    try:
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO crm_discovery_calls
              (contact_id, call_date, transcript, extraction_json,
               exec_summary, pain_analysis, mvp_scope,
               scorecard_json, suggested_stage)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (contact_id) DO UPDATE
              SET call_date       = EXCLUDED.call_date,
                  transcript      = EXCLUDED.transcript,
                  extraction_json = EXCLUDED.extraction_json,
                  exec_summary    = EXCLUDED.exec_summary,
                  pain_analysis   = EXCLUDED.pain_analysis,
                  mvp_scope       = EXCLUDED.mvp_scope,
                  scorecard_json  = EXCLUDED.scorecard_json,
                  suggested_stage = EXCLUDED.suggested_stage,
                  updated_at      = NOW()
        """, (contact_id, call_date, transcript, extraction_json,
              exec_summary, pain_analysis, mvp_scope,
              sc_json, sc["suggested_stage"]))
        conn.commit()
        cur.close()
        return sc
    finally:
        conn.close()


def delete_call(contact_id: int) -> bool:
    conn = _get_conn()
    if not conn:
        return False
    try:
        cur = conn.cursor()
        cur.execute("DELETE FROM crm_discovery_calls WHERE contact_id = %s",
                    (contact_id,))
        conn.commit()
        cur.close()
        return True
    finally:
        conn.close()


def render_prompt(template: str, *, transcript: str = "",
                  extraction_json: str = "") -> str:
    """Substitute {transcript} / {extraction_json} into a prompt
    template without choking on the JSON's curly braces."""
    return (template
            .replace("{transcript}", transcript or "")
            .replace("{extraction_json}", extraction_json or ""))


# ─── Email-template seeds ───────────────────────────────────────────
# The two emails the user shipped in the build spec — INTRO and the
# SCHEDULING follow-up. Industry tagged to Government / Municipal
# Finance since that's the primary funnel today. Add more industries
# later from the /pipeline/templates UI.
SEED_TEMPLATES = [
    dict(
        industry="Government / Municipal Finance",
        trigger="INTRO",
        subject="20 minutes to scope your contract budgeting tool",
        body=("Hi {first_name},\n\n"
              "I build custom software for businesses that run on contracts, "
              "specifically around the budgeting side, which tends to live in "
              "a tangle of spreadsheets until a contract goes over and nobody "
              "catches it in time.\n\n"
              "I come at this from financial operations, not just the tech "
              "side, so I care less about the software itself and more about "
              "how the money actually moves: how you set a budget for each "
              "contract, track spend against it, see where it's going to land, "
              "and catch overruns before they hurt.\n\n"
              "It's not a big platform or a long contract. Whatever I build "
              "layers onto how you already work and adapts to your structure: "
              "your contracts, your cost categories, the thresholds you "
              "actually watch.\n\n"
              "Before suggesting anything, I'd want to understand how you "
              "manage a contract's budget today, from signing through "
              "closeout. Would a short call in the next couple of weeks be "
              "worth your time?\n\n"
              "{my_name}"),
    ),
    dict(
        industry="Government / Municipal Finance",
        trigger="SCHEDULING",
        subject="Re: 20 minutes to scope your contract budgeting tool",
        body=("Hi {first_name},\n\n"
              "Great — let's get it on the calendar. It's about 20 minutes, "
              "and I'll mostly be asking questions: how you handle contract "
              "budgets today, where it gets painful, and what you'd actually "
              "want a tool to do. No pitch — I just want to understand the "
              "workflow so anything I build fits how you already work.\n\n"
              "What works in the next week or so? Send me a couple of windows "
              "and I'll lock one in, or I can propose a few times if that's "
              "easier.\n\n"
              "One heads-up: I'll record the call so I can focus on listening "
              "instead of scribbling notes — good with you?\n\n"
              "Looking forward to it.\n\n"
              "{my_name}"),
    ),
]


def maybe_seed_templates() -> int:
    """Idempotent: insert only if the templates table is empty. Called
    once at app start, after init_db."""
    conn = _get_conn()
    if not conn:
        return 0
    try:
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM crm_email_templates")
        if cur.fetchone()[0] > 0:
            cur.close()
            return 0
        cur.close()
    finally:
        conn.close()

    inserted = 0
    for t in SEED_TEMPLATES:
        try:
            if upsert_template(**t):
                inserted += 1
        except Exception as e:
            logger.warning("Seed template %s/%s failed: %s",
                           t.get("industry"), t.get("trigger"), e)
    logger.info("CRM seed: inserted %d email templates", inserted)
    return inserted

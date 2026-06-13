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
                   created_at, updated_at
            FROM crm_contacts
            ORDER BY updated_at DESC
        """)
        rows = cur.fetchall()
        cols = ["id", "name", "title", "agency", "email", "stage",
                "pilot_value", "recurring_value", "date_emailed",
                "next_date", "subject", "notes", "created_at", "updated_at"]
        out = [dict(zip(cols, r)) for r in rows]
        cur.close()
        return out
    finally:
        conn.close()


def add_contact(*, name: str, title: str | None, agency: str | None,
                email: str | None, stage: str, pilot_value: int,
                recurring_value: int, date_emailed: date | None,
                next_date: date | None, subject: str | None,
                notes: str | None) -> int | None:
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
               date_emailed, next_date, subject, notes)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            RETURNING id
        """, (name, title, agency, email, stage, pilot_value, recurring_value,
              date_emailed, next_date, subject, notes))
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

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


# ─── Weekly goals ────────────────────────────────────────────────────
def get_weekly_goals(week_start: date | None = None) -> dict[str, int]:
    week_start = week_start or iso_week_start()
    conn = _get_conn()
    if not conn:
        return {}
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT metric, target FROM crm_weekly_goals
            WHERE week_start = %s
        """, (week_start,))
        out = {r[0]: r[1] for r in cur.fetchall()}
        cur.close()
        return out
    finally:
        conn.close()


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

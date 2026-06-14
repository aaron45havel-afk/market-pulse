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
import os
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
    "VERCEL_PROJECT",
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
    "VERCEL_PROJECT": "Vercel project",
    "RECURRING":      "Recurring",
    "LOST":           "Lost",
}

# ─── Prototype tracking ─────────────────────────────────────────────
PROTOTYPE_STATUSES = (
    "BUILDING",   # Aaron is iterating, not shareable yet
    "LIVE",       # Public URL is up, client can view + give feedback
    "PAUSED",     # Waiting on a decision or client response
    "CLOSED",     # Wrapped — handed off, abandoned, or rolled into prod
)
PROTOTYPE_STATUS_LABELS = {
    "BUILDING": "Building",
    "LIVE":     "Live",
    "PAUSED":   "Paused",
    "CLOSED":   "Closed",
}


# ─── Industries + email templates ────────────────────────────────────
INDUSTRIES = (
    "Government / Municipal Finance",
    "Construction",
    "Architecture & Engineering",
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

ROLES = (
    "CFO",
    "Finance Director",
    "Controller",
    "Clerk-Treasurer",
    "Project Manager",
    "Operations",
    "Other",
)

HOSTING_MODELS = ("TBD", "Managed", "Client-hosted", "Hybrid")
HOSTING_MODEL_LABELS = {
    "TBD":           "TBD (ask client)",
    "Managed":       "Managed by us",
    "Client-hosted": "Client-hosted",
    "Hybrid":        "Hybrid",
}

# ─── Pilot Agreement Checklist template ────────────────────────────
# Used to render the structured form on the contact card. Each section
# becomes a collapsible group; each item is a checkbox plus optional
# inline inputs (text / number / select).
PILOT_AGREEMENT_SECTIONS = [
    {
        "key": "scope", "title": "A. Scope & deliverable",
        "items": [
            {"key": "problem_statement", "label": "Problem statement (one sentence, in client's words)", "input": "text", "placeholder": "Their words, not yours"},
            {"key": "deliverable",       "label": "Pilot deliverable (what gets built — be specific)", "input": "text"},
            {"key": "success_criteria",  "label": "Success criteria (3 measurable outcomes)", "input": "text", "placeholder": "e.g. close cycle 5 days → 2 days"},
            {"key": "mock_data_only",    "label": "Mock data only flag (pilot uses NO production data)"},
        ],
    },
    {
        "key": "money", "title": "B. Money",
        "items": [
            {"key": "pilot_fee",         "label": "Pilot fee $", "input": "number", "placeholder": "12000"},
            {"key": "billing_terms",     "label": "Billing terms (e.g. 50% upfront / 50% on delivery)", "input": "text"},
            {"key": "duration_weeks",    "label": "Pilot duration (weeks)", "input": "number", "placeholder": "3"},
            {"key": "post_pilot_pricing","label": "Post-pilot pricing discussed (monthly managed-hosting retainer)", "input": "text", "placeholder": "$1,500/mo, includes 4 hrs/mo changes"},
            {"key": "out_of_scope_rate", "label": "Out-of-scope hourly rate $", "input": "number", "placeholder": "200"},
        ],
    },
    {
        "key": "ownership", "title": "C. Code & IP ownership",
        "items": [
            {"key": "client_owns_code",        "label": "Client owns all code at end of pilot"},
            {"key": "focusedops_keeps_patterns","label": "FocusedOps retains right to reuse generic patterns / prompts"},
            {"key": "repo_destination",        "label": "GitHub repo destination", "input": "select", "options": ["TBD", "Client GitHub org", "FocusedOps (managed)"]},
            {"key": "source_escrow",           "label": "Source-code escrow offered (paranoid clients only)"},
        ],
    },
    {
        "key": "hosting", "title": "D. Hosting model",
        "items": [
            {"key": "model_decided", "label": "Hosting model agreed in writing (set in contact's Hosting field)"},
            {"key": "monthly_fee",   "label": "Managed-hosting monthly fee $", "input": "number", "placeholder": "1500"},
        ],
    },
    {
        "key": "auth", "title": "E. Auth & access — triggers for Tier 2",
        "items": [
            {"key": "credentials_in",    "label": "Client wants to plug in real credentials → add auth"},
            {"key": "colleagues_in",     "label": "Client wants colleagues to use it → add auth"},
            {"key": "real_data_in",      "label": "Client wants real data flowing → Tier 3, contract first"},
            {"key": "url_stays",         "label": "Client says 'we'll just keep using your URL' → contract first"},
            {"key": "auth_method",       "label": "Auth method picked", "input": "select", "options": ["TBD", "Google OAuth", "Magic link (Resend)", "Vercel Authentication"]},
        ],
    },
    {
        "key": "data", "title": "F. Data handling — when real data flows",
        "items": [
            {"key": "dpa_signed",          "label": "Data Processing Agreement (DPA) signed"},
            {"key": "data_residency",      "label": "Data residency confirmed (US / EU / other)"},
            {"key": "retention_policy",    "label": "Data retention policy documented (how long after end?)"},
            {"key": "backup_model",        "label": "Backup model documented"},
            {"key": "incident_response",   "label": "Incident response plan written (who to call if breach)"},
        ],
    },
    {
        "key": "liability", "title": "G. Liability & insurance",
        "items": [
            {"key": "liability_cap",   "label": "Liability cap = 12 months of fees paid"},
            {"key": "cyber_insurance", "label": "FocusedOps carries cyber insurance (Hiscox / Thimble)"},
            {"key": "cgl_insurance",   "label": "FocusedOps carries CGL insurance"},
            {"key": "indemnification", "label": "Mutual indemnification clause (you for code, them for their data)"},
        ],
    },
    {
        "key": "support", "title": "H. Support & SLA (post-pilot, managed only)",
        "items": [
            {"key": "response_sla",      "label": "Response time SLA (e.g. 4 business hours)", "input": "text"},
            {"key": "uptime_target",     "label": "Uptime target (don't promise 99.9%)", "input": "text"},
            {"key": "monthly_hours",     "label": "Included monthly change hours", "input": "number", "placeholder": "4"},
            {"key": "maintenance_window","label": "Maintenance window agreed", "input": "text"},
        ],
    },
    {
        "key": "exit", "title": "I. Exit terms",
        "items": [
            {"key": "notice_period",     "label": "Notice period (30 or 60 days)", "input": "text"},
            {"key": "repo_handoff",      "label": "Repo handoff included on notice"},
            {"key": "vercel_transfer",   "label": "Vercel project transfer included on notice"},
            {"key": "knowledge_handoff", "label": "30-day knowledge handoff offered"},
            {"key": "no_noncompete",     "label": "NO punitive non-compete language"},
        ],
    },
]


def list_agreement_keys() -> list[str]:
    """Flat list of every item key across all sections — used by the
    progress calculator and the form serializer."""
    keys = []
    for section in PILOT_AGREEMENT_SECTIONS:
        for item in section["items"]:
            keys.append(f"{section['key']}.{item['key']}")
    return keys


def agreement_progress(agreement_json: str | None) -> dict:
    """Returns {done, total, pct} from a pilot_agreement JSON blob.
    Counts a key as 'done' if its checkbox is true OR its inline input
    has a non-empty value."""
    import json as _json
    total = sum(len(s["items"]) for s in PILOT_AGREEMENT_SECTIONS)
    if not agreement_json:
        return {"done": 0, "total": total, "pct": 0}
    try:
        data = _json.loads(agreement_json)
    except Exception:
        return {"done": 0, "total": total, "pct": 0}
    done = 0
    for section in PILOT_AGREEMENT_SECTIONS:
        sd = data.get(section["key"], {}) or {}
        for item in section["items"]:
            v = sd.get(item["key"])
            if isinstance(v, bool):
                if v: done += 1
            elif isinstance(v, dict):
                if v.get("checked") or (v.get("value") not in (None, "")):
                    done += 1
            elif v not in (None, "", 0):
                done += 1
    return {"done": done, "total": total,
            "pct": round(100 * done / total) if total else 0}


def save_pilot_agreement(contact_id: int, agreement_json: str) -> bool:
    """Overwrite the contact's pilot_agreement column. Caller validates
    the JSON before passing it in."""
    conn = _get_conn()
    if not conn:
        return False
    try:
        cur = conn.cursor()
        cur.execute("""
            UPDATE crm_contacts
            SET pilot_agreement = %s, updated_at = NOW()
            WHERE id = %s
        """, (agreement_json, contact_id))
        conn.commit()
        cur.close()
        return True
    finally:
        conn.close()

STAGE_TO_NEXT_TRIGGER = {
    "QUEUED":         "INTRO",
    "CONTACTED":      "BUMP_NO_REPLY",
    "REPLIED":        "SCHEDULING",
    "DISCOVERY_CALL": "POST_CALL",
    "PILOT":          "CHECKIN",
    "VERCEL_PROJECT": "CHECKIN",
    "RECURRING":      "RENEWAL",
    "LOST":           None,
}

# Sender block used in {my_name} substitution. Multiline OK — Resend
# preserves newlines in the text body. Override per-user via env var
# SENDER_NAME (e.g. Jim's outgoing identity).
SENDER_NAME = os.environ.get(
    "SENDER_NAME",
    "Aaron Havel\nCEO, FocusedOps\nfocusedops.io"
)


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
                   industry, email_thread, role,
                   hosting_model, engagement_notes, pilot_agreement,
                   created_at, updated_at
            FROM crm_contacts
            ORDER BY updated_at DESC
        """)
        rows = cur.fetchall()
        cols = ["id", "name", "title", "agency", "email", "stage",
                "pilot_value", "recurring_value", "date_emailed",
                "next_date", "subject", "notes",
                "industry", "email_thread", "role",
                "hosting_model", "engagement_notes", "pilot_agreement",
                "created_at", "updated_at"]
        out = [dict(zip(cols, r)) for r in rows]
        cur.close()
        return out
    finally:
        conn.close()


def find_contact_by_email(email: str) -> dict | None:
    """Case-insensitive lookup by email. Returns minimal contact dict
    (id, name, stage, agency) or None. Used by the Add Contact form
    to warn before creating a duplicate."""
    if not email or not email.strip():
        return None
    conn = _get_conn()
    if not conn:
        return None
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT id, name, stage, agency
            FROM crm_contacts
            WHERE LOWER(email) = LOWER(%s)
            LIMIT 1
        """, (email.strip(),))
        row = cur.fetchone()
        cur.close()
        if not row:
            return None
        return {"id": row[0], "name": row[1], "stage": row[2], "agency": row[3]}
    finally:
        conn.close()


def add_contact(*, name: str, title: str | None, agency: str | None,
                email: str | None, stage: str, pilot_value: int,
                recurring_value: int, date_emailed: date | None,
                next_date: date | None, subject: str | None,
                notes: str | None, industry: str | None = None,
                role: str | None = None) -> int | None:
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
               date_emailed, next_date, subject, notes, industry, role)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            RETURNING id
        """, (name, title, agency, email, stage, pilot_value, recurring_value,
              date_emailed, next_date, subject, notes, industry, role))
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
                   notes: str | None = None,
                   email_thread: str | None = None,
                   role: str | None = None,
                   hosting_model: str | None = None,
                   engagement_notes: str | None = None) -> bool:
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
            ("email_thread", email_thread), ("role", role),
            ("hosting_model", hosting_model),
            ("engagement_notes", engagement_notes),
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


# ─── Prototype CRUD ─────────────────────────────────────────────────
def list_prototypes() -> list[dict]:
    """All prototypes joined with the contact name + agency. Ordered
    by status (live first), then most-recent update."""
    conn = _get_conn()
    if not conn:
        return []
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT p.id, p.contact_id, c.name AS contact_name, c.agency,
                   p.name, p.prototype_url, p.status, p.description,
                   p.feedback, p.notes, p.feedback_token,
                   p.created_at, p.updated_at
            FROM crm_prototypes p
            LEFT JOIN crm_contacts c ON c.id = p.contact_id
            ORDER BY
              CASE p.status
                WHEN 'LIVE' THEN 0
                WHEN 'BUILDING' THEN 1
                WHEN 'PAUSED' THEN 2
                WHEN 'CLOSED' THEN 3
                ELSE 4
              END,
              p.updated_at DESC
        """)
        rows = cur.fetchall()
        cols = ["id", "contact_id", "contact_name", "agency",
                "name", "prototype_url", "status", "description",
                "feedback", "notes", "feedback_token",
                "created_at", "updated_at"]
        out = [dict(zip(cols, r)) for r in rows]
        cur.close()
        return out
    finally:
        conn.close()


def find_prototype_by_token(token: str) -> dict | None:
    """Public lookup for the feedback page. Token is the auth."""
    if not token or not token.strip():
        return None
    conn = _get_conn()
    if not conn:
        return None
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT p.id, p.contact_id, c.name AS contact_name,
                   p.name, p.prototype_url, p.status, p.description
            FROM crm_prototypes p
            LEFT JOIN crm_contacts c ON c.id = p.contact_id
            WHERE p.feedback_token = %s
            LIMIT 1
        """, (token.strip(),))
        row = cur.fetchone()
        cur.close()
        if not row:
            return None
        cols = ["id", "contact_id", "contact_name",
                "name", "prototype_url", "status", "description"]
        return dict(zip(cols, row))
    finally:
        conn.close()


def add_prototype(*, contact_id: int | None, name: str,
                  prototype_url: str | None = None,
                  status: str = "BUILDING",
                  description: str | None = None) -> int | None:
    import secrets as _secrets
    if status not in PROTOTYPE_STATUSES:
        status = "BUILDING"
    token = _secrets.token_urlsafe(16)
    conn = _get_conn()
    if not conn:
        return None
    try:
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO crm_prototypes
              (contact_id, name, prototype_url, status, description, feedback_token)
            VALUES (%s, %s, %s, %s, %s, %s)
            RETURNING id
        """, (contact_id or None, name, prototype_url, status, description, token))
        new_id = cur.fetchone()[0]
        conn.commit()
        cur.close()
        return new_id
    finally:
        conn.close()


def ensure_feedback_tokens() -> int:
    """Backfill feedback tokens for any prototype that doesn't have
    one yet (i.e. created before the column existed). Idempotent —
    returns the count of rows updated."""
    import secrets as _secrets
    conn = _get_conn()
    if not conn:
        return 0
    try:
        cur = conn.cursor()
        cur.execute("SELECT id FROM crm_prototypes WHERE feedback_token IS NULL")
        ids = [r[0] for r in cur.fetchall()]
        for pid in ids:
            cur.execute("UPDATE crm_prototypes SET feedback_token = %s WHERE id = %s",
                        (_secrets.token_urlsafe(16), pid))
        conn.commit()
        cur.close()
        return len(ids)
    finally:
        conn.close()


def update_prototype(prototype_id: int, *,
                     name: str | None = None,
                     prototype_url: str | None = None,
                     status: str | None = None,
                     description: str | None = None,
                     notes: str | None = None,
                     append_feedback: str | None = None) -> bool:
    """Patch any subset of fields. append_feedback prepends a dated
    block to the feedback log instead of overwriting it."""
    if status is not None and status not in PROTOTYPE_STATUSES:
        return False
    conn = _get_conn()
    if not conn:
        return False
    try:
        cur = conn.cursor()
        sets = []
        params: list = []
        for col, val in [
            ("name", name), ("prototype_url", prototype_url),
            ("status", status), ("description", description),
            ("notes", notes),
        ]:
            if val is not None:
                sets.append(f"{col} = %s")
                params.append(val)
        if append_feedback is not None and append_feedback.strip():
            cur.execute("SELECT feedback FROM crm_prototypes WHERE id = %s",
                        (prototype_id,))
            row = cur.fetchone()
            prev = (row[0] if row else "") or ""
            stamp = datetime.now().strftime("%Y-%m-%d %H:%M")
            entry = f"--- {stamp} ---\n{append_feedback.strip()}"
            new_log = (entry + "\n\n" + prev).strip() if prev else entry
            sets.append("feedback = %s")
            params.append(new_log)
        if not sets:
            cur.close()
            return True
        sets.append("updated_at = NOW()")
        params.append(prototype_id)
        cur.execute(
            f"UPDATE crm_prototypes SET {', '.join(sets)} WHERE id = %s",
            tuple(params),
        )
        conn.commit()
        cur.close()
        return True
    finally:
        conn.close()


def delete_prototype(prototype_id: int) -> bool:
    conn = _get_conn()
    if not conn:
        return False
    try:
        cur = conn.cursor()
        cur.execute("DELETE FROM crm_prototypes WHERE id = %s", (prototype_id,))
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
            SELECT id, industry, role, trigger, subject, body,
                   created_at, updated_at
            FROM crm_email_templates
            ORDER BY industry, role, trigger
        """)
        rows = cur.fetchall()
        cols = ["id", "industry", "role", "trigger", "subject", "body",
                "created_at", "updated_at"]
        out = [dict(zip(cols, r)) for r in rows]
        cur.close()
        return out
    finally:
        conn.close()


def get_template(industry: str, trigger: str,
                 role: str | None = None) -> dict | None:
    """Most-specific-wins lookup with role fallback.
    1) (industry, role, trigger)
    2) (industry, '',   trigger) — any-role default
    Returns the first hit or None."""
    conn = _get_conn()
    if not conn:
        return None
    cols = ["id", "industry", "role", "trigger", "subject", "body"]
    try:
        cur = conn.cursor()
        if role:
            cur.execute("""
                SELECT id, industry, role, trigger, subject, body
                FROM crm_email_templates
                WHERE industry = %s AND role = %s AND trigger = %s
            """, (industry, role, trigger))
            row = cur.fetchone()
            if row:
                cur.close()
                return dict(zip(cols, row))
        cur.execute("""
            SELECT id, industry, role, trigger, subject, body
            FROM crm_email_templates
            WHERE industry = %s AND role = '' AND trigger = %s
        """, (industry, trigger))
        row = cur.fetchone()
        cur.close()
        if not row:
            return None
        return dict(zip(cols, row))
    finally:
        conn.close()


def upsert_template(*, industry: str, trigger: str,
                    subject: str, body: str,
                    role: str | None = None) -> bool:
    """Upsert a template. Empty/None role acts as the industry-wide
    default that's used when a contact's role doesn't match anything
    more specific."""
    if industry not in INDUSTRIES or trigger not in EMAIL_TRIGGERS:
        return False
    role = (role or "").strip()
    if role and role not in ROLES:
        return False
    conn = _get_conn()
    if not conn:
        return False
    try:
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO crm_email_templates
              (industry, role, trigger, subject, body)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (industry, role, trigger) DO UPDATE
              SET subject = EXCLUDED.subject,
                  body    = EXCLUDED.body,
                  updated_at = NOW()
        """, (industry, role, trigger, subject, body))
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
    """Substitute template placeholders.

    Always-available variables: {first_name}, {name}, {title},
    {agency}, {my_name}.

    Discovery-call variables (filled from the contact's most recent
    saved call when one exists, else empty strings):
      {call_summary}    — Stage-2 executive summary
      {pain_analysis}   — Stage-2 ranked pain analysis
      {mvp_scope}       — Stage-2 MVP scope markdown
      {win_condition}   — extraction.win_condition (their words)
      {root_problem}    — extraction.problems[is_root_problem].description
      {problem_quote}   — supporting_quote of the root problem
      {concrete_story}  — extraction.concrete_story
      {process_name}    — extraction.core_process.name
      {time_cost}       — first metric formatted as "value unit"

    Falls through to the raw string if a template references a
    placeholder we don't know about."""
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
        "root_problem":   "",
        "problem_quote":  "",
        "concrete_story": "",
        "process_name":   "",
        "time_cost":      "",
        "pain_analysis":  "",
        "mvp_scope":      "",
    }

    # Pull call data if the contact has had a discovery call.
    cid = contact.get("id")
    if cid:
        try:
            call = get_call_for_contact(cid)
            if call:
                variables["call_summary"]  = (call.get("exec_summary") or "").strip()
                variables["pain_analysis"] = (call.get("pain_analysis") or "").strip()
                variables["mvp_scope"]     = (call.get("mvp_scope") or "").strip()
                try:
                    import json as _json
                    ex = _json.loads(call.get("extraction_json") or "{}")
                    variables["win_condition"]  = (ex.get("win_condition") or "").strip()
                    variables["concrete_story"] = (ex.get("concrete_story") or "").strip()
                    cp = ex.get("core_process") or {}
                    variables["process_name"]   = (cp.get("name") or "").strip()
                    # Root problem + verbatim quote.
                    root = next((p for p in (ex.get("problems") or [])
                                 if p.get("is_root_problem")), None)
                    if root:
                        variables["root_problem"]  = (root.get("description") or "").strip()
                        variables["problem_quote"] = (root.get("supporting_quote") or "").strip()
                    # First metric with a value+unit gets used as the
                    # time/cost shorthand. Aaron can edit the email if
                    # he wants a different metric featured.
                    for m in (ex.get("metrics") or []):
                        val  = str(m.get("value") or "").strip()
                        unit = (m.get("unit") or "").strip()
                        if val and unit:
                            variables["time_cost"] = f"{val} {unit}"
                            break
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
    role = contact.get("role")
    template = None
    fallback_industry = False
    fallback_role = False

    if trigger and industry:
        template = get_template(industry, trigger, role)
        # If get_template fell back from role-specific to any-role
        # within the same industry, flag it so the UI can hint.
        if template and role and (template.get("role") or "") != role:
            fallback_role = True
    # Last-resort cross-industry fallback if the contact's industry
    # has nothing at all for this trigger.
    if trigger and not template:
        for ind in INDUSTRIES:
            t = get_template(ind, trigger, role)
            if t:
                template = t
                fallback_industry = True
                if role and (t.get("role") or "") != role:
                    fallback_role = True
                break

    if not template:
        return {
            "trigger":          trigger,
            "trigger_label":    EMAIL_TRIGGERS.get(trigger, "") if trigger else "",
            "industry":         industry,
            "role":             role,
            "has_template":     False,
            "fallback_industry": False,
            "fallback_role":    False,
            "subject":          "",
            "body":             "",
        }

    rendered = render_template(template, contact)
    return {
        "trigger":          trigger,
        "trigger_label":    EMAIL_TRIGGERS.get(trigger, ""),
        "industry":         template.get("industry"),
        "role":             template.get("role") or "",
        "has_template":     True,
        "fallback_industry": fallback_industry,
        "fallback_role":    fallback_role,
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
                  extraction_json: str = "", locked_scope: str = "",
                  success_criteria: str = "", email_thread: str = "",
                  iteration_feedback: str = "",
                  prototype_name: str = "",
                  prototype_url: str = "",
                  prototype_description: str = "",
                  github_repo: str = "",
                  framework: str = "") -> str:
    """Substitute named placeholders into a prompt template without
    choking on JSON braces. Plain string.replace so JSON braces in the
    values don't get reinterpreted."""
    return (template
            .replace("{transcript}", transcript or "")
            .replace("{extraction_json}", extraction_json or "")
            .replace("{locked_scope}", locked_scope or "")
            .replace("{success_criteria}", success_criteria or "")
            .replace("{email_thread}", email_thread or "")
            .replace("{iteration_feedback}", iteration_feedback or "")
            .replace("{prototype_name}", prototype_name or "(not set)")
            .replace("{prototype_url}", prototype_url or "(not deployed yet)")
            .replace("{prototype_description}", prototype_description or "(no description)")
            .replace("{github_repo}", github_repo or "(not linked)")
            .replace("{framework}", framework or "(auto-detect)"))


# ─── Working-session framework (PILOT-stage pressure-test) ──────────
# After the 20-min discovery call, the next step is the 30-min
# working session: pressure-test the proposed MVP scope, answer the
# open questions, get a pricing anchor, lock in the pilot proposal.
# Same paste-prompts pattern as DiscoveryCallFramework — different
# agenda, different prompts, different scorecard.

WORKING_AGENDA = [
    {
        "time": "0:00–2:00",
        "title": "Frame the session",
        "who":   "You",
        "script": (
            "Thanks again. The goal today is to pressure-test the picture I "
            "sent. By the end of 30 minutes I want us aligned on four things: "
            "the scope of a first slice, how we'd measure success, timing, "
            "and a number that feels reasonable to you. Mind if I record so "
            "I can focus on listening?"
        ),
    },
    {
        "time": "2:00–8:00",
        "title": "Validate the one-pager",
        "who":   "Them (you probe)",
        "script": (
            "• Walk me through the picture I sent — did I get the problem "
            "right?\n"
            "• What did I miss, or get wrong, or land too strongly on?\n"
            "• Of the steps in your current process, anything I described "
            "that's not how it actually works?"
        ),
    },
    {
        "time": "8:00–22:00",
        "title": "Answer the open questions",
        "who":   "Them",
        "script": (
            "Walk through each open question from the MVP scope until you "
            "have a concrete answer. Typical for a finance/ops tool:\n"
            "• Which system are you on, and what's the access surface "
            "(API, export, database read-only)?\n"
            "• What's the lightest input we could ask of your team to fill "
            "the visibility gap?\n"
            "• What dimensions do you need to track against (code, project, "
            "vendor, time period)?\n"
            "• What defines a 'this needs attention' flag worth surfacing?\n"
            "• What's the timing — is this before a specific event, or "
            "rolling?"
        ),
    },
    {
        "time": "22:00–26:00",
        "title": "Define success",
        "who":   "Them",
        "script": (
            "• If we ran this pilot for 4 weeks and it worked, what would "
            "you be able to do that you can't today?\n"
            "• What's the metric that tells you it's working — hours back, "
            "fewer surprises, a number you can show your boss?\n"
            "• What would have to be true for you to renew at the recurring "
            "tier?"
        ),
    },
    {
        "time": "26:00–28:00",
        "title": "Pricing reality",
        "who":   "Them",
        "script": (
            "• Given the scope we just talked about, what feels reasonable "
            "as a one-time pilot fee versus what feels crazy?\n"
            "• Who else has to sign off on a number that size? (If anyone "
            "new comes up, get their name + role.)"
        ),
    },
    {
        "time": "28:00–30:00",
        "title": "Confirm next step",
        "who":   "You",
        "script": (
            "• Recap: scope is [X], success looks like [Y], pilot fee in "
            "the [Z] range, [N] signs off.\n"
            "• I'll send a one-page proposal by [date]. If it looks right, "
            "we sign and kick off [date]. Fair?"
        ),
    },
]

# Stage-1 extraction. Schema tuned to what a working-session
# transcript should yield — confirmation of the picture, scope
# deltas, answered open questions, success criteria, pricing anchor,
# decision makers, timing, and risks.
WORKING_PROMPT_EXTRACT = '''You are analyzing a 30-minute pilot working-session transcript for a custom-software consulting engagement.

Your job: extract ONLY what was actually said in the transcript. Do not infer, do not guess. If something was implied but not stated, put it in `assumptions[]` with a confidence rating.

Return strict JSON matching this schema exactly:
{
  "picture_confirmed": true,
  "picture_corrections": ["things the prospect corrected from the recap I sent"],
  "scope_deltas": {
    "add_to_mvp": ["..."],
    "remove_from_mvp": ["..."],
    "explicitly_out": ["..."]
  },
  "open_question_answers": [
    {"question": "...", "answer": "...", "supporting_quote": "..."}
  ],
  "success_criteria": [
    {"metric": "...", "target": "...", "horizon": "...", "supporting_quote": "..."}
  ],
  "renewal_trigger": "what they said would have to be true for them to renew at recurring",
  "price_anchor": {
    "reasonable_fee": "the number or range they said feels reasonable",
    "crazy_fee": "the number that feels crazy (if mentioned)",
    "verbatim_quote": "the line that gave you the anchor"
  },
  "decision_makers": [
    {"name_or_role": "...", "decision_power": "high|medium|low", "newly_introduced": true}
  ],
  "timeline": {
    "kickoff_target": "...",
    "milestones": ["..."],
    "hard_deadlines": ["..."]
  },
  "open_risks": [
    {"risk": "...", "supporting_quote": "..."}
  ],
  "go_no_go_signal": "go | hold | no-go",
  "next_step_committed": "what was agreed to verbatim as the next step",
  "assumptions": [{"text": "...", "confidence": "low|medium|high"}]
}

Rules:
- Never fabricate names, numbers, or quotes.
- If a field has no evidence, return [] or "" or null as appropriate.
- Output the JSON object only, no preamble, no markdown fence.

TRANSCRIPT:
"""
{transcript}
"""'''

WORKING_PROMPT_LOCKED_SCOPE = '''Using ONLY the extraction object below, write a locked-in pilot scope statement in this Markdown structure:

**In scope (what we will build):**
- [items reflecting scope_deltas.add_to_mvp + the MVP items that survived picture_corrections; each cites the source field]

**Explicitly out of scope:**
- [items from scope_deltas.remove_from_mvp + scope_deltas.explicitly_out]

**Open questions that still need answers before kickoff:**
- [any open_question_answers where answer was vague or marked ASSUMPTION; cite]

Every line must trace to a field in the extraction. Anything not supported, label "ASSUMPTION — confirm".

EXTRACTION:
{extraction_json}'''

WORKING_PROMPT_CRITERIA = '''Using ONLY the extraction object below, produce the success criteria + pricing recommendation in this Markdown structure:

**Success criteria (locked):**
- For each item in `success_criteria`: metric, target, horizon, verbatim quote.

**Renewal trigger:** [from `renewal_trigger`]

**Pricing recommendation:**
- Anchor: [from `price_anchor` — both numbers if available, and the verbatim quote]
- My recommendation: [a single number or tight range that's at or below "reasonable", and a one-line rationale]
- Decision makers: [from `decision_makers`, flagging anyone marked `newly_introduced: true` as a risk]

**Timeline target:**
- Kickoff: [from `timeline.kickoff_target`]
- Hard deadlines: [from `timeline.hard_deadlines`]

Anything not in the extraction, label "ASSUMPTION — confirm".

EXTRACTION:
{extraction_json}'''

WORKING_PROMPT_PROTOTYPE = '''You are designing a working prototype the prospect can interact with to give us feedback BEFORE we build the full pilot.

The prototype will be deployed via Vercel. The user already pre-created the GitHub repo and Vercel project from the CRM's 🚀 button, then ran `git clone` locally and started a fresh Claude Code session inside the cloned folder. So the executing Claude Code session lands inside an empty repo that is already wired: the GitHub remote is configured, and every `git push` triggers a Vercel auto-deploy.

Use the discovery + working-session extractions, the locked scope, the success criteria, and any email correspondence the user pastes in. Output a build brief structured in TWO parts.

# PART A — Build brief (for the engineering team)

**Prototype name:** [short memorable name]

**One-line pitch:** [what the prototype lets the prospect do, in their domain language]

**Stack (Vercel-friendly — pick one):**
- **Recommended:** Next.js (App Router) + Tailwind + simple in-memory mock data — Vercel auto-detects and deploys with zero config
- Alternative: Vite + React + Tailwind
- Alternative: Plain HTML + CSS + JS (for very small UIs)
- Avoid: FastAPI / Django / Flask — these don't deploy cleanly as a standard Vercel project for a prototype

**Data model (cite source extraction fields):**
- [entity 1 — fields — source]
- [entity 2 — fields — source]

**Screens / interactions:**
For each screen, cite which problem or success criterion it addresses.
1. [screen — what the user does — which problem from extraction it solves]
2. ...

**Mocked integrations:**
- [integration — what's faked — how the prototype simulates it]

**Sample data to seed (must look real in the prospect's domain):**
- [3-5 example records using their vendor / code / project naming conventions if mentioned anywhere]

**Acceptance criteria (maps the prototype back to the prospect's stated success_criteria):**
- For each item in success_criteria → which prototype feature proves it.

**Open questions / risks for this prototype:**
- [things still ambiguous; cite which extraction or email_thread line raised them]

# PART B — Claude Code prompt (drop into a NEW Claude Code session)

A self-contained prompt that, when pasted into Claude Code, will build the prototype end-to-end. Format:

```
Build a working prototype for [prospect first-name's] team called [prototype_name].

CONTEXT FOR YOU (the Claude Code session):
You are inside an empty Git repository that is already wired up:
- The folder was just cloned from a private GitHub repo
- The GitHub repo is linked to a Vercel project
- Every `git push` to `main` auto-deploys to a live Vercel URL
- DO NOT try to set up a new git repo. DO NOT change the remote.
- DO NOT spend time configuring local dev servers beyond what's needed for build-time verification

Goal: [one-line pitch, plus 'so they can give us feedback on scope and pain points'].

Stack: [stack details — bias toward Next.js with App Router unless something specific argues otherwise]

Data model:
[the data model from Part A, concrete column types]

Build these screens:
[screen list with key features]

Mock these integrations:
[mocked integration list with shape of fake data — no real API calls]

Seed in-app data (or a JSON file in the repo) with this sample data:
[sample data list]

When the build is working:
1. Verify it locally one time with `npm run dev` (or equivalent) — quick smoke test
2. Stage all changes: `git add -A`
3. Commit with a clear message: `git commit -m "Prototype v1: <one-line summary of what's in it>"`
4. Push to deploy: `git push origin main` (use `--force` if Vercel's auto-init README is in the way of your first commit — that README is throwaway)
5. Tell me when the push succeeds. Vercel auto-deploys in ~30 seconds. The live URL is in the Vercel dashboard.

For every subsequent change I ask for: edit → `git add -A && git commit -m "<change>" && git push`. The client sees the change live ~30 sec later by refreshing the same URL.
```

RULES:
- Use the prospect's own terminology (their vendor names, code formats, scope words) wherever the extraction or email_thread provides them.
- Keep the prototype SCOPE MINIMAL — the goal is feedback, not a finished product.
- Mock anything that would require real API credentials.
- Every claim in Part A must cite the extraction, locked_scope, success_criteria, or email_thread field it derives from.
- Anything not supported by those inputs, label "ASSUMPTION — confirm".
- Part B is a literal copy-paste prompt — write it as if Claude Code will execute it verbatim.
- The Part B prompt MUST tell Claude Code to commit + push instead of running locally as a final deliverable — Vercel hosts the running version.

EXTRACTION (Stage 1):
{extraction_json}

LOCKED SCOPE (Stage 2):
{locked_scope}

SUCCESS CRITERIA + PRICING (Stage 3):
{success_criteria}

EMAIL CORRESPONDENCE (async info gathered between calls):
{email_thread}'''

# Step 6 — post-review iteration. Take free-form feedback from a
# client review of the deployed prototype, plus all the context we
# already have (prototype repo, locked scope, original success
# criteria), and output TWO ready-to-paste prompts:
#   • PART A — a Claude Code prompt that implements the changes
#   • PART B — a claude.ai design prompt for UX thinking
WORKING_PROMPT_ITERATION = '''You are a senior software engineer + product designer reviewing a client's post-review feedback on a working prototype. Turn their feedback into TWO ready-to-execute prompts:

  PART A — a Claude Code prompt the user will paste into a Claude Code session running inside the prototype's GitHub repo. It implements the requested changes end-to-end (data model, code, deploy).
  PART B — a claude.ai design prompt the user will paste into the claude.ai chat interface. It asks Claude to think through the UX BEFORE code: flows, screens, components. Output should be markdown reasoning, not code.

CONTEXT — THE PROTOTYPE
- Prototype name:    {prototype_name}
- Live URL:          {prototype_url}
- GitHub repo:       {github_repo}
- Framework:         {framework}
- What it does now:  {prototype_description}

CONTEXT — ORIGINAL SCOPE
- Locked scope (Stage 2): {locked_scope}
- Success criteria (Stage 3): {success_criteria}

CLIENT'S POST-REVIEW FEEDBACK (their words, do not edit):
"""
{iteration_feedback}
"""

# PART A — Claude Code prompt (paste into a Claude Code session inside the prototype repo)

```
You are inside the GitHub repository for {prototype_name}, deployed to {prototype_url} via Vercel. Every push to main auto-deploys.

A client review just happened. They want the following added or changed:

[bullet list of every requested feature or change, citing the client's verbatim phrase from their feedback where possible]

DATA MODEL CHANGES (be specific about types and relationships):
[for each new field / table / relationship — name, type, what it stores, where it lives in the schema, how it relates to existing data]

SCREEN / COMPONENT CHANGES:
[for each affected screen — what changes, what's added, what's removed; cite the client's words]

RULES:
- Minimal additive changes. Do not refactor existing working features.
- Use the client's terminology / vendor names / domain language wherever they spoke it.
- Mock new integrations rather than calling real APIs.
- When done:
    npm run dev (or equivalent) — quick smoke test
    git add -A
    git commit -m "Iteration: <one-line summary of what was added>"
    git push origin main
  Vercel auto-deploys. Tell me when the push succeeded.
```

# PART B — claude.ai design prompt (paste into claude.ai chat, NOT Claude Code)

```
You are a senior product designer. I'm iterating on a small consulting prototype called {prototype_name} (currently live at {prototype_url}). The original scope was:

  [one-paragraph summary of the locked scope, in the client's words]

After a review with the client, they want these additions:

  [bullet list of requested changes, in the client's verbatim language]

Think through the UX BEFORE I write any code. Don't generate code — give me design reasoning in markdown.

Specifically:

1. **Flow** — for each new feature, walk me through what the user does step-by-step. Where do they enter the system? What screen are they on when the change matters? Where do they go next?

2. **Screen-level changes** — for each screen of the prototype that's affected, describe what's added / removed / reorganized. Sketch the layout in words (top bar, left column, table, right rail, etc.).

3. **Component-level decisions** — name the specific UI patterns (data table vs. card grid vs. timeline; modal vs. side panel vs. inline edit). Explain why each pattern fits this client's mental model.

4. **Data-shape implications** — if a feature implies a new field, table, or relationship in the data model, call it out so the engineer's prompt can pick it up. You don't have to design the schema — just flag where data structure matters.

5. **Open questions to ask the client before building** — anything ambiguous in their feedback that you'd want clarified.

Use their domain language (vendor names, terminology) where they used it. Cite quotes from their feedback to ground each recommendation.
```

RULES FOR YOU (the meta-AI):
- Every claim in Part A or B must cite the client's feedback, the locked scope, the success criteria, or the prototype description. Don't invent.
- Anything not supported by the inputs, label `[ASSUMPTION — confirm with client]`.
- Part A is a literal copy-paste prompt for Claude Code. Write it as if Claude Code will execute it verbatim.
- Part B is a literal copy-paste prompt for claude.ai. Write it as if claude.ai will respond to it verbatim.
- Be CONCRETE about data structure decisions (column types, relationships). The whole reason for this meta-prompt is the client asked you to think about how to "structure the data for smooth operation."
- Use the client's actual words wherever you can.
'''

WORKING_PROMPT_PROPOSAL = '''Using ONLY the extraction object below, draft a one-page pilot proposal email — both subject and body — in this exact structure:

Subject: <<SUBJECT>>

Hi <<FIRST_NAME>>,

Recap of what we agreed on:

**What we're building:** [1-2 sentences pulled from the locked scope]

**Success criteria:** [2-3 bullet points from success_criteria]

**Investment:** <<FEE>> fixed pilot fee, <<DURATION>>.

**Timeline:** Kickoff <<KICKOFF_DATE>> → mid-pilot check-in <<MILESTONE_DATE>> → pilot wrap <<WRAP_DATE>>.

**Next step:** [what's needed to start — sign, send, intro to [decision_maker]]

[One-line closer that weaves the prospect's verbatim `next_step_committed` quote in, if available]

<<MY_NAME>>

Rules:
- Use the LITERAL placeholder tokens `<<SUBJECT>>`, `<<FIRST_NAME>>`, `<<FEE>>`, `<<DURATION>>`, `<<KICKOFF_DATE>>`, `<<MILESTONE_DATE>>`, `<<WRAP_DATE>>`, `<<MY_NAME>>` exactly as shown. The user's UI fills them in. Do NOT replace, paraphrase, or rewrite any of these tokens — leave them as `<<TOKEN_NAME>>` so the find-and-replace works.
- For everything ELSE (scope, success criteria, next step, closing line), cite each claim to the extraction.
- Anything not supported by the extraction, leave a `[ASSUMPTION — confirm]` placeholder so the user fixes it before sending.
- Do not invent numbers, dates, or names.
- Keep it under ~180 words.

EXTRACTION:
{extraction_json}'''


# Scorecard for the working session — what makes the prospect
# ready for a real proposal vs. needs another touch.
WORKING_SCORECARD_DIMENSIONS = [
    ("picture_confirmed",     10, "Picture confirmed / corrected"),
    ("questions_answered",    25, "Open questions answered concretely"),
    ("success_criteria",      15, "Success criteria defined + measurable"),
    ("price_anchor",          15, "Pricing anchor (reasonable vs crazy)"),
    ("decision_makers",       10, "Decision makers identified"),
    ("timeline_committed",    10, "Timeline + kickoff date committed"),
    ("risks_surfaced",        10, "≥2 open risks surfaced"),
    ("next_step_committed",    5, "Specific next step committed"),
]


def compute_working_scorecard(extraction_json: str) -> dict:
    """Heuristic 0-100 from a working-session extraction JSON.
    Maps total to band + suggested_action:
      ≥85 → SEND_PROPOSAL  (proposal-ready, ship the SOW)
      65-84 → ONE_MORE_TOUCH (one gap to close async first)
      <65 → DONT_PROPOSE_YET (rerun discovery, this isn't ripe)"""
    import json as _json
    try:
        ex = _json.loads(extraction_json) if extraction_json else {}
    except (ValueError, TypeError):
        ex = {}

    scores: dict[str, int] = {}

    # picture_confirmed — explicit True + supporting corrections gets
    # full credit; bare True gets 5; missing or False gets 0.
    pc = ex.get("picture_confirmed")
    if pc is True and ex.get("picture_corrections"):
        scores["picture_confirmed"] = 10
    elif pc is True:
        scores["picture_confirmed"] = 5
    else:
        scores["picture_confirmed"] = 0

    # questions_answered — count answers with a verbatim quote.
    answers = ex.get("open_question_answers") or []
    answered = sum(1 for a in answers
                   if (a.get("answer") or "").strip()
                   and (a.get("supporting_quote") or "").strip())
    # 5 concrete answers = full marks; scale linearly otherwise.
    scores["questions_answered"] = min(25, answered * 5)

    # success_criteria — at least one with metric + target.
    sc = ex.get("success_criteria") or []
    well_formed = sum(1 for s in sc
                      if (s.get("metric") or "").strip()
                      and (s.get("target") or "").strip())
    if well_formed >= 2:
        scores["success_criteria"] = 15
    elif well_formed == 1:
        scores["success_criteria"] = 10
    else:
        scores["success_criteria"] = 0

    # price_anchor — both numbers + quote = full, one = half.
    pa = ex.get("price_anchor") or {}
    has_reasonable = bool((pa.get("reasonable_fee") or "").strip())
    has_crazy      = bool((pa.get("crazy_fee") or "").strip())
    has_quote      = bool((pa.get("verbatim_quote") or "").strip())
    if has_reasonable and has_crazy and has_quote:
        scores["price_anchor"] = 15
    elif has_reasonable:
        scores["price_anchor"] = 8
    else:
        scores["price_anchor"] = 0

    # decision_makers — at least one with name_or_role.
    dms = ex.get("decision_makers") or []
    named = sum(1 for d in dms if (d.get("name_or_role") or "").strip())
    scores["decision_makers"] = 10 if named >= 1 else 0

    # timeline_committed — kickoff_target OR a hard deadline.
    tl = ex.get("timeline") or {}
    if (tl.get("kickoff_target") or "").strip() or (tl.get("hard_deadlines") or []):
        scores["timeline_committed"] = 10
    else:
        scores["timeline_committed"] = 0

    # risks_surfaced — 2+ items.
    risks = ex.get("open_risks") or []
    if len(risks) >= 2:
        scores["risks_surfaced"] = 10
    elif len(risks) == 1:
        scores["risks_surfaced"] = 5
    else:
        scores["risks_surfaced"] = 0

    # next_step_committed — non-empty.
    scores["next_step_committed"] = 5 if (ex.get("next_step_committed") or "").strip() else 0

    total = sum(scores.values())
    go = ex.get("go_no_go_signal") or ""
    if go == "no-go":
        band = "no-go — disqualify or rerun discovery"
        suggested_action = "DONT_PROPOSE_YET"
    elif total >= 85:
        band = "proposal-ready — send the SOW"
        suggested_action = "SEND_PROPOSAL"
    elif total >= 65:
        band = "usable — one gap to close async before proposal"
        suggested_action = "ONE_MORE_TOUCH"
    else:
        band = "underperformed — don't write a proposal yet"
        suggested_action = "DONT_PROPOSE_YET"

    return {
        "scores":            scores,
        "total":             total,
        "band":              band,
        "suggested_action":  suggested_action,
    }


# ─── Working-session CRUD ───────────────────────────────────────────
def get_session_for_contact(contact_id: int) -> dict | None:
    conn = _get_conn()
    if not conn:
        return None
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT id, contact_id, session_date, transcript, extraction_json,
                   locked_scope, success_criteria, proposal_draft,
                   prototype_brief,
                   iteration_feedback, iteration_code_prompt,
                   iteration_design_prompt,
                   scorecard_json, suggested_action,
                   created_at, updated_at
            FROM crm_working_sessions
            WHERE contact_id = %s
        """, (contact_id,))
        row = cur.fetchone()
        cur.close()
        if not row:
            return None
        cols = ["id", "contact_id", "session_date", "transcript",
                "extraction_json", "locked_scope", "success_criteria",
                "proposal_draft", "prototype_brief",
                "iteration_feedback", "iteration_code_prompt",
                "iteration_design_prompt",
                "scorecard_json", "suggested_action",
                "created_at", "updated_at"]
        return dict(zip(cols, row))
    finally:
        conn.close()


def upsert_session(*, contact_id: int, session_date: date | None,
                   transcript: str, extraction_json: str,
                   locked_scope: str, success_criteria: str,
                   proposal_draft: str,
                   prototype_brief: str = "",
                   iteration_feedback: str = "",
                   iteration_code_prompt: str = "",
                   iteration_design_prompt: str = "") -> dict | None:
    sc = compute_working_scorecard(extraction_json)
    import json as _json
    sc_json = _json.dumps(sc)
    conn = _get_conn()
    if not conn:
        return None
    try:
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO crm_working_sessions
              (contact_id, session_date, transcript, extraction_json,
               locked_scope, success_criteria, proposal_draft,
               prototype_brief,
               iteration_feedback, iteration_code_prompt,
               iteration_design_prompt,
               scorecard_json, suggested_action)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (contact_id) DO UPDATE
              SET session_date            = EXCLUDED.session_date,
                  transcript              = EXCLUDED.transcript,
                  extraction_json         = EXCLUDED.extraction_json,
                  locked_scope            = EXCLUDED.locked_scope,
                  success_criteria        = EXCLUDED.success_criteria,
                  proposal_draft          = EXCLUDED.proposal_draft,
                  prototype_brief         = EXCLUDED.prototype_brief,
                  iteration_feedback      = EXCLUDED.iteration_feedback,
                  iteration_code_prompt   = EXCLUDED.iteration_code_prompt,
                  iteration_design_prompt = EXCLUDED.iteration_design_prompt,
                  scorecard_json          = EXCLUDED.scorecard_json,
                  suggested_action        = EXCLUDED.suggested_action,
                  updated_at              = NOW()
        """, (contact_id, session_date, transcript, extraction_json,
              locked_scope, success_criteria, proposal_draft,
              prototype_brief,
              iteration_feedback, iteration_code_prompt,
              iteration_design_prompt,
              sc_json, sc["suggested_action"]))
        conn.commit()
        cur.close()
        return sc
    finally:
        conn.close()


def delete_session(contact_id: int) -> bool:
    conn = _get_conn()
    if not conn:
        return False
    try:
        cur = conn.cursor()
        cur.execute("DELETE FROM crm_working_sessions WHERE contact_id = %s",
                    (contact_id,))
        conn.commit()
        cur.close()
        return True
    finally:
        conn.close()


# ─── Anthropic API auto-processing ──────────────────────────────────
# When ANTHROPIC_API_KEY is set, the Process tabs in the discovery /
# working-session modals can run the full 4-step chain in one click
# instead of copy-paste through claude.ai. ~20-30 sec end-to-end.
# Falls back to the existing paste-prompts flow when the key isn't set.
ANTHROPIC_MODEL = "claude-sonnet-4-6"


def call_claude(prompt: str, *, max_tokens: int = 4096) -> str:
    """One-shot Claude API call. Returns the assistant's text reply.

    Uses plain urllib to avoid a new dependency. Raises RuntimeError
    when the key is missing or the API errors so the caller can
    surface a clean message to the UI."""
    import json as _json
    import os as _os
    import urllib.request as _urlreq
    import urllib.error as _urlerr
    api_key = _os.environ.get("ANTHROPIC_API_KEY", "").strip()
    if not api_key:
        raise RuntimeError(
            "ANTHROPIC_API_KEY not set — auto-processing disabled. "
            "Add it to Railway env vars to enable."
        )
    body = _json.dumps({
        "model": ANTHROPIC_MODEL,
        "max_tokens": max_tokens,
        "messages": [{"role": "user", "content": prompt}],
    }).encode("utf-8")
    req = _urlreq.Request(
        "https://api.anthropic.com/v1/messages",
        data=body,
        headers={
            "x-api-key": api_key,
            "anthropic-version": "2023-06-01",
            "content-type": "application/json",
        },
        method="POST",
    )
    try:
        with _urlreq.urlopen(req, timeout=180) as r:
            response = _json.loads(r.read())
    except _urlerr.HTTPError as e:
        detail = ""
        try:
            detail = e.read().decode("utf-8", errors="replace")[:300]
        except Exception:
            pass
        raise RuntimeError(f"Anthropic API HTTP {e.code}: {detail}")
    return "".join(b.get("text", "") for b in (response.get("content") or [])
                   if b.get("type") == "text").strip()


def _strip_code_fence(s: str) -> str:
    """Claude sometimes wraps JSON in ```json …``` despite being told
    not to. Strip a single leading + trailing fence so the JSON parse
    downstream doesn't choke."""
    s = (s or "").strip()
    if s.startswith("```"):
        lines = s.split("\n")
        if lines[0].startswith("```"):
            lines = lines[1:]
        if lines and lines[-1].strip() == "```":
            lines = lines[:-1]
        s = "\n".join(lines).strip()
    return s


def process_discovery_call_auto(contact_id: int, transcript: str,
                                call_date_) -> dict:
    """Full discovery-call chain via Claude API: extract JSON, then
    exec summary + pain analysis + MVP scope in parallel. Upserts to
    the DB. Returns everything for the UI."""
    from concurrent.futures import ThreadPoolExecutor

    extraction = _strip_code_fence(call_claude(
        render_prompt(DISCOVERY_PROMPT_EXTRACT, transcript=transcript),
        max_tokens=8192,
    ))
    prompts = [
        ("exec_summary",  render_prompt(DISCOVERY_PROMPT_EXEC_SUMMARY, extraction_json=extraction)),
        ("pain_analysis", render_prompt(DISCOVERY_PROMPT_PAIN,         extraction_json=extraction)),
        ("mvp_scope",     render_prompt(DISCOVERY_PROMPT_MVP,          extraction_json=extraction)),
    ]
    with ThreadPoolExecutor(max_workers=3) as ex:
        futures = {key: ex.submit(call_claude, p, max_tokens=2048)
                   for key, p in prompts}
        outputs = {k: f.result().strip() for k, f in futures.items()}

    sc = upsert_call(
        contact_id=contact_id,
        call_date=call_date_,
        transcript=transcript,
        extraction_json=extraction,
        exec_summary=outputs["exec_summary"],
        pain_analysis=outputs["pain_analysis"],
        mvp_scope=outputs["mvp_scope"],
    )
    return {
        "extraction_json": extraction,
        "exec_summary":    outputs["exec_summary"],
        "pain_analysis":   outputs["pain_analysis"],
        "mvp_scope":       outputs["mvp_scope"],
        "scorecard":       sc,
    }


def process_working_session_auto(contact_id: int, transcript: str,
                                 session_date_) -> dict:
    """Same shape as process_discovery_call_auto but for the
    working-session chain (locked scope + success criteria +
    proposal draft + prototype brief)."""
    from concurrent.futures import ThreadPoolExecutor

    extraction = _strip_code_fence(call_claude(
        render_prompt(WORKING_PROMPT_EXTRACT, transcript=transcript),
        max_tokens=8192,
    ))
    prompts = [
        ("locked_scope",     render_prompt(WORKING_PROMPT_LOCKED_SCOPE, extraction_json=extraction)),
        ("success_criteria", render_prompt(WORKING_PROMPT_CRITERIA,    extraction_json=extraction)),
        ("proposal_draft",   render_prompt(WORKING_PROMPT_PROPOSAL,    extraction_json=extraction)),
    ]
    with ThreadPoolExecutor(max_workers=3) as ex:
        futures = {key: ex.submit(call_claude, p, max_tokens=2048)
                   for key, p in prompts}
        outputs = {k: f.result().strip() for k, f in futures.items()}

    # Step 5 (sequential — needs outputs from Step 2). Pulls in the
    # contact's email_thread so async context lands in the brief too.
    email_thread = ""
    try:
        contact = next((c for c in list_contacts() if c["id"] == contact_id), None)
        if contact:
            email_thread = contact.get("email_thread") or ""
    except Exception:
        pass

    prototype_brief = call_claude(
        render_prompt(WORKING_PROMPT_PROTOTYPE,
                      extraction_json=extraction,
                      locked_scope=outputs["locked_scope"],
                      success_criteria=outputs["success_criteria"],
                      email_thread=email_thread),
        max_tokens=4096,
    ).strip()

    sc = upsert_session(
        contact_id=contact_id,
        session_date=session_date_,
        transcript=transcript,
        extraction_json=extraction,
        locked_scope=outputs["locked_scope"],
        success_criteria=outputs["success_criteria"],
        proposal_draft=outputs["proposal_draft"],
        prototype_brief=prototype_brief,
    )
    return {
        "extraction_json":  extraction,
        "locked_scope":     outputs["locked_scope"],
        "success_criteria": outputs["success_criteria"],
        "proposal_draft":   outputs["proposal_draft"],
        "prototype_brief":  prototype_brief,
        "scorecard":        sc,
    }


def _split_iteration_output(text: str) -> tuple[str, str]:
    """Split Claude's combined Part A / Part B output into the two
    prompts. Looks for the `# PART A` and `# PART B` markers; falls
    back to splitting on the first `# PART B` if `# PART A` isn't
    explicit. Returns (code_prompt, design_prompt) — each fenced
    code block extracted when present."""
    import re as _re
    if not text:
        return "", ""
    # Cut anything before "# PART A" if present, then split on "# PART B".
    body = text
    a_match = _re.search(r"#\s*PART A[^\n]*\n", body)
    if a_match:
        body = body[a_match.start():]
    b_match = _re.search(r"#\s*PART B[^\n]*\n", body)
    if not b_match:
        return body.strip(), ""
    part_a_raw = body[:b_match.start()].strip()
    part_b_raw = body[b_match.start():].strip()

    def _strip_header(s: str) -> str:
        s = _re.sub(r"^#\s*PART [AB][^\n]*\n", "", s)
        return s.strip()

    def _extract_fenced(s: str) -> str:
        # Prefer the FIRST ``` block — that's the literal copy-paste prompt.
        m = _re.search(r"```(?:[a-zA-Z0-9_-]*\n)?([\s\S]+?)```", s)
        if m:
            return m.group(1).strip()
        return s.strip()

    code_prompt   = _extract_fenced(_strip_header(part_a_raw))
    design_prompt = _extract_fenced(_strip_header(part_b_raw))
    return code_prompt, design_prompt


def process_iteration_auto(contact_id: int,
                           iteration_feedback: str) -> dict:
    """Step 6 — pull the contact's session context + their most
    recently updated prototype, send everything to Claude with the
    iteration meta-prompt, split the response into Part A (Claude
    Code prompt) and Part B (claude.ai design prompt), persist, and
    return both."""
    if not iteration_feedback.strip():
        raise RuntimeError("No feedback provided.")

    session = get_session_for_contact(contact_id) or {}
    locked_scope     = session.get("locked_scope") or ""
    success_criteria = session.get("success_criteria") or ""

    # Most recent prototype for this contact, for context.
    protos = [p for p in list_prototypes()
              if p.get("contact_id") == contact_id]
    proto = protos[0] if protos else {}

    prompt = render_prompt(
        WORKING_PROMPT_ITERATION,
        locked_scope=locked_scope,
        success_criteria=success_criteria,
        iteration_feedback=iteration_feedback,
        prototype_name=proto.get("name") or "",
        prototype_url=proto.get("prototype_url") or "",
        prototype_description=proto.get("description") or "",
        github_repo="",  # not stored on prototype yet
        framework="",
    )
    reply = call_claude(prompt, max_tokens=4096)
    code_prompt, design_prompt = _split_iteration_output(reply)

    # Persist alongside the other session artifacts.
    upsert_session(
        contact_id=contact_id,
        session_date=session.get("session_date"),
        transcript=session.get("transcript") or "",
        extraction_json=session.get("extraction_json") or "",
        locked_scope=locked_scope,
        success_criteria=success_criteria,
        proposal_draft=session.get("proposal_draft") or "",
        prototype_brief=session.get("prototype_brief") or "",
        iteration_feedback=iteration_feedback,
        iteration_code_prompt=code_prompt,
        iteration_design_prompt=design_prompt,
    )
    return {
        "iteration_feedback":      iteration_feedback,
        "iteration_code_prompt":   code_prompt,
        "iteration_design_prompt": design_prompt,
        "raw":                     reply,
    }


def anthropic_configured() -> bool:
    """Quick check used by the UI to show/hide the auto-process button."""
    import os as _os
    return bool(_os.environ.get("ANTHROPIC_API_KEY", "").strip())


# ─── Resend (transactional email) ───────────────────────────────────
# When RESEND_API_KEY is set, the 📧 Email modal can send directly
# via the Resend API instead of opening mailto:. After send, we
# append the email to the contact's email_thread and (optionally)
# bump QUEUED → CONTACTED.
def resend_configured() -> bool:
    import os as _os
    return bool(_os.environ.get("RESEND_API_KEY", "").strip())


def resend_from_address() -> str:
    import os as _os
    return _os.environ.get("RESEND_FROM_EMAIL", "aaron@focusedops.io").strip() \
        or "aaron@focusedops.io"


def send_via_resend(*, to_email: str, subject: str, body: str,
                    from_email: str | None = None,
                    reply_to: str | None = None,
                    scheduled_at: str | None = None,
                    attachments: list | None = None) -> dict:
    """POST to resend.com/api/v1/emails. Returns
    {ok: bool, id?: str, scheduled_at?: str, error?: str}. Does not
    raise — caller can surface the error to the UI.

    scheduled_at: ISO8601 UTC timestamp (e.g. "2026-06-15T17:00:00Z")
    or natural-language string Resend accepts (e.g. "in 1 hour")."""
    import json as _json
    import os as _os
    import urllib.request as _urlreq
    import urllib.error as _urlerr

    api_key = _os.environ.get("RESEND_API_KEY", "").strip()
    if not api_key:
        return {"ok": False, "error": "RESEND_API_KEY not set"}
    if not to_email:
        return {"ok": False, "error": "missing recipient email"}
    if not subject:
        return {"ok": False, "error": "missing subject"}
    from_email = (from_email or resend_from_address()).strip()

    payload = {
        "from":    from_email,
        "to":      [to_email],
        "subject": subject,
        "text":    body or "",
    }
    if reply_to:
        payload["reply_to"] = reply_to
    if scheduled_at:
        payload["scheduled_at"] = scheduled_at
    if attachments:
        payload["attachments"] = attachments

    req = _urlreq.Request(
        "https://api.resend.com/emails",
        data=_json.dumps(payload).encode("utf-8"),
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type":  "application/json",
            "User-Agent":    "market-pulse/1.0 (focusedops.io)",
            "Accept":        "application/json",
        },
        method="POST",
    )
    try:
        with _urlreq.urlopen(req, timeout=30) as r:
            data = _json.loads(r.read())
            return {
                "ok": True,
                "id": data.get("id", ""),
                "scheduled_at": scheduled_at or "",
            }
    except _urlerr.HTTPError as e:
        detail = ""
        try:
            detail = e.read().decode("utf-8", errors="replace")[:300]
        except Exception:
            pass
        return {"ok": False, "error": f"HTTP {e.code}: {detail}"}
    except Exception as e:
        return {"ok": False, "error": str(e)}


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
    dict(
        industry="Government / Municipal Finance",
        trigger="POST_CALL",
        subject="Recap + 30-min working session — {process_name}",
        body=("Hi {first_name},\n\n"
              "Thanks for the time today. Quick recap so we're aligned before "
              "the working session:\n\n"
              "The core problem in your words: \"{problem_quote}\"\n\n"
              "What it's costing you today: roughly {time_cost} of focused "
              "time each month.\n\n"
              "What a win looks like for you: {win_condition}\n\n"
              "Where I'd start: a thin visibility layer that pulls from your "
              "accounting system and a lightweight \"what's coming\" input "
              "from each project manager — surfaced as one budget-vs-actual "
              "view per project. No change to how the PMs or accounting work "
              "today. Goal: make the monthly meetings pointed instead of "
              "exploratory.\n\n"
              "Before I scope anything for real, I'd like to pressure-test "
              "it together. A 30-minute working session — we'd cover:\n"
              "• Which accounting software you're on and how I'd pull from it\n"
              "• What dimensions matter most to track (code, project, vendor)\n"
              "• What defines a \"budget bust\" worth flagging\n"
              "• Timing — is this for the next monthly meeting or a longer rollout?\n\n"
              "What works next week? Send me two or three windows and I'll "
              "lock one in.\n\n"
              "{my_name}"),
    ),
    # ── Construction (default + role-specific INTRO) ───────────────
    dict(
        industry="Construction",
        role="",
        trigger="INTRO",
        subject="Job-cost variance you only see at month-close",
        body=("Hi {first_name},\n\n"
              "Most construction finance teams I talk to have the same "
              "blind spot: by the time job-cost variance shows up in your "
              "monthly close, the overruns are already booked. Field PMs "
              "see it 6 weeks earlier in change-order intake — but their "
              "view rarely makes it back to finance in time to act.\n\n"
              "I build small, custom tools that close that loop. Not a "
              "platform — a thin layer over Sage 300 / Procore / whatever "
              "your PMs are actually using, surfaced as one variance-vs-"
              "budget view per active job.\n\n"
              "Worth 20 minutes to walk through how your team handles this "
              "today? No pitch — I just want to understand the workflow "
              "before suggesting anything.\n\n"
              "{my_name}"),
    ),
    dict(
        industry="Construction",
        role="CFO",
        trigger="INTRO",
        subject="Job-cost variance you only see at month-close",
        body=("Hi {first_name},\n\n"
              "Most construction CFOs I talk to have the same blind spot: "
              "by the time you see job-cost variance in your monthly close, "
              "the overruns are already booked. Field PMs see it 6 weeks "
              "earlier in change-order intake — but their view never makes "
              "it into the finance picture in time.\n\n"
              "I build small, custom tools that close that loop. Not a "
              "platform — a thin layer over Sage 300 / Procore / whatever "
              "your PMs are actually using, surfaced as one variance-vs-"
              "budget view per active job.\n\n"
              "Worth 20 minutes to walk through how your team handles this "
              "today? No pitch — I just want to understand the workflow "
              "before suggesting anything. If a few weeks of build effort "
              "could surface overruns 30+ days earlier, that's the "
              "conversation.\n\n"
              "{my_name}"),
    ),
    dict(
        industry="Construction",
        role="Project Manager",
        trigger="INTRO",
        subject="Change orders that disappear into email threads",
        body=("Hi {first_name},\n\n"
              "I keep hearing the same story from PMs in construction "
              "finance: a change request comes in via text or email, gets "
              "verbally approved, the work happens — and three weeks later "
              "nobody can find the paper trail when it's time to bill.\n\n"
              "I build small tools that capture change orders out of email "
              "threads, route them through whatever approval flow your firm "
              "uses, and surface the ones that have stalled. Not a new "
              "system — a layer that catches what's slipping today.\n\n"
              "I'd like 20 minutes to hear how your team handles change-"
              "order intake on a typical job. No pitch. If there's a fit, "
              "I can show you specifically what I'd build before you "
              "commit a dollar.\n\n"
              "{my_name}"),
    ),
    dict(
        industry="Construction",
        role="Operations",
        trigger="INTRO",
        subject="Schedule slips that don't surface until billing",
        body=("Hi {first_name},\n\n"
              "Most construction operations leads I talk to live with the "
              "same gap: schedule slippage is visible to the field weeks "
              "before it shows up as a billing problem. By the time finance "
              "flags an underbilled job, the recovery window is already "
              "gone.\n\n"
              "I build small custom tools — not platforms — that pull "
              "schedule and budget signals into one early-warning view per "
              "job. Layers onto Sage / Procore / whatever you already use. "
              "Goal: surface the ones at risk while there's still time to "
              "intervene.\n\n"
              "Worth 20 minutes to walk through how your team catches this "
              "today? No pitch — I'd rather understand the workflow first.\n\n"
              "{my_name}"),
    ),
    # ── Government / Municipal Finance — role-specific INTROs ──────
    dict(
        industry="Government / Municipal Finance",
        role="CFO",
        trigger="INTRO",
        subject="Contract spend visibility before the council meeting",
        body=("Hi {first_name},\n\n"
              "Most municipal CFOs I talk to track vendor contracts in "
              "three places at once: the GL, a Sharepoint folder of POs, "
              "and a spreadsheet someone updates by hand. By month-end "
              "nothing reconciles, and the $50k overrun gets caught a "
              "quarter too late — usually right before a council meeting.\n\n"
              "I build small custom tools — not platforms — that pull all "
              "three into one budget-vs-actual view per contract. The PMs "
              "and accounting team keep working how they work; you just "
              "get the picture you need before the next public session.\n\n"
              "Worth 20 minutes to walk through how your team handles this "
              "today? I'd rather understand your workflow than pitch you "
              "anything generic.\n\n"
              "{my_name}"),
    ),
    dict(
        industry="Government / Municipal Finance",
        role="Finance Director",
        trigger="INTRO",
        subject="Contract spend tracking outside the GL",
        body=("Hi {first_name},\n\n"
              "Most municipal finance directors I work with track vendor "
              "contracts in three places at once: the GL, a Sharepoint "
              "folder of POs, and a spreadsheet someone updates by hand. "
              "By month-end nothing reconciles, and a $50k overrun gets "
              "caught a quarter too late.\n\n"
              "I build small custom tools — not platforms — that pull all "
              "three into one budget-vs-actual view per contract. The PMs "
              "and accounting team keep working how they work; you just "
              "get the picture you need before the council meeting.\n\n"
              "Worth 20 minutes to walk through how your team handles this "
              "today? I'd rather understand your workflow than pitch you "
              "anything generic.\n\n"
              "{my_name}"),
    ),
    # ── Architecture & Engineering — Controller INTRO ──────────────
    dict(
        industry="Architecture & Engineering",
        role="Controller",
        trigger="INTRO",
        subject="Project margins you only see when it's too late",
        body=("Hi {first_name},\n\n"
              "Most A&E controllers I talk to live with the same gap: "
              "project margins look fine on the timesheet rollup until "
              "month-close, when utilization, write-downs, and out-of-scope "
              "hours all hit at once. By then the project's already "
              "underwater.\n\n"
              "I build small custom tools — not platforms — that pull "
              "utilization, fee burn, and scope-creep signals into one "
              "project-level margin view that updates weekly. Layers onto "
              "Deltek / BST / whatever your firm already runs.\n\n"
              "Worth 20 minutes to walk through how your team tracks "
              "project profitability today? No pitch — I just want to "
              "understand the workflow before suggesting anything.\n\n"
              "{my_name}"),
    ),
    dict(
        industry="Architecture & Engineering",
        role="",
        trigger="INTRO",
        subject="Project margins you only see when it's too late",
        body=("Hi {first_name},\n\n"
              "Most A&E firms I work with live with the same gap: project "
              "margins look fine on the timesheet rollup until month-close, "
              "when utilization, write-downs, and out-of-scope hours all "
              "hit at once. By then the project's already underwater.\n\n"
              "I build small custom tools — not platforms — that pull "
              "utilization, fee burn, and scope-creep signals into one "
              "project-level margin view that updates weekly. Layers onto "
              "Deltek / BST / whatever your firm already runs.\n\n"
              "Worth 20 minutes to walk through how your team tracks "
              "project profitability today?\n\n"
              "{my_name}"),
    ),
]


def maybe_seed_templates() -> int:
    """Idempotent at the (industry, role, trigger) level: inserts any
    seed template not already in the DB. Existing rows are left alone
    so user-edited copy doesn't get overwritten. Called once at app
    start, after init_db."""
    conn = _get_conn()
    if not conn:
        return 0
    existing: set[tuple[str, str, str]] = set()
    try:
        cur = conn.cursor()
        cur.execute("SELECT industry, role, trigger FROM crm_email_templates")
        existing = {(r[0], r[1] or "", r[2]) for r in cur.fetchall()}
        cur.close()
    finally:
        conn.close()

    inserted = 0
    for t in SEED_TEMPLATES:
        key = (t.get("industry"), t.get("role", "") or "", t.get("trigger"))
        if key in existing:
            continue
        try:
            if upsert_template(**t):
                inserted += 1
        except Exception as e:
            logger.warning("Seed template %s/%s/%s failed: %s",
                           t.get("industry"), t.get("role", ""),
                           t.get("trigger"), e)
    if inserted:
        logger.info("CRM seed: inserted %d new email templates", inserted)
    return inserted

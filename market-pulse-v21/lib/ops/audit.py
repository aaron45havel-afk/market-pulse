"""The append-only log, and the one write path into it.

CLAUDE.md: "every read of tenant PII, every write". Two things make that
real rather than aspirational, and neither is this module on its own:

  * mf_audit_log refuses UPDATE, DELETE and TRUNCATE at the database
    level (0001_foundation), so nothing downstream can tidy the log up.
  * lib/ops/repository.py calls record_read() for any entity marked
    pii=True in scope.ENTITIES. The decision to audit lives on the table
    definition, not at each call site, because a rule applied by hand at
    forty call sites is applied at thirty-eight.

This module's own job is narrow: build a well-formed row and insert it.
It deliberately does NOT commit. An audit row must land in the same
transaction as the thing it describes — a log entry that survives a
rolled-back write is a record of something that never happened, and one
that is lost when the write succeeds is worse.

Failure policy: a broken audit insert RAISES. That is the opposite of
lib/ops/bootstrap.py's fail-open rule and the difference is deliberate.
Boot-time migrations fail open because no data is at risk. An audit
write fails closed because the whole point is that the record exists —
if it cannot be written, the read or write it describes must not happen
either.
"""
from __future__ import annotations

import json
import logging

log = logging.getLogger("mf.audit")

# Actions are a closed vocabulary. A free-text action field turns into
# 'update', 'updated', 'UPDATE' and 'user_update' within a year, and then
# no query over the log is trustworthy.
ACTIONS = frozenset({
    "read_pii",       # a scoped read of a table marked pii=True
    "create",
    "update",
    "archive",
    "login",
    "login_failed",
    "logout",
    "mfa_enrolled",
    "mfa_failed",
    "privilege_change",
    "denied",         # an authorization refusal — the interesting ones
    "export",
    "job",
})


class AuditError(RuntimeError):
    pass


def _detail(value) -> str:
    """JSON for the detail column, never raising on an odd object.

    A detail payload that cannot be serialised must not be the reason an
    audit row fails to write. It records that it could not be encoded,
    which is at least true.
    """
    try:
        return json.dumps(value or {}, default=str, sort_keys=True)
    except Exception as e:      # pragma: no cover - defensive
        return json.dumps({"_unencodable": str(e)})


def record(conn, *, action: str, target_type: str, target_id=None,
           actor_user_id: int | None = None, actor_label: str = "",
           detail=None, ip: str | None = None,
           request_id: str | None = None) -> None:
    """Append one row. Does not commit — the caller's transaction owns it."""
    if action not in ACTIONS:
        raise AuditError(
            f"Unknown audit action {action!r}. The vocabulary is closed so "
            f"that a query over the log means something; add the action to "
            f"audit.ACTIONS deliberately.")
    if not target_type:
        raise AuditError("An audit row with no target_type cannot be "
                         "searched for later, which is the only reason it "
                         "exists.")
    cur = conn.cursor()
    try:
        cur.execute(
            "INSERT INTO mf_audit_log "
            "(actor_user_id, actor_label, action, target_type, target_id, "
            " detail, ip, request_id) "
            "VALUES (%s, %s, %s, %s, %s, %s::jsonb, %s, %s)",
            (actor_user_id, actor_label or "", action, target_type,
             None if target_id is None else str(target_id),
             _detail(detail), ip, request_id))
    finally:
        cur.close()


def record_read(conn, scope, entity: str, ids=None, count: int | None = None,
                request_id: str | None = None) -> None:
    """A read of PII. Records WHAT was seen, not just that something was.

    `ids` matters. "someone read tenant records" answers no question worth
    asking; "user 4 read tenant rows 11, 12 and 19 at 14:02" answers the
    one that comes up in a dispute or a subject access request. Capped,
    because a report over ten thousand rows should not write a ten
    thousand element array into the log — the count still tells the
    truth about the size.
    """
    ids = list(ids or [])
    detail = {"portal": scope.portal, "roles": sorted(scope.roles),
              "count": count if count is not None else len(ids)}
    if ids:
        detail["ids"] = [str(i) for i in ids[:100]]
        if len(ids) > 100:
            detail["ids_truncated"] = len(ids) - 100
    record(conn, action="read_pii", target_type=entity,
           target_id=str(ids[0]) if len(ids) == 1 else None,
           actor_user_id=scope.user_id, detail=detail, request_id=request_id)


def record_denied(conn, scope, entity: str, action: str, reason: str,
                  request_id: str | None = None) -> None:
    """A refusal. These are the rows worth alerting on.

    A successful read by someone entitled to it is routine. A tenant
    session asking for the staff user list is either a bug or an attempt,
    and both are things you want to find out about from the log rather
    than from the outcome.
    """
    record(conn, action="denied", target_type=entity,
           actor_user_id=scope.user_id,
           detail={"portal": scope.portal, "roles": sorted(scope.roles),
                   "attempted": action, "reason": reason},
           request_id=request_id)

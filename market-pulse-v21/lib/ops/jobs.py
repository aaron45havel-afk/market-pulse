"""A Postgres-backed work queue. No Redis, and a dead-letter that is real.

ARCHITECTURE.md §4: "a mf_jobs table with SELECT … FOR UPDATE SKIP
LOCKED rather than Redis. One fewer service to run, transactional with
the writes that enqueue, and adequate to several orders of magnitude
beyond this portfolio's volume."

That middle clause is the one that matters. A rent charge and the job
that emails the receipt are written in ONE transaction, so there is no
state where the charge exists and the notification was lost, and none
where the email goes out for a charge that rolled back. A separate queue
service cannot offer that at any price.

The three things a queue gets wrong:

  * TWO WORKERS TAKING THE SAME JOB. SKIP LOCKED is the whole answer —
    a claim takes a row lock and any concurrent claimer walks past it
    rather than blocking. Tested with two real connections, because
    concurrency asserted in a comment is concurrency untested.
  * A FAILING JOB RETRYING FOREVER. Attempts are counted and a job that
    exhausts them goes to 'dead' and STAYS there. A dead-letter that
    silently retries is a log file.
  * A JOB LOST BECAUSE A WORKER DIED. A claim stamps locked_at; anything
    still 'running' past a deadline is reclaimed. Without that, one
    container restart at the wrong moment loses work with no error
    anywhere.

This module talks to mf_jobs directly rather than through the
repository. A worker is not a user: it has no session, no portal and no
Scope, so there is nothing for the scoping layer to filter by, and
FOR UPDATE SKIP LOCKED is deliberately not in the repository's
vocabulary. tests/test_ops_schema.py allows this file mf_jobs and
nothing else.
"""
from __future__ import annotations

import json
import logging
import os
import socket
import time
from datetime import timedelta

from lib.ops import audit as A
from lib.ops import clock as C

log = logging.getLogger("mf.jobs")

MAX_ATTEMPTS = 5
# Exponential, capped. The cap matters: doubling without one reaches
# multi-day delays by attempt ten, and a job nobody sees fail for two
# days is a job nobody knew about.
BACKOFF = [timedelta(seconds=s) for s in (10, 60, 300, 1800, 3600)]
STALL_AFTER = timedelta(minutes=15)
CLAIM_BATCH = 1


class JobError(RuntimeError):
    pass


def worker_name() -> str:
    return f"{socket.gethostname()}:{os.getpid()}"


def enqueue(conn, kind: str, payload=None, ts: C.TimeService | None = None,
            run_after=None, max_attempts: int = MAX_ATTEMPTS,
            idempotency_key: str | None = None) -> int | None:
    """Add work. Returns the job id, or None if the key already exists.

    None is not an error. An idempotency key means "at most once", and a
    duplicate payment webhook arriving twice SHOULD produce one job and
    one charge — the second enqueue doing nothing is the feature.

    Does not commit: the caller's transaction owns both this and the
    write it accompanies.
    """
    if not kind:
        raise JobError("a job needs a kind")
    ts = ts or C.TimeService()
    when = run_after or ts.now()
    cur = conn.cursor()
    try:
        cur.execute(
            "INSERT INTO mf_jobs (kind, payload, run_after, max_attempts, "
            "idempotency_key) VALUES (%s, %s::jsonb, %s, %s, %s) "
            "ON CONFLICT (idempotency_key) DO NOTHING RETURNING id",
            (kind, json.dumps(payload or {}, default=str), when,
             int(max_attempts), idempotency_key))
        row = cur.fetchone()
        return row[0] if row else None
    finally:
        cur.close()


def claim(conn, worker: str | None = None, kinds=None,
          ts: C.TimeService | None = None):
    """Take one runnable job, or None. Commits — a claim must be visible.

    THE LOCK IS THE POINT. `FOR UPDATE SKIP LOCKED` makes a concurrent
    claimer step over this row instead of queueing behind it, which is
    what lets two workers drain one queue without either duplicating the
    other's work or serialising against it.

    This is the one function here that commits, and it has to: a claim
    held inside an uncommitted transaction is invisible to every other
    worker, so they would all claim the same job and discover the clash
    only at completion.
    """
    ts = ts or C.TimeService()
    now = ts.now()
    worker = worker or worker_name()
    cur = conn.cursor()
    try:
        cur.execute(
            "WITH taken AS ("
            "  SELECT id FROM mf_jobs"
            "   WHERE status = 'queued' AND run_after <= %s"
            "     AND (%s::text[] IS NULL OR kind = ANY(%s::text[]))"
            "   ORDER BY run_after, id"
            "   FOR UPDATE SKIP LOCKED"
            "   LIMIT %s"
            ") "
            "UPDATE mf_jobs j SET status = 'running', locked_at = %s, "
            "       locked_by = %s, attempts = j.attempts + 1 "
            "  FROM taken WHERE j.id = taken.id "
            "RETURNING j.id, j.kind, j.payload, j.attempts, j.max_attempts",
            (now, list(kinds) if kinds else None,
             list(kinds) if kinds else None, CLAIM_BATCH, now, worker))
        row = cur.fetchone()
        conn.commit()
        if not row:
            return None
        return {"id": row[0], "kind": row[1], "payload": row[2],
                "attempts": row[3], "max_attempts": row[4]}
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()


def complete(conn, job_id: int, ts: C.TimeService | None = None) -> bool:
    ts = ts or C.TimeService()
    cur = conn.cursor()
    cur.execute("UPDATE mf_jobs SET status = 'done', completed_at = %s, "
                "locked_at = NULL, locked_by = NULL "
                "WHERE id = %s AND status = 'running'", (ts.now(), job_id))
    touched = cur.rowcount
    cur.close()
    return bool(touched)


def fail(conn, job_id: int, error: str, ts: C.TimeService | None = None,
         retry: bool = True) -> str:
    """Record a failure. Returns the new status: 'queued' or 'dead'.

    A job that has used its attempts goes to 'dead' and is not picked up
    again — claim() only looks at 'queued'. Somebody has to look at the
    dead letter; that is a monitoring problem, and it is a better one
    than a job retrying every ten seconds for a week.
    """
    ts = ts or C.TimeService()
    now = ts.now()
    cur = conn.cursor()
    cur.execute("SELECT attempts, max_attempts FROM mf_jobs WHERE id = %s",
                (job_id,))
    row = cur.fetchone()
    if not row:
        cur.close()
        raise JobError(f"no such job {job_id}")
    attempts, max_attempts = row

    if not retry or attempts >= max_attempts:
        cur.execute("UPDATE mf_jobs SET status = 'dead', last_error = %s, "
                    "locked_at = NULL, locked_by = NULL, completed_at = %s "
                    "WHERE id = %s", (str(error)[:2000], now, job_id))
        cur.close()
        log.error("job %s dead after %s attempts: %s", job_id, attempts,
                  error)
        A.record(conn, action="job", target_type="mf_jobs", target_id=job_id,
                 actor_label="worker",
                 detail={"status": "dead", "attempts": attempts,
                         "error": str(error)[:500]})
        return "dead"

    delay = BACKOFF[min(attempts - 1, len(BACKOFF) - 1)]
    cur.execute("UPDATE mf_jobs SET status = 'queued', last_error = %s, "
                "run_after = %s, locked_at = NULL, locked_by = NULL "
                "WHERE id = %s", (str(error)[:2000], now + delay, job_id))
    cur.close()
    log.warning("job %s failed (attempt %s/%s), retrying in %s: %s", job_id,
                attempts, max_attempts, delay, error)
    return "queued"


def reap_stalled(conn, ts: C.TimeService | None = None,
                 older_than: timedelta = STALL_AFTER) -> int:
    """Return jobs whose worker vanished to the queue. Returns how many.

    A container restart mid-job leaves a row 'running' with nobody
    running it, and without this it stays that way forever — the single
    most common way a Postgres queue quietly stops doing some of its
    work. The reaped job keeps its attempt count, so a job that stalls
    repeatedly still reaches the dead letter rather than looping.
    """
    ts = ts or C.TimeService()
    cutoff = ts.now() - older_than
    cur = conn.cursor()
    cur.execute(
        "UPDATE mf_jobs SET status = 'queued', locked_at = NULL, "
        "locked_by = NULL, last_error = COALESCE(last_error, '') || "
        "' [reclaimed: worker stopped responding]' "
        "WHERE status = 'running' AND locked_at < %s "
        "  AND attempts < max_attempts", (cutoff,))
    n = cur.rowcount
    # A stalled job that has ALSO exhausted its attempts is dead, not
    # requeued — otherwise the reaper becomes a way around max_attempts.
    cur.execute(
        "UPDATE mf_jobs SET status = 'dead', locked_at = NULL, "
        "locked_by = NULL, last_error = COALESCE(last_error, '') || "
        "' [stalled with no attempts left]' "
        "WHERE status = 'running' AND locked_at < %s "
        "  AND attempts >= max_attempts", (cutoff,))
    dead = cur.rowcount
    cur.close()
    if n or dead:
        log.warning("reaped %s stalled jobs (%s to the dead letter)", n + dead,
                    dead)
    return n + dead


def dead_letter(conn, limit: int = 100) -> list[dict]:
    cur = conn.cursor()
    cur.execute("SELECT id, kind, payload, attempts, last_error, completed_at "
                "FROM mf_jobs WHERE status = 'dead' "
                "ORDER BY completed_at DESC NULLS LAST, id DESC LIMIT %s",
                (limit,))
    out = [{"id": r[0], "kind": r[1], "payload": r[2], "attempts": r[3],
            "last_error": r[4], "died_at": r[5]} for r in cur.fetchall()]
    cur.close()
    return out


def revive(conn, job_id: int, ts: C.TimeService | None = None) -> bool:
    """Put a dead job back, with a fresh attempt budget. Deliberate only.

    Used after the cause is fixed. Resetting attempts is the whole point:
    a revived job that immediately re-exhausts a spent counter would die
    on its first try and look like the fix did not work.
    """
    ts = ts or C.TimeService()
    cur = conn.cursor()
    cur.execute("UPDATE mf_jobs SET status = 'queued', attempts = 0, "
                "run_after = %s, completed_at = NULL WHERE id = %s "
                "AND status = 'dead'", (ts.now(), job_id))
    touched = cur.rowcount
    cur.close()
    return bool(touched)


def stats(conn) -> dict:
    cur = conn.cursor()
    cur.execute("SELECT status, COUNT(*) FROM mf_jobs GROUP BY status")
    out = {s: 0 for s in ("queued", "running", "done", "failed", "dead")}
    out.update({r[0]: r[1] for r in cur.fetchall()})
    cur.close()
    return out


def run_one(conn, handlers: dict, worker: str | None = None,
            kinds=None, ts: C.TimeService | None = None) -> dict | None:
    """Claim and run a single job. Returns what happened, or None if idle.

    A handler raising is a FAILED JOB, not a crashed worker. The whole
    reason for a queue is that the thing being retried is allowed to go
    wrong; a worker that dies on the first exception turns a retryable
    failure into an outage.
    """
    ts = ts or C.TimeService()
    job = claim(conn, worker=worker, kinds=kinds, ts=ts)
    if not job:
        return None

    handler = handlers.get(job["kind"])
    if handler is None:
        # Unknown kind. Dead immediately rather than retried: no amount
        # of waiting will make a handler appear, and five attempts at it
        # only delays somebody noticing.
        fail(conn, job["id"], f"no handler registered for {job['kind']!r}",
             ts=ts, retry=False)
        conn.commit()
        return {**job, "outcome": "dead", "error": "no handler"}

    try:
        handler(conn, job["payload"])
    except Exception as e:
        conn.rollback()
        status = fail(conn, job["id"], f"{type(e).__name__}: {e}", ts=ts)
        conn.commit()
        return {**job, "outcome": status, "error": str(e)}

    complete(conn, job["id"], ts=ts)
    conn.commit()
    return {**job, "outcome": "done"}


def run_forever(get_conn, handlers: dict, kinds=None, idle_sleep: float = 2.0,
                reap_every: float = 60.0, stop=None) -> None:      # pragma: no cover
    """The worker entrypoint. Runs as a second Railway process.

    Not exercised by the test suite — a loop that never returns cannot
    be. run_one() holds all the logic and IS tested; this is the thin
    shell around it, kept thin for exactly that reason.
    """
    ts = C.TimeService()
    worker = worker_name()
    last_reap = 0.0
    log.info("job worker %s starting (kinds=%s)", worker, kinds or "all")
    while not (stop and stop()):
        conn = None
        try:
            conn = get_conn()
            if conn is None:
                time.sleep(idle_sleep)
                continue
            if time.monotonic() - last_reap > reap_every:
                reap_stalled(conn, ts=ts)
                conn.commit()
                last_reap = time.monotonic()
            result = run_one(conn, handlers, worker=worker, kinds=kinds, ts=ts)
            if result is None:
                time.sleep(idle_sleep)
        except Exception as e:
            log.error("worker loop error: %s", e)
            time.sleep(idle_sleep)
        finally:
            if conn:
                try:
                    conn.close()
                except Exception:
                    pass

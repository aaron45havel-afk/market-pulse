"""The work queue — including the concurrency, with two real connections.

Run:  python tests/test_ops_jobs.py
SKIPS (exit 0) without DATABASE_URL.

    DATABASE_URL="postgresql://postgres@/mfops?host=/var/tmp&port=55433" \
        python tests/test_ops_jobs.py

The claim path is the only interesting part of a queue and it is the
part that cannot be tested on one connection: `FOR UPDATE SKIP LOCKED`
does nothing observable until a second session is trying to take the
same row. So this opens two, holds a claim open in one, and checks the
other steps over it — which is the actual promise, rather than the
comment above the SQL.

Everything else here is about failure. A queue that runs the happy path
is a function call with extra latency; what earns the table is what
happens when a handler raises, when a worker dies mid-job, and when a
job is simply never going to succeed.
"""
import os
import sys
from datetime import datetime, timedelta, timezone

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

if not os.environ.get("DATABASE_URL"):
    print("SKIP — no DATABASE_URL. See this file's docstring to run it.")
    sys.exit(0)

import psycopg2

from lib.ops import clock as C
from lib.ops import jobs as J
from lib.ops.migrations import runner as R

_COUNT = 0
_FAILS = []


def check(cond, msg):
    global _COUNT
    _COUNT += 1
    if not cond:
        _FAILS.append(msg)


def raises(exc, fn, *a, **k):
    try:
        fn(*a, **k)
        return False
    except exc:
        return True


URL = os.environ["DATABASE_URL"]
conn = psycopg2.connect(URL)

for t in ["mf_sessions", "mf_jobs", "mf_documents", "mf_audit_log",
          "mf_jurisdiction_rules", "mf_jurisdictions", "mf_user_roles",
          "mf_roles", "mf_users", "mf_divisions", "mf_organizations",
          "mf_migrations"]:
    cur = conn.cursor()
    cur.execute(f"DROP TABLE IF EXISTS {t} CASCADE")
    conn.commit()
    cur.close()
for fn in ("mf_audit_log_immutable()", "mf_user_roles_scope_check()"):
    cur = conn.cursor()
    cur.execute(f"DROP FUNCTION IF EXISTS {fn} CASCADE")
    conn.commit()
    cur.close()
R.migrate(conn, actor="test_ops_jobs")

NOW = datetime(2026, 8, 27, 12, 0, tzinfo=timezone.utc)
TS = C.TimeService(NOW)


def sql(q, args=None, c=None):
    cur = (c or conn).cursor()
    cur.execute(q, args)
    rows = cur.fetchall() if cur.description else []
    cur.close()
    return rows


def wipe():
    cur = conn.cursor()
    cur.execute("DELETE FROM mf_jobs")
    conn.commit()
    cur.close()


def status_of(job_id):
    r = sql("SELECT status FROM mf_jobs WHERE id = %s", (job_id,))
    return r[0][0] if r else None


# ── enqueue ──
jid = J.enqueue(conn, "send_receipt", {"charge_id": 42}, ts=TS)
conn.commit()
check(jid is not None, "a job enqueues")
check(status_of(jid) == "queued", "and starts queued")
check(sql("SELECT payload FROM mf_jobs WHERE id = %s", (jid,))[0][0]
      == {"charge_id": 42}, "with its payload intact as jsonb")
check(raises(J.JobError, J.enqueue, conn, "", {}),
      "a job with no kind is refused — an unroutable job is a row that "
      "sits queued forever")
conn.commit()

dup1 = J.enqueue(conn, "charge", {"amount_cents": 150000}, ts=TS,
                 idempotency_key="webhook-abc123")
conn.commit()
dup2 = J.enqueue(conn, "charge", {"amount_cents": 150000}, ts=TS,
                 idempotency_key="webhook-abc123")
conn.commit()
check(dup1 is not None and dup2 is None,
      "AN IDEMPOTENCY KEY MAKES THE SECOND ENQUEUE A NO-OP, returning "
      "None rather than raising. A duplicate payment webhook should "
      "produce one job and one charge, and the second call doing nothing "
      "quietly is the feature")
check(sql("SELECT COUNT(*) FROM mf_jobs WHERE kind = 'charge'")[0][0] == 1,
      "and exactly one row exists")

later = J.enqueue(conn, "reminder", {}, ts=TS,
                  run_after=NOW + timedelta(hours=1))
conn.commit()


# ── claim ──
wipe()
a = J.enqueue(conn, "task", {"n": 1}, ts=TS)
b = J.enqueue(conn, "task", {"n": 2}, ts=TS)
future = J.enqueue(conn, "task", {"n": 3}, ts=TS,
                   run_after=NOW + timedelta(hours=2))
conn.commit()

got = J.claim(conn, worker="w1", ts=TS)
check(got and got["id"] == a,
      f"claim takes the oldest runnable job first (got {got})")
check(status_of(a) == "running", "which becomes running")
check(sql("SELECT locked_by, attempts FROM mf_jobs WHERE id = %s",
          (a,))[0] == ("w1", 1),
      "stamped with the worker and its attempt count")

got2 = J.claim(conn, worker="w1", ts=TS)
check(got2 and got2["id"] == b, "the next claim takes the next job")

got3 = J.claim(conn, worker="w1", ts=TS)
check(got3 is None,
      "AND THE THIRD RETURNS NONE RATHER THAN THE FUTURE-DATED JOB. "
      "run_after is a promise not to run before a time, and a queue that "
      "ignores it makes every scheduled job immediate")

_later = C.TimeService(NOW + timedelta(hours=3))
got4 = J.claim(conn, worker="w1", ts=_later)
check(got4 and got4["id"] == future,
      "which becomes claimable once its time arrives")

wipe()
k1 = J.enqueue(conn, "email", {}, ts=TS)
k2 = J.enqueue(conn, "sms", {}, ts=TS)
conn.commit()
only_sms = J.claim(conn, worker="w1", kinds=["sms"], ts=TS)
check(only_sms and only_sms["id"] == k2,
      "a worker can restrict itself to certain kinds, so a slow job type "
      "cannot starve a fast one")
check(status_of(k1) == "queued", "leaving the other kind untouched")


# ══════════════════════════════════════════════════════════════════
# THE ONE THAT NEEDS TWO CONNECTIONS
# ══════════════════════════════════════════════════════════════════
wipe()
j1 = J.enqueue(conn, "task", {"n": 1}, ts=TS)
j2 = J.enqueue(conn, "task", {"n": 2}, ts=TS)
conn.commit()

other = psycopg2.connect(URL)
claim_a = J.claim(conn, worker="worker-A", ts=TS)
claim_b = J.claim(other, worker="worker-B", ts=TS)
check(claim_a and claim_b and claim_a["id"] != claim_b["id"],
      f"two workers on two connections get different jobs "
      f"(A={claim_a}, B={claim_b})")
check({claim_a["id"], claim_b["id"]} == {j1, j2},
      "between them they took both, so neither duplicated the other")
check(J.claim(other, worker="worker-B", ts=TS) is None,
      "and a third claim finds nothing left rather than re-taking one "
      "already running")

# THOSE THREE CHECKS DO NOT PROVE SKIP LOCKED, and it took a mutation to
# notice. claim() commits before returning, so by the time B looks, A's
# row is already status='running' and B's WHERE clause excludes it. The
# STATUS FILTER is doing all the work; deleting SKIP LOCKED from the
# query leaves every check above passing.
#
# The lock only matters while a claim is still open, which claim() never
# leaves. So hold one open by hand — the situation two workers hitting
# the same row at the same instant are actually in — and give the second
# connection a statement timeout, because the failure mode without SKIP
# LOCKED is not a wrong answer but an indefinite block.
wipe()
lock1 = J.enqueue(conn, "task", {"n": 1}, ts=TS)
lock2 = J.enqueue(conn, "task", {"n": 2}, ts=TS)
conn.commit()

holder = psycopg2.connect(URL)
hcur = holder.cursor()
hcur.execute("SELECT id FROM mf_jobs WHERE status = 'queued' "
             "ORDER BY run_after, id LIMIT 1 FOR UPDATE")
locked_id = hcur.fetchone()[0]          # transaction left OPEN, lock held
check(locked_id == lock1, "the holder has a row lock on the first job")

tcur = other.cursor()
tcur.execute("SET statement_timeout = '4s'")
other.commit()
tcur.close()

try:
    skipped = J.claim(other, worker="worker-B", ts=TS)
    timed_out = False
except psycopg2.errors.QueryCanceled:
    other.rollback()
    skipped, timed_out = None, True

check(not timed_out,
      "A CLAIM AGAINST A ROW ANOTHER SESSION HAS LOCKED DOES NOT BLOCK. "
      "Without SKIP LOCKED this waits for the holder's transaction and "
      "hits the statement timeout — which is what a second worker would "
      "do in production, silently, under load")
check(skipped is not None and skipped["id"] == lock2,
      f"AND IT STEPS OVER THE LOCKED ROW TO THE NEXT JOB rather than "
      f"returning nothing. Skipping to work that IS available is the "
      f"whole reason the queue scales to two workers (got {skipped})")
check(status_of(lock1) == "queued",
      "while the locked job is untouched, still waiting for its holder")

hcur.close()
holder.rollback()
holder.close()
tcur = other.cursor()
tcur.execute("SET statement_timeout = 0")
other.commit()
tcur.close()
other.close()


# ── failure and retry ──
wipe()
f1 = J.enqueue(conn, "flaky", {}, ts=TS, max_attempts=3)
conn.commit()

J.claim(conn, worker="w", ts=TS)
outcome = J.fail(conn, f1, "connection reset", ts=TS)
conn.commit()
check(outcome == "queued", "a first failure requeues")
check(status_of(f1) == "queued", "and the job is queued again")
run_after = sql("SELECT run_after FROM mf_jobs WHERE id = %s", (f1,))[0][0]
check(run_after > NOW,
      "WITH A DELAY BEFORE THE RETRY. Immediate retry against something "
      "that is down is a denial of service aimed at your own dependency")

first_delay = run_after - NOW
J.claim(conn, worker="w", ts=C.TimeService(run_after))
J.fail(conn, f1, "still down", ts=C.TimeService(run_after))
conn.commit()
second_after = sql("SELECT run_after FROM mf_jobs WHERE id = %s",
                   (f1,))[0][0]
check(second_after - run_after > first_delay,
      f"and the delay GROWS between attempts ({first_delay} then "
      f"{second_after - run_after})")

J.claim(conn, worker="w", ts=C.TimeService(second_after))
final = J.fail(conn, f1, "gave up", ts=C.TimeService(second_after))
conn.commit()
check(final == "dead",
      "the third failure of a three-attempt job is DEAD, not a fourth "
      "retry")
check(status_of(f1) == "dead", "and the row says so")
check(J.claim(conn, worker="w",
              ts=C.TimeService(NOW + timedelta(days=30))) is None,
      "A DEAD JOB IS NEVER PICKED UP AGAIN, at any later time. A dead "
      "letter that silently retries is a log file")

dead = J.dead_letter(conn)
check(len(dead) == 1 and dead[0]["id"] == f1 and "gave up" in
      dead[0]["last_error"],
      "it appears in the dead letter with the error that killed it")

check(J.revive(conn, f1, ts=TS), "a dead job can be deliberately revived")
conn.commit()
check(status_of(f1) == "queued" and
      sql("SELECT attempts FROM mf_jobs WHERE id = %s", (f1,))[0][0] == 0,
      "WITH ITS ATTEMPT COUNT RESET. Reviving into a spent counter would "
      "kill it on the first try and make the fix look like it did not "
      "work")
check(not J.revive(conn, f1, ts=TS),
      "and reviving something that is not dead does nothing")
conn.commit()


# ── stalled workers ──
wipe()
s1 = J.enqueue(conn, "task", {}, ts=TS)
conn.commit()
J.claim(conn, worker="worker-that-dies", ts=TS)
check(status_of(s1) == "running", "a job is running")
check(J.reap_stalled(conn, ts=TS) == 0,
      "a job running for a moment is not stalled")
conn.commit()

_much_later = C.TimeService(NOW + timedelta(hours=2))
check(J.reap_stalled(conn, ts=_much_later) == 1,
      "BUT ONE STILL RUNNING TWO HOURS LATER IS RECLAIMED. A container "
      "restart mid-job otherwise leaves the row running forever with "
      "nobody running it — the commonest way a Postgres queue quietly "
      "stops doing some of its work")
conn.commit()
check(status_of(s1) == "queued", "and it is queued again")
check(sql("SELECT attempts FROM mf_jobs WHERE id = %s", (s1,))[0][0] == 1,
      "KEEPING ITS ATTEMPT COUNT. A reaper that reset it would be a way "
      "around max_attempts, and a job that stalls every time would loop "
      "forever")

wipe()
s2 = J.enqueue(conn, "task", {}, ts=TS, max_attempts=1)
conn.commit()
J.claim(conn, worker="dies-again", ts=TS)
J.reap_stalled(conn, ts=_much_later)
conn.commit()
check(status_of(s2) == "dead",
      "a stalled job with no attempts left goes to the dead letter rather "
      "than being requeued — otherwise the reaper is a way around the "
      "attempt limit")


# ── run_one ──
wipe()
seen = []


def ok_handler(conn, payload):
    seen.append(payload)


def boom_handler(conn, payload):
    raise ValueError("handler exploded")


h1 = J.enqueue(conn, "ok", {"v": 1}, ts=TS)
conn.commit()
result = J.run_one(conn, {"ok": ok_handler}, worker="w", ts=TS)
check(result and result["outcome"] == "done", "run_one runs a job")
check(seen == [{"v": 1}], "the handler saw the payload")
check(status_of(h1) == "done", "and the job is done")
check(J.run_one(conn, {"ok": ok_handler}, worker="w", ts=TS) is None,
      "an empty queue returns None rather than spinning")

h2 = J.enqueue(conn, "boom", {}, ts=TS, max_attempts=2)
conn.commit()
result = J.run_one(conn, {"boom": boom_handler}, worker="w", ts=TS)
check(result and result["outcome"] == "queued",
      "A HANDLER RAISING IS A FAILED JOB, NOT A CRASHED WORKER. The whole "
      "reason for a queue is that the retried thing is allowed to go "
      "wrong; a worker that dies on the first exception turns a "
      "retryable failure into an outage")
check("handler exploded" in
      sql("SELECT last_error FROM mf_jobs WHERE id = %s", (h2,))[0][0],
      "and the error is recorded on the row")

h3 = J.enqueue(conn, "nobody_handles_this", {}, ts=TS)
conn.commit()
result = J.run_one(conn, {"ok": ok_handler}, worker="w", ts=TS)
check(result and result["outcome"] == "dead",
      "AN UNKNOWN KIND DIES IMMEDIATELY rather than retrying five times. "
      "No amount of waiting makes a handler appear, and retrying only "
      "delays somebody noticing")
check(status_of(h3) == "dead", "confirmed on the row")


# ── a handler's writes roll back with its failure ──
wipe()


def writes_then_fails(conn, payload):
    cur = conn.cursor()
    cur.execute("INSERT INTO mf_organizations (legal_name) VALUES "
                "('should not survive')")
    cur.close()
    raise RuntimeError("after writing")


before = sql("SELECT COUNT(*) FROM mf_organizations "
             "WHERE legal_name = 'should not survive'")[0][0]
h4 = J.enqueue(conn, "half", {}, ts=TS, max_attempts=2)
conn.commit()
J.run_one(conn, {"half": writes_then_fails}, worker="w", ts=TS)
after = sql("SELECT COUNT(*) FROM mf_organizations "
            "WHERE legal_name = 'should not survive'")[0][0]
check(before == after == 0,
      "A HANDLER THAT WRITES AND THEN RAISES LEAVES NOTHING BEHIND. Its "
      "work is rolled back before the failure is recorded, so a retry "
      "starts from the same state as the first attempt rather than on top "
      "of a half-finished one")
check(status_of(h4) == "queued", "while the job itself is queued to retry")


# ── stats ──
counts = J.stats(conn)
check(set(counts) >= {"queued", "running", "done", "failed", "dead"},
      f"stats reports EVERY status, including the ones with no rows. A "
      f"missing key reads as an error at the call site; a zero reads as "
      f"the truth (got {counts})")
check(counts["dead"] == 0 and counts["queued"] == 1,
      f"and the counts match what is actually there — one queued job "
      f"awaiting retry, nothing dead (got {counts})")
J.enqueue(conn, "doomed", {}, ts=TS, max_attempts=1)
conn.commit()
J.run_one(conn, {}, worker="w", ts=TS)
check(J.stats(conn)["dead"] == 1,
      "and the dead count moves when something dies")

conn.close()

if _FAILS:
    print(f"FAIL — {len(_FAILS)}/{_COUNT} checks failed:")
    for m in _FAILS:
        print("  ✗", m)
    sys.exit(1)
print(f"OK — all {_COUNT} ops-jobs checks passed.")
print("   Two connections proved SKIP LOCKED, a dead job stayed dead, a "
      "stalled one came\n   back with its attempt count, and a failing "
      "handler left no partial write.")
sys.exit(0)

"""The ops routes refuse to serve against a schema nobody verified.

Run:  python tests/test_ops_failclosed.py
The pure half always runs. The end-to-end half SKIPS without DATABASE_URL:

    DATABASE_URL="postgresql://postgres@/mfops?host=/var/tmp/mfpg&port=55432" \
        python tests/test_ops_failclosed.py

TWO FAILURES IN TWO DIRECTIONS, and this file exists because believing one
implies the other is the easy mistake.

  * THE PROCESS FAILS OPEN. database.init_db() runs at import time, so an
    exception out of the ops bootstrap does not degrade the ops platform —
    it stops /screener, /norcal and every board that has been serving for
    months. migrate_on_boot() never raises. tests/test_ops_schema.py holds
    that half.

  * THE OPS ROUTES FAIL CLOSED. Writing to a schema you did not expect is
    how data gets corrupted. Phase 1-D put real traffic on the mf_ tables,
    so an unverified schema means 503, not best effort.

The pure half proves the gate refuses. The end-to-end half proves it
refuses OVER HTTP while /map still answers 200 in the same process, which
is the whole claim and is not provable by calling a function.
"""
import os
import subprocess
import sys
import time
import urllib.error
import urllib.request

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from fastapi import HTTPException

from lib.ops import bootstrap as B
from routers.ops import deps

_COUNT = 0
_FAILS = []


def check(cond, msg):
    global _COUNT
    _COUNT += 1
    if not cond:
        _FAILS.append(msg)


def _report_and_exit(note=""):
    if _FAILS:
        print(f"FAIL — {len(_FAILS)}/{_COUNT} checks failed:")
        for m in _FAILS:
            print("  ✗", m)
        sys.exit(1)
    print(f"OK — all {_COUNT} ops fail-closed checks passed.")
    if note:
        print(note)
    sys.exit(0)


def _drive(gen):
    """Run the db() dependency far enough to see whether it refuses."""
    try:
        next(gen)
        return None
    except HTTPException as e:
        return e
    finally:
        gen.close()


# ══════════════════════════════════════════════════════════════════
# THE GATE ITSELF — no database needed to prove a refusal
# ══════════════════════════════════════════════════════════════════
# THE DEFAULT IS READ IN A FRESH INTERPRETER, because reset_state() below
# would mask it: a module that shipped with _READY = True would still look
# closed to every check in this file once reset_state() had been called.
# The only way to see the value the module actually loads with is to load
# it somewhere nothing has touched it.
_fresh = subprocess.run(
    [sys.executable, "-c",
     "import sys; sys.path.insert(0, '.'); "
     "from lib.ops import bootstrap as B; print(B.schema_ready())"],
    cwd=ROOT, capture_output=True, text=True)
check(_fresh.stdout.strip() == "False",
      f"A FRESHLY IMPORTED bootstrap IS NOT READY. The flag defaults "
      f"closed, so a process that mounts the routers without running "
      f"init_db fails safe instead of opening the platform onto a database "
      f"nobody checked (got {_fresh.stdout.strip()!r})")

B.reset_state()
check(B.schema_ready() is False,
      "and reset_state() puts it back there")

exc = _drive(deps.db())
check(exc is not None and exc.status_code == 503,
      "and db() REFUSES rather than handing out a connection. This is the "
      "whole change: before it, an ops request against an unverified "
      "schema got a working connection and wrote to whatever was there")
check(exc is not None and "ops unavailable" in str(exc.detail),
      "the body says ops is unavailable, not 'internal error' — the "
      "request was fine, the deployment is not")
check(exc is not None and "have not run" in str(exc.detail),
      "AND IT QUOTES THE REASON. A 503 that does not say why sends the "
      "reader to the logs of a process that may have restarted since")

# 503 rather than 500 is not cosmetic: it is the difference between "retry
# once this is fixed" and "this request is broken, do not retry".
check(exc is not None and exc.status_code != 500,
      "503 not 500, because a retry after the schema is current is the "
      "correct client behaviour and a 500 tells the client the opposite")

B._mark(False, "schema drift: 0001 foundation was EDITED after being applied")
exc = _drive(deps.db())
check(exc is not None and "drift" in str(exc.detail),
      "drift closes the gate too, and the drift message reaches the client "
      "— the case where the database has a schema no checkout describes is "
      "precisely the one where a write must not happen")

# ── and it OPENS again, so the gate is a gate and not a wall ──
B._mark(True, "schema current")
exc = _drive(deps.db())
check(exc is None or exc.status_code != 503
      or "ops unavailable" not in str(exc.detail),
      "MUTATION — with the schema marked current the gate stops refusing. "
      "Without this, a db() that raised 503 unconditionally would pass "
      "every check above and the platform would never serve at all")

B.reset_state()


# ══════════════════════════════════════════════════════════════════
# END TO END — ops closed and the boards open, in one process
# ══════════════════════════════════════════════════════════════════
if not os.environ.get("DATABASE_URL"):
    _report_and_exit(
        "   Pure half only — no DATABASE_URL, so the over-HTTP half did NOT\n"
        "   run. See this file's docstring to run it.")

import psycopg2                                            # noqa: E402

PORT = int(os.environ.get("OPS_FAILCLOSED_PORT", "58237"))
BASE = f"http://127.0.0.1:{PORT}"

conn = psycopg2.connect(os.environ["DATABASE_URL"])
for t in ["mf_sessions", "mf_jobs", "mf_documents", "mf_audit_log",
          "mf_jurisdiction_rules", "mf_jurisdictions", "mf_user_roles",
          "mf_roles", "mf_users", "mf_divisions", "mf_organizations",
          "mf_migrations"]:
    cur = conn.cursor()
    cur.execute(f"DROP TABLE IF EXISTS {t} CASCADE")
    conn.commit()
    cur.close()
cur = conn.cursor()
cur.execute("DROP FUNCTION IF EXISTS mf_audit_log_immutable() CASCADE")
conn.commit()
cur.close()


def http(method, path):
    req = urllib.request.Request(BASE + path, method=method)
    try:
        with urllib.request.urlopen(req, timeout=15) as r:
            return r.status, r.read().decode("utf-8", "replace")
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode("utf-8", "replace")


def start(env_extra):
    env = dict(os.environ, **env_extra)
    p = subprocess.Popen(
        [sys.executable, "-m", "uvicorn", "main:app", "--host", "127.0.0.1",
         "--port", str(PORT), "--log-level", "warning"],
        cwd=ROOT, env=env, stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT, text=True)
    deadline = time.time() + 45
    while time.time() < deadline:
        if p.poll() is not None:
            out = p.stdout.read() if p.stdout else ""
            print("server died during startup:\n", out[-3000:])
            return None
        try:
            urllib.request.urlopen(f"{BASE}/map", timeout=2)
            return p
        except urllib.error.HTTPError:
            return p
        except Exception:
            time.sleep(0.4)
    return None


def stop(p):
    if p and p.poll() is None:
        p.terminate()
        try:
            p.wait(timeout=10)
        except subprocess.TimeoutExpired:
            p.kill()


# MF_OPS_MIGRATE=0 against a database with no mf_ tables at all. This is
# the exact deployment the old code would have treated as a clean boot:
# it skipped entirely and returned [], and the portals then served over
# tables that did not exist.
server = start({"MF_OPS_MIGRATE": "0"})
if server is None:
    print("FAIL — the server never came up")
    sys.exit(1)
try:
    status, body = http("POST", "/ops/staff/login")
    check(status == 503,
          f"AN OPS ROUTE RETURNS 503 when the schema is behind and "
          f"migrations are disabled (got {status})")
    check("ops unavailable" in body or "not applied" in body,
          "and the body names the reason rather than making the operator "
          "go and read a log")

    status, _ = http("GET", "/ops/api/users")
    check(status == 503,
          f"so does the API surface, because the gate is on db() — the one "
          f"chokepoint every ops data path goes through — and not on each "
          f"route, where the next route added would miss it (got {status})")

    status, body = http("GET", "/map")
    check(status == 200 and len(body) > 1000,
          f"AND /map STILL SERVES, in the same process, at the same moment. "
          f"That is the entire point of the split: the ops platform refusing "
          f"must never be why the boards stop (got {status})")
    status, body = http("GET", "/holt")
    check(status == 200 and len(body) > 1000,
          f"and so does /holt (got {status})")
finally:
    stop(server)


# Now bring the schema up and prove the gate OPENS — otherwise every check
# above is satisfied by a platform that is simply broken.
server = start({"MF_OPS_MIGRATE": "1"})
if server is None:
    print("FAIL — the server never came up on the second start")
    sys.exit(1)
try:
    status, _ = http("GET", "/ops/staff/login")
    check(status == 200,
          f"with the schema migrated, the staff login serves again (got "
          f"{status}) — the gate opens, so the refusals above are the gate "
          f"working and not the platform being dead")
    status, _ = http("GET", "/map")
    check(status == 200, "and the boards are unaffected either way")
finally:
    stop(server)

cur = conn.cursor()
cur.execute("SELECT count(*) FROM mf_migrations")
applied = cur.fetchone()[0]
cur.close()
conn.close()
check(applied >= 2,
      "and the second boot really did apply the migrations, so the 200 "
      "above came from a live schema rather than from the gate being off")

_report_and_exit(
    "   Over real HTTP: ops returned 503 against an unmigrated schema while\n"
    "   /map and /holt served 200 from the same process, and ops came back\n"
    "   once the migrations ran.")

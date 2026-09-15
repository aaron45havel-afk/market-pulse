"""Phase 1's acceptance criteria, walked literally, in order.

Run:  python tests/test_ops_acceptance.py
SKIPS (exit 0) without DATABASE_URL.

    DATABASE_URL="postgresql://postgres@/mfops?host=/var/tmp&port=55433" \
        python tests/test_ops_acceptance.py

The other eight suites prove the parts. This proves the whole, in the
words the phase was specified in:

  1. create org/divisions/users in each role
  2. a tenant-role session hitting a staff API route directly gets 403
  3. audit log records a read of tenant PII
  4. migrations run clean up and down

Nothing here is a new mechanism. It exists because a set of green unit
suites is not the same claim as "the thing described has been built",
and the gap between them is where a phase gets called done while
something obvious is missing. Writing this is what surfaced that there
was no way to create the first administrator at all.

It starts from an EMPTY database and uses the real bootstrap script as a
subprocess, so the first step is the one an operator would actually
take.
"""
import json
import os
import subprocess
import sys
import urllib.error
import urllib.parse
import urllib.request

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

if not os.environ.get("DATABASE_URL"):
    print("SKIP — no DATABASE_URL. See this file's docstring to run it.")
    sys.exit(0)

import psycopg2

from lib.ops import auth as AU
from lib.ops import clock as C
from lib.ops import repository as RP
from lib.ops import scope as S
from lib.ops.migrations import runner as R

_COUNT = 0
_FAILS = []


def check(cond, msg):
    global _COUNT
    _COUNT += 1
    if not cond:
        _FAILS.append(msg)


URL = os.environ["DATABASE_URL"]
ADMIN_PW = "bootstrap-admin-password-1"
conn = psycopg2.connect(URL)


def sql(q, args=None):
    cur = conn.cursor()
    cur.execute(q, args)
    rows = cur.fetchall() if cur.description else []
    cur.close()
    return rows


def wipe_everything():
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


# ══════════════════════════════════════════════════════════════════
# 4. migrations run clean up and down   (first, since nothing works
#    without them and the acceptance list is not an execution order)
# ══════════════════════════════════════════════════════════════════
wipe_everything()
check(sql("SELECT COUNT(*) FROM pg_tables WHERE schemaname='public' "
          "AND tablename LIKE 'mf!_%' ESCAPE '!'")[0][0] == 0,
      "the acceptance run starts against an empty database")

names = [f"{m['version']:04d}_{m['name']}" for m in R.discover()]
check(R.migrate(conn, actor="acceptance") == names,
      f"every migration applies from nothing ({names})")
check(R.rollback(conn, steps=len(names)) == list(reversed(names)),
      "and all of them roll back")
check(sql("SELECT COUNT(*) FROM pg_tables WHERE schemaname='public' "
          "AND tablename LIKE 'mf!_%' ESCAPE '!'")[0][0] == 1,
      "leaving only the migration ledger behind")
check(R.migrate(conn, actor="acceptance") == names,
      "AND APPLY AGAIN — up, down, up, which is criterion 4 in the words "
      "it was written in")
conn.commit()


# ══════════════════════════════════════════════════════════════════
# 1. create org / divisions / users in each role
# ══════════════════════════════════════════════════════════════════
# Through the real bootstrap script, as a subprocess, because the first
# administrator cannot be created any other way — and discovering that
# is what this file was for.
proc = subprocess.run(
    [sys.executable, "scripts/mfops_bootstrap.py",
     "--org", "Havel Property Holdings LLC", "--division", "California",
     "--email", "owner@example.invalid", "--name", "The Owner",
     "--password", ADMIN_PW],
    cwd=ROOT, capture_output=True, text=True, env={**os.environ})
check(proc.returncode == 0,
      f"THE BOOTSTRAP SCRIPT CREATES THE FIRST ADMINISTRATOR. Every write "
      f"path needs a Scope, a Scope needs a session, a session needs a "
      f"user — so without this there is no way into the platform at all "
      f"(rc={proc.returncode}: {proc.stderr[-500:]})")
check("MFA IS REQUIRED" in proc.stdout and "secret" in proc.stdout,
      "and prints the TOTP secret once, since platform_admin cannot sign "
      "in without it")

secret_line = [l for l in proc.stdout.splitlines()
               if l.strip().startswith("secret")]
ADMIN_SECRET = secret_line[0].split()[-1] if secret_line else ""
check(len(ADMIN_SECRET) >= 32, "the printed secret is a real TOTP secret")

again = subprocess.run(
    [sys.executable, "scripts/mfops_bootstrap.py",
     "--org", "Second Co", "--email", "sneaky@example.invalid",
     "--password", ADMIN_PW],
    cwd=ROOT, capture_output=True, text=True, env={**os.environ})
check(again.returncode != 0 and "already exists" in again.stderr,
      "AND REFUSES TO RUN A SECOND TIME. A bootstrap that runs again "
      "'just in case' is how a forgotten full-access account ends up in a "
      "production database")
check(sql("SELECT COUNT(*) FROM mf_organizations")[0][0] == 1,
      "so there is still exactly one organization")

ORG = sql("SELECT id FROM mf_organizations")[0][0]
ADMIN = sql("SELECT id FROM mf_users WHERE email = 'owner@example.invalid'"
            )[0][0]
CA = sql("SELECT id FROM mf_divisions WHERE name = 'California'")[0][0]

# From here on, through the repository as the administrator — the way a
# real operator would, rather than by writing rows.
admin_scope = S.Scope(user_id=ADMIN, organization_id=ORG, portal="staff",
                      grants=AU.roles_for(conn, ADMIN))
check(admin_scope.roles == {"platform_admin"} and admin_scope.org_wide,
      "the bootstrapped account is an organization-wide platform admin")

repo = RP.Repository(conn, admin_scope, request_id="acceptance")
RI = repo.insert("mf_divisions", {"name": "Rhode Island",
                                  "description": "The RI building"})
check(RI is not None, "the administrator creates a second division")
check(repo.count("mf_divisions") == 2, "and sees both")

created = {}
for role, email, division in (
        ("division_manager", "camanager@example.invalid", CA),
        ("division_manager", "rimanager@example.invalid", RI),
        ("staff", "maintenance@example.invalid", CA),
        ("owner_client", "investor@example.invalid", CA),
        ("tenant", "resident@example.invalid", CA),
        ("vendor", "plumber@example.invalid", CA)):
    uid = repo.insert("mf_users", {"email": email,
                                   "full_name": email.split("@")[0],
                                   "division_id": division})
    check(uid is not None, f"a {role} user is created ({email})")
    grant = repo.grant_role(uid, role, division_id=division)
    check(grant is not None, f"and granted the {role} role")
    AU.set_password(conn, uid, f"password-for-{role}-1", ts=C.TimeService())
    created[email] = (uid, role, division)
conn.commit()

check(len(created) == 6, "six users across all six roles exist")
check(set(r[0] for r in sql(
          "SELECT DISTINCT r.key FROM mf_user_roles ur "
          "JOIN mf_roles r ON r.id = ur.role_id "
          "WHERE ur.revoked_at IS NULL")) == S.ROLES,
      "EVERY ONE OF THE SIX ROLES IS HELD BY SOMEBODY — criterion 1, and "
      "a role nobody can hold is a role nobody has tested")

# The managers need MFA to be usable at all.
for email in ("camanager@example.invalid", "rimanager@example.invalid"):
    uid = created[email][0]
    created[email] = created[email] + (AU.enroll_mfa(conn, uid,
                                                     C.TimeService()),)
conn.commit()


# ══════════════════════════════════════════════════════════════════
# 3. the audit log records a read of tenant PII
# ══════════════════════════════════════════════════════════════════
tenant_id = created["resident@example.invalid"][0]
before = sql("SELECT COUNT(*) FROM mf_audit_log WHERE action = 'read_pii'"
             )[0][0]
row = repo.fetch_one("mf_users", tenant_id)
conn.commit()
check(row and row["email"] == "resident@example.invalid",
      "the administrator reads the resident's record")
after = sql("SELECT actor_user_id, target_type, detail FROM mf_audit_log "
            "WHERE action = 'read_pii' ORDER BY id DESC LIMIT 1")
check(sql("SELECT COUNT(*) FROM mf_audit_log WHERE action = 'read_pii'"
          )[0][0] > before,
      "AND THE AUDIT LOG RECORDS IT — criterion 3")
check(after and after[0][0] == ADMIN and after[0][1] == "mf_users",
      f"naming who read it and from which table (got {after})")
check(after and str(tenant_id) in str(after[0][2]),
      f"AND WHICH RECORD. 'Someone read tenant records' answers no "
      f"question worth asking (detail: {after[0][2] if after else None})")


# ══════════════════════════════════════════════════════════════════
# 2. a tenant-role session hitting a staff API route gets 403
# ══════════════════════════════════════════════════════════════════
PORT = int(os.environ.get("OPS_ACCEPT_PORT", "58311"))
BASE = f"http://127.0.0.1:{PORT}"
conn.commit()

server = subprocess.Popen(
    [sys.executable, "-m", "uvicorn", "main:app", "--host", "127.0.0.1",
     "--port", str(PORT), "--log-level", "warning"],
    cwd=ROOT, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *a, **k):
        return None


opener = urllib.request.build_opener(NoRedirect)


def http(method, path, data=None, cookies=None):
    body = urllib.parse.urlencode(data).encode() if data else None
    req = urllib.request.Request(BASE + path, data=body, method=method)
    if body:
        req.add_header("Content-Type", "application/x-www-form-urlencoded")
    if cookies:
        req.add_header("Cookie", "; ".join(f"{k}={v}" for k, v in
                                           cookies.items()))
    try:
        r = opener.open(req, timeout=20)
        return r.status, r.read().decode("utf-8", "replace"), r.headers
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode("utf-8", "replace"), e.headers


try:
    import time
    up = False
    for _ in range(100):
        if server.poll() is not None:
            print("server died:\n", (server.stdout.read() or "")[-2000:])
            break
        try:
            http("GET", "/ops/staff/login")
            up = True
            break
        except Exception:
            time.sleep(0.4)
    check(up, "the application starts with the ops routes mounted")

    def sign_in(portal, email, password):
        status, _, headers = http("POST", f"/ops/{portal}/login",
                                  {"email": email, "password": password})
        if status != 303:
            return None
        for raw in headers.get_all("Set-Cookie") or []:
            name, _, rest = raw.partition("=")
            if name == f"mfops_{portal}":
                return rest.split(";")[0]
        return None

    tenant_cookie = sign_in("tenant", "resident@example.invalid",
                            "password-for-tenant-1")
    check(tenant_cookie is not None,
          "the resident signs in at the resident portal")

    status, body, _ = http("GET", "/ops/api/users",
                           cookies={"mfops_tenant": tenant_cookie})
    check(status == 403,
          f"A TENANT-ROLE SESSION HITTING A STAFF API ROUTE DIRECTLY GETS "
          f"403 — criterion 2, in the words it was written in, with a real "
          f"cookie against a real endpoint on a real server "
          f"(got {status}: {body[:200]})")

    # And the same for every other outside role, since "tenant" in the
    # criterion is an example of a class, not the only case.
    for portal, email, role in (
            ("owner", "investor@example.invalid", "owner_client"),
            ("vendor", "plumber@example.invalid", "vendor")):
        c = sign_in(portal, email, f"password-for-{role}-1")
        check(c is not None, f"the {role} signs in at the {portal} portal")
        st, _, _ = http("GET", "/ops/api/users", cookies={f"mfops_{portal}": c})
        check(st == 403, f"and is refused the staff API too (got {st})")

    # The administrator, through the full two-step sign-in, is not.
    from datetime import datetime, timezone
    admin_cookie = sign_in("staff", "owner@example.invalid", ADMIN_PW)
    check(admin_cookie is not None, "the administrator signs in")
    st, _, _ = http("GET", "/ops/api/users",
                    cookies={"mfops_staff": admin_cookie})
    check(st == 401,
          f"and reaches nothing before MFA, their role requiring it "
          f"(got {st})")

    final = admin_cookie
    for _ in range(4):
        code = AU.totp_code(ADMIN_SECRET, datetime.now(timezone.utc))
        st, _, headers = http("POST", "/ops/staff/mfa", {"code": code},
                              cookies={"mfops_staff": final})
        if st == 303:
            for raw in headers.get_all("Set-Cookie") or []:
                if raw.startswith("mfops_staff="):
                    final = raw.split("=", 1)[1].split(";")[0]
            break
        time.sleep(31)
    st, body, _ = http("GET", "/ops/api/users",
                       cookies={"mfops_staff": final})
    check(st == 200,
          f"and after the second factor, reaches the staff API (got {st})")
    users = json.loads(body)["users"] if st == 200 else []
    check(len(users) == 7,
          f"seeing all seven people, being organization-wide (got "
          f"{len(users)})")
    check(all("password_hash" not in u and "mfa_secret" not in u
              for u in users),
          "with no credential columns in the payload")

finally:
    if server.poll() is None:
        server.terminate()
        try:
            server.wait(timeout=10)
        except subprocess.TimeoutExpired:
            server.kill()

conn.commit()
conn.close()

if _FAILS:
    print(f"FAIL — {len(_FAILS)}/{_COUNT} checks failed:")
    for m in _FAILS:
        print("  ✗", m)
    sys.exit(1)
print(f"OK — all {_COUNT} Phase 1 acceptance checks passed.")
print("   From an empty database: migrations up/down/up, an organization "
      "and users in\n   all six roles created through the bootstrap and "
      "the repository, a PII read\n   in the audit log, and three outside "
      "portals refused at the staff API.")
sys.exit(0)

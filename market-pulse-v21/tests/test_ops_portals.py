"""The four portals over real HTTP — the acceptance criterion, end to end.

Run:  python tests/test_ops_portals.py
SKIPS (exit 0) without DATABASE_URL.

    DATABASE_URL="postgresql://postgres@/mfops?host=/var/tmp&port=55433" \
        python tests/test_ops_portals.py

This starts a REAL uvicorn server and talks to it with urllib from the
standard library. Not fastapi.testclient, which needs httpx — a
dependency this repo does not have and which is not worth adding to a
production image to run a test. Talking to a live socket also proves the
things an in-process client would paper over: actual status codes,
actual Set-Cookie headers, actual cookie scoping by path and name.

Phase 1's acceptance says: "a tenant-role session hitting a staff API
route directly gets 403 (by test)". That check is below, and it is done
with a genuine tenant session cookie against a genuine staff endpoint —
not by calling a permission function and trusting the wiring.
"""
import json
import os
import subprocess
import sys
import time
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
from lib.ops.migrations import runner as R

_COUNT = 0
_FAILS = []


def check(cond, msg):
    global _COUNT
    _COUNT += 1
    if not cond:
        _FAILS.append(msg)


PORT = int(os.environ.get("OPS_TEST_PORT", "58234"))
BASE = f"http://127.0.0.1:{PORT}"
PW = "correct-horse-battery-staple"


# ── fixture ──
conn = psycopg2.connect(os.environ["DATABASE_URL"])
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
R.migrate(conn, actor="test_ops_portals")

cur = conn.cursor()
cur.execute("INSERT INTO mf_organizations (legal_name) VALUES ('Test Co') "
            "RETURNING id")
ORG = cur.fetchone()[0]
cur.execute("INSERT INTO mf_divisions (organization_id, name) VALUES "
            "(%s, 'California') RETURNING id", (ORG,))
CA = cur.fetchone()[0]
cur.execute("INSERT INTO mf_divisions (organization_id, name) VALUES "
            "(%s, 'Rhode Island') RETURNING id", (ORG,))
RI = cur.fetchone()[0]


def make(email, role, division):
    cur.execute("INSERT INTO mf_users (organization_id, email, full_name, "
                "division_id, password_hash) VALUES (%s, %s, %s, %s, %s) "
                "RETURNING id",
                (ORG, email, email.split("@")[0], division,
                 AU.hash_password(PW)))
    uid = cur.fetchone()[0]
    cur.execute("INSERT INTO mf_user_roles (user_id, role_id, division_id) "
                "SELECT %s, id, %s FROM mf_roles WHERE key = %s",
                (uid, division, role))
    return uid


DM_CA = make("camanager@example.invalid", "division_manager", CA)
DM_RI = make("rimanager@example.invalid", "division_manager", RI)
STAFF_CA = make("castaff@example.invalid", "staff", CA)
STAFF_RI = make("ristaff@example.invalid", "staff", RI)
TENANT = make("tenant@example.invalid", "tenant", CA)

# The two managers hold division_manager, whose mf_roles row says
# requires_mfa = TRUE — so they cannot reach anything on a
# password-only session. Enrol them here so the test can drive the
# second factor over HTTP rather than skipping the gate it is meant to
# be proving.
MFA_SECRET = {}
for uid in (DM_CA, DM_RI):
    cur.execute("UPDATE mf_users SET mfa_secret = %s, mfa_enrolled_at = NOW() "
                "WHERE id = %s", (AU.new_totp_secret(), uid))
    cur.execute("SELECT mfa_secret FROM mf_users WHERE id = %s", (uid,))
    MFA_SECRET[uid] = cur.fetchone()[0]
conn.commit()
cur.close()
conn.close()


# ── the server ──
server = subprocess.Popen(
    [sys.executable, "-m", "uvicorn", "main:app", "--host", "127.0.0.1",
     "--port", str(PORT), "--log-level", "warning"],
    cwd=ROOT, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)


def stop_server():
    if server.poll() is None:
        server.terminate()
        try:
            server.wait(timeout=10)
        except subprocess.TimeoutExpired:
            server.kill()


def wait_up(seconds=45):
    deadline = time.time() + seconds
    while time.time() < deadline:
        if server.poll() is not None:
            out = server.stdout.read() if server.stdout else ""
            print("server died during startup:\n", out[-3000:])
            return False
        try:
            urllib.request.urlopen(f"{BASE}/ops/staff/login", timeout=2)
            return True
        except urllib.error.HTTPError:
            return True          # any HTTP answer means it is listening
        except Exception:
            time.sleep(0.4)
    return False


if not wait_up():
    stop_server()
    print("FAIL — the server never came up")
    sys.exit(1)


# ── a tiny HTTP client: no redirects followed, cookies held by hand ──
class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *a, **k):
        return None          # a 303 is a RESULT here, not a step to follow


opener = urllib.request.build_opener(NoRedirect)


def http(method, path, data=None, cookies=None, headers=None):
    body = urllib.parse.urlencode(data).encode() if data else None
    req = urllib.request.Request(BASE + path, data=body, method=method)
    if body:
        req.add_header("Content-Type", "application/x-www-form-urlencoded")
    if cookies:
        req.add_header("Cookie", "; ".join(f"{k}={v}" for k, v in
                                           cookies.items()))
    for k, v in (headers or {}).items():
        req.add_header(k, v)
    try:
        r = opener.open(req, timeout=20)
        return r.status, r.read().decode("utf-8", "replace"), r.headers
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode("utf-8", "replace"), e.headers


def _cookie_from(headers, portal):
    for raw in headers.get_all("Set-Cookie") or []:
        name, _, rest = raw.partition("=")
        if name == f"mfops_{portal}":
            return rest.split(";")[0]
    return None


def login(portal, email, password=PW):
    """Returns (cookie, status, redirect_target). Does NOT do MFA."""
    status, _, headers = http("POST", f"/ops/{portal}/login",
                              {"email": email, "password": password})
    if status != 303:
        return None, status, None
    return (_cookie_from(headers, portal), status,
            headers.get("Location"))


def login_with_mfa(portal, email, user_id):
    """Full two-step sign-in. Returns the cookie AFTER rotation.

    A code is single-use, so each call must land in its own 30-second
    step. Rather than sleeping, this retries on refusal — the previous
    call in the same step is the only thing that can cause one here.
    """
    cookie, status, target = login(portal, email)
    if not cookie:
        return None, status
    from datetime import datetime, timezone
    for _ in range(4):
        code = AU.totp_code(MFA_SECRET[user_id], datetime.now(timezone.utc))
        st, _, headers = http("POST", f"/ops/{portal}/mfa", {"code": code},
                              cookies={f"mfops_{portal}": cookie})
        if st == 303:
            return _cookie_from(headers, portal) or cookie, st
        time.sleep(TOTP_STEP_WAIT)
    return None, st


TOTP_STEP_WAIT = 31


try:
    # ── the login pages are public ──
    for portal in ("staff", "tenant", "owner", "vendor"):
        status, body, _ = http("GET", f"/ops/{portal}/login")
        check(status == 200, f"the {portal} login page is served ({status})")
        check("sign-in" in body.lower(),
              f"and looks like a {portal} sign-in form")

    # ── a bad password says nothing useful ──
    status, body, _ = http("POST", "/ops/staff/login",
                           {"email": "staff@example.invalid",
                            "password": "wrong"})
    check(status == 401, f"a wrong password is 401 (got {status})")
    status2, body2, _ = http("POST", "/ops/staff/login",
                             {"email": "nobody@example.invalid",
                              "password": "wrong"})
    check(body.strip() == body2.strip(),
          "AND A WRONG PASSWORD AND AN UNKNOWN ADDRESS RETURN THE SAME PAGE, "
          "byte for byte. Different messages turn the login form into an "
          "account-enumeration oracle")

    # ── signing in ──
    staff_cookie, st, target = login("staff", "castaff@example.invalid")
    check(staff_cookie is not None,
          f"a staff member signs in and gets a session cookie (status {st})")
    check(target == "/ops/staff/",
          f"and goes straight to the portal, their role not requiring a "
          f"second factor (got {target})")

    tenant_cookie, st, _ = login("tenant", "tenant@example.invalid")
    check(tenant_cookie is not None,
          f"and a tenant on the tenant portal (status {st})")

    _, st, _ = login("staff", "tenant@example.invalid")
    check(st == 401,
          "A TENANT CANNOT SIGN IN AT THE STAFF DOOR with correct "
          "credentials — the portal is part of the credential check")

    # ── MFA is required for the roles whose mf_roles row says so ──
    dm_partial, st, target = login("staff", "camanager@example.invalid")
    check(dm_partial is not None and target == "/ops/staff/mfa",
          f"A DIVISION MANAGER IS SENT TO THE SECOND FACTOR. The "
          f"requirement is read from mf_roles.requires_mfa, so it is a "
          f"fact in the database rather than an if-statement (got {target})")
    status, _, _ = http("GET", "/ops/api/users",
                        cookies={"mfops_staff": dm_partial})
    check(status == 401,
          f"AND THEIR PASSWORD-ONLY SESSION REACHES NOTHING. Being issued "
          f"a cookie is not being signed in — the MFA gate is in "
          f"resolve(), not in the redirect (got {status})")
    status, _, _ = http("POST", "/ops/staff/mfa", {"code": "000000"},
                        cookies={"mfops_staff": dm_partial})
    check(status == 401, f"a wrong code is refused (got {status})")

    # ── cookie hygiene ──
    _, _, headers = http("POST", "/ops/staff/login",
                         {"email": "castaff@example.invalid", "password": PW})
    raw = [c for c in (headers.get_all("Set-Cookie") or [])
           if c.startswith("mfops_staff=")]
    check(raw and "HttpOnly" in raw[0],
          "the session cookie is HttpOnly — one readable from JavaScript "
          "is one XSS away from being stolen")
    check(raw and "Path=/ops" in raw[0],
          "AND SCOPED TO /ops, so nothing on the analysis side ever "
          "receives it")
    check(raw and "samesite=lax" in raw[0].lower(),
          f"and SameSite=Lax, so a form on another site cannot post with "
          f"it (header: {raw[0] if raw else 'none'})")

    # ── THE ACCEPTANCE CRITERION ──
    status, body, _ = http("GET", "/ops/api/users",
                           cookies={"mfops_tenant": tenant_cookie})
    check(status == 403,
          f"A TENANT SESSION HITTING THE STAFF API GETS 403. Not a "
          f"redirect to a login page it is already past, not a 200 with an "
          f"empty list — a refusal. (got {status})")

    status, body, _ = http("GET", "/ops/api/users",
                           cookies={"mfops_staff": staff_cookie})
    check(status == 200, f"while the staff session gets 200 (got {status})")
    payload = json.loads(body) if status == 200 else {"users": []}
    check(all("password_hash" not in u and "mfa_secret" not in u
              for u in payload["users"]),
          "and the payload contains no password hash or TOTP seed")

    # A tenant cookie presented under the STAFF cookie name — the case a
    # server-side portal check must catch, since the browser's own
    # separation has been bypassed by hand.
    status, _, _ = http("GET", "/ops/api/users",
                        cookies={"mfops_staff": tenant_cookie})
    check(status in (401, 403),
          f"A TENANT TOKEN PRESENTED AS A STAFF COOKIE IS STILL REFUSED. "
          f"The cookie name is the browser's half of portal separation; "
          f"the session's own recorded portal is the server's, and this "
          f"is the check that survives an attacker with curl (got {status})")

    status, _, _ = http("GET", "/ops/api/users")
    check(status == 401,
          f"no cookie at all is 401, not 403 — nothing to authorise yet "
          f"(got {status})")
    status, _, _ = http("GET", "/ops/api/users",
                        cookies={"mfops_staff": "forged-token-value"})
    check(status == 401, f"and a forged token is 401 (got {status})")

    # ── division isolation over HTTP ──
    ristaff_cookie, _, _ = login("staff", "ristaff@example.invalid")
    status, body, _ = http("GET", "/ops/api/users",
                           cookies={"mfops_staff": staff_cookie})
    ca_emails = {u["email"] for u in json.loads(body)["users"]}
    status, body, _ = http("GET", "/ops/api/users",
                           cookies={"mfops_staff": ristaff_cookie})
    ri_emails = {u["email"] for u in json.loads(body)["users"]}
    check("ristaff@example.invalid" not in ca_emails
          and "rimanager@example.invalid" not in ca_emails,
          f"THE CALIFORNIA STAFF MEMBER'S API RESPONSE CONTAINS NOBODY "
          f"FROM RHODE ISLAND. Same route, same code, two answers "
          f"(saw {sorted(ca_emails)})")
    check("castaff@example.invalid" not in ri_emails
          and "camanager@example.invalid" not in ri_emails,
          f"and the reverse (saw {sorted(ri_emails)})")
    check(ca_emails and ri_emails and not (ca_emails & ri_emails),
          "with no overlap at all")

    # Now the manager, through the full two-step sign-in.
    ca_cookie, st = login_with_mfa("staff", "camanager@example.invalid", DM_CA)
    check(ca_cookie is not None,
          f"THE DIVISION MANAGER SIGNS IN WITH A REAL TOTP CODE and the "
          f"session becomes usable (status {st})")
    check(ca_cookie != dm_partial,
          "AND THE TOKEN WAS ROTATED. The session just became more capable "
          "than it was, so the cookie that got through the door is retired "
          "rather than promoted")
    status, _, _ = http("GET", "/ops/api/users",
                        cookies={"mfops_staff": dm_partial})
    check(status == 401,
          f"the pre-MFA token is dead afterwards (got {status})")
    status, body, _ = http("GET", "/ops/api/users",
                           cookies={"mfops_staff": ca_cookie})
    check(status == 200, f"and the rotated one works (got {status})")

    # ── the audit route: platform_admin only ──
    status, _, _ = http("GET", "/ops/api/audit",
                        cookies={"mfops_staff": ca_cookie})
    check(status == 403,
          f"a division manager is refused the audit log — it has no "
          f"division column, so there is no honest way to scope them to "
          f"part of it (got {status})")

    # ── portal homes ──
    status, body, _ = http("GET", "/ops/tenant/",
                           cookies={"mfops_tenant": tenant_cookie})
    check(status == 200, f"the tenant portal home renders (got {status})")
    check("tenant" in body.lower(),
          "and names the role in effect, so holding the wrong one is "
          "visible rather than silent")
    check("People visible</dt>" in body and ">1<" in body,
          "showing exactly one person visible: themselves")

    status, _, _ = http("GET", "/ops/staff/",
                        cookies={"mfops_tenant": tenant_cookie})
    check(status in (401, 403),
          f"and a tenant cookie does not open the staff home (got {status})")

    # ── privilege escalation over HTTP ──
    status, _, _ = http("POST", f"/ops/api/users/{STAFF_CA}/roles",
                        {"role": "platform_admin"},
                        cookies={"mfops_staff": ca_cookie})
    check(status == 403,
          f"A DIVISION MANAGER POSTING A platform_admin GRANT GETS 403. "
          f"This is the escalation path, exercised through the actual "
          f"route rather than the function behind it (got {status})")
    status, _, _ = http("POST", f"/ops/api/users/{STAFF_CA}/roles",
                        {"role": "division_manager", "division_id": str(CA)},
                        cookies={"mfops_staff": ca_cookie})
    check(status == 403, f"nor another division manager (got {status})")
    status, _, _ = http("POST", f"/ops/api/users/{STAFF_CA}/roles",
                        {"role": "tenant", "division_id": str(CA)},
                        cookies={"mfops_staff": ca_cookie})
    check(status == 201, f"but a role below them is granted (got {status})")

    # That grant bumped the target's privilege_epoch. Any session STAFF
    # held is now dead — proved over HTTP rather than asserted.
    status, _, _ = http("GET", "/ops/api/users",
                        cookies={"mfops_staff": staff_cookie})
    check(status == 401,
          f"AND THE AFFECTED USER'S EXISTING SESSION IS DEAD ON ITS NEXT "
          f"REQUEST, because the grant bumped their privilege_epoch. No "
          f"session-table scan, no cache to clear (got {status})")

    # ── a division manager cannot create outside their division ──
    status, _, _ = http("POST", "/ops/api/users",
                        {"email": "newri@example.invalid",
                         "division_id": str(RI)},
                        cookies={"mfops_staff": ca_cookie})
    check(status == 403,
          f"the California manager cannot create a user in Rhode Island "
          f"by posting to the API directly (got {status})")
    status, _, _ = http("POST", "/ops/api/users",
                        {"email": "newca@example.invalid",
                         "division_id": str(CA)},
                        cookies={"mfops_staff": ca_cookie})
    check(status == 201, f"but can in their own (got {status})")

    # ── logout ──
    status, _, headers = http("POST", "/ops/tenant/logout",
                              cookies={"mfops_tenant": tenant_cookie})
    check(status == 303, f"logout redirects (got {status})")
    status, _, _ = http("GET", "/ops/tenant/",
                        cookies={"mfops_tenant": tenant_cookie})
    check(status == 401,
          f"and the token stops working immediately afterwards, whether or "
          f"not the browser dropped the cookie (got {status})")

    # ── the analysis side is untouched ──
    status, _, headers = http("GET", "/")
    check(status == 308 and headers.get("Location") == "/map",
          f"the existing app's home still 308s to /map, exactly as it did "
          f"before (got {status} -> {headers.get('Location')})")
    status, body, _ = http("GET", "/map")
    check(status == 200 and len(body) > 1000,
          f"AND /map STILL SERVES ITS PAGE. The ops router mounts into the "
          f"same process; the whole seam is worth nothing if it broke the "
          f"boards that were already working (got {status})")
    status, body, _ = http("GET", "/holt")
    check(status == 200 and len(body) > 1000,
          f"and so does /holt, built earlier this week (got {status})")

finally:
    stop_server()

if _FAILS:
    print(f"FAIL — {len(_FAILS)}/{_COUNT} checks failed:")
    for m in _FAILS:
        print("  ✗", m)
    sys.exit(1)
print(f"OK — all {_COUNT} ops-portal checks passed.")
print("   Against a real uvicorn server: a tenant session got 403 from the "
      "staff API,\n   two managers got disjoint answers from one route, and "
      "a role grant killed\n   the affected user's live session.")
sys.exit(0)

"""Passwords, TOTP and sessions — proved against the attacks, not the demo.

Run:  python tests/test_ops_auth.py
SKIPS (exit 0) without DATABASE_URL for the session half; the password
and TOTP checks are pure and always run.

    DATABASE_URL="postgresql://postgres@/mfops?host=/var/tmp&port=55433" \
        python tests/test_ops_auth.py

The TOTP checks pin the clock. A time-based verifier tested against the
real clock passes at every moment except the ones that matter — the step
boundary, the skew window, and the second use of a code already spent.
"""
import os
import sys
from datetime import datetime, timedelta, timezone

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from lib.ops import auth as AU
from lib.ops import clock as C

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


UTC = timezone.utc
PW = "correct-horse-battery-staple"


# ══════════════════════════════════════════════════════════════════
# passwords — pure
# ══════════════════════════════════════════════════════════════════
h = AU.hash_password(PW)
check(AU.verify_password(PW, h), "the right password verifies")
check(not AU.verify_password(PW + "x", h), "a wrong one does not")
check(not AU.verify_password("", h), "nor an empty one")
check(PW not in h and PW.encode().hex() not in h,
      "THE PASSWORD DOES NOT APPEAR IN THE STORED VALUE, in plain text or "
      "hex. Obvious, and worth asserting: this is the check that fails "
      "loudly the day someone 'simplifies' the hashing")
check(AU.hash_password(PW) != h,
      "hashing the same password twice gives different output — the salt "
      "is per-hash, so two users with the same password are not visibly "
      "the same user in a dump")
check(h.startswith("scrypt$"), "the hash names its own scheme")
check(h.split("$")[1] == str(AU.SCRYPT_N),
      "and its parameters, so they can be raised later without a mass "
      "password reset")

check(AU.verify_password(PW, None) is False,
      "a user with NO password set verifies False rather than raising — an "
      "account mid-provisioning must not be a 500, and must not be a way "
      "in either")
check(AU.verify_password(PW, "") is False, "same for an empty hash")
check(AU.verify_password(PW, "garbage") is False,
      "a malformed stored hash is a failed login, not a crash")
check(AU.verify_password(PW, "scrypt$1$1$1$notbase64$alsonot") is False,
      "including one that parses but decodes to nonsense")
check(AU.verify_password(PW, "plain$" + PW) is False,
      "AND AN UNKNOWN SCHEME NEVER VERIFIES. A fallback that compared "
      "plaintext for an unrecognised prefix is the kind of 'compatibility' "
      "shim that turns a hash column into a password column")

check(raises(AU.AuthError, AU.hash_password, "short"),
      "a password under the minimum length is refused")
check(raises(AU.AuthError, AU.hash_password, "x" * 2000),
      "and one over the maximum, because an unbounded input to a "
      "deliberately slow function is a denial of service with extra steps")
check(raises(AU.AuthError, AU.hash_password, None), "and a non-string")

check(AU.needs_rehash("scrypt$1024$8$1$aaaa$bbbb"),
      "a hash made with weaker parameters is flagged for upgrade")
check(not AU.needs_rehash(h), "a current one is not")
check(AU.needs_rehash("md5$whatever"), "and an unknown scheme certainly is")


# ══════════════════════════════════════════════════════════════════
# TOTP — pure, clock pinned
# ══════════════════════════════════════════════════════════════════
SECRET = AU.new_totp_secret()
check(len(SECRET) >= 32, "a TOTP secret is at least 160 bits, per RFC 4226")
check(AU.new_totp_secret() != AU.new_totp_secret(), "and is random each time")

T0 = datetime(2026, 8, 27, 12, 0, 0, tzinfo=UTC)
code = AU.totp_code(SECRET, T0)
check(len(code) == 6 and code.isdigit(), "a code is six digits")
check(AU.totp_code(SECRET, T0) == code, "and is stable within its step")
check(AU.totp_code(SECRET, T0 + timedelta(seconds=29)) == code,
      "for the whole 30-second step")
check(AU.totp_code(SECRET, T0 + timedelta(seconds=31)) != code,
      "and changes in the next one")

# Against the RFC 6238 test vector, so this is a real TOTP and not merely
# a self-consistent hash. Seed "12345678901234567890" in base32.
RFC = "GEZDGNBVGY3TQOJQGEZDGNBVGY3TQOJQ"
check(AU.totp_code(RFC, datetime(1970, 1, 1, 0, 0, 59, tzinfo=UTC)) == "287082",
      "RFC 6238 TEST VECTOR AT T=59 IS 287082. Without this the whole "
      "module could be a private code generator that no authenticator app "
      "would ever agree with")
check(AU.totp_code(RFC, datetime(2005, 3, 18, 1, 58, 29, tzinfo=UTC)) == "081804",
      "and the T=1111111109 vector is 081804")

ok, counter = AU.verify_totp(SECRET, code, T0)
check(ok and counter is not None, "the current code verifies")
check(AU.verify_totp(SECRET, code, T0 + timedelta(seconds=30))[0],
      "one step late still verifies — clock skew between a phone and a "
      "server is ordinary")
check(AU.verify_totp(SECRET, code, T0 - timedelta(seconds=30))[0],
      "and one step early")
check(not AU.verify_totp(SECRET, code, T0 + timedelta(seconds=120))[0],
      "FOUR STEPS LATE DOES NOT. The window is one step either side; a "
      "generous window is a longer life for a code somebody watched being "
      "typed")
check(not AU.verify_totp(SECRET, "000000", T0)[0] or code == "000000",
      "a wrong code fails")
check(not AU.verify_totp(SECRET, "12345", T0)[0], "a short code fails")
check(not AU.verify_totp(SECRET, "abcdef", T0)[0], "a non-numeric one fails")
check(not AU.verify_totp(SECRET, "", T0)[0]
      and not AU.verify_totp(SECRET, None, T0)[0], "and an empty one")
check(AU.verify_totp(SECRET, f"  {code} ", T0)[0],
      "surrounding whitespace is tolerated — people paste from an app")

check(not AU.verify_totp(SECRET, code, T0, last_counter=counter)[0],
      "A CODE ALREADY USED IS REFUSED. Without the stored counter the "
      "skew window leaves every code valid for ninety seconds, which is "
      "ample time to reuse one somebody just read over a shoulder")
_next = AU.totp_code(SECRET, T0 + timedelta(seconds=30))
check(AU.verify_totp(SECRET, _next, T0 + timedelta(seconds=30),
                     last_counter=counter)[0] or _next == code,
      "but the NEXT code still works, so replay protection does not lock "
      "the user out of their own account")
check(not AU.verify_totp(SECRET, code, T0, last_counter=counter + 5)[0],
      "and a code older than one already accepted is refused too — that is "
      "a replay from further back, not a fresh login")

check(raises(AU.AuthError, AU.totp_code, SECRET),
      "totp_code REFUSES TO READ THE CLOCK ITSELF. A verifier that calls "
      "now() cannot be tested at a step boundary, and the step boundary is "
      "where it breaks")
uri = AU.provisioning_uri(SECRET, "someone@example.invalid")
check(uri.startswith("otpauth://totp/") and SECRET in uri
      and "period=30" in uri and "digits=6" in uri,
      "the provisioning URI carries the parameters the app needs")


# ══════════════════════════════════════════════════════════════════
# sessions — needs a database
# ══════════════════════════════════════════════════════════════════
def report(tail=""):
    if _FAILS:
        print(f"FAIL — {len(_FAILS)}/{_COUNT} checks failed:")
        for m in _FAILS:
            print("  ✗", m)
        sys.exit(1)
    print(f"OK — all {_COUNT} ops-auth checks passed.")
    if tail:
        print(tail)
    sys.exit(0)


if not os.environ.get("DATABASE_URL"):
    report("   Password and TOTP halves only — no DATABASE_URL, so login, "
           "MFA gating,\n   rotation and the privilege-epoch revocation "
           "path did NOT run.")

import psycopg2

from lib.ops import scope as S
from lib.ops.migrations import runner as R

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
R.migrate(conn, actor="test_ops_auth")


def sql(q, args=None):
    cur = conn.cursor()
    cur.execute(q, args)
    rows = cur.fetchall() if cur.description else []
    cur.close()
    return rows


NOW = datetime(2026, 8, 27, 12, 0, tzinfo=UTC)
TS = C.TimeService(NOW)

cur = conn.cursor()
cur.execute("INSERT INTO mf_organizations (legal_name) VALUES ('Test') "
            "RETURNING id")
ORG = cur.fetchone()[0]
cur.execute("INSERT INTO mf_divisions (organization_id, name) VALUES "
            "(%s, 'California') RETURNING id", (ORG,))
DIV = cur.fetchone()[0]


def make_user(email, role=None, division=DIV, password=PW):
    cur.execute("INSERT INTO mf_users (organization_id, email) VALUES "
                "(%s, %s) RETURNING id", (ORG, email))
    uid = cur.fetchone()[0]
    if password:
        cur.execute("UPDATE mf_users SET password_hash = %s WHERE id = %s",
                    (AU.hash_password(password), uid))
    if role:
        cur.execute(
            "INSERT INTO mf_user_roles (user_id, role_id, division_id) "
            "SELECT %s, id, %s FROM mf_roles WHERE key = %s",
            (uid, None if role == "platform_admin" else division, role))
    return uid


ADMIN = make_user("admin@example.invalid", "platform_admin")
STAFF = make_user("staff@example.invalid", "staff")
TEN = make_user("tenant@example.invalid", "tenant")
NOPW = make_user("nopw@example.invalid", "staff", password=None)
conn.commit()
cur.close()


# ── login ──
r = AU.login(conn, "staff@example.invalid", PW, "staff", TS)
check(bool(r) and r.token, "a staff member logs in to the staff portal")
check(r.mfa_required is False,
      "and is not asked for MFA, their role not requiring it")
check(len(sql("SELECT 1 FROM mf_sessions WHERE token_hash = %s",
              [__import__("hashlib").sha256(r.token.encode()).hexdigest()])) == 1,
      "a session row exists for the hash of the token")
check(sql("SELECT COUNT(*) FROM mf_sessions WHERE token_hash = %s",
          [r.token])[0][0] == 0,
      "THE TOKEN ITSELF IS NOWHERE IN THE DATABASE. Only its SHA-256 is "
      "stored, so a stolen dump cannot be replayed as a stolen login")
check(r.token not in repr(r),
      "and it does not appear in the result's repr, which is what would "
      "put a live session into a traceback or a debug log")

check(not AU.login(conn, "staff@example.invalid", "wrong", "staff", TS),
      "a wrong password fails")
check(not AU.login(conn, "nobody@example.invalid", PW, "staff", TS),
      "an unknown address fails")
check(not AU.login(conn, "nopw@example.invalid", PW, "staff", TS),
      "and an account with no password set cannot be logged into")

# The acceptance criterion, at the door rather than at the route.
check(not AU.login(conn, "tenant@example.invalid", PW, "staff", TS),
      "A TENANT CANNOT OPEN A STAFF SESSION EVEN WITH THE RIGHT PASSWORD. "
      "The portal is part of the credential check, not a preference "
      "applied afterwards")
check(AU.login(conn, "tenant@example.invalid", PW, "tenant", TS).ok,
      "the same credentials work on the tenant portal")
check(not AU.login(conn, "staff@example.invalid", PW, "tenant", TS),
      "and a staff member cannot open a tenant session either — it runs "
      "both ways")

_denied = sql("SELECT detail FROM mf_audit_log WHERE action = 'denied' "
              "AND detail->>'reason' = 'no role for portal'")
check(len(_denied) >= 1,
      "wrong-door logins are audited as refusals — a burst of these is "
      "somebody walking one valid password along all four portals")
conn.commit()


# ── lockout ──
for _ in range(AU.MAX_FAILED_LOGINS):
    AU.login(conn, "staff@example.invalid", "wrong", "staff", TS)
conn.commit()
check(sql("SELECT locked_until FROM mf_users WHERE id = %s",
          (STAFF,))[0][0] is not None,
      "repeated failures lock the ACCOUNT, which the attacker does not "
      "choose, rather than an IP, which they do")
check(not AU.login(conn, "staff@example.invalid", PW, "staff", TS),
      "and the correct password is refused while locked")
_late = C.TimeService(NOW + AU.LOCKOUT + timedelta(minutes=1))
check(AU.login(conn, "staff@example.invalid", PW, "staff", _late).ok,
      "the lock expires on its own — a permanent lock is a denial of "
      "service anybody can trigger by guessing at somebody else's account")
check(sql("SELECT failed_login_count FROM mf_users WHERE id = %s",
          (STAFF,))[0][0] == 0,
      "and a successful login clears the counter")
conn.commit()


# ── MFA gating ──
r_admin = AU.login(conn, "admin@example.invalid", PW, "staff", TS)
check(r_admin.ok and r_admin.mfa_required is True,
      "A PLATFORM ADMIN IS TOLD MFA IS REQUIRED. The requirement is read "
      "from mf_roles.requires_mfa, so it is a fact in the database rather "
      "than an if-statement somebody can forget")
conn.commit()
check(raises(AU.SessionRefusal, AU.resolve, conn, r_admin.token, TS),
      "and their session DOES NOT RESOLVE until MFA is satisfied — being "
      "issued a token is not the same as being logged in")

secret = AU.enroll_mfa(conn, ADMIN, TS)
conn.commit()
check(len(secret) >= 32, "enrolment returns a secret for the QR code")
check(sql("SELECT mfa_secret FROM mf_users WHERE id = %s",
          (ADMIN,))[0][0] == secret, "stored on the user")
check("mfa_secret" in S.NEVER_SELECT["mf_users"],
      "AND UNREADABLE THROUGH THE REPOSITORY EVER AFTER. A TOTP seed that "
      "can be read back is a seed that can be exfiltrated, and its holder "
      "mints valid codes forever without touching the account")

check(not AU.satisfy_mfa(conn, r_admin.session_id, ADMIN, "000000", TS)
      or AU.totp_code(secret, NOW) == "000000",
      "a wrong code does not satisfy MFA")
good = AU.totp_code(secret, NOW)
check(AU.satisfy_mfa(conn, r_admin.session_id, ADMIN, good, TS),
      "the right one does")
conn.commit()
scope = AU.resolve(conn, r_admin.token, TS)
check(scope is not None and "platform_admin" in scope.roles,
      "and now the session resolves to an admin scope")

check(not AU.satisfy_mfa(conn, r_admin.session_id, ADMIN, good, TS),
      "THE SAME CODE CANNOT BE USED TWICE. The accepted counter is stored "
      "on the user, which is the only thing standing between the skew "
      "window and a ninety-second replay")
conn.commit()


# ── resolve ──
r_staff = AU.login(conn, "staff@example.invalid", PW, "staff", TS)
conn.commit()
sc = AU.resolve(conn, r_staff.token, TS)
check(sc is not None and sc.portal == "staff" and sc.roles == {"staff"},
      "a live token resolves to the scope it was issued for")
check(sc.session_id is not None,
      "carrying its session id, so a route can rotate or revoke itself")
check(AU.resolve(conn, "not-a-token", TS) is None,
      "an unknown token resolves to nothing")
check(AU.resolve(conn, "", TS) is None, "as does an empty one")

_expired = C.TimeService(NOW + AU.SESSION_TTL["staff"] + timedelta(minutes=1))
check(raises(AU.SessionRefusal, AU.resolve, conn, r_staff.token, _expired),
      "AN EXPIRED SESSION IS REFUSED. Staff sessions are the shortest — "
      "eight hours, not the fourteen days a tenant gets")
check(AU.SESSION_TTL["staff"] < AU.SESSION_TTL["tenant"],
      "and that ordering is deliberate: a staff session reaches everyone's "
      "records, a tenant session reaches one person's")


# ── the revocation path ──
before = sql("SELECT privilege_epoch FROM mf_users WHERE id = %s",
             (STAFF,))[0][0]
cur = conn.cursor()
cur.execute("UPDATE mf_users SET privilege_epoch = privilege_epoch + 1 "
            "WHERE id = %s", (STAFF,))
conn.commit()
cur.close()
check(raises(AU.SessionRefusal, AU.resolve, conn, r_staff.token, TS),
      "BUMPING privilege_epoch KILLS EVERY LIVE SESSION ON ITS NEXT "
      "REQUEST. No scan of the session table, no cache to invalidate, and "
      "it works for sessions this process has never seen — which is the "
      "whole reason the epoch is copied onto the session at issue")
check(sql("SELECT privilege_epoch FROM mf_users WHERE id = %s",
          (STAFF,))[0][0] == before + 1, "and the epoch really moved")

r2 = AU.login(conn, "staff@example.invalid", PW, "staff", TS)
conn.commit()
check(AU.resolve(conn, r2.token, TS) is not None,
      "a fresh login after the change works")

AU.set_password(conn, STAFF, "another-long-enough-password", TS)
conn.commit()
check(raises(AU.SessionRefusal, AU.resolve, conn, r2.token, TS),
      "CHANGING A PASSWORD ALSO KILLS LIVE SESSIONS. Whoever changes a "
      "password usually means 'and stop whoever else is in there', and a "
      "system that leaves the intruder logged in has done the opposite")
check(AU.login(conn, "staff@example.invalid",
               "another-long-enough-password", "staff", TS).ok,
      "and the new password works")
conn.commit()


# ── rotation and revocation ──
r3 = AU.login(conn, "tenant@example.invalid", PW, "tenant", TS)
conn.commit()
new_token = AU.rotate(conn, r3.token, TS)
conn.commit()
check(new_token and new_token != r3.token, "rotation issues a new token")
check(AU.resolve(conn, new_token, TS) is not None, "which works")
check(raises(AU.SessionRefusal, AU.resolve, conn, r3.token, TS),
      "and the OLD one is dead — rotation limits how long a token that "
      "leaked into a log or a referrer header stays useful")
check(sql("SELECT rotated_from FROM mf_sessions WHERE token_hash = %s",
          [__import__("hashlib").sha256(new_token.encode()).hexdigest()]
          )[0][0] is not None,
      "the new session records what it replaced, so a request against the "
      "revoked one afterwards reads as theft rather than as an expiry")

check(AU.revoke(conn, new_token, TS), "an explicit logout revokes")
conn.commit()
check(raises(AU.SessionRefusal, AU.resolve, conn, new_token, TS),
      "and the token stops working immediately")
check(not AU.revoke(conn, new_token, TS),
      "revoking twice reports that nothing was live to revoke")

r4 = AU.login(conn, "tenant@example.invalid", PW, "tenant", TS)
r5 = AU.login(conn, "tenant@example.invalid", PW, "tenant", TS)
conn.commit()
_live_before = sql("SELECT COUNT(*) FROM mf_sessions WHERE user_id = %s "
                   "AND revoked_at IS NULL", (TEN,))[0][0]
check(_live_before >= 2, "the tenant has at least the two sessions just made")
check(AU.revoke_all_for_user(conn, TEN, TS) == _live_before,
      "revoke_all closes EVERY live session for a user, however many there "
      "are — counted from the database rather than from what this test "
      "happens to have created, since a stale one left over from an "
      "earlier login is exactly the session somebody wants closed")
conn.commit()
check(sql("SELECT COUNT(*) FROM mf_sessions WHERE user_id = %s "
          "AND revoked_at IS NULL", (TEN,))[0][0] == 0,
      "and none is left live afterwards")
check(raises(AU.SessionRefusal, AU.resolve, conn, r4.token, TS)
      and raises(AU.SessionRefusal, AU.resolve, conn, r5.token, TS),
      "and both are gone — the difference between 'the next request will "
      "be refused' and 'there is no live session'")


# ── audit ──
check(sql("SELECT COUNT(*) FROM mf_audit_log WHERE action = 'login'")[0][0] > 0
      and sql("SELECT COUNT(*) FROM mf_audit_log "
              "WHERE action = 'login_failed'")[0][0] > 0,
      "logins and failures are both in the audit log")
check(sql("SELECT COUNT(*) FROM mf_audit_log WHERE action = 'mfa_failed'"
          )[0][0] > 0, "as are failed MFA attempts")
check(sql("SELECT COUNT(*) FROM mf_audit_log WHERE detail::text LIKE %s",
          ["%" + PW + "%"])[0][0] == 0,
      "AND NO PASSWORD APPEARS ANYWHERE IN THE AUDIT LOG. The log is the "
      "one table that cannot be corrected afterwards, so a credential "
      "written into it is written into it permanently")
check(sql("SELECT COUNT(*) FROM mf_audit_log WHERE detail::text LIKE %s",
          ["%" + secret + "%"])[0][0] == 0, "nor a TOTP secret")

conn.close()
report("   RFC 6238 vectors matched, codes single-use, tenants refused at "
       "the staff door,\n   and a privilege bump killed every live session "
       "without a table scan.")

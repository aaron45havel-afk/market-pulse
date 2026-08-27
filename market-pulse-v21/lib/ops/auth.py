"""Passwords, TOTP, and sessions — on the standard library, deliberately.

requirements.txt has no auth library and this adds none. `hashlib.scrypt`
is a memory-hard KDF built into CPython, and RFC 6238 TOTP is about
fifteen lines of HMAC. The alternative was three new dependencies in the
security-critical path of an application whose other twelve dependencies
are pinned and boring. Fewer moving parts here is worth more than the
convenience.

What this module refuses to do is as much of the design as what it does:

  * A password is never stored, logged, or returned. The hash encodes its
    own parameters so they can be raised later without invalidating
    everyone's credentials.
  * A session TOKEN is never stored. Only its SHA-256 goes in the
    database, so a stolen database dump cannot be replayed as a stolen
    login.
  * A TOTP code is single-use. The accepted counter is recorded on the
    user, because the ±1-step skew window otherwise leaves a code valid
    for ninety seconds after somebody read it over a shoulder.
  * A session carries the user's privilege_epoch. Any role change bumps
    it, and every live session with an older value dies on its next
    request — revocation without a session-store scan.

MFA is required for platform_admin and division_manager, and that
requirement is read from mf_roles.requires_mfa rather than from an
if-statement, so it is a fact in the database that a migration can change.
"""
from __future__ import annotations

import base64
import hashlib
import hmac
import logging
import secrets
import struct
from datetime import timedelta

from lib.ops import audit as A
from lib.ops import clock as C
from lib.ops import scope as S

log = logging.getLogger("mf.auth")

# ── scrypt parameters ──
# n=2**15 is roughly 100ms and 32MB per hash on ordinary hardware: slow
# enough to make offline cracking expensive, fast enough that a login
# does not feel broken. Encoded into every hash so raising them later is
# a one-line change plus a rehash-on-next-login, not a mass reset.
SCRYPT_N = 2 ** 15
SCRYPT_R = 8
SCRYPT_P = 1
SCRYPT_LEN = 32
SALT_BYTES = 16

MIN_PASSWORD_LEN = 12          # length beats composition rules
MAX_PASSWORD_LEN = 1024        # a megabyte password is a denial of service

TOTP_STEP = 30                 # seconds
TOTP_DIGITS = 6
TOTP_SKEW_STEPS = 1            # accept one step either side, no more

SESSION_TTL = {                # by portal; staff sessions are shortest
    "staff": timedelta(hours=8),
    "owner": timedelta(days=7),
    "tenant": timedelta(days=14),
    "vendor": timedelta(days=7),
}
MAX_FAILED_LOGINS = 8
LOCKOUT = timedelta(minutes=15)


class AuthError(Exception):
    """Raised for programmer errors. NEVER for a failed login.

    A failed login returns a result object with a reason the caller may
    log but must not show; raising would tempt a route into leaking which
    half was wrong.
    """


# ══════════════════════════════════════════════════════════════════
# passwords
# ══════════════════════════════════════════════════════════════════

def hash_password(password: str) -> str:
    """scrypt$n$r$p$salt$hash — self-describing, so parameters can rise."""
    if not isinstance(password, str):
        raise AuthError("password must be a string")
    if len(password) < MIN_PASSWORD_LEN:
        raise AuthError(
            f"password must be at least {MIN_PASSWORD_LEN} characters. "
            f"Length is the only property that reliably buys entropy; "
            f"composition rules mostly buy Password1!")
    if len(password) > MAX_PASSWORD_LEN:
        raise AuthError(
            f"password over {MAX_PASSWORD_LEN} characters refused — an "
            f"unbounded input to a deliberately slow function is a denial "
            f"of service with extra steps")
    salt = secrets.token_bytes(SALT_BYTES)
    dk = hashlib.scrypt(password.encode(), salt=salt, n=SCRYPT_N, r=SCRYPT_R,
                        p=SCRYPT_P, dklen=SCRYPT_LEN, maxmem=2 ** 26)
    return "$".join(("scrypt", str(SCRYPT_N), str(SCRYPT_R), str(SCRYPT_P),
                     base64.b64encode(salt).decode(),
                     base64.b64encode(dk).decode()))


def verify_password(password: str, encoded: str | None) -> bool:
    """Constant-time check. False for anything malformed, never an error.

    A user with no password set (encoded is None) verifies False rather
    than raising — an account mid-provisioning must not be a 500, and it
    must not be a way in either.
    """
    if not encoded or not isinstance(password, str):
        return False
    try:
        scheme, n, r, p, salt_b64, hash_b64 = encoded.split("$")
        if scheme != "scrypt":
            return False
        dk = hashlib.scrypt(password.encode(),
                            salt=base64.b64decode(salt_b64),
                            n=int(n), r=int(r), p=int(p),
                            dklen=len(base64.b64decode(hash_b64)),
                            maxmem=2 ** 26)
        return hmac.compare_digest(dk, base64.b64decode(hash_b64))
    except Exception:
        return False


def needs_rehash(encoded: str | None) -> bool:
    """True when the stored hash used weaker parameters than current."""
    if not encoded:
        return False
    try:
        scheme, n, r, p, _, _ = encoded.split("$")
        return (scheme != "scrypt" or int(n) < SCRYPT_N or int(r) < SCRYPT_R
                or int(p) < SCRYPT_P)
    except Exception:
        return True


# ══════════════════════════════════════════════════════════════════
# TOTP (RFC 6238)
# ══════════════════════════════════════════════════════════════════

def new_totp_secret(length: int = 20) -> str:
    """A base32 shared secret. 160 bits, which is what RFC 4226 assumes."""
    return base64.b32encode(secrets.token_bytes(length)).decode().rstrip("=")


def _counter(instant) -> int:
    return int(instant.timestamp()) // TOTP_STEP


def totp_code(secret: str, instant=None, counter: int | None = None) -> str:
    """The code for a moment. HMAC-SHA1 because that is what authenticator
    apps implement; the weakness of SHA-1 as a collision resistant hash is
    not a weakness of HMAC-SHA1 as a MAC here."""
    if counter is None:
        if instant is None:
            raise AuthError("totp_code needs an instant or a counter — it "
                            "must not read the clock itself, or no test can "
                            "pin it")
        counter = _counter(instant)
    pad = "=" * (-len(secret) % 8)
    key = base64.b32decode(secret + pad, casefold=True)
    mac = hmac.new(key, struct.pack(">Q", counter), hashlib.sha1).digest()
    offset = mac[-1] & 0x0F
    truncated = struct.unpack(">I", mac[offset:offset + 4])[0] & 0x7FFFFFFF
    return str(truncated % (10 ** TOTP_DIGITS)).zfill(TOTP_DIGITS)


def verify_totp(secret: str, code: str, instant, last_counter=None):
    """(ok, counter). A code is valid once, then never again.

    Returns the counter it matched so the caller can store it. Passing the
    stored value back as `last_counter` is what makes replay impossible:
    without it, the skew window leaves every code valid for ninety
    seconds, which is ample time to use one somebody else just typed.
    """
    if not secret or not code:
        return False, None
    code = str(code).strip().replace(" ", "")
    if not code.isdigit() or len(code) != TOTP_DIGITS:
        return False, None
    now_counter = _counter(instant)
    for delta in range(-TOTP_SKEW_STEPS, TOTP_SKEW_STEPS + 1):
        c = now_counter + delta
        if last_counter is not None and c <= int(last_counter):
            continue          # already used, or older than one already used
        if hmac.compare_digest(totp_code(secret, counter=c), code):
            return True, c
    return False, None


def provisioning_uri(secret: str, email: str, issuer: str = "Multifamily Ops") -> str:
    """otpauth:// URI for the enrolment QR code."""
    from urllib.parse import quote
    label = quote(f"{issuer}:{email}")
    return (f"otpauth://totp/{label}?secret={secret}&issuer={quote(issuer)}"
            f"&algorithm=SHA1&digits={TOTP_DIGITS}&period={TOTP_STEP}")


# ══════════════════════════════════════════════════════════════════
# sessions
# ══════════════════════════════════════════════════════════════════

def _token_hash(token: str) -> str:
    return hashlib.sha256(token.encode()).hexdigest()


class LoginResult:
    """Outcome of an attempt. `reason` is for the log, never for the user.

    "No such account" and "wrong password" must be the same message on
    screen — the difference tells an attacker which addresses are worth
    attacking. They are recorded separately in the audit log, where the
    audience is the operator.
    """

    def __init__(self, ok, token=None, user_id=None, reason="",
                 mfa_required=False, session_id=None):
        self.ok = ok
        self.token = token
        self.user_id = user_id
        self.reason = reason
        self.mfa_required = mfa_required
        self.session_id = session_id

    def __bool__(self):
        return bool(self.ok)

    def __repr__(self):
        # Never repr the token. A LoginResult in a traceback or a debug
        # log would otherwise hand over a live session.
        return (f"<LoginResult ok={self.ok} user={self.user_id} "
                f"mfa_required={self.mfa_required} reason={self.reason!r}>")


def roles_for(conn, user_id: int) -> tuple:
    """Live role grants for a user, as scope.RoleGrant objects."""
    cur = conn.cursor()
    cur.execute(
        "SELECT r.key, ur.division_id, ur.property_id "
        "FROM mf_user_roles ur JOIN mf_roles r ON r.id = ur.role_id "
        "WHERE ur.user_id = %s AND ur.revoked_at IS NULL", (user_id,))
    grants = tuple(S.RoleGrant(k, d, p) for k, d, p in cur.fetchall())
    cur.close()
    return grants


def mfa_required_for(conn, user_id: int) -> bool:
    """Does any role this user holds require MFA?

    Read from mf_roles.requires_mfa, not from a hard-coded set. The
    requirement is a fact about a role, and a migration can change it
    without anyone hunting for the if-statement.
    """
    cur = conn.cursor()
    cur.execute(
        "SELECT COALESCE(BOOL_OR(r.requires_mfa), FALSE) "
        "FROM mf_user_roles ur JOIN mf_roles r ON r.id = ur.role_id "
        "WHERE ur.user_id = %s AND ur.revoked_at IS NULL", (user_id,))
    out = bool(cur.fetchone()[0])
    cur.close()
    return out


def login(conn, email: str, password: str, portal: str, ts: C.TimeService,
          ip=None, user_agent=None, request_id=None) -> LoginResult:
    """Authenticate for one portal. Does not commit.

    The portal is part of the credential check, not a preference applied
    afterwards: a tenant who knows a staff member's password still cannot
    open a staff session, because the staff portal admits no tenant
    grants and a session with no effective grants is refused at issue.
    """
    if portal not in S.PORTALS:
        raise AuthError(f"unknown portal {portal!r}")
    now = ts.now()

    cur = conn.cursor()
    cur.execute(
        "SELECT id, password_hash, is_active, privilege_epoch, locked_until, "
        "failed_login_count FROM mf_users "
        "WHERE lower(email) = lower(%s) AND archived_at IS NULL", (email,))
    row = cur.fetchone()
    cur.close()

    if not row:
        # Still hash something. Returning instantly for an unknown address
        # and slowly for a known one turns login timing into an account
        # enumeration oracle.
        verify_password(password, hash_password("x" * MIN_PASSWORD_LEN))
        A.record(conn, action="login_failed", target_type="mf_users",
                 actor_label=email, detail={"reason": "no such account",
                                            "portal": portal},
                 ip=ip, request_id=request_id)
        return LoginResult(False, reason="no such account")

    uid, pw_hash, active, epoch, locked_until, failures = row

    if locked_until and locked_until > now:
        A.record(conn, action="login_failed", target_type="mf_users",
                 target_id=uid, actor_user_id=uid,
                 detail={"reason": "locked", "portal": portal}, ip=ip,
                 request_id=request_id)
        return LoginResult(False, reason="locked out")

    if not active:
        A.record(conn, action="login_failed", target_type="mf_users",
                 target_id=uid, actor_user_id=uid,
                 detail={"reason": "inactive", "portal": portal}, ip=ip,
                 request_id=request_id)
        return LoginResult(False, reason="inactive")

    if not verify_password(password, pw_hash):
        failures = (failures or 0) + 1
        cur = conn.cursor()
        if failures >= MAX_FAILED_LOGINS:
            cur.execute("UPDATE mf_users SET failed_login_count = %s, "
                        "locked_until = %s WHERE id = %s",
                        (failures, now + LOCKOUT, uid))
        else:
            cur.execute("UPDATE mf_users SET failed_login_count = %s "
                        "WHERE id = %s", (failures, uid))
        cur.close()
        A.record(conn, action="login_failed", target_type="mf_users",
                 target_id=uid, actor_user_id=uid,
                 detail={"reason": "bad password", "portal": portal,
                         "failures": failures}, ip=ip, request_id=request_id)
        return LoginResult(False, reason="bad password")

    grants = roles_for(conn, uid)
    probe = S.Scope(user_id=uid, organization_id=0, portal=portal,
                    grants=grants)
    if not probe.effective:
        # Correct credentials, wrong door. Logged as a refusal because it
        # is one — and because a burst of these is somebody walking a
        # valid password along all four portals.
        A.record(conn, action="denied", target_type="mf_users", target_id=uid,
                 actor_user_id=uid,
                 detail={"reason": "no role for portal", "portal": portal,
                         "holds": sorted(g.role for g in grants)},
                 ip=ip, request_id=request_id)
        return LoginResult(False, reason="no role for this portal")

    needs_mfa = mfa_required_for(conn, uid)
    token, session_id = issue_session(
        conn, uid, portal, ts, mfa_satisfied=not needs_mfa, ip=ip,
        user_agent=user_agent, privilege_epoch=epoch)

    cur = conn.cursor()
    cur.execute("UPDATE mf_users SET last_login_at = %s, "
                "failed_login_count = 0, locked_until = NULL WHERE id = %s",
                (now, uid))
    cur.close()

    A.record(conn, action="login", target_type="mf_users", target_id=uid,
             actor_user_id=uid,
             detail={"portal": portal, "mfa_required": needs_mfa},
             ip=ip, request_id=request_id)
    return LoginResult(True, token=token, user_id=uid,
                       mfa_required=needs_mfa, session_id=session_id)


def issue_session(conn, user_id, portal, ts: C.TimeService,
                  mfa_satisfied=False, ip=None, user_agent=None,
                  privilege_epoch=None, rotated_from=None):
    """Create a session. Returns (token, session_id); stores only the hash."""
    if portal not in S.PORTALS:
        raise AuthError(f"unknown portal {portal!r}")
    now = ts.now()
    if privilege_epoch is None:
        cur = conn.cursor()
        cur.execute("SELECT privilege_epoch FROM mf_users WHERE id = %s",
                    (user_id,))
        r = cur.fetchone()
        cur.close()
        if not r:
            raise AuthError(f"no such user {user_id}")
        privilege_epoch = r[0]

    token = secrets.token_urlsafe(32)
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO mf_sessions (user_id, token_hash, portal, "
        "privilege_epoch, mfa_satisfied, issued_at, expires_at, ip, "
        "user_agent, last_seen_at, rotated_from) "
        "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) RETURNING id",
        (user_id, _token_hash(token), portal, privilege_epoch, mfa_satisfied,
         now, now + SESSION_TTL[portal], ip, user_agent, now, rotated_from))
    session_id = cur.fetchone()[0]
    cur.close()
    return token, session_id


def satisfy_mfa(conn, session_id, user_id, code, ts: C.TimeService,
                request_id=None) -> bool:
    """Check a TOTP code and mark the session MFA-satisfied. Single-use."""
    now = ts.now()
    cur = conn.cursor()
    cur.execute("SELECT mfa_secret, mfa_last_counter FROM mf_users "
                "WHERE id = %s", (user_id,))
    row = cur.fetchone()
    cur.close()
    if not row or not row[0]:
        A.record(conn, action="mfa_failed", target_type="mf_users",
                 target_id=user_id, actor_user_id=user_id,
                 detail={"reason": "not enrolled"}, request_id=request_id)
        return False

    ok, counter = verify_totp(row[0], code, now, last_counter=row[1])
    if not ok:
        A.record(conn, action="mfa_failed", target_type="mf_users",
                 target_id=user_id, actor_user_id=user_id,
                 detail={"reason": "bad or reused code"},
                 request_id=request_id)
        return False

    cur = conn.cursor()
    # Recording the counter is what makes the code single-use. Done in the
    # same statement-batch as marking the session, so a code cannot be
    # spent without the session being marked or vice versa.
    cur.execute("UPDATE mf_users SET mfa_last_counter = %s WHERE id = %s",
                (counter, user_id))
    cur.execute("UPDATE mf_sessions SET mfa_satisfied = TRUE WHERE id = %s "
                "AND user_id = %s", (session_id, user_id))
    touched = cur.rowcount
    cur.close()
    if not touched:
        return False
    A.record(conn, action="login", target_type="mf_users", target_id=user_id,
             actor_user_id=user_id, detail={"step": "mfa satisfied"},
             request_id=request_id)
    return True


def pending_session(conn, token: str):
    """(session_id, user_id) for a live session that has not passed MFA.

    resolve() deliberately refuses such a session — that is exactly what
    "MFA not satisfied" means — so the second-factor step needs a way to
    identify it without being granted one. Returns identifiers only: no
    roles, no scope, nothing a caller could mistake for authentication.

    It lives here rather than in the router because lib/ops/auth.py is
    the only module outside the repository allowed to query mf_sessions,
    and tests/test_ops_schema.py enforces that by scanning the tree.
    """
    if not token:
        return None
    cur = conn.cursor()
    cur.execute("SELECT id, user_id FROM mf_sessions "
                "WHERE token_hash = %s AND revoked_at IS NULL",
                (_token_hash(token),))
    row = cur.fetchone()
    cur.close()
    return (row[0], row[1]) if row else None


class SessionRefusal(Exception):
    """Why a token did not resolve. Carries a reason for the log only."""

    def __init__(self, reason):
        super().__init__(reason)
        self.reason = reason


def resolve(conn, token: str, ts: C.TimeService, organization_id=None):
    """Token -> Scope, or None. Every refusal path is a separate check.

    Order matters only in that each condition is checked explicitly; none
    is inferred from another. A session is live when it exists, has not
    expired, has not been revoked, satisfies MFA if its user's roles
    require it, and carries the user's CURRENT privilege_epoch.
    """
    if not token:
        return None
    cur = conn.cursor()
    cur.execute(
        "SELECT s.id, s.user_id, s.portal, s.privilege_epoch, "
        "       s.mfa_satisfied, s.expires_at, s.revoked_at, "
        "       u.privilege_epoch, u.is_active, u.organization_id "
        "FROM mf_sessions s JOIN mf_users u ON u.id = s.user_id "
        "WHERE s.token_hash = %s", (_token_hash(token),))
    row = cur.fetchone()
    cur.close()
    if not row:
        return None

    (sid, uid, portal, sess_epoch, mfa_ok, expires_at, revoked_at,
     user_epoch, active, org_id) = row
    now = ts.now()

    if revoked_at is not None:
        raise SessionRefusal("revoked")
    if expires_at <= now:
        raise SessionRefusal("expired")
    if not active:
        raise SessionRefusal("user deactivated")
    if sess_epoch != user_epoch:
        # THE REVOCATION PATH. A role grant bumped the user's epoch, so
        # every session issued before it is now stale — no scan of the
        # session table, no cache to invalidate, and it works for sessions
        # this process has never seen.
        raise SessionRefusal("privileges changed since this session was "
                             "issued")
    if not mfa_ok and mfa_required_for(conn, uid):
        raise SessionRefusal("MFA not satisfied")

    grants = roles_for(conn, uid)
    scope = S.Scope(user_id=uid,
                    organization_id=organization_id or org_id,
                    portal=portal, grants=grants, privilege_epoch=user_epoch,
                    session_id=sid)
    if not scope.effective:
        # Roles were revoked after issue and the epoch bump was missed, or
        # the portal no longer admits anything this user holds. Either way
        # the session is not usable, and returning it would hand a route a
        # scope that silently denies everything.
        raise SessionRefusal("no role admitted by this portal")

    cur = conn.cursor()
    cur.execute("UPDATE mf_sessions SET last_seen_at = %s WHERE id = %s",
                (now, sid))
    cur.close()
    return scope


def rotate(conn, token: str, ts: C.TimeService):
    """Issue a fresh token and revoke the old one. Returns the new token.

    Called on privilege change and after MFA. Rotation limits how long a
    token that leaked into a log or a referrer header stays useful, and
    it leaves the old session revoked with its successor recorded — a
    request against the revoked one afterwards is the signature of theft
    rather than of an ordinary expiry.
    """
    cur = conn.cursor()
    cur.execute("SELECT id, user_id, portal, mfa_satisfied, ip, user_agent "
                "FROM mf_sessions WHERE token_hash = %s AND revoked_at IS NULL",
                (_token_hash(token),))
    row = cur.fetchone()
    cur.close()
    if not row:
        return None
    sid, uid, portal, mfa_ok, ip, ua = row
    new_token, _ = issue_session(conn, uid, portal, ts, mfa_satisfied=mfa_ok,
                                 ip=ip, user_agent=ua, rotated_from=sid)
    cur = conn.cursor()
    cur.execute("UPDATE mf_sessions SET revoked_at = %s, "
                "revoked_reason = 'rotated' WHERE id = %s", (ts.now(), sid))
    cur.close()
    return new_token


def revoke(conn, token: str, ts: C.TimeService, reason="logout") -> bool:
    cur = conn.cursor()
    cur.execute("UPDATE mf_sessions SET revoked_at = %s, revoked_reason = %s "
                "WHERE token_hash = %s AND revoked_at IS NULL",
                (ts.now(), reason, _token_hash(token)))
    touched = cur.rowcount
    cur.close()
    return bool(touched)


def revoke_all_for_user(conn, user_id: int, ts: C.TimeService,
                        reason="privilege change") -> int:
    """Kill every live session for a user, now rather than on next request.

    The privilege_epoch check already makes them fail, so this is belt
    and braces — but it is the difference between "the next request will
    be refused" and "there is no live session", and the second is what
    somebody means when they say revoke access.
    """
    cur = conn.cursor()
    cur.execute("UPDATE mf_sessions SET revoked_at = %s, revoked_reason = %s "
                "WHERE user_id = %s AND revoked_at IS NULL",
                (ts.now(), reason, user_id))
    n = cur.rowcount
    cur.close()
    return n


def set_password(conn, user_id: int, password: str, ts: C.TimeService,
                 actor_user_id=None, request_id=None) -> None:
    """The dedicated credential path repository.update() refuses to be.

    Hashes, audits, bumps privilege_epoch, and kills live sessions — the
    four things a generic column write would each skip in silence.
    """
    encoded = hash_password(password)
    cur = conn.cursor()
    cur.execute("UPDATE mf_users SET password_hash = %s, "
                "privilege_epoch = privilege_epoch + 1 WHERE id = %s",
                (encoded, user_id))
    if not cur.rowcount:
        cur.close()
        raise AuthError(f"no such user {user_id}")
    cur.close()
    revoke_all_for_user(conn, user_id, ts, reason="password changed")
    A.record(conn, action="privilege_change", target_type="mf_users",
             target_id=user_id, actor_user_id=actor_user_id or user_id,
             detail={"change": "password set"}, request_id=request_id)


def enroll_mfa(conn, user_id: int, ts: C.TimeService, request_id=None) -> str:
    """Generate and store a TOTP secret. Returns it ONCE, for the QR code.

    The secret is never readable afterwards — scope.NEVER_SELECT bans it
    and the repository expands columns from a ban-listed set. Losing the
    enrolment means re-enrolling, which is correct: a secret that can be
    read back is a secret that can be exfiltrated.
    """
    secret = new_totp_secret()
    cur = conn.cursor()
    cur.execute("UPDATE mf_users SET mfa_secret = %s, mfa_enrolled_at = %s, "
                "mfa_last_counter = NULL WHERE id = %s",
                (secret, ts.now(), user_id))
    if not cur.rowcount:
        cur.close()
        raise AuthError(f"no such user {user_id}")
    cur.close()
    A.record(conn, action="mfa_enrolled", target_type="mf_users",
             target_id=user_id, actor_user_id=user_id, request_id=request_id)
    return secret

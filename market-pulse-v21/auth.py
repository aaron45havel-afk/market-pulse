"""Google OAuth + signed-cookie sessions.

Two access tiers:
  admin → full app (legacy ADMIN_TOKEN path still works)
  sales → /pipeline only (Jim + future sales team via Google sign-in)

Env vars:
  GOOGLE_CLIENT_ID, GOOGLE_CLIENT_SECRET — from Google Cloud Console
  SESSION_SECRET — HMAC key for signing session cookies (any random
                   long string — generate with secrets.token_urlsafe(48))
  ADMIN_EMAILS   — comma-separated emails that get admin role on
                   Google sign-in (e.g. aaron@focusedops.io)
  SALES_EMAILS   — comma-separated emails that get sales role
                   (e.g. jim@focusedops.io)

The legacy ADMIN_TOKEN env var continues to work — anyone hitting
/admin/login?token=<...> still gets full admin access.

EMAIL SIGN-IN LINKS do not depend on Google at all. The Google OAuth app
is a Workspace "Internal" app, so Google refuses every account outside
that organisation (Error 403: org_internal) before this server is asked —
the owner's personal Gmail included. A one-time link mailed to an address
on ADMIN_EMAILS / SALES_EMAILS signs that address in the same way.
"""
from __future__ import annotations

import base64
import hashlib
import hmac
import json
import os
import secrets
import threading
import time
import urllib.parse
import urllib.request


SESSION_COOKIE       = "mp_session"
OAUTH_STATE_COOKIE   = "mp_oauth_state"
OAUTH_REDIRECT_COOKIE = "mp_oauth_redirect"


def _session_secret() -> bytes:
    return os.environ.get("SESSION_SECRET", "").encode("utf-8")


def make_session(email: str, role: str, ttl_days: int = 30) -> str:
    """Returns a base64url-encoded `payload.sig` session token.
    Returns "" if SESSION_SECRET isn't configured."""
    secret = _session_secret()
    if not secret:
        return ""
    payload = json.dumps(
        {"email": email, "role": role, "exp": int(time.time()) + ttl_days * 86400},
        separators=(",", ":"),
    ).encode("utf-8")
    sig = hmac.new(secret, payload, hashlib.sha256).digest()
    raw = base64.urlsafe_b64encode(payload).decode("ascii").rstrip("=") \
        + "." + base64.urlsafe_b64encode(sig).decode("ascii").rstrip("=")
    return raw


def verify_session(token: str) -> dict | None:
    """Returns the session dict {email, role, exp} if valid + unexpired,
    else None. Constant-time HMAC compare."""
    if not token or "." not in token:
        return None
    secret = _session_secret()
    if not secret:
        return None
    try:
        payload_b64, sig_b64 = token.split(".", 1)
        payload = base64.urlsafe_b64decode(payload_b64 + "=" * (-len(payload_b64) % 4))
        sig     = base64.urlsafe_b64decode(sig_b64     + "=" * (-len(sig_b64)     % 4))
        expected = hmac.new(secret, payload, hashlib.sha256).digest()
        if not hmac.compare_digest(sig, expected):
            return None
        data = json.loads(payload)
        if int(data.get("exp", 0)) < int(time.time()):
            return None
        return data
    except (ValueError, json.JSONDecodeError):
        return None


def role_for_email(email: str) -> str | None:
    """Returns 'admin', 'sales', or None for the given Google email."""
    e = (email or "").strip().lower()
    if not e:
        return None
    admins = {x.strip().lower() for x in os.environ.get("ADMIN_EMAILS", "").split(",") if x.strip()}
    sales  = {x.strip().lower() for x in os.environ.get("SALES_EMAILS", "").split(",") if x.strip()}
    if e in admins:
        return "admin"
    if e in sales:
        return "sales"
    return None


def google_oauth_redirect(callback_url: str, state: str) -> str:
    """Build the Google authorization URL. Returns "" if
    GOOGLE_CLIENT_ID is unset."""
    client_id = os.environ.get("GOOGLE_CLIENT_ID", "").strip()
    if not client_id:
        return ""
    params = {
        "client_id":     client_id,
        "redirect_uri":  callback_url,
        "response_type": "code",
        "scope":         "openid email profile",
        "state":         state,
        "access_type":   "online",
        "prompt":        "select_account",
    }
    return "https://accounts.google.com/o/oauth2/v2/auth?" + urllib.parse.urlencode(params)


def google_exchange_code(code: str, callback_url: str) -> dict | None:
    """Exchange the authorization code for tokens. Returns the token
    dict (with access_token) or None on failure."""
    client_id     = os.environ.get("GOOGLE_CLIENT_ID", "").strip()
    client_secret = os.environ.get("GOOGLE_CLIENT_SECRET", "").strip()
    if not client_id or not client_secret:
        return None
    body = urllib.parse.urlencode({
        "code":          code,
        "client_id":     client_id,
        "client_secret": client_secret,
        "redirect_uri":  callback_url,
        "grant_type":    "authorization_code",
    }).encode("utf-8")
    req = urllib.request.Request(
        "https://oauth2.googleapis.com/token",
        data=body,
        headers={
            "Content-Type": "application/x-www-form-urlencoded",
            "Accept":       "application/json",
            "User-Agent":   "market-pulse/1.0 (focusedops.io)",
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=15) as r:
            return json.loads(r.read())
    except Exception:
        return None


def google_fetch_userinfo(access_token: str) -> dict | None:
    """Fetch the authenticated user's profile (email, name, picture)."""
    req = urllib.request.Request(
        "https://www.googleapis.com/oauth2/v2/userinfo",
        headers={
            "Authorization": f"Bearer {access_token}",
            "Accept":        "application/json",
            "User-Agent":    "market-pulse/1.0 (focusedops.io)",
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=15) as r:
            return json.loads(r.read())
    except Exception:
        return None


def new_state() -> str:
    """CSRF state token for the OAuth round-trip."""
    return secrets.token_urlsafe(24)


# ── one-time email sign-in links ─────────────────────────────────────
EMAIL_LINK_TTL = 15 * 60
# Signed under its own context, so a link token can never pass as a session
# cookie and a session cookie can never pass as a link.
_EMAIL_LINK_CONTEXT = b"market-pulse email-link v1\x00"
_USED_EMAIL_LINKS: dict[str, int] = {}
_EMAIL_LINK_LOCK = threading.Lock()


def _b64(b: bytes) -> str:
    return base64.urlsafe_b64encode(b).decode("ascii").rstrip("=")


def _unb64(s: str) -> bytes:
    return base64.urlsafe_b64decode(s + "=" * (-len(s) % 4))


def make_email_link(email: str, ttl: int = EMAIL_LINK_TTL, now: float | None = None) -> str:
    """A signed, expiring, single-use sign-in token for one address. "" when
    SESSION_SECRET is not configured."""
    secret = _session_secret()
    if not secret or not email:
        return ""
    t = int(now if now is not None else time.time())
    payload = json.dumps({"k": "email-link", "email": email.strip().lower(),
                          "exp": t + int(ttl), "n": secrets.token_urlsafe(12)},
                         separators=(",", ":")).encode("utf-8")
    sig = hmac.new(secret, _EMAIL_LINK_CONTEXT + payload, hashlib.sha256).digest()
    return _b64(payload) + "." + _b64(sig)


def read_email_link(token: str, now: float | None = None) -> dict | None:
    """{email, exp, n} when the token is genuine, unexpired and unused. Does
    not use it up — the confirm page reads it, only the button consumes it,
    because mail scanners open links on their own."""
    secret = _session_secret()
    if not secret or not token or "." not in token:
        return None
    try:
        p64, s64 = token.split(".", 1)
        payload, sig = _unb64(p64), _unb64(s64)
        expected = hmac.new(secret, _EMAIL_LINK_CONTEXT + payload, hashlib.sha256).digest()
        if not hmac.compare_digest(sig, expected):
            return None
        data = json.loads(payload)
    except (ValueError, json.JSONDecodeError):
        return None
    t = int(now if now is not None else time.time())
    if (not isinstance(data, dict) or data.get("k") != "email-link"
            or int(data.get("exp", 0)) < t or not data.get("email")):
        return None
    with _EMAIL_LINK_LOCK:
        if data.get("n") in _USED_EMAIL_LINKS:
            return None
    return data


def consume_email_link(token: str, now: float | None = None) -> str | None:
    """The address the link signs in, once. A second use, or a use after it
    expires, returns None."""
    data = read_email_link(token, now)
    if not data:
        return None
    t = int(now if now is not None else time.time())
    with _EMAIL_LINK_LOCK:
        for n, exp in list(_USED_EMAIL_LINKS.items()):
            if exp < t:
                del _USED_EMAIL_LINKS[n]
        if data["n"] in _USED_EMAIL_LINKS:
            return None
        _USED_EMAIL_LINKS[data["n"]] = int(data["exp"])
    return data["email"]

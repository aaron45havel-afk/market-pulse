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
"""
from __future__ import annotations

import base64
import hashlib
import hmac
import json
import os
import secrets
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

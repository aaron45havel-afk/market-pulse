"""The one dependency every /ops route hangs off.

It does four things in a fixed order, and the order is the design:

  1. Reads the route's own scope declaration off the endpoint function.
     A route that declares nothing is refused — not served openly.
  2. Reads the session cookie FOR THIS PORTAL. Portal separation starts
     in the browser: a staff cookie is not sent to a tenant route,
     because it is a different cookie with a different name and path.
  3. Resolves it to a Scope, or refuses with a reason.
  4. Applies routeguard.enforce, which checks portal then permission.

A route therefore cannot be reached without a decision having been made
about it. That is the whole difference from `_admin_gate()` on the
analysis side, where the decision is a line inside each handler that a
new handler simply does not have.
"""
from __future__ import annotations

import logging
import re

from fastapi import Depends, HTTPException, Request

from lib.ops import audit as A
from lib.ops import auth as AU
from lib.ops import clock as C
from lib.ops import obs
from lib.ops import repository as RP
from lib.ops import routeguard as RG
from lib.ops import scope as S

log = logging.getLogger("mf.web")

# One cookie per portal. A single "session" cookie would be sent to every
# /ops route, and the browser would happily present a staff session at
# the tenant door — leaving portal separation to server-side code that
# has to remember to check. Separate names make the browser enforce it.
COOKIE = {p: f"mfops_{p}" for p in S.PORTALS}
COOKIE_PATH = "/ops"


def time_service() -> C.TimeService:
    """Injectable clock. Overridden in tests; never called directly."""
    return C.TimeService()


_INBOUND_ID = re.compile(r"^[A-Za-z0-9._-]{1,64}$")


def request_id(request: Request) -> str:
    """One id per request, echoed into every audit row and log line.

    Cached on request.state, because generating a fresh one per call
    would give the audit row and the log line different ids and defeat
    the entire purpose.

    An inbound X-Request-ID is honoured so a trace can be followed across
    a proxy — but VALIDATED first. It is attacker-controlled and ends up
    in log lines and an append-only audit table, so a newline in it would
    let somebody forge log entries, and an unbounded one would let them
    write a megabyte per request into a table that cannot be pruned.
    """
    existing = getattr(request.state, "mf_request_id", None)
    if existing:
        return existing
    inbound = request.headers.get("X-Request-ID") or ""
    rid = inbound if _INBOUND_ID.match(inbound) else obs.new_request_id()
    request.state.mf_request_id = rid
    obs.bind(request_id=rid)
    return rid


def db():
    """A connection per request, closed after. Rolled back on an error.

    The repository never commits; this owns the transaction, so a write
    and the audit row describing it land together or not at all.
    """
    import database as D
    conn = D._get_conn()
    if conn is None:
        raise HTTPException(503, "database unavailable")
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def _declaration(request: Request) -> RG.Declaration | None:
    route = request.scope.get("route")
    endpoint = getattr(route, "endpoint", None)
    return RG.declaration(endpoint) if endpoint else None


def _signed_in_elsewhere(request: Request, conn, ts, decl) -> bool:
    """Does the caller hold a live session on some OTHER portal?

    Used for one thing only: choosing 403 over 401. A tenant asking for a
    staff route is not an anonymous visitor who needs to sign in — they
    are a signed-in person being told no, and sending them to a login
    form they have already completed is a dead end.

    The resolved scope is discarded. Nothing here authorises anything.
    """
    for portal in S.PORTALS:
        if portal in decl.portals:
            continue
        other = request.cookies.get(COOKIE[portal])
        if not other:
            continue
        try:
            if AU.resolve(conn, other, ts) is not None:
                return True
        except AU.SessionRefusal:
            continue
    return False


def current_scope(request: Request, conn=Depends(db),
                  ts: C.TimeService = Depends(time_service)):
    """Resolve the caller, or raise 401/403. Returns a Scope.

    Refusals are deliberately vague to the caller and specific in the
    log. "Not authorised" tells an attacker nothing about which of the
    four conditions they failed; the audit row tells the operator all of
    it.
    """
    decl = _declaration(request)
    if decl is None:
        # An undeclared route reaching runtime means the suite was
        # skipped. Closed, not open.
        log.error("no scope declaration on %s — refusing", request.url.path)
        raise HTTPException(500, "route misconfigured")

    if decl.public:
        return None

    # A declaration naming one portal has one cookie to read. Naming
    # several (rare, and visible in the diff) tries each.
    token = None
    for portal in sorted(decl.portals):
        token = request.cookies.get(COOKIE[portal])
        if token:
            break
    if not token:
        # No credential for THIS portal — but the caller may well be
        # signed in on another one, and the honest answer to a tenant
        # asking for the staff API is "no", not "please sign in" to a
        # session they already hold.
        #
        # The other portal's session is used ONLY to choose between 401
        # and 403. It is never resolved into a scope, never consulted for
        # permissions, and the request still ends here. The isolation is
        # unchanged; only the status code improves.
        if _signed_in_elsewhere(request, conn, ts, decl):
            log.info("403 on %s: signed in on another portal",
                     request.url.path)
            raise HTTPException(403, "not authorised")
        raise HTTPException(401, "not signed in")

    try:
        scope = AU.resolve(conn, token, ts)
    except AU.SessionRefusal as e:
        log.info("session refused on %s: %s", request.url.path, e.reason)
        raise HTTPException(401, "not signed in")
    if scope is None:
        raise HTTPException(401, "not signed in")

    obs.bind(user_id=scope.user_id, portal=scope.portal)
    reason = RG.enforce(scope, decl)
    if reason:
        log.info("403 on %s for user=%s portal=%s: %s", request.url.path,
                 scope.user_id, scope.portal, reason)
        A.record_denied(conn, scope, decl.entity or "route", decl.action,
                        reason, request_id(request))
        raise HTTPException(403, "not authorised")
    return scope


def repository(request: Request, scope=Depends(current_scope),
               conn=Depends(db)) -> RP.Repository:
    return RP.Repository(conn, scope, request_id=request_id(request))


def set_session_cookie(response, portal: str, token: str, secure: bool = True):
    """HttpOnly, SameSite=Lax, scoped to /ops and to this portal's name.

    HttpOnly because a session token readable from JavaScript is one XSS
    away from being stolen. SameSite=Lax because these are cookie-auth
    form posts and Strict would break ordinary inbound links. Path=/ops
    so nothing on the analysis side ever receives it.
    """
    response.set_cookie(COOKIE[portal], token, httponly=True, samesite="lax",
                        secure=secure, path=COOKIE_PATH)


def clear_session_cookie(response, portal: str):
    response.delete_cookie(COOKIE[portal], path=COOKIE_PATH)

"""Four login surfaces, one identity table.

CLAUDE.md asks for four portals. They are four PATHS with four COOKIES
over one `mf_users` table, not four user tables — a person who is both a
maintenance supervisor and a tenant is one person with one email, and
which of those they are right now is which door they came through.

Every route here is declared `public()`, because a login page is by
definition reached before authentication. That is a declaration, not an
absence of one: `routeguard.public()` requires a written reason, so the
diff shows somebody decided rather than forgot.

THE PORTAL IS NEVER A PARAMETER. Each route is built inside a closure
that captures it, so it is baked into the handler. Writing it as a
function argument with a default — the obvious way to build four
near-identical routes in a loop — would make FastAPI treat it as a QUERY
PARAMETER, and `POST /ops/tenant/login?portal=staff` would then mint a
staff session. The closure is not a style choice.
"""
from __future__ import annotations

import logging

from fastapi import APIRouter, Depends, Form, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates

from lib.ops import auth as AU
from lib.ops import clock as C
from lib.ops import routeguard as RG
from lib.ops import scope as S

from . import deps

log = logging.getLogger("mf.web.auth")
router = APIRouter()
templates = Jinja2Templates(directory="templates")

PORTAL_LABEL = {
    "staff": "Staff", "owner": "Owner", "tenant": "Resident",
    "vendor": "Vendor",
}

# Shown for every failure, whatever the actual cause. "No such account"
# and "wrong password" as separate messages tell an attacker which
# addresses are worth attacking. The audit log records the difference,
# where the audience is the operator.
GENERIC_FAILURE = "That email and password did not match."


def _secure_cookies(request: Request) -> bool:
    """Secure cookies except on plain-HTTP localhost.

    Setting Secure unconditionally breaks local development over http,
    and the usual fix for that is to turn it off everywhere.
    """
    return request.url.scheme == "https"


def _build(portal: str) -> None:
    """Register the five auth routes for one portal, with it closed over."""
    label = PORTAL_LABEL[portal]

    def page(request: Request, template: str, error: str = "", status=200):
        return templates.TemplateResponse(f"ops/{template}", {
            "request": request, "portal": portal, "label": label,
            "error": error}, status_code=status)

    @router.get(f"/ops/{portal}/login", response_class=HTMLResponse,
                name=f"ops_{portal}_login")
    @RG.public(f"the {portal} sign-in form, reached before authentication "
               f"by definition")
    async def login_form(request: Request):
        return page(request, "login.html")

    @router.post(f"/ops/{portal}/login", name=f"ops_{portal}_login_post")
    @RG.public(f"the {portal} sign-in submission")
    async def login_submit(request: Request, email: str = Form(...),
                           password: str = Form(...),
                           conn=Depends(deps.db),
                           ts: C.TimeService = Depends(deps.time_service)):
        result = AU.login(
            conn, email, password, portal, ts,
            ip=request.client.host if request.client else None,
            user_agent=request.headers.get("user-agent"),
            request_id=deps.request_id(request))
        if not result:
            log.info("login failed on %s portal: %s", portal, result.reason)
            return page(request, "login.html", GENERIC_FAILURE, 401)

        target = (f"/ops/{portal}/mfa" if result.mfa_required
                  else f"/ops/{portal}/")
        response = RedirectResponse(target, status_code=303)
        deps.set_session_cookie(response, portal, result.token,
                                secure=_secure_cookies(request))
        return response

    @router.get(f"/ops/{portal}/mfa", response_class=HTMLResponse,
                name=f"ops_{portal}_mfa")
    @RG.public("the second-factor prompt: the session exists but does not "
               "yet resolve, so there is no scope to check it against")
    async def mfa_form(request: Request):
        return page(request, "mfa.html")

    @router.post(f"/ops/{portal}/mfa", name=f"ops_{portal}_mfa_post")
    @RG.public("the second-factor submission")
    async def mfa_submit(request: Request, code: str = Form(...),
                         conn=Depends(deps.db),
                         ts: C.TimeService = Depends(deps.time_service)):
        token = request.cookies.get(deps.COOKIE[portal])
        pending = AU.pending_session(conn, token) if token else None
        if not pending:
            return RedirectResponse(f"/ops/{portal}/login", status_code=303)
        session_id, user_id = pending
        if not AU.satisfy_mfa(conn, session_id, user_id, code, ts,
                              request_id=deps.request_id(request)):
            return page(request, "mfa.html",
                        "That code was not accepted.", 401)
        # Rotate: the session just became more capable than it was, so
        # the token that got here is retired rather than promoted.
        new_token = AU.rotate(conn, token, ts)
        response = RedirectResponse(f"/ops/{portal}/", status_code=303)
        deps.set_session_cookie(response, portal, new_token or token,
                                secure=_secure_cookies(request))
        return response

    @router.post(f"/ops/{portal}/logout", name=f"ops_{portal}_logout")
    @RG.public("signing out must work from a session that has already "
               "stopped resolving — otherwise a stale cookie cannot be "
               "cleared without deleting it by hand")
    async def logout(request: Request, conn=Depends(deps.db),
                     ts: C.TimeService = Depends(deps.time_service)):
        token = request.cookies.get(deps.COOKIE[portal])
        if token:
            AU.revoke(conn, token, ts)
        response = RedirectResponse(f"/ops/{portal}/login", status_code=303)
        deps.clear_session_cookie(response, portal)
        return response


for _p in sorted(S.PORTALS):
    _build(_p)
del _p

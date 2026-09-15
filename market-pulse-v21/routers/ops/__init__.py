"""The ops surface, mounted into the existing app with two lines.

ARCHITECTURE.md §3: "market-pulse-v21/main.py gains one include_router
call per portal and nothing else." One router covering all of them is
fewer lines still, and keeps the ops routes discoverable in one place
for the enumeration guard in tests/test_ops_schema.py.
"""
import logging
import os

from fastapi import APIRouter

from lib.ops import obs

from . import auth_routes, deps, portals

router = APIRouter()
router.include_router(auth_routes.router)
router.include_router(portals.router)

# JSON logs for mf.* only. Reformatting the root logger would change how
# the analysis boards in this same process log, which is not this
# package's business. MF_LOG_JSON=0 turns it off for local reading.
obs.configure(json_output=os.getenv("MF_LOG_JSON", "1") != "0")


async def request_id_middleware(request, call_next):
    """Bind a request id for the whole request and echo it back.

    The header is the half that matters operationally: a resident quotes
    it off an error page or a support email, and it finds the access log
    line, the audit rows and the exception in one search. Without it the
    id exists but nobody outside the process ever sees one.
    """
    rid = deps.request_id(request)
    try:
        response = await call_next(request)
    finally:
        # Reset so a pooled worker task does not inherit this request's
        # identity into the next one — a stale user_id on somebody else's
        # log lines is worse than none.
        obs.user_id_var.set(None)
        obs.portal_var.set("")
    response.headers["X-Request-ID"] = rid
    return response


def attach(app) -> None:
    """Mount the ops routes and their middleware onto an app."""
    app.include_router(router)
    app.middleware("http")(request_id_middleware)


__all__ = ["router", "attach", "request_id_middleware"]
